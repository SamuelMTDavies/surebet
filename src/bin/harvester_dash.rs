//! Harvester Web Dashboard — shows markets near end date.  Click a market
//! to fetch its order book on demand, then select a winning outcome to
//! execute a buy + mint-and-sell-losers strategy.
//!
//! Usage:
//!   cargo run --bin harvester_dash              # paper mode
//!   cargo run --bin harvester_dash -- --live    # live execution
//!
//! Environment variables for live mint+sell:
//!   POLYMARKET_PRIVATE_KEY  — wallet private key for CTF split (on-chain)
//!   POLYGON_HTTP_URL        — Polygon HTTP RPC (default: https://polygon-rpc.com)

use alloy::primitives::{B256, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::Signer as _;
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use anyhow::{bail, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk::auth::Normal as NormalAuth;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{OrderType as SdkOrderType, Side as SdkSide};
use polymarket_client_sdk::clob::{Client as SdkClobClient, Config as SdkClobConfig};
use polymarket_client_sdk::ctf::types::{
    CollectionIdRequest, MergePositionsRequest, PositionIdRequest, RedeemPositionsRequest,
    SplitPositionRequest,
};
use polymarket_client_sdk::types::address;
use polymarket_client_sdk::POLYGON;
use std::sync::atomic::{AtomicBool, Ordering};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::info;

use surebet::auth::{ClobApiClient, L2Credentials, OrderSide};
use surebet::config::Config;
use surebet::harvester::{build_outcome_info, scan_markets, HarvestableMarket, OutcomeInfo};
use surebet::orderbook::OrderBookStore;
use surebet::ws::clob::{start_clob_ws, ClobEvent};

/// USDC on Polygon (USDC.e bridged)
const USDC: alloy::primitives::Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
/// CTF contract on Polygon (needs USDC approval for splits)
const CTF_CONTRACT: alloy::primitives::Address =
    address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

// Minimal ERC20 ABI for allowance checks
alloy::sol! {
    #[sol(rpc)]
    interface IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

// ERC-1155 balanceOf for CTF position tokens (used by merge guard +
// holdings queries). The CTF contract is a standard ERC-1155.
//
// `payoutDenominator` is a CTF-specific view that returns 0 for an unresolved
// condition and > 0 once UMA has reported the outcome. We use it to decide
// whether to show "Redeem" as actionable on the trades page.
alloy::sol! {
    #[sol(rpc)]
    interface IConditionalTokensView {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
    }
}

// ─── CTF Split wrapper ──────────────────────────────────────────────────────

/// Type-erased wrapper around `polymarket_client_sdk::ctf::Client` so we can
/// store it in `AppState` without leaking the generic provider type.
struct CtfSplitter {
    inner: Box<dyn CtfSplitTrait + Send + Sync>,
}

/// Result of an on-chain CTF transaction. Includes the gas accounting we
/// extract from the receipt so we can show running gas costs in the UI.
#[derive(Debug, Clone)]
struct OnchainTxResult {
    tx_hash: String,
    #[allow(dead_code)]
    gas_used: u64,
    /// Cost in MATIC: `gas_used * effective_gas_price`, scaled to whole MATIC.
    gas_cost_matic: Decimal,
}

#[async_trait::async_trait]
trait CtfSplitTrait: Send + Sync {
    async fn split_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<OnchainTxResult>;

    async fn merge_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<OnchainTxResult>;

    async fn redeem_position(&self, condition_id: B256) -> anyhow::Result<OnchainTxResult>;

    /// Returns `(yes_balance, no_balance)` for the wallet, in raw 6-decimal
    /// units. Used to gate merge calls so we don't submit a guaranteed-revert
    /// transaction (and waste gas) when the user has already sold one side.
    async fn balance_for(&self, condition_id: B256) -> anyhow::Result<(U256, U256)>;

    /// Returns the resolution status for a condition. Free read-only call.
    /// - `denominator == 0` → the market has not been resolved yet
    /// - `denominator > 0`  → resolved; `(yes_payout, no_payout)` indicates
    ///   which side won (one will be 1, the other 0 for binary markets)
    async fn resolution_for(
        &self,
        condition_id: B256,
    ) -> anyhow::Result<ResolutionStatus>;

    /// Current Polygon gas price in wei. Used by the gas-spike guard before
    /// minting. Read-only RPC call (eth_gasPrice), zero gas cost.
    async fn current_gas_price_wei(&self) -> anyhow::Result<u128>;
}

#[derive(Debug, Clone, Copy)]
struct ResolutionStatus {
    denominator: u128,
    yes_payout: u128,
    no_payout: u128,
}

impl ResolutionStatus {
    fn is_resolved(&self) -> bool {
        self.denominator > 0
    }

    /// Which side won (only meaningful when `is_resolved()` is true).
    /// Returns `Some("YES")`, `Some("NO")`, or `None` for tie / unresolved.
    fn winner(&self) -> Option<&'static str> {
        if !self.is_resolved() {
            return None;
        }
        if self.yes_payout > 0 && self.no_payout == 0 {
            Some("YES")
        } else if self.no_payout > 0 && self.yes_payout == 0 {
            Some("NO")
        } else {
            None
        }
    }
}

struct CtfClientWrapper<P: alloy::providers::Provider + Clone + Send + Sync + 'static> {
    client: polymarket_client_sdk::ctf::Client<P>,
    wallet: alloy::primitives::Address,
}

impl<P: alloy::providers::Provider + Clone + Send + Sync + 'static> CtfClientWrapper<P> {
    /// Fetch the receipt for `tx_hash` and compute MATIC gas cost. Falls back
    /// to a tx_hash-only result if the receipt is missing or fields are
    /// unavailable — we'd rather lose gas accuracy than fail an entire trade.
    async fn gas_for(&self, tx_hash: B256) -> OnchainTxResult {
        let provider = self.client.provider();
        let hash_str = format!("{tx_hash:?}");
        match provider.get_transaction_receipt(tx_hash).await {
            Ok(Some(receipt)) => {
                let gas_used: u64 = receipt.gas_used as u64;
                let gas_price: u128 = receipt.effective_gas_price;
                // gas_cost (wei) = gas_used * effective_gas_price
                // MATIC = wei / 1e18
                let wei = U256::from(gas_used) * U256::from(gas_price);
                let cost_matic = wei_to_matic_decimal(wei);
                OnchainTxResult {
                    tx_hash: hash_str,
                    gas_used,
                    gas_cost_matic: cost_matic,
                }
            }
            _ => OnchainTxResult {
                tx_hash: hash_str,
                gas_used: 0,
                gas_cost_matic: Decimal::ZERO,
            },
        }
    }
}

/// Convert a `U256` wei value to a `Decimal` representing whole MATIC. Rounds
/// to 9 decimal places (we never need more precision than nano-MATIC).
fn wei_to_matic_decimal(wei: U256) -> Decimal {
    // Down-cast to u128 — Polygon gas fees are tiny, fits comfortably.
    let wei_u128: u128 = wei.try_into().unwrap_or(u128::MAX);
    let matic = Decimal::from(wei_u128) / Decimal::from(1_000_000_000_000_000_000u128);
    matic.round_dp(9)
}

#[async_trait::async_trait]
impl<P: alloy::providers::Provider + Clone + Send + Sync + 'static> CtfSplitTrait
    for CtfClientWrapper<P>
{
    async fn split_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<OnchainTxResult> {
        let req = SplitPositionRequest::for_binary_market(USDC, condition_id, amount_usdc_units);
        let resp = self
            .client
            .split_position(&req)
            .await
            .map_err(|e| anyhow::anyhow!("CTF split failed: {}", e))?;
        Ok(self.gas_for(resp.transaction_hash).await)
    }

    async fn merge_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<OnchainTxResult> {
        let req = MergePositionsRequest::for_binary_market(USDC, condition_id, amount_usdc_units);
        let resp = self
            .client
            .merge_positions(&req)
            .await
            .map_err(|e| anyhow::anyhow!("CTF merge failed: {}", e))?;
        Ok(self.gas_for(resp.transaction_hash).await)
    }

    async fn redeem_position(&self, condition_id: B256) -> anyhow::Result<OnchainTxResult> {
        let req = RedeemPositionsRequest::for_binary_market(USDC, condition_id);
        let resp = self
            .client
            .redeem_positions(&req)
            .await
            .map_err(|e| anyhow::anyhow!("CTF redeem failed: {}", e))?;
        Ok(self.gas_for(resp.transaction_hash).await)
    }

    async fn balance_for(&self, condition_id: B256) -> anyhow::Result<(U256, U256)> {
        let provider = self.client.provider().clone();
        let erc1155 = IConditionalTokensView::new(CTF_CONTRACT, provider);
        let mut out = [U256::ZERO; 2];
        for (idx, index_set) in [(0, U256::from(1u64)), (1, U256::from(2u64))] {
            let coll = self
                .client
                .collection_id(
                    &CollectionIdRequest::builder()
                        .parent_collection_id(B256::ZERO)
                        .condition_id(condition_id)
                        .index_set(index_set)
                        .build(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("collection_id failed: {e}"))?;
            let pos = self
                .client
                .position_id(
                    &PositionIdRequest::builder()
                        .collateral_token(USDC)
                        .collection_id(coll.collection_id)
                        .build(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("position_id failed: {e}"))?;
            let bal = erc1155
                .balanceOf(self.wallet, pos.position_id)
                .call()
                .await
                .map_err(|e| anyhow::anyhow!("balanceOf failed: {e}"))?;
            out[idx] = bal;
        }
        Ok((out[0], out[1]))
    }

    async fn current_gas_price_wei(&self) -> anyhow::Result<u128> {
        let provider = self.client.provider();
        let price = provider
            .get_gas_price()
            .await
            .map_err(|e| anyhow::anyhow!("eth_gasPrice failed: {e}"))?;
        Ok(price)
    }

    async fn resolution_for(
        &self,
        condition_id: B256,
    ) -> anyhow::Result<ResolutionStatus> {
        let provider = self.client.provider().clone();
        let ctf = IConditionalTokensView::new(CTF_CONTRACT, provider);

        let denom = ctf
            .payoutDenominator(condition_id)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("payoutDenominator failed: {e}"))?;
        let denom_u128: u128 = denom.try_into().unwrap_or(0);

        // If unresolved, skip the payoutNumerators reads (saves 2 RPC calls).
        if denom_u128 == 0 {
            return Ok(ResolutionStatus {
                denominator: 0,
                yes_payout: 0,
                no_payout: 0,
            });
        }

        let yes = ctf
            .payoutNumerators(condition_id, U256::from(0u64))
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("payoutNumerators(0) failed: {e}"))?;
        let no = ctf
            .payoutNumerators(condition_id, U256::from(1u64))
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("payoutNumerators(1) failed: {e}"))?;

        Ok(ResolutionStatus {
            denominator: denom_u128,
            yes_payout: yes.try_into().unwrap_or(0),
            no_payout: no.try_into().unwrap_or(0),
        })
    }
}

// ─── SDK-backed order placement ─────────────────────────────────────────────
//
// Wraps the Polymarket SDK's authenticated CLOB client + signer to place
// fully EIP-712 signed orders. Replaces the naive `ClobApiClient::place_order`
// (which sent a stub payload and always got rejected as "Invalid order payload").

#[derive(Clone)]
struct OrderClient {
    client: SdkClobClient<Authenticated<NormalAuth>>,
    signer: PrivateKeySigner,
}

impl OrderClient {
    /// Build the authenticated SDK client from a private key.
    async fn new(host: &str, private_key: &str) -> anyhow::Result<Self> {
        let signer = LocalSigner::from_str(private_key)?.with_chain_id(Some(POLYGON));
        let config = SdkClobConfig::builder().use_server_time(true).build();
        let client = SdkClobClient::new(host, config)?
            .authentication_builder(&signer)
            .authenticate()
            .await
            .map_err(|e| anyhow::anyhow!("SDK authenticate failed: {}", e))?;
        Ok(Self { client, signer })
    }

    /// Place a GTC limit order. `token_id` is the CLOB ERC-1155 token id
    /// (as a decimal string — that's what Polymarket hands out).
    /// `price` and `size` are in USDC dollars.
    async fn place_limit(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        side: OrderSide,
    ) -> anyhow::Result<PlaceOrderOutcome> {
        let token_id_u = U256::from_str(token_id)
            .map_err(|e| anyhow::anyhow!("invalid token_id: {}", e))?;
        let sdk_side = match side {
            OrderSide::Buy => SdkSide::Buy,
            OrderSide::Sell => SdkSide::Sell,
        };

        let order = self
            .client
            .limit_order()
            .token_id(token_id_u)
            .order_type(SdkOrderType::GTC)
            .price(price)
            .size(size)
            .side(sdk_side)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("order build failed: {}", e))?;

        let signed = self
            .client
            .sign(&self.signer, order)
            .await
            .map_err(|e| anyhow::anyhow!("order sign failed: {}", e))?;

        let resp = self
            .client
            .post_order(signed)
            .await
            .map_err(|e| anyhow::anyhow!("post_order failed: {}", e))?;

        Ok(PlaceOrderOutcome {
            success: resp.success,
            order_id: format!("{}", resp.order_id),
            error_msg: resp.error_msg.unwrap_or_default(),
        })
    }
}

/// Normalized result shape so the harvest handler doesn't need to know about
/// SDK response types.
#[derive(Debug, Clone)]
struct PlaceOrderOutcome {
    success: bool,
    order_id: String,
    error_msg: String,
}

// ─── State ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    markets: Arc<RwLock<Vec<HarvestableMarket>>>,
    book_store: Arc<OrderBookStore>,
    clob_url: String,
    api: Option<Arc<ClobApiClient>>,
    /// SDK-backed order placer (EIP-712 signed). Only populated in live mode
    /// with a private key. Used for place_order; balance queries still go
    /// through `api` (ClobApiClient).
    order_client: Option<Arc<OrderClient>>,
    ctf: Option<Arc<CtfSplitter>>,
    activity: Arc<RwLock<Vec<ActivityEntry>>>,
    max_buy: Decimal,
    min_sell: Decimal,
    max_trade: Decimal,
    max_daily: Decimal,
    /// Max acceptable Polygon gas price in Gwei. Above this, mints are refused.
    max_gas_gwei: Decimal,
    /// Minimum estimated net profit (sell_revenue - gas_cost_usdc) for a
    /// mint+sell trade to proceed.
    min_profit_usdc: Decimal,
    /// MATIC → USD price assumption for gas cost estimation.
    matic_usd_price: Decimal,
    /// Rolling spend tracker: (timestamp, amount) pairs for daily limit.
    daily_spend: Arc<RwLock<Vec<(chrono::DateTime<Utc>, Decimal)>>>,
    live_mode: bool,
    /// Sticky flag: set to true when the CLOB API returns a 403 region-block.
    /// While true, the harvest endpoint refuses to mint new positions (to avoid
    /// stranding more tokens). User can clear it via /api/clear-geoblock after
    /// enabling a VPN.
    geoblocked: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityEntry {
    time: String,
    market: String,
    outcome: String,
    strategy: String,       // "BUY" | "MINT+SELL" | "HOOVER" | "MERGE" | "REDEEM"
    buy_shares: String,
    buy_cost: String,
    mint_cost: String,      // USDC spent minting complete sets
    sell_revenue: String,   // revenue from selling loser tokens
    net_profit: String,     // total profit accounting for all costs
    status: String,
    /// Hex condition_id (0x...) for the market — needed to recover stuck
    /// positions via `ctf_admin merge` when the sell-side fails.
    #[serde(default)]
    condition_id: String,
    /// Total MATIC gas spent for the on-chain operations in this entry
    /// (split + merge + redeem combined). Decimal-as-string so the JSONL log
    /// is human-readable.
    #[serde(default = "default_zero")]
    gas_matic: String,
    /// ISO date string (YYYY-MM-DD) for grouping totals by day. Optional —
    /// older entries don't have it and `time` (HH:MM:SS) suffices for ordering
    /// within the file.
    #[serde(default)]
    date: String,
}

fn default_zero() -> String {
    "0".to_string()
}

#[derive(Debug, Deserialize)]
struct HarvestRequest {
    market_idx: usize,      // index into markets list
    winner_idx: usize,      // which outcome is the winner
    label: String,
    market_question: String,
}

#[derive(Debug, Deserialize)]
struct BookQuery {
    idx: usize,
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let live_mode = args.iter().any(|a| a == "--live");

    let config = match Config::load(std::path::Path::new("surebet.toml")) {
        Ok(c) => c,
        Err(_) => Config::from_env(),
    };

    let gamma_url = &config.polymarket.gamma_url;
    let clob_url = config.polymarket.clob_url.clone();
    let harvester = &config.harvester;

    let max_buy = Decimal::from_str(&format!("{}", harvester.max_buy_price))
        .unwrap_or_else(|_| Decimal::from_str("0.995").unwrap());
    let min_sell = Decimal::from_str(&format!("{}", harvester.min_sell_price))
        .unwrap_or_else(|_| Decimal::from_str("0.005").unwrap());
    let max_trade = Decimal::from_str(&format!("{}", harvester.max_trade_usd))
        .unwrap_or_else(|_| Decimal::from_str("25").unwrap());
    let max_daily = Decimal::from_str(&format!("{}", harvester.max_daily_usd))
        .unwrap_or_else(|_| Decimal::from_str("100").unwrap());
    let max_gas_gwei = Decimal::from_str(&format!("{}", harvester.max_gas_gwei))
        .unwrap_or_else(|_| Decimal::from(600));
    let min_profit_usdc = Decimal::from_str(&format!("{}", harvester.min_profit_usdc))
        .unwrap_or_else(|_| Decimal::from_str("0.005").unwrap());
    let matic_usd_price = Decimal::from_str(&format!("{}", harvester.matic_usd_price))
        .unwrap_or_else(|_| Decimal::from_str("0.40").unwrap());

    let bind_addr = config
        .harvester
        .dash_bind
        .clone()
        .unwrap_or_else(|| "127.0.0.1:3031".to_string());

    println!("=== Harvester Dashboard ===");
    println!(
        "Mode: {}  |  Buy limit: ${}  |  Window: {}d",
        if live_mode { "LIVE" } else { "PAPER" },
        max_buy,
        harvester.end_date_window_days,
    );

    // ── Step 1: Scan markets (no book fetching) ──────────────────────────

    println!("Scanning Gamma API...");
    // Dashboard shows ALL markets — no truncation.  Books fetched on demand.
    let markets = scan_markets(gamma_url, harvester.end_date_window_days, usize::MAX).await?;

    if markets.is_empty() {
        bail!("No markets found matching criteria.");
    }

    println!("  {} markets loaded (books fetched on demand)", markets.len());

    // ── Step 2: Build API client ─────────────────────────────────────────

    let book_store = Arc::new(OrderBookStore::new());

    let api = if live_mode {
        let creds = L2Credentials::from_config(
            &config.polymarket.api_address,
            &config.polymarket.api_key,
            &config.polymarket.api_secret,
            &config.polymarket.api_passphrase,
        );
        match creds {
            Some(c) => Some(Arc::new(ClobApiClient::new(clob_url.clone(), c))),
            None => {
                println!("WARNING: --live but missing credentials, falling back to paper mode");
                None
            }
        }
    } else {
        None
    };

    // ── Step 2a: Build SDK-backed order client (EIP-712 signed orders) ────
    //
    // The naive ClobApiClient.place_order sends an invalid payload. All order
    // placement routes through this SDK-backed client instead, which signs
    // each order with the wallet's private key per Polymarket's EIP-712
    // requirement. Requires POLYMARKET_PRIVATE_KEY in the environment.
    let order_client: Option<Arc<OrderClient>> = if live_mode && api.is_some() {
        match std::env::var("POLYMARKET_PRIVATE_KEY") {
            Ok(pk) => match OrderClient::new(&clob_url, &pk).await {
                Ok(oc) => {
                    println!("  SDK order client ready (EIP-712 signing enabled)");
                    Some(Arc::new(oc))
                }
                Err(e) => {
                    println!(
                        "WARNING: SDK order client init failed: {e} — order placement disabled"
                    );
                    None
                }
            },
            Err(_) => {
                println!(
                    "WARNING: POLYMARKET_PRIVATE_KEY not set — order placement disabled"
                );
                None
            }
        }
    } else {
        None
    };

    // ── Step 2b: Build CTF client for mint+sell (on-chain) ────────────────

    let ctf: Option<Arc<CtfSplitter>> = if live_mode {
        match std::env::var("POLYMARKET_PRIVATE_KEY") {
            Ok(pk) => {
                let rpc_url = std::env::var("POLYGON_HTTP_URL")
                    .unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
                match LocalSigner::from_str(&pk) {
                    Ok(signer) => {
                        let wallet_addr = signer.address();
                        let signer = signer.with_chain_id(Some(POLYGON_CHAIN_ID));
                        match ProviderBuilder::new()
                            .wallet(signer)
                            .connect(&rpc_url)
                            .await
                        {
                            Ok(provider) => {
                                // Check USDC approval for CTF contract
                                let usdc_contract = IERC20::new(USDC, provider.clone());
                                match usdc_contract
                                    .allowance(wallet_addr, CTF_CONTRACT)
                                    .call()
                                    .await
                                {
                                    Ok(allowance) => {
                                        if allowance.is_zero() {
                                            println!("WARNING: USDC not approved for CTF contract!");
                                            println!("  Run the approvals example first:");
                                            println!("  cd polymarket-client-sdk-patch && cargo run --example approvals --features ctf");
                                            println!("  Mint+sell DISABLED until approved.");
                                            None
                                        } else {
                                            let allowance_usdc = allowance / U256::from(1_000_000u64);
                                            println!("  USDC allowance for CTF: ${allowance_usdc}");
                                            match polymarket_client_sdk::ctf::Client::with_neg_risk(
                                                provider,
                                                POLYGON_CHAIN_ID,
                                            ) {
                                                Ok(c) => {
                                                    println!("  CTF client ready (mint+sell enabled)");
                                                    Some(Arc::new(CtfSplitter {
                                                        inner: Box::new(CtfClientWrapper {
                                                            client: c,
                                                            wallet: wallet_addr,
                                                        }),
                                                    }))
                                                }
                                                Err(e) => {
                                                    println!("WARNING: CTF client init failed: {e}, mint+sell disabled");
                                                    None
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        println!("WARNING: could not check USDC allowance: {e}");
                                        println!("  Proceeding without mint+sell.");
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                println!("WARNING: Polygon RPC connect failed: {e}, mint+sell disabled");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        println!("WARNING: invalid private key: {e}, mint+sell disabled");
                        None
                    }
                }
            }
            Err(_) => {
                println!("  No POLYMARKET_PRIVATE_KEY set, mint+sell disabled (buy-only)");
                None
            }
        }
    } else {
        None
    };

    // ── Step 3: Serve dashboard ──────────────────────────────────────────

    println!(
        "  Limits: ${} per trade, ${} daily",
        max_trade, max_daily,
    );

    // Rehydrate activity log from disk so the dashboard isn't empty after a
    // restart. The JSONL file is the source of truth.
    let prior_activity = load_activity_log().await;
    println!(
        "  Activity log: {} prior entries loaded from harvester_trades.jsonl",
        prior_activity.len()
    );

    let state = AppState {
        markets: Arc::new(RwLock::new(markets)),
        book_store,
        clob_url,
        api,
        order_client,
        ctf,
        activity: Arc::new(RwLock::new(prior_activity)),
        max_buy,
        min_sell,
        max_trade,
        max_daily,
        max_gas_gwei,
        min_profit_usdc,
        matic_usd_price,
        daily_spend: Arc::new(RwLock::new(Vec::new())),
        live_mode,
        geoblocked: Arc::new(AtomicBool::new(false)),
    };

    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/trades", get(trades_html))
        .route("/api/markets", get(api_markets))
        .route("/api/book", get(api_book))
        .route("/api/harvest", post(api_harvest))
        .route("/api/activity", get(api_activity))
        .route("/api/merge", post(api_merge))
        .route("/api/redeem", post(api_redeem))
        .route("/api/clear-geoblock", post(api_clear_geoblock))
        .route("/api/status", get(api_status))
        .route("/api/positions", get(api_positions))
        .route("/ws/book", get(ws_book_handler))
        .with_state(state);

    println!("Dashboard: http://{}", bind_addr);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ─── API handlers ────────────────────────────────────────────────────────────

async fn api_markets(State(state): State<AppState>) -> impl IntoResponse {
    let markets = state.markets.read().await;
    Json(markets.clone())
}

async fn api_activity(State(state): State<AppState>) -> impl IntoResponse {
    let activity = state.activity.read().await;
    Json(activity.clone())
}

async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "live_mode": state.live_mode,
        "geoblocked": state.geoblocked.load(Ordering::Relaxed),
        "ctf_enabled": state.ctf.is_some(),
        "max_trade": state.max_trade.to_string(),
        "max_daily": state.max_daily.to_string(),
    }))
}

async fn api_clear_geoblock(State(state): State<AppState>) -> impl IntoResponse {
    state.geoblocked.store(false, Ordering::Relaxed);
    Json(serde_json::json!({"ok": true, "geoblocked": false}))
}

#[derive(Debug, Deserialize)]
struct CtfActionRequest {
    /// Hex condition_id (0x...)
    condition_id: String,
    /// USDC amount to merge (only used by /api/merge). Ignored by /api/redeem.
    #[serde(default)]
    amount: Option<String>,
    /// Optional human-readable label of the original market (for the activity log)
    #[serde(default)]
    market: Option<String>,
}

async fn api_merge(
    State(state): State<AppState>,
    Json(req): Json<CtfActionRequest>,
) -> impl IntoResponse {
    let ctf = match state.ctf.as_ref() {
        Some(c) => c.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "CTF client not enabled (run with --live and POLYMARKET_PRIVATE_KEY)"})),
            )
                .into_response();
        }
    };

    let condition_id = match B256::from_str(&req.condition_id) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid condition_id: {e}")})),
            )
                .into_response();
        }
    };

    let amount_str = match req.amount.as_deref() {
        Some(s) if !s.is_empty() => s,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "amount is required for merge"})),
            )
                .into_response();
        }
    };

    let amount_dec = match Decimal::from_str(amount_str) {
        Ok(d) if d > Decimal::ZERO => d,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "amount must be a positive decimal"})),
            )
                .into_response();
        }
    };

    let usdc_units = usdc_units_from_decimal(amount_dec);
    if usdc_units == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "amount too small"})),
        )
            .into_response();
    }

    // Pre-flight: read on-chain YES/NO balances. Merge requires equal amounts
    // of both — if either side is zero (or below the requested amount), the
    // CTF contract will revert with SafeMath underflow. Refuse here so we
    // don't waste gas on a guaranteed failure.
    match ctf.inner.balance_for(condition_id).await {
        Ok((yes_bal, no_bal)) => {
            let need = U256::from(usdc_units);
            if yes_bal < need || no_bal < need {
                let yes_dec =
                    Decimal::from(yes_bal.to::<u128>()) / Decimal::from(1_000_000u64);
                let no_dec = Decimal::from(no_bal.to::<u128>()) / Decimal::from(1_000_000u64);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!(
                            "Merge not possible. Need {} of each but you hold YES={} NO={}. \
                             If you sold one side already, the position is no longer mergeable — \
                             wait for resolution and use Redeem instead.",
                            amount_dec, yes_dec, no_dec
                        ),
                        "yes_balance": yes_dec.to_string(),
                        "no_balance": no_dec.to_string(),
                    })),
                )
                    .into_response();
            }
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Balance pre-check failed: {e}")
                })),
            )
                .into_response();
        }
    }

    let result = ctf
        .inner
        .merge_position(condition_id, U256::from(usdc_units))
        .await;

    let (status_text, ok, gas_matic_str) = match result {
        Ok(tx) => (
            format!(
                "MERGE OK tx={} gas={} MATIC",
                tx.tx_hash, tx.gas_cost_matic
            ),
            true,
            format!("{}", tx.gas_cost_matic),
        ),
        Err(e) => (format!("MERGE ERR: {e}"), false, "0".to_string()),
    };

    // Append a new activity entry for the merge
    let entry = ActivityEntry {
        time: Utc::now().format("%H:%M:%S").to_string(),
        market: req.market.clone().unwrap_or_default(),
        outcome: String::new(),
        strategy: "MERGE".to_string(),
        buy_shares: "0".to_string(),
        buy_cost: "0.0000".to_string(),
        mint_cost: "0.0000".to_string(),
        // Merging recovers `amount` USDC.e by burning the YES+NO pair.
        sell_revenue: format!("{:.4}", amount_dec),
        net_profit: "0.0000".to_string(),
        status: status_text.clone(),
        condition_id: req.condition_id.clone(),
        gas_matic: gas_matic_str,
        date: Utc::now().format("%Y-%m-%d").to_string(),
    };

    persist_and_record(&state, entry.clone()).await;

    // If merge succeeded, update earlier stuck trades for this condition_id
    // so the UI reflects that the position is recovered.
    if ok {
        mark_position_recovered(&state, &req.condition_id, "RECOVERED via merge").await;
    }

    Json(serde_json::json!({"ok": ok, "entry": entry})).into_response()
}

async fn api_redeem(
    State(state): State<AppState>,
    Json(req): Json<CtfActionRequest>,
) -> impl IntoResponse {
    let ctf = match state.ctf.as_ref() {
        Some(c) => c.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "CTF client not enabled (run with --live and POLYMARKET_PRIVATE_KEY)"})),
            )
                .into_response();
        }
    };

    let condition_id = match B256::from_str(&req.condition_id) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid condition_id: {e}")})),
            )
                .into_response();
        }
    };

    let result = ctf.inner.redeem_position(condition_id).await;

    let (status_text, ok, gas_matic_str) = match result {
        Ok(tx) => (
            format!(
                "REDEEM OK tx={} gas={} MATIC",
                tx.tx_hash, tx.gas_cost_matic
            ),
            true,
            format!("{}", tx.gas_cost_matic),
        ),
        Err(e) => (format!("REDEEM ERR: {e}"), false, "0".to_string()),
    };

    let entry = ActivityEntry {
        time: Utc::now().format("%H:%M:%S").to_string(),
        market: req.market.clone().unwrap_or_default(),
        outcome: String::new(),
        strategy: "REDEEM".to_string(),
        buy_shares: "0".to_string(),
        buy_cost: "0.0000".to_string(),
        mint_cost: "0.0000".to_string(),
        sell_revenue: "0.0000".to_string(),
        net_profit: "0.0000".to_string(),
        status: status_text.clone(),
        condition_id: req.condition_id.clone(),
        gas_matic: gas_matic_str,
        date: Utc::now().format("%Y-%m-%d").to_string(),
    };

    persist_and_record(&state, entry.clone()).await;

    if ok {
        mark_position_recovered(&state, &req.condition_id, "RECOVERED via redeem").await;
    }

    Json(serde_json::json!({"ok": ok, "entry": entry})).into_response()
}

/// Append entry to in-memory activity AND the on-disk JSONL log.
async fn persist_and_record(state: &AppState, entry: ActivityEntry) {
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("harvester_trades.jsonl")
        .await
    {
        use tokio::io::AsyncWriteExt;
        let line = serde_json::to_string(&entry).unwrap_or_default();
        let _ = f.write_all(format!("{}\n", line).as_bytes()).await;
    }
    let mut activity = state.activity.write().await;
    activity.push(entry);
}

/// Mark prior stuck trades for this condition_id as recovered. We mutate the
/// in-memory copy so the dashboard reflects it; the on-disk JSONL log retains
/// the original status (we append a new RECOVERED entry instead).
async fn mark_position_recovered(state: &AppState, condition_id: &str, suffix: &str) {
    let mut activity = state.activity.write().await;
    for e in activity.iter_mut() {
        if e.condition_id.eq_ignore_ascii_case(condition_id)
            && e.status.contains("MINT OK")
            && (e.status.contains("SELL ERR") || e.status.contains("SELL FAIL"))
            && !e.status.contains("RECOVERED")
        {
            e.status = format!("{} | {}", e.status, suffix);
        }
    }
}

// ─── Stats + positions ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize)]
struct TradeStats {
    /// Number of activity entries (any kind)
    total_entries: usize,
    /// Buy/mint/hoover trades that landed (excludes paper, excludes recovery actions)
    successful_trades: usize,
    /// Trades where mint+sell or buy returned an error
    failed_trades: usize,
    /// Stuck trades: minted but sell/buy failed and not yet recovered
    stuck_trades: usize,
    /// Lifetime totals (in USDC)
    total_buy_cost: Decimal,
    total_mint_cost: Decimal,
    total_sell_revenue: Decimal,
    /// Realized profit from completed trades (excludes pending positions)
    realized_pnl: Decimal,
    /// Total MATIC paid in gas across all on-chain operations
    total_gas_matic: Decimal,
    /// Gas converted to USDC at the configured MATIC price
    total_gas_usdc: Decimal,
    /// realized_pnl - total_gas_usdc — what you actually netted after gas
    net_pnl_after_gas: Decimal,
}

fn compute_stats(activity: &[ActivityEntry], matic_usd: Decimal) -> TradeStats {
    let mut s = TradeStats::default();
    s.total_entries = activity.len();
    for a in activity {
        // Sum gas regardless of success
        if let Ok(g) = Decimal::from_str(&a.gas_matic) {
            s.total_gas_matic += g;
        }
        // Skip paper-mode entries from monetary totals
        if a.strategy.starts_with("PAPER") {
            continue;
        }
        if let Ok(v) = Decimal::from_str(&a.buy_cost) {
            s.total_buy_cost += v;
        }
        if let Ok(v) = Decimal::from_str(&a.mint_cost) {
            s.total_mint_cost += v;
        }
        if let Ok(v) = Decimal::from_str(&a.sell_revenue) {
            s.total_sell_revenue += v;
        }
        if let Ok(v) = Decimal::from_str(&a.net_profit) {
            s.realized_pnl += v;
        }
        // Classify
        let is_stuck = a.status.contains("MINT OK")
            && (a.status.contains("SELL ERR") || a.status.contains("SELL FAIL"))
            && !a.status.contains("RECOVERED");
        if is_stuck {
            s.stuck_trades += 1;
        }
        if a.status.contains("ERR") || a.status.contains("FAIL") {
            s.failed_trades += 1;
        } else if a.status.contains("OK") {
            s.successful_trades += 1;
        }
    }
    s.total_gas_usdc = (s.total_gas_matic * matic_usd).round_dp(6);
    s.net_pnl_after_gas = s.realized_pnl - s.total_gas_usdc;
    s
}

#[derive(Debug, Clone, Serialize)]
struct OpenPosition {
    condition_id: String,
    market: String,
    /// Last activity entry's outcome label (the side the user picked as winner)
    winner_label: String,
    /// Raw on-chain balances (6-decimal base units)
    yes_tokens: Decimal,
    no_tokens: Decimal,
    /// True if the user holds equal YES + NO and could merge
    mergeable: bool,
    /// On-chain resolution state. `unresolved` / `resolved-yes` / `resolved-no`.
    resolution: String,
    /// True iff the market is resolved (read from CTF.payoutDenominator)
    redeemable: bool,
    /// Original mint cost from the activity log (for P&L estimation)
    mint_cost_dec: Decimal,
    /// Time-left string ("3d 4h", "in 12h", "ended 2h ago", or "unknown") if
    /// the market is in the loaded markets list
    time_left: String,
    end_date_iso: Option<String>,
}

/// Build the list of open positions by:
/// 1. Walking the activity log for unique condition_ids that had a successful MINT OK
/// 2. For each, querying the on-chain CTF balance
/// 3. Filtering out fully-emptied positions
async fn compute_open_positions(state: &AppState) -> Vec<OpenPosition> {
    let activity_snap: Vec<ActivityEntry> = {
        let activity = state.activity.read().await;
        activity.clone()
    };

    // Index latest market/outcome/mint_cost per condition_id
    use std::collections::BTreeMap;
    let mut latest: BTreeMap<String, (String, String, Decimal)> = BTreeMap::new();
    for a in &activity_snap {
        if a.condition_id.is_empty() {
            continue;
        }
        if !a.status.contains("MINT OK") {
            continue;
        }
        let mc = Decimal::from_str(&a.mint_cost).unwrap_or(Decimal::ZERO);
        latest.insert(
            a.condition_id.clone(),
            (a.market.clone(), a.outcome.clone(), mc),
        );
    }

    // Lookup market end dates from the in-memory market list (if loaded)
    let markets_snap: Vec<HarvestableMarket> = {
        let m = state.markets.read().await;
        m.clone()
    };

    let mut positions = Vec::new();
    let Some(ctf) = state.ctf.as_ref() else {
        return positions; // CTF not enabled — can't query holdings
    };
    for (cid_str, (market_q, outcome, mint_cost_dec)) in latest {
        let cid = match B256::from_str(&cid_str) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let (yes_raw, no_raw) = match ctf.inner.balance_for(cid).await {
            Ok(b) => b,
            Err(_) => (U256::ZERO, U256::ZERO),
        };
        if yes_raw == U256::ZERO && no_raw == U256::ZERO {
            // Position fully closed/redeemed/sold — don't show
            continue;
        }
        let yes = Decimal::from(yes_raw.to::<u128>()) / Decimal::from(1_000_000u64);
        let no = Decimal::from(no_raw.to::<u128>()) / Decimal::from(1_000_000u64);

        // Try to find the market in the loaded list to get end_date
        let (time_left, end_iso) = markets_snap
            .iter()
            .find(|m| m.condition_id.eq_ignore_ascii_case(&cid_str))
            .and_then(|m| m.end_date.map(|d| (format_time_left(d), d.to_rfc3339())))
            .unwrap_or_else(|| ("unknown (outside window)".to_string(), String::new()));

        // Resolution status — free read-only call to CTF.payoutDenominator.
        // Falls back to "unresolved" if the call errors out so a flaky RPC
        // can't break the page.
        let res = ctf
            .inner
            .resolution_for(cid)
            .await
            .unwrap_or(ResolutionStatus {
                denominator: 0,
                yes_payout: 0,
                no_payout: 0,
            });
        let resolution = if !res.is_resolved() {
            "unresolved".to_string()
        } else {
            match res.winner() {
                Some(side) => format!("resolved-{}", side.to_lowercase()),
                None => "resolved-tied".to_string(),
            }
        };

        positions.push(OpenPosition {
            condition_id: cid_str,
            market: market_q,
            winner_label: outcome,
            yes_tokens: yes,
            no_tokens: no,
            mergeable: yes_raw > U256::ZERO && no_raw > U256::ZERO,
            redeemable: res.is_resolved(),
            resolution,
            mint_cost_dec,
            time_left,
            end_date_iso: if end_iso.is_empty() { None } else { Some(end_iso) },
        });
    }
    positions
}

/// Format a time-left string like "2d 4h", "in 3h", "ended 2h ago".
fn format_time_left(end: chrono::DateTime<Utc>) -> String {
    let now = Utc::now();
    let diff = end - now;
    let total_secs = diff.num_seconds();
    if total_secs < 0 {
        let past = -total_secs;
        if past < 3600 {
            return format!("ended {}m ago", past / 60);
        }
        if past < 86400 {
            return format!("ended {}h ago", past / 3600);
        }
        return format!("ended {}d ago", past / 86400);
    }
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let mins = (total_secs % 3600) / 60;
    if days > 0 {
        format!("{}d {}h", days, hours)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

async fn api_positions(State(state): State<AppState>) -> impl IntoResponse {
    Json(compute_open_positions(&state).await)
}

/// Convert a USDC amount in dollars (e.g. `2.0000`) to 6-decimal base units
/// (`2_000_000`). Handles any scale correctly — unlike `.to_string().parse()`
/// which breaks on fractional scales like `"2000000.0000"`.
///
/// Returns 0 on overflow or non-positive input.
fn usdc_units_from_decimal(amount: Decimal) -> u64 {
    if amount <= Decimal::ZERO {
        return 0;
    }
    (amount * Decimal::from(1_000_000u64))
        .round()
        .to_u64()
        .unwrap_or(0)
}

/// Load the on-disk JSONL activity log into memory at startup. Silently skips
/// malformed lines rather than aborting — the log is append-only from multiple
/// writers and any single bad entry shouldn't kill the dashboard.
async fn load_activity_log() -> Vec<ActivityEntry> {
    let path = "harvester_trades.jsonl";
    let contents = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(_) => return Vec::new(), // File doesn't exist yet on first run
    };
    contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<ActivityEntry>(l).ok())
        .collect()
}

/// Fetch order book on demand for a specific market index.
async fn api_book(
    State(state): State<AppState>,
    Query(q): Query<BookQuery>,
) -> impl IntoResponse {
    let markets = state.markets.read().await;
    let market = match markets.get(q.idx) {
        Some(m) => m.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid market index"})),
            )
                .into_response();
        }
    };
    drop(markets);

    let mut outcomes: Vec<OutcomeInfo> = Vec::new();
    for (i, token_id) in market.clob_token_ids.iter().enumerate() {
        let label = market
            .outcomes
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("Outcome {}", i));
        let book = state.book_store.fetch_rest_book(&state.clob_url, token_id).await;

        let info = match book {
            Some(ref b) => build_outcome_info(&label, token_id, b, state.max_buy, state.min_sell),
            None => OutcomeInfo {
                label: label.clone(),
                token_id: token_id.clone(),
                best_ask: None,
                sweepable_shares: Decimal::ZERO,
                sweepable_cost: Decimal::ZERO,
                sweepable_profit: Decimal::ZERO,
                best_bid: None,
                sellable_shares: Decimal::ZERO,
                sellable_revenue: Decimal::ZERO,
            },
        };
        outcomes.push(info);
    }

    Json(serde_json::json!({
        "idx": q.idx,
        "question": market.question,
        "outcomes": outcomes,
    }))
    .into_response()
}

async fn api_harvest(
    State(state): State<AppState>,
    Json(req): Json<HarvestRequest>,
) -> impl IntoResponse {
    let now = Utc::now().format("%H:%M:%S").to_string();

    // ── 1. Look up market and validate ──────────────────────────────────
    let market = {
        let markets = state.markets.read().await;
        match markets.get(req.market_idx) {
            Some(m) => m.clone(),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "Invalid market index"})),
                )
                    .into_response();
            }
        }
    };

    if req.winner_idx >= market.clob_token_ids.len() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid winner outcome index"})),
        )
            .into_response();
    }

    let winner_token_id = &market.clob_token_ids[req.winner_idx];

    // ── 2. Re-fetch ALL outcome books (fresh data) ──────────────────────
    let mut outcomes: Vec<OutcomeInfo> = Vec::new();
    for (i, token_id) in market.clob_token_ids.iter().enumerate() {
        let label = market
            .outcomes
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("Outcome {}", i));
        let book = state
            .book_store
            .fetch_rest_book(&state.clob_url, token_id)
            .await;
        let info = match book {
            Some(ref b) => build_outcome_info(&label, token_id, b, state.max_buy, state.min_sell),
            None => OutcomeInfo {
                label: label.clone(),
                token_id: token_id.clone(),
                best_ask: None,
                sweepable_shares: Decimal::ZERO,
                sweepable_cost: Decimal::ZERO,
                sweepable_profit: Decimal::ZERO,
                best_bid: None,
                sellable_shares: Decimal::ZERO,
                sellable_revenue: Decimal::ZERO,
            },
        };
        outcomes.push(info);
    }

    let winner = &outcomes[req.winner_idx];
    let buy_shares = winner.sweepable_shares;
    let buy_cost = winner.sweepable_cost;

    // ── 3. Check loser sell-side (mint+sell opportunity) ─────────────────
    // For each loser outcome, check fillable bids.  The mint amount is
    // capped by the minimum sellable depth across ALL losers (bottleneck).
    let mut min_loser_sellable = Decimal::MAX;
    let mut total_sell_revenue = Decimal::ZERO;
    let mut loser_sell_plans: Vec<(String, Decimal, Decimal)> = Vec::new(); // (token_id, shares, best_bid)
    let mut has_loser_bids = true;

    for (i, info) in outcomes.iter().enumerate() {
        if i == req.winner_idx {
            continue;
        }
        if info.sellable_shares == Decimal::ZERO || info.best_bid.is_none() {
            has_loser_bids = false;
            break;
        }
        min_loser_sellable = min_loser_sellable.min(info.sellable_shares);
        total_sell_revenue += info.sellable_revenue;
        loser_sell_plans.push((
            info.token_id.clone(),
            info.sellable_shares,
            info.best_bid.unwrap(),
        ));
    }

    // Mint+sell is viable if: all losers have bids AND selling them recovers
    // more than the minting cost of those shares.
    // mint_cost = mint_amount (1 USDC per complete set)
    // net profit from mint+sell = sell_revenue - mint_amount + mint_amount
    //   (the minted winner tokens are kept, each worth $1 at resolution)
    //   So: mint profit = total_sell_revenue (revenue from selling losers,
    //   since the winner tokens you keep are worth exactly what you minted)
    let use_mint_sell = has_loser_bids
        && !loser_sell_plans.is_empty()
        && min_loser_sellable > Decimal::ZERO
        && state.ctf.is_some();

    let mut mint_amount = if use_mint_sell {
        min_loser_sellable
    } else {
        Decimal::ZERO
    };

    // ── 3b. SAFETY: enforce spending limits ─────────────────────────────
    // Total cost of this trade = buy_cost + mint_amount
    let mut capped_buy_cost = buy_cost;
    let mut capped_buy_shares = buy_shares;

    // Cap per-trade spend
    let total_raw_cost = capped_buy_cost + mint_amount;
    if total_raw_cost > state.max_trade {
        // Scale down proportionally to fit within max_trade
        if mint_amount > Decimal::ZERO && capped_buy_cost > Decimal::ZERO {
            // Both strategies active — prioritise mint (higher margin), then buy with remainder
            mint_amount = mint_amount.min(state.max_trade);
            let remainder = state.max_trade - mint_amount;
            if remainder > Decimal::ZERO && capped_buy_cost > remainder {
                // Scale buy shares proportionally to capped cost
                let scale = remainder / capped_buy_cost;
                capped_buy_shares = (capped_buy_shares * scale).round_dp(2);
                capped_buy_cost = remainder;
            } else if remainder == Decimal::ZERO {
                capped_buy_shares = Decimal::ZERO;
                capped_buy_cost = Decimal::ZERO;
            }
        } else if mint_amount > Decimal::ZERO {
            mint_amount = mint_amount.min(state.max_trade);
        } else {
            // Buy only — cap cost directly
            let scale = state.max_trade / capped_buy_cost;
            capped_buy_shares = (capped_buy_shares * scale).round_dp(2);
            capped_buy_cost = state.max_trade;
        }
    }

    // Check rolling 24h daily limit
    let now_utc = Utc::now();
    let cutoff = now_utc - chrono::Duration::hours(24);
    {
        let mut spend_log = state.daily_spend.write().await;
        // Prune old entries
        spend_log.retain(|(ts, _)| *ts > cutoff);
        let spent_today: Decimal = spend_log.iter().map(|(_, amt)| amt).sum();
        let remaining_daily = state.max_daily - spent_today;

        if remaining_daily <= Decimal::ZERO {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!(
                        "Daily limit reached (${:.2} spent of ${:.2} max in last 24h)",
                        spent_today, state.max_daily
                    )
                })),
            )
                .into_response();
        }

        // Cap total cost to remaining daily budget
        let total_capped = capped_buy_cost + mint_amount;
        if total_capped > remaining_daily {
            let scale = remaining_daily / total_capped;
            capped_buy_shares = (capped_buy_shares * scale).round_dp(2);
            capped_buy_cost = (capped_buy_cost * scale).round_dp(4);
            mint_amount = (mint_amount * scale).round_dp(4);
        }
    }

    // Recalculate buy profit after capping
    let buy_profit = capped_buy_shares - capped_buy_cost;
    let buy_shares = capped_buy_shares;
    let buy_cost = capped_buy_cost;

    // Check USDC balance (live mode only)
    if state.live_mode {
        if let Some(ref api) = state.api {
            let balance = api.get_balance_usdc().await;
            let needed = buy_cost + mint_amount;
            if needed > balance {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!(
                            "Insufficient USDC: need ${:.4} but balance is ${:.4}",
                            needed, balance
                        )
                    })),
                )
                    .into_response();
            }
        }
    }

    // Also re-check that sell-side bids still have enough depth for capped mint
    if use_mint_sell && mint_amount > Decimal::ZERO {
        // Recalculate total_sell_revenue for the capped mint_amount
        total_sell_revenue = Decimal::ZERO;
        for (_, sellable, best_bid) in &loser_sell_plans {
            let sell_size = (*sellable).min(mint_amount);
            total_sell_revenue += *best_bid * sell_size;
        }
    }

    // ── 4. Require at least some opportunity ────────────────────────────
    if buy_shares == Decimal::ZERO && mint_amount == Decimal::ZERO {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "No opportunity — no asks below limit and no loser bids"})),
        )
            .into_response();
    }

    // ── 5. Execute strategy ─────────────────────────────────────────────
    let mut status_parts: Vec<String> = Vec::new();
    let mut strategy = "BUY".to_string();
    let mut actual_mint_cost = Decimal::ZERO;
    let mut actual_sell_revenue = Decimal::ZERO;
    let mut total_gas_matic = Decimal::ZERO;

    // The outer `state.api` gate stays — we still need ClobApiClient for the
    // balance check earlier. Order placement uses `state.order_client` instead.
    if state.api.is_some() {
        // ── 5a. Mint+sell if viable ─────────────────────────────────────
        if use_mint_sell && mint_amount > Decimal::ZERO {
            strategy = "MINT+SELL".to_string();
            actual_mint_cost = mint_amount; // 1 USDC per complete set

            // On-chain CTF split
            let ctf = state.ctf.as_ref().unwrap();
            // Convert Decimal to USDC base units (6 decimals)
            let usdc_units = usdc_units_from_decimal(mint_amount);

            if usdc_units == 0 {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "Mint amount too small"})),
                )
                    .into_response();
            }

            let condition_id = match B256::from_str(&market.condition_id) {
                Ok(c) => c,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"error": format!("Invalid condition_id: {e}")})),
                    )
                        .into_response();
                }
            };

            // Geoblock guard: refuse to mint if the CLOB has previously
            // returned a region-block 403. Otherwise we'd strand tokens again.
            if state.geoblocked.load(Ordering::Relaxed) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "error": "Geoblocked: CLOB API returned 403 earlier in this session. Mint is disabled to avoid stranding tokens. Enable a VPN, then POST /api/clear-geoblock to re-enable."
                    })),
                )
                    .into_response();
            }

            // Gas-spike + profitability guard: read current gas price, estimate
            // total gas cost in USDC, refuse the trade if either the spot gas
            // price exceeds `max_gas_gwei` or the projected net profit
            // (sell_revenue - estimated_gas_cost_usdc) is below `min_profit_usdc`.
            //
            // splitPosition() empirically uses ~146-153k gas units; round up to 160k for safety.
            const ESTIMATED_MINT_GAS_UNITS: u128 = 160_000;
            match ctf.inner.current_gas_price_wei().await {
                Ok(gas_wei) => {
                    let gas_gwei = Decimal::from(gas_wei) / Decimal::from(1_000_000_000u64);
                    if gas_gwei > state.max_gas_gwei {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({
                                "error": format!(
                                    "Gas spike: current Polygon gas is {} Gwei, above the configured max of {} Gwei. \
                                     Wait for gas to come down, or raise max_gas_gwei in surebet.toml.",
                                    gas_gwei.round_dp(0), state.max_gas_gwei
                                )
                            })),
                        ).into_response();
                    }

                    let est_gas_wei =
                        Decimal::from(ESTIMATED_MINT_GAS_UNITS) * Decimal::from(gas_wei);
                    let est_gas_matic =
                        est_gas_wei / Decimal::from(1_000_000_000_000_000_000u128);
                    let est_gas_usdc = (est_gas_matic * state.matic_usd_price).round_dp(6);
                    let projected_net = total_sell_revenue - est_gas_usdc;
                    if projected_net < state.min_profit_usdc {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "error": format!(
                                    "Trade unprofitable: expected sell revenue ${} - estimated gas ${} = projected net ${} which is below the minimum ${}. \
                                     Either trade size is too small for the loser bid prices, or gas is too high right now.",
                                    total_sell_revenue.round_dp(4),
                                    est_gas_usdc.round_dp(4),
                                    projected_net.round_dp(4),
                                    state.min_profit_usdc
                                ),
                                "expected_sell_revenue": total_sell_revenue.to_string(),
                                "estimated_gas_usdc": est_gas_usdc.to_string(),
                                "projected_net": projected_net.to_string(),
                                "gas_gwei": gas_gwei.round_dp(2).to_string(),
                            })),
                        ).into_response();
                    }
                }
                Err(e) => {
                    // Don't block the trade if the RPC call fails — log it but
                    // proceed. Worst case is we mint at unknown gas price.
                    status_parts.push(format!("WARN: gas price check failed: {e}"));
                }
            }

            match ctf
                .inner
                .split_position(condition_id, U256::from(usdc_units))
                .await
            {
                Ok(tx) => {
                    total_gas_matic += tx.gas_cost_matic;
                    status_parts.push(format!(
                        "MINT OK tx={} gas={} MATIC",
                        tx.tx_hash, tx.gas_cost_matic
                    ));

                    // Sell each loser outcome's tokens via limit orders.
                    // Orders are built + EIP-712 signed through the SDK-backed
                    // `order_client` — the bare HMAC `api.place_order` path
                    // was rejected by Polymarket with "Invalid order payload".
                    if let Some(order_client) = state.order_client.as_ref() {
                        for (loser_token_id, sellable_shares, best_bid) in &loser_sell_plans {
                            // Sell up to mint_amount shares (not more than we minted)
                            let sell_size = (*sellable_shares).min(mint_amount);
                            let resp = order_client
                                .place_limit(
                                    loser_token_id,
                                    *best_bid,
                                    sell_size,
                                    OrderSide::Sell,
                                )
                                .await;
                            match resp {
                                Ok(r) if r.success => {
                                    actual_sell_revenue += *best_bid * sell_size;
                                    status_parts
                                        .push(format!("SELL OK id={}", r.order_id));
                                }
                                Ok(r) => {
                                    status_parts
                                        .push(format!("SELL FAIL: {}", r.error_msg));
                                }
                                Err(e) => {
                                    let msg = format!("{e}");
                                    if msg.contains("403")
                                        || msg.contains("restricted in your region")
                                    {
                                        state.geoblocked.store(true, Ordering::Relaxed);
                                    }
                                    status_parts.push(format!("SELL ERR: {}", msg));
                                }
                            }
                        }
                    } else {
                        status_parts
                            .push("SELL SKIP: order_client not initialized".to_string());
                    }
                }
                Err(e) => {
                    status_parts.push(format!("MINT FAIL: {e}"));
                    // Fall through — still try direct buy below
                }
            }
        }

        // ── 5b. Direct buy of winner tokens from asks ───────────────────
        // Goes through the SDK-backed order_client (EIP-712 signed). Falls
        // back to "BUY SKIP" if no order_client is available.
        if buy_shares > Decimal::ZERO {
            let Some(order_client) = state.order_client.as_ref() else {
                status_parts.push("BUY SKIP: order_client not initialized".to_string());
                // Record the partial result and return
                // (mint may still have happened)
                let entry = ActivityEntry {
                    time: now,
                    market: req.market_question,
                    outcome: req.label.clone(),
                    strategy,
                    buy_shares: buy_shares.to_string(),
                    buy_cost: format!("{:.4}", buy_cost),
                    mint_cost: format!("{:.4}", actual_mint_cost),
                    sell_revenue: format!("{:.4}", actual_sell_revenue),
                    net_profit: "0.0000".to_string(),
                    status: status_parts.join(" | "),
                    condition_id: market.condition_id.clone(),
                    gas_matic: format!("{}", total_gas_matic),
                    date: Utc::now().format("%Y-%m-%d").to_string(),
                };
                persist_and_record(&state, entry.clone()).await;
                return Json(serde_json::json!({"ok": false, "entry": entry})).into_response();
            };

            let resp = order_client
                .place_limit(
                    winner_token_id,
                    state.max_buy,
                    buy_shares,
                    OrderSide::Buy,
                )
                .await;

            match resp {
                Ok(r) if r.success => {
                    if strategy == "BUY" {
                        strategy = "BUY".to_string();
                    } else {
                        strategy = "MINT+SELL+BUY".to_string();
                    }
                    status_parts.push(format!("BUY OK id={}", r.order_id));

                    // Start hoover in background for additional shares
                    let book_store = state.book_store.clone();
                    let token_id = winner_token_id.clone();
                    let label = req.label.clone();
                    let max_buy = state.max_buy;
                    let order_client_clone = order_client.clone();
                    let activity = state.activity.clone();
                    tokio::spawn(async move {
                        hoover_task(
                            &book_store,
                            &token_id,
                            &label,
                            max_buy,
                            &order_client_clone,
                            &activity,
                        )
                        .await;
                    });
                }
                Ok(r) => status_parts.push(format!("BUY FAIL: {}", r.error_msg)),
                Err(e) => {
                    let msg = format!("{e}");
                    if msg.contains("403") || msg.contains("restricted in your region") {
                        state.geoblocked.store(true, Ordering::Relaxed);
                    }
                    status_parts.push(format!("BUY ERR: {}", msg));
                }
            }
        }
    } else {
        strategy = if use_mint_sell && mint_amount > Decimal::ZERO {
            actual_mint_cost = mint_amount;
            actual_sell_revenue = total_sell_revenue;
            "PAPER MINT+SELL".to_string()
        } else {
            "PAPER BUY".to_string()
        };
        status_parts.push("PAPER — no orders placed".to_string());
    }

    // ── 6. Compute net profit and log ───────────────────────────────────
    // net_profit = buy_profit + (sell_revenue - mint_cost + mint_amount_as_winner_value)
    // Since minted winners are worth $1 each at resolution:
    //   mint+sell profit = sell_revenue (losers) + mint_amount (winner value) - mint_amount (cost) = sell_revenue
    // Plus direct buy profit from the ask sweep:
    let net_profit = buy_profit + actual_sell_revenue;

    let entry = ActivityEntry {
        time: now,
        market: req.market_question,
        outcome: req.label.clone(),
        strategy,
        buy_shares: buy_shares.to_string(),
        buy_cost: format!("{:.4}", buy_cost),
        mint_cost: format!("{:.4}", actual_mint_cost),
        sell_revenue: format!("{:.4}", actual_sell_revenue),
        net_profit: format!("{:.4}", net_profit),
        status: status_parts.join(" | "),
        condition_id: market.condition_id.clone(),
        gas_matic: format!("{}", total_gas_matic),
        date: Utc::now().format("%Y-%m-%d").to_string(),
    };

    // Record spend in daily tracker
    let total_spent = buy_cost + actual_mint_cost;
    if total_spent > Decimal::ZERO {
        let mut spend_log = state.daily_spend.write().await;
        spend_log.push((Utc::now(), total_spent));
    }

    // Persist to trade log file (append, one JSON line per trade)
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("harvester_trades.jsonl")
        .await
    {
        use tokio::io::AsyncWriteExt;
        let line = serde_json::to_string(&entry).unwrap_or_default();
        let _ = f.write_all(format!("{}\n", line).as_bytes()).await;
    }

    let mut activity = state.activity.write().await;
    activity.push(entry.clone());

    Json(serde_json::json!({"ok": true, "entry": entry})).into_response()
}

// ─── WebSocket book streaming ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct WsBookQuery {
    idx: usize,
}

async fn ws_book_handler(
    State(state): State<AppState>,
    Query(q): Query<WsBookQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_book_stream(socket, state, q.idx))
}

async fn ws_book_stream(socket: WebSocket, state: AppState, idx: usize) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Look up the market
    let market = {
        let markets = state.markets.read().await;
        markets.get(idx).cloned()
    };
    let market = match market {
        Some(m) => m,
        None => {
            let _ = ws_tx
                .send(Message::Text(
                    serde_json::json!({"error": "Invalid market index"}).to_string().into(),
                ))
                .await;
            return;
        }
    };

    let token_ids = market.clob_token_ids.clone();

    // Send initial snapshot via REST fetch
    let initial = build_book_payload(idx, &market, &state).await;
    if ws_tx
        .send(Message::Text(initial.into()))
        .await
        .is_err()
    {
        return;
    }

    // Start CLOB WS subscription for this market's tokens
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ClobEvent>();
    if let Err(e) = start_clob_ws(
        (*state.book_store).clone(),
        event_tx,
        token_ids.clone(),
    ) {
        let _ = ws_tx
            .send(Message::Text(
                serde_json::json!({"error": format!("WS start failed: {}", e)})
                    .to_string()
                    .into(),
            ))
            .await;
        return;
    }

    // Forward CLOB book updates → browser WS
    loop {
        tokio::select! {
            // CLOB event received — rebuild outcome info and send
            event = event_rx.recv() => {
                match event {
                    Some(ClobEvent::BookSnapshot { asset_id, .. }) => {
                        // Only send if this asset belongs to our market
                        if !token_ids.contains(&asset_id) {
                            continue;
                        }
                        let payload = build_book_payload_from_store(idx, &market, &state);
                        if ws_tx.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(ClobEvent::Disconnected) | None => break,
                    _ => {}
                }
            }
            // Browser closed the WS
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

/// Build book payload using REST fetch (for initial load).
async fn build_book_payload(
    idx: usize,
    market: &HarvestableMarket,
    state: &AppState,
) -> String {
    let mut outcomes: Vec<OutcomeInfo> = Vec::new();
    for (i, token_id) in market.clob_token_ids.iter().enumerate() {
        let label = market
            .outcomes
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("Outcome {}", i));
        let book = state
            .book_store
            .fetch_rest_book(&state.clob_url, token_id)
            .await;
        let info = match book {
            Some(ref b) => build_outcome_info(&label, token_id, b, state.max_buy, state.min_sell),
            None => OutcomeInfo {
                label: label.clone(),
                token_id: token_id.clone(),
                best_ask: None,
                sweepable_shares: Decimal::ZERO,
                sweepable_cost: Decimal::ZERO,
                sweepable_profit: Decimal::ZERO,
                best_bid: None,
                sellable_shares: Decimal::ZERO,
                sellable_revenue: Decimal::ZERO,
            },
        };
        outcomes.push(info);
    }
    serde_json::json!({
        "idx": idx,
        "question": market.question,
        "outcomes": outcomes,
    })
    .to_string()
}

/// Build book payload from the in-memory store (after WS updates).
fn build_book_payload_from_store(
    idx: usize,
    market: &HarvestableMarket,
    state: &AppState,
) -> String {
    let mut outcomes: Vec<OutcomeInfo> = Vec::new();
    for (i, token_id) in market.clob_token_ids.iter().enumerate() {
        let label = market
            .outcomes
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("Outcome {}", i));
        let book = state.book_store.get_book(token_id);
        let info = match book {
            Some(ref b) => build_outcome_info(&label, token_id, b, state.max_buy, state.min_sell),
            None => OutcomeInfo {
                label: label.clone(),
                token_id: token_id.clone(),
                best_ask: None,
                sweepable_shares: Decimal::ZERO,
                sweepable_cost: Decimal::ZERO,
                sweepable_profit: Decimal::ZERO,
                best_bid: None,
                sellable_shares: Decimal::ZERO,
                sellable_revenue: Decimal::ZERO,
            },
        };
        outcomes.push(info);
    }
    serde_json::json!({
        "idx": idx,
        "question": market.question,
        "outcomes": outcomes,
    })
    .to_string()
}

// ─── Hoover background task ─────────────────────────────────────────────────

async fn hoover_task(
    book_store: &Arc<OrderBookStore>,
    token_id: &str,
    label: &str,
    max_buy: Decimal,
    order_client: &OrderClient,
    activity: &Arc<RwLock<Vec<ActivityEntry>>>,
) {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ClobEvent>();
    let min_order = Decimal::from(5);

    if let Err(e) = start_clob_ws(
        (**book_store).clone(),
        event_tx,
        vec![token_id.to_string()],
    ) {
        info!("Hoover WS start failed for {}: {}", label, e);
        return;
    }

    let mut last_ordered_depth = Decimal::ZERO;

    loop {
        match event_rx.recv().await {
            Some(ClobEvent::BookSnapshot { asset_id, .. }) => {
                if asset_id != token_id {
                    continue;
                }

                let book = match book_store.get_book(&asset_id) {
                    Some(b) => b,
                    None => continue,
                };

                let mut sweepable_shares = Decimal::ZERO;
                let mut sweepable_cost = Decimal::ZERO;
                for (&price, &size) in book.asks.levels.iter() {
                    if price > max_buy {
                        break;
                    }
                    sweepable_shares += size;
                    sweepable_cost += price * size;
                }

                if sweepable_shares <= Decimal::ZERO || sweepable_cost < min_order {
                    if sweepable_shares < last_ordered_depth {
                        last_ordered_depth = sweepable_shares;
                    }
                    continue;
                }

                if sweepable_shares <= last_ordered_depth {
                    continue;
                }

                let profit = sweepable_shares - sweepable_cost;

                let resp = order_client
                    .place_limit(token_id, max_buy, sweepable_shares, OrderSide::Buy)
                    .await;

                let status = match resp {
                    Ok(r) if r.success => {
                        last_ordered_depth = sweepable_shares;
                        format!("HOOVER OK — id: {}", r.order_id)
                    }
                    Ok(r) => format!("HOOVER FAIL — {}", r.error_msg),
                    Err(e) => format!("HOOVER ERR — {}", e),
                };

                let entry = ActivityEntry {
                    time: Utc::now().format("%H:%M:%S").to_string(),
                    market: String::new(),
                    outcome: label.to_string(),
                    strategy: "HOOVER".to_string(),
                    buy_shares: sweepable_shares.to_string(),
                    buy_cost: format!("{:.4}", sweepable_cost),
                    mint_cost: "0".to_string(),
                    sell_revenue: "0".to_string(),
                    net_profit: format!("{:.4}", profit),
                    status,
                    condition_id: String::new(),
                    gas_matic: "0".to_string(),
                    date: Utc::now().format("%Y-%m-%d").to_string(),
                };

                // Persist hoover trade to file
                if let Ok(mut f) = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("harvester_trades.jsonl")
                    .await
                {
                    use tokio::io::AsyncWriteExt;
                    let line = serde_json::to_string(&entry).unwrap_or_default();
                    let _ = f.write_all(format!("{}\n", line).as_bytes()).await;
                }

                let mut log = activity.write().await;
                log.push(entry);
            }
            Some(ClobEvent::Disconnected) | None => break,
            _ => {}
        }
    }
}

// ─── HTML Dashboard ──────────────────────────────────────────────────────────

// ─── Trades + Positions page ────────────────────────────────────────────────

async fn trades_html(State(state): State<AppState>) -> Html<String> {
    // Snapshot activity log
    let activity_snap: Vec<ActivityEntry> = {
        let activity = state.activity.read().await;
        activity.clone()
    };
    let stats = compute_stats(&activity_snap, state.matic_usd_price);
    let positions = compute_open_positions(&state).await;

    // Stats panel
    let stats_html = format!(
        r##"<div class="stats-grid">
          <div class="stat-card"><div class="slabel">Total Trades</div><div class="sval">{total}</div></div>
          <div class="stat-card"><div class="slabel">Successful</div><div class="sval" style="color:#2ecc71">{success}</div></div>
          <div class="stat-card"><div class="slabel">Failed</div><div class="sval" style="color:#e74c3c">{failed}</div></div>
          <div class="stat-card"><div class="slabel">Stuck</div><div class="sval" style="color:#f39c12">{stuck}</div></div>
          <div class="stat-card"><div class="slabel">Open Positions</div><div class="sval" style="color:#58a6ff">{positions}</div></div>
          <div class="stat-card"><div class="slabel">Total Mint</div><div class="sval">${mint:.4}</div></div>
          <div class="stat-card"><div class="slabel">Total Buy</div><div class="sval">${buy:.4}</div></div>
          <div class="stat-card"><div class="slabel">Total Sell Rev</div><div class="sval">${sell:.4}</div></div>
          <div class="stat-card"><div class="slabel">Realized P&amp;L</div><div class="sval" style="color:{pnl_color}">${pnl:.4}</div></div>
          <div class="stat-card"><div class="slabel">Gas (MATIC)</div><div class="sval" style="color:#f39c12">{gas}</div></div>
          <div class="stat-card"><div class="slabel">Gas (USDC)</div><div class="sval" style="color:#f39c12">${gas_usdc:.4}</div></div>
          <div class="stat-card"><div class="slabel">Net P&amp;L (after gas)</div><div class="sval" style="color:{net_color}">${net:.4}</div></div>
        </div>"##,
        total = stats.total_entries,
        success = stats.successful_trades,
        failed = stats.failed_trades,
        stuck = stats.stuck_trades,
        mint = stats.total_mint_cost,
        buy = stats.total_buy_cost,
        sell = stats.total_sell_revenue,
        pnl = stats.realized_pnl,
        pnl_color = if stats.realized_pnl > Decimal::ZERO { "#2ecc71" } else { "#8b949e" },
        gas = stats.total_gas_matic,
        gas_usdc = stats.total_gas_usdc,
        net = stats.net_pnl_after_gas,
        net_color = if stats.net_pnl_after_gas > Decimal::ZERO { "#2ecc71" } else { "#e74c3c" },
        positions = positions.len(),
    );

    // Positions table
    let positions_rows: String = if positions.is_empty() {
        r#"<tr><td colspan="7" style="text-align:center;color:#666">No open positions on-chain</td></tr>"#.to_string()
    } else {
        positions.iter().map(|p| {
            // Merge button: only when both sides held
            let merge_btn = if p.mergeable {
                let amt = p.yes_tokens.min(p.no_tokens);
                format!(
                    r#"<button class="action-btn merge-btn" onclick="recoverPosition('{}','{}','{}','merge')">Merge</button>"#,
                    p.condition_id, amt, p.market.replace('\'', "\\'")
                )
            } else {
                "—".to_string()
            };

            // Redeem button: only enabled when CTF says the market is resolved
            let redeem_btn = if p.redeemable {
                format!(
                    r#"<button class="action-btn redeem-btn" onclick="recoverPosition('{}','','{}','redeem')">Redeem</button>"#,
                    p.condition_id, p.market.replace('\'', "\\'")
                )
            } else {
                r#"<button class="action-btn" disabled title="Market not resolved yet" style="opacity:0.4;cursor:not-allowed">Redeem</button>"#.to_string()
            };

            // Resolution status badge
            let (res_label, res_color) = match p.resolution.as_str() {
                "unresolved" => ("unresolved", "#8b949e"),
                "resolved-yes" => ("RESOLVED → YES", "#2ecc71"),
                "resolved-no" => ("RESOLVED → NO", "#e74c3c"),
                _ => (p.resolution.as_str(), "#f39c12"),
            };

            format!(
                r#"<tr>
                  <td class="pos-market-cell" title="{cid}">{market}</td>
                  <td>{winner}</td>
                  <td>{yes}</td>
                  <td>{no}</td>
                  <td><span style="color:{rcol}">{rlabel}</span></td>
                  <td>{tl}</td>
                  <td>{merge} {redeem}</td>
                </tr>"#,
                cid = p.condition_id,
                market = p.market,
                winner = p.winner_label,
                yes = p.yes_tokens,
                no = p.no_tokens,
                rcol = res_color,
                rlabel = res_label,
                tl = p.time_left,
                merge = merge_btn,
                redeem = redeem_btn,
            )
        }).collect()
    };

    // Activity log table
    let activity_rows: String = if activity_snap.is_empty() {
        r#"<tr><td colspan="11" style="text-align:center;color:#666">No activity yet</td></tr>"#.to_string()
    } else {
        activity_snap.iter().rev().take(100).map(|a| {
            let status_color = if a.status.contains("OK") {
                "#2ecc71"
            } else if a.status.contains("PAPER") {
                "#f39c12"
            } else {
                "#e74c3c"
            };
            let profit_val: f64 = a.net_profit.parse().unwrap_or(0.0);
            let profit_color = if profit_val > 0.0 { "#2ecc71" } else { "#8b949e" };
            // Recovery buttons for stuck mint trades
            let is_stuck = a.status.contains("MINT OK")
                && (a.status.contains("SELL ERR") || a.status.contains("SELL FAIL"))
                && !a.status.contains("RECOVERED");
            let actions = if is_stuck && !a.condition_id.is_empty() {
                let mint_amount = if a.mint_cost.is_empty() { "0" } else { &a.mint_cost };
                format!(
                    r#"<button class="action-btn merge-btn" onclick="recoverPosition('{}','{}','{}','merge')">Merge</button>"#,
                    a.condition_id, mint_amount, a.market.replace('\'', "\\'")
                )
            } else {
                "—".to_string()
            };

            // Strategy-aware Shares display:
            //   BUY/HOOVER → buy_shares
            //   MINT+SELL  → derived from mint_cost ($1 per minted token = 1 share each side)
            //   MERGE/REDEEM → "—" (no shares involved at the order level)
            let shares_display = if a.strategy.contains("MINT") {
                let mc = Decimal::from_str(&a.mint_cost).unwrap_or(Decimal::ZERO);
                if mc > Decimal::ZERO {
                    format!("{} (minted)", mc)
                } else {
                    "0".to_string()
                }
            } else if a.strategy == "MERGE" || a.strategy == "REDEEM" {
                "—".to_string()
            } else {
                a.buy_shares.clone()
            };

            // Cost cell — buy_cost for buys, mint_cost for mints, dash for recovery
            let cost_display = if a.strategy.contains("MINT") {
                a.mint_cost.clone()
            } else if a.strategy == "MERGE" || a.strategy == "REDEEM" {
                "—".to_string()
            } else {
                a.buy_cost.clone()
            };

            // Gas cell: blank dash if zero, otherwise the value (helps distinguish
            // legacy entries that pre-date gas tracking from new entries)
            let gas_display = {
                let g = Decimal::from_str(&a.gas_matic).unwrap_or(Decimal::ZERO);
                if g == Decimal::ZERO {
                    "—".to_string()
                } else {
                    format!("{} MATIC", g)
                }
            };

            format!(
                r#"<tr><td>{date} {time}</td><td class="market-cell" title="{market_full}">{market}</td><td>{outcome}</td><td>{strategy}</td><td>{shares}</td><td>{cost}</td><td>{sm}</td><td style="color:{pcol}">{profit}</td><td>{gas}</td><td style="color:{scol};max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{stitle}">{status}</td><td>{actions}</td></tr>"#,
                date = a.date,
                time = a.time,
                market_full = a.market.replace('"', "&quot;"),
                market = a.market,
                outcome = a.outcome,
                strategy = a.strategy,
                shares = shares_display,
                cost = cost_display,
                sm = if a.sell_revenue != "0" && a.sell_revenue != "0.0000" {
                    format!("${} sell / ${} mint", a.sell_revenue, a.mint_cost)
                } else {
                    "—".to_string()
                },
                pcol = profit_color,
                profit = a.net_profit,
                gas = gas_display,
                scol = status_color,
                stitle = a.status.replace('"', "&quot;"),
                status = a.status,
                actions = actions,
            )
        }).collect()
    };

    let geoblocked = state.geoblocked.load(Ordering::Relaxed);
    let geoblock_banner = if geoblocked {
        r#"<div class="geo-banner">⚠ CLOB API geoblock detected — minting is disabled. Enable a VPN, then click <button class="action-btn" style="background:#2ecc71" onclick="clearGeoblock()">Clear after VPN</button></div>"#.to_string()
    } else {
        String::new()
    };

    let html = format!(
        r##"<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Trades & Positions — Harvester</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; max-width: 1800px; margin: 0 auto; }}
  .market-cell {{ max-width: 360px; white-space: normal; word-break: break-word; }}
  .pos-market-cell {{ max-width: 380px; white-space: normal; word-break: break-word; }}
  h1 {{ color: #58a6ff; margin-bottom: 5px; font-size: 1.4em; }}
  h2 {{ color: #8b949e; margin: 24px 0 10px 0; font-size: 1.05em; border-bottom: 1px solid #21262d; padding-bottom: 5px; }}
  .nav {{ display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 1px solid #21262d; padding-bottom: 8px; }}
  .nav a {{ color: #8b949e; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 0.9em; }}
  .nav a.active {{ background: #1f6feb; color: #fff; }}
  .nav a:hover:not(.active) {{ background: #21262d; }}
  .stats-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; margin-bottom: 20px; }}
  .stat-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px; }}
  .slabel {{ color: #8b949e; font-size: 0.75em; text-transform: uppercase; }}
  .sval {{ color: #c9d1d9; font-size: 1.15em; font-weight: bold; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.82em; }}
  th, td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #21262d; }}
  th {{ color: #8b949e; font-weight: normal; text-transform: uppercase; font-size: 0.72em; }}
  tr:hover {{ background: #161b22; }}
  .action-btn {{ background: #30363d; color: #c9d1d9; border: 1px solid #484f58; border-radius: 4px; padding: 3px 9px; font-family: inherit; font-size: 0.75em; cursor: pointer; }}
  .action-btn:hover {{ background: #484f58; }}
  .merge-btn {{ background: #1f6feb; border-color: #388bfd; color: #fff; }}
  .merge-btn:hover {{ background: #388bfd; }}
  .redeem-btn {{ background: #6e40c9; border-color: #8957e5; color: #fff; }}
  .redeem-btn:hover {{ background: #8957e5; }}
  .geo-banner {{ background: #4a1818; border: 1px solid #e74c3c; border-radius: 6px; padding: 12px 16px; margin: 12px 0; color: #f5b7b1; font-size: 0.9em; }}
  .recover-panel {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 16px; }}
  .search-box {{ width: 100%; padding: 7px 11px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-family: inherit; font-size: 0.85em; outline: none; }}
  .search-box:focus {{ border-color: #58a6ff; }}
</style>
</head><body>
<h1>Trades &amp; Positions</h1>
<div class="nav">
  <a href="/">Markets</a>
  <a href="/trades" class="active">Trades</a>
</div>

{geoblock_banner}

<h2>Stats</h2>
{stats_html}

<h2>Open Positions <span style="color:#666;font-size:0.8em">(on-chain)</span></h2>
<table>
  <tr><th>Market</th><th>Winner</th><th>YES</th><th>NO</th><th>Resolution</th><th>Time Left</th><th>Actions</th></tr>
  {positions_rows}
</table>

<h2>Manual Recovery</h2>
<div class="recover-panel">
  <input type="text" id="recover-cid" placeholder="0xCondition... (paste from a stuck trade or any market)" class="search-box" style="margin-bottom:8px">
  <div style="display:flex;gap:8px;align-items:center">
    <input type="text" id="recover-amount" placeholder="USDC amount (merge only)" class="search-box" style="flex:1">
    <button class="action-btn merge-btn" onclick="manualRecover('merge')">Merge</button>
    <button class="action-btn redeem-btn" onclick="manualRecover('redeem')">Redeem</button>
  </div>
  <div id="recover-result" style="margin-top:8px;font-size:0.85em;color:#8b949e"></div>
</div>

<h2>Activity Log</h2>
<table>
  <tr><th>When</th><th>Market</th><th>Outcome</th><th>Strategy</th><th>Shares</th><th>Buy Cost</th><th>Sell/Mint</th><th>Net Profit</th><th>Gas</th><th>Status</th><th>Actions</th></tr>
  {activity_rows}
</table>

<script>
async function recoverPosition(conditionId, amount, market, action) {{
  const verb = action === 'merge' ? 'merge' : 'redeem';
  const desc = action === 'merge'
    ? `Merge $${{amount}} (burn YES+NO pair → recover USDC.e)?`
    : `Redeem winning tokens for "${{market}}"? Only works AFTER market resolution.`;
  if (!confirm(desc)) return;
  const body = {{ condition_id: conditionId, market: market }};
  if (action === 'merge') body.amount = amount;
  try {{
    const resp = await fetch('/api/' + action, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(body),
    }});
    const data = await resp.json();
    if (data.ok) {{
      alert(verb.toUpperCase() + ' OK\n\n' + (data.entry ? data.entry.status : ''));
      location.reload();
    }} else {{
      alert(verb.toUpperCase() + ' failed: ' + (data.error || (data.entry ? data.entry.status : 'unknown')));
      if (data.entry) location.reload();
    }}
  }} catch (e) {{
    alert('Request failed: ' + e);
  }}
}}

async function manualRecover(action) {{
  const cid = document.getElementById('recover-cid').value.trim();
  const amount = document.getElementById('recover-amount').value.trim();
  const result = document.getElementById('recover-result');
  if (!cid) {{ result.textContent = 'condition_id is required'; return; }}
  if (action === 'merge' && !amount) {{ result.textContent = 'amount is required for merge'; return; }}
  result.textContent = 'submitting ' + action + '...';
  await recoverPosition(cid, amount, '', action);
}}

async function clearGeoblock() {{
  if (!confirm('Clear the geoblock flag? Only do this AFTER you have enabled a VPN and verified you are routing through a permitted region.')) return;
  await fetch('/api/clear-geoblock', {{ method: 'POST' }});
  location.reload();
}}
</script>
</body></html>"##,
        geoblock_banner = geoblock_banner,
        stats_html = stats_html,
        positions_rows = positions_rows,
        activity_rows = activity_rows,
    );

    Html(html)
}

async fn dashboard_html(State(state): State<AppState>) -> Html<String> {
    let markets = state.markets.read().await;
    let activity = state.activity.read().await;
    let now = Utc::now();

    // Compute 24h spend for display
    let spent_today_str = {
        let cutoff = now - chrono::Duration::hours(24);
        let spend_log = state.daily_spend.read().await;
        let total: Decimal = spend_log
            .iter()
            .filter(|(ts, _)| *ts > cutoff)
            .map(|(_, amt)| amt)
            .sum();
        format!("{:.2}", total)
    };

    // Build market rows — collapsed by default, no book data yet
    let mut market_rows = String::new();
    for (mi, m) in markets.iter().enumerate() {
        let end_str = match m.end_date {
            Some(ed) => {
                let tl = format_time_left(ed);
                let color = if tl.starts_with("ended") {
                    "#e74c3c"
                } else if tl.contains('d') {
                    "#8b949e"
                } else {
                    "#f39c12"
                };
                format!(
                    "<span style=\"color:{}\">{}</span> <span style=\"color:#484f58\">({})</span>",
                    color,
                    tl,
                    ed.format("%Y-%m-%d %H:%M UTC")
                )
            }
            None => "???".to_string(),
        };

        let q_display = if m.question.len() > 90 {
            format!("{}...", &m.question[..87])
        } else {
            m.question.clone()
        };

        // Escape question for JS data attribute
        let q_escaped = m.question.replace('"', "&quot;");

        market_rows.push_str(&format!(
            r#"<div class="market-card" data-question="{q_escaped}" data-idx="{idx}">
                <div class="market-header" onclick="toggleMarket({idx})">
                    <span class="market-num">#{num}</span>
                    <span class="market-question">{q_display}</span>
                    <span class="market-meta">{end_str} | {n_out} outcomes | {cat}{neg}</span>
                    <span class="expand-icon" id="icon-{idx}">▶</span>
                </div>
                <div class="outcomes" id="outcomes-{idx}" style="display:none">
                    <div class="loading" id="loading-{idx}">Loading order book...</div>
                </div>
            </div>"#,
            q_escaped = q_escaped,
            idx = mi,
            num = mi + 1,
            q_display = q_display,
            end_str = end_str,
            n_out = m.outcomes.len(),
            cat = if m.category.len() > 15 { &m.category[..15] } else { &m.category },
            neg = if m.is_neg_risk { " | neg_risk" } else { "" },
        ));
    }

    // Activity log moved to /trades — only the geoblock banner is rendered here.
    let geoblocked = state.geoblocked.load(Ordering::Relaxed);
    let geoblock_banner = if geoblocked {
        r#"<div class="geo-banner">⚠ CLOB API geoblock detected — minting is disabled. Enable a VPN, then click <button class="action-btn" style="background:#2ecc71" onclick="clearGeoblock()">Clear after VPN</button></div>"#.to_string()
    } else {
        String::new()
    };

    let html = format!(
        r##"<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Harvester Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
  h1 {{ color: #58a6ff; margin-bottom: 5px; font-size: 1.4em; }}
  .subtitle {{ color: #8b949e; margin-bottom: 20px; font-size: 0.85em; }}
  h2 {{ color: #8b949e; margin: 20px 0 10px 0; font-size: 1.1em; border-bottom: 1px solid #21262d; padding-bottom: 5px; }}
  .search-box {{ width: 100%; padding: 8px 12px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-family: inherit; font-size: 0.9em; margin-bottom: 12px; outline: none; }}
  .search-box:focus {{ border-color: #58a6ff; }}
  .geo-banner {{ background: #4a1818; border: 1px solid #e74c3c; border-radius: 6px; padding: 12px 16px; margin: 12px 0; color: #f5b7b1; font-size: 0.9em; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
  .recover-panel {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 16px; margin-bottom: 12px; }}
  .nav {{ display: flex; gap: 8px; margin-bottom: 12px; }}
  .nav a {{ color: #8b949e; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 0.9em; border: 1px solid #21262d; }}
  .nav a.active {{ background: #1f6feb; color: #fff; border-color: #388bfd; }}
  .nav a:hover:not(.active) {{ background: #21262d; }}
  .action-btn {{ background: #30363d; color: #c9d1d9; border: 1px solid #484f58; border-radius: 4px; padding: 4px 10px; font-family: inherit; font-size: 0.8em; cursor: pointer; }}
  .action-btn:hover {{ background: #484f58; }}
  .merge-btn {{ background: #1f6feb; border-color: #388bfd; color: #fff; }}
  .merge-btn:hover {{ background: #388bfd; }}
  .redeem-btn {{ background: #6e40c9; border-color: #8957e5; color: #fff; }}
  .redeem-btn:hover {{ background: #8957e5; }}
  .market-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 6px; overflow: hidden; }}
  .market-card.hidden {{ display: none; }}
  .market-header {{ display: flex; align-items: baseline; gap: 10px; padding: 10px 16px; cursor: pointer; flex-wrap: wrap; }}
  .market-header:hover {{ background: #1c2128; }}
  .market-num {{ color: #8b949e; font-size: 0.8em; font-weight: bold; }}
  .market-question {{ color: #58a6ff; font-weight: bold; font-size: 0.9em; }}
  .market-meta {{ color: #484f58; font-size: 0.72em; margin-left: auto; white-space: nowrap; }}
  .expand-icon {{ color: #484f58; font-size: 0.7em; transition: transform 0.15s; }}
  .expand-icon.open {{ transform: rotate(90deg); }}
  .outcomes {{ padding: 0 16px 12px 16px; }}
  .outcome-row {{ display: flex; align-items: center; gap: 12px; padding: 4px 8px; background: #0d1117; border-radius: 4px; font-size: 0.82em; flex-wrap: wrap; margin-bottom: 3px; }}
  .outcome-label {{ min-width: 130px; font-weight: bold; color: #c9d1d9; }}
  .outcome-ask {{ min-width: 75px; color: #8b949e; }}
  .outcome-bid {{ min-width: 75px; color: #8b949e; }}
  .outcome-shares {{ min-width: 140px; color: #8b949e; }}
  .outcome-cost {{ min-width: 90px; color: #8b949e; }}
  .outcome-profit {{ min-width: 140px; }}
  .harvest-btn {{ background: #238636; color: #fff; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.8em; font-family: inherit; margin-left: auto; }}
  .harvest-btn:hover {{ background: #2ea043; }}
  .loading {{ color: #484f58; font-size: 0.82em; padding: 8px; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }}
  th {{ background: #21262d; color: #8b949e; text-align: left; padding: 8px 12px; font-size: 0.8em; text-transform: uppercase; }}
  td {{ padding: 6px 12px; border-top: 1px solid #21262d; font-size: 0.82em; }}
  tr:hover {{ background: #1c2128; }}
  .status-bar {{ display: flex; gap: 15px; margin-bottom: 15px; flex-wrap: wrap; }}
  .status-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 8px 14px; }}
  .status-item .slabel {{ color: #8b949e; font-size: 0.7em; text-transform: uppercase; }}
  .status-item .sval {{ font-size: 1.2em; font-weight: bold; margin-top: 2px; }}
  .page-controls {{ display: flex; gap: 8px; align-items: center; margin-bottom: 12px; }}
  .page-btn {{ background: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 0.82em; }}
  .page-btn:hover {{ background: #30363d; }}
  .page-btn:disabled {{ opacity: 0.4; cursor: default; }}
  .page-info {{ color: #8b949e; font-size: 0.82em; }}

  /* ── Research side panel ─────────────────────────────────────── */
  .dashboard-wrap {{ display: flex; gap: 0; min-height: calc(100vh - 40px); }}
  .main-col {{ flex: 1; min-width: 0; }}
  .research-panel {{
    width: 0; overflow: hidden; transition: width 0.25s ease;
    background: #161b22; border-left: 2px solid #30363d;
    display: flex; flex-direction: column; position: sticky; top: 0;
    height: 100vh;
  }}
  .research-panel.open {{ width: 45vw; min-width: 400px; }}
  .research-bar {{
    display: flex; align-items: center; gap: 8px; padding: 8px 12px;
    background: #21262d; border-bottom: 1px solid #30363d; flex-shrink: 0;
  }}
  .research-bar input {{
    flex: 1; padding: 6px 10px; background: #0d1117; border: 1px solid #30363d;
    border-radius: 4px; color: #c9d1d9; font-family: inherit; font-size: 0.85em; outline: none;
  }}
  .research-bar input:focus {{ border-color: #58a6ff; }}
  .research-bar button {{
    background: #238636; color: #fff; border: none; padding: 6px 12px;
    border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 0.82em;
  }}
  .research-bar button:hover {{ background: #2ea043; }}
  .research-bar .close-btn {{
    background: #da3633; padding: 6px 10px; font-weight: bold;
  }}
  .research-bar .close-btn:hover {{ background: #f85149; }}
  .research-bar .engine-select {{
    background: #0d1117; color: #c9d1d9; border: 1px solid #30363d;
    border-radius: 4px; padding: 5px 6px; font-family: inherit; font-size: 0.82em;
  }}
  .research-iframe {{ flex: 1; border: none; background: #fff; }}
  .research-title {{
    color: #58a6ff; font-size: 0.78em; padding: 4px 12px;
    background: #161b22; border-bottom: 1px solid #21262d;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 0;
  }}
</style>
</head>
<body>
<div class="dashboard-wrap">
<div class="main-col">
<h1>Harvester Dashboard</h1>
<div class="nav">
  <a href="/" class="active">Markets</a>
  <a href="/trades">Trades</a>
</div>
<div class="subtitle">Mode: <b style="color:{mode_color}">{mode}</b> | Buy limit: ${max_buy} | Window: future only</div>

<div class="status-bar">
  <div class="status-item">
    <div class="slabel">Markets</div>
    <div class="sval" style="color:#58a6ff">{market_count}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Showing</div>
    <div class="sval" style="color:#58a6ff" id="showing-count">{market_count}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Trades</div>
    <div class="sval" style="color:#2ecc71">{trade_count}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Max Trade</div>
    <div class="sval" style="color:#f39c12">${max_trade}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Daily Limit</div>
    <div class="sval" style="color:#f39c12">${max_daily}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Spent (24h)</div>
    <div class="sval" style="color:#e74c3c">${spent_today}</div>
  </div>
</div>

{geoblock_banner}

<h2>Markets</h2>
<input type="text" class="search-box" id="search" placeholder="Search markets..." oninput="filterMarkets()">

<div class="page-controls">
  <button class="page-btn" id="prev-btn" onclick="changePage(-1)" disabled>← Prev</button>
  <span class="page-info" id="page-info">Page 1</span>
  <button class="page-btn" id="next-btn" onclick="changePage(1)">Next →</button>
  <span class="page-info" style="margin-left:8px">Per page:</span>
  <select class="page-btn" id="per-page" onchange="changePerPage()">
    <option value="25">25</option>
    <option value="50" selected>50</option>
    <option value="100">100</option>
    <option value="9999">All</option>
  </select>
</div>

<div id="market-list">
{market_rows}
</div>

<div style="margin-top:20px;padding:12px;background:#161b22;border:1px solid #30363d;border-radius:6px;font-size:0.85em;color:#8b949e">
  Trades, positions, and recovery actions are on the <a href="/trades" style="color:#58a6ff">Trades page</a>.
</div>
</div><!-- end main-col -->

<div class="research-panel" id="research-panel">
  <div class="research-bar">
    <select class="engine-select" id="search-engine" onchange="reSearch()">
      <option value="google">Google</option>
      <option value="ddg">DuckDuckGo</option>
      <option value="bing">Bing</option>
    </select>
    <input type="text" id="research-query" placeholder="Search..." onkeydown="if(event.key==='Enter')reSearch()">
    <button onclick="reSearch()">Search</button>
    <button onclick="openResearchExternal()" title="Open in new tab" style="background:#30363d">↗</button>
    <button class="close-btn" onclick="closeResearch()">✕</button>
  </div>
  <div class="research-title" id="research-title">No market selected</div>
  <iframe class="research-iframe" id="research-iframe" sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"></iframe>
</div>
</div><!-- end dashboard-wrap -->

<script>
// ── State ──────────────────────────────────────────────────────────────
let openIdx = null;         // which market is expanded
let cachedBooks = {{}};     // idx -> outcome data
let currentPage = 0;
let perPage = 50;

// ── Search & Pagination ────────────────────────────────────────────────
function getVisibleCards() {{
  const q = document.getElementById('search').value.toLowerCase();
  const cards = document.querySelectorAll('.market-card');
  const visible = [];
  cards.forEach(c => {{
    const question = (c.dataset.question || '').toLowerCase();
    if (!q || question.includes(q)) {{
      visible.push(c);
    }}
  }});
  return visible;
}}

function filterMarkets() {{
  currentPage = 0;
  renderPage();
}}

function changePage(delta) {{
  currentPage += delta;
  renderPage();
}}

function changePerPage() {{
  perPage = parseInt(document.getElementById('per-page').value);
  currentPage = 0;
  renderPage();
}}

function renderPage() {{
  const cards = document.querySelectorAll('.market-card');
  const q = document.getElementById('search').value.toLowerCase();

  // First hide all
  cards.forEach(c => c.classList.add('hidden'));

  // Get matching cards
  const visible = [];
  cards.forEach(c => {{
    const question = (c.dataset.question || '').toLowerCase();
    if (!q || question.includes(q)) {{
      visible.push(c);
    }}
  }});

  // Paginate
  const totalPages = Math.max(1, Math.ceil(visible.length / perPage));
  if (currentPage >= totalPages) currentPage = totalPages - 1;
  if (currentPage < 0) currentPage = 0;

  const start = currentPage * perPage;
  const end = Math.min(start + perPage, visible.length);

  for (let i = start; i < end; i++) {{
    visible[i].classList.remove('hidden');
  }}

  // Update controls
  document.getElementById('prev-btn').disabled = (currentPage === 0);
  document.getElementById('next-btn').disabled = (currentPage >= totalPages - 1);
  document.getElementById('page-info').textContent = `Page ${{currentPage + 1}} / ${{totalPages}}`;
  document.getElementById('showing-count').textContent = visible.length;
}}

// ── WebSocket book streaming ──────────────────────────────────────────
let bookWs = null;
let wsUpdateCount = 0;

function closeBookWs() {{
  if (bookWs) {{
    bookWs.close();
    bookWs = null;
  }}
  wsUpdateCount = 0;
}}

function openBookWs(idx) {{
  closeBookWs();
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${{proto}}//${{location.host}}/ws/book?idx=${{idx}}`;
  bookWs = new WebSocket(url);

  bookWs.onmessage = (evt) => {{
    try {{
      const data = JSON.parse(evt.data);
      if (data.error) {{
        const loadingEl = document.getElementById('loading-' + idx);
        if (loadingEl) loadingEl.textContent = 'Error: ' + data.error;
        return;
      }}
      wsUpdateCount++;
      cachedBooks[idx] = data;
      if (openIdx === idx) renderOutcomes(idx, data);
    }} catch (e) {{
      console.error('WS parse error:', e);
    }}
  }};

  bookWs.onclose = () => {{
    bookWs = null;
  }};

  bookWs.onerror = (err) => {{
    console.error('Book WS error:', err);
  }};
}}

// ── Expand/Collapse ────────────────────────────────────────────────────
function toggleMarket(idx) {{
  const outcomesEl = document.getElementById('outcomes-' + idx);
  const iconEl = document.getElementById('icon-' + idx);

  if (openIdx === idx) {{
    // Collapse — close WS and research panel
    outcomesEl.style.display = 'none';
    iconEl.classList.remove('open');
    openIdx = null;
    closeBookWs();
    closeResearch();
    return;
  }}

  // Collapse previous
  if (openIdx !== null) {{
    document.getElementById('outcomes-' + openIdx).style.display = 'none';
    document.getElementById('icon-' + openIdx).classList.remove('open');
  }}

  // Expand this one
  outcomesEl.style.display = 'block';
  iconEl.classList.add('open');
  openIdx = idx;

  // Open research panel with market question
  const card = document.querySelector(`.market-card[data-idx="${{idx}}"]`);
  if (card) openResearch(card.dataset.question);

  // Show loading and open WS stream
  const loadingEl = document.getElementById('loading-' + idx);
  if (loadingEl) loadingEl.style.display = 'block';
  openBookWs(idx);
}}

function renderOutcomes(idx, data) {{
  const container = document.getElementById('outcomes-' + idx);
  const outcomes = data.outcomes;
  const n = outcomes.length;
  let html = '';

  const refreshTime = new Date().toLocaleTimeString();
  const wsStatus = bookWs && bookWs.readyState === WebSocket.OPEN
    ? `<span style="color:#2ecc71">● LIVE</span>`
    : `<span style="color:#f39c12">● connecting...</span>`;
  html += `<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;font-size:0.78em;color:#8b949e">
    ${{wsStatus}}
    <span>Updated: ${{refreshTime}}</span>
    <span style="color:#484f58">${{wsUpdateCount}} updates</span>
  </div>`;

  for (let oi = 0; oi < n; oi++) {{
    const info = outcomes[oi];
    const askStr = info.best_ask ? ('$' + info.best_ask) : '-';
    const bidStr = info.best_bid ? ('$' + info.best_bid) : '-';

    const buyCost = parseFloat(info.sweepable_cost) || 0;
    const buyProfit = parseFloat(info.sweepable_profit) || 0;
    const buyShares = info.sweepable_shares;
    const sellShares = info.sellable_shares;

    const buyPct = buyCost > 0 ? ((buyProfit / buyCost) * 100).toFixed(2) + '%' : '-';

    // Sell-losers revenue: sum of sellable_revenue for OTHER outcomes
    let sellRev = 0;
    for (let j = 0; j < n; j++) {{
      if (j !== oi) sellRev += parseFloat(outcomes[j].sellable_revenue) || 0;
    }}

    const combined = buyProfit + sellRev;
    const profitColor = combined > 0 ? '#2ecc71' : '#8b949e';
    const sellDetail = sellRev > 0 ? ` <span style="color:#8b949e;font-size:0.8em">+ $${{sellRev.toFixed(4)}} sell losers</span>` : '';
    const hasOpp = parseFloat(buyShares) > 0 || sellRev > 0;
    // Use data attributes to avoid quote-escaping issues in onclick
    const btn = hasOpp
      ? `<button class="harvest-btn" data-token="${{info.token_id}}" data-oi="${{oi}}" data-idx="${{idx}}">Select Winner</button>`
      : '';

    html += `<div class="outcome-row">
      <span class="outcome-label">${{info.label}}</span>
      <span class="outcome-ask">ask ${{askStr}}</span>
      <span class="outcome-bid">bid ${{bidStr}}</span>
      <span class="outcome-shares">${{buyShares}} buy / ${{sellShares}} sell</span>
      <span class="outcome-cost">$${{buyCost.toFixed(4)}}</span>
      <span class="outcome-profit" style="color:${{profitColor}}">$${{combined.toFixed(4)}} (${{buyPct}})${{sellDetail}}</span>
      ${{btn}}
    </div>`;
  }}

  container.innerHTML = html;

  // Attach click handlers via delegation (avoids quote-escaping bugs)
  container.querySelectorAll('.harvest-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const bookIdx = parseInt(btn.dataset.idx);
      const oi = parseInt(btn.dataset.oi);
      const cached = cachedBooks[bookIdx];
      if (!cached) return;
      const label = cached.outcomes[oi].label;
      const question = cached.question;
      harvest(bookIdx, oi, label, question);
    }});
  }});
}}

// ── Harvest ────────────────────────────────────────────────────────────
async function harvest(marketIdx, winnerIdx, label, question) {{
  if (!confirm('Select "' + label + '" as winner for "' + question + '"?\n\nWill mint+sell losers if bids available, otherwise buy directly.')) return;
  try {{
    const resp = await fetch('/api/harvest', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ market_idx: marketIdx, winner_idx: winnerIdx, label: label, market_question: question }}),
    }});
    const data = await resp.json();
    if (data.ok) {{
      const e = data.entry;
      let msg = `Strategy: ${{e.strategy}}\n`;
      if (e.mint_cost !== '0' && e.mint_cost !== '0.0000') {{
        msg += `Minted: $${{e.mint_cost}} | Sell rev: $${{e.sell_revenue}}\n`;
      }}
      if (e.buy_cost !== '0.0000') {{
        msg += `Buy: ${{e.buy_shares}} shares @ $${{e.buy_cost}}\n`;
      }}
      msg += `Net profit: $${{e.net_profit}}\n\n${{e.status}}`;
      alert(msg);
      location.reload();
    }} else {{
      alert('Error: ' + (data.error || 'Unknown error'));
    }}
  }} catch (e) {{
    alert('Request failed: ' + e);
  }}
}}

// ── Position recovery (merge / redeem) ─────────────────────────────────
async function recoverPosition(conditionId, amount, market, action) {{
  const verb = action === 'merge' ? 'merge' : 'redeem';
  const desc = action === 'merge'
    ? `Merge $${{amount}} (burn YES+NO pair → recover USDC.e)?`
    : `Redeem winning tokens for "${{market}}"? (only works after market resolution)`;
  if (!confirm(desc)) return;
  const body = {{ condition_id: conditionId, market: market }};
  if (action === 'merge') body.amount = amount;
  try {{
    const resp = await fetch('/api/' + action, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(body),
    }});
    const data = await resp.json();
    if (data.ok) {{
      alert(verb.toUpperCase() + ' OK\n\n' + (data.entry ? data.entry.status : ''));
      location.reload();
    }} else {{
      alert(verb.toUpperCase() + ' failed: ' + (data.error || (data.entry ? data.entry.status : 'unknown')));
      if (data.entry) location.reload();
    }}
  }} catch (e) {{
    alert('Request failed: ' + e);
  }}
}}

async function manualRecover(action) {{
  const cid = document.getElementById('recover-cid').value.trim();
  const amount = document.getElementById('recover-amount').value.trim();
  const result = document.getElementById('recover-result');
  if (!cid) {{ result.textContent = 'condition_id is required'; return; }}
  if (action === 'merge' && !amount) {{ result.textContent = 'amount is required for merge'; return; }}
  result.textContent = 'submitting ' + action + '...';
  await recoverPosition(cid, amount, '', action);
}}

async function clearGeoblock() {{
  if (!confirm('Clear the geoblock flag? Only do this AFTER you have enabled a VPN and verified you are routing through a permitted region.')) return;
  await fetch('/api/clear-geoblock', {{ method: 'POST' }});
  location.reload();
}}

// ── Research Panel ─────────────────────────────────────────────────────
function getSearchUrl(query) {{
  const engine = document.getElementById('search-engine').value;
  const q = encodeURIComponent(query);
  switch (engine) {{
    case 'google': return `https://www.google.com/search?igu=1&q=${{q}}`;
    case 'ddg':    return `https://duckduckgo.com/?q=${{q}}`;
    case 'bing':   return `https://www.bing.com/search?q=${{q}}`;
    default:       return `https://www.google.com/search?igu=1&q=${{q}}`;
  }}
}}

function openResearch(question) {{
  const panel = document.getElementById('research-panel');
  const input = document.getElementById('research-query');
  const title = document.getElementById('research-title');
  const iframe = document.getElementById('research-iframe');

  panel.classList.add('open');
  input.value = question;
  title.textContent = question;
  iframe.src = getSearchUrl(question);
}}

function reSearch() {{
  const query = document.getElementById('research-query').value.trim();
  if (!query) return;
  const iframe = document.getElementById('research-iframe');
  const title = document.getElementById('research-title');
  title.textContent = query;
  iframe.src = getSearchUrl(query);
}}

function openResearchExternal() {{
  const query = document.getElementById('research-query').value.trim();
  if (!query) return;
  window.open(getSearchUrl(query), '_blank');
}}

function closeResearch() {{
  const panel = document.getElementById('research-panel');
  const iframe = document.getElementById('research-iframe');
  panel.classList.remove('open');
  iframe.src = 'about:blank';
}}

// ── Init ───────────────────────────────────────────────────────────────
renderPage();
</script>
</body>
</html>"##,
        mode = if state.live_mode { "LIVE" } else { "PAPER" },
        mode_color = if state.live_mode { "#e74c3c" } else { "#f39c12" },
        max_buy = state.max_buy,
        max_trade = state.max_trade,
        max_daily = state.max_daily,
        spent_today = spent_today_str,
        market_count = markets.len(),
        trade_count = activity.len(),
        market_rows = market_rows,
        geoblock_banner = geoblock_banner,
    );

    Html(html)
}
