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
use surebet::harvester::{build_outcome_info, filter_with_resting_orders, scan_markets, HarvestableMarket, OutcomeInfo, ScanMode};
use surebet::orderbook::OrderBookStore;
use surebet::weather::forecast::{fetch_forecast, fetch_observations};
use surebet::weather::strategy::{
    bracket_probability, bracket_probability_high_observed, bracket_probability_low_observed,
    sigma_scale_high, sigma_scale_low,
};
use surebet::weather::{lookup_station, parse_bracket_label, TemperatureMeasure};
use surebet::ws::clob::{start_clob_ws, ClobEvent};

/// USDC on Polygon (USDC.e bridged)
const USDC: alloy::primitives::Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
/// CTF contract on Polygon (needs USDC approval for splits)
const CTF_CONTRACT: alloy::primitives::Address =
    address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");
/// NegRiskAdapter on Polygon — neg_risk markets must route split/merge through
/// this contract so the minted tokens are CLOB-tradeable (wrapped ERC-1155).
const NEG_RISK_ADAPTER: alloy::primitives::Address =
    address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296");
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

/// Browser User-Agent for outgoing HTTP requests so external sites
/// (Wunderground, etc.) don't block us as a bot. Recent Chrome on macOS.
const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                          AppleWebKit/537.36 (KHTML, like Gecko) \
                          Chrome/124.0.0.0 Safari/537.36";

// Minimal ERC20 ABI for allowance checks
alloy::sol! {
    #[sol(rpc)]
    interface IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

// NegRiskAdapter split/merge interface. For neg_risk markets the CLOB trades
// wrapped tokens; calling splitPosition/mergePositions on the raw CTF contract
// produces tokens the CLOB can't see. The NegRiskAdapter provides compatible
// overloads that produce CLOB-tradeable wrapped ERC-1155 tokens.
alloy::sol! {
    #[sol(rpc)]
    interface INegRiskAdapterSplit {
        function splitPosition(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;

        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;
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
        is_neg_risk: bool,
    ) -> anyhow::Result<OnchainTxResult>;

    async fn merge_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
        is_neg_risk: bool,
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
        is_neg_risk: bool,
    ) -> anyhow::Result<OnchainTxResult> {
        if is_neg_risk {
            // Route through NegRiskAdapter so the minted tokens are wrapped
            // ERC-1155s that the CLOB exchange can recognise.
            let adapter = INegRiskAdapterSplit::new(
                NEG_RISK_ADAPTER,
                self.client.provider().clone(),
            );
            let partition = vec![U256::from(1u64), U256::from(2u64)];
            let pending_tx = adapter
                .splitPosition(USDC, B256::ZERO, condition_id, partition, amount_usdc_units)
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("NegRisk split failed: {e}"))?;
            let tx_hash = *pending_tx.tx_hash();
            let _receipt = pending_tx
                .get_receipt()
                .await
                .map_err(|e| anyhow::anyhow!("NegRisk split receipt failed: {e}"))?;
            Ok(self.gas_for(tx_hash).await)
        } else {
            let req =
                SplitPositionRequest::for_binary_market(USDC, condition_id, amount_usdc_units);
            let resp = self
                .client
                .split_position(&req)
                .await
                .map_err(|e| anyhow::anyhow!("CTF split failed: {}", e))?;
            Ok(self.gas_for(resp.transaction_hash).await)
        }
    }

    async fn merge_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
        is_neg_risk: bool,
    ) -> anyhow::Result<OnchainTxResult> {
        if is_neg_risk {
            let adapter = INegRiskAdapterSplit::new(
                NEG_RISK_ADAPTER,
                self.client.provider().clone(),
            );
            let partition = vec![U256::from(1u64), U256::from(2u64)];
            let pending_tx = adapter
                .mergePositions(USDC, B256::ZERO, condition_id, partition, amount_usdc_units)
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("NegRisk merge failed: {e}"))?;
            let tx_hash = *pending_tx.tx_hash();
            let _receipt = pending_tx
                .get_receipt()
                .await
                .map_err(|e| anyhow::anyhow!("NegRisk merge receipt failed: {e}"))?;
            Ok(self.gas_for(tx_hash).await)
        } else {
            let req =
                MergePositionsRequest::for_binary_market(USDC, condition_id, amount_usdc_units);
            let resp = self
                .client
                .merge_positions(&req)
                .await
                .map_err(|e| anyhow::anyhow!("CTF merge failed: {}", e))?;
            Ok(self.gas_for(resp.transaction_hash).await)
        }
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
    /// True while the background market scan is in progress. The dashboard
    /// shows a loading banner until this clears.
    scanning: Arc<AtomicBool>,
    /// Current scan mode — switchable at runtime via POST /api/rescan.
    scan_mode: Arc<RwLock<ScanMode>>,
    /// Gamma API base URL, stored for runtime rescans.
    gamma_url: String,
    /// Harvester config values needed to re-run scan_markets() on demand.
    end_date_window_days: i64,
    resolution_window_behind_hours: i64,
    resolution_window_ahead_hours: i64,
    min_volume_usd: f64,
    min_depth_usd: f64,
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
    let closed_mode = args.iter().any(|a| a == "--closed");

    let config = match Config::load(std::path::Path::new("surebet.toml")) {
        Ok(c) => c,
        Err(_) => Config::from_env(),
    };

    let gamma_url = config.polymarket.gamma_url.clone();
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

    let scan_mode = if closed_mode {
        ScanMode::ResolutionWindow
    } else {
        ScanMode::NearExpiry
    };

    println!("=== Harvester Dashboard ===");
    println!(
        "Mode: {}  |  Scan: {}  |  Buy limit: ${}",
        if live_mode { "LIVE" } else { "PAPER" },
        match scan_mode {
            ScanMode::NearExpiry => format!("near-expiry ({}d window)", harvester.end_date_window_days),
            ScanMode::ResolutionWindow => format!(
                "resolution window ({}h ago → +{}h)",
                harvester.resolution_window_behind_hours,
                harvester.resolution_window_ahead_hours,
            ),
        },
        max_buy,
    );

    // ── Step 1 (deferred): market scan runs in background after server starts ──

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
                                // Check USDC approval for CTF contract + NegRiskAdapter
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

                                            // Also check NegRiskAdapter allowance
                                            match usdc_contract
                                                .allowance(wallet_addr, NEG_RISK_ADAPTER)
                                                .call()
                                                .await
                                            {
                                                Ok(nr_allowance) => {
                                                    if nr_allowance.is_zero() {
                                                        println!("  WARNING: USDC not approved for NegRiskAdapter — neg_risk mints will fail!");
                                                        println!("  Approve 0x{:x} to spend USDC.", NEG_RISK_ADAPTER);
                                                    } else {
                                                        let nr_usdc = nr_allowance / U256::from(1_000_000u64);
                                                        println!("  USDC allowance for NegRiskAdapter: ${nr_usdc}");
                                                    }
                                                }
                                                Err(e) => {
                                                    println!("  WARNING: could not check NegRiskAdapter allowance: {e}");
                                                }
                                            }
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
        markets: Arc::new(RwLock::new(Vec::new())),
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
        scanning: Arc::new(AtomicBool::new(true)),
        scan_mode: Arc::new(RwLock::new(scan_mode)),
        gamma_url,
        end_date_window_days: harvester.end_date_window_days,
        resolution_window_behind_hours: harvester.resolution_window_behind_hours,
        resolution_window_ahead_hours: harvester.resolution_window_ahead_hours,
        min_volume_usd: harvester.min_volume_usd,
        min_depth_usd: harvester.min_depth_usd,
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
        .route("/api/rescan", post(api_rescan))
        .route("/api/status", get(api_status))
        .route("/api/positions", get(api_positions))
        .route("/weather", get(weather_html))
        .route("/api/weather", get(api_weather))
        .route("/observations", get(observations_html))
        .route("/api/observations", get(api_observations))
        .route("/paper-trades", get(paper_trades_html))
        .route("/api/paper-trade", post(api_save_paper_trade))
        .route("/api/paper-trades", get(api_paper_trades))
        .route("/api/paper-trade/close", post(api_close_paper_trade))
        .route("/ws/book", get(ws_book_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    println!("Dashboard: http://{}", bind_addr);
    println!("  Scanning markets in background — dashboard available immediately");

    // Spawn the initial market scan so the HTTP server starts right away.
    {
        let markets_ref = state.markets.clone();
        let scanning_ref = state.scanning.clone();
        let scan_mode_ref = state.scan_mode.clone();
        let g_url = state.gamma_url.clone();
        let c_url = state.clob_url.clone();
        let edw = state.end_date_window_days;
        let rbh = state.resolution_window_behind_hours;
        let rah = state.resolution_window_ahead_hours;
        let min_vol = state.min_volume_usd;
        let min_dep = state.min_depth_usd;
        tokio::spawn(async move {
            let mode = *scan_mode_ref.read().await;
            match scan_markets(&g_url, mode, edw, rbh, rah, usize::MAX, min_vol, min_dep).await {
                Ok(raw) => {
                    let markets = if mode == ScanMode::ResolutionWindow {
                        filter_with_resting_orders(&c_url, raw).await
                    } else {
                        raw
                    };
                    println!("  Background scan complete: {} markets loaded", markets.len());
                    *markets_ref.write().await = markets;
                }
                Err(e) => {
                    println!("  Background scan failed: {e}");
                }
            }
            scanning_ref.store(false, Ordering::Relaxed);
        });
    }

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
        "scanning": state.scanning.load(Ordering::Relaxed),
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
struct RescanRequest {
    /// "near_expiry" or "resolution_window"
    mode: String,
}

async fn api_rescan(
    State(state): State<AppState>,
    Json(req): Json<RescanRequest>,
) -> impl IntoResponse {
    let mode = match req.mode.as_str() {
        "resolution_window" => ScanMode::ResolutionWindow,
        _ => ScanMode::NearExpiry,
    };

    // Update the stored scan mode before scanning so the dashboard reflects it
    // immediately even if the scan is slow.
    *state.scan_mode.write().await = mode;

    let result = scan_markets(
        &state.gamma_url,
        mode,
        state.end_date_window_days,
        state.resolution_window_behind_hours,
        state.resolution_window_ahead_hours,
        usize::MAX,
        state.min_volume_usd,
        state.min_depth_usd,
    )
    .await;

    match result {
        Ok(raw_markets) => {
            let markets = if mode == ScanMode::ResolutionWindow {
                filter_with_resting_orders(&state.clob_url, raw_markets).await
            } else {
                raw_markets
            };
            let count = markets.len();
            *state.markets.write().await = markets;
            Json(serde_json::json!({
                "ok": true,
                "count": count,
                "mode": req.mode,
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
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
    /// Whether this is a neg_risk market (routes through NegRiskAdapter).
    #[serde(default)]
    is_neg_risk: bool,
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

    // Use the explicit is_neg_risk flag from the request. Defaults to false
    // (raw CTF merge) which is correct for positions minted before the
    // NegRiskAdapter routing was added. Future mints through the adapter will
    // need to set is_neg_risk=true when merging.
    let result = ctf
        .inner
        .merge_position(condition_id, U256::from(usdc_units), req.is_neg_risk)
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
                .split_position(condition_id, U256::from(usdc_units), market.is_neg_risk)
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
                    //
                    // The CLOB exchange indexes on-chain balances asynchronously,
                    // so we wait briefly for it to recognise the freshly-minted
                    // tokens before placing sell orders.
                    if let Some(order_client) = state.order_client.as_ref() {
                        // Wait for the CLOB balance indexer to catch up.
                        // Initial 2s pause, then retry sells up to 4 more times
                        // with 2s backoff — covers the typical 2-8s CLOB lag.
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                        for (loser_token_id, sellable_shares, best_bid) in &loser_sell_plans {
                            let sell_size = (*sellable_shares).min(mint_amount);

                            let mut last_err: Option<String> = None;
                            let mut sold = false;
                            for attempt in 0u32..5 {
                                if attempt > 0 {
                                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                }
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
                                        if attempt > 0 {
                                            status_parts.push(format!(
                                                "SELL OK id={} (retry {})", r.order_id, attempt
                                            ));
                                        } else {
                                            status_parts
                                                .push(format!("SELL OK id={}", r.order_id));
                                        }
                                        sold = true;
                                        break;
                                    }
                                    Ok(r) => {
                                        last_err = Some(format!("SELL FAIL: {}", r.error_msg));
                                        // Not a balance lag issue — don't retry
                                        if !r.error_msg.contains("not enough balance") {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let msg = format!("{e}");
                                        if msg.contains("403")
                                            || msg.contains("restricted in your region")
                                        {
                                            state.geoblocked.store(true, Ordering::Relaxed);
                                            last_err = Some(format!("SELL ERR: {}", msg));
                                            break;
                                        }
                                        last_err = Some(format!("SELL ERR: {}", msg));
                                        // Retry on balance/allowance errors from post_order
                                        if !msg.contains("not enough balance") {
                                            break;
                                        }
                                    }
                                }
                            }
                            if !sold {
                                if let Some(err) = last_err {
                                    status_parts.push(err);
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

// ─── Weather page ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct WeatherBracketJson {
    label: String,
    token_id: String,
    lower: Option<f64>,
    upper: Option<f64>,
    ask_price: Option<f64>,
    forecast_prob: Option<f64>,
    edge: Option<f64>,
    is_signal: bool,
}

#[derive(Serialize)]
struct ColdFrontCheckJson {
    /// Forecasted minimum temp across remaining hours of today (local).
    min_temp: f64,
    /// Local hour when that min is forecasted (e.g., "23:00").
    min_hour: String,
    /// Number of hourly samples that contributed.
    sample_count: usize,
    /// "normal" if forecast_min >= observed.min_so_far (morning trough holds),
    /// or "cold_front" if forecast_min < observed.min_so_far (evening drops below morning).
    scenario: String,
}

#[derive(Serialize)]
struct WeatherForecastJson {
    /// Effective mean used in bracket-probability math. For intra-day markets
    /// this is the station's current observed temperature; for future-date
    /// markets it's the Open-Meteo forecast mean.
    mean: f64,
    /// Effective σ used in bracket-probability math. For intra-day markets
    /// this is a baseline × peak-aware scale; for future-date it's the
    /// forecast's own σ (no scaling — the day hasn't started).
    std_dev: f64,
    /// Human-readable source identifier (e.g. "intra-day (METAR)" or "Open-Meteo").
    source: String,
}

#[derive(Serialize)]
struct WeatherMarketJson {
    condition_id: String,
    question: String,
    location: String,
    date: Option<String>,
    measure: String,
    unit: String,
    end_date: Option<String>,
    created_at: Option<String>,
    days_ahead: Option<i64>,
    forecast: Option<WeatherForecastJson>,
    /// "intra-day" or "future" — determines which data pipeline was used.
    /// Intra-day is sensor-only (METAR/Wunderground); future is Open-Meteo.
    regime: String,
    /// Running observed max/min/current for the resolution day. When present,
    /// bracket probabilities condition on this (dead brackets collapse to 0,
    /// remaining brackets absorb the redistributed mass — tighter edges).
    observed: Option<ObservationsBlock>,
    /// For LOW intra-day markets only: Open-Meteo hourly forecast for the
    /// remaining hours of today. If `min_temp < observed.min_so_far` it
    /// signals a "cold-front" scenario and the model mean shifts to `min_temp`.
    cold_front_check: Option<ColdFrontCheckJson>,
    brackets: Vec<WeatherBracketJson>,
    /// Best K-bracket coverage bets ("buy N likely outcomes, one wins"), if any
    /// positive-EV subsets exist. Sorted by the metric named in the struct.
    coverage: Vec<CoverageComboJson>,
    /// How far our model's expected value drifts from the market-implied one.
    /// When the gap is large, this market is almost certainly a forecast error
    /// on our side — signals should be ignored even if they look profitable.
    divergence: Option<DivergenceJson>,
}

#[derive(Serialize)]
struct DivergenceJson {
    /// Expected resolution value under our observation-aware model, in market unit.
    model_mean: f64,
    /// Expected resolution value under the market's normalized asks, in market unit.
    market_mean: f64,
    /// Signed gap: market_mean − model_mean. Positive = market expects warmer
    /// (for HIGH markets) or less cold (for LOW markets) than our forecast.
    delta: f64,
    /// Gap normalized by our effective σ. Values ≥ 2 indicate the two
    /// distributions barely overlap; one of them is almost certainly wrong.
    delta_sigmas: f64,
    /// "aligned" / "moderate" / "high" — calibrate UI warnings from this.
    alert: String,
}

#[derive(Serialize)]
struct CoverageComboJson {
    /// How this combo was selected: "max_ev" or "max_win_prob".
    strategy: String,
    /// 2 or 3 (how many YES tokens would be bought).
    size: usize,
    /// Bracket labels in the combo, e.g. ["17°C", "18°C"].
    labels: Vec<String>,
    /// YES token IDs to buy.
    token_ids: Vec<String>,
    /// Sum of YES asks (USD cost per share-set).
    cost: f64,
    /// Model probability that at least one bracket wins.
    win_prob: f64,
    /// Expected profit per share-set: `win_prob − cost`.
    ev: f64,
}

#[derive(Serialize)]
struct WeatherApiResponse {
    markets: Vec<WeatherMarketJson>,
    fetched_at: String,
    error: Option<String>,
}

/// GET /api/weather — fetch active weather bracket events from Gamma, enrich with
/// NWS/Open-Meteo forecasts, and return edge calculations as JSON.
///
/// Polymarket weather brackets are exposed as Gamma `/events` rows with `tag_slug=weather`.
/// Each event ("Highest temperature in <City> on <Date>?") groups many binary Y/N child
/// markets, one per bracket. Each child's `groupItemTitle` is the bracket label
/// (e.g. "11°C", "10°C or below"). `bestAsk` is included in the response so no separate
/// CLOB book fetch is needed.
async fn api_weather(State(state): State<AppState>) -> impl IntoResponse {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent(BROWSER_UA)
        .build()
        .unwrap_or_default();

    let now = chrono::Utc::now();

    let url = format!(
        "{}/events?tag_slug=weather&active=true&closed=false&limit=200",
        state.gamma_url
    );

    let raw_events: Vec<serde_json::Value> = match http.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
        Ok(resp) => {
            let status = resp.status();
            return Json(WeatherApiResponse {
                markets: vec![],
                fetched_at: now.to_rfc3339(),
                error: Some(format!("Gamma /events returned {status}")),
            });
        }
        Err(e) => {
            return Json(WeatherApiResponse {
                markets: vec![],
                fetched_at: now.to_rfc3339(),
                error: Some(format!("Gamma /events failed: {e}")),
            });
        }
    };

    let per_event_futures = raw_events.into_iter().map(|event| {
        let http = http.clone();
        async move { process_weather_market(&http, event, now).await }
    });
    let collected: Vec<Option<WeatherMarketJson>> = futures_util::stream::iter(per_event_futures)
        .buffer_unordered(20)
        .collect()
        .await;
    let mut results: Vec<WeatherMarketJson> = collected.into_iter().flatten().collect();

    results.sort_by(|a, b| a.end_date.cmp(&b.end_date));

    Json(WeatherApiResponse {
        markets: results,
        fetched_at: now.to_rfc3339(),
        error: None,
    })
}

/// Which regime a weather market falls into, determined from the station's
/// local time vs. the event's target date.
enum MarketRegime {
    /// Target date is today in the station's local timezone. Use only METAR
    /// (and Wunderground scrape) — the resolution-grade sensor data. No
    /// forecasting from gridded models.
    IntraDay { local_hour: f64 },
    /// Target date is in the future. No observations exist yet; use Open-Meteo
    /// forecast. This is where the "hoover new markets cheap, sell later when
    /// prices move" strategy lives.
    FutureDate,
    /// Market already resolved. Skip.
    Past,
}

/// The (mean, σ) pair the bracket-probability math actually uses, plus the
/// human-readable source string. Unified across regimes.
struct EffectiveModel {
    mean: f64,
    sigma: f64,
    source: String,
}

/// Process one weather event for the `/weather` tab: fetch the forecast, fetch
/// the day's running observations (when not a future-dated market), compute
/// observation-aware bracket probabilities, and assemble the JSON row.
async fn process_weather_market(
    http: &reqwest::Client,
    event: serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<WeatherMarketJson> {
    let title = event.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let (measure, city) = parse_weather_event_title(title)?;

    let end_date_utc = event
        .get("endDate")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    let target_date = end_date_utc.map(|d| d.date_naive());

    let (station_id, lat, lon) = match lookup_station(&city) {
        Some((sid, lat, lon)) => {
            let sid_opt = if sid.is_empty() { None } else { Some(sid.to_string()) };
            (sid_opt, Some(lat), Some(lon))
        }
        None => (None, None, None),
    };

    let unit = event
        .get("markets")
        .and_then(|v| v.as_array())
        .and_then(|markets| {
            markets.iter().find_map(|m| {
                let label = m
                    .get("groupItemTitle")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if label.contains("°F") {
                    Some(surebet::weather::TemperatureUnit::Fahrenheit)
                } else if label.contains("°C") {
                    Some(surebet::weather::TemperatureUnit::Celsius)
                } else {
                    None
                }
            })
        })
        .unwrap_or(surebet::weather::TemperatureUnit::Celsius);

    let resolution_url = event
        .get("resolutionSource")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let icao = surebet::weather::wunderground::icao_from_resolution_url(&resolution_url);

    // Determine regime: intra-day vs future-date vs past.
    // Intra-day uses ONLY METAR/Wunderground (the resolution sensor);
    // future-date uses Open-Meteo forecast (no observations exist yet).
    // Past markets are skipped — they've already resolved.
    let regime: MarketRegime = match (target_date, icao.as_deref()) {
        (Some(date), Some(icao_code)) => {
            let tz = surebet::weather::metar::tz_offset_for_icao(icao_code);
            let local_now = now + chrono::Duration::seconds(tz);
            let local_today = local_now.date_naive();
            if date == local_today {
                use chrono::Timelike;
                let t = local_now.time();
                let h = t.hour() as f64
                    + t.minute() as f64 / 60.0
                    + t.second() as f64 / 3600.0;
                MarketRegime::IntraDay { local_hour: h }
            } else if date > local_today {
                MarketRegime::FutureDate
            } else {
                MarketRegime::Past
            }
        }
        (Some(_), None) => MarketRegime::FutureDate, // no tz info; treat as future to use forecast
        _ => MarketRegime::FutureDate,
    };
    if matches!(regime, MarketRegime::Past) {
        return None;
    }

    // Dispatch data sources by regime. Intra-day = sensor-only (METAR +
    // Wunderground scrape); future-date = Open-Meteo forecast.
    let (forecast, observed) = match regime {
        MarketRegime::IntraDay { .. } => {
            let obs = fetch_sensor_observations(http, &resolution_url, target_date, unit).await;
            (None, obs)
        }
        MarketRegime::FutureDate => {
            let fcst = if let (Some(lat), Some(lon), Some(date)) = (lat, lon, target_date) {
                fetch_forecast(http, station_id.as_deref(), lat, lon, date, measure, unit)
                    .await
                    .ok()
            } else {
                None
            };
            (fcst, None)
        }
        MarketRegime::Past => unreachable!(),
    };

    // For LOW intra-day markets, also fetch Open-Meteo's hourly forecast for
    // the remaining hours of today — lets us detect "late-evening cooling may
    // produce a new daily min below the morning trough" scenarios.
    // This is the ONE place Open-Meteo enters intra-day calculations, per the
    // user's "cold-front sense check" directive.
    let cold_front_check: Option<surebet::weather::forecast::RemainingDayForecast> =
        match (&regime, measure, observed.as_ref(), lat, lon, target_date) {
            (
                MarketRegime::IntraDay { local_hour },
                TemperatureMeasure::Low,
                Some(_),
                Some(lat),
                Some(lon),
                Some(date),
            ) => surebet::weather::forecast::fetch_remaining_day_forecast(
                http, lat, lon, date, *local_hour, unit,
            )
            .await
            .ok(),
            _ => None,
        };

    // Build the model (mean, σ, source) the bracket-probability math will use.
    // - Intra-day HIGH: mean = observed.max_so_far, σ = baseline × peak-aware scale.
    // - Intra-day LOW:  mean anchors to the running morning-trough in normal
    //                   conditions, or shifts to the forecast's evening min if
    //                   a cold-front pushes below the morning trough.
    // - Future-date:    mean = forecast mean; σ = forecast.std_dev (day hasn't started).
    const INTRADAY_BASELINE_SIGMA: f64 = 3.0;
    let model: Option<EffectiveModel> = match (&regime, observed.as_ref(), forecast.as_ref()) {
        (MarketRegime::IntraDay { local_hour }, Some(o), _) => {
            let scale = match measure {
                TemperatureMeasure::High => sigma_scale_high(*local_hour),
                TemperatureMeasure::Low => sigma_scale_low(*local_hour),
            };
            let (mean, source_suffix) = match measure {
                TemperatureMeasure::High => {
                    // HIGH: running max so far — peak may still rise pre-3 PM.
                    (o.max_so_far, String::from("obs.max"))
                }
                TemperatureMeasure::Low => {
                    // LOW: check for cold-front scenario.
                    match &cold_front_check {
                        Some(f) if f.min_temp < o.min_so_far => {
                            // Evening cooling dips below the morning trough.
                            // Anchor mean to the forecasted evening min.
                            (
                                f.min_temp,
                                format!("cold-front → min at {}", f.min_hour),
                            )
                        }
                        _ => {
                            // Normal case: morning trough holds. Use observed.current
                            // so the distribution mass concentrates on the bracket
                            // containing observed.min_so_far.
                            (o.current, String::from("obs.current"))
                        }
                    }
                }
            };
            Some(EffectiveModel {
                mean,
                sigma: INTRADAY_BASELINE_SIGMA * scale,
                source: format!("intra-day ({}, {})", o.source, source_suffix),
            })
        }
        (MarketRegime::FutureDate, _, Some(f)) => Some(EffectiveModel {
            mean: f.mean,
            sigma: f.std_dev,
            source: f.source.to_string(),
        }),
        _ => None,
    };

    let mut brackets: Vec<WeatherBracketJson> = Vec::new();
    if let Some(child_markets) = event.get("markets").and_then(|v| v.as_array()) {
        for m in child_markets {
            let label = m
                .get("groupItemTitle")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if label.is_empty() {
                continue;
            }
            let Some((lower, upper)) = parse_bracket_label(&label) else {
                continue;
            };

            let token_id = m
                .get("clobTokenIds")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .and_then(|arr| arr.into_iter().next())
                .unwrap_or_default();
            if token_id.is_empty() {
                continue;
            }

            let ask = m
                .get("bestAsk")
                .and_then(|v| v.as_f64())
                .filter(|p| *p > 0.0);

            // Probability math — by regime:
            // - Intra-day: observation-aware, mean = observed current, σ =
            //   baseline × peak-aware scale. No forecast input.
            // - Future-date: plain forecast CDF; no observation to condition on.
            let fp = model.as_ref().map(|mdl| match (&regime, observed.as_ref()) {
                (MarketRegime::IntraDay { .. }, Some(o)) => match measure {
                    TemperatureMeasure::High => bracket_probability_high_observed(
                        lower, upper, o.max_so_far, mdl.mean, mdl.sigma,
                    ),
                    TemperatureMeasure::Low => bracket_probability_low_observed(
                        lower, upper, o.min_so_far, mdl.mean, mdl.sigma,
                    ),
                },
                _ => bracket_probability(lower, upper, mdl.mean, mdl.sigma),
            });

            let edge = match (fp, ask) {
                (Some(p), Some(a)) => Some(p - a),
                _ => None,
            };
            let is_signal = edge.map(|e| e >= 0.10).unwrap_or(false);

            brackets.push(WeatherBracketJson {
                label,
                token_id,
                lower,
                upper,
                ask_price: ask,
                forecast_prob: fp,
                edge,
                is_signal,
            });
        }
    }

    if brackets.is_empty() {
        return None;
    }

    let created_at = event
        .get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    let days_ahead: Option<i64> = match (created_at, end_date_utc) {
        (Some(ca), Some(ed)) => Some(ed.signed_duration_since(ca).num_days()),
        _ => end_date_utc.map(|ed| ed.signed_duration_since(now).num_days()),
    };

    let condition_id = event
        .get("slug")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| event.get("id").map(|v| v.to_string()))
        .unwrap_or_default();

    let coverage = compute_coverage_combos(&brackets);

    let divergence = model
        .as_ref()
        .and_then(|mdl| compute_divergence(&brackets, mdl.sigma));

    let regime_str = match regime {
        MarketRegime::IntraDay { .. } => "intra-day".to_string(),
        MarketRegime::FutureDate => "future".to_string(),
        MarketRegime::Past => unreachable!(),
    };

    Some(WeatherMarketJson {
        condition_id,
        question: title.to_string(),
        location: city,
        date: target_date.map(|d| d.format("%Y-%m-%d").to_string()),
        measure: match measure {
            TemperatureMeasure::High => "High".to_string(),
            TemperatureMeasure::Low => "Low".to_string(),
        },
        unit: match unit {
            surebet::weather::TemperatureUnit::Celsius => "°C".to_string(),
            surebet::weather::TemperatureUnit::Fahrenheit => "°F".to_string(),
        },
        end_date: end_date_utc.map(|d| d.to_rfc3339()),
        created_at: created_at.map(|d| d.to_rfc3339()),
        days_ahead,
        forecast: model.map(|mdl| WeatherForecastJson {
            mean: mdl.mean,
            std_dev: mdl.sigma,
            source: mdl.source,
        }),
        regime: regime_str,
        observed: observed.clone(),
        cold_front_check: cold_front_check.map(|f| ColdFrontCheckJson {
            scenario: if let Some(o) = observed.as_ref() {
                if f.min_temp < o.min_so_far {
                    "cold_front".to_string()
                } else {
                    "normal".to_string()
                }
            } else {
                "normal".to_string()
            },
            min_temp: f.min_temp,
            min_hour: f.min_hour,
            sample_count: f.sample_count,
        }),
        brackets,
        coverage,
        divergence,
    })
}

/// METAR → Wunderground fallback. Open-Meteo is intentionally excluded —
/// intra-day calculations use only the resolution-grade sensor at the
/// station, not gridded-model interpolations.
async fn fetch_sensor_observations(
    http: &reqwest::Client,
    resolution_url: &str,
    target_date: Option<chrono::NaiveDate>,
    unit: surebet::weather::TemperatureUnit,
) -> Option<ObservationsBlock> {
    let icao = surebet::weather::wunderground::icao_from_resolution_url(resolution_url)?;
    let c_to_market_unit = |c: f64| match unit {
        surebet::weather::TemperatureUnit::Celsius => c,
        surebet::weather::TemperatureUnit::Fahrenheit => c * 9.0 / 5.0 + 32.0,
    };

    // 1. METAR — native integer °C at the airport station.
    if let Some(date) = target_date {
        if let Ok(reports) =
            surebet::weather::metar::fetch_metar_reports(http, &icao, 36).await
        {
            let tz = surebet::weather::metar::tz_offset_for_icao(&icao);
            if let Some(agg) =
                surebet::weather::metar::aggregate_by_local_day(&reports, date, tz)
            {
                return Some(ObservationsBlock {
                    max_so_far: c_to_market_unit(agg.max_c),
                    min_so_far: c_to_market_unit(agg.min_c),
                    current: c_to_market_unit(agg.current_c),
                    last_observation_at: agg.latest_obs_utc.to_rfc3339(),
                    timezone: format!("UTC{:+}", tz / 3600),
                    source: format!("METAR ({} reports)", agg.report_count),
                    station: Some(agg.icao),
                    resolution_url: Some(resolution_url.to_string()),
                });
            }
        }
    }

    // 2. Wunderground scrape — same underlying data as METAR, reformatted.
    // Used when METAR is momentarily unavailable or the ICAO is archived.
    if let Ok(wu) = surebet::weather::wunderground::fetch_wunderground(http, &icao).await {
        let f_to_unit = |f: f64| match unit {
            surebet::weather::TemperatureUnit::Celsius => (f - 32.0) * 5.0 / 9.0,
            surebet::weather::TemperatureUnit::Fahrenheit => f,
        };
        return Some(ObservationsBlock {
            max_so_far: f_to_unit(wu.max_since_7am_f),
            min_so_far: f_to_unit(wu.min_24h_f),
            current: f_to_unit(wu.current_f),
            last_observation_at: wu.observation_time_utc.to_rfc3339(),
            timezone: String::new(),
            source: "Wunderground (scrape)".to_string(),
            station: Some(wu.icao),
            resolution_url: Some(resolution_url.to_string()),
        });
    }

    None
}

/// Compare our model's expected resolution value against the market's
/// implied expected value (from normalized YES asks). When they differ by
/// ≥2σ the two distributions barely overlap — usually a forecast error on
/// our side (markets aggregate better data than a single gridded forecast).
///
/// Returns `None` when there aren't enough priced brackets with computable
/// midpoints to get a meaningful market-implied mean.
fn compute_divergence(
    brackets: &[WeatherBracketJson],
    effective_sigma: f64,
) -> Option<DivergenceJson> {
    // Midpoint of each bracket. For unbounded brackets we approximate ±1
    // beyond the edge (matches our bucket-width convention).
    fn midpoint(b: &WeatherBracketJson) -> Option<f64> {
        match (b.lower, b.upper) {
            (Some(l), Some(u)) => Some((l + u) / 2.0),
            (Some(l), None) => Some(l + 1.0), // "X or above"
            (None, Some(u)) => Some(u - 1.0), // "X or below" (our upper = N+1)
            (None, None) => None,
        }
    }

    let priced: Vec<(&WeatherBracketJson, f64)> = brackets
        .iter()
        .filter_map(|b| b.ask_price.filter(|a| *a > 0.0).map(|a| (b, a)))
        .collect();
    if priced.len() < 3 {
        return None;
    }
    let ask_total: f64 = priced.iter().map(|(_, a)| a).sum();
    if ask_total <= 0.0 {
        return None;
    }

    let mut market_num = 0.0_f64;
    let mut market_w = 0.0_f64;
    for (b, a) in &priced {
        if let Some(m) = midpoint(b) {
            let w = a / ask_total;
            market_num += w * m;
            market_w += w;
        }
    }
    if market_w < 0.5 {
        return None;
    }
    let market_mean = market_num / market_w;

    let mut model_num = 0.0_f64;
    let mut model_w = 0.0_f64;
    for b in brackets {
        if let (Some(p), Some(m)) = (b.forecast_prob, midpoint(b)) {
            model_num += p * m;
            model_w += p;
        }
    }
    if model_w < 0.5 {
        return None;
    }
    let model_mean = model_num / model_w;

    let delta = market_mean - model_mean;
    let delta_sigmas = if effective_sigma > 0.0 {
        delta.abs() / effective_sigma
    } else {
        0.0
    };

    let alert = if delta_sigmas >= 2.0 {
        "high"
    } else if delta_sigmas >= 1.0 {
        "moderate"
    } else {
        "aligned"
    }
    .to_string();

    Some(DivergenceJson {
        model_mean,
        market_mean,
        delta,
        delta_sigmas,
        alert,
    })
}

/// Enumerate **adjacent** 2- and 3-bracket coverage combos for a market.
///
/// Two constraints, both for sanity:
/// 1. **Adjacency** — combo brackets must be consecutive after sorting by lower
///    bound. "16°C + 17°C" is fine; "9°C or below + 16°C" is not — physical
///    daily temperatures don't skip entire ranges, so such combos mix a live
///    bet with a lottery ticket.
/// 2. **Meaningful probability** — each bracket must contribute ≥1% win
///    probability under our model. Dead or near-dead brackets are excluded.
///
/// For each combo size (2, 3) we return up to two flavors: the combo with the
/// highest EV ("best profit") and the combo with the highest win probability
/// ("safest"). Dedup when both flavors pick the same window.
///
/// Combos are only returned if `cost < $1` (upside capacity) and EV > 0.
fn compute_coverage_combos(brackets: &[WeatherBracketJson]) -> Vec<CoverageComboJson> {
    #[derive(Clone)]
    struct Cand {
        idx_in_market: usize,
        lower: f64, // used for sorting — +∞-sentinel for "or below" type bounds
        label: String,
        token_id: String,
        prob: f64,
        ask: f64,
    }

    // Collect candidates, assigning each a sortable "lower" anchor:
    // - `Some(l)` → l                      (normal and "X or above" brackets)
    // - `None` upper-bound → use `upper − 1` as an anchor below the smallest l
    //   so "X or below" sorts to the far left.
    let mut cands: Vec<Cand> = brackets
        .iter()
        .enumerate()
        .filter_map(|(i, b)| match (b.forecast_prob, b.ask_price) {
            (Some(p), Some(a)) if a > 0.0 && a < 1.0 && p >= 0.01 => {
                let anchor = b.lower.or(b.upper.map(|u| u - 1.0))?;
                Some(Cand {
                    idx_in_market: i,
                    lower: anchor,
                    label: b.label.clone(),
                    token_id: b.token_id.clone(),
                    prob: p,
                    ask: a,
                })
            }
            _ => None,
        })
        .collect();
    cands.sort_by(|a, b| a.lower.partial_cmp(&b.lower).unwrap_or(std::cmp::Ordering::Equal));
    if cands.len() < 2 {
        return Vec::new();
    }
    let _ = |_c: &Cand| (); // silence idx_in_market-unused if adjacency check changes

    let make_combo = |strategy: &str, window: &[&Cand]| -> CoverageComboJson {
        let cost: f64 = window.iter().map(|c| c.ask).sum();
        let win_prob: f64 = window.iter().map(|c| c.prob).sum();
        CoverageComboJson {
            strategy: strategy.to_string(),
            size: window.len(),
            labels: window.iter().map(|c| c.label.clone()).collect(),
            token_ids: window.iter().map(|c| c.token_id.clone()).collect(),
            cost,
            win_prob,
            ev: win_prob - cost,
        }
    };

    // Adjacency: only contiguous windows of size k.
    let windows_of_size = |k: usize| -> Vec<Vec<&Cand>> {
        if k > cands.len() {
            return vec![];
        }
        (0..=(cands.len() - k))
            .map(|start| cands[start..start + k].iter().collect::<Vec<&Cand>>())
            .collect()
    };

    let mut results: Vec<CoverageComboJson> = Vec::new();
    for k in [2, 3] {
        let mut scored: Vec<(Vec<&Cand>, f64, f64, f64)> = windows_of_size(k)
            .into_iter()
            .filter_map(|w| {
                let cost: f64 = w.iter().map(|c| c.ask).sum();
                if cost >= 1.0 {
                    return None;
                }
                let wp: f64 = w.iter().map(|c| c.prob).sum();
                let ev = wp - cost;
                if ev <= 0.0 {
                    return None;
                }
                Some((w, cost, wp, ev))
            })
            .collect();
        if scored.is_empty() {
            continue;
        }

        // "Best profit" = max EV
        scored.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
        let best_ev_window: Vec<&Cand> = scored[0].0.clone();
        results.push(make_combo("best_profit", &best_ev_window));

        // "Safest" = max P(win). Skip if identical to best-profit pick.
        scored.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        let safest_window: Vec<&Cand> = scored[0].0.clone();
        let same = safest_window.len() == best_ev_window.len()
            && safest_window
                .iter()
                .zip(best_ev_window.iter())
                .all(|(a, b)| a.idx_in_market == b.idx_in_market);
        if !same {
            results.push(make_combo("safest", &safest_window));
        }
    }
    results
}

/// Shared helper used by both `/api/weather` and `/api/observations`:
/// fetch today's running max/min/current using METAR → Wunderground → Open-Meteo.
///
/// Returns `None` when the market resolves on a future local date (no
/// observations exist yet) or when every provider fails.
async fn fetch_obs_block_for_event(
    http: &reqwest::Client,
    resolution_url: &str,
    target_date: Option<chrono::NaiveDate>,
    lat: Option<f64>,
    lon: Option<f64>,
    unit: surebet::weather::TemperatureUnit,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<ObservationsBlock> {
    let icao = surebet::weather::wunderground::icao_from_resolution_url(resolution_url);

    let target_in_future = match (target_date, icao.as_deref()) {
        (Some(date), Some(icao_code)) => {
            let tz = surebet::weather::metar::tz_offset_for_icao(icao_code);
            let local_today = (now + chrono::Duration::seconds(tz)).date_naive();
            date > local_today
        }
        (Some(date), None) => date > now.date_naive(),
        _ => false,
    };
    if target_in_future {
        return None;
    }

    let c_to_market_unit = |c: f64| match unit {
        surebet::weather::TemperatureUnit::Celsius => c,
        surebet::weather::TemperatureUnit::Fahrenheit => c * 9.0 / 5.0 + 32.0,
    };

    // 1. METAR
    if let (Some(icao_code), Some(date)) = (&icao, target_date) {
        if let Ok(reports) =
            surebet::weather::metar::fetch_metar_reports(http, icao_code, 36).await
        {
            let tz = surebet::weather::metar::tz_offset_for_icao(icao_code);
            if let Some(agg) =
                surebet::weather::metar::aggregate_by_local_day(&reports, date, tz)
            {
                return Some(ObservationsBlock {
                    max_so_far: c_to_market_unit(agg.max_c),
                    min_so_far: c_to_market_unit(agg.min_c),
                    current: c_to_market_unit(agg.current_c),
                    last_observation_at: agg.latest_obs_utc.to_rfc3339(),
                    timezone: format!("UTC{:+}", tz / 3600),
                    source: format!("METAR ({} reports)", agg.report_count),
                    station: Some(agg.icao),
                    resolution_url: Some(resolution_url.to_string()),
                });
            }
        }
    }

    // 2. Wunderground
    if let Some(icao_code) = &icao {
        if let Ok(wu) =
            surebet::weather::wunderground::fetch_wunderground(http, icao_code).await
        {
            let f_to_unit = |f: f64| match unit {
                surebet::weather::TemperatureUnit::Celsius => (f - 32.0) * 5.0 / 9.0,
                surebet::weather::TemperatureUnit::Fahrenheit => f,
            };
            return Some(ObservationsBlock {
                max_so_far: f_to_unit(wu.max_since_7am_f),
                min_so_far: f_to_unit(wu.min_24h_f),
                current: f_to_unit(wu.current_f),
                last_observation_at: wu.observation_time_utc.to_rfc3339(),
                timezone: String::new(),
                source: "Wunderground (fallback)".to_string(),
                station: Some(wu.icao),
                resolution_url: Some(resolution_url.to_string()),
            });
        }
    }

    // 3. Open-Meteo
    if let (Some(lat), Some(lon), Some(date)) = (lat, lon, target_date) {
        if let Ok(om) = fetch_observations(http, lat, lon, date, unit).await {
            return Some(ObservationsBlock {
                max_so_far: om.max_so_far,
                min_so_far: om.min_so_far,
                current: om.current,
                last_observation_at: om.last_observation_at.to_rfc3339(),
                timezone: om.timezone,
                source: om.source.to_string(),
                station: icao.clone(),
                resolution_url: if resolution_url.is_empty() {
                    None
                } else {
                    Some(resolution_url.to_string())
                },
            });
        }
    }

    None
}

/// Parse a Polymarket weather event title like "Highest temperature in London on April 14?"
/// into (measure, city). Returns None if the title doesn't match the expected pattern.
fn parse_weather_event_title(title: &str) -> Option<(TemperatureMeasure, String)> {
    for (prefix, measure) in [
        ("Highest temperature in ", TemperatureMeasure::High),
        ("highest temperature in ", TemperatureMeasure::High),
        ("Lowest temperature in ", TemperatureMeasure::Low),
        ("lowest temperature in ", TemperatureMeasure::Low),
    ] {
        if let Some(rest) = title.strip_prefix(prefix) {
            if let Some((city, _)) = rest.split_once(" on ") {
                return Some((measure, city.trim().to_string()));
            }
        }
    }
    None
}

/// GET /weather — static HTML shell; JS loads /api/weather on page load.
async fn weather_html() -> Html<String> {
    Html(r##"<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Weather Markets — Harvester</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; max-width: 1400px; margin: 0 auto; }
  h1 { color: #58a6ff; margin-bottom: 5px; font-size: 1.4em; }
  .subtitle { color: #8b949e; margin-bottom: 20px; font-size: 0.85em; }
  h2 { color: #8b949e; margin: 20px 0 10px 0; font-size: 1.05em; border-bottom: 1px solid #21262d; padding-bottom: 5px; }
  .nav { display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 1px solid #21262d; padding-bottom: 8px; }
  .nav a { color: #8b949e; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 0.9em; border: 1px solid #21262d; }
  .nav a.active { background: #1f6feb; color: #fff; border-color: #388bfd; }
  .nav a:hover:not(.active) { background: #21262d; }
  .loading { color: #8b949e; padding: 40px; text-align: center; font-size: 0.95em; }
  .error { background: #4a1818; border: 1px solid #e74c3c; border-radius: 6px; padding: 12px 16px; color: #f5b7b1; margin: 12px 0; }
  .market-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 12px; overflow: hidden; }
  .market-header { padding: 12px 16px; border-bottom: 1px solid #21262d; }
  .market-title { color: #58a6ff; font-size: 0.9em; font-weight: bold; margin-bottom: 4px; }
  .market-meta { display: flex; gap: 16px; font-size: 0.78em; color: #8b949e; flex-wrap: wrap; margin-top: 4px; }
  .meta-item { display: flex; gap: 4px; }
  .meta-label { color: #484f58; }
  .meta-value { color: #8b949e; }
  .forecast-box { background: #0d1117; border: 1px solid #21262d; border-radius: 4px; padding: 8px 12px; font-size: 0.82em; margin: 10px 16px; }
  .forecast-label { color: #484f58; font-size: 0.8em; text-transform: uppercase; margin-bottom: 4px; }
  .forecast-val { color: #f0e68c; font-size: 1.05em; font-weight: bold; }
  .forecast-source { color: #8b949e; font-size: 0.78em; margin-top: 2px; }
  .no-forecast { color: #484f58; font-size: 0.8em; padding: 8px 16px; font-style: italic; }
  .brackets-table { width: 100%; border-collapse: collapse; font-size: 0.82em; }
  .brackets-table th { background: #21262d; color: #8b949e; text-align: left; padding: 7px 16px; font-size: 0.78em; text-transform: uppercase; }
  .brackets-table td { padding: 7px 16px; border-top: 1px solid #21262d; }
  .brackets-table tr:hover { background: #1c2128; }
  .bracket-label { color: #c9d1d9; font-weight: bold; }
  .price-cell { color: #8b949e; }
  .prob-cell { color: #79c0ff; }
  .edge-pos { color: #2ecc71; font-weight: bold; }
  .edge-neg { color: #484f58; }
  .signal-row { background: rgba(46, 204, 113, 0.06) !important; }
  .signal-badge { background: #2ecc71; color: #000; border-radius: 3px; padding: 1px 6px; font-size: 0.78em; font-weight: bold; }
  .no-ask { color: #484f58; font-style: italic; font-size: 0.85em; }
  .summary-bar { display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }
  .summary-card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 8px 14px; }
  .slabel { color: #8b949e; font-size: 0.7em; text-transform: uppercase; }
  .sval { font-size: 1.1em; font-weight: bold; margin-top: 2px; }
  .refresh-btn { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 6px 14px; border-radius: 4px; font-family: inherit; font-size: 0.82em; cursor: pointer; margin-left: auto; }
  .refresh-btn:hover { background: #30363d; }
  .spinner { display: inline-block; animation: spin 1s linear infinite; }
  @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
</head>
<body>
<h1>Weather Markets</h1>
<div class="nav">
  <a href="/">Markets</a>
  <a href="/trades">Trades</a>
  <a href="/weather" class="active">Weather</a>
  <a href="/observations">Observations</a>
  <a href="/paper-trades">Paper Trades</a>
</div>

<div id="content">
  <div class="loading"><span class="spinner">⟳</span> Loading weather markets...</div>
</div>

<!-- Floating paper-trade builder: appears when ≥1 bracket checkbox is picked.
     Only allows picks from a single market at a time. -->
<div id="trade-builder" style="
  position:fixed; bottom:0; left:0; right:0; background:#161b22;
  border-top:2px solid #30363d; padding:0.6em 1em; display:none; z-index:100;
  box-shadow:0 -4px 16px rgba(0,0,0,0.4);">
  <div style="display:flex; align-items:center; gap:1em; flex-wrap:wrap">
    <div style="font-weight:600" id="tb-market"></div>
    <div id="tb-summary" style="color:#8b949e"></div>
    <div style="margin-left:auto; display:flex; align-items:center; gap:0.5em">
      <label style="color:#8b949e;font-size:0.9em">Shares per leg:
        <input type="number" id="tb-shares" value="100" min="1" max="100000"
               style="width:5em; background:#0d1117; color:#c9d1d9; border:1px solid #30363d; padding:0.15em 0.3em" onchange="renderBuilder()">
      </label>
      <label style="color:#8b949e;font-size:0.9em">Mode:
        <select id="tb-mode" onchange="renderBuilder()"
                style="background:#0d1117; color:#c9d1d9; border:1px solid #30363d; padding:0.2em 0.4em">
          <option value="paper">Paper (simulated)</option>
          <option value="live" id="tb-mode-live" disabled>Live (real order)</option>
        </select>
      </label>
      <button id="tb-save" onclick="savePaperTrade()" style="
        background:#238636; color:white; border:none; padding:0.4em 1em;
        border-radius:4px; cursor:pointer; font-weight:600">Save paper trade</button>
      <button onclick="clearBuilder()" style="
        background:#21262d; color:#c9d1d9; border:1px solid #30363d; padding:0.4em 0.8em;
        border-radius:4px; cursor:pointer">Clear</button>
    </div>
  </div>
  <div id="tb-status" style="color:#7ee2a8; font-size:0.9em; margin-top:0.3em; display:none"></div>
</div>

<script>
let selectedLegs = [];
let selectedMarketSlug = null;
let serverLiveMode = false;

async function initMode() {
  try {
    const r = await fetch('/api/status');
    const j = await r.json();
    serverLiveMode = !!j.live_mode;
    const liveOpt = document.getElementById('tb-mode-live');
    if (liveOpt) {
      liveOpt.disabled = !serverLiveMode;
      liveOpt.textContent = serverLiveMode
        ? 'Live (real order) ⚠️'
        : 'Live — server not in --live mode';
    }
  } catch (e) { /* best-effort */ }
}

async function load() {
  const content = document.getElementById('content');
  try {
    const r = await fetch('/api/weather');
    const data = await r.json();
    render(content, data);
  } catch (e) {
    content.innerHTML = `<div class="error">Failed to load: ${e}</div>`;
  }
}

function fmt(v, decimals=3) {
  if (v == null) return '<span class="no-ask">—</span>';
  return (v * 100).toFixed(1) + '%';
}

function fmtEdge(v) {
  if (v == null) return '<span class="no-ask">—</span>';
  const pct = (v * 100).toFixed(1) + '%';
  if (v >= 0.10) return `<span class="edge-pos">+${pct} ▲</span>`;
  if (v >= 0) return `<span class="edge-pos">+${pct}</span>`;
  return `<span class="edge-neg">${pct}</span>`;
}

function onLegPick(el) {
  const d = el.dataset;
  const mkt = d.marketSlug;
  if (el.checked) {
    // Only allow picks from a single market at a time.
    if (selectedMarketSlug && selectedMarketSlug !== mkt) {
      clearBuilder();
      el.checked = true;
    }
    selectedMarketSlug = mkt;
    if (selectedLegs.length >= 3) {
      alert('Max 3 legs per combo.');
      el.checked = false;
      return;
    }
    selectedLegs.push({
      market_slug: d.marketSlug,
      market_question: d.marketQuestion,
      unit: d.unit,
      target_date: d.targetDate || null,
      regime: d.regime,
      bracket_label: d.bracket,
      token_id: d.tokenId,
      ask_at_buy: parseFloat(d.ask),
      model_prob_at_buy: d.prob === '' ? null : parseFloat(d.prob),
    });
  } else {
    selectedLegs = selectedLegs.filter(l => l.token_id !== d.tokenId);
    if (selectedLegs.length === 0) selectedMarketSlug = null;
  }
  renderBuilder();
}

function renderBuilder() {
  const bar = document.getElementById('trade-builder');
  if (selectedLegs.length === 0) { bar.style.display = 'none'; return; }
  bar.style.display = 'block';
  const shares = Math.max(1, parseInt(document.getElementById('tb-shares').value) || 100);
  const mode = document.getElementById('tb-mode').value;
  const cost = selectedLegs.reduce((s, l) => s + l.ask_at_buy * shares, 0);
  const prob = selectedLegs.reduce((s, l) => s + (l.model_prob_at_buy || 0), 0);
  const payout = shares;
  const ev = prob * payout - cost;
  document.getElementById('tb-market').textContent = selectedLegs[0].market_question;
  const legsStr = selectedLegs.map(l => `${l.bracket_label} @ $${l.ask_at_buy.toFixed(3)}`).join(' + ');
  const evColor = ev >= 0 ? '#7ee2a8' : '#ffa198';
  const sign = ev >= 0 ? '+' : '';
  document.getElementById('tb-summary').innerHTML = `
    <span style="color:#c9d1d9">${legsStr}</span>
    &nbsp;·&nbsp; cost <b style="color:#c9d1d9">$${cost.toFixed(2)}</b>
    &nbsp;·&nbsp; P(any wins) <b style="color:#c9d1d9">${(prob*100).toFixed(1)}%</b>
    &nbsp;·&nbsp; EV <b style="color:${evColor}">${sign}$${ev.toFixed(2)}</b>
    &nbsp;·&nbsp; max payout <b style="color:#c9d1d9">$${payout.toFixed(2)}</b>`;

  // Toggle button color + label based on mode.
  const btn = document.getElementById('tb-save');
  if (mode === 'live') {
    btn.style.background = '#da3633';
    btn.textContent = '⚠️ Place LIVE order (real money)';
  } else {
    btn.style.background = '#238636';
    btn.textContent = 'Save paper trade';
  }
}

function clearBuilder() {
  selectedLegs = [];
  selectedMarketSlug = null;
  document.querySelectorAll('.leg-pick:checked').forEach(cb => cb.checked = false);
  document.getElementById('tb-status').style.display = 'none';
  renderBuilder();
}

async function savePaperTrade() {
  if (selectedLegs.length === 0) return;
  const shares = Math.max(1, parseInt(document.getElementById('tb-shares').value) || 100);
  const mode = document.getElementById('tb-mode').value;
  const btn = document.getElementById('tb-save');

  if (mode === 'live') {
    const cost = selectedLegs.reduce((s, l) => s + l.ask_at_buy * shares, 0);
    const confirmMsg = `⚠️ PLACE LIVE ORDER? This will spend $${cost.toFixed(2)} of REAL USDC on ${selectedLegs.length} buy order(s). This cannot be undone. Type 'place' to confirm.`;
    const resp = prompt(confirmMsg);
    if (resp !== 'place') {
      alert('Cancelled.');
      return;
    }
  }

  btn.disabled = true;
  btn.textContent = mode === 'live' ? 'Placing orders…' : 'Saving…';
  const first = selectedLegs[0];
  const body = {
    market_slug: first.market_slug,
    market_question: first.market_question,
    unit: first.unit,
    target_date: first.target_date,
    regime: first.regime,
    legs: selectedLegs.map(l => ({
      bracket_label: l.bracket_label,
      token_id: l.token_id,
      ask_at_buy: l.ask_at_buy,
      shares: shares,
      model_prob_at_buy: l.model_prob_at_buy,
    })),
    model_win_prob_at_buy: selectedLegs.reduce((s, l) => s + (l.model_prob_at_buy || 0), 0),
    mode,
  };
  try {
    const r = await fetch('/api/paper-trade', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const j = await r.json();
    const status = document.getElementById('tb-status');
    if (j.saved) {
      const modeTag = j.mode === 'live' ? ' [LIVE]' : '';
      const orderSummary = j.order_ids
        ? ` · orders: ${j.order_ids.filter(x => x).length}/${j.order_ids.length} filled`
        : '';
      status.innerHTML = `✓ Saved trade${modeTag} <code>${j.id}</code> · cost $${j.total_cost.toFixed(2)}${orderSummary} · <a href="/paper-trades" style="color:#58a6ff">view on /paper-trades</a>`;
      status.style.color = '#7ee2a8';
      status.style.display = 'block';
      setTimeout(clearBuilder, 3500);
    } else {
      status.textContent = `Error: ${j.error || 'unknown'}`;
      status.style.color = '#ffa198';
      status.style.display = 'block';
    }
  } catch (e) {
    const status = document.getElementById('tb-status');
    status.textContent = `Error: ${e}`;
    status.style.color = '#ffa198';
    status.style.display = 'block';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Save paper trade';
  }
}

function render(el, data) {
  if (data.error) {
    el.innerHTML = `<div class="error">API error: ${data.error}</div>`;
    return;
  }
  const markets = data.markets || [];
  const totalSignals = markets.reduce((s, m) => s + m.brackets.filter(b => b.is_signal).length, 0);
  const withForecast = markets.filter(m => m.forecast != null).length;

  let html = `<div class="summary-bar">
    <div class="summary-card"><div class="slabel">Weather Markets</div><div class="sval">${markets.length}</div></div>
    <div class="summary-card"><div class="slabel">With Forecast</div><div class="sval" style="color:#79c0ff">${withForecast}</div></div>
    <div class="summary-card"><div class="slabel">Buy Signals</div><div class="sval" style="color:#2ecc71">${totalSignals}</div></div>
    <div class="summary-card"><div class="slabel">Fetched</div><div class="sval" style="font-size:0.85em;color:#8b949e">${new Date(data.fetched_at).toLocaleTimeString()}</div></div>
    <button class="refresh-btn" onclick="location.reload()">↻ Refresh</button>
  </div>`;

  if (markets.length === 0) {
    html += '<div class="loading" style="margin-top:40px">No active weather markets found in the next 20 days.</div>';
  }

  for (const m of markets) {
    const daysLeft = m.end_date ? Math.round((new Date(m.end_date) - Date.now()) / 86400000) : '?';
    const daysAheadStr = m.days_ahead != null ? `${m.days_ahead}d window` : '';
    const createdTime = m.created_at ? new Date(m.created_at).toISOString().slice(0,16).replace('T', ' ') + ' UTC' : '—';
    const endTime = m.end_date ? new Date(m.end_date).toISOString().slice(0,10) : '—';
    const signalCount = m.brackets.filter(b => b.is_signal).length;
    const signalBadge = signalCount > 0 ? ` <span class="signal-badge">${signalCount} signal${signalCount>1?'s':''}</span>` : '';

    let forecastHtml = '';
    if (m.forecast) {
      const f = m.forecast;
      const regimeLabel = m.regime === 'intra-day' ? 'Intra-day model' : 'Forecast';
      forecastHtml = `<div class="forecast-box">
        <div class="forecast-label">${regimeLabel} (${f.source})</div>
        <div class="forecast-val">${f.mean.toFixed(1)}${m.unit} <span style="color:#8b949e;font-size:0.85em">± ${f.std_dev.toFixed(2)}°</span></div>
      </div>`;
    } else {
      forecastHtml = `<div class="no-forecast">No forecast available (date may be out of range or location unmapped)</div>`;
    }

    // Observed running max/min from METAR (or fallback). When present,
    // bracket probabilities below are observation-aware: brackets that are
    // already mathematically unreachable get 0, remaining brackets absorb the
    // collapsed mass.
    let observedHtml = '';
    if (m.observed) {
      const o = m.observed;
      const stationLink = o.resolution_url
        ? `<a href="${o.resolution_url}" target="_blank" style="color:#58a6ff">${o.station || o.source}</a>`
        : (o.station || o.source);
      const ageMin = Math.round((Date.now() - new Date(o.last_observation_at).getTime()) / 60000);
      observedHtml = `<div class="forecast-box" style="background:#0d2818">
        <div class="forecast-label">Observed so far (${o.source})</div>
        <div class="forecast-val">
          max ${o.max_so_far.toFixed(1)}${m.unit} · min ${o.min_so_far.toFixed(1)}${m.unit} · now ${o.current.toFixed(1)}${m.unit}
          <span style="color:#8b949e;font-size:0.85em"> · ${stationLink} · ${ageMin}m ago</span>
        </div>
      </div>`;
    }

    let bracketsHtml = `<table class="brackets-table">
      <tr><th style="width:2em">Pick</th><th>Bracket</th><th>Ask</th><th>Forecast Prob</th><th>Edge</th><th></th></tr>`;
    for (const b of m.brackets) {
      const rowClass = b.is_signal ? ' class="signal-row"' : '';
      // Only offer selection if the bracket has a tradeable ask.
      const checkable = b.ask_price != null && b.ask_price > 0 && b.ask_price < 1;
      // Encode the payload we need when saving a leg — the onchange handler
      // reads data-* attrs.
      const cbData = checkable
        ? `data-market-slug="${m.condition_id}" data-market-question="${m.question.replace(/"/g, '&quot;')}" data-unit="${m.unit}" data-target-date="${m.date || ''}" data-regime="${m.regime}" data-bracket="${b.label}" data-token-id="${b.token_id}" data-ask="${b.ask_price}" data-prob="${b.forecast_prob != null ? b.forecast_prob : ''}"`
        : '';
      const cb = checkable
        ? `<input type="checkbox" class="leg-pick" ${cbData} onchange="onLegPick(this)">`
        : '';
      bracketsHtml += `<tr${rowClass}>
        <td>${cb}</td>
        <td class="bracket-label">${b.label}</td>
        <td class="price-cell">${fmt(b.ask_price)}</td>
        <td class="prob-cell">${fmt(b.forecast_prob)}</td>
        <td>${fmtEdge(b.edge)}</td>
        <td>${b.is_signal ? '<span class="signal-badge">BUY</span>' : ''}</td>
      </tr>`;
    }
    bracketsHtml += '</table>';

    // Coverage combos: buy multiple YES tokens so one of them wins. Each row
    // shows labels, total cost, P(one wins), and net EV per share-set.
    let coverageHtml = '';
    if (m.coverage && m.coverage.length > 0) {
      const rows = m.coverage.map(c => {
        const strategyLabel = c.strategy === 'best_profit'
          ? `Best profit (${c.size}-combo)`
          : `Safest (${c.size}-combo)`;
        const labelsStr = c.labels.join(' + ');
        return `<tr>
          <td style="padding-right:1em">${strategyLabel}</td>
          <td class="bracket-label">${labelsStr}</td>
          <td class="price-cell">$${c.cost.toFixed(3)}</td>
          <td class="prob-cell">${(c.win_prob * 100).toFixed(1)}%</td>
          <td class="edge-pos">+$${c.ev.toFixed(3)}</td>
        </tr>`;
      }).join('');
      coverageHtml = `<div class="coverage-section">
        <div class="coverage-label">Coverage bets — buy adjacent YES tokens, win if any resolves</div>
        <table class="brackets-table" style="margin-top:0.3em">
          <tr><th>Strategy</th><th>Brackets</th><th>Cost</th><th>P(win)</th><th>EV/share-set</th></tr>
          ${rows}
        </table>
      </div>`;
    }

    // Divergence badge — flag events where our model strongly disagrees
    // with the market (probably a forecast error on our side).
    let divBadge = '';
    if (m.divergence) {
      const dv = m.divergence;
      const dir = dv.delta > 0 ? '↑ market warmer' : '↓ market cooler';
      const color = dv.alert === 'high' ? '#ff7b72'
                  : dv.alert === 'moderate' ? '#d29922'
                  : '#7ee2a8';
      const tag = dv.alert === 'high' ? '⚠️ model vs market'
                : dv.alert === 'moderate' ? 'model vs market'
                : 'model vs market';
      const deltaStr = `${dv.delta > 0 ? '+' : ''}${dv.delta.toFixed(1)}${m.unit} (${dv.delta_sigmas.toFixed(1)}σ)`;
      divBadge = ` <span style="background:#21262d;color:${color};padding:0.05em 0.4em;border-radius:3px;font-size:0.78em">${tag}: ${deltaStr} ${dir}</span>`;
    }

    html += `<div class="market-card">
      <div class="market-header">
        <div class="market-title">${m.question}${signalBadge}${divBadge}</div>
        <div class="market-meta">
          <span class="meta-item"><span class="meta-label">City:</span><span class="meta-value">${m.location}</span></span>
          <span class="meta-item"><span class="meta-label">Date:</span><span class="meta-value">${m.date || '—'}</span></span>
          <span class="meta-item"><span class="meta-label">Measure:</span><span class="meta-value">${m.measure} ${m.unit}</span></span>
          <span class="meta-item"><span class="meta-label">End:</span><span class="meta-value">${endTime} (${daysLeft}d)</span></span>
          <span class="meta-item"><span class="meta-label">Created:</span><span class="meta-value">${createdTime}</span></span>
          <span class="meta-item"><span class="meta-label">Window:</span><span class="meta-value">${daysAheadStr}</span></span>
        </div>
      </div>
      ${forecastHtml}
      ${observedHtml}
      ${bracketsHtml}
      ${coverageHtml}
    </div>`;
  }

  el.innerHTML = html;
}

load();
initMode();
</script>
</body>
</html>"##.to_string())
}

// ─── Observations / dead-bracket page ───────────────────────────────────────

#[derive(Serialize)]
struct ObsBracketJson {
    label: String,
    token_id_yes: String,
    token_id_no: String,
    lower: Option<f64>,
    upper: Option<f64>,
    yes_ask: Option<f64>,
    yes_bid: Option<f64>,
    /// NO ask price ≈ 1 − YES bestBid (orderbook inversion). None if YES has no bid.
    no_ask_estimate: Option<f64>,
    /// "DEAD" (cannot win), "LOCKED_WIN" (already won), or "LIVE".
    status: String,
    /// For DEAD brackets: profit/share if you buy NO at no_ask_estimate
    /// and the bracket resolves NO (= YES bestBid).
    profit_per_share: Option<f64>,
}

#[derive(Serialize, Clone)]
struct ObservationsBlock {
    max_so_far: f64,
    min_so_far: f64,
    current: f64,
    last_observation_at: String,
    timezone: String,
    /// Data provider: "Wunderground" (scraped from station page) or "Open-Meteo" (gridded fallback).
    source: String,
    /// Station ICAO code if available (e.g. "EGLC"). Only set for Wunderground reads.
    station: Option<String>,
    /// Resolution source URL from Polymarket's event metadata.
    resolution_url: Option<String>,
}

#[derive(Serialize)]
struct ObservationEventJson {
    condition_id: String,
    question: String,
    location: String,
    date: Option<String>,
    measure: String,
    unit: String,
    end_date: Option<String>,
    observed: Option<ObservationsBlock>,
    brackets: Vec<ObsBracketJson>,
    dead_count: usize,
    /// Sum of `yes_bid` across DEAD brackets — rough proxy for arbitrage size.
    total_dead_opportunity: f64,
}

#[derive(Serialize)]
struct ObservationsApiResponse {
    events: Vec<ObservationEventJson>,
    fetched_at: String,
    error: Option<String>,
}

/// GET /api/observations — fetch active weather events, pull current-day
/// observations from Open-Meteo, and tag each bracket as DEAD / LOCKED_WIN / LIVE.
///
/// For "Highest temperature" markets, the running max can only go up — so any
/// bracket whose upper bound is already passed is mathematically dead. Buying
/// the NO side at (1 − YES bestBid) yields a guaranteed $1 payout.
async fn api_observations(State(state): State<AppState>) -> impl IntoResponse {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent(BROWSER_UA)
        .build()
        .unwrap_or_default();

    let now = chrono::Utc::now();

    let url = format!(
        "{}/events?tag_slug=weather&active=true&closed=false&limit=200",
        state.gamma_url
    );

    let raw_events: Vec<serde_json::Value> = match http.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.unwrap_or_default(),
        Ok(resp) => {
            let status = resp.status();
            return Json(ObservationsApiResponse {
                events: vec![],
                fetched_at: now.to_rfc3339(),
                error: Some(format!("Gamma /events returned {status}")),
            });
        }
        Err(e) => {
            return Json(ObservationsApiResponse {
                events: vec![],
                fetched_at: now.to_rfc3339(),
                error: Some(format!("Gamma /events failed: {e}")),
            });
        }
    };

    // Fan out per-event processing concurrently. Each event does up to 3 HTTP
    // calls (METAR → Wunderground → Open-Meteo); doing them sequentially for
    // 150 events takes ~30 seconds. Bounded concurrency = 20 keeps the host
    // honest without hammering remote APIs.
    let per_event_futures = raw_events.into_iter().map(|event| {
        let http = http.clone();
        async move {
            process_weather_event(&http, event, now).await
        }
    });
    let collected: Vec<Option<ObservationEventJson>> = futures_util::stream::iter(per_event_futures)
        .buffer_unordered(20)
        .collect()
        .await;
    let mut results: Vec<ObservationEventJson> = collected.into_iter().flatten().collect();

    // Sort by arbitrage opportunity size (highest first)
    results.sort_by(|a, b| {
        b.total_dead_opportunity
            .partial_cmp(&a.total_dead_opportunity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Json(ObservationsApiResponse {
        events: results,
        fetched_at: now.to_rfc3339(),
        error: None,
    })
}

/// Process one weather event: fetch observations (METAR → Wunderground →
/// Open-Meteo), classify each bracket, return the JSON row the UI will render.
///
/// Returns `None` for events that aren't temperature-bracket markets or have
/// no parseable brackets.
async fn process_weather_event(
    http: &reqwest::Client,
    event: serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<ObservationEventJson> {
    let title = event.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let (measure, city) = parse_weather_event_title(title)?;

    let end_date_utc = event
        .get("endDate")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    let target_date = end_date_utc.map(|d| d.date_naive());

    let (lat, lon) = lookup_station(&city)
        .map(|(_, la, lo)| (Some(la), Some(lo)))
        .unwrap_or((None, None));

    let unit = event
        .get("markets")
        .and_then(|v| v.as_array())
        .and_then(|markets| {
            markets.iter().find_map(|m| {
                let label = m
                    .get("groupItemTitle")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if label.contains("°F") {
                    Some(surebet::weather::TemperatureUnit::Fahrenheit)
                } else if label.contains("°C") {
                    Some(surebet::weather::TemperatureUnit::Celsius)
                } else {
                    None
                }
            })
        })
        .unwrap_or(surebet::weather::TemperatureUnit::Celsius);

    // Provider dispatch: METAR (native integer °C from aviationweather.gov) is
    // the same data Wunderground reformats — hitting it directly avoids the
    // °C → °F → °C round-trip that Wunderground's embedded JSON introduces.
    // Order: METAR → Wunderground scrape → Open-Meteo gridded fallback.
    let resolution_url = event
        .get("resolutionSource")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let icao = surebet::weather::wunderground::icao_from_resolution_url(resolution_url);

    // Skip observation fetch for markets whose target date is still in the future:
    // METAR has no reports yet, and Wunderground's "today's running max" is about
    // the wrong day. The Weather tab is for forecasts; Observations is for
    // already-observed data only.
    let target_in_future = match (target_date, icao.as_deref()) {
        (Some(date), Some(icao_code)) => {
            let tz = surebet::weather::metar::tz_offset_for_icao(icao_code);
            let local_today = (now + chrono::Duration::seconds(tz)).date_naive();
            date > local_today
        }
        (Some(date), None) => date > now.date_naive(),
        _ => false,
    };

    let obs: Option<ObservationsBlock> = if target_in_future {
        None
    } else {
        let mut result = None;

        let c_to_market_unit = |c: f64| match unit {
            surebet::weather::TemperatureUnit::Celsius => c,
            surebet::weather::TemperatureUnit::Fahrenheit => c * 9.0 / 5.0 + 32.0,
        };

        // 1. METAR.
        if let (Some(icao_code), Some(date)) = (&icao, target_date) {
            match surebet::weather::metar::fetch_metar_reports(http, icao_code, 36).await {
                Ok(reports) => {
                    let tz = surebet::weather::metar::tz_offset_for_icao(icao_code);
                    if let Some(agg) =
                        surebet::weather::metar::aggregate_by_local_day(&reports, date, tz)
                    {
                        result = Some(ObservationsBlock {
                            max_so_far: c_to_market_unit(agg.max_c),
                            min_so_far: c_to_market_unit(agg.min_c),
                            current: c_to_market_unit(agg.current_c),
                            last_observation_at: agg.latest_obs_utc.to_rfc3339(),
                            timezone: format!("UTC{:+}", tz / 3600),
                            source: format!("METAR ({} reports)", agg.report_count),
                            station: Some(agg.icao),
                            resolution_url: Some(resolution_url.to_string()),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(icao = %icao_code, error = %e, "METAR fetch failed");
                }
            }
        }

        // 2. Wunderground HTML fallback.
        if result.is_none() {
            if let Some(icao_code) = &icao {
                if let Ok(wu) =
                    surebet::weather::wunderground::fetch_wunderground(http, icao_code).await
                {
                    let f_to_unit = |f: f64| match unit {
                        surebet::weather::TemperatureUnit::Celsius => (f - 32.0) * 5.0 / 9.0,
                        surebet::weather::TemperatureUnit::Fahrenheit => f,
                    };
                    result = Some(ObservationsBlock {
                        max_so_far: f_to_unit(wu.max_since_7am_f),
                        min_so_far: f_to_unit(wu.min_24h_f),
                        current: f_to_unit(wu.current_f),
                        last_observation_at: wu.observation_time_utc.to_rfc3339(),
                        timezone: String::new(),
                        source: "Wunderground (fallback)".to_string(),
                        station: Some(wu.icao),
                        resolution_url: Some(resolution_url.to_string()),
                    });
                }
            }
        }

        // 3. Open-Meteo last-resort (past-hours only, never forecast hours).
        if result.is_none() {
            if let (Some(lat), Some(lon), Some(date)) = (lat, lon, target_date) {
                if let Ok(om) = fetch_observations(http, lat, lon, date, unit).await {
                    result = Some(ObservationsBlock {
                        max_so_far: om.max_so_far,
                        min_so_far: om.min_so_far,
                        current: om.current,
                        last_observation_at: om.last_observation_at.to_rfc3339(),
                        timezone: om.timezone,
                        source: om.source.to_string(),
                        station: icao.clone(),
                        resolution_url: if resolution_url.is_empty() {
                            None
                        } else {
                            Some(resolution_url.to_string())
                        },
                    });
                }
            }
        }
        result
    };

    let mut brackets: Vec<ObsBracketJson> = Vec::new();
    let mut dead_count = 0;
    let mut total_dead_opportunity = 0.0_f64;

    if let Some(child_markets) = event.get("markets").and_then(|v| v.as_array()) {
        for m in child_markets {
            let label = m
                .get("groupItemTitle")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if label.is_empty() {
                continue;
            }
            let Some((lower, upper)) = parse_bracket_label(&label) else {
                continue;
            };

            let token_ids: Vec<String> = m
                .get("clobTokenIds")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default();
            if token_ids.len() < 2 {
                continue;
            }
            let token_id_yes = token_ids[0].clone();
            let token_id_no = token_ids[1].clone();

            let yes_ask = m.get("bestAsk").and_then(|v| v.as_f64()).filter(|p| *p > 0.0);
            let yes_bid = m.get("bestBid").and_then(|v| v.as_f64()).filter(|p| *p > 0.0);
            let no_ask_estimate = yes_bid.map(|b| 1.0 - b);

            let status = if let Some(o) = obs.as_ref() {
                classify_bracket(measure, lower, upper, o.max_so_far, o.min_so_far)
            } else {
                "LIVE"
            }
            .to_string();

            let mut profit_per_share = None;
            if status == "DEAD" {
                dead_count += 1;
                if let Some(b) = yes_bid {
                    profit_per_share = Some(b);
                    total_dead_opportunity += b;
                }
            }

            brackets.push(ObsBracketJson {
                label,
                token_id_yes,
                token_id_no,
                lower,
                upper,
                yes_ask,
                yes_bid,
                no_ask_estimate,
                status,
                profit_per_share,
            });
        }
    }

    if brackets.is_empty() {
        return None;
    }

    let condition_id = event
        .get("slug")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| event.get("id").map(|v| v.to_string()))
        .unwrap_or_default();

    Some(ObservationEventJson {
        condition_id,
        question: title.to_string(),
        location: city,
        date: target_date.map(|d| d.format("%Y-%m-%d").to_string()),
        measure: match measure {
            TemperatureMeasure::High => "High".to_string(),
            TemperatureMeasure::Low => "Low".to_string(),
        },
        unit: match unit {
            surebet::weather::TemperatureUnit::Celsius => "°C".to_string(),
            surebet::weather::TemperatureUnit::Fahrenheit => "°F".to_string(),
        },
        end_date: end_date_utc.map(|d| d.to_rfc3339()),
        observed: obs,
        brackets,
        dead_count,
        total_dead_opportunity,
    })
}

/// Classify a bracket given the day's running max/min so far.
///
/// Floor-rounded brackets [lower, upper) — for HIGH markets max can only go up,
/// for LOW markets min can only go down.
fn classify_bracket(
    measure: TemperatureMeasure,
    lower: Option<f64>,
    upper: Option<f64>,
    max_so_far: f64,
    min_so_far: f64,
) -> &'static str {
    match measure {
        TemperatureMeasure::High => {
            if let Some(u) = upper {
                if max_so_far >= u {
                    return "DEAD";
                }
            } else if let Some(l) = lower {
                // "X or above" — locked once max ≥ X
                if max_so_far >= l {
                    return "LOCKED_WIN";
                }
            }
            "LIVE"
        }
        TemperatureMeasure::Low => {
            if let Some(l) = lower {
                if min_so_far < l {
                    return "DEAD";
                }
            } else if let Some(u) = upper {
                // "X or below" — locked once min < X+1
                if min_so_far < u {
                    return "LOCKED_WIN";
                }
            }
            "LIVE"
        }
    }
}

async fn observations_html() -> Html<String> {
    Html(r##"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Weather observations · dead-bracket scanner</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 1em; background: #0d1117; color: #c9d1d9; }
    h1 { margin: 0 0 0.5em; }
    .meta { color: #8b949e; font-size: 0.9em; margin-bottom: 1em; }
    nav a { color: #58a6ff; margin-right: 1em; text-decoration: none; }
    .event { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 0.75em 1em; margin-bottom: 1em; }
    .event-title { font-weight: 600; font-size: 1.05em; }
    .event-sub { color: #8b949e; font-size: 0.85em; margin: 0.3em 0 0.6em; }
    .obs { color: #c9d1d9; font-size: 0.9em; padding: 0.4em 0.6em; background: #0d1117; border-radius: 4px; margin-bottom: 0.5em; display: inline-block; }
    .obs strong { color: #58a6ff; }
    table { border-collapse: collapse; width: 100%; font-size: 0.85em; }
    th, td { padding: 0.3em 0.5em; text-align: right; border-bottom: 1px solid #21262d; }
    th { color: #8b949e; font-weight: normal; text-align: right; }
    th:first-child, td:first-child { text-align: left; }
    .badge { display: inline-block; padding: 0.05em 0.4em; border-radius: 3px; font-size: 0.8em; font-weight: 600; }
    .badge-dead { background: #6e1818; color: #ffa198; }
    .badge-locked { background: #1a4d2e; color: #7ee2a8; }
    .badge-live { background: #1f2933; color: #8b949e; }
    .opportunity { color: #7ee2a8; font-weight: 600; }
    .no-obs { color: #8b949e; font-style: italic; }
    .dead-count { color: #ffa198; }
  </style>
</head>
<body>
  <nav><a href="/">Markets</a> <a href="/weather">Weather</a> <a href="/observations">Observations</a> <a href="/paper-trades">Paper Trades</a></nav>
  <h1>Weather observations · dead bracket scanner</h1>
  <div class="meta" id="meta">Loading…</div>
  <div id="events"></div>
<script>
async function load() {
  const r = await fetch('/api/observations');
  const d = await r.json();
  const meta = document.getElementById('meta');
  const events = document.getElementById('events');
  if (d.error) { meta.innerHTML = `<span style="color:#ffa198">Error: ${d.error}</span>`; return; }

  const totalDead = d.events.reduce((s,e) => s + e.dead_count, 0);
  const totalOpp = d.events.reduce((s,e) => s + e.total_dead_opportunity, 0);
  meta.innerHTML = `Fetched at ${d.fetched_at} · ${d.events.length} events · <span class="dead-count">${totalDead} dead brackets</span> · total YES-bid opportunity $${totalOpp.toFixed(2)}/share-set`;

  events.innerHTML = d.events.map(e => {
    const obsHtml = e.observed
      ? (() => {
          const o = e.observed;
          const stationLink = o.resolution_url
            ? `<a href="${o.resolution_url}" target="_blank" style="color:#58a6ff">${o.station || o.source}</a>`
            : (o.station || o.source);
          const tzBit = o.timezone ? ` · tz=${o.timezone}` : '';
          const ageMin = Math.round((Date.now() - new Date(o.last_observation_at).getTime()) / 60000);
          return `<div class="obs">
            <strong>Max so far: ${o.max_so_far.toFixed(1)}${e.unit}</strong> ·
            Min: ${o.min_so_far.toFixed(1)}${e.unit} ·
            Current: ${o.current.toFixed(1)}${e.unit} ·
            <span style="color:#8b949e">${o.source} ${stationLink}${tzBit} · ${ageMin}m ago</span>
          </div>`;
        })()
      : (() => {
          // Distinguish future-date markets from missing-data markets.
          const today = new Date().toISOString().slice(0,10);
          if (e.date && e.date > today) {
            return `<div class="obs no-obs">Target date is in the future — see <a href="/weather" style="color:#58a6ff">Weather tab</a> for forecast</div>`;
          }
          return `<div class="obs no-obs">No observations available (no station mapping)</div>`;
        })();

    const rows = e.brackets.map(b => {
      const cls = b.status === 'DEAD' ? 'badge-dead'
                : b.status === 'LOCKED_WIN' ? 'badge-locked'
                : 'badge-live';
      const yesAsk = b.yes_ask != null ? '$' + b.yes_ask.toFixed(4) : '—';
      const yesBid = b.yes_bid != null ? '$' + b.yes_bid.toFixed(4) : '—';
      const noAsk  = b.no_ask_estimate != null ? '$' + b.no_ask_estimate.toFixed(4) : '—';
      const profit = b.profit_per_share != null
        ? `<span class="opportunity">+$${b.profit_per_share.toFixed(4)}</span>`
        : '—';
      return `<tr>
        <td>${b.label}</td>
        <td><span class="badge ${cls}">${b.status}</span></td>
        <td>${yesAsk}</td>
        <td>${yesBid}</td>
        <td>${noAsk}</td>
        <td>${profit}</td>
      </tr>`;
    }).join('');

    const oppStr = e.total_dead_opportunity > 0
      ? ` · <span class="opportunity">$${e.total_dead_opportunity.toFixed(4)}/set arb</span>`
      : '';

    return `<div class="event">
      <div class="event-title">${e.question}</div>
      <div class="event-sub">${e.location} · ${e.measure} · resolves ${e.date} <span class="dead-count">${e.dead_count} dead</span>${oppStr}</div>
      ${obsHtml}
      <table>
        <thead><tr><th>Bracket</th><th>Status</th><th>YES ask</th><th>YES bid</th><th>NO ask (est)</th><th>Buy-NO profit/share</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
  }).join('');
}
load();
</script>
</body>
</html>"##.to_string())
}

// ─── Paper trades (multi-outcome weather combos) ─────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PaperTradeLeg {
    bracket_label: String,
    token_id: String,
    ask_at_buy: f64,
    shares: f64,
    /// Model-assigned probability of this bracket winning at the time of purchase.
    #[serde(default)]
    model_prob_at_buy: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PaperTrade {
    id: String,
    created_at: String,
    market_slug: String,
    market_question: String,
    unit: String,
    target_date: Option<String>,
    regime: String,
    legs: Vec<PaperTradeLeg>,
    total_cost: f64,
    /// Model-assigned sum of leg probabilities ≈ P(any leg wins) at entry.
    #[serde(default)]
    model_win_prob_at_buy: Option<f64>,
    /// "paper" (default, no on-chain activity) or "live" (real CLOB orders placed).
    #[serde(default = "default_paper_mode")]
    mode: String,
    /// For live trades: the CLOB order IDs returned when each leg was placed.
    /// Aligned positionally with `legs` — `order_ids[i]` is the order for `legs[i]`.
    #[serde(default)]
    order_ids: Vec<Option<String>>,
    /// For live trades: per-leg fill error messages (if any placement failed).
    #[serde(default)]
    leg_errors: Vec<Option<String>>,
    /// "open" | "closed_manual" | "resolved".
    /// - open: still waiting on resolution or exit
    /// - closed_manual: user hit the Close button, capturing a snapshot of exit state
    /// - resolved: market resolved, one leg won, realized P&L recorded
    status: String,
    /// Populated when `status == "closed_manual"` or `"resolved"`. Sum of exit
    /// revenue across all legs at close time.
    #[serde(default)]
    close_sell_revenue: Option<f64>,
    /// Realized P&L at close/resolve: close_sell_revenue − total_cost.
    #[serde(default)]
    realized_pnl: Option<f64>,
    /// Timestamp of close/resolve (RFC3339).
    #[serde(default)]
    closed_at: Option<String>,
    /// For resolved trades: the bracket label that won (if any leg won).
    #[serde(default)]
    winning_bracket: Option<String>,
}

#[derive(Deserialize)]
struct PaperTradeRequest {
    market_slug: String,
    market_question: String,
    unit: String,
    target_date: Option<String>,
    regime: String,
    legs: Vec<PaperTradeLeg>,
    model_win_prob_at_buy: Option<f64>,
    /// "paper" (default) or "live". Live mode places real buy orders through
    /// the CLOB SDK; paper mode only records the intended trade.
    #[serde(default = "default_paper_mode")]
    mode: String,
}

const PAPER_TRADES_FILE: &str = "paper_trades.jsonl";

fn default_paper_mode() -> String {
    "paper".to_string()
}

async fn api_save_paper_trade(
    State(state): State<AppState>,
    Json(req): Json<PaperTradeRequest>,
) -> impl IntoResponse {
    if req.legs.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "no legs in trade"})),
        )
            .into_response();
    }

    let is_live = req.mode == "live";

    // Live-mode gate: the server must be running with --live, and we need
    // the SDK-backed order client + a USDC balance covering the total.
    if is_live {
        if !state.live_mode {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "server not in live mode — restart with --live to place real orders"
                })),
            )
                .into_response();
        }
        let Some(order_client) = state.order_client.as_ref() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "order_client not initialized"})),
            )
                .into_response();
        };
        // Balance check before committing
        if let Some(api) = state.api.as_ref() {
            let needed: Decimal = req
                .legs
                .iter()
                .map(|l| {
                    Decimal::from_f64_retain(l.ask_at_buy * l.shares).unwrap_or(Decimal::ZERO)
                })
                .sum();
            let balance = api.get_balance_usdc().await;
            if needed > balance {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("Insufficient USDC: need ${needed:.4}, have ${balance:.4}")
                    })),
                )
                    .into_response();
            }
        }

        // Place buy orders for each leg. Errors are captured per leg; a partial
        // fill still saves the trade (the user can handle cleanup manually).
        let mut order_ids: Vec<Option<String>> = Vec::with_capacity(req.legs.len());
        let mut leg_errors: Vec<Option<String>> = Vec::with_capacity(req.legs.len());
        for leg in &req.legs {
            let price = Decimal::from_f64_retain(leg.ask_at_buy).unwrap_or(Decimal::ZERO);
            let size = Decimal::from_f64_retain(leg.shares).unwrap_or(Decimal::ZERO);
            match order_client
                .place_limit(&leg.token_id, price, size, OrderSide::Buy)
                .await
            {
                Ok(r) if r.success => {
                    order_ids.push(Some(r.order_id));
                    leg_errors.push(None);
                }
                Ok(r) => {
                    order_ids.push(None);
                    leg_errors.push(Some(r.error_msg));
                }
                Err(e) => {
                    order_ids.push(None);
                    leg_errors.push(Some(format!("{e}")));
                }
            }
        }

        // If NOTHING placed successfully, treat as an error; otherwise save with
        // whatever got through and the user can decide what to do.
        if order_ids.iter().all(|o| o.is_none()) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "all legs failed",
                    "leg_errors": leg_errors,
                })),
            )
                .into_response();
        }

        let total_cost: f64 = req.legs.iter().map(|l| l.ask_at_buy * l.shares).sum();
        let trade = PaperTrade {
            id: uuid::Uuid::new_v4().to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            market_slug: req.market_slug,
            market_question: req.market_question,
            unit: req.unit,
            target_date: req.target_date,
            regime: req.regime,
            legs: req.legs,
            total_cost,
            model_win_prob_at_buy: req.model_win_prob_at_buy,
            mode: "live".to_string(),
            order_ids: order_ids.clone(),
            leg_errors: leg_errors.clone(),
            status: "open".to_string(),
            close_sell_revenue: None,
            realized_pnl: None,
            closed_at: None,
            winning_bracket: None,
        };
        if let Err(e) = persist_trade(&trade).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
                .into_response();
        }
        return Json(serde_json::json!({
            "saved": true,
            "id": trade.id,
            "mode": "live",
            "total_cost": trade.total_cost,
            "order_ids": order_ids,
            "leg_errors": leg_errors,
        }))
        .into_response();
    }

    // Paper mode: just record the intended trade, no on-chain activity.
    let total_cost: f64 = req.legs.iter().map(|l| l.ask_at_buy * l.shares).sum();
    let trade = PaperTrade {
        id: uuid::Uuid::new_v4().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        market_slug: req.market_slug,
        market_question: req.market_question,
        unit: req.unit,
        target_date: req.target_date,
        regime: req.regime,
        legs: req.legs,
        total_cost,
        model_win_prob_at_buy: req.model_win_prob_at_buy,
        mode: "paper".to_string(),
        order_ids: Vec::new(),
        leg_errors: Vec::new(),
        status: "open".to_string(),
        close_sell_revenue: None,
        realized_pnl: None,
        closed_at: None,
        winning_bracket: None,
    };

    if let Err(e) = persist_trade(&trade).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response();
    }
    Json(serde_json::json!({
        "saved": true,
        "id": trade.id,
        "mode": "paper",
        "total_cost": trade.total_cost,
    }))
    .into_response()
}

/// Fetch the best (highest) bid for a CLOB YES token.
///
/// Polymarket's CLOB `/book` endpoint returns bids sorted ASCENDING (lowest
/// first) and asks sorted DESCENDING (highest first) — both moving away from
/// the mid. So `bids.first()` is the WORST bid, not the best. Take the max
/// across all levels to get the real best bid.
async fn fetch_best_bid(
    http: &reqwest::Client,
    clob_url: &str,
    token_id: &str,
) -> Option<f64> {
    #[derive(Deserialize)]
    struct BookResp {
        bids: Option<Vec<PriceLevel>>,
    }
    #[derive(Deserialize)]
    struct PriceLevel {
        price: String,
    }
    let url = format!("{clob_url}/book?token_id={token_id}");
    let resp = http.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let book: BookResp = resp.json().await.ok()?;
    book.bids.and_then(|bs| {
        bs.iter()
            .filter_map(|p| p.price.parse::<f64>().ok())
            .reduce(f64::max)
    })
}

/// Append a trade to the JSONL log. Shared by both paper and live save paths.
async fn persist_trade(trade: &PaperTrade) -> Result<(), String> {
    let line = serde_json::to_string(trade)
        .map_err(|e| format!("serialize failed: {e}"))?;
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(PAPER_TRADES_FILE)
        .await
        .map_err(|e| format!("open failed: {e}"))?;
    use tokio::io::AsyncWriteExt;
    f.write_all(format!("{line}\n").as_bytes())
        .await
        .map_err(|e| format!("write failed: {e}"))?;
    Ok(())
}

#[derive(Deserialize)]
struct CloseTradeRequest {
    id: String,
}

/// POST /api/paper-trade/close
/// Mark a paper trade as closed, snapshot current exit revenue, and rewrite
/// the JSONL. Load-modify-rewrite is fine for a local file with small trade
/// counts; if this ever grows we'd move to a real store.
async fn api_close_paper_trade(
    State(state): State<AppState>,
    Json(req): Json<CloseTradeRequest>,
) -> impl IntoResponse {
    let contents = match tokio::fs::read_to_string(PAPER_TRADES_FILE).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("no paper trades file: {e}")})),
            )
                .into_response();
        }
    };
    let mut trades: Vec<PaperTrade> = contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<PaperTrade>(l).ok())
        .collect();

    let idx = match trades.iter().position(|t| t.id == req.id) {
        Some(i) => i,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "trade not found"})),
            )
                .into_response();
        }
    };

    if trades[idx].status != "open" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("trade already {}", trades[idx].status)})),
        )
            .into_response();
    }

    // Snapshot current exit revenue by refetching each leg's best bid.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .user_agent(BROWSER_UA)
        .build()
        .unwrap_or_default();
    let trade = trades[idx].clone();
    let mut exit_revenue = 0.0_f64;
    for leg in &trade.legs {
        if let Some(b) = fetch_best_bid(&http, &state.clob_url, &leg.token_id).await {
            exit_revenue += b * leg.shares;
        }
    }

    let realized = exit_revenue - trade.total_cost;
    trades[idx].status = "closed_manual".to_string();
    trades[idx].close_sell_revenue = Some(exit_revenue);
    trades[idx].realized_pnl = Some(realized);
    trades[idx].closed_at = Some(chrono::Utc::now().to_rfc3339());

    // Rewrite the file atomically via temp-file + rename.
    let tmp_path = format!("{}.tmp", PAPER_TRADES_FILE);
    let mut out = String::new();
    for t in &trades {
        match serde_json::to_string(t) {
            Ok(s) => {
                out.push_str(&s);
                out.push('\n');
            }
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("serialize failed: {e}")})),
                )
                    .into_response();
            }
        }
    }
    if let Err(e) = tokio::fs::write(&tmp_path, out).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("write failed: {e}")})),
        )
            .into_response();
    }
    if let Err(e) = tokio::fs::rename(&tmp_path, PAPER_TRADES_FILE).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("rename failed: {e}")})),
        )
            .into_response();
    }

    Json(serde_json::json!({
        "closed": true,
        "id": req.id,
        "exit_revenue": exit_revenue,
        "realized_pnl": realized,
    }))
    .into_response()
}

#[derive(Serialize)]
struct PaperTradeWithPnl {
    #[serde(flatten)]
    trade: PaperTrade,
    /// Current bid on each leg's YES (what we could sell it for right now).
    current_bids: Vec<Option<f64>>,
    /// Sum of (current_bid × shares) across legs — hypothetical sell-all revenue.
    current_sell_revenue: f64,
    /// `current_sell_revenue - total_cost`.
    unrealized_pnl: f64,
    /// Best-case payout at resolution: max shares across legs (only one can win).
    max_payout_at_resolution: f64,
    /// Profit at best case: max_payout - total_cost.
    profit_if_any_leg_wins: f64,
    /// Loss at worst case: -total_cost.
    loss_if_all_legs_lose: f64,
    /// Live lifecycle signal:
    /// - "exit_opportunity": `current_sell_revenue > total_cost` — you can sell now for locked profit
    /// - "open":             still waiting on resolution, sell-now would lose
    live_status: String,
}

async fn api_paper_trades(State(state): State<AppState>) -> impl IntoResponse {
    let mut trades: Vec<PaperTrade> = match tokio::fs::read_to_string(PAPER_TRADES_FILE).await {
        Ok(s) => s
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<PaperTrade>(l).ok())
            .collect(),
        Err(_) => Vec::new(),
    };

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .user_agent(BROWSER_UA)
        .build()
        .unwrap_or_default();

    // Cache Gamma event lookups by slug so we don't fetch the same event
    // repeatedly when multiple trades reference it.
    let mut event_cache: std::collections::HashMap<String, Option<serde_json::Value>> =
        std::collections::HashMap::new();
    let mut dirty = false;

    // Resolution detection: for each open trade, fetch the event and see if
    // one of its child markets has resolved (outcomePrices == ["1","0"] →
    // that market's YES won). If so, flip our trade to "resolved" and compute
    // realized_pnl. Persist the state change back to disk so we don't repeat work.
    for t in &mut trades {
        if t.status != "open" {
            continue;
        }
        let event_opt = if let Some(cached) = event_cache.get(&t.market_slug) {
            cached.clone()
        } else {
            let url = format!("{}/events/slug/{}", state.gamma_url, t.market_slug);
            let fetched: Option<serde_json::Value> = match http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => resp.json().await.ok(),
                _ => None,
            };
            event_cache.insert(t.market_slug.clone(), fetched.clone());
            fetched
        };
        let Some(event) = event_opt else { continue };

        // Find the winning bracket by scanning child markets for
        // outcomePrices == ["1","0"] (YES pays $1).
        let winner_label: Option<String> = event
            .get("markets")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                arr.iter().find_map(|m| {
                    let prices = m
                        .get("outcomePrices")
                        .and_then(|v| v.as_str())
                        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())?;
                    let yes_price: f64 = prices.first()?.parse().ok()?;
                    if yes_price >= 0.99 {
                        m.get("groupItemTitle")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                })
            });

        if let Some(winner) = winner_label {
            // Resolution detected. Realized = $1 × shares if we held that leg, else 0.
            let winning_leg_payout: f64 = t
                .legs
                .iter()
                .find(|l| l.bracket_label == winner)
                .map(|l| l.shares)
                .unwrap_or(0.0);
            let realized = winning_leg_payout - t.total_cost;
            t.status = "resolved".to_string();
            t.close_sell_revenue = Some(winning_leg_payout);
            t.realized_pnl = Some(realized);
            t.closed_at = Some(chrono::Utc::now().to_rfc3339());
            t.winning_bracket = Some(winner);
            dirty = true;
        }
    }

    // Persist any resolutions we just detected.
    if dirty {
        let tmp_path = format!("{}.tmp", PAPER_TRADES_FILE);
        let mut out = String::new();
        for t in &trades {
            if let Ok(s) = serde_json::to_string(t) {
                out.push_str(&s);
                out.push('\n');
            }
        }
        if tokio::fs::write(&tmp_path, out).await.is_ok() {
            let _ = tokio::fs::rename(&tmp_path, PAPER_TRADES_FILE).await;
        }
    }

    let mut enriched: Vec<PaperTradeWithPnl> = Vec::new();
    for t in trades {
        let mut current_bids: Vec<Option<f64>> = Vec::new();
        let mut current_sell_revenue = 0.0_f64;
        // Only fetch live bids for open trades — closed/resolved don't need them.
        if t.status == "open" {
            for leg in &t.legs {
                let bid = fetch_best_bid(&http, &state.clob_url, &leg.token_id).await;
                if let Some(b) = bid {
                    current_sell_revenue += b * leg.shares;
                }
                current_bids.push(bid);
            }
        } else {
            current_bids = t.legs.iter().map(|_| None).collect();
        }
        let unrealized_pnl = current_sell_revenue - t.total_cost;
        let max_payout_at_resolution = t
            .legs
            .iter()
            .map(|l| l.shares)
            .fold(0.0_f64, f64::max);
        let profit_if_any_leg_wins = max_payout_at_resolution - t.total_cost;
        let loss_if_all_legs_lose = -t.total_cost;
        let live_status = match t.status.as_str() {
            "open" if unrealized_pnl > 0.0 => "exit_opportunity".to_string(),
            other => other.to_string(),
        };
        enriched.push(PaperTradeWithPnl {
            trade: t,
            current_bids,
            current_sell_revenue,
            unrealized_pnl,
            max_payout_at_resolution,
            profit_if_any_leg_wins,
            loss_if_all_legs_lose,
            live_status,
        });
    }

    // Newest first
    enriched.sort_by(|a, b| b.trade.created_at.cmp(&a.trade.created_at));
    Json(serde_json::json!({ "trades": enriched }))
}

async fn paper_trades_html() -> Html<String> {
    Html(r##"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Paper trades</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 1em; background: #0d1117; color: #c9d1d9; }
    h1 { margin: 0 0 0.5em; }
    nav a { color: #58a6ff; margin-right: 1em; text-decoration: none; }
    .meta { color: #8b949e; font-size: 0.9em; margin: 0.5em 0 1em; }
    .trade { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 0.75em 1em; margin-bottom: 1em; }
    .trade-title { font-weight: 600; font-size: 1.02em; }
    .trade-sub { color: #8b949e; font-size: 0.85em; margin: 0.3em 0; }
    table { border-collapse: collapse; width: 100%; font-size: 0.9em; margin-top: 0.5em; }
    th, td { padding: 0.3em 0.5em; text-align: right; border-bottom: 1px solid #21262d; }
    th { color: #8b949e; font-weight: normal; }
    th:first-child, td:first-child { text-align: left; }
    .pnl-pos { color: #7ee2a8; }
    .pnl-neg { color: #ffa198; }
    .regime-badge { background: #1f2933; color: #8b949e; padding: 0.1em 0.4em; border-radius: 3px; font-size: 0.75em; }
  </style>
</head>
<body>
  <nav><a href="/">Markets</a> <a href="/weather">Weather</a> <a href="/observations">Observations</a> <a href="/paper-trades">Paper Trades</a></nav>
  <h1>Paper Trades</h1>
  <div class="meta" id="meta">Loading…</div>
  <div id="last-updated" style="color:#8b949e; font-size:0.85em; margin-bottom:0.75em"></div>
  <div id="trades"></div>
<script>
let lastFetch = null;

async function load() {
  const r = await fetch('/api/paper-trades');
  const d = await r.json();
  lastFetch = Date.now();
  const meta = document.getElementById('meta');
  const el = document.getElementById('trades');
  const trades = d.trades || [];
  const totalCost = trades.reduce((s,t) => s + t.total_cost, 0);
  const totalPnl = trades.reduce((s,t) => s + t.unrealized_pnl, 0);
  const exitOpps = trades.filter(t => t.live_status === 'exit_opportunity').length;
  const pnlClass = totalPnl >= 0 ? 'pnl-pos' : 'pnl-neg';
  const sign = totalPnl >= 0 ? '+' : '';

  let exitBadge = '';
  if (exitOpps > 0) {
    exitBadge = ` · <span style="background:#238636;color:white;padding:0.1em 0.5em;border-radius:3px;font-weight:600">${exitOpps} EXIT OPPORTUNITY${exitOpps>1?'s':''}</span>`;
  }

  meta.innerHTML = trades.length === 0
    ? 'No paper trades yet. Select brackets on the <a href="/weather" style="color:#58a6ff">Weather tab</a> to create one.'
    : `${trades.length} trade${trades.length>1?'s':''} · total cost $${totalCost.toFixed(2)} · <span class="${pnlClass}">sell-now P&amp;L ${sign}$${totalPnl.toFixed(2)}</span>${exitBadge}`;

  el.innerHTML = trades.map(t => {
    const pnlClass = t.unrealized_pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    const sign = t.unrealized_pnl >= 0 ? '+' : '';
    const exitBanner = t.live_status === 'exit_opportunity'
      ? `<div style="background:#238636;color:white;padding:0.4em 0.8em;border-radius:4px;margin-bottom:0.5em;font-weight:600">
          ⚡ EXIT NOW — locked profit if you sell at current bids: +$${t.unrealized_pnl.toFixed(2)}
         </div>`
      : '';

    const legsHtml = t.legs.map((leg, i) => {
      const bid = t.current_bids[i];
      const bidStr = bid == null ? '—' : `$${bid.toFixed(3)}`;
      const leg_revenue = bid == null ? 0 : bid * leg.shares;
      const leg_cost = leg.ask_at_buy * leg.shares;
      const legPnl = leg_revenue - leg_cost;
      const legSign = legPnl >= 0 ? '+' : '';
      const legCls = legPnl >= 0 ? 'pnl-pos' : 'pnl-neg';
      const probStr = leg.model_prob_at_buy != null ? `${(leg.model_prob_at_buy*100).toFixed(1)}%` : '—';
      return `<tr>
        <td>${leg.bracket_label}</td>
        <td>${leg.shares}</td>
        <td>$${leg.ask_at_buy.toFixed(3)}</td>
        <td>${probStr}</td>
        <td>$${leg_cost.toFixed(2)}</td>
        <td>${bidStr}</td>
        <td class="${legCls}">${legSign}$${legPnl.toFixed(2)}</td>
      </tr>`;
    }).join('');
    const modelProb = t.model_win_prob_at_buy != null ? `${(t.model_win_prob_at_buy*100).toFixed(1)}%` : '—';
    const created = new Date(t.created_at).toLocaleString();
    const profitIfWin = t.profit_if_any_leg_wins;
    const lossIfLose = t.loss_if_all_legs_lose;
    const cardBorder = t.live_status === 'exit_opportunity'
      ? 'border:2px solid #2ea043'
      : 'border:1px solid #30363d';

    // Lifecycle state and buttons.
    const isOpen = t.status === 'open';
    const statusBadge = t.status === 'open' ? ''
      : t.status === 'closed_manual'
        ? `<span style="background:#30363d;color:#c9d1d9;padding:0.1em 0.4em;border-radius:3px;font-size:0.75em">CLOSED MANUALLY</span>`
        : t.status === 'resolved'
          ? `<span style="background:#1a4d2e;color:#7ee2a8;padding:0.1em 0.4em;border-radius:3px;font-size:0.75em">RESOLVED</span>`
          : `<span style="background:#30363d;color:#c9d1d9;padding:0.1em 0.4em;border-radius:3px;font-size:0.75em">${t.status.toUpperCase()}</span>`;

    // When closed, show realized P&L + close timestamp instead of live sell-now.
    let closedBlock = '';
    if (!isOpen) {
      const rp = t.realized_pnl;
      const rpCls = rp != null && rp >= 0 ? 'pnl-pos' : 'pnl-neg';
      const rpSign = rp != null && rp >= 0 ? '+' : '';
      const rpStr = rp != null ? `${rpSign}$${rp.toFixed(2)}` : '—';
      const csr = t.close_sell_revenue != null ? `$${t.close_sell_revenue.toFixed(2)}` : '—';
      const winnerStr = t.winning_bracket ? ` · winner <b>${t.winning_bracket}</b>` : '';
      const closedAt = t.closed_at ? new Date(t.closed_at).toLocaleString() : '—';
      closedBlock = `<div style="background:#0d2818;color:#c9d1d9;padding:0.4em 0.8em;border-radius:4px;margin-bottom:0.5em;font-size:0.9em">
        Closed at ${closedAt}${winnerStr} · exit revenue ${csr} · realized P&amp;L <b class="${rpCls}">${rpStr}</b>
      </div>`;
    }

    const closeBtn = isOpen
      ? `<button onclick="closeTrade('${t.id}')" style="background:#21262d;color:#c9d1d9;border:1px solid #30363d;padding:0.3em 0.8em;border-radius:4px;cursor:pointer;font-size:0.85em;margin-left:auto">Close (snapshot at current bids)</button>`
      : '';

    const modeTag = (t.mode === 'live')
      ? `<span style="background:#da3633;color:white;padding:0.1em 0.4em;border-radius:3px;font-size:0.75em;font-weight:600">LIVE</span>`
      : `<span style="background:#1f2933;color:#8b949e;padding:0.1em 0.4em;border-radius:3px;font-size:0.75em">PAPER</span>`;
    return `<div class="trade" style="${cardBorder}${!isOpen ? ';opacity:0.75' : ''}">
      <div class="trade-title" style="display:flex;align-items:center;gap:0.5em">
        <span>${t.market_question}</span>
        ${modeTag}
        <span class="regime-badge">${t.regime}</span>
        ${statusBadge}
        ${closeBtn}
      </div>
      <div class="trade-sub">Entered ${created} · Model P(win at entry) ${modelProb}</div>
      ${closedBlock}
      ${isOpen ? exitBanner : ''}
      <div style="display:flex;gap:2em;font-size:0.9em;margin:0.5em 0;flex-wrap:wrap">
        <div>Cost: <b>$${t.total_cost.toFixed(2)}</b></div>
        <div>Max payout (any leg wins): <b>$${t.max_payout_at_resolution.toFixed(2)}</b></div>
        <div>Profit if any wins: <b class="pnl-pos">+$${profitIfWin.toFixed(2)}</b></div>
        <div>Loss if all lose: <b class="pnl-neg">$${lossIfLose.toFixed(2)}</b></div>
        ${isOpen ? `<div>Sell-now revenue: <b>$${t.current_sell_revenue.toFixed(2)}</b></div>
        <div>Sell-now P&amp;L: <b class="${pnlClass}">${sign}$${t.unrealized_pnl.toFixed(2)}</b></div>` : ''}
      </div>
      <table>
        <tr><th>Bracket</th><th>Shares</th><th>Ask at buy</th><th>Model P</th><th>Leg cost</th><th>Current bid</th><th>Leg sell-now P&amp;L</th></tr>
        ${legsHtml}
      </table>
    </div>`;
  }).join('');

  tickAge();
}

function tickAge() {
  if (!lastFetch) return;
  const s = Math.round((Date.now() - lastFetch) / 1000);
  document.getElementById('last-updated').textContent = `updated ${s}s ago · auto-refresh every 15s`;
}

async function closeTrade(id) {
  if (!confirm('Close this trade at current bids? This snapshots the exit P&L and marks it closed (cannot be undone).')) return;
  try {
    const r = await fetch('/api/paper-trade/close', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    const j = await r.json();
    if (j.closed) {
      await load();
    } else {
      alert(`Close failed: ${j.error || 'unknown'}`);
    }
  } catch (e) {
    alert(`Close failed: ${e}`);
  }
}

load();
setInterval(load, 15000);
setInterval(tickAge, 1000);
</script>
</body>
</html>"##.to_string())
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
  <a href="/weather">Weather</a>
  <a href="/observations">Observations</a>
  <a href="/paper-trades">Paper Trades</a>
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
    let current_mode = *state.scan_mode.read().await;
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

        let q_display = if m.question.chars().count() > 90 {
            let end = m.question.char_indices().nth(87).map(|(i, _)| i).unwrap_or(m.question.len());
            format!("{}...", &m.question[..end])
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
            cat = {
                let end = m.category.char_indices().nth(15).map(|(i, _)| i).unwrap_or(m.category.len());
                &m.category[..end]
            },
            neg = if m.is_neg_risk { " | neg_risk" } else { "" },
        ));
    }

    // Activity log moved to /trades — only banners are rendered here.
    let geoblocked = state.geoblocked.load(Ordering::Relaxed);
    let is_scanning = state.scanning.load(Ordering::Relaxed);
    let geoblock_banner = if geoblocked {
        r#"<div class="geo-banner">⚠ CLOB API geoblock detected — minting is disabled. Enable a VPN, then click <button class="action-btn" style="background:#2ecc71" onclick="clearGeoblock()">Clear after VPN</button></div>"#.to_string()
    } else {
        String::new()
    };
    let scanning_banner = if is_scanning {
        r#"<div id="scanning-banner" class="scanning-banner">Scanning markets... <span id="scan-spinner">⟳</span></div>"#.to_string()
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
  .scanning-banner {{ background: #1a2a3a; border: 1px solid #388bfd; border-radius: 6px; padding: 10px 16px; margin: 8px 0; color: #79c0ff; font-size: 0.88em; }}
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
  .rescan-bar {{ display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }}
</style>
</head>
<body>
<div class="dashboard-wrap">
<div class="main-col">
<h1>Harvester Dashboard</h1>
<div class="nav">
  <a href="/" class="active">Markets</a>
  <a href="/trades">Trades</a>
  <a href="/weather">Weather</a>
  <a href="/observations">Observations</a>
  <a href="/paper-trades">Paper Trades</a>
</div>
<div class="subtitle">Mode: <b style="color:{mode_color}">{mode}</b> | Buy limit: ${max_buy} | Scan: <b style="color:{scan_color}">{scan_label}</b></div>

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

{scanning_banner}
{geoblock_banner}

<h2>Markets</h2>
<div class="rescan-bar">
  <select id="scan-mode-select" class="page-btn">
    <option value="near_expiry"{ne_sel}>Near Expiry</option>
    <option value="resolution_window"{rw_sel}>Resolution Window (±hours)</option>
  </select>
  <button id="rescan-btn" class="action-btn" onclick="rescan()" style="background:#1f6feb;border-color:#388bfd;color:#fff">Rescan</button>
  <span id="rescan-status" style="font-size:0.8em;color:#8b949e"></span>
</div>
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

// ── Rescan ─────────────────────────────────────────────────────────────
async function rescan() {{
  const btn = document.getElementById('rescan-btn');
  const status = document.getElementById('rescan-status');
  const mode = document.getElementById('scan-mode-select').value;
  btn.textContent = 'Scanning...';
  btn.disabled = true;
  status.textContent = '';
  try {{
    const resp = await fetch('/api/rescan', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{mode: mode}}),
    }});
    const data = await resp.json();
    if (data.ok) {{
      status.textContent = `Found ${{data.count}} markets`;
      location.reload();
    }} else {{
      status.textContent = 'Error: ' + (data.error || 'unknown');
      btn.textContent = 'Rescan';
      btn.disabled = false;
    }}
  }} catch (e) {{
    status.textContent = 'Request failed: ' + e;
    btn.textContent = 'Rescan';
    btn.disabled = false;
  }}
}}

// ── Init ───────────────────────────────────────────────────────────────
renderPage();

// Poll /api/status while scanning banner is visible; reload when done.
(function() {{
  const banner = document.getElementById('scanning-banner');
  if (!banner) return;
  const interval = setInterval(async () => {{
    try {{
      const r = await fetch('/api/status');
      const d = await r.json();
      if (!d.scanning) {{
        clearInterval(interval);
        location.reload();
      }}
    }} catch (_) {{}}
  }}, 2000);
}})();
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
        scanning_banner = scanning_banner,
        scan_label = match current_mode {
            ScanMode::NearExpiry => "near-expiry",
            ScanMode::ResolutionWindow => "resolution-window",
        },
        scan_color = match current_mode {
            ScanMode::NearExpiry => "#8b949e",
            ScanMode::ResolutionWindow => "#58a6ff",
        },
        ne_sel = if current_mode == ScanMode::NearExpiry { " selected" } else { "" },
        rw_sel = if current_mode == ScanMode::ResolutionWindow { " selected" } else { "" },
    );

    Html(html)
}
