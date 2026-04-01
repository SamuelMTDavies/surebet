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
use alloy::signers::local::LocalSigner;
use anyhow::{bail, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk::ctf::types::SplitPositionRequest;
use polymarket_client_sdk::types::address;
use rust_decimal::Decimal;
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
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

// ─── CTF Split wrapper ──────────────────────────────────────────────────────

/// Type-erased wrapper around `polymarket_client_sdk::ctf::Client` so we can
/// store it in `AppState` without leaking the generic provider type.
struct CtfSplitter {
    inner: Box<dyn CtfSplitTrait + Send + Sync>,
}

#[async_trait::async_trait]
trait CtfSplitTrait: Send + Sync {
    async fn split_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<String>; // returns tx hash
}

struct CtfClientWrapper<P: alloy::providers::Provider + Clone + Send + Sync + 'static> {
    client: polymarket_client_sdk::ctf::Client<P>,
}

#[async_trait::async_trait]
impl<P: alloy::providers::Provider + Clone + Send + Sync + 'static> CtfSplitTrait
    for CtfClientWrapper<P>
{
    async fn split_position(
        &self,
        condition_id: B256,
        amount_usdc_units: U256,
    ) -> anyhow::Result<String> {
        let req = SplitPositionRequest::for_binary_market(USDC, condition_id, amount_usdc_units);
        let resp = self
            .client
            .split_position(&req)
            .await
            .map_err(|e| anyhow::anyhow!("CTF split failed: {}", e))?;
        Ok(format!("{:?}", resp.transaction_hash))
    }
}

// ─── State ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    markets: Arc<RwLock<Vec<HarvestableMarket>>>,
    book_store: Arc<OrderBookStore>,
    clob_url: String,
    api: Option<Arc<ClobApiClient>>,
    ctf: Option<Arc<CtfSplitter>>,
    activity: Arc<RwLock<Vec<ActivityEntry>>>,
    max_buy: Decimal,
    min_sell: Decimal,
    live_mode: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ActivityEntry {
    time: String,
    market: String,
    outcome: String,
    strategy: String,       // "BUY" | "MINT+SELL" | "HOOVER"
    buy_shares: String,
    buy_cost: String,
    mint_cost: String,      // USDC spent minting complete sets
    sell_revenue: String,   // revenue from selling loser tokens
    net_profit: String,     // total profit accounting for all costs
    status: String,
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

    // ── Step 2b: Build CTF client for mint+sell (on-chain) ────────────────

    let ctf: Option<Arc<CtfSplitter>> = if live_mode {
        match std::env::var("POLYMARKET_PRIVATE_KEY") {
            Ok(pk) => {
                let rpc_url = std::env::var("POLYGON_HTTP_URL")
                    .unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
                match LocalSigner::from_str(&pk) {
                    Ok(signer) => {
                        let signer = signer.with_chain_id(Some(POLYGON_CHAIN_ID));
                        match ProviderBuilder::new()
                            .wallet(signer)
                            .connect(&rpc_url)
                            .await
                        {
                            Ok(provider) => {
                                match polymarket_client_sdk::ctf::Client::with_neg_risk(
                                    provider,
                                    POLYGON_CHAIN_ID,
                                ) {
                                    Ok(c) => {
                                        println!("  CTF client ready (mint+sell enabled)");
                                        Some(Arc::new(CtfSplitter {
                                            inner: Box::new(CtfClientWrapper { client: c }),
                                        }))
                                    }
                                    Err(e) => {
                                        println!("WARNING: CTF client init failed: {e}, mint+sell disabled");
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

    let state = AppState {
        markets: Arc::new(RwLock::new(markets)),
        book_store,
        clob_url,
        api,
        ctf,
        activity: Arc::new(RwLock::new(Vec::new())),
        max_buy,
        min_sell,
        live_mode,
    };

    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/markets", get(api_markets))
        .route("/api/book", get(api_book))
        .route("/api/harvest", post(api_harvest))
        .route("/api/activity", get(api_activity))
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
    let buy_profit = winner.sweepable_profit;

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

    let mint_amount = if use_mint_sell {
        min_loser_sellable
    } else {
        Decimal::ZERO
    };

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

    if let Some(ref api) = state.api {
        // ── 5a. Mint+sell if viable ─────────────────────────────────────
        if use_mint_sell && mint_amount > Decimal::ZERO {
            strategy = "MINT+SELL".to_string();
            actual_mint_cost = mint_amount; // 1 USDC per complete set

            // On-chain CTF split
            let ctf = state.ctf.as_ref().unwrap();
            // Convert Decimal to USDC base units (6 decimals)
            let usdc_units = (mint_amount * Decimal::from(1_000_000u64))
                .to_string()
                .parse::<u64>()
                .unwrap_or(0);

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

            match ctf
                .inner
                .split_position(condition_id, U256::from(usdc_units))
                .await
            {
                Ok(tx_hash) => {
                    status_parts.push(format!("MINT OK tx={tx_hash}"));

                    // Sell each loser outcome's tokens via limit orders
                    for (loser_token_id, sellable_shares, best_bid) in &loser_sell_plans {
                        // Sell up to mint_amount shares (not more than we minted)
                        let sell_size = (*sellable_shares).min(mint_amount);
                        let resp = api
                            .place_order(
                                loser_token_id,
                                &best_bid.to_string(),
                                &sell_size.to_string(),
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
                                status_parts.push(format!("SELL FAIL: {}", r.error_msg));
                            }
                            Err(e) => {
                                status_parts.push(format!("SELL ERR: {}", e));
                            }
                        }
                    }
                }
                Err(e) => {
                    status_parts.push(format!("MINT FAIL: {e}"));
                    // Fall through — still try direct buy below
                }
            }
        }

        // ── 5b. Direct buy of winner tokens from asks ───────────────────
        if buy_shares > Decimal::ZERO {
            let resp = api
                .place_order(
                    winner_token_id,
                    &state.max_buy.to_string(),
                    &buy_shares.to_string(),
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
                    let api = api.clone();
                    let activity = state.activity.clone();
                    tokio::spawn(async move {
                        hoover_task(&book_store, &token_id, &label, max_buy, &api, &activity)
                            .await;
                    });
                }
                Ok(r) => status_parts.push(format!("BUY FAIL: {}", r.error_msg)),
                Err(e) => status_parts.push(format!("BUY ERR: {}", e)),
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
    };

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
    api: &ClobApiClient,
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

                let resp = api
                    .place_order(
                        token_id,
                        &max_buy.to_string(),
                        &sweepable_shares.to_string(),
                        OrderSide::Buy,
                    )
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
                };

                let mut log = activity.write().await;
                log.push(entry);
            }
            Some(ClobEvent::Disconnected) | None => break,
            _ => {}
        }
    }
}

// ─── HTML Dashboard ──────────────────────────────────────────────────────────

async fn dashboard_html(State(state): State<AppState>) -> Html<String> {
    let markets = state.markets.read().await;
    let activity = state.activity.read().await;
    let now = Utc::now();

    // Build market rows — collapsed by default, no book data yet
    let mut market_rows = String::new();
    for (mi, m) in markets.iter().enumerate() {
        let end_str = match m.end_date {
            Some(ed) => {
                let diff = ed - now;
                let hours = diff.num_hours();
                if hours < 0 {
                    format!("<span style=\"color:#e74c3c\">{}h ago</span>", -hours)
                } else if hours < 24 {
                    format!("<span style=\"color:#f39c12\">in {}h</span>", hours)
                } else {
                    ed.format("%Y-%m-%d").to_string()
                }
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

    // Activity log
    let activity_rows: String = if activity.is_empty() {
        r#"<tr><td colspan="9" style="text-align:center;color:#666">No activity yet</td></tr>"#.to_string()
    } else {
        activity.iter().rev().take(50).map(|a| {
            let status_color = if a.status.contains("OK") {
                "#2ecc71"
            } else if a.status.contains("PAPER") {
                "#f39c12"
            } else {
                "#e74c3c"
            };
            let profit_val: f64 = a.net_profit.parse().unwrap_or(0.0);
            let profit_color = if profit_val > 0.0 { "#2ecc71" } else { "#8b949e" };
            format!(
                r#"<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td style="color:{}">{}</td><td style="color:{};max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{}">{}</td></tr>"#,
                a.time,
                if a.market.len() > 35 { &a.market[..35] } else { &a.market },
                a.outcome,
                a.strategy,
                a.buy_shares,
                a.buy_cost,
                if a.sell_revenue != "0" && a.sell_revenue != "0.0000" {
                    format!("${} sell / ${} mint", a.sell_revenue, a.mint_cost)
                } else {
                    "-".to_string()
                },
                profit_color,
                a.net_profit,
                status_color,
                a.status,
                a.status,
            )
        }).collect()
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
</div>

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

<h2>Activity Log</h2>
<table>
  <tr><th>Time</th><th>Market</th><th>Outcome</th><th>Strategy</th><th>Shares</th><th>Buy Cost</th><th>Sell/Mint</th><th>Net Profit</th><th>Status</th></tr>
  {activity_rows}
</table>
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
        market_count = markets.len(),
        trade_count = activity.len(),
        market_rows = market_rows,
        activity_rows = activity_rows,
    );

    Html(html)
}
