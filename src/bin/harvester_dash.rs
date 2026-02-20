//! Harvester Web Dashboard — shows markets near end date.  Click a market
//! to fetch its order book on demand, then select a winning outcome to
//! execute a buy + enter hoover mode.
//!
//! Usage:
//!   cargo run --bin harvester_dash              # paper mode
//!   cargo run --bin harvester_dash -- --live    # live execution

use anyhow::{bail, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use chrono::Utc;
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

// ─── State ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    markets: Arc<RwLock<Vec<HarvestableMarket>>>,
    book_store: Arc<OrderBookStore>,
    clob_url: String,
    api: Option<Arc<ClobApiClient>>,
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
    action: String,
    shares: String,
    cost: String,
    profit: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct HarvestRequest {
    token_id: String,
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

    // ── Step 3: Serve dashboard ──────────────────────────────────────────

    let state = AppState {
        markets: Arc::new(RwLock::new(markets)),
        book_store,
        clob_url,
        api,
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

    // Re-fetch the book to get fresh data
    let book = state
        .book_store
        .fetch_rest_book(&state.clob_url, &req.token_id)
        .await;

    let info = match book {
        Some(ref b) => build_outcome_info(&req.label, &req.token_id, b, state.max_buy, state.min_sell),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Could not fetch book for token"})),
            )
                .into_response();
        }
    };

    let shares = info.sweepable_shares;
    let cost = info.sweepable_cost;
    let profit = info.sweepable_profit;

    if shares == Decimal::ZERO {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "No sweepable shares available"})),
        )
            .into_response();
    }

    let status = if let Some(ref api) = state.api {
        let resp = api
            .place_order(
                &req.token_id,
                &state.max_buy.to_string(),
                &shares.to_string(),
                OrderSide::Buy,
            )
            .await;

        match resp {
            Ok(r) if r.success => {
                // Start hoover in background
                let book_store = state.book_store.clone();
                let token_id = req.token_id.clone();
                let label = req.label.clone();
                let max_buy = state.max_buy;
                let api = api.clone();
                let activity = state.activity.clone();
                tokio::spawn(async move {
                    hoover_task(&book_store, &token_id, &label, max_buy, &api, &activity).await;
                });

                format!("OK — order_id: {}", r.order_id)
            }
            Ok(r) => format!("FAIL — {}", r.error_msg),
            Err(e) => format!("ERR — {}", e),
        }
    } else {
        "PAPER — no order placed".to_string()
    };

    let entry = ActivityEntry {
        time: now,
        market: req.market_question,
        outcome: req.label,
        action: "BUY".to_string(),
        shares: shares.to_string(),
        cost: format!("{:.4}", cost),
        profit: format!("{:.4}", profit),
        status,
    };

    let mut activity = state.activity.write().await;
    activity.push(entry.clone());

    Json(serde_json::json!({"ok": true, "entry": entry})).into_response()
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
                    action: "HOOVER".to_string(),
                    shares: sweepable_shares.to_string(),
                    cost: format!("{:.4}", sweepable_cost),
                    profit: format!("{:.4}", profit),
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

        let poly_url = match &m.slug {
            Some(slug) => format!("https://polymarket.com/event/{}", slug),
            None => format!("https://polymarket.com/event/{}", m.condition_id),
        };

        // Escape question for JS data attribute
        let q_escaped = m.question.replace('"', "&quot;");

        market_rows.push_str(&format!(
            r#"<div class="market-card" data-question="{q_escaped}" data-idx="{idx}">
                <div class="market-header" onclick="toggleMarket({idx})">
                    <span class="market-num">#{num}</span>
                    <a href="{poly_url}" target="_blank" class="market-question" onclick="event.stopPropagation()">{q_display}</a>
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
            poly_url = poly_url,
            q_display = q_display,
            end_str = end_str,
            n_out = m.outcomes.len(),
            cat = if m.category.len() > 15 { &m.category[..15] } else { &m.category },
            neg = if m.is_neg_risk { " | neg_risk" } else { "" },
        ));
    }

    // Activity log
    let activity_rows: String = if activity.is_empty() {
        r#"<tr><td colspan="8" style="text-align:center;color:#666">No activity yet</td></tr>"#.to_string()
    } else {
        activity.iter().rev().take(50).map(|a| {
            let status_color = if a.status.starts_with("OK") || a.status.starts_with("HOOVER OK") {
                "#2ecc71"
            } else if a.status.starts_with("PAPER") {
                "#f39c12"
            } else {
                "#e74c3c"
            };
            format!(
                r#"<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td style="color:{}">{}</td></tr>"#,
                a.time,
                if a.market.len() > 40 { &a.market[..40] } else { &a.market },
                a.outcome,
                a.action,
                a.shares,
                a.cost,
                a.profit,
                status_color,
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
  a.market-question {{ color: #58a6ff; font-weight: bold; font-size: 0.9em; text-decoration: none; }}
  a.market-question:hover {{ text-decoration: underline; }}
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
</style>
</head>
<body>
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
  <tr><th>Time</th><th>Market</th><th>Outcome</th><th>Action</th><th>Shares</th><th>Cost</th><th>Profit</th><th>Status</th></tr>
  {activity_rows}
</table>

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

// ── Expand/Collapse ────────────────────────────────────────────────────
async function toggleMarket(idx) {{
  const outcomesEl = document.getElementById('outcomes-' + idx);
  const iconEl = document.getElementById('icon-' + idx);

  if (openIdx === idx) {{
    // Collapse
    outcomesEl.style.display = 'none';
    iconEl.classList.remove('open');
    openIdx = null;
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

  // Fetch book if not cached
  if (!cachedBooks[idx]) {{
    document.getElementById('loading-' + idx).style.display = 'block';
    try {{
      const resp = await fetch('/api/book?idx=' + idx);
      const data = await resp.json();
      if (data.error) {{
        document.getElementById('loading-' + idx).textContent = 'Error: ' + data.error;
        return;
      }}
      cachedBooks[idx] = data;
      renderOutcomes(idx, data);
    }} catch (e) {{
      document.getElementById('loading-' + idx).textContent = 'Fetch failed: ' + e;
    }}
  }} else {{
    renderOutcomes(idx, cachedBooks[idx]);
  }}
}}

function renderOutcomes(idx, data) {{
  const container = document.getElementById('outcomes-' + idx);
  const outcomes = data.outcomes;
  const n = outcomes.length;
  let html = '';

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
    const btn = hasOpp
      ? `<button class="harvest-btn" onclick="harvest('${{info.token_id}}','${{info.label.replace(/'/g,"\\\\'")}}','${{data.question.replace(/'/g,"\\\\'")}}')">Select Winner</button>`
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
}}

// ── Harvest ────────────────────────────────────────────────────────────
async function harvest(tokenId, label, question) {{
  if (!confirm(`Buy "${{label}}" as winner for "${{question}}"?`)) return;
  try {{
    const resp = await fetch('/api/harvest', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ token_id: tokenId, label: label, market_question: question }}),
    }});
    const data = await resp.json();
    if (data.ok) {{
      alert(`Order submitted: ${{data.entry.status}}`);
      location.reload();
    }} else {{
      alert(`Error: ${{data.error || 'Unknown error'}}`);
    }}
  }} catch (e) {{
    alert(`Request failed: ${{e}}`);
  }}
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
