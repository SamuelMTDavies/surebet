//! Harvester Web Dashboard — shows markets near end date with live order book
//! data.  Select a winning outcome to execute a buy + enter hoover mode.
//!
//! Usage:
//!   cargo run --bin harvester_dash              # paper mode
//!   cargo run --bin harvester_dash -- --live    # live execution

use anyhow::{bail, Result};
use axum::extract::State;
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
    markets: Arc<RwLock<Vec<MarketWithBook>>>,
    book_store: Arc<OrderBookStore>,
    api: Option<Arc<ClobApiClient>>,
    activity: Arc<RwLock<Vec<ActivityEntry>>>,
    max_buy: Decimal,
    #[allow(dead_code)]
    min_sell: Decimal,
    live_mode: bool,
}

#[derive(Debug, Clone, Serialize)]
struct MarketWithBook {
    market: HarvestableMarket,
    outcomes: Vec<OutcomeInfo>,
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

    // ── Step 1: Scan markets ────────────────────────────────────────────────

    println!("Scanning Gamma API...");
    let markets = scan_markets(gamma_url, harvester.end_date_window_days, harvester.max_display).await?;

    if markets.is_empty() {
        bail!("No markets found matching criteria.");
    }

    // ── Step 2: Fetch books for all markets ─────────────────────────────────

    let book_store = Arc::new(OrderBookStore::new());

    println!("Fetching order books for {} markets...", markets.len());
    let mut markets_with_books = Vec::new();

    for market in &markets {
        let mut outcomes = Vec::new();
        for (i, token_id) in market.clob_token_ids.iter().enumerate() {
            let label = market.outcomes.get(i).cloned().unwrap_or_else(|| format!("Outcome {}", i));
            let book = book_store.fetch_rest_book(&clob_url, token_id).await;

            let info = match book {
                Some(ref b) => build_outcome_info(&label, token_id, b, max_buy, min_sell),
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
        markets_with_books.push(MarketWithBook {
            market: market.clone(),
            outcomes,
        });
    }

    // Markets are already sorted by scan_markets() — soonest end date first,
    // >24h-past pushed to bottom.  No need to re-sort here.
    println!("  Books fetched for {} markets (soonest end date first).", markets_with_books.len());

    // ── Step 3: Build API client ────────────────────────────────────────────

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

    // ── Step 4: Serve dashboard ─────────────────────────────────────────────

    let state = AppState {
        markets: Arc::new(RwLock::new(markets_with_books)),
        book_store,
        api,
        activity: Arc::new(RwLock::new(Vec::new())),
        max_buy,
        min_sell,
        live_mode,
    };

    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/markets", get(api_markets))
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

async fn api_harvest(
    State(state): State<AppState>,
    Json(req): Json<HarvestRequest>,
) -> impl IntoResponse {
    let now = Utc::now().format("%H:%M:%S").to_string();

    // Find outcome info
    let markets = state.markets.read().await;
    let outcome = markets
        .iter()
        .flat_map(|m| m.outcomes.iter())
        .find(|o| o.token_id == req.token_id);

    let (shares, cost, profit) = match outcome {
        Some(o) => (o.sweepable_shares, o.sweepable_cost, o.sweepable_profit),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Token not found"})),
            )
                .into_response();
        }
    };
    drop(markets);

    if shares == Decimal::ZERO {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "No sweepable shares available"})),
        )
            .into_response();
    }

    let status = if let Some(ref api) = state.api {
        // Live mode: place order
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
        // Paper mode
        "PAPER — no order placed".to_string()
    };

    // Log activity
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

    // Build market rows
    let mut market_rows = String::new();
    for (mi, mwb) in markets.iter().enumerate() {
        let m = &mwb.market;
        let end_str = match m.end_date {
            Some(ed) => {
                let diff = ed - now;
                let hours = diff.num_hours();
                if hours < -24 {
                    // >24h past — likely postponed
                    format!("<span style=\"color:#e74c3c\">{}h ago</span>", -hours)
                } else if hours < 0 {
                    // Within last 24h — could be timezone mismatch, show as "ending"
                    format!("<span style=\"color:#f39c12\">~{}h ago</span>", -hours)
                } else if hours < 24 {
                    format!("<span style=\"color:#f39c12\">in {}h</span>", hours)
                } else {
                    ed.format("%Y-%m-%d").to_string()
                }
            }
            None => "???".to_string(),
        };

        // Outcome sub-rows
        //
        // For each outcome we show:
        //   BUY side  — sweep asks (buy-winner strategy)
        //   SELL side — hit bids (sell-loser via split strategy)
        //
        // When you "Select Winner" on outcome X:
        //   1. Buy X tokens from asks (profit = shares - cost)
        //   2. Split USDC to get loser tokens for every OTHER outcome,
        //      sell those into their bids (revenue is pure profit,
        //      since the split winner token redeems at $1).
        let mut outcome_html = String::new();
        let n_outcomes = mwb.outcomes.len();

        for (oi, info) in mwb.outcomes.iter().enumerate() {
            let ask_str = info
                .best_ask
                .map(|p| format!("${}", p))
                .unwrap_or_else(|| "-".to_string());
            let bid_str = info
                .best_bid
                .map(|p| format!("${}", p))
                .unwrap_or_else(|| "-".to_string());
            let buy_pct = if info.sweepable_cost > Decimal::ZERO {
                let v = (info.sweepable_profit / info.sweepable_cost) * Decimal::from(100);
                format!("{:.2}%", v)
            } else {
                "-".to_string()
            };

            // Calculate combined profit if THIS outcome is the winner:
            //   buy_winner_profit + sum of sell revenue for ALL OTHER outcomes
            let buy_profit = info.sweepable_profit;
            let sell_losers_revenue: Decimal = mwb
                .outcomes
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != oi)
                .map(|(_, o)| o.sellable_revenue)
                .sum();
            let combined_profit = buy_profit + sell_losers_revenue;

            let profit_color = if combined_profit > Decimal::ZERO {
                "#2ecc71"
            } else {
                "#8b949e"
            };

            let has_opportunity = info.sweepable_shares > Decimal::ZERO || sell_losers_revenue > Decimal::ZERO;
            let btn = if has_opportunity {
                format!(
                    r#"<button class="harvest-btn" onclick="harvest('{}','{}','{}')">Select Winner</button>"#,
                    info.token_id,
                    info.label.replace('\'', "\\'"),
                    m.question.replace('\'', "\\'"),
                )
            } else {
                String::new()
            };

            // Show sell-loser detail only when there's something
            let sell_detail = if sell_losers_revenue > Decimal::ZERO && n_outcomes >= 2 {
                format!(
                    r#"<span style="color:#8b949e;font-size:0.8em"> + ${:.4} sell losers</span>"#,
                    sell_losers_revenue,
                )
            } else {
                String::new()
            };

            outcome_html.push_str(&format!(
                r#"<div class="outcome-row">
                    <span class="outcome-label">{label}</span>
                    <span class="outcome-ask">ask {ask}</span>
                    <span class="outcome-bid">bid {bid}</span>
                    <span class="outcome-shares">{buy_shares} buy / {sell_shares} sell</span>
                    <span class="outcome-cost">${buy_cost:.4}</span>
                    <span class="outcome-profit" style="color:{profit_color}">${combined:.4} ({buy_pct}){sell_detail}</span>
                    {btn}
                </div>"#,
                label = info.label,
                ask = ask_str,
                bid = bid_str,
                buy_shares = info.sweepable_shares,
                sell_shares = info.sellable_shares,
                buy_cost = info.sweepable_cost,
                profit_color = profit_color,
                combined = combined_profit,
                buy_pct = buy_pct,
                sell_detail = sell_detail,
                btn = btn,
            ));
        }

        let q_display = if m.question.len() > 80 {
            format!("{}...", &m.question[..77])
        } else {
            m.question.clone()
        };

        let poly_url = match &m.slug {
            Some(slug) => format!("https://polymarket.com/event/{}", slug),
            None => format!("https://polymarket.com/event/{}", m.condition_id),
        };

        market_rows.push_str(&format!(
            r#"<div class="market-card">
                <div class="market-header">
                    <span class="market-num">#{}</span>
                    <a href="{}" target="_blank" class="market-question">{}</a>
                    <span class="market-meta">{} | {} outcomes | {}{}</span>
                </div>
                <div class="outcomes">{}</div>
            </div>"#,
            mi + 1,
            poly_url,
            q_display,
            end_str,
            m.outcomes.len(),
            if m.category.len() > 15 { &m.category[..15] } else { &m.category },
            if m.is_neg_risk { " | neg_risk" } else { "" },
            outcome_html,
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
  .market-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 16px; margin-bottom: 10px; }}
  .market-header {{ display: flex; align-items: baseline; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }}
  .market-num {{ color: #8b949e; font-size: 0.8em; font-weight: bold; }}
  a.market-question {{ color: #58a6ff; font-weight: bold; font-size: 0.95em; text-decoration: none; }}
  a.market-question:hover {{ text-decoration: underline; }}
  .market-meta {{ color: #484f58; font-size: 0.75em; margin-left: auto; }}
  .outcomes {{ display: flex; flex-direction: column; gap: 4px; }}
  .outcome-row {{ display: flex; align-items: center; gap: 12px; padding: 4px 8px; background: #0d1117; border-radius: 4px; font-size: 0.85em; flex-wrap: wrap; }}
  .outcome-label {{ min-width: 140px; font-weight: bold; color: #c9d1d9; }}
  .outcome-ask {{ min-width: 80px; color: #8b949e; }}
  .outcome-bid {{ min-width: 80px; color: #8b949e; }}
  .outcome-shares {{ min-width: 120px; color: #8b949e; }}
  .outcome-cost {{ min-width: 100px; color: #8b949e; }}
  .outcome-profit {{ min-width: 140px; }}
  .harvest-btn {{ background: #238636; color: #fff; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.8em; font-family: inherit; margin-left: auto; }}
  .harvest-btn:hover {{ background: #2ea043; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }}
  th {{ background: #21262d; color: #8b949e; text-align: left; padding: 8px 12px; font-size: 0.8em; text-transform: uppercase; }}
  td {{ padding: 6px 12px; border-top: 1px solid #21262d; font-size: 0.82em; }}
  tr:hover {{ background: #1c2128; }}
  .status-bar {{ display: flex; gap: 15px; margin-bottom: 15px; flex-wrap: wrap; }}
  .status-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 8px 14px; }}
  .status-item .slabel {{ color: #8b949e; font-size: 0.7em; text-transform: uppercase; }}
  .status-item .sval {{ font-size: 1.2em; font-weight: bold; margin-top: 2px; }}
</style>
</head>
<body>
<h1>Harvester Dashboard</h1>
<div class="subtitle">Mode: <b style="color:{mode_color}">{mode}</b> | Buy limit: ${max_buy} | Polling: 30s</div>

<div class="status-bar">
  <div class="status-item">
    <div class="slabel">Markets</div>
    <div class="sval" style="color:#58a6ff">{market_count}</div>
  </div>
  <div class="status-item">
    <div class="slabel">Trades</div>
    <div class="sval" style="color:#2ecc71">{trade_count}</div>
  </div>
</div>

<h2>Markets</h2>
{market_rows}

<h2>Activity Log</h2>
<table>
  <tr><th>Time</th><th>Market</th><th>Outcome</th><th>Action</th><th>Shares</th><th>Cost</th><th>Profit</th><th>Status</th></tr>
  {activity_rows}
</table>

<script>
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

// Auto-refresh every 30s
setTimeout(() => location.reload(), 30000);
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
