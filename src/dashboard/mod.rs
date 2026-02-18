//! Axum-based monitoring dashboard for the trading engine.
//!
//! Provides:
//!   GET /                 → HTML dashboard (auto-refresh 5s)
//!   GET /api/summary      → JSON ExposureSummary
//!   GET /api/orders       → JSON list of open orders
//!   GET /api/positions    → JSON list of positions
//!   GET /api/fills        → JSON list of recent fills
//!   GET /api/pnl          → JSON PnL totals
//!   GET /api/strategies   → JSON strategy snapshots (sniper, metrics)

use crate::crossbook::CrossbookScanner;
use crate::store::{ExposureSummary, StateStore};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// Strategy-level snapshot updated by the main event loop.
/// The dashboard reads this without needing access to the actual scanner/engine objects.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StrategySnapshot {
    pub sniper: SniperSnapshot,
    pub metrics: MetricsSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SniperSnapshot {
    pub enabled: bool,
    pub mapped_markets: usize,
    pub sniped_count: usize,
    pub recent_signals: VecDeque<SniperSignalEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SniperSignalEntry {
    pub condition_id: String,
    pub source: String,
    pub confidence: f64,
    pub action: String, // "dispatched", "ignored", "on_chain"
    pub time: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MetricsSnapshot {
    pub trades_5m: usize,
    pub profit_5m: String,
    pub avg_edge_5m: String,
    pub avg_fill_rate_5m: String,
    pub avg_latency_ms_5m: String,
    pub trades_1h: usize,
    pub profit_1h: String,
    pub utilisation_pct: String,
    pub capital_deployed: String,
    pub capital_available: String,
}

/// Max recent entries to keep in dashboard snapshots.
const MAX_RECENT: usize = 50;

impl StrategySnapshot {
    pub fn push_sniper_signal(&mut self, entry: SniperSignalEntry) {
        if self.sniper.recent_signals.len() >= MAX_RECENT {
            self.sniper.recent_signals.pop_back();
        }
        self.sniper.recent_signals.push_front(entry);
    }
}

/// Shared state for the dashboard routes.
#[derive(Clone)]
pub struct DashboardState {
    pub store: Option<Arc<Mutex<StateStore>>>,
    pub crossbook_scanner: Option<Arc<Mutex<CrossbookScanner>>>,
    pub strategy_snapshot: Arc<Mutex<StrategySnapshot>>,
}

/// Build the Axum router.
pub fn build_router(state: DashboardState) -> Router {
    Router::new()
        .route("/", get(dashboard_html))
        .route("/api/summary", get(api_summary))
        .route("/api/orders", get(api_orders))
        .route("/api/positions", get(api_positions))
        .route("/api/fills", get(api_fills))
        .route("/api/pnl", get(api_pnl))
        .route("/api/crossbook", get(api_crossbook))
        .route("/api/strategies", get(api_strategies))
        .with_state(state)
}

/// Start the dashboard server.
pub async fn serve(state: DashboardState, bind_addr: &str) -> anyhow::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(addr = bind_addr, "dashboard listening");
    axum::serve(listener, app).await?;
    Ok(())
}

// --- API Handlers ---

async fn api_summary(
    State(state): State<DashboardState>,
) -> impl IntoResponse {
    if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        match s.get_exposure_summary().await {
            Ok(summary) => Json(serde_json::to_value(summary).unwrap_or_default()).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        Json(serde_json::json!({
            "total_open_orders": 0,
            "total_positions": 0,
            "total_exposure_usd": "0",
            "total_pnl": "0",
            "daily_pnl": "0",
            "recent_fills": []
        })).into_response()
    }
}

async fn api_orders(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        match s.get_all_open_orders().await {
            Ok(orders) => Json(orders).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        Json(serde_json::json!([])).into_response()
    }
}

async fn api_positions(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        match s.get_all_positions().await {
            Ok(positions) => Json(positions).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        Json(serde_json::json!([])).into_response()
    }
}

async fn api_fills(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        match s.get_recent_fills(100).await {
            Ok(fills) => Json(fills).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        Json(serde_json::json!([])).into_response()
    }
}

async fn api_pnl(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        let total = s.get_total_pnl().await.unwrap_or_default();
        let daily = s.get_daily_pnl().await.unwrap_or_default();
        Json(serde_json::json!({
            "total_pnl": total.to_string(),
            "daily_pnl": daily.to_string(),
        }))
        .into_response()
    } else {
        Json(serde_json::json!({
            "total_pnl": "0",
            "daily_pnl": "0",
        }))
        .into_response()
    }
}

async fn api_crossbook(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref scanner) = state.crossbook_scanner {
        let s = scanner.lock().await;
        Json(serde_json::json!({
            "opportunities": s.opportunities.iter().collect::<Vec<_>>(),
            "best_odds": &s.best_odds,
        }))
        .into_response()
    } else {
        Json(serde_json::json!({"opportunities": [], "best_odds": []})).into_response()
    }
}

async fn api_strategies(State(state): State<DashboardState>) -> impl IntoResponse {
    let snap = state.strategy_snapshot.lock().await;
    Json(snap.clone()).into_response()
}

// --- HTML Dashboard ---

async fn dashboard_html(State(state): State<DashboardState>) -> Html<String> {
    let default_summary = ExposureSummary {
        total_open_orders: 0,
        total_positions: 0,
        total_exposure_usd: "0".to_string(),
        total_pnl: "0".to_string(),
        daily_pnl: "0".to_string(),
        recent_fills: vec![],
    };

    let (summary, positions, orders) = if let Some(ref store) = state.store {
        let mut s = store.lock().await;
        let sum = s.get_exposure_summary().await.unwrap_or(default_summary);
        let pos = s.get_all_positions().await.unwrap_or_default();
        let ord = s.get_all_open_orders().await.unwrap_or_default();
        (sum, pos, ord)
    } else {
        (default_summary, vec![], vec![])
    };

    // Strategy snapshot
    let strat = state.strategy_snapshot.lock().await.clone();

    // Build positions table rows
    let position_rows: String = if positions.is_empty() {
        "<tr><td colspan=\"6\" style=\"text-align:center;color:#666\">No active positions</td></tr>"
            .to_string()
    } else {
        positions
            .iter()
            .map(|p| {
                let legs_html: String = p
                    .legs
                    .iter()
                    .map(|l| format!("{}: {} @ ${}", l.label, l.inventory, l.avg_price))
                    .collect::<Vec<_>>()
                    .join("<br>");
                let pending = if p.pending_fill { "&#x23F3;" } else { "" };
                format!(
                    "<tr><td title=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    p.condition_id,
                    &p.question[..50.min(p.question.len())],
                    legs_html,
                    p.bid_sum,
                    p.edge,
                    pending,
                    p.updated_at.chars().take(19).collect::<String>(),
                )
            })
            .collect()
    };

    // Build orders table rows
    let order_rows: String = if orders.is_empty() {
        "<tr><td colspan=\"7\" style=\"text-align:center;color:#666\">No open orders</td></tr>"
            .to_string()
    } else {
        orders
            .iter()
            .map(|o| {
                format!(
                    "<tr><td title=\"{}\">{}</td><td>{}</td><td>{}</td><td>${}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    o.order_id,
                    &o.order_id[..12.min(o.order_id.len())],
                    o.label,
                    o.side,
                    o.price,
                    o.size,
                    o.status,
                    &o.question[..30.min(o.question.len())],
                )
            })
            .collect()
    };

    // Build fills table rows
    let fill_rows: String = if summary.recent_fills.is_empty() {
        "<tr><td colspan=\"7\" style=\"text-align:center;color:#666\">No fills yet</td></tr>"
            .to_string()
    } else {
        summary
            .recent_fills
            .iter()
            .take(20)
            .map(|f| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>${}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    &f.order_id[..12.min(f.order_id.len())],
                    f.label,
                    f.price,
                    f.size,
                    f.side,
                    f.strategy,
                    f.timestamp.chars().take(19).collect::<String>(),
                )
            })
            .collect()
    };

    // Build crossbook best odds table rows
    let (crossbook_rows, crossbook_count) = if let Some(ref scanner) = state.crossbook_scanner {
        let s = scanner.lock().await;
        let items: Vec<_> = s.best_odds.iter().take(30).collect();
        let count = s.opportunities.len();
        if items.is_empty() {
            (
                "<tr><td colspan=\"6\" style=\"text-align:center;color:#666\">No bookmaker odds fetched yet</td></tr>"
                    .to_string(),
                count,
            )
        } else {
            let rows: String = items
                .iter()
                .map(|opp| {
                    let legs_html: String = opp
                        .legs
                        .iter()
                        .map(|l| {
                            format!(
                                "{}: <b>{:.2}</b> @ {}",
                                l.outcome,
                                l.raw_price,
                                l.provider
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("<br>");
                    let arb_color = if opp.arb_return_pct > 0.0 {
                        "#2ecc71"
                    } else if opp.arb_return_pct > -2.0 {
                        "#f39c12"
                    } else {
                        "#8b949e"
                    };
                    let poly_link = opp
                        .poly_question
                        .as_deref()
                        .map(|q| format!("{}", &q[..q.len().min(35)]))
                        .unwrap_or_else(|| "-".to_string());
                    format!(
                        "<tr><td>{}</td><td>{}</td><td>{}</td><td style=\"color:{}\">{:.2}%</td><td>{:.4}</td><td>{}</td></tr>",
                        &opp.event[..opp.event.len().min(35)],
                        legs_html,
                        poly_link,
                        arb_color,
                        opp.arb_return_pct,
                        opp.implied_prob_sum,
                        opp.detected_at.chars().take(19).collect::<String>(),
                    )
                })
                .collect();
            (rows, count)
        }
    } else {
        (
            "<tr><td colspan=\"6\" style=\"text-align:center;color:#666\">Crossbook scanner disabled</td></tr>"
                .to_string(),
            0,
        )
    };

    // Build sniper signal rows
    let sniper_rows: String = if strat.sniper.recent_signals.is_empty() {
        "<tr><td colspan=\"5\" style=\"text-align:center;color:#666\">No sniper signals yet</td></tr>".to_string()
    } else {
        strat.sniper.recent_signals.iter().take(20).map(|s| {
            let action_color = match s.action.as_str() {
                "dispatched" => "#2ecc71",
                "on_chain" => "#3498db",
                "ignored" => "#8b949e",
                _ => "#c9d1d9",
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{:.2}</td><td style=\"color:{}\">{}</td><td>{}</td></tr>",
                &s.condition_id[..s.condition_id.len().min(12)],
                s.source,
                s.confidence,
                action_color,
                s.action,
                &s.time,
            )
        }).collect()
    };

    // PnL color
    let pnl_color = |s: &str| -> &str {
        if s.starts_with('-') {
            "#e74c3c"
        } else if s == "0" {
            "#888"
        } else {
            "#2ecc71"
        }
    };

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>Surebet Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
  h1 {{ color: #58a6ff; margin-bottom: 20px; font-size: 1.4em; }}
  h2 {{ color: #8b949e; margin: 20px 0 10px 0; font-size: 1.1em; border-bottom: 1px solid #21262d; padding-bottom: 5px; }}
  .cards {{ display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px 20px; min-width: 160px; }}
  .card .label {{ color: #8b949e; font-size: 0.75em; text-transform: uppercase; letter-spacing: 1px; }}
  .card .value {{ font-size: 1.5em; font-weight: bold; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; margin-bottom: 15px; }}
  th {{ background: #21262d; color: #8b949e; text-align: left; padding: 8px 12px; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.5px; }}
  td {{ padding: 8px 12px; border-top: 1px solid #21262d; font-size: 0.85em; }}
  tr:hover {{ background: #1c2128; }}
  .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; margin-bottom: 20px; }}
  .metric {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 14px; }}
  .metric .mlabel {{ color: #8b949e; font-size: 0.7em; text-transform: uppercase; }}
  .metric .mval {{ font-size: 1.1em; font-weight: bold; margin-top: 2px; }}
  .auto {{ color: #484f58; font-size: 0.7em; margin-top: 15px; }}
  .badge {{ display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 0.7em; font-weight: bold; }}
  .badge-on {{ background: #238636; color: #fff; }}
  .badge-off {{ background: #30363d; color: #8b949e; }}
</style>
</head>
<body>
<h1>Surebet &mdash; Resolution Sniping</h1>

<div class="cards">
  <div class="card">
    <div class="label">Sniper Signals</div>
    <div class="value" style="color:#e67e22">{sniper_signals}</div>
  </div>
  <div class="card">
    <div class="label">Sniped</div>
    <div class="value" style="color:#2ecc71">{sniped_count}</div>
  </div>
  <div class="card">
    <div class="label">Crossbook Arbs</div>
    <div class="value" style="color:#3498db">{crossbook_count}</div>
  </div>
  <div class="card">
    <div class="label">Daily PnL</div>
    <div class="value" style="color:{daily_color}">${daily_pnl}</div>
  </div>
  <div class="card">
    <div class="label">Total PnL</div>
    <div class="value" style="color:{total_color}">${total_pnl}</div>
  </div>
</div>

<h2>Metrics (5min / 1hr)</h2>
<div class="metrics-grid">
  <div class="metric"><div class="mlabel">Trades (5m)</div><div class="mval">{m_trades_5m}</div></div>
  <div class="metric"><div class="mlabel">Profit (5m)</div><div class="mval" style="color:{m_profit_5m_color}">${m_profit_5m}</div></div>
  <div class="metric"><div class="mlabel">Avg Edge (5m)</div><div class="mval">{m_edge_5m}</div></div>
  <div class="metric"><div class="mlabel">Fill Rate (5m)</div><div class="mval">{m_fill_5m}</div></div>
  <div class="metric"><div class="mlabel">Latency (5m)</div><div class="mval">{m_latency_5m}</div></div>
  <div class="metric"><div class="mlabel">Trades (1h)</div><div class="mval">{m_trades_1h}</div></div>
  <div class="metric"><div class="mlabel">Profit (1h)</div><div class="mval" style="color:{m_profit_1h_color}">${m_profit_1h}</div></div>
  <div class="metric"><div class="mlabel">Utilisation</div><div class="mval">{m_util}</div></div>
  <div class="metric"><div class="mlabel">Deployed</div><div class="mval">${m_deployed}</div></div>
  <div class="metric"><div class="mlabel">Available</div><div class="mval">${m_available}</div></div>
</div>

<h2>Resolution Sniper <span class="badge {sniper_badge}">{sniper_status}</span> ({sniper_mapped} mapped)</h2>
<table>
  <tr><th>Condition</th><th>Source</th><th>Confidence</th><th>Action</th><th>Time</th></tr>
  {sniper_rows}
</table>

<h2>Cross-Bookmaker Odds ({crossbook_count} arbs)</h2>
<table>
  <tr><th>Event</th><th>Best Legs</th><th>Polymarket</th><th>Arb %</th><th>Implied Sum</th><th>Time</th></tr>
  {crossbook_rows}
</table>

<h2>Positions</h2>
<table>
  <tr><th>Market</th><th>Legs</th><th>Bid Sum</th><th>Edge</th><th>Pending</th><th>Updated</th></tr>
  {position_rows}
</table>

<h2>Open Orders</h2>
<table>
  <tr><th>Order ID</th><th>Outcome</th><th>Side</th><th>Price</th><th>Size</th><th>Status</th><th>Market</th></tr>
  {order_rows}
</table>

<h2>Recent Fills</h2>
<table>
  <tr><th>Order</th><th>Outcome</th><th>Price</th><th>Size</th><th>Side</th><th>Strategy</th><th>Time</th></tr>
  {fill_rows}
</table>

<div class="auto">Auto-refresh 5s | API: /api/summary, /api/orders, /api/positions, /api/fills, /api/pnl, /api/crossbook, /api/strategies</div>
</body>
</html>"#,
        daily_pnl = summary.daily_pnl,
        total_pnl = summary.total_pnl,
        daily_color = pnl_color(&summary.daily_pnl),
        total_color = pnl_color(&summary.total_pnl),
        // Strategy cards
        sniper_signals = strat.sniper.recent_signals.len(),
        sniped_count = strat.sniper.sniped_count,
        // Metrics
        m_trades_5m = strat.metrics.trades_5m,
        m_profit_5m = strat.metrics.profit_5m,
        m_profit_5m_color = pnl_color(&strat.metrics.profit_5m),
        m_edge_5m = strat.metrics.avg_edge_5m,
        m_fill_5m = strat.metrics.avg_fill_rate_5m,
        m_latency_5m = strat.metrics.avg_latency_ms_5m,
        m_trades_1h = strat.metrics.trades_1h,
        m_profit_1h = strat.metrics.profit_1h,
        m_profit_1h_color = pnl_color(&strat.metrics.profit_1h),
        m_util = strat.metrics.utilisation_pct,
        m_deployed = strat.metrics.capital_deployed,
        m_available = strat.metrics.capital_available,
        // Sniper
        sniper_badge = if strat.sniper.enabled { "badge-on" } else { "badge-off" },
        sniper_status = if strat.sniper.enabled { "ON" } else { "OFF" },
        sniper_mapped = strat.sniper.mapped_markets,
        sniper_rows = sniper_rows,
        // Crossbook
        crossbook_count = crossbook_count,
        crossbook_rows = crossbook_rows,
        position_rows = position_rows,
        order_rows = order_rows,
        fill_rows = fill_rows,
    );

    Html(html)
}
