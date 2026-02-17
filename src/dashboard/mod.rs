//! Axum-based monitoring dashboard for the trading engine.
//!
//! Provides:
//!   GET /                 → HTML dashboard (auto-refresh 5s)
//!   GET /api/summary      → JSON ExposureSummary
//!   GET /api/orders       → JSON list of open orders
//!   GET /api/positions    → JSON list of positions
//!   GET /api/fills        → JSON list of recent fills
//!   GET /api/pnl          → JSON PnL totals

use crate::anomaly::AnomalyDetector;
use crate::store::{ExposureSummary, StateStore};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// Shared state for the dashboard routes.
#[derive(Clone)]
pub struct DashboardState {
    pub store: Arc<Mutex<StateStore>>,
    pub anomaly_detector: Option<Arc<Mutex<AnomalyDetector>>>,
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
        .route("/api/anomalies", get(api_anomalies))
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
) -> Result<Json<ExposureSummary>, StatusCode> {
    let mut store = state.store.lock().await;
    store
        .get_exposure_summary()
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_orders(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    match store.get_all_open_orders().await {
        Ok(orders) => Json(orders).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn api_positions(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    match store.get_all_positions().await {
        Ok(positions) => Json(positions).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn api_fills(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    match store.get_recent_fills(100).await {
        Ok(fills) => Json(fills).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn api_pnl(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    let total = store.get_total_pnl().await.unwrap_or_default();
    let daily = store.get_daily_pnl().await.unwrap_or_default();
    Json(serde_json::json!({
        "total_pnl": total.to_string(),
        "daily_pnl": daily.to_string(),
    }))
    .into_response()
}

async fn api_anomalies(State(state): State<DashboardState>) -> impl IntoResponse {
    if let Some(ref det) = state.anomaly_detector {
        let detector = det.lock().await;
        let anomalies: Vec<_> = detector.recent_anomalies().into_iter().rev().cloned().collect();
        Json(anomalies).into_response()
    } else {
        Json(serde_json::json!([])).into_response()
    }
}

// --- HTML Dashboard ---

async fn dashboard_html(State(state): State<DashboardState>) -> Html<String> {
    let mut store = state.store.lock().await;
    let summary = store
        .get_exposure_summary()
        .await
        .unwrap_or_else(|_| ExposureSummary {
            total_open_orders: 0,
            total_positions: 0,
            total_exposure_usd: "0".to_string(),
            total_pnl: "0".to_string(),
            daily_pnl: "0".to_string(),
            recent_fills: vec![],
        });

    let positions = store.get_all_positions().await.unwrap_or_default();
    let orders = store.get_all_open_orders().await.unwrap_or_default();

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

    // Build anomaly table rows
    let anomaly_rows: String = if let Some(ref det) = state.anomaly_detector {
        let detector = det.lock().await;
        let anomalies: Vec<_> = detector.recent_anomalies().into_iter().rev().take(30).collect();
        if anomalies.is_empty() {
            "<tr><td colspan=\"7\" style=\"text-align:center;color:#666\">No anomalies detected yet</td></tr>"
                .to_string()
        } else {
            anomalies
                .iter()
                .map(|a| {
                    let sev_color = match a.severity {
                        crate::anomaly::Severity::High => "#e74c3c",
                        crate::anomaly::Severity::Medium => "#f39c12",
                        crate::anomaly::Severity::Low => "#3498db",
                    };
                    format!(
                        "<tr><td style=\"color:{}\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                        sev_color,
                        a.severity,
                        a.kind,
                        &a.market_question[..a.market_question.len().min(45)],
                        a.price.as_deref().unwrap_or("-"),
                        a.size.as_deref().unwrap_or("-"),
                        a.detail,
                        a.detected_at.chars().take(19).collect::<String>(),
                    )
                })
                .collect()
        }
    } else {
        "<tr><td colspan=\"7\" style=\"text-align:center;color:#666\">Anomaly detector disabled</td></tr>"
            .to_string()
    };

    let anomaly_count = if let Some(ref det) = state.anomaly_detector {
        det.lock().await.anomalies.len()
    } else {
        0
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
  .auto {{ color: #484f58; font-size: 0.7em; margin-top: 15px; }}
</style>
</head>
<body>
<h1>Surebet Trading Engine</h1>

<div class="cards">
  <div class="card">
    <div class="label">Open Orders</div>
    <div class="value">{open_orders}</div>
  </div>
  <div class="card">
    <div class="label">Positions</div>
    <div class="value">{positions}</div>
  </div>
  <div class="card">
    <div class="label">Exposure</div>
    <div class="value">${exposure}</div>
  </div>
  <div class="card">
    <div class="label">Daily PnL</div>
    <div class="value" style="color:{daily_color}">${daily_pnl}</div>
  </div>
  <div class="card">
    <div class="label">Total PnL</div>
    <div class="value" style="color:{total_color}">${total_pnl}</div>
  </div>
  <div class="card">
    <div class="label">Anomalies</div>
    <div class="value" style="color:#f39c12">{anomaly_count}</div>
  </div>
</div>

<h2>Anomalies (Suspicious Activity)</h2>
<table>
  <tr><th>Severity</th><th>Type</th><th>Market</th><th>Price</th><th>Size</th><th>Detail</th><th>Time</th></tr>
  {anomaly_rows}
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

<div class="auto">Auto-refresh 5s | API: /api/summary, /api/orders, /api/positions, /api/fills, /api/pnl, /api/anomalies</div>
</body>
</html>"#,
        open_orders = summary.total_open_orders,
        positions = summary.total_positions,
        exposure = summary.total_exposure_usd,
        daily_pnl = summary.daily_pnl,
        total_pnl = summary.total_pnl,
        daily_color = pnl_color(&summary.daily_pnl),
        total_color = pnl_color(&summary.total_pnl),
        anomaly_count = anomaly_count,
        anomaly_rows = anomaly_rows,
        position_rows = position_rows,
        order_rows = order_rows,
        fill_rows = fill_rows,
    );

    Html(html)
}
