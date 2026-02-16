//! Polymarket Real-Time Data Socket (RTDS) connector.
//!
//! Connects to wss://ws-live-data.polymarket.com and streams
//! trade executions, order matches, and other live activity.

use crate::ws::WsError;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const PING_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_BASE: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// A trade execution observed on the RTDS feed.
#[derive(Debug, Clone, Deserialize)]
pub struct TradeEvent {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub timestamp: String,
}

/// An order match observed on the RTDS feed.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderMatchEvent {
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub timestamp: String,
}

/// Events emitted by the RTDS connector.
#[derive(Debug, Clone)]
pub enum RtdsEvent {
    Trade(TradeEvent),
    OrderMatch(OrderMatchEvent),
    CryptoPrice { symbol: String, price: String },
    Connected,
    Disconnected,
    RawEvent(String),
}

/// Subscription filter for RTDS topics.
#[derive(Debug, Clone, Serialize)]
pub struct RtdsSubscription {
    pub topic: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
}

/// RTDS WebSocket client for streaming trades and activity.
pub struct RtdsWsClient {
    ws_url: String,
    event_tx: mpsc::UnboundedSender<RtdsEvent>,
}

impl RtdsWsClient {
    pub fn new(ws_url: String, event_tx: mpsc::UnboundedSender<RtdsEvent>) -> Self {
        Self { ws_url, event_tx }
    }

    /// Subscribe to RTDS topics. Each subscription specifies a topic
    /// (e.g., "activity") and event type (e.g., "trades", "*").
    pub async fn subscribe(&self, subscriptions: Vec<RtdsSubscription>) -> Result<(), WsError> {
        let url = self.ws_url.clone();
        let tx = self.event_tx.clone();
        let subs = subscriptions;

        tokio::spawn(async move {
            run_rtds_connection(url, subs, tx).await;
        });

        Ok(())
    }
}

async fn run_rtds_connection(
    ws_url: String,
    subscriptions: Vec<RtdsSubscription>,
    event_tx: mpsc::UnboundedSender<RtdsEvent>,
) {
    let mut backoff = RECONNECT_BASE;

    loop {
        info!(url = %ws_url, "connecting to RTDS WebSocket");

        match connect_and_stream_rtds(&ws_url, &subscriptions, &event_tx).await {
            Ok(()) => {
                info!("RTDS WebSocket closed cleanly");
                backoff = RECONNECT_BASE;
            }
            Err(e) => {
                error!(error = %e, "RTDS WebSocket error");
            }
        }

        let _ = event_tx.send(RtdsEvent::Disconnected);

        info!(delay = ?backoff, "reconnecting to RTDS WebSocket");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect_and_stream_rtds(
    ws_url: &str,
    subscriptions: &[RtdsSubscription],
    event_tx: &mpsc::UnboundedSender<RtdsEvent>,
) -> Result<(), WsError> {
    let (ws_stream, _response) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("RTDS WebSocket connected");
    let _ = event_tx.send(RtdsEvent::Connected);

    // Send subscription message
    let sub_msg = serde_json::json!({
        "subscriptions": subscriptions,
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await?;

    info!(
        topics = subscriptions.len(),
        "subscribed to RTDS topics"
    );

    // Spawn ping task (RTDS requires 5-second pings)
    let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
    let mut ping_interval = interval(PING_INTERVAL);
    let ping_handle = tokio::spawn({
        let mut write = write;
        async move {
            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        if let Err(e) = write.send(Message::Ping(vec![])).await {
                            warn!(error = %e, "failed to send RTDS ping");
                            break;
                        }
                    }
                    _ = ping_rx.recv() => {
                        break;
                    }
                }
            }
            write
        }
    });

    // Process incoming messages
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Err(e) = process_rtds_message(&text, event_tx) {
                    warn!(error = %e, "failed to process RTDS message");
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("RTDS pong received");
            }
            Ok(Message::Close(frame)) => {
                info!(frame = ?frame, "RTDS received close frame");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "RTDS read error");
                break;
            }
        }
    }

    let _ = ping_tx.send(()).await;
    let _ = ping_handle.await;

    Ok(())
}

fn process_rtds_message(
    text: &str,
    event_tx: &mpsc::UnboundedSender<RtdsEvent>,
) -> Result<(), WsError> {
    let val: Value = serde_json::from_str(text)?;

    // RTDS messages have a "type" field indicating the event kind
    let msg_type = val.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let topic = val.get("topic").and_then(|v| v.as_str()).unwrap_or("");

    match (topic, msg_type) {
        ("activity", "trades") => {
            if let Some(data) = val.get("data") {
                if let Ok(trade) = serde_json::from_value::<TradeEvent>(data.clone()) {
                    debug!(
                        asset = %trade.asset_id,
                        price = %trade.price,
                        size = %trade.size,
                        side = %trade.side,
                        "trade"
                    );
                    let _ = event_tx.send(RtdsEvent::Trade(trade));
                }
            }
        }
        ("activity", "orders_matched") => {
            if let Some(data) = val.get("data") {
                if let Ok(matched) = serde_json::from_value::<OrderMatchEvent>(data.clone()) {
                    debug!(
                        asset = %matched.asset_id,
                        price = %matched.price,
                        size = %matched.size,
                        "order matched"
                    );
                    let _ = event_tx.send(RtdsEvent::OrderMatch(matched));
                }
            }
        }
        ("crypto_prices", _) => {
            if let Some(data) = val.get("data") {
                let symbol = data
                    .get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let price = data
                    .get("price")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _ = event_tx.send(RtdsEvent::CryptoPrice { symbol, price });
            }
        }
        _ => {
            debug!(topic = topic, msg_type = msg_type, "unhandled RTDS event");
            let _ = event_tx.send(RtdsEvent::RawEvent(text.to_string()));
        }
    }

    Ok(())
}
