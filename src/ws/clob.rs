//! Polymarket CLOB WebSocket connector.
//!
//! Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
//! and streams order book snapshots, price changes, and last trade prices.

use crate::orderbook::{
    BookSnapshotEvent, LastTradePriceEvent, OrderBookStore, PriceChangeEvent,
};
use crate::ws::WsError;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const PING_INTERVAL: Duration = Duration::from_secs(10);
const RECONNECT_BASE: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
const MAX_ASSETS_PER_CONNECTION: usize = 500;

/// Events emitted by the CLOB WebSocket for downstream processing.
#[derive(Debug, Clone)]
pub enum ClobEvent {
    BookSnapshot {
        asset_id: String,
        bid_levels: usize,
        ask_levels: usize,
    },
    PriceChange {
        asset_id: String,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
    },
    LastTradePrice {
        asset_id: String,
        price: Decimal,
    },
    Connected,
    Disconnected,
}

/// CLOB WebSocket client that maintains persistent connections
/// and feeds data into the OrderBookStore.
pub struct ClobWsClient {
    ws_url: String,
    store: OrderBookStore,
    event_tx: mpsc::UnboundedSender<ClobEvent>,
}

impl ClobWsClient {
    pub fn new(
        ws_url: String,
        store: OrderBookStore,
        event_tx: mpsc::UnboundedSender<ClobEvent>,
    ) -> Self {
        Self {
            ws_url,
            store,
            event_tx,
        }
    }

    /// Subscribe to a set of asset IDs. Spawns a connection task that
    /// auto-reconnects with exponential backoff.
    pub async fn subscribe(&self, asset_ids: Vec<String>) -> Result<(), WsError> {
        // Polymarket limits ~500 instruments per connection
        for chunk in asset_ids.chunks(MAX_ASSETS_PER_CONNECTION) {
            let url = self.ws_url.clone();
            let store = self.store.clone();
            let tx = self.event_tx.clone();
            let ids = chunk.to_vec();

            tokio::spawn(async move {
                run_clob_connection(url, ids, store, tx).await;
            });
        }
        Ok(())
    }
}

/// Persistent connection loop with auto-reconnect.
async fn run_clob_connection(
    ws_url: String,
    asset_ids: Vec<String>,
    store: OrderBookStore,
    event_tx: mpsc::UnboundedSender<ClobEvent>,
) {
    let mut backoff = RECONNECT_BASE;

    loop {
        info!(
            url = %ws_url,
            assets = asset_ids.len(),
            "connecting to CLOB WebSocket"
        );

        match connect_and_stream(&ws_url, &asset_ids, &store, &event_tx).await {
            Ok(()) => {
                info!("CLOB WebSocket closed cleanly");
                backoff = RECONNECT_BASE;
            }
            Err(e) => {
                error!(error = %e, "CLOB WebSocket error");
            }
        }

        let _ = event_tx.send(ClobEvent::Disconnected);

        info!(delay = ?backoff, "reconnecting to CLOB WebSocket");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect_and_stream(
    ws_url: &str,
    asset_ids: &[String],
    store: &OrderBookStore,
    event_tx: &mpsc::UnboundedSender<ClobEvent>,
) -> Result<(), WsError> {
    let (ws_stream, _response) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("CLOB WebSocket connected");
    let _ = event_tx.send(ClobEvent::Connected);

    // Send subscription message
    let sub_msg = serde_json::json!({
        "assets_ids": asset_ids,
        "type": "market"
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await?;

    info!(
        count = asset_ids.len(),
        "subscribed to market channel"
    );

    // Spawn ping task
    let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
    let mut ping_interval = interval(PING_INTERVAL);
    let ping_handle = tokio::spawn({
        let mut write = write;
        async move {
            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        if let Err(e) = write.send(Message::Ping(vec![])).await {
                            warn!(error = %e, "failed to send ping");
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
                if let Err(e) = process_clob_message(&text, store, event_tx) {
                    warn!(error = %e, "failed to process CLOB message");
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("received pong");
            }
            Ok(Message::Close(frame)) => {
                info!(frame = ?frame, "received close frame");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "WebSocket read error");
                break;
            }
        }
    }

    // Signal ping task to stop
    let _ = ping_tx.send(()).await;
    let _ = ping_handle.await;

    Ok(())
}

fn process_clob_message(
    text: &str,
    store: &OrderBookStore,
    event_tx: &mpsc::UnboundedSender<ClobEvent>,
) -> Result<(), WsError> {
    // Messages usually come as JSON arrays, but the server also sends single
    // objects (ack, heartbeat, error frames). Handle both.
    let events: Vec<Value> = match serde_json::from_str::<Vec<Value>>(text) {
        Ok(arr) => arr,
        Err(_) => {
            // Try as single object and wrap in a vec
            let val: Value = serde_json::from_str(text)?;
            if val.is_object() {
                vec![val]
            } else {
                return Ok(()); // ignore unexpected scalars
            }
        }
    };

    for event_val in events {
        let event_type = event_val
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match event_type {
            "book" => {
                let snapshot: BookSnapshotEvent = serde_json::from_value(event_val)?;
                let bid_levels = snapshot.bids.len();
                let ask_levels = snapshot.asks.len();
                let asset_id = snapshot.asset_id.clone();
                store.apply_book_snapshot(&snapshot);
                let _ = event_tx.send(ClobEvent::BookSnapshot {
                    asset_id,
                    bid_levels,
                    ask_levels,
                });
            }
            "price_change" => {
                let price_change: PriceChangeEvent = serde_json::from_value(event_val)?;
                let asset_id = price_change.asset_id.clone();
                let best_bid = price_change
                    .price_changes
                    .first()
                    .and_then(|c| c.best_bid.parse().ok());
                let best_ask = price_change
                    .price_changes
                    .first()
                    .and_then(|c| c.best_ask.parse().ok());
                store.apply_price_change(&price_change);
                let _ = event_tx.send(ClobEvent::PriceChange {
                    asset_id,
                    best_bid,
                    best_ask,
                });
            }
            "last_trade_price" => {
                let ltp: LastTradePriceEvent = serde_json::from_value(event_val)?;
                if let Ok(price) = ltp.price.parse::<Decimal>() {
                    store.apply_last_trade_price(&ltp.asset_id, price);
                    let _ = event_tx.send(ClobEvent::LastTradePrice {
                        asset_id: ltp.asset_id,
                        price,
                    });
                }
            }
            "tick_size_change" => {
                debug!(event = ?event_val, "tick size change");
            }
            other => {
                debug!(event_type = other, "unknown CLOB event type");
            }
        }
    }

    Ok(())
}
