//! Coinbase WebSocket price feed.
//!
//! Subscribes to Coinbase Advanced Trade WebSocket for real-time
//! ticker and match (trade) data. Coinbase is one of Chainlink's
//! primary data sources for US-based price discovery.

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const COINBASE_WS: &str = "wss://ws-feed.exchange.coinbase.com";
const RECONNECT_BASE: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// A price tick from Coinbase.
#[derive(Debug, Clone)]
pub struct CoinbaseTick {
    pub product_id: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub last_price: Decimal,
    pub last_size: Decimal,
    pub time: String,
}

/// A trade from Coinbase.
#[derive(Debug, Clone)]
pub struct CoinbaseTrade {
    pub product_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: String,
    pub time: String,
    pub trade_id: u64,
}

/// Events from the Coinbase feed.
#[derive(Debug, Clone)]
pub enum CoinbaseEvent {
    Tick(CoinbaseTick),
    Trade(CoinbaseTrade),
    Connected,
    Disconnected,
}

// --- Wire types ---

#[derive(Deserialize)]
struct CoinbaseMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct TickerData {
    product_id: String,
    #[serde(default)]
    best_bid: String,
    #[serde(default)]
    best_ask: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    last_size: String,
    #[serde(default)]
    time: String,
}

#[derive(Deserialize)]
struct MatchData {
    product_id: String,
    price: String,
    size: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    time: String,
    #[serde(default)]
    trade_id: u64,
}

pub struct CoinbaseFeed {
    event_tx: mpsc::UnboundedSender<CoinbaseEvent>,
}

impl CoinbaseFeed {
    pub fn new(event_tx: mpsc::UnboundedSender<CoinbaseEvent>) -> Self {
        Self { event_tx }
    }

    /// Subscribe to ticker and matches for given product IDs.
    /// Product IDs should be like ["BTC-USD", "ETH-USD"].
    pub fn subscribe(&self, product_ids: Vec<String>) {
        let tx = self.event_tx.clone();
        let pids = product_ids;

        tokio::spawn(async move {
            run_coinbase_connection(pids, tx).await;
        });
    }
}

async fn run_coinbase_connection(
    product_ids: Vec<String>,
    event_tx: mpsc::UnboundedSender<CoinbaseEvent>,
) {
    let mut backoff = RECONNECT_BASE;

    loop {
        info!(products = ?product_ids, "connecting to Coinbase WebSocket");

        match connect_and_stream_coinbase(&product_ids, &event_tx).await {
            Ok(()) => {
                info!("Coinbase WebSocket closed cleanly");
                backoff = RECONNECT_BASE;
            }
            Err(e) => {
                error!(error = %e, "Coinbase WebSocket error");
            }
        }

        let _ = event_tx.send(CoinbaseEvent::Disconnected);
        info!(delay = ?backoff, "reconnecting to Coinbase");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect_and_stream_coinbase(
    product_ids: &[String],
    event_tx: &mpsc::UnboundedSender<CoinbaseEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = connect_async(COINBASE_WS).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("Coinbase WebSocket connected");
    let _ = event_tx.send(CoinbaseEvent::Connected);

    // Subscribe to ticker and matches channels
    let sub_msg = serde_json::json!({
        "type": "subscribe",
        "product_ids": product_ids,
        "channels": ["ticker", "matches"]
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await?;

    info!(
        products = product_ids.len(),
        "subscribed to Coinbase channels"
    );

    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                process_coinbase_message(&text, event_tx);
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(frame)) => {
                info!(frame = ?frame, "Coinbase close frame");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "Coinbase read error");
                break;
            }
        }
    }

    Ok(())
}

fn process_coinbase_message(
    text: &str,
    event_tx: &mpsc::UnboundedSender<CoinbaseEvent>,
) {
    let msg: CoinbaseMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            debug!(msg = text, "unparseable Coinbase message");
            return;
        }
    };

    match msg.msg_type.as_str() {
        "ticker" => {
            if let Ok(ticker) = serde_json::from_value::<TickerData>(msg.data) {
                if let (Ok(bid), Ok(ask), Ok(price), Ok(size)) = (
                    ticker.best_bid.parse::<Decimal>(),
                    ticker.best_ask.parse::<Decimal>(),
                    ticker.price.parse::<Decimal>(),
                    ticker.last_size.parse::<Decimal>(),
                ) {
                    let _ = event_tx.send(CoinbaseEvent::Tick(CoinbaseTick {
                        product_id: ticker.product_id,
                        bid,
                        ask,
                        last_price: price,
                        last_size: size,
                        time: ticker.time,
                    }));
                }
            }
        }
        "match" | "last_match" => {
            if let Ok(m) = serde_json::from_value::<MatchData>(msg.data) {
                if let (Ok(price), Ok(size)) = (
                    m.price.parse::<Decimal>(),
                    m.size.parse::<Decimal>(),
                ) {
                    let _ = event_tx.send(CoinbaseEvent::Trade(CoinbaseTrade {
                        product_id: m.product_id,
                        price,
                        size,
                        side: m.side,
                        time: m.time,
                        trade_id: m.trade_id,
                    }));
                }
            }
        }
        "subscriptions" | "heartbeat" => {
            debug!(msg_type = %msg.msg_type, "Coinbase system message");
        }
        "error" => {
            warn!(msg = text, "Coinbase error message");
        }
        _ => {
            debug!(msg_type = %msg.msg_type, "unhandled Coinbase message type");
        }
    }
}
