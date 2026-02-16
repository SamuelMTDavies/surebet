//! Binance WebSocket price feed.
//!
//! Subscribes to Binance's combined stream for real-time trade
//! and book ticker data. Binance typically leads Chainlink by
//! 50-200ms on price moves.

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream";
const RECONNECT_BASE: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// A price tick from Binance.
#[derive(Debug, Clone)]
pub struct BinanceTick {
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
    pub timestamp: u64,
}

/// A trade from Binance.
#[derive(Debug, Clone)]
pub struct BinanceTrade {
    pub symbol: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
    pub trade_time: u64,
}

/// Events from the Binance feed.
#[derive(Debug, Clone)]
pub enum BinanceEvent {
    Tick(BinanceTick),
    Trade(BinanceTrade),
    Connected,
    Disconnected,
}

// --- Wire types for Binance combined stream ---

#[derive(Deserialize)]
struct CombinedMessage {
    stream: String,
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct BookTickerData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "B")]
    bid_qty: String,
    #[serde(rename = "a")]
    ask_price: String,
    #[serde(rename = "A")]
    ask_qty: String,
    #[serde(rename = "T")]
    #[serde(default)]
    timestamp: u64,
}

#[derive(Deserialize)]
struct TradeData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
    #[serde(rename = "T")]
    trade_time: u64,
}

pub struct BinanceFeed {
    event_tx: mpsc::UnboundedSender<BinanceEvent>,
}

impl BinanceFeed {
    pub fn new(event_tx: mpsc::UnboundedSender<BinanceEvent>) -> Self {
        Self { event_tx }
    }

    /// Subscribe to book ticker and trade streams for given symbols.
    /// Symbols should be lowercase, e.g., ["btcusdt", "ethusdt"].
    pub fn subscribe(&self, symbols: Vec<String>) {
        let tx = self.event_tx.clone();
        let syms = symbols;

        tokio::spawn(async move {
            run_binance_connection(syms, tx).await;
        });
    }
}

async fn run_binance_connection(
    symbols: Vec<String>,
    event_tx: mpsc::UnboundedSender<BinanceEvent>,
) {
    let mut backoff = RECONNECT_BASE;

    loop {
        info!(symbols = ?symbols, "connecting to Binance WebSocket");

        match connect_and_stream_binance(&symbols, &event_tx).await {
            Ok(()) => {
                info!("Binance WebSocket closed cleanly");
                backoff = RECONNECT_BASE;
            }
            Err(e) => {
                error!(error = %e, "Binance WebSocket error");
            }
        }

        let _ = event_tx.send(BinanceEvent::Disconnected);
        info!(delay = ?backoff, "reconnecting to Binance");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect_and_stream_binance(
    symbols: &[String],
    event_tx: &mpsc::UnboundedSender<BinanceEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = connect_async(BINANCE_WS_BASE).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("Binance WebSocket connected");
    let _ = event_tx.send(BinanceEvent::Connected);

    // Build subscription: bookTicker + trade for each symbol
    let mut streams: Vec<String> = Vec::new();
    for sym in symbols {
        let s = sym.to_lowercase();
        streams.push(format!("{}@bookTicker", s));
        streams.push(format!("{}@trade", s));
    }

    let sub_msg = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await?;

    info!(streams = streams.len(), "subscribed to Binance streams");

    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                process_binance_message(&text, event_tx);
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(frame)) => {
                info!(frame = ?frame, "Binance close frame");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "Binance read error");
                break;
            }
        }
    }

    Ok(())
}

fn process_binance_message(
    text: &str,
    event_tx: &mpsc::UnboundedSender<BinanceEvent>,
) {
    // Combined stream messages have { "stream": "...", "data": {...} }
    let msg: CombinedMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            // Could be a subscription confirmation or error
            debug!(msg = text, "non-stream Binance message");
            return;
        }
    };

    if msg.stream.ends_with("@bookTicker") {
        if let Ok(ticker) = serde_json::from_value::<BookTickerData>(msg.data) {
            if let (Ok(bid), Ok(ask), Ok(bq), Ok(aq)) = (
                ticker.bid_price.parse::<Decimal>(),
                ticker.ask_price.parse::<Decimal>(),
                ticker.bid_qty.parse::<Decimal>(),
                ticker.ask_qty.parse::<Decimal>(),
            ) {
                let _ = event_tx.send(BinanceEvent::Tick(BinanceTick {
                    symbol: ticker.symbol,
                    bid,
                    ask,
                    bid_qty: bq,
                    ask_qty: aq,
                    timestamp: ticker.timestamp,
                }));
            }
        }
    } else if msg.stream.ends_with("@trade") {
        if let Ok(trade) = serde_json::from_value::<TradeData>(msg.data) {
            if let (Ok(price), Ok(qty)) = (
                trade.price.parse::<Decimal>(),
                trade.quantity.parse::<Decimal>(),
            ) {
                let _ = event_tx.send(BinanceEvent::Trade(BinanceTrade {
                    symbol: trade.symbol,
                    price,
                    quantity: qty,
                    is_buyer_maker: trade.is_buyer_maker,
                    trade_time: trade.trade_time,
                }));
            }
        }
    }
}
