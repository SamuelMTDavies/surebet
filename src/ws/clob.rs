//! Polymarket CLOB WebSocket connector using the official SDK.
//!
//! Uses polymarket-client-sdk's clob::ws::Client for automatic reconnection,
//! subscription management, and message deserialization.

use crate::orderbook::OrderBookStore;
use futures::StreamExt;
use polymarket_client_sdk::clob::ws as sdk_ws;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use std::pin::pin;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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

/// Start CLOB WebSocket subscriptions using the official SDK.
///
/// Creates an SDK WS client, subscribes to book/price/LTP streams,
/// and spawns a single task that multiplexes all streams into our mpsc channel.
/// The SDK handles reconnection and subscription management internally.
pub fn start_clob_ws(
    store: OrderBookStore,
    event_tx: mpsc::UnboundedSender<ClobEvent>,
    asset_ids: Vec<String>,
) -> Result<(), crate::ws::WsError> {
    // Convert string asset IDs to U256
    let u256_ids: Vec<U256> = asset_ids
        .iter()
        .filter_map(|s| U256::from_str(s).ok())
        .collect();

    if u256_ids.is_empty() {
        warn!("no valid asset IDs to subscribe to");
        return Ok(());
    }

    info!(assets = u256_ids.len(), "subscribing via SDK WebSocket");

    // Single task owns the SDK client and all streams.
    // We use tokio::select! to multiplex the three streams.
    tokio::spawn(async move {
        let ws_client = sdk_ws::Client::default();

        let book_stream = match ws_client.subscribe_orderbook(u256_ids.clone()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to subscribe to orderbook");
                return;
            }
        };
        let ltp_stream = match ws_client.subscribe_last_trade_price(u256_ids.clone()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to subscribe to LTP");
                return;
            }
        };
        let price_stream = match ws_client.subscribe_prices(u256_ids) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to subscribe to prices");
                return;
            }
        };

        let mut book_stream = pin!(book_stream);
        let mut ltp_stream = pin!(ltp_stream);
        let mut price_stream = pin!(price_stream);

        let _ = event_tx.send(ClobEvent::Connected);

        loop {
            tokio::select! {
                Some(result) = book_stream.next() => {
                    match result {
                        Ok(book) => {
                            let asset_id = book.asset_id.to_string();
                            let bid_levels = book.bids.len();
                            let ask_levels = book.asks.len();
                            store.apply_sdk_book_update(&book);
                            let _ = event_tx.send(ClobEvent::BookSnapshot {
                                asset_id,
                                bid_levels,
                                ask_levels,
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "SDK book stream error");
                        }
                    }
                }
                Some(result) = ltp_stream.next() => {
                    match result {
                        Ok(ltp) => {
                            let asset_id = ltp.asset_id.to_string();
                            store.apply_last_trade_price(&asset_id, ltp.price);
                            let _ = event_tx.send(ClobEvent::LastTradePrice {
                                asset_id,
                                price: ltp.price,
                            });
                        }
                        Err(e) => {
                            debug!(error = %e, "SDK LTP stream error");
                        }
                    }
                }
                Some(result) = price_stream.next() => {
                    match result {
                        Ok(pc) => {
                            for change in &pc.price_changes {
                                let _ = event_tx.send(ClobEvent::PriceChange {
                                    asset_id: change.asset_id.to_string(),
                                    best_bid: change.best_bid,
                                    best_ask: change.best_ask,
                                });
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "SDK price stream error");
                        }
                    }
                }
                else => {
                    warn!("all SDK WS streams ended");
                    break;
                }
            }
        }

        let _ = event_tx.send(ClobEvent::Disconnected);
    });

    Ok(())
}
