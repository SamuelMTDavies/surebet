use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// A single price level in the order book.
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Snapshot of one side of the book at a point in time.
#[derive(Debug, Clone, Default)]
pub struct BookSide {
    /// BTreeMap keyed by price. For bids, iterate descending; for asks, ascending.
    pub levels: BTreeMap<Decimal, Decimal>,
}

impl BookSide {
    pub fn best(&self, is_bid: bool) -> Option<(Decimal, Decimal)> {
        if is_bid {
            self.levels.iter().next_back().map(|(p, s)| (*p, *s))
        } else {
            self.levels.iter().next().map(|(p, s)| (*p, *s))
        }
    }

    pub fn depth(&self, n_levels: usize, is_bid: bool) -> Decimal {
        if is_bid {
            self.levels.iter().rev().take(n_levels).map(|(p, s)| p * s).sum()
        } else {
            self.levels.iter().take(n_levels).map(|(p, s)| p * s).sum()
        }
    }

    pub fn apply_snapshot(&mut self, levels: &[PriceLevel]) {
        self.levels.clear();
        for level in levels {
            if level.size > Decimal::ZERO {
                self.levels.insert(level.price, level.size);
            }
        }
    }

    pub fn update_level(&mut self, price: Decimal, size: Decimal) {
        if size == Decimal::ZERO {
            self.levels.remove(&price);
        } else {
            self.levels.insert(price, size);
        }
    }
}

/// Full order book for a single asset (one side of a binary market).
#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    pub asset_id: String,
    pub bids: BookSide,
    pub asks: BookSide,
    pub last_trade_price: Option<Decimal>,
    pub last_update_ts: u64,
}

impl OrderBook {
    pub fn new(asset_id: String) -> Self {
        Self {
            asset_id,
            ..Default::default()
        }
    }

    pub fn spread(&self) -> Option<Decimal> {
        let best_bid = self.bids.best(true)?.0;
        let best_ask = self.asks.best(false)?.0;
        Some(best_ask - best_bid)
    }

    pub fn spread_pct(&self) -> Option<Decimal> {
        let best_bid = self.bids.best(true)?.0;
        let best_ask = self.asks.best(false)?.0;
        if best_bid == Decimal::ZERO {
            return None;
        }
        let mid = (best_bid + best_ask) / Decimal::from(2);
        Some((best_ask - best_bid) / mid * Decimal::from(100))
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        let best_bid = self.bids.best(true)?.0;
        let best_ask = self.asks.best(false)?.0;
        Some((best_bid + best_ask) / Decimal::from(2))
    }

    /// Total depth in USDC across top N levels on each side.
    pub fn total_depth(&self, n_levels: usize) -> Decimal {
        self.bids.depth(n_levels, true) + self.asks.depth(n_levels, false)
    }
}

/// Thread-safe store of all order books, keyed by asset_id.
#[derive(Debug, Clone)]
pub struct OrderBookStore {
    books: Arc<DashMap<String, OrderBook>>,
}

impl OrderBookStore {
    pub fn new() -> Self {
        Self {
            books: Arc::new(DashMap::new()),
        }
    }

    pub fn apply_book_snapshot(&self, event: &BookSnapshotEvent) {
        let bids: Vec<PriceLevel> = event
            .bids
            .iter()
            .filter_map(|l| parse_price_level(l))
            .collect();
        let asks: Vec<PriceLevel> = event
            .asks
            .iter()
            .filter_map(|l| parse_price_level(l))
            .collect();

        let mut book = self
            .books
            .entry(event.asset_id.clone())
            .or_insert_with(|| OrderBook::new(event.asset_id.clone()));

        book.bids.apply_snapshot(&bids);
        book.asks.apply_snapshot(&asks);
        book.last_update_ts = event.timestamp.parse().unwrap_or(0);

        debug!(
            asset_id = %event.asset_id,
            bid_levels = bids.len(),
            ask_levels = asks.len(),
            "applied book snapshot"
        );
    }

    pub fn apply_price_change(&self, event: &PriceChangeEvent) {
        if let Some(mut book) = self.books.get_mut(&event.asset_id) {
            for change in &event.price_changes {
                if let (Ok(bid), Ok(ask)) = (
                    change.best_bid.parse::<Decimal>(),
                    change.best_ask.parse::<Decimal>(),
                ) {
                    // Price change events only tell us the new best bid/ask,
                    // not the full depth. Update accordingly.
                    debug!(
                        asset_id = %event.asset_id,
                        best_bid = %bid,
                        best_ask = %ask,
                        "price change"
                    );
                }
            }
        } else {
            warn!(
                asset_id = %event.asset_id,
                "price change for unknown asset, requesting snapshot"
            );
        }
    }

    pub fn apply_last_trade_price(&self, asset_id: &str, price: Decimal) {
        if let Some(mut book) = self.books.get_mut(asset_id) {
            book.last_trade_price = Some(price);
        }
    }

    /// Apply a book update from the official SDK's WebSocket stream.
    /// The SDK already parses prices/sizes into Decimal, so no string
    /// parsing needed here.
    pub fn apply_sdk_book_update(
        &self,
        update: &polymarket_client_sdk::clob::ws::types::response::BookUpdate,
    ) {
        let asset_id = update.asset_id.to_string();

        let bids: Vec<PriceLevel> = update
            .bids
            .iter()
            .map(|l| PriceLevel {
                price: l.price,
                size: l.size,
            })
            .collect();
        let asks: Vec<PriceLevel> = update
            .asks
            .iter()
            .map(|l| PriceLevel {
                price: l.price,
                size: l.size,
            })
            .collect();

        let mut book = self
            .books
            .entry(asset_id.clone())
            .or_insert_with(|| OrderBook::new(asset_id));

        book.bids.apply_snapshot(&bids);
        book.asks.apply_snapshot(&asks);
        book.last_update_ts = update.timestamp as u64;
    }

    pub fn get_book(&self, asset_id: &str) -> Option<OrderBook> {
        self.books.get(asset_id).map(|b| b.clone())
    }

    pub fn all_asset_ids(&self) -> Vec<String> {
        self.books.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn summary(&self) -> Vec<BookSummary> {
        self.books
            .iter()
            .filter_map(|entry| {
                let book = entry.value();
                let best_bid = book.bids.best(true);
                let best_ask = book.asks.best(false);
                Some(BookSummary {
                    asset_id: book.asset_id.clone(),
                    best_bid: best_bid.map(|(p, _)| p),
                    best_ask: best_ask.map(|(p, _)| p),
                    spread: book.spread(),
                    spread_pct: book.spread_pct(),
                    depth_5: book.total_depth(5),
                    last_trade: book.last_trade_price,
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct BookSummary {
    pub asset_id: String,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub spread_pct: Option<Decimal>,
    pub depth_5: Decimal,
    pub last_trade: Option<Decimal>,
}

impl std::fmt::Display for BookSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  bid={} ask={} spread={} ({}) depth_5={} last={}",
            &self.asset_id[..12.min(self.asset_id.len())],
            self.best_bid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            self.best_ask
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            self.spread
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            self.spread_pct
                .map(|p| format!("{:.2}%", p))
                .unwrap_or_else(|| "-".into()),
            self.depth_5,
            self.last_trade
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
        )
    }
}

// --- Wire format types from Polymarket CLOB WebSocket ---

#[derive(Debug, Deserialize)]
pub struct BookSnapshotEvent {
    pub event_type: String,
    pub asset_id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub bids: Vec<WireLevel>,
    #[serde(default)]
    pub asks: Vec<WireLevel>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub hash: String,
}

#[derive(Debug, Deserialize)]
pub struct WireLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Deserialize)]
pub struct PriceChangeEvent {
    pub event_type: String,
    pub asset_id: String,
    #[serde(default)]
    pub price_changes: Vec<WirePriceChange>,
}

#[derive(Debug, Deserialize)]
pub struct WirePriceChange {
    pub best_bid: String,
    pub best_ask: String,
}

#[derive(Debug, Deserialize)]
pub struct LastTradePriceEvent {
    pub event_type: String,
    pub asset_id: String,
    pub price: String,
}

fn parse_price_level(wire: &WireLevel) -> Option<PriceLevel> {
    let price = wire.price.parse::<Decimal>().ok()?;
    let size = wire.size.parse::<Decimal>().ok()?;
    Some(PriceLevel { price, size })
}
