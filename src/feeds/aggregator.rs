//! Multi-source price aggregator.
//!
//! Fuses price data from Binance, Coinbase, and other exchanges into
//! a single weighted-median price — replicating Chainlink's aggregation
//! methodology. This lets us predict Chainlink oracle reports before
//! they're published on-chain.

use crate::feeds::binance::BinanceEvent;
use crate::feeds::coinbase::CoinbaseEvent;
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// A price observation from a single source.
#[derive(Debug, Clone)]
pub struct PriceObservation {
    pub source: PriceSource,
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub mid: Decimal,
    pub observed_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceSource {
    Binance,
    Coinbase,
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceSource::Binance => write!(f, "Binance"),
            PriceSource::Coinbase => write!(f, "Coinbase"),
        }
    }
}

/// Aggregated price for a single asset across all sources.
#[derive(Debug, Clone)]
pub struct AggregatedPrice {
    /// The canonical symbol (e.g., "BTCUSD").
    pub symbol: String,
    /// Weighted median price across all sources.
    pub price: Decimal,
    /// Number of sources contributing.
    pub source_count: usize,
    /// Spread across sources (max - min mid).
    pub source_spread: Decimal,
    /// Per-source observations used.
    pub observations: Vec<PriceObservation>,
    /// Confidence: 1.0 if all sources agree, lower if spread is wide.
    pub confidence: Decimal,
    pub computed_at: Instant,
}

/// Events from the aggregator.
#[derive(Debug, Clone)]
pub enum AggregatorEvent {
    PriceUpdate(AggregatedPrice),
}

/// Symbol mapping between exchanges.
/// Binance uses "BTCUSDT", Coinbase uses "BTC-USD", Chainlink uses "BTC/USD".
/// We normalize to a canonical form: "BTCUSD".
fn normalize_symbol(raw: &str, source: PriceSource) -> String {
    match source {
        PriceSource::Binance => {
            // "BTCUSDT" -> "BTCUSD" (strip trailing T)
            let s = raw.to_uppercase();
            if s.ends_with("USDT") {
                format!("{}USD", &s[..s.len() - 4])
            } else {
                s
            }
        }
        PriceSource::Coinbase => {
            // "BTC-USD" -> "BTCUSD"
            raw.to_uppercase().replace('-', "")
        }
    }
}

/// Source weight for the weighted median.
/// Binance gets higher weight due to higher volume and
/// typically being the price leader.
fn source_weight(source: PriceSource) -> Decimal {
    match source {
        PriceSource::Binance => Decimal::from(3),
        PriceSource::Coinbase => Decimal::from(2),
    }
}

/// Thread-safe price store holding the latest observation per (symbol, source).
#[derive(Clone)]
pub struct PriceStore {
    /// Key: (normalized_symbol, source) -> latest observation
    observations: Arc<DashMap<(String, PriceSource), PriceObservation>>,
    /// Maximum age before an observation is considered stale.
    max_age: Duration,
}

impl PriceStore {
    pub fn new(max_age: Duration) -> Self {
        Self {
            observations: Arc::new(DashMap::new()),
            max_age,
        }
    }

    pub fn update(&self, obs: PriceObservation) {
        let key = (obs.symbol.clone(), obs.source);
        self.observations.insert(key, obs);
    }

    /// Get all fresh observations for a symbol.
    pub fn get_observations(&self, symbol: &str) -> Vec<PriceObservation> {
        let now = Instant::now();
        let mut result = Vec::new();

        for source in [PriceSource::Binance, PriceSource::Coinbase] {
            let key = (symbol.to_string(), source);
            if let Some(obs) = self.observations.get(&key) {
                if now.duration_since(obs.observed_at) < self.max_age {
                    result.push(obs.clone());
                }
            }
        }

        result
    }

    /// Compute weighted median for a symbol.
    pub fn aggregate(&self, symbol: &str) -> Option<AggregatedPrice> {
        let observations = self.get_observations(symbol);
        if observations.is_empty() {
            return None;
        }

        let price = weighted_median(&observations);

        let mids: Vec<Decimal> = observations.iter().map(|o| o.mid).collect();
        let min_mid = mids.iter().copied().min().unwrap_or(price);
        let max_mid = mids.iter().copied().max().unwrap_or(price);
        let source_spread = max_mid - min_mid;

        // Confidence: 1.0 if sources agree, degrades with spread
        // A 0.1% spread across sources = ~0.99 confidence
        let confidence = if price > Decimal::ZERO {
            let spread_pct = source_spread / price;
            (Decimal::ONE - spread_pct * Decimal::from(100))
                .max(Decimal::ZERO)
                .min(Decimal::ONE)
        } else {
            Decimal::ZERO
        };

        Some(AggregatedPrice {
            symbol: symbol.to_string(),
            price,
            source_count: observations.len(),
            source_spread,
            observations,
            confidence,
            computed_at: Instant::now(),
        })
    }
}

/// Compute weighted median from a set of observations.
/// Each source's price is weighted by source_weight().
/// The weighted median is the price where cumulative weight
/// reaches 50% of total weight.
fn weighted_median(observations: &[PriceObservation]) -> Decimal {
    if observations.is_empty() {
        return Decimal::ZERO;
    }
    if observations.len() == 1 {
        return observations[0].mid;
    }

    // Build (price, weight) pairs and sort by price
    let mut weighted: Vec<(Decimal, Decimal)> = observations
        .iter()
        .map(|obs| (obs.mid, source_weight(obs.source)))
        .collect();
    weighted.sort_by(|a, b| a.0.cmp(&b.0));

    let total_weight: Decimal = weighted.iter().map(|(_, w)| *w).sum();
    let half = total_weight / Decimal::from(2);

    let mut cumulative = Decimal::ZERO;
    for (price, weight) in &weighted {
        cumulative += weight;
        if cumulative >= half {
            return *price;
        }
    }

    // Fallback: last price
    weighted.last().map(|(p, _)| *p).unwrap_or(Decimal::ZERO)
}

/// The main aggregator that consumes exchange feeds and produces
/// aggregated prices.
pub struct PriceAggregator {
    store: PriceStore,
    event_tx: mpsc::UnboundedSender<AggregatorEvent>,
    /// Symbols we're actively tracking.
    tracked_symbols: Vec<String>,
}

impl PriceAggregator {
    pub fn new(
        event_tx: mpsc::UnboundedSender<AggregatorEvent>,
        tracked_symbols: Vec<String>,
        max_observation_age: Duration,
    ) -> Self {
        Self {
            store: PriceStore::new(max_observation_age),
            event_tx,
            tracked_symbols,
        }
    }

    pub fn store(&self) -> &PriceStore {
        &self.store
    }

    /// Process a Binance event into the price store.
    pub fn handle_binance(&self, event: &BinanceEvent) {
        if let BinanceEvent::Tick(tick) = event {
            let symbol = normalize_symbol(&tick.symbol, PriceSource::Binance);
            let mid = (tick.bid + tick.ask) / Decimal::from(2);

            let obs = PriceObservation {
                source: PriceSource::Binance,
                symbol: symbol.clone(),
                bid: tick.bid,
                ask: tick.ask,
                mid,
                observed_at: Instant::now(),
            };
            self.store.update(obs);
            self.maybe_emit(&symbol);
        }
    }

    /// Process a Coinbase event into the price store.
    pub fn handle_coinbase(&self, event: &CoinbaseEvent) {
        if let CoinbaseEvent::Tick(tick) = event {
            let symbol = normalize_symbol(&tick.product_id, PriceSource::Coinbase);
            let mid = (tick.bid + tick.ask) / Decimal::from(2);

            let obs = PriceObservation {
                source: PriceSource::Coinbase,
                symbol: symbol.clone(),
                bid: tick.bid,
                ask: tick.ask,
                mid,
                observed_at: Instant::now(),
            };
            self.store.update(obs);
            self.maybe_emit(&symbol);
        }
    }

    /// After updating the store, check if we should emit an aggregated price.
    fn maybe_emit(&self, symbol: &str) {
        if !self.tracked_symbols.iter().any(|s| s == symbol) {
            return;
        }

        if let Some(agg) = self.store.aggregate(symbol) {
            debug!(
                symbol = %agg.symbol,
                price = %agg.price,
                sources = agg.source_count,
                spread = %agg.source_spread,
                confidence = %agg.confidence,
                "aggregated price"
            );
            let _ = self.event_tx.send(AggregatorEvent::PriceUpdate(agg));
        }
    }

    /// Get current aggregated price for a symbol.
    pub fn get_price(&self, symbol: &str) -> Option<AggregatedPrice> {
        self.store.aggregate(symbol)
    }

    /// Get all current aggregated prices.
    pub fn snapshot(&self) -> Vec<AggregatedPrice> {
        self.tracked_symbols
            .iter()
            .filter_map(|s| self.store.aggregate(s))
            .collect()
    }
}
