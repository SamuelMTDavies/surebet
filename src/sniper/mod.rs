//! Resolution sniping engine (Strategy 1B).
//!
//! Monitors external data sources for resolution outcomes and immediately
//! sweeps Polymarket order books when an outcome becomes publicly knowable
//! but the market hasn't priced it in yet.
//!
//! Architecture:
//! - Each data source is a `ResolutionSource` that emits `ResolutionSignal`s
//! - The `SniperEngine` maps active markets to their resolution sources
//! - When a signal fires, the `SniperExecutor` sweeps the book aggressively
//!
//! Priority data sources:
//! 1. Crypto price thresholds (already have exchange feeds, sub-second latency)
//! 2. Billboard 200 (weekly, predictable timing, multi-outcome)
//! 3. Rotten Tomatoes (per-film, many brackets)
//! 4. Election/political (irregular, large notional)
//! 5. Weather/earthquake (USGS/NOAA, easy APIs)
//!
//! The SDK's `MarketResolved` WS event is used as a secondary confirmation
//! signal (the on-chain resolution), but the _primary_ signal comes from
//! polling the external data source _before_ on-chain resolution.

pub mod executor;
pub mod sources;

use crate::market::TrackedMarket;
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// A resolution signal from an external data source.
#[derive(Debug, Clone)]
pub struct ResolutionSignal {
    /// Which market this resolves.
    pub condition_id: String,
    /// The source that produced this signal.
    pub source: String,
    /// Resolved outcomes: maps outcome label → whether it won.
    /// For bracket markets (e.g. RT 70+, 75+, 80+), multiple outcomes may be resolved.
    pub outcomes: HashMap<String, OutcomeResult>,
    /// Confidence level (0.0 - 1.0). Only act on >= 0.99.
    pub confidence: f64,
    /// The raw data that caused the resolution.
    pub evidence: String,
    /// When the data was observed.
    pub observed_at: Instant,
}

/// Result for a single outcome.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutcomeResult {
    /// This outcome definitively won.
    Won,
    /// This outcome definitively lost.
    Lost,
    /// Cannot determine yet.
    Unknown,
}

/// A market-to-source mapping.
#[derive(Debug, Clone)]
pub struct MarketSourceMapping {
    pub condition_id: String,
    pub question: String,
    pub outcomes: Vec<(String, String)>, // (label, token_id)
    /// The source type that resolves this market.
    pub source_type: SourceType,
    /// Source-specific parameters (e.g., crypto symbol, chart URL slug).
    pub source_params: HashMap<String, String>,
}

/// Types of resolution data sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceType {
    /// Crypto price threshold (e.g., "Will BTC be above $100k on March 1?")
    CryptoPrice,
    /// Billboard 200 / Hot 100 charts
    Billboard,
    /// Rotten Tomatoes scores
    RottenTomatoes,
    /// Election / political results
    Election,
    /// Weather / earthquake (USGS, NOAA)
    Weather,
    /// Generic API-resolved market
    GenericApi,
    /// On-chain resolution (SDK MarketResolved event) — fallback
    OnChain,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::CryptoPrice => write!(f, "crypto"),
            SourceType::Billboard => write!(f, "billboard"),
            SourceType::RottenTomatoes => write!(f, "rotten_tomatoes"),
            SourceType::Election => write!(f, "election"),
            SourceType::Weather => write!(f, "weather"),
            SourceType::GenericApi => write!(f, "generic_api"),
            SourceType::OnChain => write!(f, "on_chain"),
        }
    }
}

/// Snipe action to take on the CLOB.
#[derive(Debug, Clone)]
pub struct SnipeAction {
    pub condition_id: String,
    pub question: String,
    /// Orders to place: (token_id, side, max_price, target_shares).
    pub orders: Vec<SnipeOrder>,
    pub signal: ResolutionSignal,
}

#[derive(Debug, Clone)]
pub struct SnipeOrder {
    pub token_id: String,
    pub label: String,
    pub side: SnipeSide,
    /// For BUY (winning): sweep asks up to this price (e.g. $0.99).
    /// For SELL (losing): hit bids above this price (e.g. $0.01).
    pub limit_price: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SnipeSide {
    /// Buy the winning outcome — sweep all asks up to limit.
    BuyWinner,
    /// Sell the losing outcome — hit all bids above limit.
    SellLoser,
}

/// Events emitted by the sniper engine.
#[derive(Debug, Clone)]
pub enum SniperEvent {
    /// A resolution signal was received from an external source.
    SignalReceived {
        condition_id: String,
        source: String,
        confidence: f64,
    },
    /// Snipe orders were generated and dispatched to the executor.
    SnipeDispatched {
        condition_id: String,
        question: String,
        num_orders: usize,
    },
    /// A market was resolved on-chain (SDK MarketResolved event).
    OnChainResolution {
        condition_id: String,
        winning_outcome: String,
    },
    /// Signal was too low confidence; ignored.
    SignalIgnored {
        condition_id: String,
        confidence: f64,
        reason: String,
    },
}

/// The sniper engine: maps markets to sources and converts signals into actions.
pub struct SniperEngine {
    store: OrderBookStore,
    event_tx: mpsc::UnboundedSender<SniperEvent>,
    /// Maps condition_id → source mapping.
    mappings: HashMap<String, MarketSourceMapping>,
    /// Tracks which markets we've already sniped (avoid double-firing).
    sniped: HashMap<String, Instant>,
    /// Minimum confidence to act on a signal.
    min_confidence: f64,
    /// Maximum price to pay for winning outcome (sweep asks up to this).
    max_buy_price: Decimal,
    /// Minimum price to sell losing outcome at (hit bids above this).
    min_sell_price: Decimal,
}

impl SniperEngine {
    pub fn new(
        store: OrderBookStore,
        event_tx: mpsc::UnboundedSender<SniperEvent>,
        min_confidence: f64,
    ) -> Self {
        Self {
            store,
            event_tx,
            mappings: HashMap::new(),
            sniped: HashMap::new(),
            min_confidence,
            max_buy_price: Decimal::from_str("0.99").unwrap(),
            min_sell_price: Decimal::from_str("0.01").unwrap(),
        }
    }

    /// Register a market-to-source mapping.
    pub fn add_mapping(&mut self, mapping: MarketSourceMapping) {
        self.mappings
            .insert(mapping.condition_id.clone(), mapping);
    }

    /// Process a resolution signal and generate snipe actions.
    pub fn process_signal(&mut self, signal: ResolutionSignal) -> Option<SnipeAction> {
        // Don't re-snipe the same market
        if self.sniped.contains_key(&signal.condition_id) {
            debug!(
                cid = %signal.condition_id,
                "already sniped, ignoring duplicate signal"
            );
            return None;
        }

        if signal.confidence < self.min_confidence {
            let _ = self.event_tx.send(SniperEvent::SignalIgnored {
                condition_id: signal.condition_id.clone(),
                confidence: signal.confidence,
                reason: format!(
                    "confidence {:.2} < threshold {:.2}",
                    signal.confidence, self.min_confidence
                ),
            });
            return None;
        }

        let _ = self.event_tx.send(SniperEvent::SignalReceived {
            condition_id: signal.condition_id.clone(),
            source: signal.source.clone(),
            confidence: signal.confidence,
        });

        let mapping = self.mappings.get(&signal.condition_id)?;

        let mut orders = Vec::new();

        for (label, token_id) in &mapping.outcomes {
            let result = signal
                .outcomes
                .get(label)
                .copied()
                .unwrap_or(OutcomeResult::Unknown);

            match result {
                OutcomeResult::Won => {
                    // Buy the winner: sweep asks up to max_buy_price
                    // Only if asks exist below our limit
                    if let Some(book) = self.store.get_book(token_id) {
                        if let Some((best_ask, _)) = book.asks.best(false) {
                            if best_ask < self.max_buy_price {
                                orders.push(SnipeOrder {
                                    token_id: token_id.clone(),
                                    label: label.clone(),
                                    side: SnipeSide::BuyWinner,
                                    limit_price: self.max_buy_price,
                                });
                            }
                        }
                    }
                }
                OutcomeResult::Lost => {
                    // Sell the loser: hit bids above min_sell_price
                    // Only profitable if we hold tokens (from prior splits) or bids are high
                    if let Some(book) = self.store.get_book(token_id) {
                        if let Some((best_bid, _)) = book.bids.best(true) {
                            if best_bid > self.min_sell_price {
                                orders.push(SnipeOrder {
                                    token_id: token_id.clone(),
                                    label: label.clone(),
                                    side: SnipeSide::SellLoser,
                                    limit_price: self.min_sell_price,
                                });
                            }
                        }
                    }
                }
                OutcomeResult::Unknown => {} // Skip unknowns
            }
        }

        if orders.is_empty() {
            debug!(
                cid = %signal.condition_id,
                "no actionable orders from signal"
            );
            return None;
        }

        self.sniped
            .insert(signal.condition_id.clone(), Instant::now());

        let action = SnipeAction {
            condition_id: signal.condition_id.clone(),
            question: mapping.question.clone(),
            orders,
            signal,
        };

        let _ = self.event_tx.send(SniperEvent::SnipeDispatched {
            condition_id: action.condition_id.clone(),
            question: action.question.clone(),
            num_orders: action.orders.len(),
        });

        info!(
            market = %action.question,
            orders = action.orders.len(),
            source = %action.signal.source,
            confidence = action.signal.confidence,
            "SNIPE DISPATCHED"
        );

        Some(action)
    }

    /// Handle an on-chain MarketResolved event from the SDK WS.
    /// This fires _after_ resolution, so it's a secondary/confirmation signal.
    pub fn on_chain_resolution(
        &mut self,
        condition_id: &str,
        winning_outcome: &str,
        detected_at: Instant,
    ) -> Option<SnipeAction> {
        let _ = self.event_tx.send(SniperEvent::OnChainResolution {
            condition_id: condition_id.to_string(),
            winning_outcome: winning_outcome.to_string(),
        });

        // If we already sniped from the primary source, don't double-fire
        if self.sniped.contains_key(condition_id) {
            info!(
                cid = %condition_id,
                outcome = %winning_outcome,
                "on-chain resolution confirmed (already sniped)"
            );
            return None;
        }

        // Build signal from on-chain event — use the original detection time
        let mapping = self.mappings.get(condition_id)?;
        let mut outcomes = HashMap::new();
        for (label, _) in &mapping.outcomes {
            if label == winning_outcome {
                outcomes.insert(label.clone(), OutcomeResult::Won);
            } else {
                outcomes.insert(label.clone(), OutcomeResult::Lost);
            }
        }

        let signal = ResolutionSignal {
            condition_id: condition_id.to_string(),
            source: "on_chain".to_string(),
            outcomes,
            confidence: 1.0,
            evidence: format!("MarketResolved: winner={}", winning_outcome),
            observed_at: detected_at,
        };

        self.process_signal(signal)
    }

    /// Clean up stale sniped entries (after markets have fully resolved).
    pub fn cleanup_sniped(&mut self, max_age: Duration) {
        let before = self.sniped.len();
        self.sniped
            .retain(|_, instant| instant.elapsed() < max_age);
        let removed = before - self.sniped.len();
        if removed > 0 {
            info!(removed, "cleaned stale snipe records");
        }
    }

    pub fn mapping_count(&self) -> usize {
        self.mappings.len()
    }

    pub fn sniped_count(&self) -> usize {
        self.sniped.len()
    }
}

use std::time::Duration;

/// Auto-classify markets to resolution source types based on question text.
/// This is a heuristic mapper — markets that don't match any pattern are skipped.
pub fn auto_map_markets(markets: &[TrackedMarket]) -> Vec<MarketSourceMapping> {
    let mut mappings = Vec::new();

    for market in markets {
        let q = market.question.to_lowercase();
        let mut params = HashMap::new();

        let source_type = if is_crypto_market(&q, &mut params) {
            SourceType::CryptoPrice
        } else if is_billboard_market(&q, &mut params) {
            SourceType::Billboard
        } else if is_rotten_tomatoes_market(&q, &mut params) {
            SourceType::RottenTomatoes
        } else if is_election_market(&q) {
            SourceType::Election
        } else if is_weather_market(&q) {
            SourceType::Weather
        } else {
            continue; // No recognizable source
        };

        mappings.push(MarketSourceMapping {
            condition_id: market.condition_id.clone(),
            question: market.question.clone(),
            outcomes: market.outcomes.clone(),
            source_type,
            source_params: params,
        });
    }

    info!(
        total = markets.len(),
        mapped = mappings.len(),
        "auto-mapped markets to resolution sources"
    );

    mappings
}

fn is_crypto_market(q: &str, params: &mut HashMap<String, String>) -> bool {
    // Match patterns like "Will BTC be above $100,000", "ETH price above $4000",
    // "Will Bitcoin reach $150k", "SOL above $200 on March 1"
    let crypto_keywords = [
        ("btc", "BTCUSD"),
        ("bitcoin", "BTCUSD"),
        ("eth", "ETHUSD"),
        ("ethereum", "ETHUSD"),
        ("sol", "SOLUSD"),
        ("solana", "SOLUSD"),
        ("doge", "DOGEUSD"),
        ("xrp", "XRPUSD"),
    ];

    for (keyword, symbol) in &crypto_keywords {
        if q.contains(keyword)
            && (q.contains("price") || q.contains("above") || q.contains("below")
                || q.contains("reach") || q.contains("$"))
        {
            params.insert("symbol".to_string(), symbol.to_string());

            // Try to extract price threshold
            if let Some(threshold) = extract_price_threshold(q) {
                params.insert("threshold".to_string(), threshold.to_string());
            }

            return true;
        }
    }
    false
}

fn is_billboard_market(q: &str, params: &mut HashMap<String, String>) -> bool {
    q.contains("billboard") || q.contains("hot 100") || q.contains("#1 album")
        || q.contains("number one album") || q.contains("chart")
}

fn is_rotten_tomatoes_market(q: &str, params: &mut HashMap<String, String>) -> bool {
    q.contains("rotten tomatoes") || q.contains("tomatometer")
        || (q.contains("rating") && (q.contains("movie") || q.contains("film")))
}

fn is_election_market(q: &str) -> bool {
    q.contains("election") || q.contains("electoral") || q.contains("nominee")
        || q.contains("president") || q.contains("governor") || q.contains("senate")
        || q.contains("congress") || q.contains("vote")
}

fn is_weather_market(q: &str) -> bool {
    q.contains("earthquake") || q.contains("hurricane") || q.contains("tornado")
        || q.contains("temperature") || q.contains("rainfall")
        || q.contains("magnitude") || q.contains("noaa") || q.contains("usgs")
}

/// Extract a numeric price threshold from a question string.
/// E.g., "Will BTC be above $100,000?" → 100000.0
fn extract_price_threshold(q: &str) -> Option<f64> {
    // Find $ followed by digits with optional commas and k/K suffix
    let mut chars = q.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            let mut num_str = String::new();
            while let Some(&nc) = chars.peek() {
                if nc.is_ascii_digit() || nc == ',' || nc == '.' {
                    num_str.push(nc);
                    chars.next();
                } else if nc == 'k' || nc == 'K' {
                    chars.next();
                    let base: f64 = num_str.replace(',', "").parse().ok()?;
                    return Some(base * 1000.0);
                } else {
                    break;
                }
            }
            if !num_str.is_empty() {
                return num_str.replace(',', "").parse().ok();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_price_threshold() {
        assert_eq!(extract_price_threshold("above $100,000?"), Some(100000.0));
        assert_eq!(extract_price_threshold("reach $150k"), Some(150000.0));
        assert_eq!(extract_price_threshold("below $4,500.50"), Some(4500.50));
        assert_eq!(extract_price_threshold("no price here"), None);
    }

    #[test]
    fn test_crypto_detection() {
        let mut params = HashMap::new();
        assert!(is_crypto_market(
            "will btc be above $100,000 on march 1?",
            &mut params
        ));
        assert_eq!(params.get("symbol").unwrap(), "BTCUSD");
        assert_eq!(
            params.get("threshold").unwrap().parse::<f64>().unwrap(),
            100000.0
        );
    }
}
