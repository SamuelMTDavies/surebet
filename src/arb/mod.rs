//! Cross-outcome arbitrage detection engine.
//!
//! Monitors YES/NO token pairs within binary markets and multi-outcome
//! markets. Detects when the sum of best asks across all outcomes
//! deviates from $1.00 by more than the fee structure allows.

pub mod executor;

use crate::market::DiscoveredMarket;
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// A tracked market with its outcome token IDs.
#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub condition_id: String,
    pub question: String,
    /// Ordered list of (outcome_label, token_id) pairs.
    /// For binary: [("Yes", token_yes), ("No", token_no)]
    pub outcomes: Vec<(String, String)>,
}

impl TrackedMarket {
    pub fn from_discovered(m: &DiscoveredMarket) -> Option<Self> {
        if m.clob_token_ids.len() < 2 || m.outcomes.len() != m.clob_token_ids.len() {
            return None;
        }
        let outcomes = m
            .outcomes
            .iter()
            .zip(m.clob_token_ids.iter())
            .map(|(label, tid)| (label.clone(), tid.clone()))
            .collect();
        Some(Self {
            condition_id: m.condition_id.clone(),
            question: m.question.clone(),
            outcomes,
        })
    }
}

/// An arbitrage opportunity detected by the scanner.
#[derive(Debug, Clone)]
pub struct ArbOpportunity {
    pub condition_id: String,
    pub question: String,
    /// Per-outcome: (label, token_id, best_ask_price, ask_size)
    pub legs: Vec<ArbLeg>,
    /// Sum of best asks across all outcomes.
    pub ask_sum: Decimal,
    /// Gross edge: 1.00 - ask_sum (positive = profitable before fees).
    pub gross_edge: Decimal,
    /// Net edge after estimated taker fees.
    pub net_edge: Decimal,
    /// Minimum fillable size across all legs.
    pub max_fillable_size: Decimal,
    /// Timestamp of detection.
    pub detected_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct ArbLeg {
    pub label: String,
    pub token_id: String,
    pub best_ask: Decimal,
    pub ask_size: Decimal,
}

/// Events emitted by the arb scanner.
#[derive(Debug, Clone)]
pub enum ArbEvent {
    OpportunityDetected(ArbOpportunity),
    OpportunityGone { condition_id: String },
    ScanComplete { markets_scanned: usize, opportunities: usize },
    /// Periodic diagnostics showing scanner health + near-misses.
    ScanDiagnostics {
        total_markets: usize,
        markets_with_books: usize,
        tightest_ask_sum: Option<Decimal>,
        tightest_market: Option<String>,
        near_misses: usize,
    },
}

/// Dynamic taker fee model (Polymarket's fee curve).
/// Fee peaks at ~3.15% when price is near 0.50, drops near 0/1.
fn estimated_taker_fee(price: Decimal) -> Decimal {
    // Polymarket's fee is roughly: fee = base_rate * 4 * p * (1 - p)
    // where p is the price and base_rate ≈ 0.0315
    // This peaks at p=0.50 → fee=3.15%, and drops to 0 at p=0 or p=1
    let base_rate = Decimal::from_str("0.0315").unwrap();
    let four = Decimal::from(4);
    let one = Decimal::ONE;
    base_rate * four * price * (one - price)
}

/// Calculate the total taker fee for buying all outcomes.
fn total_taker_fees(legs: &[ArbLeg]) -> Decimal {
    legs.iter()
        .map(|leg| {
            let fee_rate = estimated_taker_fee(leg.best_ask);
            leg.best_ask * fee_rate
        })
        .sum()
}

/// The cross-outcome arbitrage scanner.
pub struct ArbScanner {
    store: OrderBookStore,
    markets: Vec<TrackedMarket>,
    event_tx: mpsc::UnboundedSender<ArbEvent>,
    /// Minimum net edge (after fees) to report as opportunity.
    min_net_edge: Decimal,
    /// Track which markets currently have live opportunities.
    active_opps: HashMap<String, ArbOpportunity>,
    /// Scan counter for periodic diagnostics.
    scan_count: u64,
}

impl ArbScanner {
    pub fn new(
        store: OrderBookStore,
        markets: Vec<TrackedMarket>,
        event_tx: mpsc::UnboundedSender<ArbEvent>,
        min_net_edge: Decimal,
    ) -> Self {
        Self {
            store,
            markets,
            event_tx,
            min_net_edge,
            active_opps: HashMap::new(),
            scan_count: 0,
        }
    }

    /// Scan all tracked markets for arbitrage opportunities.
    /// Call this on every book update for the affected market,
    /// or periodically.
    pub fn scan_all(&mut self) -> Vec<ArbOpportunity> {
        self.scan_count += 1;
        let mut opportunities = Vec::new();

        // Diagnostic tracking
        let mut markets_with_books = 0usize;
        let mut tightest_ask_sum: Option<Decimal> = None;
        let mut tightest_market: Option<String> = None;
        let mut near_misses = 0usize;

        for market in &self.markets {
            // Check if both sides have book data
            let has_all_books = market
                .outcomes
                .iter()
                .all(|(_, tid)| self.store.get_book(tid).and_then(|b| b.asks.best(false)).is_some());

            if has_all_books {
                markets_with_books += 1;

                // Compute ask_sum even for non-opportunities (for diagnostics)
                let ask_sum: Decimal = market
                    .outcomes
                    .iter()
                    .filter_map(|(_, tid)| {
                        self.store.get_book(tid).and_then(|b| b.asks.best(false).map(|(p, _)| p))
                    })
                    .sum();

                // Track tightest ask_sum
                if tightest_ask_sum.is_none() || ask_sum < tightest_ask_sum.unwrap() {
                    tightest_ask_sum = Some(ask_sum);
                    tightest_market = Some(market.question.clone());
                }

                // Near miss: ask_sum < 1.02 (within 2% of arb)
                let threshold = Decimal::from_str("1.02").unwrap();
                if ask_sum < threshold {
                    near_misses += 1;
                }
            }

            if let Some(opp) = self.check_market(market) {
                opportunities.push(opp);
            }
        }

        // Emit events for new/gone opportunities
        let current_ids: std::collections::HashSet<String> =
            opportunities.iter().map(|o| o.condition_id.clone()).collect();

        // Gone opportunities
        let gone: Vec<String> = self
            .active_opps
            .keys()
            .filter(|id| !current_ids.contains(*id))
            .cloned()
            .collect();
        for id in gone {
            self.active_opps.remove(&id);
            let _ = self
                .event_tx
                .send(ArbEvent::OpportunityGone { condition_id: id });
        }

        // New or updated opportunities
        for opp in &opportunities {
            let is_new = !self.active_opps.contains_key(&opp.condition_id);
            self.active_opps
                .insert(opp.condition_id.clone(), opp.clone());
            if is_new {
                let _ = self
                    .event_tx
                    .send(ArbEvent::OpportunityDetected(opp.clone()));
            }
        }

        let _ = self.event_tx.send(ArbEvent::ScanComplete {
            markets_scanned: self.markets.len(),
            opportunities: opportunities.len(),
        });

        // Emit diagnostics every 30 scans (~30s)
        if self.scan_count % 30 == 0 {
            let _ = self.event_tx.send(ArbEvent::ScanDiagnostics {
                total_markets: self.markets.len(),
                markets_with_books,
                tightest_ask_sum,
                tightest_market,
                near_misses,
            });
        }

        opportunities
    }

    /// Check a single market for cross-outcome mispricing.
    fn check_market(&self, market: &TrackedMarket) -> Option<ArbOpportunity> {
        let mut legs = Vec::with_capacity(market.outcomes.len());

        for (label, token_id) in &market.outcomes {
            let book = self.store.get_book(token_id)?;
            let (best_ask_price, best_ask_size) = book.asks.best(false)?;

            legs.push(ArbLeg {
                label: label.clone(),
                token_id: token_id.clone(),
                best_ask: best_ask_price,
                ask_size: best_ask_size,
            });
        }

        let ask_sum: Decimal = legs.iter().map(|l| l.best_ask).sum();
        let gross_edge = Decimal::ONE - ask_sum;

        // Only interesting if ask_sum < 1.00 (can buy all outcomes for less than payout)
        if gross_edge <= Decimal::ZERO {
            return None;
        }

        let fees = total_taker_fees(&legs);
        let net_edge = gross_edge - fees;

        if net_edge < self.min_net_edge {
            debug!(
                market = %market.question,
                ask_sum = %ask_sum,
                gross_edge = %gross_edge,
                fees = %fees,
                net_edge = %net_edge,
                "sub-threshold arb"
            );
            return None;
        }

        // Min fillable: smallest ask size across all legs
        let max_fillable_size = legs
            .iter()
            .map(|l| l.ask_size)
            .min()
            .unwrap_or(Decimal::ZERO);

        let opp = ArbOpportunity {
            condition_id: market.condition_id.clone(),
            question: market.question.clone(),
            legs,
            ask_sum,
            gross_edge,
            net_edge,
            max_fillable_size,
            detected_at: chrono::Utc::now(),
        };

        info!(
            market = %opp.question,
            ask_sum = %opp.ask_sum,
            gross = %opp.gross_edge,
            net = %opp.net_edge,
            fillable = %opp.max_fillable_size,
            "ARB DETECTED"
        );

        Some(opp)
    }

    /// Check the maker strategy: post passive bids on all outcomes
    /// where the bid sum > 1.00 would be unprofitable for the other side.
    /// Returns optimal passive bid prices that maximize fill probability
    /// while maintaining positive expected value including rebates.
    pub fn compute_maker_prices(
        &self,
        market: &TrackedMarket,
    ) -> Option<Vec<(String, Decimal)>> {
        let mut mid_prices = Vec::new();

        for (_, token_id) in &market.outcomes {
            let book = self.store.get_book(token_id)?;
            let mid = book.mid_price()?;
            mid_prices.push((token_id.clone(), mid));
        }

        let mid_sum: Decimal = mid_prices.iter().map(|(_, p)| *p).sum();

        // If mids sum close to 1.00, we can post bids slightly inside
        // the spread on each outcome. When both legs fill, we profit
        // from the spread + maker rebates.
        //
        // Target: bid_i = mid_i - (spread_share)
        // where bid_sum should be < 1.00 to maintain arb profit
        let target_sum = Decimal::from_str("0.98").unwrap();
        let adjustment = (mid_sum - target_sum) / Decimal::from(market.outcomes.len() as i64);

        let prices: Vec<(String, Decimal)> = mid_prices
            .iter()
            .map(|(tid, mid)| {
                let bid = (*mid - adjustment).max(Decimal::from_str("0.01").unwrap());
                // Round to tick size 0.01
                let rounded = (bid * Decimal::from(100)).floor() / Decimal::from(100);
                (tid.clone(), rounded)
            })
            .collect();

        let bid_sum: Decimal = prices.iter().map(|(_, p)| *p).sum();
        info!(
            market = %market.question,
            mid_sum = %mid_sum,
            bid_sum = %bid_sum,
            edge = %(Decimal::ONE - bid_sum),
            "maker prices computed"
        );

        Some(prices)
    }
}
