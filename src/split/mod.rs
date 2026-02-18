//! Enhanced multi-outcome split arbitrage engine (Strategy 1A).
//!
//! Extends the basic arb scanner with:
//! - Bidirectional arb detection: sum > 1 (sell-side) AND sum < 1 (buy-side)
//! - Depth-aware position sizing at multiple tiers ($100, $500, $1000, $5000)
//! - Priority weighting for markets with 5+ outcomes
//! - New-market bias (first-seen within 2 hours scanned more aggressively)
//! - Book-walking for executable edge including slippage
//!
//! Execution uses the CLOB API (limit orders at/near best price) rather than
//! on-chain CTF split/merge, since the CLOB path is faster and the project
//! doesn't currently have an alloy provider for on-chain txns.

pub mod executor;

use crate::arb::TrackedMarket;
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Position size tiers for depth analysis (in USDC).
pub const SIZE_TIERS: &[u64] = &[100, 500, 1000, 5000];

/// Polymarket's dynamic taker fee model.
/// Fee peaks at ~3.15% when price is near 0.50, drops to 0 near 0/1.
fn estimated_taker_fee(price: Decimal) -> Decimal {
    let base_rate = Decimal::from_str("0.0315").unwrap();
    Decimal::from(4) * base_rate * price * (Decimal::ONE - price)
}

/// Direction of the split arbitrage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SplitDirection {
    /// Sum of asks < 1: buy all outcomes, merge/hold full set worth $1.
    BuyAndMerge,
    /// Sum of bids > 1: mint/split $1 into tokens, sell each at bid.
    SplitAndSell,
}

impl std::fmt::Display for SplitDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitDirection::BuyAndMerge => write!(f, "BUY_MERGE"),
            SplitDirection::SplitAndSell => write!(f, "SPLIT_SELL"),
        }
    }
}

/// Depth analysis for one position-size tier.
#[derive(Debug, Clone)]
pub struct TierAnalysis {
    pub tier_usd: Decimal,
    /// Shares executable at this tier.
    pub executable_shares: Decimal,
    /// Volume-weighted average price across all levels consumed per leg.
    pub vwap_per_leg: Vec<Decimal>,
    /// Net edge after fees and slippage at this tier.
    pub net_edge: Decimal,
    /// Expected profit in USDC.
    pub expected_profit_usd: Decimal,
    /// True if every leg has enough depth for full fill.
    pub fully_fillable: bool,
}

/// A single leg in a split arb.
#[derive(Debug, Clone)]
pub struct SplitLeg {
    pub label: String,
    pub token_id: String,
    /// Best price (ask for buy-side, bid for sell-side).
    pub best_price: Decimal,
    /// Size available at best price.
    pub best_size: Decimal,
    /// All levels on the relevant side, price-ordered.
    /// Ascending for asks, descending for bids.
    pub levels: Vec<(Decimal, Decimal)>,
}

/// A detected split arbitrage opportunity.
#[derive(Debug, Clone)]
pub struct SplitOpportunity {
    pub condition_id: String,
    pub question: String,
    pub direction: SplitDirection,
    pub num_outcomes: usize,
    pub legs: Vec<SplitLeg>,
    /// Sum of best prices across all outcomes.
    pub price_sum: Decimal,
    /// Gross edge at top-of-book: |1 - price_sum|.
    pub gross_edge: Decimal,
    /// Net edge at top-of-book after estimated fees.
    pub net_edge_tob: Decimal,
    /// Depth analysis at various size tiers.
    pub tier_analyses: Vec<TierAnalysis>,
    /// Minimum shares fillable across all legs at top-of-book.
    pub min_fillable_shares: Decimal,
    /// High priority: 5+ outcomes or newly listed.
    pub high_priority: bool,
    pub detected_at: Instant,
}

/// Events emitted by the split scanner.
#[derive(Debug, Clone)]
pub enum SplitEvent {
    OpportunityDetected(SplitOpportunity),
    OpportunityGone {
        condition_id: String,
        direction: SplitDirection,
    },
    ScanComplete {
        markets_scanned: usize,
        buy_merge_opps: usize,
        split_sell_opps: usize,
    },
    ScanDiagnostics {
        total_markets: usize,
        multi_outcome_markets: usize,
        tightest_buy_sum: Option<Decimal>,
        tightest_sell_sum: Option<Decimal>,
        near_misses: usize,
    },
}

/// Key for tracking active opportunities.
type OppKey = (String, SplitDirection);

/// The enhanced split arbitrage scanner.
pub struct SplitScanner {
    store: OrderBookStore,
    markets: Vec<TrackedMarket>,
    event_tx: mpsc::UnboundedSender<SplitEvent>,
    min_net_edge: Decimal,
    active_opps: HashMap<OppKey, SplitOpportunity>,
    scan_count: u64,
    /// Track when we first saw each market (for new-market bias).
    first_seen: HashMap<String, Instant>,
}

impl SplitScanner {
    pub fn new(
        store: OrderBookStore,
        markets: Vec<TrackedMarket>,
        event_tx: mpsc::UnboundedSender<SplitEvent>,
        min_net_edge: Decimal,
    ) -> Self {
        Self {
            store,
            markets,
            event_tx,
            min_net_edge,
            active_opps: HashMap::new(),
            scan_count: 0,
            first_seen: HashMap::new(),
        }
    }

    /// Run a full scan cycle across all tracked markets.
    pub fn scan_all(&mut self) {
        self.scan_count += 1;
        let mut all_opps = Vec::new();

        let mut multi_outcome_count = 0usize;
        let mut tightest_buy: Option<Decimal> = None;
        let mut tightest_sell: Option<Decimal> = None;
        let mut near_misses = 0usize;
        let near_threshold = Decimal::from_str("0.02").unwrap();

        for market in &self.markets {
            self.first_seen
                .entry(market.condition_id.clone())
                .or_insert_with(Instant::now);

            if market.outcomes.len() > 2 {
                multi_outcome_count += 1;
            }

            // --- BuyAndMerge: asks sum < 1.00 ---
            if let Some(opp) = self.check_direction(market, SplitDirection::BuyAndMerge) {
                all_opps.push(opp);
            }
            if let Some(s) = self.compute_side_sum(market, SplitDirection::BuyAndMerge) {
                if tightest_buy.is_none() || s < tightest_buy.unwrap() {
                    tightest_buy = Some(s);
                }
                if (Decimal::ONE - s).abs() < near_threshold {
                    near_misses += 1;
                }
            }

            // --- SplitAndSell: bids sum > 1.00 ---
            if let Some(opp) = self.check_direction(market, SplitDirection::SplitAndSell) {
                all_opps.push(opp);
            }
            if let Some(s) = self.compute_side_sum(market, SplitDirection::SplitAndSell) {
                if tightest_sell.is_none() || s > tightest_sell.unwrap() {
                    tightest_sell = Some(s);
                }
                if (s - Decimal::ONE).abs() < near_threshold {
                    near_misses += 1;
                }
            }
        }

        // Track new / gone opportunities
        let mut current_keys: std::collections::HashSet<OppKey> =
            std::collections::HashSet::new();

        let mut buy_merge_count = 0usize;
        let mut split_sell_count = 0usize;

        for opp in &all_opps {
            let key = (opp.condition_id.clone(), opp.direction);
            current_keys.insert(key.clone());
            match opp.direction {
                SplitDirection::BuyAndMerge => buy_merge_count += 1,
                SplitDirection::SplitAndSell => split_sell_count += 1,
            }
            if !self.active_opps.contains_key(&key) {
                let _ = self
                    .event_tx
                    .send(SplitEvent::OpportunityDetected(opp.clone()));
            }
            self.active_opps.insert(key, opp.clone());
        }

        let gone: Vec<OppKey> = self
            .active_opps
            .keys()
            .filter(|k| !current_keys.contains(*k))
            .cloned()
            .collect();
        for key in gone {
            self.active_opps.remove(&key);
            let _ = self.event_tx.send(SplitEvent::OpportunityGone {
                condition_id: key.0,
                direction: key.1,
            });
        }

        let _ = self.event_tx.send(SplitEvent::ScanComplete {
            markets_scanned: self.markets.len(),
            buy_merge_opps: buy_merge_count,
            split_sell_opps: split_sell_count,
        });

        if self.scan_count % 30 == 0 {
            let _ = self.event_tx.send(SplitEvent::ScanDiagnostics {
                total_markets: self.markets.len(),
                multi_outcome_markets: multi_outcome_count,
                tightest_buy_sum: tightest_buy,
                tightest_sell_sum: tightest_sell,
                near_misses,
            });
        }
    }

    /// Check a single market for an arb in the given direction.
    fn check_direction(
        &self,
        market: &TrackedMarket,
        direction: SplitDirection,
    ) -> Option<SplitOpportunity> {
        let mut legs = Vec::with_capacity(market.outcomes.len());

        for (label, token_id) in &market.outcomes {
            let book = self.store.get_book(token_id)?;
            let (best_price, best_size, levels) = match direction {
                SplitDirection::BuyAndMerge => {
                    let (p, s) = book.asks.best(false)?;
                    let lvls: Vec<(Decimal, Decimal)> =
                        book.asks.levels.iter().map(|(p, s)| (*p, *s)).collect();
                    (p, s, lvls)
                }
                SplitDirection::SplitAndSell => {
                    let (p, s) = book.bids.best(true)?;
                    let lvls: Vec<(Decimal, Decimal)> =
                        book.bids.levels.iter().rev().map(|(p, s)| (*p, *s)).collect();
                    (p, s, lvls)
                }
            };

            legs.push(SplitLeg {
                label: label.clone(),
                token_id: token_id.clone(),
                best_price,
                best_size,
                levels,
            });
        }

        let price_sum: Decimal = legs.iter().map(|l| l.best_price).sum();

        let gross_edge = match direction {
            SplitDirection::BuyAndMerge => Decimal::ONE - price_sum,
            SplitDirection::SplitAndSell => price_sum - Decimal::ONE,
        };

        if gross_edge <= Decimal::ZERO {
            return None;
        }

        let fees: Decimal = legs
            .iter()
            .map(|l| l.best_price * estimated_taker_fee(l.best_price))
            .sum();
        let net_edge_tob = gross_edge - fees;

        if net_edge_tob < self.min_net_edge {
            debug!(
                market = %market.question,
                direction = %direction,
                sum = %price_sum,
                net = %net_edge_tob,
                "sub-threshold split"
            );
            return None;
        }

        let min_fillable = legs
            .iter()
            .map(|l| l.best_size)
            .min()
            .unwrap_or(Decimal::ZERO);

        let tier_analyses = analyze_tiers(&legs, direction);

        let is_new = self
            .first_seen
            .get(&market.condition_id)
            .map(|t| t.elapsed().as_secs() < 7200)
            .unwrap_or(false);

        let opp = SplitOpportunity {
            condition_id: market.condition_id.clone(),
            question: market.question.clone(),
            direction,
            num_outcomes: market.outcomes.len(),
            legs,
            price_sum,
            gross_edge,
            net_edge_tob,
            tier_analyses,
            min_fillable_shares: min_fillable,
            high_priority: market.outcomes.len() >= 5 || is_new,
            detected_at: Instant::now(),
        };

        info!(
            market = %opp.question,
            dir = %direction,
            n = opp.num_outcomes,
            sum = %price_sum,
            gross = %gross_edge,
            net_tob = %net_edge_tob,
            fillable = %min_fillable,
            priority = opp.high_priority,
            "SPLIT ARB"
        );

        Some(opp)
    }

    /// Compute the relevant price sum for diagnostics.
    fn compute_side_sum(&self, market: &TrackedMarket, direction: SplitDirection) -> Option<Decimal> {
        let sum: Decimal = market
            .outcomes
            .iter()
            .filter_map(|(_, tid)| {
                let book = self.store.get_book(tid)?;
                match direction {
                    SplitDirection::BuyAndMerge => book.asks.best(false).map(|(p, _)| p),
                    SplitDirection::SplitAndSell => book.bids.best(true).map(|(p, _)| p),
                }
            })
            .sum();
        if sum > Decimal::ZERO { Some(sum) } else { None }
    }

    pub fn active_opportunities(&self) -> Vec<&SplitOpportunity> {
        self.active_opps.values().collect()
    }
}

/// Walk the book for `target_shares`, returning (total_cost, shares_filled, fees).
fn walk_book(levels: &[(Decimal, Decimal)], target_shares: Decimal) -> (Decimal, Decimal, Decimal) {
    let mut remaining = target_shares;
    let mut total_value = Decimal::ZERO;
    let mut total_fees = Decimal::ZERO;

    for &(price, size) in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let fill = remaining.min(size);
        let value = fill * price;
        let fee = value * estimated_taker_fee(price);
        total_value += value;
        total_fees += fee;
        remaining -= fill;
    }

    (total_value, target_shares - remaining, total_fees)
}

/// Compute VWAP for a leg at a given share target.
fn leg_vwap(levels: &[(Decimal, Decimal)], target: Decimal) -> Option<Decimal> {
    let (cost, filled, _) = walk_book(levels, target);
    if filled > Decimal::ZERO {
        Some(cost / filled)
    } else {
        None
    }
}

/// Analyze all tiers for a set of legs in the given direction.
fn analyze_tiers(legs: &[SplitLeg], direction: SplitDirection) -> Vec<TierAnalysis> {
    SIZE_TIERS
        .iter()
        .filter_map(|&tier_usd| {
            let tier = Decimal::from(tier_usd);

            // Estimate shares from top-of-book sum
            let tob_sum: Decimal = legs.iter().map(|l| l.best_price).sum();
            if tob_sum == Decimal::ZERO {
                return None;
            }

            let target_shares = match direction {
                // For buy-merge, we spend `tier` USDC buying all outcomes
                SplitDirection::BuyAndMerge => tier / tob_sum,
                // For split-sell, we mint `tier` sets ($1 each) and sell them
                SplitDirection::SplitAndSell => tier,
            };

            let mut total_cost = Decimal::ZERO; // cost to buy (buy-merge) or revenue from selling (split-sell)
            let mut total_fees = Decimal::ZERO;
            let mut fully_fillable = true;
            let mut vwaps = Vec::new();

            for leg in legs {
                let (value, filled, fee) = walk_book(&leg.levels, target_shares);
                total_cost += value;
                total_fees += fee;
                if filled < target_shares {
                    fully_fillable = false;
                }
                vwaps.push(
                    leg_vwap(&leg.levels, target_shares).unwrap_or(leg.best_price),
                );
            }

            let (net_profit, net_edge) = match direction {
                SplitDirection::BuyAndMerge => {
                    let revenue = target_shares; // $1 per complete set
                    let profit = revenue - total_cost - total_fees;
                    let edge = if revenue > Decimal::ZERO {
                        profit / revenue
                    } else {
                        Decimal::ZERO
                    };
                    (profit, edge)
                }
                SplitDirection::SplitAndSell => {
                    let mint_cost = target_shares; // $1 per set
                    let profit = total_cost - mint_cost - total_fees;
                    let edge = if mint_cost > Decimal::ZERO {
                        profit / mint_cost
                    } else {
                        Decimal::ZERO
                    };
                    (profit, edge)
                }
            };

            Some(TierAnalysis {
                tier_usd: tier,
                executable_shares: target_shares,
                vwap_per_leg: vwaps,
                net_edge,
                expected_profit_usd: net_profit,
                fully_fillable,
            })
        })
        .collect()
}
