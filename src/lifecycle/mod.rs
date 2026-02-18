//! Position lifecycle management (Strategy 1C).
//!
//! Manages the exit side of all positions:
//! - Winning tokens: queue for redemption once market resolves on-chain
//! - Losing tokens from splits: drip-sell to avoid walking the book down
//! - Stale positions: sell at discount if resolution is delayed > 24h
//!
//! For now (paper mode), this module _tracks_ positions and _logs_ what
//! actions it would take. When wallet connection is added, it will use:
//! - SDK's CTF client for on-chain redemption
//! - CLOB API for sell orders (rate-limited drip sells)

use crate::arb::TrackedMarket;
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// A tracked position in the lifecycle manager.
#[derive(Debug, Clone)]
pub struct ManagedPosition {
    pub condition_id: String,
    pub question: String,
    /// Per-outcome holdings: (label, token_id, shares_held, avg_cost).
    pub legs: Vec<ManagedLeg>,
    /// How this position was entered.
    pub strategy: PositionStrategy,
    /// When the position was first entered.
    pub entered_at: Instant,
    /// Current lifecycle state.
    pub state: PositionState,
    /// Total cost basis in USDC.
    pub cost_basis: Decimal,
}

#[derive(Debug, Clone)]
pub struct ManagedLeg {
    pub label: String,
    pub token_id: String,
    pub shares: Decimal,
    pub avg_cost: Decimal,
    /// Known resolution outcome, if any.
    pub resolved: Option<LegResolution>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LegResolution {
    Won,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PositionStrategy {
    SplitArb,
    ResolutionSnipe,
    Maker,
}

impl std::fmt::Display for PositionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionStrategy::SplitArb => write!(f, "split_arb"),
            PositionStrategy::ResolutionSnipe => write!(f, "snipe"),
            PositionStrategy::Maker => write!(f, "maker"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PositionState {
    /// Position is open, waiting for resolution or exit signal.
    Active,
    /// Resolution is known. Winning legs queued for redemption,
    /// losing legs being drip-sold.
    Exiting,
    /// Resolution confirmed on-chain. Winning legs redeemable.
    Redeemable,
    /// Position is stale (no resolution after threshold). Selling at discount.
    StaleExit,
    /// All legs closed.
    Closed,
}

impl std::fmt::Display for PositionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionState::Active => write!(f, "ACTIVE"),
            PositionState::Exiting => write!(f, "EXITING"),
            PositionState::Redeemable => write!(f, "REDEEMABLE"),
            PositionState::StaleExit => write!(f, "STALE_EXIT"),
            PositionState::Closed => write!(f, "CLOSED"),
        }
    }
}

/// Events emitted by the lifecycle manager.
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    PositionTracked {
        condition_id: String,
        strategy: PositionStrategy,
        legs: usize,
        cost_basis: Decimal,
    },
    StateChanged {
        condition_id: String,
        from: PositionState,
        to: PositionState,
    },
    /// Paper: would redeem winning tokens.
    RedemptionQueued {
        condition_id: String,
        token_id: String,
        shares: Decimal,
        expected_payout: Decimal,
    },
    /// Paper: would drip-sell losing tokens.
    DripSellQueued {
        condition_id: String,
        token_id: String,
        shares: Decimal,
        target_price: Decimal,
    },
    /// Paper: selling stale position at discount.
    StalePositionSell {
        condition_id: String,
        age_hours: f64,
        discount_pct: f64,
    },
    PositionClosed {
        condition_id: String,
        realized_pnl: Decimal,
    },
    /// Periodic summary of all managed positions.
    PortfolioSummary {
        total_positions: usize,
        active: usize,
        exiting: usize,
        redeemable: usize,
        stale: usize,
        total_cost_basis: Decimal,
        total_mark_value: Decimal,
        unrealized_pnl: Decimal,
    },
}

/// The lifecycle manager.
pub struct LifecycleManager {
    store: OrderBookStore,
    event_tx: mpsc::UnboundedSender<LifecycleEvent>,
    positions: HashMap<String, ManagedPosition>,
    /// How long before a position is considered stale.
    stale_threshold: Duration,
    /// Drip sell: shares per batch.
    drip_sell_batch: Decimal,
    /// Drip sell: interval between batches.
    drip_sell_interval: Duration,
    /// Track last drip sell time per (condition_id, token_id).
    last_drip_sell: HashMap<(String, String), Instant>,
    scan_count: u64,
}

impl LifecycleManager {
    pub fn new(
        store: OrderBookStore,
        event_tx: mpsc::UnboundedSender<LifecycleEvent>,
        stale_threshold: Duration,
    ) -> Self {
        Self {
            store,
            event_tx,
            positions: HashMap::new(),
            stale_threshold,
            drip_sell_batch: Decimal::from(1000),
            drip_sell_interval: Duration::from_secs(35),
            last_drip_sell: HashMap::new(),
            scan_count: 0,
        }
    }

    /// Track a new position.
    pub fn track_position(&mut self, position: ManagedPosition) {
        let _ = self.event_tx.send(LifecycleEvent::PositionTracked {
            condition_id: position.condition_id.clone(),
            strategy: position.strategy,
            legs: position.legs.len(),
            cost_basis: position.cost_basis,
        });
        self.positions
            .insert(position.condition_id.clone(), position);
    }

    /// Notify the manager that a market has resolved.
    pub fn on_resolution(
        &mut self,
        condition_id: &str,
        winning_label: &str,
    ) {
        let pos = match self.positions.get_mut(condition_id) {
            Some(p) => p,
            None => return,
        };

        let old_state = pos.state;

        for leg in &mut pos.legs {
            if leg.label == winning_label {
                leg.resolved = Some(LegResolution::Won);
            } else {
                leg.resolved = Some(LegResolution::Lost);
            }
        }

        pos.state = PositionState::Exiting;

        let _ = self.event_tx.send(LifecycleEvent::StateChanged {
            condition_id: condition_id.to_string(),
            from: old_state,
            to: pos.state,
        });
    }

    /// Notify that on-chain resolution is confirmed (tokens redeemable).
    pub fn on_chain_resolution_confirmed(&mut self, condition_id: &str) {
        if let Some(pos) = self.positions.get_mut(condition_id) {
            let old_state = pos.state;
            pos.state = PositionState::Redeemable;
            let _ = self.event_tx.send(LifecycleEvent::StateChanged {
                condition_id: condition_id.to_string(),
                from: old_state,
                to: pos.state,
            });
        }
    }

    /// Periodic tick — check all positions and emit actions.
    pub fn tick(&mut self) {
        self.scan_count += 1;
        let now = Instant::now();

        let cids: Vec<String> = self.positions.keys().cloned().collect();

        for cid in &cids {
            let pos = match self.positions.get_mut(cid) {
                Some(p) => p,
                None => continue,
            };

            match pos.state {
                PositionState::Active => {
                    // Check for staleness
                    if pos.entered_at.elapsed() > self.stale_threshold {
                        let old = pos.state;
                        pos.state = PositionState::StaleExit;
                        let _ = self.event_tx.send(LifecycleEvent::StateChanged {
                            condition_id: cid.clone(),
                            from: old,
                            to: pos.state,
                        });

                        let age_hours =
                            pos.entered_at.elapsed().as_secs_f64() / 3600.0;
                        let _ = self.event_tx.send(LifecycleEvent::StalePositionSell {
                            condition_id: cid.clone(),
                            age_hours,
                            discount_pct: 5.0, // Sell at 5% discount
                        });
                    }
                }

                PositionState::Exiting => {
                    // Drip-sell losing legs
                    for leg in &pos.legs {
                        if leg.resolved != Some(LegResolution::Lost) {
                            continue;
                        }
                        if leg.shares <= Decimal::ZERO {
                            continue;
                        }

                        let drip_key = (cid.clone(), leg.token_id.clone());
                        let can_sell = self
                            .last_drip_sell
                            .get(&drip_key)
                            .map(|t| t.elapsed() > self.drip_sell_interval)
                            .unwrap_or(true);

                        if can_sell {
                            let batch = leg.shares.min(self.drip_sell_batch);
                            let target = self
                                .store
                                .get_book(&leg.token_id)
                                .and_then(|b| b.bids.best(true).map(|(p, _)| p))
                                .unwrap_or(Decimal::from_str("0.01").unwrap());

                            let _ = self.event_tx.send(LifecycleEvent::DripSellQueued {
                                condition_id: cid.clone(),
                                token_id: leg.token_id.clone(),
                                shares: batch,
                                target_price: target,
                            });

                            self.last_drip_sell.insert(drip_key, now);
                        }
                    }

                    // Queue redemption for winning legs
                    for leg in &pos.legs {
                        if leg.resolved != Some(LegResolution::Won) {
                            continue;
                        }
                        if leg.shares <= Decimal::ZERO {
                            continue;
                        }
                        let _ = self.event_tx.send(LifecycleEvent::RedemptionQueued {
                            condition_id: cid.clone(),
                            token_id: leg.token_id.clone(),
                            shares: leg.shares,
                            expected_payout: leg.shares, // $1 per share
                        });
                    }
                }

                PositionState::Redeemable => {
                    // In paper mode, just log. In live mode, would call CTF redeem.
                    for leg in &pos.legs {
                        if leg.resolved == Some(LegResolution::Won) && leg.shares > Decimal::ZERO {
                            info!(
                                cid = %cid,
                                token = %leg.token_id,
                                shares = %leg.shares,
                                "PAPER: would redeem winning tokens"
                            );
                        }
                    }
                }

                PositionState::StaleExit => {
                    // Drip-sell everything at slight discount
                    for leg in &pos.legs {
                        if leg.shares <= Decimal::ZERO {
                            continue;
                        }
                        let drip_key = (cid.clone(), leg.token_id.clone());
                        let can_sell = self
                            .last_drip_sell
                            .get(&drip_key)
                            .map(|t| t.elapsed() > self.drip_sell_interval)
                            .unwrap_or(true);

                        if can_sell {
                            let batch = leg.shares.min(self.drip_sell_batch);
                            let mid = self
                                .store
                                .get_book(&leg.token_id)
                                .and_then(|b| b.mid_price());
                            let discount = Decimal::from_str("0.95").unwrap();
                            let target = mid
                                .map(|m| m * discount)
                                .unwrap_or(Decimal::from_str("0.01").unwrap());

                            let _ = self.event_tx.send(LifecycleEvent::DripSellQueued {
                                condition_id: cid.clone(),
                                token_id: leg.token_id.clone(),
                                shares: batch,
                                target_price: target,
                            });

                            self.last_drip_sell.insert(drip_key, now);
                        }
                    }
                }

                PositionState::Closed => {} // Nothing to do
            }
        }

        // Portfolio summary every 60 ticks (~60s at 1s interval)
        if self.scan_count % 60 == 0 {
            self.emit_summary();
        }
    }

    fn emit_summary(&self) {
        let mut active = 0usize;
        let mut exiting = 0usize;
        let mut redeemable = 0usize;
        let mut stale = 0usize;
        let mut total_cost = Decimal::ZERO;
        let mut total_mark = Decimal::ZERO;

        for pos in self.positions.values() {
            match pos.state {
                PositionState::Active => active += 1,
                PositionState::Exiting => exiting += 1,
                PositionState::Redeemable => redeemable += 1,
                PositionState::StaleExit => stale += 1,
                PositionState::Closed => {}
            }

            total_cost += pos.cost_basis;

            // Mark-to-market: use mid price for each leg
            for leg in &pos.legs {
                let mid = self
                    .store
                    .get_book(&leg.token_id)
                    .and_then(|b| b.mid_price())
                    .unwrap_or(leg.avg_cost);
                total_mark += leg.shares * mid;
            }
        }

        let _ = self.event_tx.send(LifecycleEvent::PortfolioSummary {
            total_positions: self.positions.len(),
            active,
            exiting,
            redeemable,
            stale,
            total_cost_basis: total_cost,
            total_mark_value: total_mark,
            unrealized_pnl: total_mark - total_cost,
        });
    }

    pub fn position_count(&self) -> usize {
        self.positions.len()
    }
}
