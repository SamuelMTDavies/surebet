//! Snipe executor — paper mode only for now.
//!
//! Logs snipe opportunities with full P&L estimates. When wallet connection
//! is added, this will submit aggressive CLOB orders to sweep the book.

use crate::auth::ClobApiClient;
use crate::orderbook::OrderBookStore;
use crate::sniper::{SnipeAction, SnipeOrder, SnipeSide};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Paper-mode snipe executor. Estimates P&L from order book state without
/// placing any orders.
pub struct SniperExecutor {
    store: OrderBookStore,
    api: Option<Arc<ClobApiClient>>,
    execute: bool,
}

impl SniperExecutor {
    pub fn new(store: OrderBookStore, api: Option<Arc<ClobApiClient>>, execute: bool) -> Self {
        Self {
            store,
            api,
            execute,
        }
    }

    /// Process a snipe action. In paper mode, estimates the available edge.
    /// `detected_at` is the monotonic instant when the on-chain event was first
    /// received — used to measure end-to-end pipeline latency.
    /// Returns estimated profit in USDC.
    pub async fn execute(&self, action: &SnipeAction, detected_at: Instant) -> Decimal {
        let start = Instant::now();
        let mut total_estimated_profit = Decimal::ZERO;

        for order in &action.orders {
            let estimated = self.estimate_snipe_profit(order);

            let side_str = match order.side {
                SnipeSide::BuyWinner => "BUY_WINNER",
                SnipeSide::SellLoser => "SELL_LOSER",
            };

            let e2e_us = detected_at.elapsed().as_micros();

            info!(
                market = %action.question,
                label = %order.label,
                side = side_str,
                limit = %order.limit_price,
                estimated_profit = %estimated.profit,
                sweepable_shares = %estimated.sweepable_shares,
                avg_price = %estimated.avg_fill_price,
                levels_consumed = estimated.levels_consumed,
                source = %action.signal.source,
                confidence = action.signal.confidence,
                exec_us = start.elapsed().as_micros(),
                e2e_us = e2e_us,
                "SNIPE PAPER TRADE"
            );

            total_estimated_profit += estimated.profit;
        }

        if total_estimated_profit > Decimal::ZERO {
            info!(
                market = %action.question,
                total_profit = %total_estimated_profit,
                orders = action.orders.len(),
                pipeline_us = detected_at.elapsed().as_micros(),
                "SNIPE TOTAL ESTIMATE"
            );
        }

        total_estimated_profit
    }

    /// Estimate the profit from a single snipe order by walking the book.
    fn estimate_snipe_profit(&self, order: &SnipeOrder) -> SnipeEstimate {
        let book = match self.store.get_book(&order.token_id) {
            Some(b) => b,
            None => {
                return SnipeEstimate::zero();
            }
        };

        match order.side {
            SnipeSide::BuyWinner => {
                // Sweep asks up to limit_price. Each share bought below $1.00
                // will pay out $1.00 at resolution, so profit = (1.00 - price) per share.
                let mut total_cost = Decimal::ZERO;
                let mut total_shares = Decimal::ZERO;
                let mut levels = 0usize;

                for (&price, &size) in book.asks.levels.iter() {
                    if price > order.limit_price {
                        break;
                    }
                    total_cost += price * size;
                    total_shares += size;
                    levels += 1;
                }

                let payout = total_shares; // $1 per share at resolution
                let profit = payout - total_cost;
                let avg_price = if total_shares > Decimal::ZERO {
                    total_cost / total_shares
                } else {
                    Decimal::ZERO
                };

                SnipeEstimate {
                    profit,
                    sweepable_shares: total_shares,
                    avg_fill_price: avg_price,
                    levels_consumed: levels,
                }
            }
            SnipeSide::SellLoser => {
                // Hit bids above min price. Each share sold above $0.00
                // avoids a total loss, so profit = price per share.
                let mut total_revenue = Decimal::ZERO;
                let mut total_shares = Decimal::ZERO;
                let mut levels = 0usize;

                for (&price, &size) in book.bids.levels.iter().rev() {
                    if price < order.limit_price {
                        break;
                    }
                    total_revenue += price * size;
                    total_shares += size;
                    levels += 1;
                }

                let avg_price = if total_shares > Decimal::ZERO {
                    total_revenue / total_shares
                } else {
                    Decimal::ZERO
                };

                SnipeEstimate {
                    profit: total_revenue, // All revenue is profit (tokens worth $0)
                    sweepable_shares: total_shares,
                    avg_fill_price: avg_price,
                    levels_consumed: levels,
                }
            }
        }
    }
}

struct SnipeEstimate {
    profit: Decimal,
    sweepable_shares: Decimal,
    avg_fill_price: Decimal,
    levels_consumed: usize,
}

impl SnipeEstimate {
    fn zero() -> Self {
        Self {
            profit: Decimal::ZERO,
            sweepable_shares: Decimal::ZERO,
            avg_fill_price: Decimal::ZERO,
            levels_consumed: 0,
        }
    }
}
