//! Market-making strategy for cross-outcome arbitrage.
//!
//! Posts passive bid orders on all outcomes of a market such that
//! bid_sum < $1.00. When all legs fill, you hold a guaranteed-profit
//! portfolio. Pays zero taker fees and earns maker rebates.
//!
//! The core insight: if you buy YES at $0.48 and NO at $0.50,
//! you spent $0.98 and one of them will pay $1.00. That's $0.02
//! risk-free profit per share — and you earned it passively.
//!
//! Key risks:
//! - Leg risk: only one side fills, leaving directional exposure
//! - Adverse selection: fills happen because the market knows something
//! - Inventory: accumulated one-sided positions need unwinding

use crate::arb::TrackedMarket;
use crate::auth::{AuthError, ClobApiClient, OrderRequest, OrderSide};
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

/// Configuration for the maker strategy.
#[derive(Debug, Clone)]
pub struct MakerConfig {
    /// Target bid_sum as fraction of $1.00 (e.g., 0.97 = $0.03 edge per round-trip).
    pub target_bid_sum: Decimal,
    /// Size per order in shares.
    pub order_size: Decimal,
    /// Minimum spread to post into (don't make when spread is too tight).
    pub min_spread: Decimal,
    /// Maximum inventory imbalance before pausing one side.
    /// e.g., if you hold 100 YES but 0 NO, you're exposed.
    pub max_inventory_imbalance: Decimal,
    /// How often to requote (minimum time between quote updates).
    pub requote_interval: Duration,
    /// Minimum price change to trigger a requote.
    pub requote_threshold: Decimal,
}

impl Default for MakerConfig {
    fn default() -> Self {
        Self {
            target_bid_sum: Decimal::from_str("0.97").unwrap(),
            order_size: Decimal::from(10),
            min_spread: Decimal::from_str("0.02").unwrap(),
            max_inventory_imbalance: Decimal::from(50),
            requote_interval: Duration::from_secs(5),
            requote_threshold: Decimal::from_str("0.01").unwrap(),
        }
    }
}

/// Tracks the state of a single market being made.
#[derive(Debug)]
struct MarketState {
    market: TrackedMarket,
    /// Current posted bids: token_id -> (order_id, price, size)
    live_orders: HashMap<String, LiveOrder>,
    /// Filled inventory: token_id -> shares held
    inventory: HashMap<String, Decimal>,
    /// Last time we requoted this market.
    last_requote: Instant,
    /// Last computed bid prices.
    last_prices: HashMap<String, Decimal>,
}

#[derive(Debug, Clone)]
struct LiveOrder {
    order_id: String,
    price: Decimal,
    size: Decimal,
}

/// Events from the maker strategy.
#[derive(Debug, Clone)]
pub enum MakerEvent {
    QuotesPosted {
        condition_id: String,
        question: String,
        bid_sum: Decimal,
        edge: Decimal,
    },
    QuotesUpdated {
        condition_id: String,
        bid_sum: Decimal,
    },
    QuotesCancelled {
        condition_id: String,
        reason: String,
    },
    LegFilled {
        condition_id: String,
        token_id: String,
        label: String,
        price: Decimal,
        size: Decimal,
    },
    InventoryAlert {
        condition_id: String,
        imbalance: Decimal,
    },
    RoundTripComplete {
        condition_id: String,
        profit: Decimal,
    },
}

/// The market maker engine.
pub struct MakerStrategy {
    api: Arc<ClobApiClient>,
    store: OrderBookStore,
    config: MakerConfig,
    states: HashMap<String, MarketState>,
    event_tx: mpsc::UnboundedSender<MakerEvent>,
}

impl MakerStrategy {
    pub fn new(
        api: Arc<ClobApiClient>,
        store: OrderBookStore,
        config: MakerConfig,
        event_tx: mpsc::UnboundedSender<MakerEvent>,
    ) -> Self {
        Self {
            api,
            store,
            config,
            states: HashMap::new(),
            event_tx,
        }
    }

    /// Register markets to make.
    pub fn add_markets(&mut self, markets: &[TrackedMarket]) {
        for market in markets {
            let state = MarketState {
                market: market.clone(),
                live_orders: HashMap::new(),
                inventory: market
                    .outcomes
                    .iter()
                    .map(|(_, tid)| (tid.clone(), Decimal::ZERO))
                    .collect(),
                last_requote: Instant::now() - Duration::from_secs(60),
                last_prices: HashMap::new(),
            };
            self.states.insert(market.condition_id.clone(), state);
        }
        info!(markets = self.states.len(), "maker tracking markets");
    }

    /// Main tick: for each market, check if we need to requote.
    pub async fn tick(&mut self) {
        let condition_ids: Vec<String> = self.states.keys().cloned().collect();

        for cid in condition_ids {
            if let Err(e) = self.tick_market(&cid).await {
                error!(condition_id = %cid, error = %e, "maker tick error");
            }
        }
    }

    async fn tick_market(&mut self, condition_id: &str) -> Result<(), AuthError> {
        let state = match self.states.get(condition_id) {
            Some(s) => s,
            None => return Ok(()),
        };

        // Check if enough time has passed since last requote
        if state.last_requote.elapsed() < self.config.requote_interval {
            return Ok(());
        }

        // Compute optimal bid prices from current books
        let new_prices = match self.compute_bids(&state.market) {
            Some(p) => p,
            None => return Ok(()), // no valid book data yet
        };

        // Check if prices have moved enough to warrant a requote
        let needs_requote = self.prices_changed(&state.last_prices, &new_prices);
        let has_no_orders = state.live_orders.is_empty();

        if !needs_requote && !has_no_orders {
            return Ok(());
        }

        // Check inventory imbalance — pause making if too one-sided
        if self.check_inventory_imbalance(condition_id) {
            return Ok(());
        }

        // Cancel existing orders if we have any
        let state = self.states.get(condition_id).unwrap();
        if !state.live_orders.is_empty() {
            self.api
                .cancel_market_orders(&state.market.condition_id)
                .await?;

            let _ = self.event_tx.send(MakerEvent::QuotesCancelled {
                condition_id: condition_id.to_string(),
                reason: "requote".to_string(),
            });
        }

        // Post new bids on all outcomes
        let orders: Vec<OrderRequest> = new_prices
            .iter()
            .map(|(tid, price)| OrderRequest {
                token_id: tid.clone(),
                price: price.to_string(),
                size: self.config.order_size.to_string(),
                side: OrderSide::Buy.as_str().to_string(),
            })
            .collect();

        let bid_sum: Decimal = new_prices.values().copied().sum();
        let edge = Decimal::ONE - bid_sum;

        if edge <= Decimal::ZERO {
            debug!(
                condition_id = condition_id,
                bid_sum = %bid_sum,
                "skipping — no edge at target bid sum"
            );
            return Ok(());
        }

        let result = self.api.place_orders(&orders).await;

        match result {
            Ok(resp) => {
                let state = self.states.get_mut(condition_id).unwrap();
                let is_first = state.live_orders.is_empty() && state.last_prices.is_empty();

                // Update state
                state.last_requote = Instant::now();
                state.last_prices = new_prices.clone();

                // TODO: parse order IDs from response and track in live_orders
                // For now we track by market-level cancel
                state.live_orders.clear();
                for (tid, price) in &new_prices {
                    state.live_orders.insert(
                        tid.clone(),
                        LiveOrder {
                            order_id: String::new(), // populated when we parse response
                            price: *price,
                            size: self.config.order_size,
                        },
                    );
                }

                if is_first {
                    let _ = self.event_tx.send(MakerEvent::QuotesPosted {
                        condition_id: condition_id.to_string(),
                        question: state.market.question.clone(),
                        bid_sum,
                        edge,
                    });
                } else {
                    let _ = self.event_tx.send(MakerEvent::QuotesUpdated {
                        condition_id: condition_id.to_string(),
                        bid_sum,
                    });
                }

                info!(
                    market = %state.market.question,
                    bid_sum = %bid_sum,
                    edge = %edge,
                    n_legs = orders.len(),
                    "quotes posted"
                );
            }
            Err(e) => {
                error!(
                    condition_id = condition_id,
                    error = %e,
                    "failed to post maker orders"
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Compute bid prices for each outcome such that bid_sum = target_bid_sum.
    ///
    /// Strategy: place each bid at a price that's proportional to the current
    /// mid price but scaled so the sum equals the target. This keeps our bids
    /// at a natural ratio (higher-probability outcomes get higher bid prices).
    fn compute_bids(&self, market: &TrackedMarket) -> Option<HashMap<String, Decimal>> {
        let mut mids = Vec::new();
        let hundred = Decimal::from(100);
        let one_cent = Decimal::from_str("0.01").unwrap();

        for (_, token_id) in &market.outcomes {
            let book = self.store.get_book(token_id)?;

            // Need both bid and ask to compute mid
            let best_bid = book.bids.best(true)?.0;
            let best_ask = book.asks.best(false)?.0;
            let spread = best_ask - best_bid;

            // Don't make into tiny spreads — we'd be providing liquidity
            // at prices where there's no room for edge
            if spread < self.config.min_spread {
                debug!(
                    token_id = %token_id,
                    spread = %spread,
                    min = %self.config.min_spread,
                    "spread too tight, skipping"
                );
                return None;
            }

            let mid = (best_bid + best_ask) / Decimal::from(2);
            mids.push((token_id.clone(), mid));
        }

        let mid_sum: Decimal = mids.iter().map(|(_, m)| *m).sum();

        if mid_sum == Decimal::ZERO {
            return None;
        }

        // Scale mids proportionally to reach target_bid_sum
        let mut prices = HashMap::new();
        for (tid, mid) in &mids {
            let scaled = *mid * self.config.target_bid_sum / mid_sum;
            // Round down to tick size 0.01
            let rounded = (scaled * hundred).floor() / hundred;
            let clamped = rounded.max(one_cent);
            prices.insert(tid.clone(), clamped);
        }

        // Adjust for rounding: if bid_sum != target after rounding,
        // add the remainder to the largest leg
        let bid_sum: Decimal = prices.values().copied().sum();
        let remainder = self.config.target_bid_sum - bid_sum;
        if remainder > Decimal::ZERO {
            // Add to the leg with the highest price (most likely to fill)
            if let Some((max_tid, _)) = mids.iter().max_by_key(|(_, m)| *m) {
                if let Some(p) = prices.get_mut(max_tid) {
                    *p += (remainder * hundred).floor() / hundred;
                }
            }
        }

        Some(prices)
    }

    /// Check if bid prices have moved enough to requote.
    fn prices_changed(
        &self,
        old: &HashMap<String, Decimal>,
        new: &HashMap<String, Decimal>,
    ) -> bool {
        if old.is_empty() {
            return true;
        }
        for (tid, new_price) in new {
            if let Some(old_price) = old.get(tid) {
                let diff = (*new_price - *old_price).abs();
                if diff >= self.config.requote_threshold {
                    return true;
                }
            } else {
                return true;
            }
        }
        false
    }

    /// Check if any market has dangerous inventory imbalance.
    /// Returns true if making should be paused.
    fn check_inventory_imbalance(&mut self, condition_id: &str) -> bool {
        let state = match self.states.get(condition_id) {
            Some(s) => s,
            None => return false,
        };

        let inventories: Vec<Decimal> = state.inventory.values().copied().collect();
        if inventories.len() < 2 {
            return false;
        }

        let max_inv = inventories.iter().copied().max().unwrap_or(Decimal::ZERO);
        let min_inv = inventories.iter().copied().min().unwrap_or(Decimal::ZERO);
        let imbalance = max_inv - min_inv;

        if imbalance > self.config.max_inventory_imbalance {
            warn!(
                condition_id = condition_id,
                imbalance = %imbalance,
                max = %self.config.max_inventory_imbalance,
                "inventory imbalance — pausing maker"
            );
            let _ = self.event_tx.send(MakerEvent::InventoryAlert {
                condition_id: condition_id.to_string(),
                imbalance,
            });
            return true;
        }

        false
    }

    /// Record a fill on one leg. Call this when the RTDS trade feed
    /// or user WS channel reports an order fill.
    pub fn record_fill(&mut self, condition_id: &str, token_id: &str, size: Decimal) {
        if let Some(state) = self.states.get_mut(condition_id) {
            let inv = state.inventory.entry(token_id.to_string()).or_insert(Decimal::ZERO);
            *inv += size;

            // Find label for logging
            let label = state
                .market
                .outcomes
                .iter()
                .find(|(_, tid)| tid == token_id)
                .map(|(l, _)| l.clone())
                .unwrap_or_default();

            let price = state
                .live_orders
                .get(token_id)
                .map(|o| o.price)
                .unwrap_or(Decimal::ZERO);

            info!(
                market = %state.market.question,
                label = %label,
                size = %size,
                price = %price,
                "maker leg filled"
            );

            let _ = self.event_tx.send(MakerEvent::LegFilled {
                condition_id: condition_id.to_string(),
                token_id: token_id.to_string(),
                label,
                price,
                size,
            });

            // Check if all legs have filled at least some amount
            self.check_round_trip(condition_id);
        }
    }

    /// Check if we've completed a round-trip (all outcomes filled).
    fn check_round_trip(&mut self, condition_id: &str) {
        let state = match self.states.get_mut(condition_id) {
            Some(s) => s,
            None => return,
        };

        // Find the minimum inventory across all outcomes
        let min_filled: Decimal = state
            .inventory
            .values()
            .copied()
            .min()
            .unwrap_or(Decimal::ZERO);

        if min_filled > Decimal::ZERO {
            // We have at least min_filled shares of EVERY outcome.
            // That's min_filled guaranteed round-trips.
            let total_cost: Decimal = state
                .inventory
                .iter()
                .filter_map(|(tid, _)| {
                    state.live_orders.get(tid).map(|o| o.price)
                })
                .sum::<Decimal>()
                * min_filled;

            let revenue = Decimal::ONE * min_filled; // $1.00 per share guaranteed
            let profit = revenue - total_cost;

            info!(
                market = %state.market.question,
                shares = %min_filled,
                cost = %total_cost,
                profit = %profit,
                "ROUND-TRIP COMPLETE"
            );

            let _ = self.event_tx.send(MakerEvent::RoundTripComplete {
                condition_id: condition_id.to_string(),
                profit,
            });

            // Subtract the completed round-trip from inventory
            for inv in state.inventory.values_mut() {
                *inv -= min_filled;
            }
        }
    }

    /// Cancel all maker orders across all markets.
    pub async fn cancel_all(&self) -> Result<(), AuthError> {
        warn!("cancelling all maker orders");
        self.api.cancel_all().await?;
        Ok(())
    }

    /// Get a summary of current maker state.
    pub fn summary(&self) -> Vec<MarketMakerSummary> {
        self.states
            .values()
            .map(|state| {
                let bid_sum: Decimal = state.live_orders.values().map(|o| o.price).sum();
                let total_inventory: Decimal = state.inventory.values().copied().sum();
                let inventories: Vec<Decimal> = state.inventory.values().copied().collect();
                let max_inv = inventories.iter().copied().max().unwrap_or(Decimal::ZERO);
                let min_inv = inventories.iter().copied().min().unwrap_or(Decimal::ZERO);

                MarketMakerSummary {
                    condition_id: state.market.condition_id.clone(),
                    question: state.market.question.clone(),
                    num_legs: state.market.outcomes.len(),
                    bid_sum,
                    edge: Decimal::ONE - bid_sum,
                    total_inventory,
                    imbalance: max_inv - min_inv,
                    active_orders: state.live_orders.len(),
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct MarketMakerSummary {
    pub condition_id: String,
    pub question: String,
    pub num_legs: usize,
    pub bid_sum: Decimal,
    pub edge: Decimal,
    pub total_inventory: Decimal,
    pub imbalance: Decimal,
    pub active_orders: usize,
}

impl std::fmt::Display for MarketMakerSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  legs={} bid_sum={} edge={} inv={} imbal={} orders={}",
            &self.question[..40.min(self.question.len())],
            self.num_legs,
            self.bid_sum,
            self.edge,
            self.total_inventory,
            self.imbalance,
            self.active_orders,
        )
    }
}
