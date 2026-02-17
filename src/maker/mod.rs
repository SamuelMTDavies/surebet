//! Passive market-making (liquidity-providing) strategy.
//!
//! Posts passive bid orders on all outcomes of a binary market such that
//! bid_sum < $1.00. When all legs fill, you hold a guaranteed-profit
//! portfolio. Pays zero taker fees and earns maker rebates.
//!
//! The core insight: if you buy YES at $0.48 and NO at $0.50,
//! you spent $0.98 and one of them will pay $1.00. That's $0.02
//! risk-free profit per share — and you earned it passively.
//!
//! Key risks and mitigations:
//! - Leg risk: only one side fills → aggressive re-price unfilled side
//! - Adverse selection: fills happen because market knows something → fill timeout + unwind
//! - Inventory: accumulated one-sided positions → max imbalance limit
//! - Dead markets: no taker flow → pre-flight activity check

use crate::arb::TrackedMarket;
use crate::auth::{AuthError, ClobApiClient, OrderRequest, OrderSide};
use crate::orderbook::OrderBookStore;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for the maker strategy.
#[derive(Debug, Clone)]
pub struct MakerConfig {
    /// Enable live execution (false = paper mode, logs without placing orders).
    pub execute: bool,
    /// Target bid_sum as fraction of $1.00 (e.g., 0.97 = $0.03 edge per round-trip).
    pub target_bid_sum: Decimal,
    /// Size per order in shares.
    pub order_size: Decimal,
    /// Minimum spread to post into (don't make when spread is too tight).
    pub min_spread: Decimal,
    /// Maximum inventory imbalance before pausing one side.
    pub max_inventory_imbalance: Decimal,
    /// How often to requote (minimum time between quote updates).
    pub requote_interval: Duration,
    /// Minimum price change to trigger a requote.
    pub requote_threshold: Decimal,
    /// Seconds to wait for the second leg to fill after the first.
    /// If exceeded, cancel unfilled side and sell the filled leg to unwind.
    pub fill_timeout: Duration,
    /// When one leg fills, bump unfilled leg's bid by this fraction of the
    /// remaining spread (e.g., 0.5 means move halfway to the ask).
    pub aggressive_reprice_pct: Decimal,
    /// Minimum age of last trade activity on BOTH sides before posting.
    /// Markets with no recent two-sided flow are skipped.
    pub min_activity_age: Duration,
}

impl Default for MakerConfig {
    fn default() -> Self {
        Self {
            execute: false,
            target_bid_sum: Decimal::from_str("0.97").unwrap(),
            order_size: Decimal::from(10),
            min_spread: Decimal::from_str("0.02").unwrap(),
            max_inventory_imbalance: Decimal::from(50),
            requote_interval: Duration::from_secs(5),
            requote_threshold: Decimal::from_str("0.01").unwrap(),
            fill_timeout: Duration::from_secs(30),
            aggressive_reprice_pct: Decimal::from_str("0.50").unwrap(),
            min_activity_age: Duration::from_secs(300), // 5 minutes
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
    /// When the first leg filled (None = no partial fills pending).
    first_fill_at: Option<Instant>,
    /// Which token_id filled first (so we know which side to unwind).
    first_fill_token: Option<String>,
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
        order_id: String,
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
    AggressiveReprice {
        condition_id: String,
        token_id: String,
        old_price: Decimal,
        new_price: Decimal,
    },
    FillTimeout {
        condition_id: String,
        filled_token: String,
        unfilled_tokens: Vec<String>,
    },
    Unwinding {
        condition_id: String,
        token_id: String,
        size: Decimal,
    },
    MarketSkipped {
        condition_id: String,
        reason: String,
    },
}

/// Tracks last trade activity per token_id (fed from RTDS).
#[derive(Debug, Clone, Default)]
pub struct ActivityTracker {
    last_trade: HashMap<String, Instant>,
}

impl ActivityTracker {
    pub fn record_trade(&mut self, asset_id: &str) {
        self.last_trade.insert(asset_id.to_string(), Instant::now());
    }

    /// Returns the age of the most recent trade for a given token.
    /// Returns None if no trade has ever been seen.
    pub fn last_trade_age(&self, asset_id: &str) -> Option<Duration> {
        self.last_trade.get(asset_id).map(|t| t.elapsed())
    }
}

/// The market maker engine.
pub struct MakerStrategy {
    api: Option<Arc<ClobApiClient>>,
    store: OrderBookStore,
    config: MakerConfig,
    states: HashMap<String, MarketState>,
    event_tx: mpsc::UnboundedSender<MakerEvent>,
    activity: ActivityTracker,
    /// Tick counter for periodic diagnostics.
    tick_count: u64,
}

impl MakerStrategy {
    pub fn new(
        api: Option<Arc<ClobApiClient>>,
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
            activity: ActivityTracker::default(),
            tick_count: 0,
        }
    }

    /// Whether the strategy can execute live orders.
    fn can_execute(&self) -> bool {
        self.config.execute && self.api.is_some()
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
                first_fill_at: None,
                first_fill_token: None,
            };
            self.states.insert(market.condition_id.clone(), state);
        }
        info!(markets = self.states.len(), "maker tracking markets");
    }

    /// Feed RTDS trade events to the activity tracker.
    /// Call this from the main loop when you receive RTDS trades.
    pub fn record_activity(&mut self, asset_id: &str) {
        self.activity.record_trade(asset_id);
    }

    /// Check if an observed RTDS trade would have filled one of our paper bids.
    ///
    /// A sell-side trade at price <= our bid means a seller would have hit our
    /// resting bid order. We simulate the fill by calling `record_fill`, which
    /// triggers the full state machine (inventory, round-trip check, repricing).
    ///
    /// Returns the (condition_id, fill_size) if a paper fill was triggered.
    pub async fn check_paper_fill(
        &mut self,
        asset_id: &str,
        trade_price: Decimal,
        trade_size: Decimal,
        trade_side: &str,
    ) -> Option<(String, Decimal)> {
        // Only sell-side trades can fill our buy (bid) orders.
        // RTDS "side" field: "SELL" means the taker was selling → hits resting bids.
        // Some feeds use "BUY"/"SELL", others "buy"/"sell".
        let is_sell = trade_side.eq_ignore_ascii_case("sell");
        if !is_sell {
            return None;
        }

        // Find which market/state has a live paper order on this asset_id
        let mut match_info: Option<(String, Decimal, Decimal)> = None;

        for (condition_id, state) in &self.states {
            if let Some(order) = state.live_orders.get(asset_id) {
                // A sell at or below our bid price would fill us
                if trade_price <= order.price {
                    // Fill size is min of trade size and our remaining order size
                    let already_filled = state
                        .inventory
                        .get(asset_id)
                        .copied()
                        .unwrap_or(Decimal::ZERO);
                    let remaining = order.size - already_filled;
                    if remaining > Decimal::ZERO {
                        let fill_size = trade_size.min(remaining);
                        match_info = Some((condition_id.clone(), order.price, fill_size));
                        break;
                    }
                }
            }
        }

        if let Some((condition_id, bid_price, fill_size)) = match_info {
            info!(
                asset = %asset_id,
                trade_price = %trade_price,
                our_bid = %bid_price,
                fill_size = %fill_size,
                "PAPER FILL — trade matched our bid"
            );

            if let Err(e) = self.record_fill(&condition_id, asset_id, fill_size).await {
                error!(
                    condition_id = %condition_id,
                    error = %e,
                    "paper fill record error"
                );
            }

            return Some((condition_id, fill_size));
        }

        None
    }

    /// Main tick: for each market, check requotes, fill timeouts, and unwinds.
    pub async fn tick(&mut self) {
        self.tick_count += 1;
        let condition_ids: Vec<String> = self.states.keys().cloned().collect();

        for cid in &condition_ids {
            // Check fill timeouts before normal requoting
            if let Err(e) = self.check_fill_timeout(cid).await {
                error!(condition_id = %cid, error = %e, "fill timeout check error");
            }
        }

        for cid in condition_ids {
            if let Err(e) = self.tick_market(&cid).await {
                error!(condition_id = %cid, error = %e, "maker tick error");
            }
        }

        // Emit diagnostics every 30 ticks
        if self.tick_count % 30 == 0 {
            self.emit_diagnostics();
        }
    }

    /// Emit periodic diagnostics about maker health.
    fn emit_diagnostics(&self) {
        let total_markets = self.states.len();
        let mut active_quotes = 0usize;
        let mut pending_fills = 0usize;
        let mut total_inventory = Decimal::ZERO;
        let mut markets_with_books = 0usize;
        let mut skipped_no_activity = 0usize;
        let mut skipped_tight_spread = 0usize;
        // Track the best opportunity (widest spread that passes non-activity filters)
        let mut best_spread: Option<Decimal> = None;
        let mut best_spread_market: Option<String> = None;
        let mut best_spread_edge: Option<Decimal> = None;
        // Count markets that would qualify if activity filter were relaxed
        let mut would_qualify = 0usize;

        for state in self.states.values() {
            if !state.live_orders.is_empty() {
                active_quotes += 1;
            }
            if state.first_fill_at.is_some() {
                pending_fills += 1;
            }
            let inv: Decimal = state.inventory.values().copied().sum();
            total_inventory += inv;

            // Check if this market has book data and compute edge
            let has_books = state.market.outcomes.iter().all(|(_, tid)| {
                self.store
                    .get_book(tid)
                    .and_then(|b| b.asks.best(false))
                    .is_some()
            });

            if has_books {
                markets_with_books += 1;

                let has_activity = self.check_market_activity(&state.market);
                let has_bids = self.compute_bids(&state.market).is_some();

                if !has_activity {
                    skipped_no_activity += 1;
                } else if !has_bids {
                    skipped_tight_spread += 1;
                }

                // Compute spread and edge for best-opportunity tracking
                // (ignoring activity filter — we want to see what we're missing)
                if let Some(prices) = self.compute_bids(&state.market) {
                    let bid_sum: Decimal = prices.values().copied().sum();
                    let edge = Decimal::ONE - bid_sum;

                    if edge > Decimal::ZERO {
                        would_qualify += 1;
                    }

                    // Compute the widest per-outcome spread in this market
                    let widest_spread = state.market.outcomes.iter().filter_map(|(_, tid)| {
                        let book = self.store.get_book(tid)?;
                        let bid = book.bids.best(true)?.0;
                        let ask = book.asks.best(false)?.0;
                        Some(ask - bid)
                    }).max();

                    if let Some(spread) = widest_spread {
                        if best_spread.is_none() || spread > best_spread.unwrap() {
                            best_spread = Some(spread);
                            best_spread_edge = Some(edge);
                            best_spread_market = Some(state.market.question.clone());
                        }
                    }
                }
            }
        }

        info!(
            total = total_markets,
            with_books = markets_with_books,
            quoting = active_quotes,
            pending_fills = pending_fills,
            inventory = %total_inventory,
            best_spread = ?best_spread,
            best_edge = ?best_spread_edge,
            best = ?best_spread_market.as_deref().map(|s| &s[..s.len().min(50)]),
            would_qualify = would_qualify,
            skipped_activity = skipped_no_activity,
            skipped_spread = skipped_tight_spread,
            "MAKER diagnostics"
        );
    }

    /// Check if any market has a partial fill that's timed out.
    /// If so, cancel unfilled orders and sell the filled leg to unwind.
    async fn check_fill_timeout(&mut self, condition_id: &str) -> Result<(), AuthError> {
        let state = match self.states.get(condition_id) {
            Some(s) => s,
            None => return Ok(()),
        };

        let first_fill_at = match state.first_fill_at {
            Some(t) => t,
            None => return Ok(()), // no partial fill pending
        };

        if first_fill_at.elapsed() < self.config.fill_timeout {
            return Ok(()); // still within timeout window
        }

        // Timeout exceeded — identify unfilled legs and unwind
        let filled_token = state.first_fill_token.clone().unwrap_or_default();
        let unfilled_tokens: Vec<String> = state
            .inventory
            .iter()
            .filter(|(tid, inv)| **inv == Decimal::ZERO && **tid != filled_token)
            .map(|(tid, _)| tid.clone())
            .collect();

        if unfilled_tokens.is_empty() {
            // All legs actually filled — shouldn't be in timeout state
            return Ok(());
        }

        warn!(
            condition_id = condition_id,
            filled = %filled_token,
            unfilled = ?unfilled_tokens,
            elapsed_secs = first_fill_at.elapsed().as_secs(),
            "FILL TIMEOUT — unwinding"
        );

        let _ = self.event_tx.send(MakerEvent::FillTimeout {
            condition_id: condition_id.to_string(),
            filled_token: filled_token.clone(),
            unfilled_tokens: unfilled_tokens.clone(),
        });

        // 1. Cancel all unfilled orders in this market
        let market_cid = &self
            .states
            .get(condition_id)
            .unwrap()
            .market
            .condition_id;

        if self.can_execute() {
            self.api.as_ref().unwrap().cancel_market_orders(market_cid).await?;
        } else {
            info!(condition_id = condition_id, "PAPER: would cancel market orders for unwind");
        }

        let _ = self.event_tx.send(MakerEvent::QuotesCancelled {
            condition_id: condition_id.to_string(),
            reason: "fill timeout — unwinding".to_string(),
        });

        // 2. Sell the filled leg to close the position
        let state = self.states.get(condition_id).unwrap();
        let filled_inv = state
            .inventory
            .get(&filled_token)
            .copied()
            .unwrap_or(Decimal::ZERO);

        if filled_inv > Decimal::ZERO {
            // Get the current best bid for the filled token to sell into
            if let Some(book) = self.store.get_book(&filled_token) {
                if let Some((best_bid, _)) = book.bids.best(true) {
                    let _ = self.event_tx.send(MakerEvent::Unwinding {
                        condition_id: condition_id.to_string(),
                        token_id: filled_token.clone(),
                        size: filled_inv,
                    });

                    if self.can_execute() {
                        info!(
                            condition_id = condition_id,
                            token = %filled_token,
                            size = %filled_inv,
                            sell_price = %best_bid,
                            "unwinding filled leg"
                        );
                        let _resp = self
                            .api
                            .as_ref()
                            .unwrap()
                            .place_order(
                                &filled_token,
                                &best_bid.to_string(),
                                &filled_inv.to_string(),
                                OrderSide::Sell,
                            )
                            .await?;
                    } else {
                        info!(
                            condition_id = condition_id,
                            token = %filled_token,
                            size = %filled_inv,
                            sell_price = %best_bid,
                            "PAPER: would unwind filled leg"
                        );
                    }
                }
            }
        }

        // 3. Reset state
        let state = self.states.get_mut(condition_id).unwrap();
        state.first_fill_at = None;
        state.first_fill_token = None;
        state.live_orders.clear();
        // Zero out inventory (sell order will handle the actual position)
        for inv in state.inventory.values_mut() {
            *inv = Decimal::ZERO;
        }

        Ok(())
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

        // Pre-flight: check that both sides have recent trading activity
        if !self.check_market_activity(&state.market) {
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
            if self.can_execute() {
                self.api
                    .as_ref()
                    .unwrap()
                    .cancel_market_orders(&state.market.condition_id)
                    .await?;
            } else {
                debug!(condition_id = condition_id, "PAPER: would cancel existing orders for requote");
            }

            let _ = self.event_tx.send(MakerEvent::QuotesCancelled {
                condition_id: condition_id.to_string(),
                reason: "requote".to_string(),
            });
        }

        // Build new bids on all outcomes
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

        // In paper mode, log what we'd place. In live mode, call the API.
        let order_responses: Vec<(String, String)> = if self.can_execute() {
            let result = self.api.as_ref().unwrap().place_orders(&orders).await;
            match result {
                Ok(resp) => {
                    // Match responses to orders by position (API returns in same order)
                    orders
                        .iter()
                        .zip(resp.iter())
                        .map(|(req, r)| (req.token_id.clone(), r.order_id.clone()))
                        .collect()
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
        } else {
            // Paper mode: generate synthetic order IDs for internal tracking
            orders
                .iter()
                .enumerate()
                .map(|(i, req)| {
                    let paper_id = format!("paper-{}-{}-{}", condition_id.get(..8).unwrap_or(condition_id), i, chrono::Utc::now().timestamp_millis());
                    info!(
                        token = %req.token_id.get(..12).unwrap_or(&req.token_id),
                        price = %req.price,
                        size = %req.size,
                        paper_id = %paper_id,
                        "PAPER: would place BUY order"
                    );
                    (req.token_id.clone(), paper_id)
                })
                .collect()
        };

        // Update internal state regardless of paper/live mode
        let mode = if self.can_execute() { "" } else { " [PAPER]" };
        let order_size = self.config.order_size;
        let state = self.states.get_mut(condition_id).unwrap();
        let is_first = state.live_orders.is_empty() && state.last_prices.is_empty();

        state.last_requote = Instant::now();
        state.last_prices = new_prices.clone();

        // Populate live_orders with real or paper order IDs
        state.live_orders.clear();
        for (tid, order_id) in &order_responses {
            if let Some(price) = new_prices.get(tid) {
                state.live_orders.insert(
                    tid.clone(),
                    LiveOrder {
                        order_id: order_id.clone(),
                        price: *price,
                        size: order_size,
                    },
                );
            }
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
            "quotes posted{}", mode
        );

        Ok(())
    }

    /// Pre-flight check: verify that both sides of a binary market have
    /// recent trading activity. If one side has no flow, our passive bid
    /// on that side will never fill, creating permanent leg risk.
    fn check_market_activity(&self, market: &TrackedMarket) -> bool {
        for (label, token_id) in &market.outcomes {
            match self.activity.last_trade_age(token_id) {
                None => {
                    debug!(
                        market = %market.question,
                        outcome = %label,
                        "no trade activity seen — skipping"
                    );
                    let _ = self.event_tx.send(MakerEvent::MarketSkipped {
                        condition_id: market.condition_id.clone(),
                        reason: format!("no activity on {}", label),
                    });
                    return false;
                }
                Some(age) if age > self.config.min_activity_age => {
                    debug!(
                        market = %market.question,
                        outcome = %label,
                        age_secs = age.as_secs(),
                        max = self.config.min_activity_age.as_secs(),
                        "stale activity — skipping"
                    );
                    let _ = self.event_tx.send(MakerEvent::MarketSkipped {
                        condition_id: market.condition_id.clone(),
                        reason: format!("{} last traded {}s ago", label, age.as_secs()),
                    });
                    return false;
                }
                Some(_) => {} // recent enough
            }
        }
        true
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
    /// or user WS channel reports an order fill on our token.
    ///
    /// This triggers:
    /// 1. Inventory update
    /// 2. Round-trip check (did all legs fill?)
    /// 3. Aggressive re-pricing of unfilled legs to attract takers
    pub async fn record_fill(
        &mut self,
        condition_id: &str,
        token_id: &str,
        size: Decimal,
    ) -> Result<(), AuthError> {
        let state = match self.states.get_mut(condition_id) {
            Some(s) => s,
            None => return Ok(()),
        };

        let inv = state
            .inventory
            .entry(token_id.to_string())
            .or_insert(Decimal::ZERO);
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

        // Get the order_id if we have it
        let order_id = state
            .live_orders
            .get(token_id)
            .map(|o| o.order_id.clone())
            .unwrap_or_default();

        let _ = self.event_tx.send(MakerEvent::LegFilled {
            order_id,
            condition_id: condition_id.to_string(),
            token_id: token_id.to_string(),
            label,
            price,
            size,
        });

        // Start the fill timeout clock if this is the first partial fill
        if state.first_fill_at.is_none() {
            let has_unfilled = state
                .inventory
                .iter()
                .any(|(tid, inv)| *tid != token_id && *inv == Decimal::ZERO);

            if has_unfilled {
                info!(
                    condition_id = condition_id,
                    filled = %token_id,
                    timeout_secs = self.config.fill_timeout.as_secs(),
                    "first leg filled — starting fill timeout clock"
                );
                state.first_fill_at = Some(Instant::now());
                state.first_fill_token = Some(token_id.to_string());
            }
        }

        // Check if all legs have filled (round-trip complete)
        if self.check_round_trip(condition_id) {
            return Ok(()); // round-trip handled, no need to reprice
        }

        // If we have a partial fill, aggressively reprice unfilled legs
        self.aggressive_reprice(condition_id).await?;

        Ok(())
    }

    /// When one leg fills, bump the unfilled legs' bids closer to their
    /// respective ask prices to attract a taker fill and complete the round-trip.
    ///
    /// The idea: we already have directional exposure on the filled leg, so
    /// it's worth sacrificing some edge to get the other leg filled quickly.
    async fn aggressive_reprice(&mut self, condition_id: &str) -> Result<(), AuthError> {
        let state = match self.states.get(condition_id) {
            Some(s) => s,
            None => return Ok(()),
        };

        // Only reprice if we have a partial fill (some legs filled, some not)
        let has_filled = state.inventory.values().any(|v| *v > Decimal::ZERO);
        let has_unfilled = state.inventory.values().any(|v| *v == Decimal::ZERO);

        if !has_filled || !has_unfilled {
            return Ok(());
        }

        // Find unfilled legs and their current orders
        let hundred = Decimal::from(100);
        let mut reprice_orders = Vec::new();

        for (tid, inv) in &state.inventory {
            if *inv > Decimal::ZERO {
                continue; // already filled
            }

            let old_price = match state.live_orders.get(tid) {
                Some(o) => o.price,
                None => continue,
            };

            // Get the current ask price for this token
            let best_ask = match self.store.get_book(tid) {
                Some(book) => match book.asks.best(false) {
                    Some((ask, _)) => ask,
                    None => continue,
                },
                None => continue,
            };

            // Move our bid up toward the ask by aggressive_reprice_pct of the gap
            let gap = best_ask - old_price;
            if gap <= Decimal::ZERO {
                continue;
            }

            let bump = gap * self.config.aggressive_reprice_pct;
            let new_price = old_price + (bump * hundred).floor() / hundred;

            // Don't bid above the ask (that would be a taker order)
            let one_cent = Decimal::from_str("0.01").unwrap();
            let new_price = new_price.min(best_ask - one_cent);

            if new_price <= old_price {
                continue;
            }

            info!(
                condition_id = condition_id,
                token = %tid,
                old_price = %old_price,
                new_price = %new_price,
                best_ask = %best_ask,
                "aggressive reprice — chasing unfilled leg"
            );

            let _ = self.event_tx.send(MakerEvent::AggressiveReprice {
                condition_id: condition_id.to_string(),
                token_id: tid.clone(),
                old_price,
                new_price,
            });

            reprice_orders.push(OrderRequest {
                token_id: tid.clone(),
                price: new_price.to_string(),
                size: self.config.order_size.to_string(),
                side: OrderSide::Buy.as_str().to_string(),
            });
        }

        if reprice_orders.is_empty() {
            return Ok(());
        }

        // Cancel existing orders in this market, then post the aggressive bids
        let market_cid = &state.market.condition_id;

        if self.can_execute() {
            self.api.as_ref().unwrap().cancel_market_orders(market_cid).await?;
            self.api.as_ref().unwrap().place_orders(&reprice_orders).await?;
        } else {
            for order in &reprice_orders {
                info!(
                    condition_id = condition_id,
                    token = %order.token_id.get(..12).unwrap_or(&order.token_id),
                    price = %order.price,
                    "PAPER: would reprice unfilled leg"
                );
            }
        }

        // Update live_orders with new prices
        let state = self.states.get_mut(condition_id).unwrap();
        for order in &reprice_orders {
            if let Some(live) = state.live_orders.get_mut(&order.token_id) {
                live.price = Decimal::from_str(&order.price).unwrap_or(live.price);
            }
        }

        Ok(())
    }

    /// Check if we've completed a round-trip (all outcomes filled).
    /// Returns true if a round-trip was detected and handled.
    fn check_round_trip(&mut self, condition_id: &str) -> bool {
        let state = match self.states.get_mut(condition_id) {
            Some(s) => s,
            None => return false,
        };

        // Find the minimum inventory across all outcomes
        let min_filled: Decimal = state
            .inventory
            .values()
            .copied()
            .min()
            .unwrap_or(Decimal::ZERO);

        if min_filled <= Decimal::ZERO {
            return false;
        }

        // We have at least min_filled shares of EVERY outcome.
        // That's min_filled guaranteed round-trips.
        let total_cost: Decimal = state
            .inventory
            .iter()
            .filter_map(|(tid, _)| state.live_orders.get(tid).map(|o| o.price))
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

        // Clear the fill timeout since we completed the round-trip
        state.first_fill_at = None;
        state.first_fill_token = None;

        true
    }

    /// Cancel all maker orders across all markets.
    pub async fn cancel_all(&self) -> Result<(), AuthError> {
        if let Some(ref api) = self.api {
            warn!("cancelling all maker orders");
            api.cancel_all().await?;
        } else {
            info!("PAPER: would cancel all maker orders (no API client)");
        }
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
                    pending_fill: state.first_fill_at.is_some(),
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
    pub pending_fill: bool,
}

impl std::fmt::Display for MarketMakerSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  legs={} bid_sum={} edge={} inv={} imbal={} orders={} pending={}",
            &self.question[..40.min(self.question.len())],
            self.num_legs,
            self.bid_sum,
            self.edge,
            self.total_inventory,
            self.imbalance,
            self.active_orders,
            self.pending_fill,
        )
    }
}
