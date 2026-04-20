//! Resolution Watchlist — "Stalk and Strike" sniper.
//!
//! When a ProposePrice event fires but the orderbook is empty or unprofitable,
//! the market is added to a dedicated CLOB WebSocket subscription. The actor
//! monitors book updates and fires pre-built orders the instant favorable
//! liquidity appears.
//!
//! Lifecycle:
//!   ProposePrice (no book) → Add to watchlist → dedicated CLOB WS
//!     → book update → log opportunity → threshold met → LiquidityReady
//!   Dispute / Finalize / TTL(7100s) → Remove from watchlist

use crate::market;
use crate::onchain::cache::CachedMarket;
use crate::orderbook::OrderBookStore;
use crate::sniper::SnipeSide;
use crate::ws::clob::{start_clob_ws, ClobEvent};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

// ─── Types ───────────────────────────────────────────────────────────────────

/// A market waiting for favorable liquidity after ProposePrice.
#[derive(Debug, Clone)]
pub struct WatchlistEntry {
    pub market: CachedMarket,
    /// Which outcome index is the winner (0 = YES, 1 = NO).
    pub winning_idx: usize,
    /// Raw proposed price from UMA (>0 = YES wins).
    pub proposed_price: i64,
    /// When the ProposePrice event was detected (monotonic clock).
    pub detected_at: Instant,
    /// On-chain detection latency in ms.
    pub chain_latency_ms: u64,
    /// Pre-built orders ready to fire when liquidity appears.
    pub prepared_orders: Vec<PreparedOrder>,
}

/// A pre-built order template ready to fire on liquidity trigger.
#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub token_id: String,
    pub outcome_label: String,
    pub side: SnipeSide,
    /// For BUY: max price to pay. For SELL: min price to accept.
    pub limit_price: Decimal,
    /// Max shares to fill (= max_position_usd / limit_price).
    pub max_shares: Decimal,
}

/// Commands sent from the main event loop → watchlist actor.
pub enum WatchlistCmd {
    /// Add a market to the watchlist (no liquidity at detection time).
    Add(WatchlistEntry),
    /// Remove by condition_id (dispute, finalization, or explicit).
    Remove { condition_id: String },
}

/// Events sent from the watchlist actor → main event loop.
#[derive(Debug)]
pub enum WatchlistEvent {
    /// Favorable liquidity detected — ready to execute.
    LiquidityReady {
        entry: WatchlistEntry,
        order: PreparedOrder,
        best_price: Decimal,
        available_size: Decimal,
        estimated_profit: Decimal,
    },
    /// Entry expired (TTL reached without liquidity).
    Expired {
        condition_id: String,
        question: String,
        waited_secs: u64,
    },
}

// ─── Config ──────────────────────────────────────────────────────────────────

/// Configuration for the watchlist, extracted from SniperConfig.
#[derive(Debug, Clone)]
pub struct WatchlistConfig {
    pub max_buy_price: Decimal,
    pub min_sell_price: Decimal,
    pub ttl: Duration,
    pub max_position_usd: Decimal,
}

// ─── Actor ───────────────────────────────────────────────────────────────────

/// The resolution watchlist actor. Owns a dedicated CLOB WS connection
/// for a small, dynamic set of tokens.
pub struct ResolutionWatchlist {
    /// Active entries keyed by condition_id.
    entries: HashMap<String, WatchlistEntry>,
    /// Reverse lookup: token_id → condition_id.
    token_to_condition: HashMap<String, String>,
    /// Shared orderbook store (same instance as main loop).
    store: OrderBookStore,
    /// Commands from main loop.
    cmd_rx: mpsc::UnboundedReceiver<WatchlistCmd>,
    /// Events back to main loop.
    event_tx: mpsc::UnboundedSender<WatchlistEvent>,
    /// Handle to the current dedicated WS task (aborted on rebuild).
    ws_handle: Option<JoinHandle<()>>,
    /// Channel for dedicated WS events.
    ws_event_tx: mpsc::UnboundedSender<ClobEvent>,
    ws_event_rx: mpsc::UnboundedReceiver<ClobEvent>,
    /// Config thresholds.
    config: WatchlistConfig,
}

impl ResolutionWatchlist {
    /// Spawn the watchlist actor as a background tokio task.
    pub fn spawn(
        store: OrderBookStore,
        cmd_rx: mpsc::UnboundedReceiver<WatchlistCmd>,
        event_tx: mpsc::UnboundedSender<WatchlistEvent>,
        config: WatchlistConfig,
    ) -> JoinHandle<()> {
        let (ws_event_tx, ws_event_rx) = mpsc::unbounded_channel();
        let actor = Self {
            entries: HashMap::new(),
            token_to_condition: HashMap::new(),
            store,
            cmd_rx,
            event_tx,
            ws_handle: None,
            ws_event_tx,
            ws_event_rx,
            config,
        };
        tokio::spawn(actor.run())
    }

    async fn run(mut self) {
        let mut expiry_tick = tokio::time::interval(Duration::from_secs(5));

        info!(
            ttl_secs = self.config.ttl.as_secs(),
            max_buy = %self.config.max_buy_price,
            min_sell = %self.config.min_sell_price,
            max_usd = %self.config.max_position_usd,
            "watchlist actor started"
        );

        loop {
            tokio::select! {
                // ── Commands from main loop ──
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WatchlistCmd::Add(entry)) => self.handle_add(entry),
                        Some(WatchlistCmd::Remove { condition_id }) => self.handle_remove(&condition_id),
                        None => {
                            info!("watchlist cmd channel closed, shutting down");
                            break;
                        }
                    }
                }

                // ── Book updates from dedicated WS ──
                Some(event) = self.ws_event_rx.recv() => {
                    self.handle_ws_event(event);
                }

                // ── Expiry check ──
                _ = expiry_tick.tick() => {
                    self.check_expiry();
                }
            }
        }

        // Cleanup
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }
    }

    // ── Command Handlers ─────────────────────────────────────────────────

    fn handle_add(&mut self, entry: WatchlistEntry) {
        let cid = entry.market.condition_id.clone();
        let question = &entry.market.question;

        // De-duplicate
        if self.entries.contains_key(&cid) {
            debug!(condition_id = %cid, "watchlist: already tracking, skipping duplicate");
            return;
        }

        // Register token→condition reverse mapping
        for token_id in &entry.market.clob_token_ids {
            self.token_to_condition.insert(token_id.clone(), cid.clone());
        }

        let order_count = entry.prepared_orders.len();
        warn!(
            condition_id = %&cid[..12.min(cid.len())],
            question = %question,
            tokens = entry.market.clob_token_ids.len(),
            orders = order_count,
            "WATCHLIST: added — waiting for liquidity"
        );

        self.entries.insert(cid, entry);
        self.rebuild_ws();
    }

    fn handle_remove(&mut self, condition_id: &str) {
        if let Some(entry) = self.entries.remove(condition_id) {
            // Clean up reverse mapping
            for token_id in &entry.market.clob_token_ids {
                self.token_to_condition.remove(token_id);
            }
            info!(
                condition_id = %&condition_id[..12.min(condition_id.len())],
                question = %entry.market.question,
                waited_secs = entry.detected_at.elapsed().as_secs(),
                "WATCHLIST: removed"
            );
            self.rebuild_ws();
        }
    }

    // ── WS Event Handler ─────────────────────────────────────────────────

    fn handle_ws_event(&mut self, event: ClobEvent) {
        let asset_id = match &event {
            ClobEvent::BookSnapshot { asset_id, .. } => asset_id.clone(),
            ClobEvent::PriceChange { asset_id, .. } => asset_id.clone(),
            ClobEvent::Connected => {
                info!("WATCHLIST WS: connected");
                return;
            }
            ClobEvent::Disconnected => {
                warn!("WATCHLIST WS: disconnected");
                return;
            }
            _ => return,
        };

        // Look up which entry this token belongs to
        let condition_id = match self.token_to_condition.get(&asset_id) {
            Some(cid) => cid.clone(),
            None => return,
        };

        let entry = match self.entries.get(&condition_id) {
            Some(e) => e,
            None => return,
        };

        // Check each prepared order for this entry
        let mut triggered: Option<(PreparedOrder, Decimal, Decimal, Decimal)> = None;

        for order in &entry.prepared_orders {
            if order.token_id != asset_id {
                continue;
            }

            // Read the book from the shared store
            let book = match self.store.get_book(&order.token_id) {
                Some(b) => b,
                None => continue,
            };

            let fee_type = entry.market.fee_type.as_deref();
            let (fillable_shares, avg_price, est_profit) = match order.side {
                SnipeSide::BuyWinner => self.evaluate_buy(&book, order, fee_type),
                SnipeSide::SellLoser => self.evaluate_sell(&book, order, fee_type),
            };

            if fillable_shares <= Decimal::ZERO {
                continue;
            }

            let waited = entry.detected_at.elapsed();

            // Always log the opportunity
            warn!(
                question = %entry.market.question,
                outcome = %order.outcome_label,
                side = ?order.side,
                best = %avg_price,
                shares = %fillable_shares,
                est_profit = %format!("${:.2}", est_profit),
                waited_secs = waited.as_secs(),
                "WATCHLIST OPPORTUNITY"
            );

            // Trigger if profitable
            if est_profit > Decimal::ZERO {
                triggered = Some((order.clone(), avg_price, fillable_shares, est_profit));
                break; // Fire on first profitable order
            }
        }

        // Emit event and remove entry if triggered
        if let (Some((order, best_price, available_size, estimated_profit)), Some(entry)) =
            (triggered, self.entries.get(&condition_id).cloned())
        {
            let _ = self.event_tx.send(WatchlistEvent::LiquidityReady {
                entry: entry.clone(),
                order,
                best_price,
                available_size,
                estimated_profit,
            });

            // Remove after triggering (one-shot)
            self.handle_remove(&condition_id);
        }
    }

    // ── Liquidity Evaluation ─────────────────────────────────────────────

    /// Evaluate buying the winning outcome: walk asks up to limit_price.
    /// Buying = taker, so taker fees apply (if the market has fees).
    fn evaluate_buy(
        &self,
        book: &crate::orderbook::OrderBook,
        order: &PreparedOrder,
        fee_type: Option<&str>,
    ) -> (Decimal, Decimal, Decimal) {
        let mut total_cost = Decimal::ZERO;
        let mut total_shares = Decimal::ZERO;
        let mut total_fees = Decimal::ZERO;

        for (&price, &size) in book.asks.levels.iter() {
            if price > order.limit_price {
                break;
            }

            let available = size.min(order.max_shares - total_shares);
            if available <= Decimal::ZERO {
                break;
            }

            total_cost += price * available;
            total_fees += market::taker_fee(available, price, fee_type);
            total_shares += available;
        }

        if total_shares == Decimal::ZERO {
            return (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
        }

        let avg_price = total_cost / total_shares;
        // Profit: payout ($1/share) - cost - taker fees
        let profit = total_shares - total_cost - total_fees;

        (total_shares, avg_price, profit)
    }

    /// Evaluate selling the losing outcome: walk bids above min_sell_price.
    /// Evaluate selling the losing outcome: walk bids above min_sell_price.
    /// Selling = maker, so NO fees on any market type (maker fee = 0%).
    fn evaluate_sell(
        &self,
        book: &crate::orderbook::OrderBook,
        order: &PreparedOrder,
        _fee_type: Option<&str>,
    ) -> (Decimal, Decimal, Decimal) {
        let mut total_revenue = Decimal::ZERO;
        let mut total_shares = Decimal::ZERO;

        // Walk bids descending (highest first)
        for (&price, &size) in book.bids.levels.iter().rev() {
            if price < order.limit_price {
                break;
            }

            let available = size.min(order.max_shares - total_shares);
            if available <= Decimal::ZERO {
                break;
            }

            total_revenue += price * available;
            total_shares += available;
        }

        if total_shares == Decimal::ZERO {
            return (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
        }

        let avg_price = total_revenue / total_shares;
        // Selling = maker → zero fees. Revenue IS profit (tokens become worthless).
        let profit = total_revenue;

        (total_shares, avg_price, profit)
    }

    // ── WS Rebuild ───────────────────────────────────────────────────────

    fn rebuild_ws(&mut self) {
        // Abort existing WS task
        if let Some(h) = self.ws_handle.take() {
            h.abort();
        }

        // Collect all token IDs from active entries
        let token_ids: Vec<String> = self.entries.values()
            .flat_map(|e| e.market.clob_token_ids.iter().cloned())
            .collect();

        if token_ids.is_empty() {
            debug!("watchlist: no tokens to watch, WS idle");
            return;
        }

        info!(
            tokens = token_ids.len(),
            entries = self.entries.len(),
            "WATCHLIST WS: rebuilding subscription"
        );

        // Create a new WS event channel for this connection
        let (new_tx, new_rx) = mpsc::unbounded_channel();
        self.ws_event_rx = new_rx;

        // Reuse the existing start_clob_ws function (same shard logic)
        let store = self.store.clone();
        let event_tx = new_tx;
        let ids = token_ids;

        self.ws_handle = Some(tokio::spawn(async move {
            if let Err(e) = start_clob_ws(store, event_tx, ids) {
                warn!(error = %e, "watchlist: failed to start dedicated CLOB WS");
            }
            // The shards run forever inside start_clob_ws; this task stays alive
            // until aborted by rebuild_ws.
            futures::future::pending::<()>().await;
        }));
    }

    // ── Expiry ───────────────────────────────────────────────────────────

    fn check_expiry(&mut self) {
        let expired: Vec<String> = self.entries.iter()
            .filter(|(_, e)| e.detected_at.elapsed() > self.config.ttl)
            .map(|(cid, _)| cid.clone())
            .collect();

        for cid in expired {
            if let Some(entry) = self.entries.get(&cid) {
                let waited = entry.detected_at.elapsed().as_secs();
                let _ = self.event_tx.send(WatchlistEvent::Expired {
                    condition_id: cid.clone(),
                    question: entry.market.question.clone(),
                    waited_secs: waited,
                });
                warn!(
                    condition_id = %&cid[..12.min(cid.len())],
                    question = %entry.market.question,
                    waited_secs = waited,
                    "WATCHLIST: entry expired without liquidity"
                );
            }
            self.handle_remove(&cid);
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build prepared orders for a watchlist entry.
pub fn build_prepared_orders(
    market: &CachedMarket,
    winning_idx: usize,
    max_buy_price: Decimal,
    min_sell_price: Decimal,
    max_position_usd: Decimal,
) -> Vec<PreparedOrder> {
    let mut orders = Vec::new();

    for (idx, token_id) in market.clob_token_ids.iter().enumerate() {
        let outcome_label = market.outcomes.get(idx)
            .cloned()
            .unwrap_or_else(|| format!("outcome_{}", idx));

        let is_winner = idx == winning_idx;

        let (side, limit_price) = if is_winner {
            (SnipeSide::BuyWinner, max_buy_price)
        } else {
            (SnipeSide::SellLoser, min_sell_price)
        };

        // Calculate max shares from position budget
        let max_shares = if limit_price > Decimal::ZERO {
            max_position_usd / limit_price
        } else {
            Decimal::ZERO
        };

        orders.push(PreparedOrder {
            token_id: token_id.clone(),
            outcome_label,
            side,
            limit_price,
            max_shares,
        });
    }

    orders
}
