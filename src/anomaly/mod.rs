//! Anomaly detector for Polymarket trades.
//!
//! Ingests RTDS trade events and periodically scans order books to
//! surface suspicious or unusual activity:
//!
//! - **Whale trades**: single trade large relative to book depth or average size
//! - **Volume spikes**: sudden burst of volume vs rolling average
//! - **Price dislocations**: price moves far from rolling mean
//! - **Book imbalance**: one side dramatically thicker than the other
//! - **Thin-book aggression**: large trade smashing through a thin book

use crate::arb::TrackedMarket;
use crate::config::AnomalyConfig;
use crate::orderbook::OrderBookStore;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info};

/// A single observed trade, stored in the rolling window.
#[derive(Debug, Clone)]
struct ObservedTrade {
    price: Decimal,
    size: Decimal,
    side: String,
    timestamp: Instant,
    wall_time: DateTime<Utc>,
}

/// Rolling trade history for a single asset.
#[derive(Debug)]
struct AssetHistory {
    trades: VecDeque<ObservedTrade>,
    /// Price at first observation (for baseline comparisons).
    first_price: Option<Decimal>,
}

impl AssetHistory {
    fn new() -> Self {
        Self {
            trades: VecDeque::new(),
            first_price: None,
        }
    }

    fn prune(&mut self, window: Duration) {
        let cutoff = Instant::now() - window;
        while self.trades.front().map_or(false, |t| t.timestamp < cutoff) {
            self.trades.pop_front();
        }
    }

    fn total_volume(&self) -> Decimal {
        self.trades.iter().map(|t| t.size).sum()
    }

    fn trade_count(&self) -> usize {
        self.trades.len()
    }

    fn avg_trade_size(&self) -> Option<Decimal> {
        let n = self.trades.len();
        if n == 0 {
            return None;
        }
        Some(self.total_volume() / Decimal::from(n as u64))
    }

    fn avg_price(&self) -> Option<Decimal> {
        let n = self.trades.len();
        if n == 0 {
            return None;
        }
        let sum: Decimal = self.trades.iter().map(|t| t.price).sum();
        Some(sum / Decimal::from(n as u64))
    }

    /// Volume in the most recent `recent` duration vs the rest of the window.
    fn volume_ratio(&self, recent: Duration) -> Option<f64> {
        let now = Instant::now();
        let cutoff = now - recent;
        let mut recent_vol = Decimal::ZERO;
        let mut older_vol = Decimal::ZERO;
        for t in &self.trades {
            if t.timestamp >= cutoff {
                recent_vol += t.size;
            } else {
                older_vol += t.size;
            }
        }
        if older_vol == Decimal::ZERO {
            return None;
        }
        // Normalize to per-second rates for fair comparison
        let recent_secs = recent.as_secs_f64();
        let older_secs = now.duration_since(
            self.trades.front().map(|t| t.timestamp).unwrap_or(now)
        ).as_secs_f64() - recent_secs;
        if older_secs <= 0.0 || recent_secs <= 0.0 {
            return None;
        }
        let recent_rate = recent_vol.to_string().parse::<f64>().unwrap_or(0.0) / recent_secs;
        let older_rate = older_vol.to_string().parse::<f64>().unwrap_or(0.0) / older_secs;
        if older_rate <= 0.0 {
            return None;
        }
        Some(recent_rate / older_rate)
    }
}

/// Types of anomaly we can detect.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum AnomalyKind {
    /// Single large trade relative to average size or book depth.
    WhaleTrade,
    /// Sudden spike in trading volume.
    VolumeSpike,
    /// Price moved far from rolling average.
    PriceDislocation,
    /// Order book heavily lopsided.
    BookImbalance,
    /// Large trade relative to thin book depth.
    ThinBookAggression,
}

impl std::fmt::Display for AnomalyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnomalyKind::WhaleTrade => write!(f, "WHALE_TRADE"),
            AnomalyKind::VolumeSpike => write!(f, "VOLUME_SPIKE"),
            AnomalyKind::PriceDislocation => write!(f, "PRICE_DISLOCATION"),
            AnomalyKind::BookImbalance => write!(f, "BOOK_IMBALANCE"),
            AnomalyKind::ThinBookAggression => write!(f, "THIN_BOOK_AGGRESSION"),
        }
    }
}

/// Severity level for an anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Severity {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Low => write!(f, "LOW"),
            Severity::Medium => write!(f, "MED"),
            Severity::High => write!(f, "HIGH"),
        }
    }
}

/// A detected anomaly, ready for display.
#[derive(Debug, Clone, Serialize)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub severity: Severity,
    pub market_question: String,
    pub asset_id: String,
    pub detail: String,
    pub detected_at: String,
    /// Trade price at time of anomaly.
    pub price: Option<String>,
    /// Trade size (for trade-based anomalies).
    pub size: Option<String>,
    /// Side of the triggering trade.
    pub side: Option<String>,
}

/// Events emitted by the anomaly detector.
#[derive(Debug, Clone)]
pub enum AnomalyEvent {
    Detected(Anomaly),
    ScanComplete {
        assets_scanned: usize,
        anomalies_found: usize,
    },
}

/// The anomaly detector. Holds rolling trade history and scans for oddities.
pub struct AnomalyDetector {
    store: OrderBookStore,
    /// Map from asset_id → market question (for display).
    asset_to_question: HashMap<String, String>,
    /// Rolling trade history per asset.
    histories: HashMap<String, AssetHistory>,
    /// Detected anomalies (ring buffer for dashboard).
    pub anomalies: VecDeque<Anomaly>,
    config: AnomalyConfig,
    window: Duration,
    event_tx: mpsc::UnboundedSender<AnomalyEvent>,
    /// Dedup: don't re-fire the same anomaly type for the same asset within this cooldown.
    cooldowns: HashMap<(String, String), Instant>,
}

impl AnomalyDetector {
    pub fn new(
        store: OrderBookStore,
        markets: &[TrackedMarket],
        config: AnomalyConfig,
        event_tx: mpsc::UnboundedSender<AnomalyEvent>,
    ) -> Self {
        let mut asset_to_question = HashMap::new();
        for m in markets {
            for (_, token_id) in &m.outcomes {
                asset_to_question.insert(token_id.clone(), m.question.clone());
            }
        }
        let window = Duration::from_secs(config.window_secs);
        let max = config.max_anomalies;

        Self {
            store,
            asset_to_question,
            histories: HashMap::new(),
            anomalies: VecDeque::with_capacity(max),
            config,
            window,
            event_tx,
            cooldowns: HashMap::new(),
        }
    }

    /// Ingest a trade from the RTDS feed. Checks for per-trade anomalies immediately.
    pub fn record_trade(&mut self, asset_id: &str, price_str: &str, size_str: &str, side: &str) {
        let price = match Decimal::from_str(price_str) {
            Ok(p) => p,
            Err(_) => return,
        };
        let size = match Decimal::from_str(size_str) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Extract stats from history BEFORE modifying, then drop the borrow
        let (avg_size, avg_price, trade_count) = {
            let history = self
                .histories
                .entry(asset_id.to_string())
                .or_insert_with(AssetHistory::new);

            if history.first_price.is_none() {
                history.first_price = Some(price);
            }

            let avg_size = history.avg_trade_size();
            let avg_price = history.avg_price();
            let count = history.trade_count();

            // Insert the trade
            history.trades.push_back(ObservedTrade {
                price,
                size,
                side: side.to_string(),
                timestamp: Instant::now(),
                wall_time: Utc::now(),
            });

            // Prune old entries
            history.prune(self.window);

            (avg_size, avg_price, count)
        };
        // history borrow is now dropped — safe to call &mut self methods

        let question = self
            .asset_to_question
            .get(asset_id)
            .cloned()
            .unwrap_or_else(|| asset_id[..12.min(asset_id.len())].to_string());

        // --- Check 1: Whale trade (size vs average) ---
        if let Some(avg) = avg_size {
            if avg > Decimal::ZERO {
                let ratio_f64 = size.to_string().parse::<f64>().unwrap_or(0.0)
                    / avg.to_string().parse::<f64>().unwrap_or(1.0);
                if ratio_f64 >= self.config.whale_size_multiplier {
                    let severity = if ratio_f64 >= self.config.whale_size_multiplier * 3.0 {
                        Severity::High
                    } else if ratio_f64 >= self.config.whale_size_multiplier * 1.5 {
                        Severity::Medium
                    } else {
                        Severity::Low
                    };
                    self.emit_anomaly(
                        AnomalyKind::WhaleTrade,
                        severity,
                        asset_id,
                        &question,
                        format!(
                            "{:.0}x avg trade size (trade={} avg={} trades_in_window={})",
                            ratio_f64, size, avg, trade_count
                        ),
                        Some(price),
                        Some(size),
                        Some(side),
                    );
                }
            }
        }

        // --- Check 2: Thin-book aggression ---
        if let Some(book) = self.store.get_book(asset_id) {
            let depth = book.total_depth(5);
            if depth > Decimal::ZERO {
                let frac = size.to_string().parse::<f64>().unwrap_or(0.0)
                    / depth.to_string().parse::<f64>().unwrap_or(1.0);
                if frac >= self.config.depth_aggression_fraction {
                    let severity = if frac >= 0.5 {
                        Severity::High
                    } else if frac >= 0.3 {
                        Severity::Medium
                    } else {
                        Severity::Low
                    };
                    self.emit_anomaly(
                        AnomalyKind::ThinBookAggression,
                        severity,
                        asset_id,
                        &question,
                        format!(
                            "trade={} is {:.0}% of top-5 depth ({:.2})",
                            size,
                            frac * 100.0,
                            depth
                        ),
                        Some(price),
                        Some(size),
                        Some(side),
                    );
                }
            }
        }

        // --- Check 3: Price dislocation (vs rolling average) ---
        if let Some(avg_p) = avg_price {
            if avg_p > Decimal::ZERO {
                let avg_f = avg_p.to_string().parse::<f64>().unwrap_or(0.0);
                let cur_f = price.to_string().parse::<f64>().unwrap_or(0.0);
                if avg_f > 0.0 {
                    let pct_move = ((cur_f - avg_f) / avg_f * 100.0).abs();
                    if pct_move >= self.config.price_move_pct {
                        let severity = if pct_move >= self.config.price_move_pct * 3.0 {
                            Severity::High
                        } else if pct_move >= self.config.price_move_pct * 1.5 {
                            Severity::Medium
                        } else {
                            Severity::Low
                        };
                        let direction = if cur_f > avg_f { "UP" } else { "DOWN" };
                        self.emit_anomaly(
                            AnomalyKind::PriceDislocation,
                            severity,
                            asset_id,
                            &question,
                            format!(
                                "price {} {:.1}% from rolling avg (now={} avg={:.4})",
                                direction, pct_move, price, avg_p
                            ),
                            Some(price),
                            Some(size),
                            Some(side),
                        );
                    }
                }
            }
        }
    }

    /// Periodic scan — checks book-level anomalies (imbalance, volume spikes).
    pub fn scan(&mut self) {
        let mut anomalies_found = 0usize;
        let asset_ids: Vec<String> = self.histories.keys().cloned().collect();

        for asset_id in &asset_ids {
            let question = self
                .asset_to_question
                .get(asset_id)
                .cloned()
                .unwrap_or_else(|| asset_id[..12.min(asset_id.len())].to_string());

            // --- Volume spike: compare last 60s vs the rest of the window ---
            if let Some(history) = self.histories.get(asset_id) {
                let recent = Duration::from_secs(60);
                if let Some(ratio) = history.volume_ratio(recent) {
                    if ratio >= self.config.volume_spike_multiplier {
                        let severity = if ratio >= self.config.volume_spike_multiplier * 3.0 {
                            Severity::High
                        } else if ratio >= self.config.volume_spike_multiplier * 1.5 {
                            Severity::Medium
                        } else {
                            Severity::Low
                        };
                        self.emit_anomaly(
                            AnomalyKind::VolumeSpike,
                            severity,
                            asset_id,
                            &question,
                            format!(
                                "last-60s volume rate {:.1}x the prior window (total_vol={} trades={})",
                                ratio,
                                history.total_volume(),
                                history.trade_count(),
                            ),
                            None,
                            None,
                            None,
                        );
                        anomalies_found += 1;
                    }
                }
            }

            // --- Book imbalance ---
            if let Some(book) = self.store.get_book(asset_id) {
                let bid_depth = book.bids.depth(5, true);
                let ask_depth = book.asks.depth(5, false);
                if bid_depth > Decimal::ZERO && ask_depth > Decimal::ZERO {
                    let bid_f = bid_depth.to_string().parse::<f64>().unwrap_or(1.0);
                    let ask_f = ask_depth.to_string().parse::<f64>().unwrap_or(1.0);
                    let ratio = if bid_f > ask_f {
                        bid_f / ask_f
                    } else {
                        ask_f / bid_f
                    };
                    if ratio >= self.config.book_imbalance_ratio {
                        let heavy_side = if bid_f > ask_f { "BID" } else { "ASK" };
                        let severity = if ratio >= self.config.book_imbalance_ratio * 3.0 {
                            Severity::High
                        } else if ratio >= self.config.book_imbalance_ratio * 1.5 {
                            Severity::Medium
                        } else {
                            Severity::Low
                        };
                        self.emit_anomaly(
                            AnomalyKind::BookImbalance,
                            severity,
                            asset_id,
                            &question,
                            format!(
                                "{} side {:.1}x heavier (bid_depth={:.2} ask_depth={:.2})",
                                heavy_side, ratio, bid_depth, ask_depth,
                            ),
                            None,
                            None,
                            None,
                        );
                        anomalies_found += 1;
                    }
                }
            }
        }

        let _ = self.event_tx.send(AnomalyEvent::ScanComplete {
            assets_scanned: asset_ids.len(),
            anomalies_found,
        });

        // Clean up stale cooldowns
        let now = Instant::now();
        self.cooldowns.retain(|_, t| now.duration_since(*t) < Duration::from_secs(120));
    }

    /// Get recent anomalies (for the dashboard API).
    pub fn recent_anomalies(&self) -> Vec<&Anomaly> {
        self.anomalies.iter().collect()
    }

    fn emit_anomaly(
        &mut self,
        kind: AnomalyKind,
        severity: Severity,
        asset_id: &str,
        question: &str,
        detail: String,
        price: Option<Decimal>,
        size: Option<Decimal>,
        side: Option<&str>,
    ) {
        // Dedup: don't fire the same kind for the same asset within 2 minutes
        let key = (asset_id.to_string(), kind.to_string());
        let now = Instant::now();
        if let Some(last) = self.cooldowns.get(&key) {
            if now.duration_since(*last) < Duration::from_secs(120) {
                debug!(
                    kind = %kind,
                    asset = %asset_id[..12.min(asset_id.len())],
                    "anomaly suppressed (cooldown)"
                );
                return;
            }
        }
        self.cooldowns.insert(key, now);

        let anomaly = Anomaly {
            kind: kind.clone(),
            severity,
            market_question: question.to_string(),
            asset_id: asset_id.to_string(),
            detail: detail.clone(),
            detected_at: Utc::now().to_rfc3339(),
            price: price.map(|p| p.to_string()),
            size: size.map(|s| s.to_string()),
            side: side.map(|s| s.to_string()),
        };

        info!(
            kind = %kind,
            severity = %severity,
            market = %question[..question.len().min(50)],
            detail = %detail,
            "ANOMALY DETECTED"
        );

        // Store for dashboard
        if self.anomalies.len() >= self.config.max_anomalies {
            self.anomalies.pop_front();
        }
        self.anomalies.push_back(anomaly.clone());

        let _ = self.event_tx.send(AnomalyEvent::Detected(anomaly));
    }
}
