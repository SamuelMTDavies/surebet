//! Strategy metrics tracking.
//!
//! Tracks key performance indicators across all strategies:
//! - Time from data publication to order submission (latency)
//! - Fill rate on snipe orders
//! - Edge per trade after fees
//! - Capital utilisation

use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::info;

/// Maximum history entries per metric.
const MAX_HISTORY: usize = 1000;

/// A single latency measurement.
#[derive(Debug, Clone)]
pub struct LatencyRecord {
    pub source: String,
    pub condition_id: String,
    pub latency: Duration,
    pub recorded_at: Instant,
}

/// A single trade record for edge tracking.
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub condition_id: String,
    pub strategy: String,
    pub direction: String,
    pub gross_edge: Decimal,
    pub fees: Decimal,
    pub net_edge: Decimal,
    pub size_usd: Decimal,
    pub profit_usd: Decimal,
    pub fill_rate: f64, // 0.0 - 1.0
    pub recorded_at: Instant,
}

/// Aggregate metrics over a time window.
#[derive(Debug, Clone, Default)]
pub struct WindowMetrics {
    pub count: usize,
    pub total_profit: Decimal,
    pub avg_edge: Decimal,
    pub avg_fill_rate: f64,
    pub avg_latency_ms: f64,
    pub capital_deployed: Decimal,
    pub capital_available: Decimal,
    pub utilisation_pct: f64,
}

/// The metrics tracker.
pub struct MetricsTracker {
    latencies: VecDeque<LatencyRecord>,
    trades: VecDeque<TradeRecord>,
    /// Capital tracking
    total_capital: Decimal,
    capital_in_positions: Decimal,
    capital_in_pending: Decimal,
}

impl MetricsTracker {
    pub fn new(total_capital: Decimal) -> Self {
        Self {
            latencies: VecDeque::new(),
            trades: VecDeque::new(),
            total_capital,
            capital_in_positions: Decimal::ZERO,
            capital_in_pending: Decimal::ZERO,
        }
    }

    /// Record a latency measurement.
    pub fn record_latency(&mut self, source: &str, condition_id: &str, latency: Duration) {
        if self.latencies.len() >= MAX_HISTORY {
            self.latencies.pop_front();
        }
        self.latencies.push_back(LatencyRecord {
            source: source.to_string(),
            condition_id: condition_id.to_string(),
            latency,
            recorded_at: Instant::now(),
        });
    }

    /// Record a completed (or paper) trade.
    pub fn record_trade(&mut self, trade: TradeRecord) {
        if self.trades.len() >= MAX_HISTORY {
            self.trades.pop_front();
        }
        self.trades.push_back(trade);
    }

    /// Update capital allocation.
    pub fn update_capital(&mut self, in_positions: Decimal, in_pending: Decimal) {
        self.capital_in_positions = in_positions;
        self.capital_in_pending = in_pending;
    }

    /// Compute aggregate metrics over the last `window` duration.
    pub fn window_metrics(&self, window: Duration) -> WindowMetrics {
        let cutoff = Instant::now() - window;

        let recent_trades: Vec<&TradeRecord> = self
            .trades
            .iter()
            .filter(|t| t.recorded_at >= cutoff)
            .collect();

        let recent_latencies: Vec<&LatencyRecord> = self
            .latencies
            .iter()
            .filter(|l| l.recorded_at >= cutoff)
            .collect();

        let count = recent_trades.len();
        let total_profit: Decimal = recent_trades.iter().map(|t| t.profit_usd).sum();
        let avg_edge = if count > 0 {
            recent_trades.iter().map(|t| t.net_edge).sum::<Decimal>()
                / Decimal::from(count as i64)
        } else {
            Decimal::ZERO
        };
        let avg_fill_rate = if count > 0 {
            recent_trades.iter().map(|t| t.fill_rate).sum::<f64>() / count as f64
        } else {
            0.0
        };
        let avg_latency_ms = if !recent_latencies.is_empty() {
            recent_latencies
                .iter()
                .map(|l| l.latency.as_secs_f64() * 1000.0)
                .sum::<f64>()
                / recent_latencies.len() as f64
        } else {
            0.0
        };

        let deployed = self.capital_in_positions + self.capital_in_pending;
        let available = self.total_capital - deployed;
        let utilisation = if self.total_capital > Decimal::ZERO {
            (deployed.to_string().parse::<f64>().unwrap_or(0.0)
                / self.total_capital.to_string().parse::<f64>().unwrap_or(1.0))
                * 100.0
        } else {
            0.0
        };

        WindowMetrics {
            count,
            total_profit,
            avg_edge,
            avg_fill_rate,
            avg_latency_ms,
            capital_deployed: deployed,
            capital_available: available,
            utilisation_pct: utilisation,
        }
    }

    /// Log a summary of metrics (called periodically).
    pub fn log_summary(&self) {
        let m5 = self.window_metrics(Duration::from_secs(300));
        let m60 = self.window_metrics(Duration::from_secs(3600));

        info!(
            trades_5m = m5.count,
            profit_5m = %m5.total_profit,
            avg_edge_5m = %m5.avg_edge,
            fill_rate_5m = format!("{:.1}%", m5.avg_fill_rate * 100.0),
            latency_5m = format!("{:.1}ms", m5.avg_latency_ms),
            "metrics (5min window)"
        );

        info!(
            trades_1h = m60.count,
            profit_1h = %m60.total_profit,
            utilisation = format!("{:.1}%", m60.utilisation_pct),
            deployed = %m60.capital_deployed,
            available = %m60.capital_available,
            "metrics (1h window)"
        );
    }

    /// Get per-strategy breakdown.
    pub fn strategy_breakdown(&self, window: Duration) -> Vec<(String, WindowMetrics)> {
        let cutoff = Instant::now() - window;
        let recent: Vec<&TradeRecord> = self
            .trades
            .iter()
            .filter(|t| t.recorded_at >= cutoff)
            .collect();

        let mut strategies: std::collections::HashMap<String, Vec<&TradeRecord>> =
            std::collections::HashMap::new();
        for t in &recent {
            strategies
                .entry(t.strategy.clone())
                .or_default()
                .push(t);
        }

        strategies
            .into_iter()
            .map(|(strat, trades)| {
                let count = trades.len();
                let total_profit = trades.iter().map(|t| t.profit_usd).sum();
                let avg_edge = if count > 0 {
                    trades.iter().map(|t| t.net_edge).sum::<Decimal>()
                        / Decimal::from(count as i64)
                } else {
                    Decimal::ZERO
                };
                let avg_fill = if count > 0 {
                    trades.iter().map(|t| t.fill_rate).sum::<f64>() / count as f64
                } else {
                    0.0
                };

                (
                    strat,
                    WindowMetrics {
                        count,
                        total_profit,
                        avg_edge,
                        avg_fill_rate: avg_fill,
                        ..Default::default()
                    },
                )
            })
            .collect()
    }
}
