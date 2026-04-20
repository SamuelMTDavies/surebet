//! Pattern analysis and position lifecycle reconstruction.
//!
//! Computes timing distributions, win rates, position lifecycles (FIFO
//! buy/sell pairing), category breakdowns, and trading patterns.

use std::collections::HashMap;

use polymarket_client_sdk::data::types::Side;
use rust_decimal::Decimal;

use super::correlate::CorrelatedTraderData;

/// Complete analysis of a trader's patterns.
#[derive(Debug, Clone)]
pub struct TraderAnalysis {
    pub correlated: CorrelatedTraderData,

    // ── Overview ──────────────────────────────────
    pub total_trades: usize,
    pub total_volume_usd: Decimal,
    pub unique_markets: usize,
    pub date_range: Option<(i64, i64)>,
    pub buy_count: usize,
    pub sell_count: usize,

    // ── Win rate (from closed positions) ─────────
    pub closed_position_count: usize,
    pub winning_positions: usize,
    pub total_realized_pnl: Decimal,
    pub win_rate: f64,

    // ── Position lifecycles ──────────────────────
    pub lifecycles: Vec<PositionLifecycle>,

    // ── Timing analysis ──────────────────────────
    pub timing: TimingAnalysis,

    // ── Pattern analysis ─────────────────────────
    pub patterns: PatternAnalysis,
}

/// A reconstructed position lifecycle (FIFO buy → sell pairing).
#[derive(Debug, Clone)]
pub struct PositionLifecycle {
    pub asset_id: String,
    pub condition_id: String,
    pub title: String,
    pub outcome: String,
    pub entry_timestamp: i64,
    pub exit_timestamp: Option<i64>,
    pub entry_price: Decimal,
    pub exit_price: Option<Decimal>,
    pub size: Decimal,
    /// Hold duration in seconds. None if still open.
    pub hold_duration_secs: Option<i64>,
    /// PnL = (exit_price - entry_price) * size. None if still open.
    pub pnl: Option<Decimal>,
}

/// Timing analysis relative to on-chain events.
#[derive(Debug, Clone, Default)]
pub struct TimingAnalysis {
    /// Deltas: trade_ts - creation_event_ts (seconds)
    pub creation_deltas: Vec<i64>,
    /// Deltas: trade_ts - propose_event_ts (seconds)
    pub propose_deltas: Vec<i64>,
    /// Deltas: trade_ts - resolution_event_ts (seconds)
    pub resolution_deltas: Vec<i64>,

    pub trades_within_60s_of_creation: usize,
    pub trades_within_300s_of_creation: usize,
    pub trades_within_1800s_of_creation: usize,

    pub trades_within_60s_of_propose: usize,
    pub trades_within_300s_of_propose: usize,

    pub trades_within_60s_of_resolution: usize,
    pub trades_within_300s_of_resolution: usize,
}

/// Pattern analysis across all trades.
#[derive(Debug, Clone, Default)]
pub struct PatternAnalysis {
    pub category_counts: HashMap<String, usize>,
    pub fee_type_counts: HashMap<String, usize>,
    pub crypto_15m_pct: f64,

    pub avg_trade_size_usd: Decimal,
    pub median_trade_size_usd: Decimal,
    pub max_trade_size_usd: Decimal,

    /// UTC hour -> trade count
    pub hour_distribution: [usize; 24],
    /// Day-of-week (Mon=0) -> trade count
    pub dow_distribution: [usize; 7],

    pub avg_buy_price: Decimal,
    pub avg_sell_price: Decimal,
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,

    /// Median holding duration in seconds (from lifecycles)
    pub median_hold_duration_secs: Option<i64>,
    /// Number of quick flips (hold < 1 hour)
    pub quick_flip_count: usize,
    /// Number of long holds (hold > 24 hours)
    pub long_hold_count: usize,
}

/// Run the full analysis on correlated trader data.
pub fn analyze(correlated: &CorrelatedTraderData) -> TraderAnalysis {
    let raw = &correlated.enriched.raw;
    let meta = &correlated.enriched.market_meta;

    // ── Overview ──────────────────────────────────
    let total_trades = raw.trades.len();
    let mut total_volume_usd = Decimal::ZERO;
    let mut buy_count = 0usize;
    let mut sell_count = 0usize;
    let mut unique_markets = std::collections::HashSet::new();

    for t in &raw.trades {
        total_volume_usd += t.price * t.size;
        unique_markets.insert(format!("{:#x}", t.condition_id));
        match &t.side {
            Side::Buy => buy_count += 1,
            Side::Sell => sell_count += 1,
            _ => {}
        }
    }

    let date_range = if !raw.trades.is_empty() {
        let min = raw.trades.iter().map(|t| t.timestamp).min().unwrap();
        let max = raw.trades.iter().map(|t| t.timestamp).max().unwrap();
        Some((min, max))
    } else {
        None
    };

    // ── Win rate ──────────────────────────────────
    let closed_position_count = raw.closed_positions.len();
    let winning_positions = raw
        .closed_positions
        .iter()
        .filter(|p| p.realized_pnl > Decimal::ZERO)
        .count();
    let total_realized_pnl: Decimal = raw.closed_positions.iter().map(|p| p.realized_pnl).sum();
    let win_rate = if closed_position_count > 0 {
        winning_positions as f64 / closed_position_count as f64
    } else {
        0.0
    };

    // ── Position lifecycles ──────────────────────
    let lifecycles = build_lifecycles(&raw.trades);

    // ── Timing ───────────────────────────────────
    let timing = build_timing(&correlated.trade_events);

    // ── Patterns ─────────────────────────────────
    let patterns = build_patterns(&raw.trades, meta, &lifecycles);

    TraderAnalysis {
        correlated: correlated.clone(),
        total_trades,
        total_volume_usd,
        unique_markets: unique_markets.len(),
        date_range,
        buy_count,
        sell_count,
        closed_position_count,
        winning_positions,
        total_realized_pnl,
        win_rate,
        lifecycles,
        timing,
        patterns,
    }
}

/// Build position lifecycles by FIFO pairing buys and sells on the same asset.
fn build_lifecycles(
    trades: &[polymarket_client_sdk::data::types::response::Trade],
) -> Vec<PositionLifecycle> {
    // Group trades by asset_id (token ID)
    let mut buys_by_asset: HashMap<String, Vec<(i64, Decimal, Decimal, String, String, String)>> =
        HashMap::new();
    let mut lifecycles = Vec::new();

    for t in trades {
        let asset_id = t.asset.to_string();
        let cid = format!("{:#x}", t.condition_id);

        match &t.side {
            Side::Buy => {
                buys_by_asset
                    .entry(asset_id)
                    .or_default()
                    .push((t.timestamp, t.price, t.size, cid, t.title.clone(), t.outcome.clone()));
            }
            Side::Sell => {
                // Try to match with earliest buy (FIFO)
                if let Some(buys) = buys_by_asset.get_mut(&asset_id) {
                    if let Some((entry_ts, entry_price, entry_size, entry_cid, title, outcome)) =
                        buys.first().cloned()
                    {
                        let matched_size = t.size.min(entry_size);
                        let hold_duration = t.timestamp - entry_ts;
                        let pnl = (t.price - entry_price) * matched_size;

                        lifecycles.push(PositionLifecycle {
                            asset_id: asset_id.clone(),
                            condition_id: entry_cid,
                            title,
                            outcome,
                            entry_timestamp: entry_ts,
                            exit_timestamp: Some(t.timestamp),
                            entry_price,
                            exit_price: Some(t.price),
                            size: matched_size,
                            hold_duration_secs: Some(hold_duration),
                            pnl: Some(pnl),
                        });

                        // Reduce or remove the buy
                        if entry_size <= t.size {
                            buys.remove(0);
                        } else {
                            buys[0].2 -= matched_size;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Remaining unmatched buys are open positions
    for (asset_id, buys) in &buys_by_asset {
        for (ts, price, size, cid, title, outcome) in buys {
            lifecycles.push(PositionLifecycle {
                asset_id: asset_id.clone(),
                condition_id: cid.clone(),
                title: title.clone(),
                outcome: outcome.clone(),
                entry_timestamp: *ts,
                exit_timestamp: None,
                entry_price: *price,
                exit_price: None,
                size: *size,
                hold_duration_secs: None,
                pnl: None,
            });
        }
    }

    lifecycles.sort_by_key(|l| l.entry_timestamp);
    lifecycles
}

fn build_timing(
    trade_events: &[super::correlate::TradeEventCorrelation],
) -> TimingAnalysis {
    let mut timing = TimingAnalysis::default();

    for te in trade_events {
        if let Some(ref ev) = te.nearest_creation {
            timing.creation_deltas.push(ev.delta_seconds);
            let abs_delta = ev.delta_seconds.unsigned_abs();
            if abs_delta <= 60 {
                timing.trades_within_60s_of_creation += 1;
            }
            if abs_delta <= 300 {
                timing.trades_within_300s_of_creation += 1;
            }
            if abs_delta <= 1800 {
                timing.trades_within_1800s_of_creation += 1;
            }
        }

        if let Some(ref ev) = te.nearest_propose {
            timing.propose_deltas.push(ev.delta_seconds);
            let abs_delta = ev.delta_seconds.unsigned_abs();
            if abs_delta <= 60 {
                timing.trades_within_60s_of_propose += 1;
            }
            if abs_delta <= 300 {
                timing.trades_within_300s_of_propose += 1;
            }
        }

        if let Some(ref ev) = te.nearest_resolution {
            timing.resolution_deltas.push(ev.delta_seconds);
            let abs_delta = ev.delta_seconds.unsigned_abs();
            if abs_delta <= 60 {
                timing.trades_within_60s_of_resolution += 1;
            }
            if abs_delta <= 300 {
                timing.trades_within_300s_of_resolution += 1;
            }
        }
    }

    timing
}

fn build_patterns(
    trades: &[polymarket_client_sdk::data::types::response::Trade],
    meta: &HashMap<String, super::enrich::MarketMeta>,
    lifecycles: &[PositionLifecycle],
) -> PatternAnalysis {
    let mut patterns = PatternAnalysis::default();

    let mut trade_sizes: Vec<Decimal> = Vec::new();
    let mut buy_price_sum = Decimal::ZERO;
    let mut buy_price_count = 0u64;
    let mut sell_price_sum = Decimal::ZERO;
    let mut sell_price_count = 0u64;
    let mut crypto_15m_count = 0usize;

    for t in trades {
        let size_usd = t.price * t.size;
        trade_sizes.push(size_usd);

        let cid = format!("{:#x}", t.condition_id);
        if let Some(m) = meta.get(&cid) {
            *patterns.category_counts.entry(m.category.clone()).or_default() += 1;
            let ft_key = m.fee_type.as_deref().unwrap_or("(none)").to_string();
            *patterns.fee_type_counts.entry(ft_key).or_default() += 1;
            if m.fee_type.as_deref() == Some("crypto_15_min") {
                crypto_15m_count += 1;
            }
        }

        match &t.side {
            Side::Buy => {
                buy_price_sum += t.price;
                buy_price_count += 1;
                patterns.buy_volume += size_usd;
            }
            Side::Sell => {
                sell_price_sum += t.price;
                sell_price_count += 1;
                patterns.sell_volume += size_usd;
            }
            _ => {}
        }

        // Time-of-day and day-of-week
        if let Some(dt) = chrono::DateTime::from_timestamp(t.timestamp, 0) {
            let hour = dt.format("%H").to_string().parse::<usize>().unwrap_or(0);
            patterns.hour_distribution[hour] += 1;
            let dow = dt.format("%u").to_string().parse::<usize>().unwrap_or(1) - 1; // 1=Mon -> 0
            if dow < 7 {
                patterns.dow_distribution[dow] += 1;
            }
        }
    }

    // Aggregate sizes
    trade_sizes.sort();
    let total_trades = trades.len();
    if !trade_sizes.is_empty() {
        let sum: Decimal = trade_sizes.iter().sum();
        patterns.avg_trade_size_usd = sum / Decimal::from(total_trades as u64);
        patterns.median_trade_size_usd = trade_sizes[trade_sizes.len() / 2];
        patterns.max_trade_size_usd = *trade_sizes.last().unwrap();
    }

    patterns.crypto_15m_pct = if total_trades > 0 {
        crypto_15m_count as f64 / total_trades as f64
    } else {
        0.0
    };

    patterns.avg_buy_price = if buy_price_count > 0 {
        buy_price_sum / Decimal::from(buy_price_count)
    } else {
        Decimal::ZERO
    };
    patterns.avg_sell_price = if sell_price_count > 0 {
        sell_price_sum / Decimal::from(sell_price_count)
    } else {
        Decimal::ZERO
    };

    // Lifecycle patterns
    let mut hold_durations: Vec<i64> = lifecycles
        .iter()
        .filter_map(|l| l.hold_duration_secs)
        .collect();
    hold_durations.sort();

    if !hold_durations.is_empty() {
        patterns.median_hold_duration_secs = Some(hold_durations[hold_durations.len() / 2]);
    }

    patterns.quick_flip_count = hold_durations.iter().filter(|&&d| d < 3600).count();
    patterns.long_hold_count = hold_durations.iter().filter(|&&d| d > 86400).count();

    patterns
}

// ── Stats helpers ────────────────────────────────

pub fn median(values: &[i64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) as f64 / 2.0
    } else {
        sorted[mid] as f64
    }
}

pub fn mean(values: &[i64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<i64>() as f64 / values.len() as f64
}

pub fn percentile(values: &[i64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)] as f64
}
