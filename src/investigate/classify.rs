//! Strategy classification based on analysis patterns.
//!
//! Applies heuristic scoring to classify the trader's primary and
//! secondary strategies with confidence scores.

use super::analyze::TraderAnalysis;

/// A classified trading strategy with confidence score.
#[derive(Debug, Clone)]
pub struct StrategyClassification {
    pub primary: (Strategy, f64),
    pub secondary: Vec<(Strategy, f64)>,
    pub summary: String,
}

/// Known trading strategy archetypes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Buys within minutes of market creation (ConditionPreparation)
    MarketCreationSniper,
    /// Trades within seconds of resolution proposal (ProposePrice)
    ResolutionSniper,
    /// Very short holding periods, frequent buy+sell pairs
    QuickFlipper,
    /// Dominantly trades crypto_15_min markets
    Crypto15mSpecialist,
    /// Trades cluster after ConditionResolution
    PostResolutionTrader,
    /// Balanced buy/sell, many markets, small per-trade PnL
    MarketMaker,
    /// Gradual accumulation, holds to resolution
    Accumulator,
    /// Trades cluster around event end dates
    EventDriven,
    /// No clear pattern detected
    Unknown,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Strategy::MarketCreationSniper => write!(f, "Market Creation Sniper"),
            Strategy::ResolutionSniper => write!(f, "Resolution Sniper"),
            Strategy::QuickFlipper => write!(f, "Quick Flipper"),
            Strategy::Crypto15mSpecialist => write!(f, "Crypto 15m Specialist"),
            Strategy::PostResolutionTrader => write!(f, "Post-Resolution Trader"),
            Strategy::MarketMaker => write!(f, "Market Maker"),
            Strategy::Accumulator => write!(f, "Accumulator"),
            Strategy::EventDriven => write!(f, "Event-Driven"),
            Strategy::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Classify the trader's strategy based on analysis results.
pub fn classify(analysis: &TraderAnalysis) -> StrategyClassification {
    let mut scores: Vec<(Strategy, f64)> = Vec::new();
    let total = analysis.total_trades.max(1) as f64;

    // ── Market Creation Sniper ──────────────────
    // >20% of trades within 30 min of ConditionPreparation
    let creation_30m_pct = analysis.timing.trades_within_1800s_of_creation as f64 / total;
    if creation_30m_pct > 0.2 {
        scores.push((Strategy::MarketCreationSniper, 0.5 + creation_30m_pct * 0.5));
    }

    // ── Resolution Sniper ───────────────────────
    // >30% of trades within 60s of ProposePrice
    let propose_60s_pct = analysis.timing.trades_within_60s_of_propose as f64 / total;
    if propose_60s_pct > 0.1 {
        let confidence = 0.4 + (propose_60s_pct * 1.5).min(0.5);
        scores.push((Strategy::ResolutionSniper, confidence));
    }
    // Also check 5-min window with lower threshold
    let propose_5m_pct = analysis.timing.trades_within_300s_of_propose as f64 / total;
    if propose_5m_pct > 0.3 && propose_60s_pct <= 0.1 {
        scores.push((Strategy::ResolutionSniper, 0.4 + propose_5m_pct * 0.4));
    }

    // ── Quick Flipper ───────────────────────────
    // Median hold < 1 hour, frequent buy+sell pairs
    if let Some(median_hold) = analysis.patterns.median_hold_duration_secs {
        if median_hold < 3600 && analysis.patterns.quick_flip_count > 5 {
            let flip_pct = analysis.patterns.quick_flip_count as f64
                / analysis.lifecycles.len().max(1) as f64;
            scores.push((Strategy::QuickFlipper, 0.4 + flip_pct * 0.5));
        }
    }

    // ── Crypto 15m Specialist ───────────────────
    if analysis.patterns.crypto_15m_pct > 0.5 {
        scores.push((
            Strategy::Crypto15mSpecialist,
            0.3 + analysis.patterns.crypto_15m_pct * 0.6,
        ));
    }

    // ── Post-Resolution Trader ──────────────────
    let _resolution_60s_pct = analysis.timing.trades_within_60s_of_resolution as f64 / total;
    let resolution_5m_pct = analysis.timing.trades_within_300s_of_resolution as f64 / total;
    if resolution_5m_pct > 0.2 {
        scores.push((
            Strategy::PostResolutionTrader,
            0.4 + resolution_5m_pct * 0.5,
        ));
    }

    // ── Market Maker ────────────────────────────
    // Buy/sell ratio close to 1:1, many unique markets
    let buy_sell_ratio = if analysis.sell_count > 0 {
        analysis.buy_count as f64 / analysis.sell_count as f64
    } else {
        f64::INFINITY
    };
    if (0.6..=1.6).contains(&buy_sell_ratio) && analysis.unique_markets > 10 {
        scores.push((Strategy::MarketMaker, 0.5));
    }

    // ── Accumulator ─────────────────────────────
    // High buy ratio, long hold times
    if analysis.buy_count as f64 / total > 0.75 {
        if let Some(median_hold) = analysis.patterns.median_hold_duration_secs {
            if median_hold > 86400 {
                // > 1 day
                scores.push((Strategy::Accumulator, 0.6));
            }
        }
    }

    // Sort by confidence descending
    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let primary = scores.first().cloned().unwrap_or((Strategy::Unknown, 0.0));
    let secondary: Vec<(Strategy, f64)> = scores.into_iter().skip(1).collect();

    let summary = build_summary(analysis, &primary, &secondary);

    StrategyClassification {
        primary,
        secondary,
        summary,
    }
}

fn build_summary(
    analysis: &TraderAnalysis,
    primary: &(Strategy, f64),
    secondary: &[(Strategy, f64)],
) -> String {
    let mut parts = Vec::new();

    parts.push(format!(
        "This trader is primarily classified as a {} (confidence: {:.0}%).",
        primary.0,
        primary.1 * 100.0
    ));

    if !secondary.is_empty() {
        let sec_names: Vec<String> = secondary
            .iter()
            .take(2)
            .map(|(s, c)| format!("{} ({:.0}%)", s, c * 100.0))
            .collect();
        parts.push(format!(
            "Secondary strategies: {}.",
            sec_names.join(", ")
        ));
    }

    // Add specifics based on primary strategy
    match primary.0 {
        Strategy::ResolutionSniper => {
            parts.push(format!(
                "They react to ProposePrice events rapidly — {:.1}% of trades within 60s, {:.1}% within 5 minutes.",
                analysis.timing.trades_within_60s_of_propose as f64 / analysis.total_trades.max(1) as f64 * 100.0,
                analysis.timing.trades_within_300s_of_propose as f64 / analysis.total_trades.max(1) as f64 * 100.0,
            ));
        }
        Strategy::MarketCreationSniper => {
            parts.push(format!(
                "{:.1}% of trades occur within 30 minutes of market creation.",
                analysis.timing.trades_within_1800s_of_creation as f64 / analysis.total_trades.max(1) as f64 * 100.0,
            ));
        }
        Strategy::Crypto15mSpecialist => {
            parts.push(format!(
                "{:.1}% of trades are in crypto_15_min markets.",
                analysis.patterns.crypto_15m_pct * 100.0,
            ));
        }
        Strategy::QuickFlipper => {
            if let Some(med) = analysis.patterns.median_hold_duration_secs {
                parts.push(format!(
                    "Median holding duration is {}. {} quick flips (< 1hr) detected.",
                    format_duration(med),
                    analysis.patterns.quick_flip_count,
                ));
            }
        }
        _ => {}
    }

    parts.push(format!(
        "Avg buy price: ${:.3}, avg sell price: ${:.3}. Win rate: {:.1}%.",
        analysis.patterns.avg_buy_price,
        analysis.patterns.avg_sell_price,
        analysis.win_rate * 100.0,
    ));

    parts.join(" ")
}

fn format_duration(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{:.1}min", secs as f64 / 60.0)
    } else if secs < 86400 {
        format!("{:.1}hr", secs as f64 / 3600.0)
    } else {
        format!("{:.1}d", secs as f64 / 86400.0)
    }
}
