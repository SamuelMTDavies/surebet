//! Formatted report output to stdout.
//!
//! Prints each analysis phase as a structured section.

use super::analyze::{self, TraderAnalysis};
use super::classify::StrategyClassification;
use super::fetch::RawTraderData;
use rust_decimal::Decimal;

const SEP: &str = "════════════════════════════════════════════════════════════════════════════════";


/// Print header banner.
pub fn print_header(address: &str) {
    println!("\n{SEP}");
    println!("  TRADER INVESTIGATION: {address}");
    println!("{SEP}\n");
}

/// Phase 1: Trade overview from raw data.
pub fn print_phase1(raw: &RawTraderData) {
    println!("--- PHASE 1: TRADE OVERVIEW ---\n");
    println!("  Total trades:          {}", raw.trades.len());
    println!("  Total activity:        {}", raw.activity.len());
    println!("  Open positions:        {}", raw.open_positions.len());
    println!("  Closed positions:      {}", raw.closed_positions.len());
    println!("  Markets traded:        {}", raw.markets_traded);
    println!("  Portfolio value:       ${:.2}", raw.portfolio_value);

    if !raw.trades.is_empty() {
        let min_ts = raw.trades.iter().map(|t| t.timestamp).min().unwrap();
        let max_ts = raw.trades.iter().map(|t| t.timestamp).max().unwrap();
        let days = (max_ts - min_ts) / 86400;
        let min_dt = chrono::DateTime::from_timestamp(min_ts, 0)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        let max_dt = chrono::DateTime::from_timestamp(max_ts, 0)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        println!("  Date range:            {min_dt} to {max_dt} ({days} days)");
    }

    // Quick win rate from closed positions
    let winners = raw
        .closed_positions
        .iter()
        .filter(|p| p.realized_pnl > Decimal::ZERO)
        .count();
    let total_pnl: Decimal = raw.closed_positions.iter().map(|p| p.realized_pnl).sum();
    if !raw.closed_positions.is_empty() {
        println!(
            "  Win rate (closed):     {:.1}% ({}/{})",
            winners as f64 / raw.closed_positions.len() as f64 * 100.0,
            winners,
            raw.closed_positions.len()
        );
        println!("  Realized PnL:          ${total_pnl:.2}");
    }

    println!();
}

/// Phase 2: Position lifecycles.
pub fn print_phase2_lifecycles(analysis: &TraderAnalysis) {
    println!("--- PHASE 2: POSITION LIFECYCLES ---\n");

    let closed_lifecycles: Vec<_> = analysis
        .lifecycles
        .iter()
        .filter(|l| l.hold_duration_secs.is_some())
        .collect();
    let open_lifecycles: Vec<_> = analysis
        .lifecycles
        .iter()
        .filter(|l| l.hold_duration_secs.is_none())
        .collect();

    println!("  Matched buy→sell pairs: {}", closed_lifecycles.len());
    println!("  Open (unmatched buys):  {}", open_lifecycles.len());

    if let Some(median) = analysis.patterns.median_hold_duration_secs {
        println!("  Median hold duration:   {}", format_duration(median));
    }
    println!(
        "  Quick flips (<1hr):     {}",
        analysis.patterns.quick_flip_count
    );
    println!(
        "  Long holds (>24hr):     {}",
        analysis.patterns.long_hold_count
    );

    // Top 10 positions by PnL
    let mut by_pnl: Vec<_> = closed_lifecycles
        .iter()
        .filter_map(|l| l.pnl.map(|p| (l, p)))
        .collect();
    by_pnl.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    if !by_pnl.is_empty() {
        println!("\n  Top positions by PnL:");
        for (lc, pnl) in by_pnl.iter().take(10) {
            let hold = lc
                .hold_duration_secs
                .map(format_duration)
                .unwrap_or_else(|| "open".to_string());
            println!(
                "    ${pnl:>+10.2}  {hold:>10}  {:.2} → {:.2}  {}",
                lc.entry_price,
                lc.exit_price.unwrap_or(Decimal::ZERO),
                truncate(&lc.title, 50),
            );
        }
    }

    // Worst 5
    if by_pnl.len() > 5 {
        println!("\n  Worst positions by PnL:");
        for (lc, pnl) in by_pnl.iter().rev().take(5) {
            let hold = lc
                .hold_duration_secs
                .map(format_duration)
                .unwrap_or_else(|| "open".to_string());
            println!(
                "    ${pnl:>+10.2}  {hold:>10}  {:.2} → {:.2}  {}",
                lc.entry_price,
                lc.exit_price.unwrap_or(Decimal::ZERO),
                truncate(&lc.title, 50),
            );
        }
    }

    println!();
}

/// Phase 3: Market creation analysis.
pub fn print_phase3_creation(analysis: &TraderAnalysis) {
    println!("--- PHASE 3: MARKET CREATION ANALYSIS ---\n");

    let total = analysis.total_trades.max(1);

    println!(
        "  Trades within 60s of creation:   {:>5} ({:.1}%)",
        analysis.timing.trades_within_60s_of_creation,
        analysis.timing.trades_within_60s_of_creation as f64 / total as f64 * 100.0,
    );
    println!(
        "  Trades within 5min of creation:  {:>5} ({:.1}%)",
        analysis.timing.trades_within_300s_of_creation,
        analysis.timing.trades_within_300s_of_creation as f64 / total as f64 * 100.0,
    );
    println!(
        "  Trades within 30min of creation: {:>5} ({:.1}%)",
        analysis.timing.trades_within_1800s_of_creation,
        analysis.timing.trades_within_1800s_of_creation as f64 / total as f64 * 100.0,
    );

    if !analysis.timing.creation_deltas.is_empty() {
        println!(
            "  Median creation delta:   {:.1}s",
            analyze::median(&analysis.timing.creation_deltas)
        );
        println!(
            "  Mean creation delta:     {:.1}s",
            analyze::mean(&analysis.timing.creation_deltas)
        );
    }

    println!();
}

/// Phase 4: Resolution timing analysis.
pub fn print_phase4_resolution(analysis: &TraderAnalysis) {
    println!("--- PHASE 4: RESOLUTION TIMING ANALYSIS ---\n");

    let total = analysis.total_trades.max(1);
    let correlated_count = analysis
        .correlated
        .trade_events
        .iter()
        .filter(|t| {
            t.nearest_propose.is_some()
                || t.nearest_resolution.is_some()
                || t.nearest_creation.is_some()
        })
        .count();

    println!(
        "  Trades with event correlation: {}/{} ({:.1}%)\n",
        correlated_count,
        total,
        correlated_count as f64 / total as f64 * 100.0,
    );

    // ProposePrice
    println!("  ProposePrice correlation:");
    println!(
        "    Trades within 60s:   {:>5} ({:.1}%)",
        analysis.timing.trades_within_60s_of_propose,
        analysis.timing.trades_within_60s_of_propose as f64 / total as f64 * 100.0,
    );
    println!(
        "    Trades within 5min:  {:>5} ({:.1}%)",
        analysis.timing.trades_within_300s_of_propose,
        analysis.timing.trades_within_300s_of_propose as f64 / total as f64 * 100.0,
    );
    if !analysis.timing.propose_deltas.is_empty() {
        println!(
            "    Median delta:        {:.1}s",
            analyze::median(&analysis.timing.propose_deltas)
        );
        println!(
            "    Mean delta:          {:.1}s",
            analyze::mean(&analysis.timing.propose_deltas)
        );
        println!(
            "    P10 / P90:           {:.1}s / {:.1}s",
            analyze::percentile(&analysis.timing.propose_deltas, 10.0),
            analyze::percentile(&analysis.timing.propose_deltas, 90.0),
        );
    }

    // ConditionResolution
    println!("\n  ConditionResolution correlation:");
    println!(
        "    Trades within 60s:   {:>5} ({:.1}%)",
        analysis.timing.trades_within_60s_of_resolution,
        analysis.timing.trades_within_60s_of_resolution as f64 / total as f64 * 100.0,
    );
    println!(
        "    Trades within 5min:  {:>5} ({:.1}%)",
        analysis.timing.trades_within_300s_of_resolution,
        analysis.timing.trades_within_300s_of_resolution as f64 / total as f64 * 100.0,
    );
    if !analysis.timing.resolution_deltas.is_empty() {
        println!(
            "    Median delta:        {:.1}s",
            analyze::median(&analysis.timing.resolution_deltas)
        );
    }

    println!();
}

/// Phase 5: Trading patterns.
pub fn print_phase5_patterns(analysis: &TraderAnalysis) {
    println!("--- PHASE 5: TRADING PATTERNS ---\n");

    println!("  Buy / Sell:            {} / {} ({:.1}% buys)",
        analysis.buy_count,
        analysis.sell_count,
        analysis.buy_count as f64 / analysis.total_trades.max(1) as f64 * 100.0,
    );
    println!("  Total volume:          ${:.2}", analysis.total_volume_usd);
    println!("  Avg trade size:        ${:.2}", analysis.patterns.avg_trade_size_usd);
    println!("  Median trade size:     ${:.2}", analysis.patterns.median_trade_size_usd);
    println!("  Max trade size:        ${:.2}", analysis.patterns.max_trade_size_usd);
    println!("  Avg buy price:         ${:.4}", analysis.patterns.avg_buy_price);
    println!("  Avg sell price:        ${:.4}", analysis.patterns.avg_sell_price);

    // Category distribution
    if !analysis.patterns.category_counts.is_empty() {
        println!("\n  Category distribution:");
        let mut cats: Vec<_> = analysis.patterns.category_counts.iter().collect();
        cats.sort_by(|a, b| b.1.cmp(a.1));
        for (cat, &count) in &cats {
            let pct = count as f64 / analysis.total_trades.max(1) as f64 * 100.0;
            let name = if cat.is_empty() { "(unknown)" } else { cat };
            println!("    {name:<20} {count:>6} ({pct:.1}%)");
        }
    }

    // Fee type distribution
    if !analysis.patterns.fee_type_counts.is_empty() {
        println!("\n  Fee type distribution:");
        let mut fts: Vec<_> = analysis.patterns.fee_type_counts.iter().collect();
        fts.sort_by(|a, b| b.1.cmp(a.1));
        for (ft, &count) in &fts {
            let pct = count as f64 / analysis.total_trades.max(1) as f64 * 100.0;
            println!("    {ft:<20} {count:>6} ({pct:.1}%)");
        }
    }

    // Time of day
    println!("\n  Time of day (UTC):");
    let max_hour = *analysis.patterns.hour_distribution.iter().max().unwrap_or(&1);
    for (h, &count) in analysis.patterns.hour_distribution.iter().enumerate() {
        if count > 0 {
            let bar_len = (count as f64 / max_hour as f64 * 30.0) as usize;
            let bar: String = "█".repeat(bar_len);
            println!("    {h:02}:00  {count:>5}  {bar}");
        }
    }

    // Day of week
    let days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    println!("\n  Day of week:");
    for (i, &count) in analysis.patterns.dow_distribution.iter().enumerate() {
        if count > 0 {
            println!("    {}  {count:>5}", days[i]);
        }
    }

    println!();
}

/// Phase 6: Strategy classification.
pub fn print_phase6_classification(classification: &StrategyClassification) {
    println!("--- PHASE 6: STRATEGY CLASSIFICATION ---\n");

    println!(
        "  Primary:    {} (confidence: {:.0}%)",
        classification.primary.0,
        classification.primary.1 * 100.0,
    );

    for (strategy, confidence) in &classification.secondary {
        println!(
            "  Secondary:  {} (confidence: {:.0}%)",
            strategy,
            confidence * 100.0,
        );
    }

    println!("\n  SUMMARY:");
    // Word-wrap the summary at ~76 chars
    let words: Vec<&str> = classification.summary.split_whitespace().collect();
    let mut line = String::from("  ");
    for word in words {
        if line.len() + word.len() + 1 > 78 {
            println!("{line}");
            line = String::from("  ");
        }
        if line.len() > 2 {
            line.push(' ');
        }
        line.push_str(word);
    }
    if line.len() > 2 {
        println!("{line}");
    }

    println!("\n{SEP}\n");
}

// ── Helpers ──────────────────────────────────────

fn format_duration(secs: i64) -> String {
    let abs = secs.unsigned_abs();
    if abs < 60 {
        format!("{secs}s")
    } else if abs < 3600 {
        format!("{:.1}min", secs as f64 / 60.0)
    } else if abs < 86400 {
        format!("{:.1}hr", secs as f64 / 3600.0)
    } else {
        format!("{:.1}d", secs as f64 / 86400.0)
    }
}

fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        &s[..max_len]
    }
}
