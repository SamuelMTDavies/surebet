//! Polymarket user profile analyzer.
//!
//! Pulls a user's trading activity from Polymarket's public APIs and
//! analyzes patterns: programmatic trading signals, timing strategies,
//! order size distributions, and market targeting.
//!
//! Usage:
//!   cargo run --bin profile -- MistahFox
//!   cargo run --bin profile -- 0x1234abcd...
//!
//! The tool tries multiple Polymarket API endpoints to resolve usernames,
//! fetch trades, and pull positions. All endpoints are public (no auth needed).

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;

// ── Polymarket API base URLs ──────────────────────────────────────────

const GAMMA_API: &str = "https://gamma-api.polymarket.com";
const DATA_API: &str = "https://data-api.polymarket.com";
const CLOB_API: &str = "https://clob.polymarket.com";

// ── API response types ────────────────────────────────────────────────

/// Gamma profile response.
#[derive(Debug, Deserialize)]
struct GammaProfile {
    #[serde(default, alias = "proxyWallet", alias = "proxy_wallet")]
    proxy_wallet: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    username: String,
    #[serde(default, alias = "profilePicture", alias = "profile_picture")]
    profile_picture: String,
    #[serde(default)]
    bio: String,
    #[serde(default, alias = "totalVolume", alias = "total_volume")]
    total_volume: Option<f64>,
    #[serde(default, alias = "pnl")]
    pnl: Option<f64>,
    #[serde(default, alias = "marketsTraded", alias = "markets_traded")]
    markets_traded: Option<u64>,
    #[serde(default, alias = "positions")]
    num_positions: Option<u64>,
}

/// A single trade/activity record, parsed from raw JSON to handle
/// Polymarket's varying response formats (numeric timestamps, different
/// field names across endpoints, etc.).
#[derive(Debug, Clone)]
struct Trade {
    condition_id: String,
    asset_id: String,
    side: String,
    size: f64,
    price: f64,
    outcome: String,
    /// Unix timestamp (seconds)
    timestamp_secs: Option<i64>,
    title: String,
    market_slug: String,
    /// Activity/trade type: BUY, SELL, REDEEM, MAKER, TAKER, etc.
    trade_type: String,
    transaction_hash: String,
}

impl Trade {
    /// Extract a Trade from a raw JSON object, trying multiple field name
    /// variants for each property. This handles the different schemas
    /// returned by /activity, /trades, and /data/trades endpoints.
    fn from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        Some(Self {
            condition_id: json_str(obj, &["conditionId", "condition_id", "market"]),
            asset_id: json_str(obj, &["asset", "assetId", "asset_id", "tokenID", "token_id"]),
            side: json_str(obj, &["side"]),
            size: json_f64(obj, &["size", "amount"]),
            price: json_f64(obj, &["price"]),
            outcome: json_str(obj, &["outcome", "outcomeLabel"]),
            timestamp_secs: json_timestamp(obj),
            title: json_str(obj, &["title", "question", "marketTitle", "market_title"]),
            market_slug: json_str(obj, &["marketSlug", "market_slug", "slug"]),
            trade_type: json_str(obj, &["type", "tradeType", "trade_type"]),
            transaction_hash: json_str(obj, &["transactionHash", "transaction_hash", "txHash"]),
        })
    }

    fn parsed_time(&self) -> Option<DateTime<Utc>> {
        self.timestamp_secs.and_then(|s| DateTime::from_timestamp(s, 0))
    }

    fn notional(&self) -> f64 {
        self.size * self.price
    }
}

/// Extract a string from a JSON object, trying multiple keys.
fn json_str(obj: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(val) = obj.get(*key) {
            match val {
                serde_json::Value::String(s) => return s.clone(),
                serde_json::Value::Number(n) => return n.to_string(),
                serde_json::Value::Bool(b) => return b.to_string(),
                _ => {}
            }
        }
    }
    String::new()
}

/// Extract a float from a JSON object, trying multiple keys.
fn json_f64(obj: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> f64 {
    for key in keys {
        if let Some(val) = obj.get(*key) {
            match val {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        return f;
                    }
                }
                serde_json::Value::String(s) => {
                    if let Ok(f) = s.parse::<f64>() {
                        return f;
                    }
                }
                _ => {}
            }
        }
    }
    0.0
}

/// Extract a unix timestamp (seconds) from a JSON object.
/// Handles: numeric seconds, numeric millis, ISO strings.
fn json_timestamp(obj: &serde_json::Map<String, serde_json::Value>) -> Option<i64> {
    let keys = ["timestamp", "createdAt", "created_at", "time"];
    for key in keys {
        if let Some(val) = obj.get(key) {
            match val {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        // Distinguish seconds from milliseconds
                        if i > 1_000_000_000_000 {
                            return Some(i / 1000); // millis → secs
                        }
                        return Some(i);
                    }
                    if let Some(f) = n.as_f64() {
                        return Some(f as i64);
                    }
                }
                serde_json::Value::String(s) => {
                    // ISO 8601
                    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                        return Some(dt.timestamp());
                    }
                    // Try "2025-01-15T12:00:00.000Z" without timezone offset
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
                        return Some(dt.and_utc().timestamp());
                    }
                    // Plain numeric string
                    if let Ok(i) = s.parse::<i64>() {
                        if i > 1_000_000_000_000 {
                            return Some(i / 1000);
                        }
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

/// Parse an array of JSON values into Trade structs.
fn parse_trades_from_json(text: &str) -> Vec<Trade> {
    let val: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    // Could be a plain array or wrapped in {"data": [...]} / {"trades": [...]}
    let items = match &val {
        serde_json::Value::Array(arr) => arr.clone(),
        serde_json::Value::Object(obj) => {
            for key in &["data", "trades", "results", "items", "activity"] {
                if let Some(serde_json::Value::Array(arr)) = obj.get(*key) {
                    return arr.iter().filter_map(|v| {
                        v.as_object().and_then(Trade::from_json)
                    }).collect();
                }
            }
            return Vec::new();
        }
        _ => return Vec::new(),
    };

    items.iter().filter_map(|v| v.as_object().and_then(Trade::from_json)).collect()
}

/// A position from the data API.
#[derive(Debug, Deserialize)]
struct Position {
    #[serde(default, alias = "conditionId", alias = "condition_id")]
    condition_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    outcome: String,
    #[serde(default)]
    size: f64,
    #[serde(default, alias = "avgPrice", alias = "avg_price")]
    avg_price: f64,
    #[serde(default, alias = "currentPrice", alias = "current_price")]
    current_price: f64,
    #[serde(default, alias = "initialValue", alias = "initial_value")]
    initial_value: f64,
    #[serde(default, alias = "currentValue", alias = "current_value")]
    current_value: f64,
    #[serde(default, alias = "pnl")]
    pnl: f64,
    #[serde(default, alias = "cashPnl", alias = "cash_pnl")]
    cash_pnl: Option<f64>,
    #[serde(default, alias = "percentPnl", alias = "percent_pnl")]
    percent_pnl: Option<f64>,
}

// ── API fetching ──────────────────────────────────────────────────────

async fn resolve_profile(client: &Client, username: &str) -> anyhow::Result<GammaProfile> {
    // If it looks like an address, skip username resolution
    if username.starts_with("0x") && username.len() > 10 {
        return Ok(GammaProfile {
            proxy_wallet: username.to_string(),
            username: username.to_string(),
            ..Default::default()
        });
    }

    // Try Gamma API profile search
    let endpoints = [
        format!("{}/profiles?username={}", GAMMA_API, username),
        format!("{}/profiles?_q={}", GAMMA_API, username),
        format!("{}/profiles/search?_q={}", GAMMA_API, username),
    ];

    for url in &endpoints {
        eprintln!("  trying: {}", url);
        match client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let text = resp.text().await.unwrap_or_default();
                // Response might be an array or a single object
                if let Ok(profiles) = serde_json::from_str::<Vec<GammaProfile>>(&text) {
                    if let Some(p) = profiles.into_iter().find(|p| {
                        p.username.eq_ignore_ascii_case(username)
                            || p.name.eq_ignore_ascii_case(username)
                    }) {
                        return Ok(p);
                    }
                    // If no exact match, take first result
                    if let Ok(profiles) = serde_json::from_str::<Vec<GammaProfile>>(&text) {
                        if let Some(p) = profiles.into_iter().next() {
                            return Ok(p);
                        }
                    }
                }
                if let Ok(p) = serde_json::from_str::<GammaProfile>(&text) {
                    if !p.proxy_wallet.is_empty() {
                        return Ok(p);
                    }
                }
                eprintln!("    got 200 but couldn't parse profile (body: {}...)",
                    &text[..text.len().min(200)]);
            }
            Ok(resp) => {
                eprintln!("    {} {}", resp.status(), url);
            }
            Err(e) => {
                eprintln!("    error: {}", e);
            }
        }
    }

    anyhow::bail!(
        "Could not resolve username '{}'. Try passing the wallet address (0x...) directly.",
        username
    );
}

impl Default for GammaProfile {
    fn default() -> Self {
        Self {
            proxy_wallet: String::new(),
            name: String::new(),
            username: String::new(),
            profile_picture: String::new(),
            bio: String::new(),
            total_volume: None,
            pnl: None,
            markets_traded: None,
            num_positions: None,
        }
    }
}

async fn fetch_trades(client: &Client, address: &str, limit: usize) -> anyhow::Result<Vec<Trade>> {
    let mut all_trades = Vec::new();
    let page_size = 100;

    // Try multiple API patterns — the first one that returns data wins.
    // Polymarket has several endpoints with different schemas; our JSON
    // extractor handles all of them.
    let base_urls = [
        format!("{}/activity?user={}", DATA_API, address),
        format!("{}/trades?user={}", DATA_API, address),
        format!("{}/data/trades?maker_address={}", CLOB_API, address),
    ];

    for base_url in &base_urls {
        eprintln!("  trying trades: {}&limit={}", base_url, page_size);
        let mut offset = 0;
        let mut found_trades = false;

        loop {
            let url = format!("{}&limit={}&offset={}", base_url, page_size, offset);
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let text = resp.text().await.unwrap_or_default();
                    let page = parse_trades_from_json(&text);

                    if page.is_empty() {
                        if offset == 0 && !text.is_empty() && text != "[]" {
                            eprintln!("    0 trades parsed from response ({}...)",
                                &text[..text.len().min(200)]);
                        }
                        break;
                    }

                    found_trades = true;
                    let page_len = page.len();
                    all_trades.extend(page);

                    if all_trades.len() >= limit || page_len < page_size {
                        break;
                    }
                    offset += page_size;
                }
                Ok(resp) => {
                    eprintln!("    {} for trades endpoint", resp.status());
                    break;
                }
                Err(e) => {
                    eprintln!("    error: {}", e);
                    break;
                }
            }
        }

        if found_trades {
            eprintln!("  fetched {} trades from {}", all_trades.len(), base_url);
            break;
        }
    }

    all_trades.truncate(limit);
    Ok(all_trades)
}

async fn fetch_positions(client: &Client, address: &str) -> anyhow::Result<Vec<Position>> {
    let urls = [
        format!("{}/positions?user={}", DATA_API, address),
        format!("{}/data/positions?user={}", CLOB_API, address),
    ];

    for url in &urls {
        eprintln!("  trying positions: {}", url);
        match client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let text = resp.text().await.unwrap_or_default();
                if let Ok(positions) = serde_json::from_str::<Vec<Position>>(&text) {
                    eprintln!("  fetched {} positions", positions.len());
                    return Ok(positions);
                }
                #[derive(Deserialize)]
                struct Wrapper {
                    #[serde(default, alias = "data", alias = "positions")]
                    items: Vec<Position>,
                }
                if let Ok(w) = serde_json::from_str::<Wrapper>(&text) {
                    eprintln!("  fetched {} positions", w.items.len());
                    return Ok(w.items);
                }
                eprintln!("    couldn't parse positions ({}...)", &text[..text.len().min(200)]);
            }
            Ok(resp) => eprintln!("    {} for positions", resp.status()),
            Err(e) => eprintln!("    error: {}", e),
        }
    }

    Ok(Vec::new())
}

// ── Analysis ──────────────────────────────────────────────────────────

fn analyze(profile: &GammaProfile, trades: &[Trade], positions: &[Position]) {
    println!("\n{}", "=".repeat(70));
    println!("  PROFILE ANALYSIS: @{}", profile.username);
    println!("{}", "=".repeat(70));

    // ── Profile summary ──
    println!("\n## Profile");
    if !profile.name.is_empty() {
        println!("  Name:           {}", profile.name);
    }
    println!("  Wallet:         {}", profile.proxy_wallet);
    if let Some(vol) = profile.total_volume {
        println!("  Total Volume:   ${:.2}", vol);
    }
    if let Some(pnl) = profile.pnl {
        println!("  PnL:            ${:+.2}", pnl);
    }
    if let Some(n) = profile.markets_traded {
        println!("  Markets Traded: {}", n);
    }
    if let Some(n) = profile.num_positions {
        println!("  Open Positions: {}", n);
    }

    if trades.is_empty() && positions.is_empty() {
        println!("\n  No trades or positions fetched — cannot perform analysis.");
        println!("  Try passing the proxy wallet address directly if username resolution");
        println!("  returned a different wallet.");
        return;
    }

    // ── Parse timestamps ──
    let mut timed_trades: Vec<(&Trade, DateTime<Utc>)> = trades
        .iter()
        .filter_map(|t| t.parsed_time().map(|dt| (t, dt)))
        .collect();
    timed_trades.sort_by_key(|(_, dt)| *dt);

    println!("\n## Trade Activity ({} trades fetched, {} with timestamps)",
        trades.len(), timed_trades.len());

    // Show activity type breakdown (BUY/SELL/REDEEM/etc.)
    if !trades.is_empty() {
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for t in trades {
            let key = if !t.trade_type.is_empty() {
                t.trade_type.to_uppercase()
            } else if !t.side.is_empty() {
                t.side.to_uppercase()
            } else {
                "UNKNOWN".to_string()
            };
            *type_counts.entry(key).or_insert(0) += 1;
        }
        let mut type_sorted: Vec<_> = type_counts.iter().collect();
        type_sorted.sort_by(|a, b| b.1.cmp(a.1));
        println!("  Activity types:");
        for (ty, count) in &type_sorted {
            println!("    {:12} {}", ty, count);
        }
    }

    if let (Some(first), Some(last)) = (timed_trades.first(), timed_trades.last()) {
        let span = last.1 - first.1;
        println!("  Date range:     {} → {}",
            first.1.format("%Y-%m-%d %H:%M"),
            last.1.format("%Y-%m-%d %H:%M"));
        println!("  Span:           {} days", span.num_days());
        if span.num_days() > 0 {
            println!("  Avg trades/day: {:.1}",
                timed_trades.len() as f64 / span.num_days().max(1) as f64);
        }
    }

    // ── Total volume ──
    let total_notional: f64 = trades.iter().map(|t| t.notional()).sum();
    let avg_size: f64 = if !trades.is_empty() {
        trades.iter().map(|t| t.size).sum::<f64>() / trades.len() as f64
    } else {
        0.0
    };
    println!("  Total notional: ${:.2}", total_notional);
    println!("  Avg trade size: {:.2} shares", avg_size);

    // ── MAKER vs TAKER ──
    let maker_count = trades.iter().filter(|t| {
        t.trade_type.eq_ignore_ascii_case("MAKER")
    }).count();
    let taker_count = trades.iter().filter(|t| {
        t.trade_type.eq_ignore_ascii_case("TAKER")
    }).count();
    if maker_count + taker_count > 0 {
        println!("  Maker trades:   {} ({:.1}%)", maker_count,
            100.0 * maker_count as f64 / (maker_count + taker_count) as f64);
        println!("  Taker trades:   {} ({:.1}%)", taker_count,
            100.0 * taker_count as f64 / (maker_count + taker_count) as f64);
    }

    // ── BUY vs SELL vs REDEEM ──
    let buys = trades.iter().filter(|t| {
        t.side.eq_ignore_ascii_case("BUY") || t.side == "0"
            || t.trade_type.eq_ignore_ascii_case("BUY")
    }).count();
    let sells = trades.iter().filter(|t| {
        t.side.eq_ignore_ascii_case("SELL") || t.side == "1"
            || t.trade_type.eq_ignore_ascii_case("SELL")
    }).count();
    let redeems = trades.iter().filter(|t| {
        t.trade_type.eq_ignore_ascii_case("REDEEM")
    }).count();
    if buys + sells + redeems > 0 {
        let total_bsr = (buys + sells + redeems) as f64;
        println!("  Buys:           {} ({:.1}%)", buys, 100.0 * buys as f64 / total_bsr);
        println!("  Sells:          {} ({:.1}%)", sells, 100.0 * sells as f64 / total_bsr);
        if redeems > 0 {
            println!("  Redeems:        {} ({:.1}%)", redeems, 100.0 * redeems as f64 / total_bsr);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // PROGRAMMATIC TRADING INDICATORS
    // ═══════════════════════════════════════════════════════════════════

    println!("\n## Programmatic Trading Indicators");

    // ── 1. Inter-trade interval analysis ──
    if timed_trades.len() >= 2 {
        let mut intervals: Vec<f64> = Vec::new();
        for window in timed_trades.windows(2) {
            let delta = (window[1].1 - window[0].1).num_milliseconds() as f64 / 1000.0;
            if delta >= 0.0 {
                intervals.push(delta);
            }
        }

        if !intervals.is_empty() {
            intervals.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = intervals[intervals.len() / 2];
            let mean: f64 = intervals.iter().sum::<f64>() / intervals.len() as f64;
            let variance: f64 = intervals.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                / intervals.len() as f64;
            let std_dev = variance.sqrt();
            let cv = if mean > 0.0 { std_dev / mean } else { 0.0 };

            let sub_1s = intervals.iter().filter(|&&x| x < 1.0).count();
            let sub_5s = intervals.iter().filter(|&&x| x < 5.0).count();
            let sub_60s = intervals.iter().filter(|&&x| x < 60.0).count();

            println!("\n  Inter-trade intervals ({} intervals):", intervals.len());
            println!("    Min:              {:.1}s", intervals.first().unwrap_or(&0.0));
            println!("    Median:           {:.1}s", median);
            println!("    Mean:             {:.1}s", mean);
            println!("    Std dev:          {:.1}s", std_dev);
            println!("    Coeff of var:     {:.3}", cv);
            println!("    Max:              {:.1}s", intervals.last().unwrap_or(&0.0));
            println!("    Sub-1s:           {} ({:.1}%)", sub_1s,
                100.0 * sub_1s as f64 / intervals.len() as f64);
            println!("    Sub-5s:           {} ({:.1}%)", sub_5s,
                100.0 * sub_5s as f64 / intervals.len() as f64);
            println!("    Sub-60s:          {} ({:.1}%)", sub_60s,
                100.0 * sub_60s as f64 / intervals.len() as f64);

            // Verdict
            if cv < 0.3 && median < 30.0 {
                println!("    >>> STRONG BOT SIGNAL: very regular intervals (CV={:.3})", cv);
            } else if sub_5s as f64 / intervals.len() as f64 > 0.3 {
                println!("    >>> BOT SIGNAL: >30% of trades within 5s of each other");
            } else if cv < 0.5 {
                println!("    >>> LIKELY PROGRAMMATIC: somewhat regular intervals");
            } else {
                println!("    >>> Intervals look human-like (irregular)");
            }
        }
    }

    // ── 2. Time-of-day distribution ──
    if !timed_trades.is_empty() {
        let mut hour_counts = [0u32; 24];
        for (_, dt) in &timed_trades {
            hour_counts[dt.hour() as usize] += 1;
        }

        println!("\n  Time-of-day distribution (UTC):");
        let max_count = *hour_counts.iter().max().unwrap_or(&1);
        for (hour, &count) in hour_counts.iter().enumerate() {
            let bar_len = if max_count > 0 { (count as f64 / max_count as f64 * 30.0) as usize } else { 0 };
            let bar: String = "█".repeat(bar_len);
            println!("    {:02}:00  {:4}  {}", hour, count, bar);
        }

        // Check for 24/7 activity
        let active_hours = hour_counts.iter().filter(|&&c| c > 0).count();
        let quiet_hours = hour_counts.iter().filter(|&&c| c == 0).count();
        if active_hours >= 22 {
            println!("    >>> BOT SIGNAL: active in {}/24 hours (no sleep pattern)", active_hours);
        } else if quiet_hours >= 6 {
            println!("    >>> Human pattern: {} quiet hours (likely sleeping)", quiet_hours);
        }

        // Day-of-week
        let mut dow_counts = [0u32; 7];
        for (_, dt) in &timed_trades {
            dow_counts[dt.weekday().num_days_from_monday() as usize] += 1;
        }
        let dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
        println!("\n  Day-of-week distribution:");
        for (i, &count) in dow_counts.iter().enumerate() {
            println!("    {}:  {}", dow_names[i], count);
        }
    }

    // ── 3. Order size analysis ──
    {
        let sizes: Vec<f64> = trades.iter().map(|t| t.size).filter(|&s| s > 0.0).collect();
        if !sizes.is_empty() {
            // Check for round numbers
            let round_count = sizes.iter().filter(|&&s| {
                (s * 100.0).round() == s * 100.0 // 2 decimal places
                    && (s == s.round() || s * 10.0 == (s * 10.0).round())
            }).count();
            let round_pct = 100.0 * round_count as f64 / sizes.len() as f64;

            // Check for repeated sizes
            let mut size_freq: HashMap<String, usize> = HashMap::new();
            for &s in &sizes {
                let key = format!("{:.4}", s);
                *size_freq.entry(key).or_insert(0) += 1;
            }
            let mut freq_sorted: Vec<(String, usize)> = size_freq.into_iter().collect();
            freq_sorted.sort_by(|a, b| b.1.cmp(&a.1));

            let most_common = &freq_sorted[0];
            let repeat_pct = 100.0 * most_common.1 as f64 / sizes.len() as f64;

            println!("\n  Order size analysis ({} orders):", sizes.len());
            println!("    Round-number orders: {} ({:.1}%)", round_count, round_pct);
            println!("    Most common size:    {} (used {} times, {:.1}%)",
                most_common.0, most_common.1, repeat_pct);
            println!("    Top 5 sizes:");
            for (size, count) in freq_sorted.iter().take(5) {
                println!("      {} shares × {} trades", size, count);
            }

            if repeat_pct > 40.0 {
                println!("    >>> BOT SIGNAL: {:.0}% of trades use the same size", repeat_pct);
            } else if round_pct > 70.0 {
                println!("    >>> LIKELY PROGRAMMATIC: {:.0}% round-number sizes", round_pct);
            }
        }
    }

    // ── 4. Market concentration ──
    {
        let mut market_trades: HashMap<String, Vec<&Trade>> = HashMap::new();
        for t in trades {
            let key = if !t.title.is_empty() {
                t.title.clone()
            } else if !t.market_slug.is_empty() {
                t.market_slug.clone()
            } else if !t.condition_id.is_empty() {
                t.condition_id.clone()
            } else {
                "unknown".to_string()
            };
            market_trades.entry(key).or_default().push(t);
        }

        let mut market_volumes: Vec<(String, f64, usize)> = market_trades
            .iter()
            .map(|(name, trades)| {
                let vol: f64 = trades.iter().map(|t| t.notional()).sum();
                (name.clone(), vol, trades.len())
            })
            .collect();
        market_volumes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        println!("\n  Market concentration ({} unique markets):", market_volumes.len());
        for (name, vol, count) in market_volumes.iter().take(10) {
            let display = if name.len() > 50 { &name[..50] } else { name };
            println!("    ${:>10.2}  ({:>3} trades)  {}", vol, count, display);
        }

        if market_volumes.len() > 50 {
            println!("    >>> BOT SIGNAL: trading across {} markets (high breadth)", market_volumes.len());
        }

        // Check if top market dominates
        if let Some(top) = market_volumes.first() {
            let top_pct = 100.0 * top.1 / total_notional.max(0.01);
            if top_pct > 50.0 {
                println!("    >>> CONCENTRATED: {:.0}% of volume in top market", top_pct);
            }
        }
    }

    // ── 5. Price clustering ──
    {
        let prices: Vec<f64> = trades.iter().map(|t| t.price).filter(|&p| p > 0.0).collect();
        if !prices.is_empty() {
            // Check if trades cluster at extreme prices (near 0 or 1)
            let near_extremes = prices.iter().filter(|&&p| p < 0.10 || p > 0.90).count();
            let extreme_pct = 100.0 * near_extremes as f64 / prices.len() as f64;

            // Check for mispricing exploitation: buying very cheap or selling very dear
            let cheap_buys = trades.iter().filter(|t| {
                (t.side.eq_ignore_ascii_case("BUY") || t.side == "0") && t.price < 0.15
            }).count();
            let dear_sells = trades.iter().filter(|t| {
                (t.side.eq_ignore_ascii_case("SELL") || t.side == "1") && t.price > 0.85
            }).count();

            println!("\n  Price distribution:");
            println!("    Trades at extremes (<10c or >90c): {} ({:.1}%)",
                near_extremes, extreme_pct);
            println!("    Cheap buys (<15c):  {}", cheap_buys);
            println!("    Dear sells (>85c):  {}", dear_sells);

            if extreme_pct > 40.0 {
                println!("    >>> STRATEGY: trading at price extremes (mispricing / new market)");
            }
            if cheap_buys > trades.len() / 4 {
                println!("    >>> STRATEGY: heavily buying cheap options (new market sniper)");
            }
        }
    }

    // ── 6. Burst detection ──
    if timed_trades.len() >= 5 {
        let mut bursts = Vec::new();
        let window_secs = 60; // 1-minute windows
        let mut i = 0;
        while i < timed_trades.len() {
            let window_start = timed_trades[i].1;
            let window_end = window_start + Duration::seconds(window_secs);
            let mut j = i + 1;
            while j < timed_trades.len() && timed_trades[j].1 <= window_end {
                j += 1;
            }
            let burst_size = j - i;
            if burst_size >= 5 {
                bursts.push((window_start, burst_size));
            }
            i = j.max(i + 1);
        }

        if !bursts.is_empty() {
            bursts.sort_by(|a, b| b.1.cmp(&a.1));
            println!("\n  Trade bursts (5+ trades in 60s):");
            for (time, count) in bursts.iter().take(10) {
                println!("    {}  {} trades", time.format("%Y-%m-%d %H:%M:%S"), count);
            }
            println!("    >>> BOT SIGNAL: {} burst events detected", bursts.len());
        }
    }

    // ── 7. Maker ratio analysis ──
    if maker_count + taker_count > 0 {
        let maker_ratio = maker_count as f64 / (maker_count + taker_count) as f64;
        println!("\n  Maker/Taker analysis:");
        println!("    Maker ratio: {:.1}%", maker_ratio * 100.0);
        if maker_ratio > 0.7 {
            println!("    >>> MARKET MAKER: {:.0}% maker orders — posting limit orders and providing liquidity", maker_ratio * 100.0);
        } else if maker_ratio < 0.2 {
            println!("    >>> AGGRESSIVE TAKER: {:.0}% taker — hitting existing orders (sniper/arb)", maker_ratio * 100.0);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // POSITIONS ANALYSIS
    // ═══════════════════════════════════════════════════════════════════

    if !positions.is_empty() {
        println!("\n## Open Positions ({} total)", positions.len());

        let total_value: f64 = positions.iter().map(|p| p.current_value).sum();
        let total_pnl: f64 = positions.iter().map(|p| p.pnl).sum();
        let winning = positions.iter().filter(|p| p.pnl > 0.0).count();
        let losing = positions.iter().filter(|p| p.pnl < 0.0).count();

        println!("  Total current value: ${:.2}", total_value);
        println!("  Total PnL:           ${:+.2}", total_pnl);
        println!("  Winning positions:   {} ({:.1}%)", winning,
            100.0 * winning as f64 / positions.len() as f64);
        println!("  Losing positions:    {} ({:.1}%)", losing,
            100.0 * losing as f64 / positions.len() as f64);

        // Sort by absolute PnL
        let mut sorted_pos: Vec<&Position> = positions.iter().collect();
        sorted_pos.sort_by(|a, b| b.pnl.abs().partial_cmp(&a.pnl.abs()).unwrap_or(std::cmp::Ordering::Equal));

        println!("\n  Biggest positions by PnL:");
        for p in sorted_pos.iter().take(10) {
            let display = if p.title.len() > 45 { &p.title[..45] } else { &p.title };
            println!("    ${:>+8.2}  {}  {}", p.pnl, p.outcome, display);
        }

        if winning as f64 / positions.len() as f64 > 0.65 {
            println!("\n    >>> HIGH WIN RATE: {:.0}% winning positions — systematic edge",
                100.0 * winning as f64 / positions.len() as f64);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // OVERALL VERDICT
    // ═══════════════════════════════════════════════════════════════════

    println!("\n{}", "=".repeat(70));
    println!("  VERDICT");
    println!("{}", "=".repeat(70));

    let mut bot_signals = 0u32;
    let mut strategy_notes = Vec::new();

    // Count bot signals from above analysis
    if timed_trades.len() >= 2 {
        let intervals: Vec<f64> = timed_trades.windows(2)
            .map(|w| (w[1].1 - w[0].1).num_milliseconds() as f64 / 1000.0)
            .filter(|&d| d >= 0.0)
            .collect();
        if !intervals.is_empty() {
            let mean: f64 = intervals.iter().sum::<f64>() / intervals.len() as f64;
            let variance: f64 = intervals.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                / intervals.len() as f64;
            let cv = if mean > 0.0 { variance.sqrt() / mean } else { 0.0 };
            let sub_5s = intervals.iter().filter(|&&x| x < 5.0).count();

            if cv < 0.3 { bot_signals += 2; }
            else if cv < 0.5 { bot_signals += 1; }
            if sub_5s as f64 / intervals.len() as f64 > 0.3 { bot_signals += 1; }
        }
    }

    // 24/7 activity
    if !timed_trades.is_empty() {
        let mut hour_counts = [0u32; 24];
        for (_, dt) in &timed_trades {
            hour_counts[dt.hour() as usize] += 1;
        }
        let active_hours = hour_counts.iter().filter(|&&c| c > 0).count();
        if active_hours >= 22 { bot_signals += 2; }
    }

    // Market breadth
    let unique_markets: std::collections::HashSet<&str> = trades.iter()
        .map(|t| if !t.condition_id.is_empty() { t.condition_id.as_str() } else { "?" })
        .collect();
    if unique_markets.len() > 50 { bot_signals += 1; }

    // Maker ratio
    if maker_count + taker_count > 0 {
        let maker_ratio = maker_count as f64 / (maker_count + taker_count) as f64;
        if maker_ratio > 0.7 {
            strategy_notes.push("Market maker (mostly limit orders)");
        } else if maker_ratio < 0.2 {
            strategy_notes.push("Aggressive taker (hitting orders — sniper/arb style)");
            bot_signals += 1;
        }
    }

    // Cheap buys
    let cheap_buys = trades.iter().filter(|t| {
        (t.side.eq_ignore_ascii_case("BUY") || t.side == "0") && t.price < 0.15
    }).count();
    if cheap_buys > trades.len() / 4 {
        strategy_notes.push("New-market sniper (buys cheap options early)");
    }

    let extreme_trades = trades.iter().filter(|t| t.price < 0.10 || t.price > 0.90).count();
    if extreme_trades > trades.len() * 40 / 100 {
        strategy_notes.push("Extreme-price trader (exploits mispricing at tails)");
    }

    // Final verdict
    let verdict = match bot_signals {
        0..=1 => "LIKELY HUMAN — no strong programmatic signals",
        2..=3 => "POSSIBLY PROGRAMMATIC — some bot-like patterns",
        4..=5 => "LIKELY BOT — multiple programmatic indicators",
        _ =>     "ALMOST CERTAINLY A BOT — overwhelming programmatic evidence",
    };

    println!("\n  Bot score:  {}/7", bot_signals);
    println!("  Assessment: {}", verdict);
    if !strategy_notes.is_empty() {
        println!("\n  Strategy signals:");
        for note in &strategy_notes {
            println!("    - {}", note);
        }
    }

    println!();
}

// ── Main ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let username = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo run --bin profile -- <username_or_address>");
        eprintln!("Example: cargo run --bin profile -- MistahFox");
        eprintln!("         cargo run --bin profile -- 0x1234abcd...");
        std::process::exit(1);
    });

    let max_trades: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // Step 1: Resolve profile
    eprintln!("Resolving profile for '{}'...", username);
    let profile = resolve_profile(&client, &username).await?;
    eprintln!("  → wallet: {}", profile.proxy_wallet);

    if profile.proxy_wallet.is_empty() {
        anyhow::bail!("Could not resolve wallet address for '{}'. Try the 0x address directly.", username);
    }

    // Step 2: Fetch trades
    eprintln!("Fetching trades (up to {})...", max_trades);
    let trades = fetch_trades(&client, &profile.proxy_wallet, max_trades).await?;
    eprintln!("  → {} trades", trades.len());

    // Step 3: Fetch positions
    eprintln!("Fetching positions...");
    let positions = fetch_positions(&client, &profile.proxy_wallet).await?;
    eprintln!("  → {} positions", positions.len());

    // Step 4: Analyze
    analyze(&profile, &trades, &positions);

    Ok(())
}
