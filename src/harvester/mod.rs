//! Shared harvester types and functions used by both the CLI harvester
//! and the web dashboard binary.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use polymarket_client_sdk::gamma::types::response::Market as SdkMarket;

use crate::orderbook::{OrderBook, OrderBookStore};

// ─── Scan mode ───────────────────────────────────────────────────────────────

/// Which set of markets to target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanMode {
    /// Markets whose end date falls within the next N days (original behaviour).
    NearExpiry,
    /// Markets Gamma has already marked `closed=true`, whose end date falls
    /// within the past N days.  These have stopped trading but may not yet be
    /// redeemed on-chain — stale or residual CLOB orders can still exist.
    RecentlyClosed,
}

/// Parsed market ready for display.
#[derive(Debug, Clone, Serialize)]
pub struct HarvestableMarket {
    pub market_id: String,
    pub condition_id: String,
    pub question: String,
    pub outcomes: Vec<String>,
    pub clob_token_ids: Vec<String>,
    pub end_date: Option<DateTime<Utc>>,
    pub is_neg_risk: bool,
    pub category: String,
    pub slug: Option<String>,
    /// Whether the CLOB is currently accepting new orders.
    /// False for `RecentlyClosed` markets — orders cannot be placed, but tokens
    /// may still be redeemable on-chain.
    pub accepting_orders: bool,
    /// 24-hour trading volume in USDC at scan time (0.0 if unavailable).
    pub volume_24hr: f64,
    /// Gamma liquidity depth proxy in USDC at scan time (0.0 if unavailable).
    pub liquidity: f64,
}

/// Per-outcome book info.
///
/// Buy-side: sweep asks up to `max_buy` — profit comes from buying winner tokens
/// below $1.00 and redeeming.
///
/// Sell-side: hit bids above `min_sell` — revenue from selling loser tokens
/// obtained via CTF split ($1 USDC → 1 YES + 1 NO).  Every dollar sold
/// is pure profit since the split is costless (minus negligible gas).
#[derive(Debug, Clone, Serialize)]
pub struct OutcomeInfo {
    pub label: String,
    pub token_id: String,
    // Buy-winner side (asks)
    pub best_ask: Option<Decimal>,
    pub sweepable_shares: Decimal,
    pub sweepable_cost: Decimal,
    pub sweepable_profit: Decimal,
    // Sell-loser side (bids) — for split+sell strategy
    pub best_bid: Option<Decimal>,
    pub sellable_shares: Decimal,
    pub sellable_revenue: Decimal,
}

// ─── Gamma API scan ─────────────────────────────────────────────────────────

/// Fetch harvestable markets from the Gamma API.
///
/// - `mode` controls which markets to target (see [`ScanMode`]).
/// - `end_date_window_days`: for `NearExpiry`, how many days ahead to look.
/// - `closed_lookback_days`: for `RecentlyClosed`, how many days back to look.
/// - `max_display`: truncate the result list to this many entries.
/// - `min_volume_usd`: skip markets with 24 h volume below this (0 = no filter).
/// - `min_depth_usd`: skip markets with Gamma liquidity below this (0 = no filter).
pub async fn scan_markets(
    gamma_url: &str,
    mode: ScanMode,
    end_date_window_days: i64,
    closed_lookback_days: i64,
    max_display: usize,
    min_volume_usd: f64,
    min_depth_usd: f64,
) -> Result<Vec<HarvestableMarket>> {
    let http = reqwest::Client::new();
    let now = Utc::now();

    // Build the query URL depending on mode.
    // Use YYYY-MM-DD date strings — the Gamma API rejects full ISO-8601 timestamps.
    let (query_base, date_range_label) = match mode {
        ScanMode::NearExpiry => {
            let date_min = now.format("%Y-%m-%d").to_string();
            let date_max = (now + Duration::days(end_date_window_days + 1))
                .format("%Y-%m-%d")
                .to_string();
            let base = format!(
                "{}/markets?active=true&closed=false&end_date_min={}&end_date_max={}",
                gamma_url, date_min, date_max,
            );
            let label = format!("{} → {}", date_min, date_max);
            (base, label)
        }
        ScanMode::RecentlyClosed => {
            let date_min = (now - Duration::days(closed_lookback_days))
                .format("%Y-%m-%d")
                .to_string();
            let date_max = now.format("%Y-%m-%d").to_string();
            let base = format!(
                "{}/markets?closed=true&end_date_min={}&end_date_max={}",
                gamma_url, date_min, date_max,
            );
            let label = format!("{} → {}", date_min, date_max);
            (base, label)
        }
    };

    // Paginate manually, deserialising into the SDK's Market type which
    // correctly handles Gamma's inconsistent string-vs-number encoding.
    let mut all_raw: Vec<SdkMarket> = Vec::new();
    let mut offset = 0usize;
    let limit = 100usize;

    loop {
        let url = format!("{}&limit={}&offset={}", query_base, limit, offset);
        let resp = http
            .get(&url)
            .send()
            .await
            .context("Gamma API request failed")?;

        if !resp.status().is_success() {
            bail!(
                "Gamma API returned status {} at offset {}",
                resp.status(),
                offset
            );
        }

        let page: Vec<SdkMarket> = resp.json().await.context("Failed to parse Gamma response")?;
        let count = page.len();
        if count == 0 {
            break;
        }
        all_raw.extend(page);
        offset += count;

        if offset > 2000 {
            break;
        }
    }

    let pages = if offset == 0 { 0 } else { (offset + limit - 1) / limit };
    println!(
        "  Fetched {} markets ({}) ({} pages)",
        all_raw.len(),
        date_range_label,
        pages,
    );

    let min_volume = Decimal::try_from(min_volume_usd).unwrap_or_default();
    let min_depth = Decimal::try_from(min_depth_usd).unwrap_or_default();

    let mut results: Vec<HarvestableMarket> = Vec::new();

    for raw in &all_raw {
        // For NearExpiry: must be actively accepting orders.
        // For RecentlyClosed: accepting_orders is expected to be false — skip this gate.
        if mode == ScanMode::NearExpiry && raw.accepting_orders != Some(true) {
            continue;
        }

        let condition_id = match &raw.condition_id {
            Some(c) => format!("{c}"),
            _ => continue,
        };

        let clob_token_ids: Vec<String> = match &raw.clob_token_ids {
            Some(ids) if !ids.is_empty() => ids.iter().map(|id| format!("{id}")).collect(),
            _ => continue,
        };

        let outcomes: Vec<String> = match &raw.outcomes {
            Some(o) if !o.is_empty() => o.clone(),
            _ => continue,
        };

        // Liquidity gates (use Gamma's fields as proxies; skip if fields absent).
        let vol = raw.volume_24hr.unwrap_or_default();
        let liq = raw.liquidity.unwrap_or_default();
        if min_volume_usd > 0.0 && vol < min_volume {
            continue;
        }
        if min_depth_usd > 0.0 && liq < min_depth {
            continue;
        }

        let question = raw.question.clone().unwrap_or_else(|| "???".to_string());
        let market_id = raw.id.clone();

        results.push(HarvestableMarket {
            market_id,
            condition_id,
            question,
            outcomes,
            clob_token_ids,
            end_date: raw.end_date,
            is_neg_risk: raw.neg_risk.unwrap_or(false),
            category: raw.category.clone().unwrap_or_default(),
            slug: raw.slug.clone(),
            accepting_orders: raw.accepting_orders.unwrap_or(false),
            volume_24hr: vol.to_f64().unwrap_or(0.0),
            liquidity: liq.to_f64().unwrap_or(0.0),
        });
    }

    // NearExpiry: soonest end date first (most urgent).
    // RecentlyClosed: most recently closed first (freshest stale orders).
    match mode {
        ScanMode::NearExpiry => {
            results.sort_by(|a, b| {
                let a_dt = a.end_date.unwrap_or(DateTime::<Utc>::MAX_UTC);
                let b_dt = b.end_date.unwrap_or(DateTime::<Utc>::MAX_UTC);
                a_dt.cmp(&b_dt)
            });
        }
        ScanMode::RecentlyClosed => {
            results.sort_by(|a, b| {
                let a_dt = a.end_date.unwrap_or(DateTime::<Utc>::MIN_UTC);
                let b_dt = b.end_date.unwrap_or(DateTime::<Utc>::MIN_UTC);
                b_dt.cmp(&a_dt) // descending: most recently closed first
            });
        }
    }

    results.truncate(max_display);

    println!(
        "  After filtering: {} {} markets",
        results.len(),
        match mode {
            ScanMode::NearExpiry => "near-expiry",
            ScanMode::RecentlyClosed => "recently-closed",
        },
    );

    Ok(results)
}

// ─── Resting-order filter ────────────────────────────────────────────────────

/// Drop every market in `candidates` whose CLOB books are completely empty
/// (no resting bids or asks on any outcome token).
///
/// Used after `scan_markets(RecentlyClosed)` to surface only markets where
/// there is actually something to trade against.  Makes one REST request per
/// outcome token; prints a progress line per market.
pub async fn filter_with_resting_orders(
    clob_url: &str,
    candidates: Vec<HarvestableMarket>,
) -> Vec<HarvestableMarket> {
    if candidates.is_empty() {
        return candidates;
    }

    let total = candidates.len();
    println!("  Checking order books for {} closed markets...", total);

    let store = OrderBookStore::new();
    let mut kept = Vec::new();

    for (i, market) in candidates.into_iter().enumerate() {
        let mut has_orders = false;

        for token_id in &market.clob_token_ids {
            if let Some(book) = store.fetch_rest_book(clob_url, token_id).await {
                if !book.bids.levels.is_empty() || !book.asks.levels.is_empty() {
                    has_orders = true;
                    break;
                }
            }
        }

        let q = if market.question.len() > 60 {
            format!("{}...", &market.question[..57])
        } else {
            market.question.clone()
        };

        if has_orders {
            println!("  [{}/{}] OK   {}", i + 1, total, q);
            kept.push(market);
        } else {
            println!("  [{}/{}] --   {} (empty)", i + 1, total, q);
        }
    }

    println!(
        "  {} of {} closed markets have resting orders",
        kept.len(),
        total,
    );

    kept
}

// ─── Book analysis ──────────────────────────────────────────────────────────

pub fn build_outcome_info(
    label: &str,
    token_id: &str,
    book: &OrderBook,
    max_buy: Decimal,
    min_sell: Decimal,
) -> OutcomeInfo {
    // Buy side — sweep asks up to max_buy
    let best_ask = book.asks.best(false).map(|(p, _)| p);

    let mut shares = Decimal::ZERO;
    let mut cost = Decimal::ZERO;
    for (&price, &size) in book.asks.levels.iter() {
        if price > max_buy {
            break;
        }
        shares += size;
        cost += price * size;
    }

    let profit = shares - cost;

    // Sell side — hit bids at or above min_sell.
    // These are bids for LOSING tokens.  After a CTF split ($1 → 1 YES + 1 NO)
    // the losing side can be sold into these bids for pure revenue.
    let best_bid = book.bids.best(true).map(|(p, _)| p);

    let mut sell_shares = Decimal::ZERO;
    let mut sell_revenue = Decimal::ZERO;
    // BTreeMap iterates ascending; bids should be walked top-down
    for (&price, &size) in book.bids.levels.iter().rev() {
        if price < min_sell {
            break;
        }
        sell_shares += size;
        sell_revenue += price * size;
    }

    OutcomeInfo {
        label: label.to_string(),
        token_id: token_id.to_string(),
        best_ask,
        sweepable_shares: shares,
        sweepable_cost: cost,
        sweepable_profit: profit,
        best_bid,
        sellable_shares: sell_shares,
        sellable_revenue: sell_revenue,
    }
}
