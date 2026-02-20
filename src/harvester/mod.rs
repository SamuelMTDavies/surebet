//! Shared harvester types and functions used by both the CLI harvester
//! and the web dashboard binary.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::orderbook::OrderBook;

// ─── Gamma API response type ────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    #[serde(default)]
    pub id: serde_json::Value,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub outcomes: Option<String>,
    #[serde(default)]
    pub clob_token_ids: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub end_date_iso: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub active: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    pub closed: Option<bool>,
    #[serde(default)]
    pub accepting_orders: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    pub fees_enabled: Option<bool>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
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

pub async fn scan_markets(
    gamma_url: &str,
    end_date_window_days: i64,
    max_display: usize,
) -> Result<Vec<HarvestableMarket>> {
    let http = reqwest::Client::new();
    let now = Utc::now();

    let date_min = now.format("%Y-%m-%d").to_string();
    let date_max = (now + Duration::days(end_date_window_days + 1))
        .format("%Y-%m-%d")
        .to_string();

    let mut all_raw: Vec<GammaMarket> = Vec::new();
    let mut offset = 0usize;
    let limit = 100;

    loop {
        let url = format!(
            "{}/markets?active=true&closed=false&end_date_min={}&end_date_max={}&limit={}&offset={}",
            gamma_url, date_min, date_max, limit, offset,
        );
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

        let page: Vec<GammaMarket> = resp.json().await.context("Failed to parse Gamma response")?;
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
        "  Fetched {} markets ending {} → {} ({} pages)",
        all_raw.len(),
        date_min,
        date_max,
        pages,
    );

    let mut results: Vec<HarvestableMarket> = Vec::new();

    for raw in &all_raw {
        if raw.accepting_orders != Some(true) {
            continue;
        }

        let condition_id = match &raw.condition_id {
            Some(c) if !c.is_empty() => c.clone(),
            _ => continue,
        };

        let token_ids_str = match &raw.clob_token_ids {
            Some(s) if !s.is_empty() && s != "[]" => s.clone(),
            _ => continue,
        };

        let clob_token_ids: Vec<String> = match serde_json::from_str(&token_ids_str) {
            Ok(ids) => ids,
            Err(_) => continue,
        };
        if clob_token_ids.is_empty() {
            continue;
        }

        let outcomes: Vec<String> = raw
            .outcomes
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        if outcomes.is_empty() {
            continue;
        }

        let end_date = raw
            .end_date
            .as_ref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let question = raw.question.clone().unwrap_or_else(|| "???".to_string());
        let market_id = match &raw.id {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => "?".to_string(),
        };

        results.push(HarvestableMarket {
            market_id,
            condition_id,
            question,
            outcomes,
            clob_token_ids,
            end_date,
            is_neg_risk: raw.neg_risk.unwrap_or(false),
            category: raw.category.clone().unwrap_or_default(),
            slug: raw.slug.clone(),
        });
    }

    // Sort: soonest end date first
    results.sort_by(|a, b| {
        let a_dt = a.end_date.unwrap_or(DateTime::<Utc>::MAX_UTC);
        let b_dt = b.end_date.unwrap_or(DateTime::<Utc>::MAX_UTC);
        a_dt.cmp(&b_dt)
    });

    results.truncate(max_display);

    println!(
        "  After filtering: {} markets within end-date window",
        results.len()
    );

    Ok(results)
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
