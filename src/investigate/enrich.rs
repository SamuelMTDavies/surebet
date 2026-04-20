//! Market metadata enrichment via the CLOB API.
//!
//! For each unique condition_id found in the trader's data, queries the
//! Polymarket CLOB API (`/markets/{condition_id}`) to get question, tags,
//! end_date, outcome tokens, etc.
//!
//! Note: the Gamma API's `?condition_id=` query param is non-functional
//! (returns unrelated results), so we use the CLOB API which reliably
//! supports path-based lookup.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use super::fetch::RawTraderData;

/// Metadata about a market from the CLOB API.
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub condition_id: String,
    pub question: String,
    pub category: String,
    pub fee_type: Option<String>,
    pub end_date: Option<String>,
    pub outcomes: Vec<String>,
    pub active: bool,
}

/// Trader data enriched with market metadata.
#[derive(Debug, Clone)]
pub struct EnrichedTraderData {
    pub raw: RawTraderData,
    /// condition_id hex (with 0x prefix) -> market metadata
    pub market_meta: HashMap<String, MarketMeta>,
}

/// CLOB API market response (minimal fields we need).
#[derive(Debug, serde::Deserialize)]
struct ClobMarketResponse {
    condition_id: Option<String>,
    question: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    end_date_iso: Option<String>,
    #[serde(default)]
    tokens: Vec<ClobToken>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    market_slug: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ClobToken {
    #[allow(dead_code)]
    token_id: Option<String>,
    outcome: Option<String>,
    #[allow(dead_code)]
    winner: Option<bool>,
}

const CLOB_API: &str = "https://clob.polymarket.com";
const MAX_CONCURRENT: usize = 10;

/// Enrich raw trader data with CLOB market metadata.
pub async fn enrich(raw: &RawTraderData) -> Result<EnrichedTraderData> {
    // Collect all unique condition_ids (with 0x prefix)
    let mut cids = HashSet::new();

    for t in &raw.trades {
        cids.insert(format!("{:#x}", t.condition_id));
    }
    for p in &raw.open_positions {
        cids.insert(format!("{:#x}", p.condition_id));
    }
    for p in &raw.closed_positions {
        cids.insert(format!("{:#x}", p.condition_id));
    }

    info!(unique_markets = cids.len(), "enriching with CLOB metadata");

    let client = reqwest::Client::new();
    let semaphore = Semaphore::new(MAX_CONCURRENT);
    let mut handles = Vec::new();

    for cid in cids {
        let client = client.clone();
        let sem = &semaphore;

        handles.push(async move {
            let _permit = sem.acquire().await;
            let result = fetch_clob_market(&client, &cid).await;
            (cid, result)
        });
    }

    let results = futures_util::future::join_all(handles).await;

    let mut market_meta = HashMap::new();
    let mut found = 0;
    let mut missing = 0;

    for (cid, result) in results {
        match result {
            Ok(Some(meta)) => {
                found += 1;
                market_meta.insert(cid, meta);
            }
            Ok(None) => {
                missing += 1;
                debug!(condition_id = %cid, "no CLOB market found");
            }
            Err(e) => {
                missing += 1;
                debug!(condition_id = %cid, error = %e, "failed to fetch CLOB market");
            }
        }
    }

    info!(found, missing, "CLOB enrichment complete");

    Ok(EnrichedTraderData {
        raw: raw.clone(),
        market_meta,
    })
}

async fn fetch_clob_market(
    client: &reqwest::Client,
    condition_id: &str,
) -> Result<Option<MarketMeta>> {
    // CLOB API uses condition_id WITH 0x prefix in the path
    let url = format!("{CLOB_API}/markets/{condition_id}");
    let resp = client
        .get(&url)
        .send()
        .await
        .context("CLOB API request failed")?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let m: ClobMarketResponse = match resp.json().await {
        Ok(m) => m,
        Err(_) => return Ok(None),
    };

    let outcomes: Vec<String> = m
        .tokens
        .iter()
        .filter_map(|t| t.outcome.clone())
        .collect();

    // Derive category from tags (first tag that looks like a category)
    let category = m
        .tags
        .first()
        .cloned()
        .unwrap_or_default();

    // Derive fee_type from tags/slug heuristics
    let fee_type = infer_fee_type(&m.tags, m.market_slug.as_deref());

    Ok(Some(MarketMeta {
        condition_id: condition_id.to_string(),
        question: m.question.unwrap_or_default(),
        category,
        fee_type,
        end_date: m.end_date_iso,
        outcomes,
        active: m.active && !m.closed,
    }))
}

/// Infer the Polymarket fee type from tags and slug.
fn infer_fee_type(tags: &[String], slug: Option<&str>) -> Option<String> {
    let tags_lower: Vec<String> = tags.iter().map(|t| t.to_lowercase()).collect();
    let slug_lower = slug.unwrap_or("").to_lowercase();

    if tags_lower.iter().any(|t| t.contains("crypto")) || slug_lower.contains("crypto") {
        if tags_lower.iter().any(|t| t.contains("15") || t.contains("minute"))
            || slug_lower.contains("15-min")
            || slug_lower.contains("15min")
        {
            return Some("crypto_15_min".to_string());
        }
        return Some("crypto".to_string());
    }

    if tags_lower.iter().any(|t| t.contains("sport") || t.contains("nba") || t.contains("nfl")
        || t.contains("soccer") || t.contains("tennis") || t.contains("mma"))
    {
        return Some("sports".to_string());
    }

    None
}
