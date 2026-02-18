//! Market mapping cache: on-chain identifiers ↔ Polymarket API identifiers.
//!
//! Maps between:
//! - conditionId (on-chain, bytes32) ↔ market slug/title (Gamma API)
//! - questionId (UMA oracle) ↔ conditionId (CTF)
//! - asset/tokenId (CLOB) ↔ conditionId + outcomeIndex

use alloy::primitives::B256;
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// A market entry in the cache, combining on-chain and API data.
#[derive(Debug, Clone)]
pub struct CachedMarket {
    pub condition_id: String,
    pub question_id: Option<String>,
    pub question: String,
    pub slug: Option<String>,
    pub outcomes: Vec<String>,
    /// CLOB token IDs for each outcome, indexed by outcome position.
    pub clob_token_ids: Vec<String>,
    pub end_date: Option<String>,
    pub active: bool,
}

/// Thread-safe market mapping cache.
#[derive(Clone)]
pub struct MarketCache {
    /// conditionId (hex, lowercase, no 0x) → CachedMarket
    by_condition_id: Arc<DashMap<String, CachedMarket>>,
    /// questionId (hex, lowercase, no 0x) → conditionId
    question_to_condition: Arc<DashMap<String, String>>,
    /// CLOB token_id → (conditionId, outcome_index)
    token_to_condition: Arc<DashMap<String, (String, usize)>>,
    /// Gamma API base URL
    gamma_url: String,
    /// HTTP client for Gamma API queries
    http: Client,
}

/// Gamma API market response (subset of fields we need).
#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(default)]
    condition_id: String,
    #[serde(default)]
    question_id: Option<String>,
    #[serde(default)]
    question: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    outcomes: Option<String>, // JSON-encoded string like "[\"Yes\",\"No\"]"
    #[serde(default)]
    clob_token_ids: Option<String>, // JSON-encoded string
    #[serde(default)]
    end_date_iso: Option<String>,
    #[serde(default)]
    active: Option<bool>,
}

impl MarketCache {
    pub fn new(gamma_url: String) -> Self {
        Self {
            by_condition_id: Arc::new(DashMap::new()),
            question_to_condition: Arc::new(DashMap::new()),
            token_to_condition: Arc::new(DashMap::new()),
            gamma_url,
            http: Client::new(),
        }
    }

    /// Load all active markets from Gamma API on startup.
    pub async fn load_active_markets(&self) -> anyhow::Result<usize> {
        let url = format!("{}/markets?active=true&closed=false&limit=500", self.gamma_url);
        let resp = self.http.get(&url).send().await?;

        if !resp.status().is_success() {
            anyhow::bail!("Gamma API returned status {}", resp.status());
        }

        let markets: Vec<GammaMarket> = resp.json().await?;
        let mut count = 0;

        for market in markets {
            if market.condition_id.is_empty() {
                continue;
            }
            if let Some(cached) = self.parse_gamma_market(&market) {
                self.insert(cached);
                count += 1;
            }
        }

        info!(markets = count, "market cache loaded from Gamma API");
        Ok(count)
    }

    /// Query Gamma API for a specific condition_id and add to cache.
    /// Uses exponential backoff if the API hasn't indexed it yet.
    pub async fn lookup_condition(&self, condition_id_hex: &str) -> Option<CachedMarket> {
        // Check cache first
        if let Some(entry) = self.by_condition_id.get(condition_id_hex) {
            return Some(entry.clone());
        }

        // Query Gamma with retries (new on-chain events may not be indexed yet)
        let mut delay = std::time::Duration::from_millis(500);
        for attempt in 0..5 {
            let url = format!(
                "{}/markets?condition_id={}",
                self.gamma_url, condition_id_hex
            );

            match self.http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(markets) = resp.json::<Vec<GammaMarket>>().await {
                        if let Some(market) = markets.into_iter().next() {
                            if let Some(cached) = self.parse_gamma_market(&market) {
                                self.insert(cached.clone());
                                info!(
                                    condition_id = %condition_id_hex,
                                    question = %cached.question,
                                    attempt = attempt,
                                    "market resolved via Gamma API"
                                );
                                return Some(cached);
                            }
                        }
                    }
                }
                Ok(resp) => {
                    debug!(
                        status = %resp.status(),
                        attempt = attempt,
                        "Gamma API lookup failed, retrying"
                    );
                }
                Err(e) => {
                    warn!(error = %e, attempt = attempt, "Gamma API request error");
                }
            }

            if attempt < 4 {
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff: 500ms, 1s, 2s, 4s
            }
        }

        warn!(
            condition_id = %condition_id_hex,
            "failed to resolve condition_id via Gamma API after 5 attempts"
        );
        None
    }

    /// Look up a market by its UMA questionId.
    pub fn get_by_question_id(&self, question_id_hex: &str) -> Option<CachedMarket> {
        self.question_to_condition
            .get(question_id_hex)
            .and_then(|cid| self.by_condition_id.get(cid.value()).map(|e| e.clone()))
    }

    /// Look up a market by its conditionId.
    pub fn get_by_condition_id(&self, condition_id_hex: &str) -> Option<CachedMarket> {
        self.by_condition_id.get(condition_id_hex).map(|e| e.clone())
    }

    /// Look up which market a CLOB token belongs to.
    pub fn get_by_token_id(&self, token_id: &str) -> Option<(CachedMarket, usize)> {
        self.token_to_condition.get(token_id).and_then(|entry| {
            let (cid, idx) = entry.value();
            self.by_condition_id
                .get(cid)
                .map(|e| (e.clone(), *idx))
        })
    }

    /// Insert or update a market in the cache.
    pub fn insert(&self, market: CachedMarket) {
        let cid = market.condition_id.clone();

        // Map questionId → conditionId
        if let Some(ref qid) = market.question_id {
            if !qid.is_empty() {
                self.question_to_condition
                    .insert(qid.clone(), cid.clone());
            }
        }

        // Map tokenId → (conditionId, outcomeIndex)
        for (idx, token_id) in market.clob_token_ids.iter().enumerate() {
            self.token_to_condition
                .insert(token_id.clone(), (cid.clone(), idx));
        }

        self.by_condition_id.insert(cid, market);
    }

    /// Number of markets in the cache.
    pub fn len(&self) -> usize {
        self.by_condition_id.len()
    }

    /// Parse a Gamma API market response into a CachedMarket.
    fn parse_gamma_market(&self, market: &GammaMarket) -> Option<CachedMarket> {
        let outcomes: Vec<String> = market
            .outcomes
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let clob_token_ids: Vec<String> = market
            .clob_token_ids
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        if market.condition_id.is_empty() {
            return None;
        }

        Some(CachedMarket {
            condition_id: market.condition_id.clone(),
            question_id: market.question_id.clone(),
            question: market.question.clone(),
            slug: market.slug.clone(),
            outcomes,
            clob_token_ids,
            end_date: market.end_date_iso.clone(),
            active: market.active.unwrap_or(true),
        })
    }
}
