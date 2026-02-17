//! The Odds API fetcher.
//!
//! Fetches odds from https://api.the-odds-api.com across multiple sports
//! and bookmakers. Returns structured data with decimal odds per outcome
//! per bookmaker per match.
//!
//! Rate-limit aware: reads `x-requests-remaining` and `x-requests-used`
//! headers from every API response and tracks the remaining quota.
//! Callers should check `requests_remaining()` before calling `fetch_all`.

use serde::Deserialize;
use tracing::{debug, info, warn};

/// A match from The Odds API with all bookmaker odds.
#[derive(Debug, Clone, Deserialize)]
pub struct BookmakerMatch {
    pub id: Option<String>,
    pub home_team: String,
    pub away_team: String,
    #[serde(default)]
    pub sport_key: String,
    #[serde(default)]
    pub commence_time: String,
    #[serde(default)]
    pub bookmakers: Vec<Bookmaker>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Bookmaker {
    #[serde(default)]
    pub key: String,
    pub title: String,
    #[serde(default)]
    pub markets: Vec<OddsMarket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OddsMarket {
    pub key: String,
    #[serde(default)]
    pub outcomes: Vec<Outcome>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Outcome {
    pub name: String,
    pub price: f32,
    pub point: Option<f32>,
}

/// Summary of API quota after a fetch.
#[derive(Debug, Clone)]
pub struct QuotaInfo {
    /// Requests remaining this month (from `x-requests-remaining` header).
    pub remaining: Option<u32>,
    /// Requests used this month (from `x-requests-used` header).
    pub used: Option<u32>,
}

pub struct OddsFetcher {
    api_key: String,
    base_url: String,
    regions: String,
    markets: String,
    client: reqwest::Client,
    /// Last known remaining quota (updated after every successful request).
    last_remaining: Option<u32>,
    /// Last known used count.
    last_used: Option<u32>,
    /// Total requests made by this fetcher instance.
    requests_made: u32,
}

impl OddsFetcher {
    pub fn new(api_key: String, base_url: String, regions: String, markets: String) -> Self {
        Self {
            api_key,
            base_url,
            regions,
            markets,
            client: reqwest::Client::new(),
            last_remaining: None,
            last_used: None,
            requests_made: 0,
        }
    }

    /// Returns the last known remaining API quota, or None if we haven't
    /// fetched yet.
    pub fn requests_remaining(&self) -> Option<u32> {
        self.last_remaining
    }

    /// Returns the last known used count, or None if we haven't fetched yet.
    pub fn requests_used(&self) -> Option<u32> {
        self.last_used
    }

    /// Total requests made by this instance since startup.
    pub fn requests_made(&self) -> u32 {
        self.requests_made
    }

    /// Fetch odds for a single sport key.
    pub async fn fetch_sport(&mut self, sport_key: &str) -> Result<Vec<BookmakerMatch>, String> {
        let url = format!("{}/{}/odds", self.base_url, sport_key);

        let resp = self
            .client
            .get(&url)
            .query(&[
                ("apiKey", &self.api_key),
                ("regions", &self.regions),
                ("markets", &self.markets),
                ("dateFormat", &"iso".to_string()),
                ("oddsFormat", &"decimal".to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("request failed for {}: {}", sport_key, e))?;

        self.requests_made += 1;

        // Parse rate-limit headers before consuming the response body
        if let Some(remaining) = resp
            .headers()
            .get("x-requests-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
        {
            self.last_remaining = Some(remaining);
        }
        if let Some(used) = resp
            .headers()
            .get("x-requests-used")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
        {
            self.last_used = Some(used);
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "API error for {}: {} - {}",
                sport_key, status, body
            ));
        }

        let mut matches: Vec<BookmakerMatch> = resp
            .json()
            .await
            .map_err(|e| format!("parse error for {}: {}", sport_key, e))?;

        // Tag each match with its sport key
        for m in &mut matches {
            m.sport_key = sport_key.to_string();
        }

        debug!(
            sport = sport_key,
            matches = matches.len(),
            remaining = ?self.last_remaining,
            "fetched odds"
        );

        Ok(matches)
    }

    /// Fetch odds for all configured sports sequentially (to track quota
    /// after each request and abort early if we're running low).
    ///
    /// The `min_remaining` parameter is the floor: we stop fetching more
    /// sports once the remaining quota drops to or below this value.
    pub async fn fetch_all(
        &mut self,
        sports: &[String],
        min_remaining: u32,
    ) -> Result<(Vec<BookmakerMatch>, QuotaInfo), String> {
        let mut all_matches = Vec::new();

        // Fetch sequentially so we can check remaining after each request
        for sport in sports {
            // Check remaining quota before making the next request
            if let Some(remaining) = self.last_remaining {
                if remaining <= min_remaining {
                    warn!(
                        remaining = remaining,
                        min = min_remaining,
                        skipped_sports = sports.len() - all_matches.len(),
                        "quota floor reached — skipping remaining sports"
                    );
                    break;
                }
            }

            match self.fetch_sport(sport).await {
                Ok(matches) => all_matches.extend(matches),
                Err(e) => warn!("odds fetch error: {}", e),
            }
        }

        let quota = QuotaInfo {
            remaining: self.last_remaining,
            used: self.last_used,
        };

        info!(
            sports_fetched = sports.len(),
            matches = all_matches.len(),
            quota_remaining = ?quota.remaining,
            quota_used = ?quota.used,
            session_requests = self.requests_made,
            "odds fetch complete"
        );

        Ok((all_matches, quota))
    }
}
