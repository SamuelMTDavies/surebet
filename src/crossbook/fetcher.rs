//! The Odds API fetcher.
//!
//! Fetches odds from https://api.the-odds-api.com across multiple sports
//! and bookmakers. Returns structured data with decimal odds per outcome
//! per bookmaker per match.

use serde::Deserialize;
use tracing::{debug, warn};

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

pub struct OddsFetcher {
    api_key: String,
    base_url: String,
    regions: String,
    markets: String,
    client: reqwest::Client,
}

impl OddsFetcher {
    pub fn new(api_key: String, base_url: String, regions: String, markets: String) -> Self {
        Self {
            api_key,
            base_url,
            regions,
            markets,
            client: reqwest::Client::new(),
        }
    }

    /// Fetch odds for a single sport key.
    pub async fn fetch_sport(&self, sport_key: &str) -> Result<Vec<BookmakerMatch>, String> {
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
            "fetched odds"
        );

        Ok(matches)
    }

    /// Fetch odds for all configured sports concurrently.
    pub async fn fetch_all(&self, sports: &[String]) -> Result<Vec<BookmakerMatch>, String> {
        let futures: Vec<_> = sports.iter().map(|s| self.fetch_sport(s)).collect();

        let results = futures::future::join_all(futures).await;

        let mut all_matches = Vec::new();
        for result in results {
            match result {
                Ok(matches) => all_matches.extend(matches),
                Err(e) => warn!("odds fetch error: {}", e),
            }
        }

        Ok(all_matches)
    }
}
