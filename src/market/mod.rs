//! Market discovery and filtering.
//!
//! Uses the Polymarket Gamma API to discover active markets,
//! then filters based on volume, depth, and category to find
//! markets worth subscribing to.

use crate::config::FilterConfig;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{debug, info, warn};

const GAMMA_MARKETS_ENDPOINT: &str = "/markets";
const GAMMA_EVENTS_ENDPOINT: &str = "/events";

/// A market from the Gamma API.
#[derive(Debug, Clone, Deserialize)]
pub struct GammaMarket {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub outcomes: Vec<String>,
    #[serde(default, rename = "outcomePrices")]
    pub outcome_prices: Vec<String>,
    #[serde(default, rename = "clobTokenIds")]
    pub clob_token_ids: Vec<String>,
    #[serde(default)]
    pub volume: String,
    #[serde(default, rename = "volume24hr")]
    pub volume_24hr: String,
    #[serde(default)]
    pub liquidity: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, rename = "acceptingOrders")]
    pub accepting_orders: bool,
    #[serde(default, rename = "enableOrderBook")]
    pub enable_order_book: bool,
    #[serde(default)]
    pub category: String,
    #[serde(default, rename = "endDateIso")]
    pub end_date: String,
    #[serde(default)]
    pub spread: String,
    #[serde(default, rename = "bestBid")]
    pub best_bid: String,
    #[serde(default, rename = "bestAsk")]
    pub best_ask: String,
}

impl GammaMarket {
    pub fn volume_24hr_usd(&self) -> f64 {
        self.volume_24hr.parse().unwrap_or(0.0)
    }

    pub fn liquidity_usd(&self) -> f64 {
        self.liquidity.parse().unwrap_or(0.0)
    }

    pub fn spread_value(&self) -> Option<Decimal> {
        Decimal::from_str(&self.spread).ok()
    }

    /// Get all CLOB token IDs (asset IDs) for this market.
    /// Binary markets have 2 tokens (YES/NO).
    pub fn asset_ids(&self) -> &[String] {
        &self.clob_token_ids
    }
}

/// Fetches and filters markets from the Gamma API.
pub struct MarketDiscovery {
    client: Client,
    gamma_url: String,
    filters: FilterConfig,
}

impl MarketDiscovery {
    pub fn new(gamma_url: String, filters: FilterConfig) -> Self {
        Self {
            client: Client::new(),
            gamma_url,
            filters,
        }
    }

    /// Fetch active, open markets from Gamma API with pagination.
    pub async fn fetch_active_markets(&self) -> anyhow::Result<Vec<GammaMarket>> {
        let mut all_markets = Vec::new();
        let mut offset = 0;
        let limit = 100;

        loop {
            let url = format!(
                "{}{}?active=true&closed=false&limit={}&offset={}",
                self.gamma_url, GAMMA_MARKETS_ENDPOINT, limit, offset
            );

            debug!(url = %url, "fetching markets page");

            let resp = self.client.get(&url).send().await?;
            let markets: Vec<GammaMarket> = resp.json().await?;

            if markets.is_empty() {
                break;
            }

            let count = markets.len();
            all_markets.extend(markets);
            offset += count;

            // Safety limit
            if offset > 5000 {
                warn!("hit safety limit on market pagination at {offset} markets");
                break;
            }
        }

        info!(total = all_markets.len(), "fetched markets from Gamma API");
        Ok(all_markets)
    }

    /// Filter markets based on configured criteria.
    pub fn filter_markets(&self, markets: &[GammaMarket]) -> Vec<GammaMarket> {
        let filtered: Vec<GammaMarket> = markets
            .iter()
            .filter(|m| {
                // Must be active and accepting orders
                if !m.active || m.closed || !m.accepting_orders {
                    return false;
                }

                // Must have CLOB token IDs
                if m.clob_token_ids.is_empty() {
                    return false;
                }

                // Volume filter
                if m.volume_24hr_usd() < self.filters.min_volume_usd {
                    return false;
                }

                // Liquidity as proxy for depth
                if m.liquidity_usd() < self.filters.min_depth_usd {
                    return false;
                }

                // Category filter
                if !self.filters.categories.is_empty()
                    && !self.filters.categories.iter().any(|c| {
                        c.eq_ignore_ascii_case(&m.category)
                    })
                {
                    return false;
                }

                // Exclude categories
                if self.filters.exclude_categories.iter().any(|c| {
                    c.eq_ignore_ascii_case(&m.category)
                }) {
                    return false;
                }

                true
            })
            .cloned()
            .collect();

        info!(
            total = markets.len(),
            filtered = filtered.len(),
            min_volume = self.filters.min_volume_usd,
            min_depth = self.filters.min_depth_usd,
            "filtered markets"
        );

        filtered
    }

    /// Discover markets, filter, and return all asset IDs worth subscribing to.
    pub async fn discover_asset_ids(&self) -> anyhow::Result<(Vec<String>, Vec<GammaMarket>)> {
        let all = self.fetch_active_markets().await?;
        let filtered = self.filter_markets(&all);

        let asset_ids: Vec<String> = filtered
            .iter()
            .flat_map(|m| m.clob_token_ids.clone())
            .collect();

        info!(
            markets = filtered.len(),
            asset_ids = asset_ids.len(),
            "discovered subscribable assets"
        );

        Ok((asset_ids, filtered))
    }
}

/// Categorize markets by their edge potential based on depth/volume characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeProfile {
    /// Deep liquidity, tight spreads - latency matters, hard to extract edge
    DeepCompetitive,
    /// Medium liquidity - possible edge with speed + signal
    MediumOpportunity,
    /// Thin books - easy to move price, but fills are small
    ThinFragile,
    /// Newly listed or event-driven - potential for mispricing
    EventDriven,
}

impl std::fmt::Display for EdgeProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeProfile::DeepCompetitive => write!(f, "DEEP/COMPETITIVE"),
            EdgeProfile::MediumOpportunity => write!(f, "MEDIUM/OPPORTUNITY"),
            EdgeProfile::ThinFragile => write!(f, "THIN/FRAGILE"),
            EdgeProfile::EventDriven => write!(f, "EVENT/DRIVEN"),
        }
    }
}

pub fn classify_edge_profile(market: &GammaMarket) -> EdgeProfile {
    let vol = market.volume_24hr_usd();
    let liq = market.liquidity_usd();

    // High-volume, high-liquidity markets are dominated by MMs
    if vol > 500_000.0 && liq > 100_000.0 {
        return EdgeProfile::DeepCompetitive;
    }

    // Event markets near expiry with decent volume
    if vol > 50_000.0 && liq < 20_000.0 {
        return EdgeProfile::EventDriven;
    }

    // Moderate volume and liquidity - best opportunity zone
    if vol > 10_000.0 && liq > 5_000.0 {
        return EdgeProfile::MediumOpportunity;
    }

    EdgeProfile::ThinFragile
}
