//! Market discovery and filtering.
//!
//! Uses the official Polymarket SDK (polymarket-client-sdk) Gamma client
//! for market discovery. The SDK handles all Gamma API deserialization
//! quirks (stringified JSON arrays, mixed string/float fields, etc.)

use crate::config::FilterConfig;
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::request::MarketsRequest;
use polymarket_client_sdk::gamma::types::response::Market as SdkMarket;
use tracing::{debug, info, warn};

/// A market discovered from the Gamma API, with fields converted to string
/// form for downstream consumption by the arb scanner, order book store, etc.
#[derive(Debug, Clone)]
pub struct DiscoveredMarket {
    pub condition_id: String,
    pub question: String,
    pub outcomes: Vec<String>,
    pub clob_token_ids: Vec<String>,
    pub volume_24hr: f64,
    pub liquidity: f64,
    pub category: String,
    pub active: bool,
    pub accepting_orders: bool,
}

impl DiscoveredMarket {
    /// Convert an SDK Market into our lightweight bridge type.
    /// Returns None if the market is missing required fields.
    pub fn from_sdk(m: &SdkMarket) -> Option<Self> {
        let condition_id = m.condition_id.as_ref()?.to_string();
        let clob_token_ids: Vec<String> = m
            .clob_token_ids
            .as_ref()?
            .iter()
            .map(|u| u.to_string())
            .collect();

        if clob_token_ids.is_empty() {
            return None;
        }

        Some(Self {
            condition_id,
            question: m.question.clone().unwrap_or_default(),
            outcomes: m.outcomes.clone().unwrap_or_default(),
            clob_token_ids,
            volume_24hr: m
                .volume_24hr
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            liquidity: m
                .liquidity
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            category: m.category.clone().unwrap_or_default(),
            active: m.active.unwrap_or(false),
            accepting_orders: m.accepting_orders.unwrap_or(false),
        })
    }
}

/// Fetches and filters markets from the Gamma API using the official SDK.
pub struct MarketDiscovery {
    client: gamma::Client,
    filters: FilterConfig,
}

impl MarketDiscovery {
    pub fn new(gamma_url: String, filters: FilterConfig) -> Self {
        let client = gamma::Client::new(&gamma_url)
            .unwrap_or_else(|_| gamma::Client::default());
        Self { client, filters }
    }

    /// Fetch active, open markets from Gamma API with pagination.
    pub async fn fetch_active_markets(&self) -> anyhow::Result<Vec<DiscoveredMarket>> {
        let mut all_markets = Vec::new();
        let mut offset = 0i32;
        let limit = 100i32;

        loop {
            let request = MarketsRequest::builder()
                .closed(false)
                .limit(limit)
                .offset(offset)
                .build();

            debug!(offset = offset, limit = limit, "fetching markets page");

            let sdk_markets = self.client.markets(&request).await?;

            if sdk_markets.is_empty() {
                break;
            }

            let count = sdk_markets.len() as i32;

            // Convert SDK Markets to our bridge type, filtering out any
            // that are missing required fields
            let discovered: Vec<DiscoveredMarket> = sdk_markets
                .iter()
                .filter_map(DiscoveredMarket::from_sdk)
                .collect();

            all_markets.extend(discovered);
            offset += count;

            // Safety limit
            if offset > 5000 {
                warn!("hit safety limit on market pagination at {offset} markets");
                break;
            }
        }

        info!(total = all_markets.len(), "fetched markets from Gamma API (SDK)");
        Ok(all_markets)
    }

    /// Filter markets based on configured criteria.
    pub fn filter_markets(&self, markets: &[DiscoveredMarket]) -> Vec<DiscoveredMarket> {
        let filtered: Vec<DiscoveredMarket> = markets
            .iter()
            .filter(|m| {
                // Must be active and accepting orders
                if !m.active || !m.accepting_orders {
                    return false;
                }

                // Must have CLOB token IDs
                if m.clob_token_ids.is_empty() {
                    return false;
                }

                // Volume filter
                if m.volume_24hr < self.filters.min_volume_usd {
                    return false;
                }

                // Liquidity as proxy for depth
                if m.liquidity < self.filters.min_depth_usd {
                    return false;
                }

                // Category filter
                if !self.filters.categories.is_empty()
                    && !self
                        .filters
                        .categories
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(&m.category))
                {
                    return false;
                }

                // Exclude categories
                if self
                    .filters
                    .exclude_categories
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&m.category))
                {
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
    pub async fn discover_asset_ids(
        &self,
    ) -> anyhow::Result<(Vec<String>, Vec<DiscoveredMarket>)> {
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

/// A tracked market with its outcome token IDs.
/// Used by the sniper, crossbook scanner, and main orchestrator.
#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub condition_id: String,
    pub question: String,
    /// Ordered list of (outcome_label, token_id) pairs.
    /// For binary: [("Yes", token_yes), ("No", token_no)]
    pub outcomes: Vec<(String, String)>,
}

impl TrackedMarket {
    pub fn from_discovered(m: &DiscoveredMarket) -> Option<Self> {
        if m.clob_token_ids.len() < 2 || m.outcomes.len() != m.clob_token_ids.len() {
            return None;
        }
        let outcomes = m
            .outcomes
            .iter()
            .zip(m.clob_token_ids.iter())
            .map(|(label, tid)| (label.clone(), tid.clone()))
            .collect();
        Some(Self {
            condition_id: m.condition_id.clone(),
            question: m.question.clone(),
            outcomes,
        })
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

pub fn classify_edge_profile(market: &DiscoveredMarket) -> EdgeProfile {
    let vol = market.volume_24hr;
    let liq = market.liquidity;

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
