//! Cross-bookmaker arbitrage scanner.
//!
//! Bridges Polymarket prediction markets with traditional bookmaker odds
//! from The Odds API. The core insight: Polymarket prices ARE implied
//! probabilities (price 0.40 = 40%), and bookmaker decimal odds convert
//! via `implied_prob = 1/odds` (odds 2.50 = 40%). Same maths.
//!
//! The module:
//! 1. Periodically fetches odds from The Odds API (25+ bookmakers)
//! 2. Fuzzy-matches Polymarket questions/outcomes to bookmaker events
//! 3. Finds the best price per outcome across ALL providers (bookies + Polymarket)
//! 4. Flags cross-provider arbs where sum of best implied probs < 100%

pub mod fetcher;
pub mod matcher;

use crate::arb::TrackedMarket;
use crate::config::CrossbookConfig;
use crate::orderbook::OrderBookStore;
use chrono::{DateTime, Utc};
use fetcher::{BookmakerMatch, OddsFetcher};
use matcher::MatchLink;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

/// A single leg in a cross-provider opportunity: which provider has the best
/// price for this outcome.
#[derive(Debug, Clone, Serialize)]
pub struct CrossbookLeg {
    pub outcome: String,
    /// Provider offering the best price ("Polymarket", "DraftKings", etc.)
    pub provider: String,
    /// Implied probability (0.0-1.0). For bookies: 1/odds. For Polymarket: ask price.
    pub implied_prob: f64,
    /// Original decimal odds (for bookies) or Polymarket ask price.
    pub raw_price: f64,
}

/// A cross-provider arbitrage opportunity.
#[derive(Debug, Clone, Serialize)]
pub struct CrossbookOpportunity {
    /// The event description (e.g., "Arsenal vs Chelsea").
    pub event: String,
    /// Which Polymarket market matched, if any.
    pub poly_question: Option<String>,
    /// The match link that connected them.
    pub match_confidence: f64,
    /// Best price per outcome across all providers.
    pub legs: Vec<CrossbookLeg>,
    /// Sum of implied probabilities (< 1.0 = arb).
    pub implied_prob_sum: f64,
    /// Arbitrage return percentage (positive = profit).
    pub arb_return_pct: f64,
    pub detected_at: String,
}

/// Events emitted by the crossbook scanner.
#[derive(Debug, Clone)]
pub enum CrossbookEvent {
    OpportunityDetected(CrossbookOpportunity),
    /// Best odds display even when no arb (for comparison).
    BestOddsUpdate {
        event: String,
        legs: Vec<CrossbookLeg>,
        implied_sum: f64,
    },
    FetchComplete {
        matches_fetched: usize,
        poly_links: usize,
    },
    ScanComplete {
        events_scanned: usize,
        opportunities: usize,
        best_edge_pct: Option<f64>,
    },
}

/// The cross-bookmaker scanner. Combines The Odds API data with Polymarket
/// order books to find cross-provider arbitrage.
pub struct CrossbookScanner {
    config: CrossbookConfig,
    store: OrderBookStore,
    fetcher: OddsFetcher,
    /// Polymarket markets we can potentially match against.
    poly_markets: Vec<TrackedMarket>,
    /// Last fetched bookmaker data.
    bookie_matches: Vec<BookmakerMatch>,
    /// Matched links between bookmaker events and Polymarket markets.
    links: Vec<MatchLink>,
    /// Detected opportunities (ring buffer for dashboard).
    pub opportunities: VecDeque<CrossbookOpportunity>,
    /// Best odds per event (for dashboard even without arbs).
    pub best_odds: Vec<CrossbookOpportunity>,
    event_tx: mpsc::UnboundedSender<CrossbookEvent>,
    max_results: usize,
}

impl CrossbookScanner {
    pub fn new(
        config: CrossbookConfig,
        store: OrderBookStore,
        poly_markets: Vec<TrackedMarket>,
        event_tx: mpsc::UnboundedSender<CrossbookEvent>,
    ) -> Self {
        let fetcher = OddsFetcher::new(
            config.odds_api_key.clone(),
            config.odds_api_url.clone(),
            config.regions.clone(),
            config.markets.clone(),
        );
        let max = config.max_results;
        Self {
            config,
            store,
            fetcher,
            poly_markets,
            bookie_matches: Vec::new(),
            links: Vec::new(),
            opportunities: VecDeque::with_capacity(max),
            best_odds: Vec::new(),
            event_tx,
            max_results: max,
        }
    }

    /// Check whether we should fetch based on remaining API quota.
    /// Returns false if we know we're at or below the safety floor.
    pub fn should_fetch(&self) -> bool {
        if let Some(remaining) = self.fetcher.requests_remaining() {
            if remaining <= self.config.min_remaining {
                warn!(
                    remaining = remaining,
                    floor = self.config.min_remaining,
                    "skipping odds fetch — quota floor reached"
                );
                return false;
            }
        }
        true
    }

    /// Returns the current API quota info for logging/dashboard.
    pub fn quota_remaining(&self) -> Option<u32> {
        self.fetcher.requests_remaining()
    }

    /// Returns the total requests made this session.
    pub fn requests_made(&self) -> u32 {
        self.fetcher.requests_made()
    }

    /// Fetch fresh odds from The Odds API. Respects the monthly budget
    /// by checking `x-requests-remaining` and stopping at the floor.
    pub async fn fetch_odds(&mut self) {
        if !self.should_fetch() {
            return;
        }

        match self
            .fetcher
            .fetch_all(&self.config.sports_keys, self.config.min_remaining)
            .await
        {
            Ok((matches, quota)) => {
                info!(
                    matches = matches.len(),
                    sports = self.config.sports_keys.len(),
                    quota_remaining = ?quota.remaining,
                    quota_used = ?quota.used,
                    session_requests = self.fetcher.requests_made(),
                    "fetched bookmaker odds"
                );
                self.bookie_matches = matches;
                // Re-run matching after fresh fetch
                self.links = matcher::match_markets(&self.bookie_matches, &self.poly_markets);
                info!(
                    links = self.links.len(),
                    "matched bookmaker events to Polymarket"
                );
                let _ = self.event_tx.send(CrossbookEvent::FetchComplete {
                    matches_fetched: self.bookie_matches.len(),
                    poly_links: self.links.len(),
                });
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch odds from The Odds API");
            }
        }
    }

    /// Scan for cross-provider arbs. Call this frequently (every few seconds).
    pub fn scan(&mut self) {
        let mut opportunities_found = 0usize;
        let mut best_edge: Option<f64> = None;
        let mut all_best_odds = Vec::new();
        // Collect arb opportunities separately to avoid borrow conflicts
        let mut pending_arbs: Vec<CrossbookOpportunity> = Vec::new();

        // === Part 1: Scan bookmaker events that matched to Polymarket ===
        for link in &self.links {
            let bookie_match = &self.bookie_matches[link.bookie_idx];
            let poly_market = &self.poly_markets[link.poly_idx];

            // For each outcome, find the best price across all bookmakers AND Polymarket
            let mut best_per_outcome: HashMap<String, CrossbookLeg> = HashMap::new();

            // Bookmaker odds: pick best across all bookies per outcome
            for bookie in &bookie_match.bookmakers {
                for market in &bookie.markets {
                    if market.key != "h2h" {
                        continue;
                    }
                    for outcome in &market.outcomes {
                        let implied = 1.0 / outcome.price as f64;
                        let entry = best_per_outcome
                            .entry(outcome.name.clone())
                            .or_insert_with(|| CrossbookLeg {
                                outcome: outcome.name.clone(),
                                provider: bookie.title.clone(),
                                implied_prob: implied,
                                raw_price: outcome.price as f64,
                            });
                        // Lower implied prob = better odds for the bettor
                        if implied < entry.implied_prob {
                            entry.provider = bookie.title.clone();
                            entry.implied_prob = implied;
                            entry.raw_price = outcome.price as f64;
                        }
                    }
                }
            }

            // Polymarket prices: check if any outcome is cheaper on Polymarket
            for (poly_outcome, mapped_bookie_outcome) in &link.outcome_map {
                if let Some((_, token_id)) = poly_market
                    .outcomes
                    .iter()
                    .find(|(label, _)| label == poly_outcome)
                {
                    if let Some(book) = self.store.get_book(token_id) {
                        if let Some((ask_price, _)) = book.asks.best(false) {
                            let poly_implied =
                                ask_price.to_string().parse::<f64>().unwrap_or(1.0);
                            let entry = best_per_outcome
                                .entry(mapped_bookie_outcome.clone())
                                .or_insert_with(|| CrossbookLeg {
                                    outcome: mapped_bookie_outcome.clone(),
                                    provider: "Polymarket".to_string(),
                                    implied_prob: poly_implied,
                                    raw_price: poly_implied,
                                });
                            if poly_implied < entry.implied_prob {
                                entry.provider = "Polymarket".to_string();
                                entry.implied_prob = poly_implied;
                                entry.raw_price = poly_implied;
                            }
                        }
                    }
                }
            }

            if best_per_outcome.is_empty() {
                continue;
            }

            let legs: Vec<CrossbookLeg> = best_per_outcome.into_values().collect();
            let implied_sum: f64 = legs.iter().map(|l| l.implied_prob).sum();
            let arb_return = (1.0 - implied_sum) * 100.0;

            let opp = CrossbookOpportunity {
                event: format!("{} vs {}", bookie_match.home_team, bookie_match.away_team),
                poly_question: Some(poly_market.question.clone()),
                match_confidence: link.confidence,
                legs,
                implied_prob_sum: implied_sum,
                arb_return_pct: arb_return,
                detected_at: Utc::now().to_rfc3339(),
            };

            all_best_odds.push(opp.clone());

            if arb_return >= self.config.min_edge_pct {
                info!(
                    event = %opp.event,
                    poly = ?opp.poly_question.as_deref().map(|s| &s[..s.len().min(40)]),
                    arb_pct = format!("{:.2}%", arb_return),
                    implied_sum = format!("{:.4}", implied_sum),
                    legs = opp.legs.len(),
                    "CROSS-PROVIDER ARB"
                );
                pending_arbs.push(opp);
                opportunities_found += 1;
            }

            if best_edge.is_none() || arb_return > best_edge.unwrap() {
                best_edge = Some(arb_return);
            }
        }

        // === Part 2: Pure bookie arbs (no Polymarket needed) ===
        for bm in &self.bookie_matches {
            let mut best_per_outcome: HashMap<String, CrossbookLeg> = HashMap::new();

            for bookie in &bm.bookmakers {
                for market in &bookie.markets {
                    if market.key != "h2h" {
                        continue;
                    }
                    for outcome in &market.outcomes {
                        let implied = 1.0 / outcome.price as f64;
                        let entry = best_per_outcome
                            .entry(outcome.name.clone())
                            .or_insert_with(|| CrossbookLeg {
                                outcome: outcome.name.clone(),
                                provider: bookie.title.clone(),
                                implied_prob: implied,
                                raw_price: outcome.price as f64,
                            });
                        if implied < entry.implied_prob {
                            entry.provider = bookie.title.clone();
                            entry.implied_prob = implied;
                            entry.raw_price = outcome.price as f64;
                        }
                    }
                }
            }

            if best_per_outcome.is_empty() {
                continue;
            }

            let legs: Vec<CrossbookLeg> = best_per_outcome.into_values().collect();
            let implied_sum: f64 = legs.iter().map(|l| l.implied_prob).sum();
            let arb_return = (1.0 - implied_sum) * 100.0;

            let opp = CrossbookOpportunity {
                event: format!("{} vs {}", bm.home_team, bm.away_team),
                poly_question: None,
                match_confidence: 0.0,
                legs,
                implied_prob_sum: implied_sum,
                arb_return_pct: arb_return,
                detected_at: Utc::now().to_rfc3339(),
            };

            all_best_odds.push(opp.clone());

            if arb_return >= self.config.min_edge_pct {
                info!(
                    event = %opp.event,
                    arb_pct = format!("{:.2}%", arb_return),
                    implied_sum = format!("{:.4}", implied_sum),
                    "BOOKIE-ONLY ARB"
                );
                pending_arbs.push(opp);
                opportunities_found += 1;
            }

            if best_edge.is_none() || arb_return > best_edge.unwrap() {
                best_edge = Some(arb_return);
            }
        }

        // Now push collected arbs (borrows on self.links/bookie_matches are dropped)
        for opp in pending_arbs {
            let _ = self
                .event_tx
                .send(CrossbookEvent::OpportunityDetected(opp.clone()));
            self.push_opportunity(opp);
        }

        // Sort best odds by arb_return descending and keep top N
        all_best_odds.sort_by(|a, b| {
            b.arb_return_pct
                .partial_cmp(&a.arb_return_pct)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_best_odds.truncate(self.max_results);
        self.best_odds = all_best_odds;

        let _ = self.event_tx.send(CrossbookEvent::ScanComplete {
            events_scanned: self.bookie_matches.len() + self.links.len(),
            opportunities: opportunities_found,
            best_edge_pct: best_edge,
        });
    }

    fn push_opportunity(&mut self, opp: CrossbookOpportunity) {
        if self.opportunities.len() >= self.max_results {
            self.opportunities.pop_front();
        }
        self.opportunities.push_back(opp);
    }

    pub fn recent_opportunities(&self) -> Vec<&CrossbookOpportunity> {
        self.opportunities.iter().collect()
    }
}
