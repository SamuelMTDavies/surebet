//! External data source pollers for resolution sniping.
//!
//! Each source polls an external API and emits ResolutionSignals when
//! an outcome becomes knowable. Sources run as independent tokio tasks
//! and push signals into a shared mpsc channel.

use super::{OutcomeResult, ResolutionSignal, SourceType};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Unified signal sender — all sources push to this channel.
pub type SignalSender = mpsc::UnboundedSender<ResolutionSignal>;

// ---------------------------------------------------------------------------
// Source 1: Crypto Price Thresholds
// ---------------------------------------------------------------------------

/// Watches crypto prices from the existing exchange feeds (Binance/Coinbase)
/// and fires resolution signals when a threshold is crossed.
#[derive(Debug, Clone)]
pub struct CryptoThresholdWatcher {
    /// Maps condition_id → (symbol, threshold, direction, outcomes)
    watches: Vec<CryptoWatch>,
    /// Track which watches have already fired.
    fired: HashMap<String, Instant>,
}

#[derive(Debug, Clone)]
pub struct CryptoWatch {
    pub condition_id: String,
    pub symbol: String,
    pub threshold: Decimal,
    pub direction: ThresholdDirection,
    /// Outcome labels for YES (above/reached) and NO (below/not reached).
    pub yes_label: String,
    pub no_label: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThresholdDirection {
    Above, // Market asks "will price be above X?"
    Below, // Market asks "will price be below X?"
}

impl CryptoThresholdWatcher {
    pub fn new() -> Self {
        Self {
            watches: Vec::new(),
            fired: HashMap::new(),
        }
    }

    pub fn add_watch(&mut self, watch: CryptoWatch) {
        info!(
            cid = %watch.condition_id,
            symbol = %watch.symbol,
            threshold = %watch.threshold,
            direction = ?watch.direction,
            "added crypto threshold watch"
        );
        self.watches.push(watch);
    }

    /// Called with each new price observation. Returns signals for any
    /// thresholds that were just crossed.
    pub fn check_price(&mut self, symbol: &str, price: Decimal) -> Vec<ResolutionSignal> {
        let mut signals = Vec::new();

        for watch in &self.watches {
            if watch.symbol != symbol {
                continue;
            }
            if self.fired.contains_key(&watch.condition_id) {
                continue;
            }

            let crossed = match watch.direction {
                ThresholdDirection::Above => price >= watch.threshold,
                ThresholdDirection::Below => price <= watch.threshold,
            };

            // We need to determine _which_ outcome won.
            // For "above" markets: if price >= threshold → YES wins
            // For "below" markets: if price <= threshold → YES wins
            // We also fire when the opposite is definitively true,
            // but only at the resolution time (which we don't know here).
            // So we only fire when the threshold is crossed.
            if !crossed {
                continue;
            }

            let mut outcomes = HashMap::new();
            outcomes.insert(watch.yes_label.clone(), OutcomeResult::Won);
            outcomes.insert(watch.no_label.clone(), OutcomeResult::Lost);

            signals.push(ResolutionSignal {
                condition_id: watch.condition_id.clone(),
                source: format!("crypto:{}", watch.symbol),
                outcomes,
                confidence: 0.99, // High but not 1.0 — price could revert before resolution
                evidence: format!(
                    "{}={} {} threshold={}",
                    watch.symbol,
                    price,
                    match watch.direction {
                        ThresholdDirection::Above => ">=",
                        ThresholdDirection::Below => "<=",
                    },
                    watch.threshold
                ),
                observed_at: Instant::now(),
            });
        }

        // Mark fired
        for sig in &signals {
            self.fired
                .insert(sig.condition_id.clone(), Instant::now());
        }

        signals
    }

    pub fn watch_count(&self) -> usize {
        self.watches.len()
    }
}

// ---------------------------------------------------------------------------
// Source 2: HTTP API Poller (Billboard, RT, USGS, etc.)
// ---------------------------------------------------------------------------

/// Generic HTTP API poller that can be configured for various data sources.
/// Runs as a tokio task, periodically fetching a URL and parsing the response.
pub struct HttpSourcePoller {
    pub source_type: SourceType,
    pub url: String,
    pub poll_interval: Duration,
    pub condition_id: String,
    /// Parse function: takes response body, returns resolved outcomes.
    /// Box<dyn> so each source can provide its own parser.
    parser: Box<dyn Fn(&str) -> Option<HashMap<String, OutcomeResult>> + Send + Sync>,
}

impl HttpSourcePoller {
    pub fn new(
        source_type: SourceType,
        url: String,
        poll_interval: Duration,
        condition_id: String,
        parser: Box<dyn Fn(&str) -> Option<HashMap<String, OutcomeResult>> + Send + Sync>,
    ) -> Self {
        Self {
            source_type,
            url,
            poll_interval,
            condition_id,
            parser,
        }
    }

    /// Spawn a polling task. Runs until the sender is dropped.
    pub fn spawn(self, signal_tx: SignalSender) {
        let source_name = self.source_type.to_string();
        let url = self.url.clone();
        let cid = self.condition_id.clone();
        let interval = self.poll_interval;

        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("http client");

            let mut ticker = tokio::time::interval(interval);
            let mut fired = false;

            loop {
                ticker.tick().await;
                if fired {
                    break;
                }

                match client.get(&url).send().await {
                    Ok(resp) => {
                        if let Ok(body) = resp.text().await {
                            if let Some(outcomes) = (self.parser)(&body) {
                                let signal = ResolutionSignal {
                                    condition_id: cid.clone(),
                                    source: source_name.clone(),
                                    outcomes,
                                    confidence: 0.95,
                                    evidence: format!("HTTP {} → resolved", url),
                                    observed_at: Instant::now(),
                                };
                                if signal_tx.send(signal).is_err() {
                                    break;
                                }
                                fired = true;
                                info!(
                                    source = %source_name,
                                    cid = %cid,
                                    "resolution signal fired from HTTP source"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            source = %source_name,
                            url = %url,
                            error = %e,
                            "HTTP poll failed"
                        );
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Source 3: USGS Earthquake API
// ---------------------------------------------------------------------------

/// Parse USGS earthquake GeoJSON feed for magnitude thresholds.
/// API: https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
pub fn parse_usgs_earthquake(
    body: &str,
    min_magnitude: f64,
    region: Option<&str>,
) -> Option<HashMap<String, OutcomeResult>> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    let features = json.get("features")?.as_array()?;

    for feature in features {
        let props = feature.get("properties")?;
        let mag = props.get("mag")?.as_f64()?;
        let place = props.get("place")?.as_str().unwrap_or("");

        let region_match = region
            .map(|r| place.to_lowercase().contains(&r.to_lowercase()))
            .unwrap_or(true);

        if mag >= min_magnitude && region_match {
            let mut outcomes = HashMap::new();
            outcomes.insert("Yes".to_string(), OutcomeResult::Won);
            outcomes.insert("No".to_string(), OutcomeResult::Lost);
            return Some(outcomes);
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Bracket market helper
// ---------------------------------------------------------------------------

/// For bracket markets (e.g., RT score brackets: 70+, 75+, 80+, 85+, 90+),
/// given a numeric value, determine which brackets won and which lost.
///
/// Example: RT score = 82% → 70+ YES, 75+ YES, 80+ YES, 85+ NO, 90+ NO
pub fn resolve_brackets(
    value: f64,
    brackets: &[(String, f64)], // (outcome_label, threshold)
) -> HashMap<String, OutcomeResult> {
    let mut outcomes = HashMap::new();
    for (label, threshold) in brackets {
        if value >= *threshold {
            outcomes.insert(label.clone(), OutcomeResult::Won);
        } else {
            outcomes.insert(label.clone(), OutcomeResult::Lost);
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bracket_resolution() {
        let brackets = vec![
            ("70+".to_string(), 70.0),
            ("75+".to_string(), 75.0),
            ("80+".to_string(), 80.0),
            ("85+".to_string(), 85.0),
            ("90+".to_string(), 90.0),
        ];

        let result = resolve_brackets(82.0, &brackets);
        assert_eq!(result.get("70+"), Some(&OutcomeResult::Won));
        assert_eq!(result.get("75+"), Some(&OutcomeResult::Won));
        assert_eq!(result.get("80+"), Some(&OutcomeResult::Won));
        assert_eq!(result.get("85+"), Some(&OutcomeResult::Lost));
        assert_eq!(result.get("90+"), Some(&OutcomeResult::Lost));
    }

    #[test]
    fn test_crypto_watcher() {
        let mut watcher = CryptoThresholdWatcher::new();
        watcher.add_watch(CryptoWatch {
            condition_id: "test-cid".to_string(),
            symbol: "BTCUSD".to_string(),
            threshold: Decimal::from(100_000),
            direction: ThresholdDirection::Above,
            yes_label: "Yes".to_string(),
            no_label: "No".to_string(),
        });

        // Below threshold — no signal
        let signals = watcher.check_price("BTCUSD", Decimal::from(99_000));
        assert!(signals.is_empty());

        // At threshold — fires
        let signals = watcher.check_price("BTCUSD", Decimal::from(100_000));
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].outcomes.get("Yes"),
            Some(&OutcomeResult::Won)
        );

        // Already fired — no duplicate
        let signals = watcher.check_price("BTCUSD", Decimal::from(110_000));
        assert!(signals.is_empty());
    }
}
