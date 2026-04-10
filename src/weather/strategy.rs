//! Edge calculator and buy signal emitter for weather bracket markets.
//!
//! Given a parsed `WeatherMarket` and a `ForecastDistribution`, this module:
//! 1. Computes per-bracket probabilities using the normal CDF.
//! 2. Compares against current ask prices to calculate edge.
//! 3. Returns buy signals for brackets where edge > threshold.

use std::collections::HashMap;

use tracing::{debug, info};

use super::forecast::ForecastDistribution;
use super::WeatherBracket;
use super::WeatherMarket;

/// A buy signal for a single bracket outcome.
#[derive(Debug, Clone)]
pub struct WeatherBuySignal {
    pub condition_id: String,
    pub token_id: String,
    pub bracket_label: String,
    /// Probability implied by the forecast distribution.
    pub forecast_probability: f64,
    /// Current best ask price on the CLOB (market-implied probability).
    pub ask_price: f64,
    /// edge = forecast_probability − ask_price
    pub edge: f64,
    /// Recommended position size in USD (Kelly-fraction capped at max_position_usd).
    pub position_size_usd: f64,
}

// ---------------------------------------------------------------------------
// Normal CDF — Abramowitz & Stegun 7.1.26 rational approximation
// Max absolute error < 7.5 × 10⁻⁸
// ---------------------------------------------------------------------------

/// Standard normal CDF: Φ(x) = P(Z ≤ x) for Z ~ N(0,1).
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

#[inline]
fn erf(x: f64) -> f64 {
    const A1: f64 = 0.254_829_592;
    const A2: f64 = -0.284_496_736;
    const A3: f64 = 1.421_413_741;
    const A4: f64 = -1.453_152_027;
    const A5: f64 = 1.061_405_429;
    const P: f64 = 0.327_591_1;

    let sign = if x < 0.0 { -1.0_f64 } else { 1.0_f64 };
    let x = x.abs();
    let t = 1.0 / (1.0 + P * x);
    let poly = ((((A5 * t + A4) * t) + A3) * t + A2) * t + A1;
    let y = 1.0 - poly * t * (-x * x).exp();
    sign * y
}

/// Compute P(lower ≤ T ≤ upper) under N(mean, std_dev²).
/// `lower = None` means −∞; `upper = None` means +∞.
pub fn bracket_probability(
    lower: Option<f64>,
    upper: Option<f64>,
    mean: f64,
    std_dev: f64,
) -> f64 {
    if std_dev <= 0.0 {
        return 0.0;
    }
    let cdf_upper = match upper {
        Some(u) => normal_cdf((u - mean) / std_dev),
        None => 1.0,
    };
    let cdf_lower = match lower {
        Some(l) => normal_cdf((l - mean) / std_dev),
        None => 0.0,
    };
    (cdf_upper - cdf_lower).clamp(0.0, 1.0)
}

// ---------------------------------------------------------------------------
// Edge evaluation
// ---------------------------------------------------------------------------

/// Evaluate a parsed weather market against a forecast and return buy signals.
///
/// # Parameters
/// - `ask_prices`: map from `token_id → ask price` (0.0 … 1.0).
///   Brackets with no ask entry are skipped (no liquidity).
/// - `min_edge`: minimum required edge to generate a signal (e.g. 0.15).
/// - `max_position_usd`: hard cap on recommended position size.
/// - `boundary_buffer`: skip brackets where forecast mean is within this many
///   degrees of a bracket boundary (edge is unreliable near boundaries).
pub fn evaluate_market(
    market: &WeatherMarket,
    forecast: &ForecastDistribution,
    ask_prices: &HashMap<String, f64>,
    min_edge: f64,
    max_position_usd: f64,
    boundary_buffer: f64,
) -> Vec<WeatherBuySignal> {
    let mut signals = Vec::new();

    for bracket in &market.brackets {
        // Skip if there's no ask (no liquidity to buy)
        let ask = match ask_prices.get(&bracket.token_id) {
            Some(&a) if a > 0.0 && a < 1.0 => a,
            _ => {
                debug!(token = %bracket.token_id, "skipping bracket: no valid ask price");
                continue;
            }
        };

        // Skip when forecast mean is near a boundary — edge becomes noisy
        if is_near_boundary(bracket, forecast.mean, boundary_buffer) {
            debug!(
                bracket = %bracket.label,
                mean = forecast.mean,
                buffer = boundary_buffer,
                "skipping bracket: mean within boundary buffer"
            );
            continue;
        }

        let prob =
            bracket_probability(bracket.lower, bracket.upper, forecast.mean, forecast.std_dev);
        let edge = prob - ask;

        debug!(
            bracket   = %bracket.label,
            prob      = %format!("{:.3}", prob),
            ask       = %format!("{:.3}", ask),
            edge      = %format!("{:.3}", edge),
            source    = %forecast.source,
            mean      = forecast.mean,
            std_dev   = forecast.std_dev,
            "bracket edge evaluation"
        );

        if edge < min_edge {
            continue;
        }

        // Kelly-inspired sizing: f* = edge / (1 − ask), capped at 25% of max_position_usd
        let kelly_fraction = (edge / (1.0 - ask)).min(0.25);
        let position_size = (kelly_fraction * max_position_usd).max(1.0);

        info!(
            condition_id  = %market.condition_id,
            bracket       = %bracket.label,
            location      = %market.location,
            forecast_prob = %format!("{:.3}", prob),
            ask           = %format!("{:.3}", ask),
            edge          = %format!("{:.3}", edge),
            position_usd  = %format!("{:.2}", position_size),
            source        = %forecast.source,
            "WEATHER BRACKET: buy signal"
        );

        signals.push(WeatherBuySignal {
            condition_id: market.condition_id.clone(),
            token_id: bracket.token_id.clone(),
            bracket_label: bracket.label.clone(),
            forecast_probability: prob,
            ask_price: ask,
            edge,
            position_size_usd: position_size,
        });
    }

    signals
}

/// Returns `true` if the forecast mean is within `buffer` degrees of any bracket boundary.
fn is_near_boundary(bracket: &WeatherBracket, mean: f64, buffer: f64) -> bool {
    if let Some(lower) = bracket.lower {
        if (mean - lower).abs() <= buffer {
            return true;
        }
    }
    if let Some(upper) = bracket.upper {
        if (mean - upper).abs() <= buffer {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_cdf_anchors() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6, "Φ(0) = 0.5");
        // Φ(+∞) → 1, Φ(−∞) → 0
        assert!(normal_cdf(10.0) > 0.999_999);
        assert!(normal_cdf(-10.0) < 0.000_001);
    }

    #[test]
    fn test_normal_cdf_known_quantiles() {
        // Φ(1.645) ≈ 0.950
        assert!((normal_cdf(1.645) - 0.950).abs() < 0.001);
        // Φ(1.96) ≈ 0.975
        assert!((normal_cdf(1.96) - 0.975).abs() < 0.001);
        // Φ(−1.96) ≈ 0.025
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.001);
    }

    #[test]
    fn test_bracket_probability_upper_bounded() {
        // P(T ≤ 14 | μ=12, σ=2): z=(14-12)/2=1.0, Φ(1.0)≈0.841
        let p = bracket_probability(None, Some(14.0), 12.0, 2.0);
        assert!((p - 0.841).abs() < 0.01, "got {p}");
    }

    #[test]
    fn test_bracket_probability_lower_bounded() {
        // P(T ≥ 14 | μ=16, σ=2): 1 - Φ((14-16)/2) = 1 - Φ(-1) ≈ 0.841
        let p = bracket_probability(Some(14.0), None, 16.0, 2.0);
        assert!((p - 0.841).abs() < 0.01, "got {p}");
    }

    #[test]
    fn test_bracket_probability_range() {
        // Tight range centred on mean should capture ~68% under ±1σ
        let p = bracket_probability(Some(13.0), Some(17.0), 15.0, 2.0);
        assert!((p - 0.682).abs() < 0.01, "got {p}");
    }

    #[test]
    fn test_bracket_probabilities_sum_to_one() {
        // A complete set of brackets should sum to ≈1.0
        let mean = 15.0_f64;
        let sd = 2.0_f64;
        let p1 = bracket_probability(None, Some(13.0), mean, sd);
        let p2 = bracket_probability(Some(13.0), Some(15.0), mean, sd);
        let p3 = bracket_probability(Some(15.0), Some(17.0), mean, sd);
        let p4 = bracket_probability(Some(17.0), None, mean, sd);
        let total = p1 + p2 + p3 + p4;
        assert!((total - 1.0).abs() < 1e-6, "total = {total}");
    }

    #[test]
    fn test_evaluate_market_emits_signal() {
        use super::super::{TemperatureMeasure, TemperatureUnit, WeatherBracket, WeatherMarket};
        use crate::weather::forecast::{ForecastDistribution, ForecastSource};

        let market = WeatherMarket {
            condition_id: "0xabc".to_string(),
            question: "Will the high temperature in New York City on March 15, 2025 be 19°C or above?".to_string(),
            location: "New York City".to_string(),
            date: None,
            measure: TemperatureMeasure::High,
            unit: TemperatureUnit::Celsius,
            brackets: vec![
                WeatherBracket {
                    label: "14°C or below".to_string(),
                    token_id: "tok1".to_string(),
                    lower: None,
                    upper: Some(14.0),
                },
                WeatherBracket {
                    label: "15-18°C".to_string(),
                    token_id: "tok2".to_string(),
                    lower: Some(15.0),
                    upper: Some(18.0),
                },
                WeatherBracket {
                    label: "19°C or above".to_string(),
                    token_id: "tok3".to_string(),
                    lower: Some(19.0),
                    upper: None,
                },
            ],
            station_id: Some("KLGA".to_string()),
            lat: Some(40.7769),
            lon: Some(-73.874),
        };

        // Forecast strongly suggests 20°C → tok3 should have high probability
        let forecast = ForecastDistribution {
            mean: 20.0,
            std_dev: 2.0,
            source: ForecastSource::Nws,
        };

        let mut asks = HashMap::new();
        asks.insert("tok1".to_string(), 0.05);  // cheap but low prob
        asks.insert("tok2".to_string(), 0.30);
        asks.insert("tok3".to_string(), 0.50);  // should have edge with mean=20

        let signals = evaluate_market(&market, &forecast, &asks, 0.10, 100.0, 0.5);
        assert!(!signals.is_empty(), "expected at least one signal");
        let tok3 = signals.iter().find(|s| s.token_id == "tok3");
        assert!(tok3.is_some(), "expected tok3 signal");
        assert!(tok3.unwrap().edge > 0.10);
    }
}
