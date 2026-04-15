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

/// Scale factor for a forecast's σ given the station's current local hour,
/// for a "Highest temperature" market.
///
/// Daily temperature curves are roughly sinusoidal with peak at ~3 PM local.
/// The uncertainty about the day's final max depends on whether the peak has
/// likely already happened:
/// - Before 3 PM: peak still to come; σ scales with time remaining to peak
///   (√-scaling like Brownian motion until we observe the peak).
/// - After 3 PM: peak has likely been observed; σ collapses exponentially
///   toward a small floor (0.05× original) within a few hours.
///
/// The 3 PM default is a Northern-hemisphere spring approximation. Latitude
/// and season shift it ±1–2h; this can be refined per-station later.
pub fn sigma_scale_high(local_hour: f64) -> f64 {
    // Shape:
    //   Pre-peak  (h ≤ 15):       √-scaled, floored at 0.30.
    //   Peak tail (15 < h ≤ 18):  linear decay from 0.30 down to 0.05 —
    //                              daily max almost always locks in before ~6 PM,
    //                              even in mid-summer UK.
    //   After 18 (h > 18):         σ stays at 0.05 — peak is effectively in.
    const PEAK: f64 = 15.0;
    const PEAK_LOCKED: f64 = 18.0;
    const AT_PEAK: f64 = 0.30;
    const RESIDUAL: f64 = 0.05;
    if local_hour <= PEAK {
        ((PEAK - local_hour) / 24.0).sqrt().max(AT_PEAK)
    } else if local_hour <= PEAK_LOCKED {
        let frac = (local_hour - PEAK) / (PEAK_LOCKED - PEAK);
        AT_PEAK * (1.0 - frac) + RESIDUAL * frac
    } else {
        RESIDUAL
    }
}

/// Shape for "Lowest temperature" markets. Asymmetric with HIGH: a day has
/// TWO potential minima — the morning trough (~5 AM) AND late-evening cooling
/// as temp drifts toward the next day's trough. So σ must stay wide all day
/// (we genuinely don't know which min wins until near midnight), collapsing
/// only in the final couple of hours.
pub fn sigma_scale_low(local_hour: f64) -> f64 {
    const TROUGH: f64 = 5.0;
    const LATE_NIGHT_LOCK: f64 = 22.0;
    const AT_TROUGH: f64 = 0.30;
    const RESIDUAL: f64 = 0.05;
    if local_hour <= TROUGH {
        // Pre-trough: √-scaled, floored at AT_TROUGH.
        ((TROUGH - local_hour) / 24.0).sqrt().max(AT_TROUGH)
    } else if local_hour <= LATE_NIGHT_LOCK {
        // All day: σ holds at AT_TROUGH — evening re-cooling is still live.
        // The mean (shifted by a cold-front forecast when applicable) carries
        // the where-is-the-min signal; σ captures residual uncertainty.
        AT_TROUGH
    } else {
        // Last ~2h before midnight: rapid collapse to RESIDUAL.
        let frac = (local_hour - LATE_NIGHT_LOCK) / (24.0 - LATE_NIGHT_LOCK);
        AT_TROUGH * (1.0 - frac) + RESIDUAL * frac
    }
}

/// Observation-aware bracket probability for "Highest" markets.
///
/// The daily max is `max(M, F)` where `M` is the running observed max so far
/// today and `F` is the max over the remaining hours (modeled as N(mean, σ²)).
/// This collapses probability mass below `M` — brackets with `upper ≤ M` get 0,
/// brackets containing `M` absorb the entire [−∞, M] mass they would have been
/// splitting with other brackets.
///
/// Call `bracket_probability_high_observed(lower, upper, M, mean, σ)` where
/// `M` is the observed running max. Falls back to `bracket_probability` when
/// `M` is NaN or no observations are available.
pub fn bracket_probability_high_observed(
    lower: Option<f64>,
    upper: Option<f64>,
    observed_max: f64,
    mean: f64,
    std_dev: f64,
) -> f64 {
    if std_dev <= 0.0 {
        return 0.0;
    }
    // Left-limit of F(x) = P(final_max < x):
    //   x ≤ M → 0  (impossible for final max to be strictly less than observed)
    //   x > M → Φ((x-μ)/σ)
    // F has a jump at x=M; the bracket containing M absorbs that jump via
    // F(upper⁻) − F(M⁻) where F(upper⁻) = Φ((upper-μ)/σ) and F(M⁻) = 0.
    let cdf_left = |x: f64| -> f64 {
        if x <= observed_max {
            0.0
        } else {
            normal_cdf((x - mean) / std_dev)
        }
    };
    let p_upper = match upper {
        Some(u) => cdf_left(u),
        None => 1.0,
    };
    let p_lower = match lower {
        Some(l) => cdf_left(l),
        None => 0.0,
    };
    (p_upper - p_lower).clamp(0.0, 1.0)
}

/// Observation-aware bracket probability for "Lowest" markets.
///
/// Mirror of the high variant: the daily min is `min(m, F)` where `m` is the
/// running observed min. P(final_min ≤ x) = 1 if x ≥ m (already true), else
/// Φ((x-μ)/σ).
pub fn bracket_probability_low_observed(
    lower: Option<f64>,
    upper: Option<f64>,
    observed_min: f64,
    mean: f64,
    std_dev: f64,
) -> f64 {
    if std_dev <= 0.0 {
        return 0.0;
    }
    // Left-limit of G(x) = P(final_min < x):
    //   x ≤ m → Φ((x-μ)/σ)   (continuous region below observed min)
    //   x > m → 1             (final min certainly ≤ observed min)
    let cdf_left = |x: f64| -> f64 {
        if x <= observed_min {
            normal_cdf((x - mean) / std_dev)
        } else {
            1.0
        }
    };
    let p_upper = match upper {
        Some(u) => cdf_left(u),
        None => 1.0,
    };
    let p_lower = match lower {
        Some(l) => cdf_left(l),
        None => 0.0,
    };
    (p_upper - p_lower).clamp(0.0, 1.0)
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
    fn test_sigma_scale_high_shape() {
        assert!(sigma_scale_high(6.0) > 0.5);
        assert!(sigma_scale_high(12.0) < sigma_scale_high(6.0));
        // At peak: exactly AT_PEAK = 0.30
        assert!((sigma_scale_high(15.0) - 0.30).abs() < 0.01);
        // Peak tail: linear interpolation between 15h and 18h.
        // At 16h (1/3 of the way): 0.30*2/3 + 0.05*1/3 = 0.217
        assert!((sigma_scale_high(16.0) - 0.217).abs() < 0.01);
        // At 18h: locked at 0.05
        assert!((sigma_scale_high(18.0) - 0.05).abs() < 0.01);
        // After 18h: flat at 0.05 — peak is certainly in.
        assert!((sigma_scale_high(21.0) - 0.05).abs() < 0.01);
        assert!((sigma_scale_high(23.5) - 0.05).abs() < 0.01);
        // Monotone non-increasing throughout
        let hours: Vec<f64> = (0..24).map(|h| h as f64).collect();
        for pair in hours.windows(2) {
            assert!(
                sigma_scale_high(pair[1]) <= sigma_scale_high(pair[0]) + 1e-9,
                "not monotone at {:?}",
                pair
            );
        }
    }

    #[test]
    fn test_sigma_scale_low_shape() {
        // Pre-trough: wider than AT_TROUGH
        assert!(sigma_scale_low(2.0) > 0.30);
        // At trough: AT_TROUGH = 0.30
        assert!((sigma_scale_low(5.0) - 0.30).abs() < 0.01);
        // Throughout day: stays at AT_TROUGH — evening re-cooling is still live
        assert!((sigma_scale_low(9.0) - 0.30).abs() < 0.01);
        assert!((sigma_scale_low(14.0) - 0.30).abs() < 0.01);
        assert!((sigma_scale_low(20.0) - 0.30).abs() < 0.01);
        // Last 2h: rapid collapse to RESIDUAL
        assert!((sigma_scale_low(23.0) - 0.175).abs() < 0.01);
        // Near midnight: residual only
        assert!(sigma_scale_low(23.9) < 0.10);
    }

    #[test]
    fn test_observed_high_collapses_dead_brackets() {
        // Forecast says peak 18°C ± 2°C. Observed max already = 17°C.
        // Bracket [15, 16) should now have P=0 since the observed max is
        // already above 16.
        let p_dead = bracket_probability_high_observed(Some(15.0), Some(16.0), 17.0, 18.0, 2.0);
        assert!(p_dead.abs() < 1e-9, "expected 0, got {p_dead}");

        // Bracket [17, 18) contains the observation — it absorbs all the mass
        // that would have been split with brackets below 17. So P(final in [17,18))
        // = P(final < 18), since P(final < 17) = 0 (can't drop below observed).
        let p_observed_bucket = bracket_probability_high_observed(
            Some(17.0), Some(18.0), 17.0, 18.0, 2.0,
        );
        // Φ((18-18)/2) = 0.5 — so we'd see ~50% here
        assert!((p_observed_bucket - 0.5).abs() < 0.01, "got {p_observed_bucket}");
    }

    #[test]
    fn test_observed_low_collapses_dead_brackets() {
        // Lowest market: observed min = 13°C. Bracket [14, 15) is now dead
        // because the min has already gone below 14.
        let p_dead = bracket_probability_low_observed(Some(14.0), Some(15.0), 13.0, 15.0, 2.0);
        assert!(p_dead.abs() < 1e-9, "expected 0, got {p_dead}");
    }

    #[test]
    fn test_observed_probs_sum_to_one() {
        // With observations, a complete bracket set should still sum to 1.
        let mean = 15.0_f64;
        let sd = 2.0_f64;
        let obs = 14.5; // observed max = 14.5
        let total = bracket_probability_high_observed(None, Some(13.0), obs, mean, sd)
            + bracket_probability_high_observed(Some(13.0), Some(15.0), obs, mean, sd)
            + bracket_probability_high_observed(Some(15.0), Some(17.0), obs, mean, sd)
            + bracket_probability_high_observed(Some(17.0), None, obs, mean, sd);
        assert!((total - 1.0).abs() < 1e-6, "total={total}");
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
