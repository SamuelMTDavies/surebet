//! Weather bracket market parsing and early-entry strategy.
//!
//! Polymarket weather markets ask "What will the high/low temperature be in CITY on DATE?"
//! with multi-outcome brackets like "14°C or below", "15-16°C", "19°C or above".
//! This module parses those markets and wires them to a forecast-backed edge calculator.

pub mod forecast;
pub mod metar;
pub mod strategy;
pub mod wunderground;

use chrono::NaiveDate;
use tracing::debug;

/// Whether the market is asking about the daily high or low temperature.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TemperatureMeasure {
    High,
    Low,
}

/// Temperature unit used by the market's outcome labels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
}

/// A single outcome bracket in a multi-outcome temperature market.
///
/// Examples:
/// - "14°C or below"  → lower: None,       upper: Some(14.0)
/// - "15-16°C"        → lower: Some(15.0), upper: Some(16.0)
/// - "19°C or above"  → lower: Some(19.0), upper: None
#[derive(Debug, Clone)]
pub struct WeatherBracket {
    /// The raw outcome label from Polymarket.
    pub label: String,
    /// CLOB token ID for this outcome.
    pub token_id: String,
    /// Inclusive lower bound (None = unbounded below / "or below" style).
    pub lower: Option<f64>,
    /// Inclusive upper bound (None = unbounded above / "or above" style).
    pub upper: Option<f64>,
}

/// A fully parsed weather temperature bracket market, ready for forecast lookup.
#[derive(Debug, Clone)]
pub struct WeatherMarket {
    pub condition_id: String,
    pub question: String,
    /// Extracted city/location string (e.g. "New York", "Chicago").
    pub location: String,
    /// Target date parsed from the question, if found.
    pub date: Option<NaiveDate>,
    pub measure: TemperatureMeasure,
    pub unit: TemperatureUnit,
    /// All parseable outcome brackets (skips any that can't be parsed).
    pub brackets: Vec<WeatherBracket>,
    /// NWS station ID (e.g. "KLGA") — None for non-US cities.
    pub station_id: Option<String>,
    /// Latitude for forecast API calls.
    pub lat: Option<f64>,
    /// Longitude for forecast API calls.
    pub lon: Option<f64>,
}

// ---------------------------------------------------------------------------
// Station / location mapping table
// (pattern, nws_station_id, lat, lon)
// Empty station_id → use Open-Meteo fallback (non-US)
// ---------------------------------------------------------------------------

const STATION_MAPPINGS: &[(&str, &str, f64, f64)] = &[
    ("new york city", "KLGA",  40.7769, -73.8740),
    ("new york",      "KLGA",  40.7769, -73.8740),
    ("nyc",           "KLGA",  40.7769, -73.8740),
    ("los angeles",   "KLAX",  33.9425, -118.4081),
    ("chicago",       "KMDW",  41.7868, -87.7522),
    ("miami",         "KMIA",  25.7959, -80.2870),
    ("houston",       "KHOU",  29.6454, -95.2789),
    ("dallas",        "KDFW",  32.8998, -97.0403),
    ("phoenix",       "KPHX",  33.4373, -112.0078),
    ("seattle",       "KSEA",  47.4502, -122.3088),
    ("boston",        "KBOS",  42.3656, -71.0096),
    ("denver",        "KDEN",  39.8561, -104.6737),
    ("atlanta",       "KATL",  33.6407, -84.4277),
    ("washington dc", "KDCA",  38.8512, -77.0402),
    ("washington",    "KDCA",  38.8512, -77.0402),
    ("las vegas",     "KLAS",  36.0840, -115.1537),
    ("minneapolis",   "KMSP",  44.8820, -93.2218),
    ("detroit",       "KDTW",  42.2162, -83.3554),
    ("portland",      "KPDX",  45.5898, -122.5951),
    ("baltimore",     "KBWI",  39.1754, -76.6684),
    ("san francisco", "KSFO",  37.6213, -122.3790),
    ("san jose",      "KSJC",  37.3626, -121.9290),
    ("san diego",     "KSAN",  32.7338, -117.1933),
    ("oklahoma city", "KOKC",  35.3931, -97.6007),
    ("memphis",       "KMEM",  35.0424, -89.9767),
    ("new orleans",   "KMSY",  29.9934, -90.2580),
    ("charlotte",     "KCLT",  35.2140, -80.9431),
    ("raleigh",       "KRDU",  35.8776, -78.7875),
    ("salt lake",     "KSLC",  40.7884, -111.9778),
    ("kansas city",   "KMCI",  39.2976, -94.7139),
    ("nashville",     "KBNA",  36.1245, -86.6782),
    ("jacksonville",  "KJAX",  30.4941, -81.6879),
    ("indianapolis",  "KIND",  39.7173, -86.2944),
    ("columbus",      "KCMH",  39.9999, -82.8919),
    ("cincinnati",    "KCVG",  39.0488, -84.6678),
    ("pittsburgh",    "KPIT",  40.4915, -80.2329),
    ("st. louis",     "KSTL",  38.7487, -90.3700),
    ("st louis",      "KSTL",  38.7487, -90.3700),
    ("richmond",      "KRIC",  37.5052, -77.3197),
    ("sacramento",    "KSMF",  38.6954, -121.5908),
    ("austin",        "KAUS",  30.1975, -97.6664),
    ("san antonio",   "KSAT",  29.5337, -98.4698),
    ("el paso",       "KELP",  31.8072, -106.3779),
    ("albuquerque",   "KABQ",  35.0402, -106.6090),
    ("tucson",        "KTUS",  32.1161, -110.9410),
    ("omaha",         "KOMA",  41.3032, -95.8941),
    ("buffalo",       "KBUF",  42.9405, -78.7322),
    ("hartford",      "KBDL",  41.9388, -72.6832),
    ("providence",    "KPVD",  41.7240, -71.4328),
    ("milwaukee",     "KMKE",  42.9472, -87.8965),
    ("louisville",    "KSDF",  38.1744, -85.7360),
    // International — empty station_id → Open-Meteo only
    ("london",        "",      51.5085,  -0.1257),
    ("paris",         "",      48.8534,   2.3488),
    ("berlin",        "",      52.5170,  13.3889),
    ("tokyo",         "",      35.6839, 139.7744),
    ("sydney",        "",     -33.8678, 151.2073),
    ("toronto",       "",      43.7001, -79.4163),
    ("amsterdam",     "",      52.3740,   4.8897),
    ("madrid",        "",      40.4165,  -3.7026),
    ("dubai",         "",      25.2532,  55.3657),
    ("singapore",     "",       1.2894, 103.8500),
];

/// Look up the NWS station and coordinates for a location string.
/// Returns `(station_id, lat, lon)` where `station_id` is empty for non-US cities.
pub fn lookup_station(location: &str) -> Option<(&'static str, f64, f64)> {
    let lower = location.to_lowercase();
    for (pattern, station, lat, lon) in STATION_MAPPINGS {
        if lower.contains(pattern) {
            return Some((station, *lat, *lon));
        }
    }
    None
}

/// Returns true if this market looks like a temperature bracket market.
/// Used as a fast pre-filter before attempting full parsing.
pub fn is_temperature_bracket_market(question: &str, outcomes: &[String]) -> bool {
    let q = question.to_lowercase();
    let has_temp_keyword = q.contains("temperature")
        || q.contains("high temp")
        || q.contains("low temp")
        || q.contains("degrees")
        || q.contains("°c")
        || q.contains("°f");

    // At least one outcome must look like a numeric bracket
    let has_bracket = outcomes.iter().any(|o| {
        let ol = o.to_lowercase();
        (ol.contains('°') || ol.contains("degree") || ol.contains("°f") || ol.contains("°c"))
            && (ol.contains("below")
                || ol.contains("above")
                || ol.contains("or more")
                || ol.contains('-'))
    });

    has_temp_keyword || has_bracket
}

/// Parse a bracket label into (lower_bound, upper_bound) of the underlying
/// continuous temperature reading.
///
/// Polymarket resolves to the **floor** of the actual reading (16.9°C → 16),
/// so each integer label `N` covers the half-open interval `[N, N+1)`.
///
/// Handles patterns like:
/// - "14°C or below"   → (None, Some(15.0))      // floor ≤ 14 ⇔ x < 15
/// - "15-16°C"         → (Some(15.0), Some(17.0)) // floor ∈ {15,16} ⇔ 15 ≤ x < 17
/// - "16°C"            → (Some(16.0), Some(17.0)) // floor = 16 ⇔ 16 ≤ x < 17
/// - "19°C or above"   → (Some(19.0), None)      // floor ≥ 19 ⇔ x ≥ 19
/// - "-5°C or below"   → (None, Some(-4.0))      // floor ≤ -5 ⇔ x < -4
pub fn parse_bracket_label(label: &str) -> Option<(Option<f64>, Option<f64>)> {
    let l = label.to_lowercase();

    // "X or below / or less / and below" → upper-bounded; floor ≤ X ⇔ x < X+1
    if l.contains("or below") || l.contains("or less") || l.contains("and below") {
        let val = extract_first_number(label)?;
        return Some((None, Some(val + 1.0)));
    }

    // "X or above / or more / or higher / and above" → lower-bounded;
    // floor ≥ X ⇔ x ≥ X (no adjustment needed)
    if l.contains("or above")
        || l.contains("or more")
        || l.contains("or higher")
        || l.contains("and above")
    {
        let val = extract_first_number(label)?;
        return Some((Some(val), None));
    }

    // Range: "X-Y°C" → floor ∈ [X, Y] ⇔ X ≤ x < Y+1
    // Singleton: "X°C" → floor = X ⇔ X ≤ x < X+1
    let numbers = extract_all_numbers(label);
    match numbers.len() {
        2.. => Some((Some(numbers[0]), Some(numbers[1] + 1.0))),
        1 => Some((Some(numbers[0]), Some(numbers[0] + 1.0))),
        _ => None,
    }
}

/// Parse a full weather market from its metadata.
/// Returns `None` if the market can't be meaningfully parsed.
pub fn parse_weather_market(
    condition_id: &str,
    question: &str,
    outcome_labels: &[String],
    token_ids: &[String],
) -> Option<WeatherMarket> {
    if outcome_labels.len() != token_ids.len() || outcome_labels.is_empty() {
        return None;
    }

    let q_lower = question.to_lowercase();

    // Detect temperature unit (check question first, then outcome labels)
    let unit = if q_lower.contains("°f")
        || q_lower.contains("fahrenheit")
        || outcome_labels
            .iter()
            .any(|o| o.to_lowercase().contains("°f"))
    {
        TemperatureUnit::Fahrenheit
    } else {
        TemperatureUnit::Celsius
    };

    // Detect measure (high vs low temperature)
    let measure = if q_lower.contains("low temp")
        || q_lower.contains("low temperature")
        || q_lower.contains("overnight low")
        || q_lower.contains("minimum temp")
    {
        TemperatureMeasure::Low
    } else {
        TemperatureMeasure::High
    };

    // Extract city name
    let location = extract_location(question)?;

    // Parse date from question
    let date = extract_date(question);

    // Station lookup
    let (station_id, lat, lon) = match lookup_station(&location) {
        Some((s, lat, lon)) => (
            if s.is_empty() { None } else { Some(s.to_string()) },
            Some(lat),
            Some(lon),
        ),
        None => {
            debug!(location = %location, "no station mapping for location");
            (None, None, None)
        }
    };

    // Parse each outcome into a bracket
    let brackets: Vec<WeatherBracket> = outcome_labels
        .iter()
        .zip(token_ids.iter())
        .filter_map(|(label, token_id)| {
            let (lower, upper) = parse_bracket_label(label)?;
            Some(WeatherBracket {
                label: label.clone(),
                token_id: token_id.clone(),
                lower,
                upper,
            })
        })
        .collect();

    if brackets.is_empty() {
        return None;
    }

    Some(WeatherMarket {
        condition_id: condition_id.to_string(),
        question: question.to_string(),
        location,
        date,
        measure,
        unit,
        brackets,
        station_id,
        lat,
        lon,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn extract_location(question: &str) -> Option<String> {
    let words: Vec<&str> = question.split_whitespace().collect();

    // Look for "in CITY" or "for CITY" patterns
    for (i, &word) in words.iter().enumerate() {
        if (word.eq_ignore_ascii_case("in") || word.eq_ignore_ascii_case("for"))
            && i + 1 < words.len()
        {
            let mut parts: Vec<&str> = Vec::new();
            for j in (i + 1)..words.len() {
                let w = words[j].trim_matches(|c: char| c == ',' || c == '?');
                let wl = w.to_lowercase();
                // Stop at common sentence words and date markers
                if matches!(
                    wl.as_str(),
                    "on" | "be" | "exceed" | "the" | "a" | "an" | "will"
                ) {
                    break;
                }
                if is_month_name(w) || w.parse::<i32>().is_ok() {
                    break;
                }
                if w.is_empty() {
                    break;
                }
                parts.push(w);
                if parts.len() >= 3 {
                    break;
                }
            }
            if !parts.is_empty() {
                return Some(parts.join(" "));
            }
        }
    }

    // Fallback: scan known locations directly in the question
    let q_lower = question.to_lowercase();
    for (pattern, _, _, _) in STATION_MAPPINGS {
        if q_lower.contains(pattern) {
            // Capitalise the pattern for display
            let mut loc = pattern.to_string();
            if let Some(c) = loc.get_mut(0..1) {
                c.make_ascii_uppercase();
            }
            return Some(loc);
        }
    }

    None
}

fn is_month_name(s: &str) -> bool {
    matches!(
        s.to_lowercase().trim_matches(|c: char| !c.is_alphabetic()),
        "january"
            | "february"
            | "march"
            | "april"
            | "may"
            | "june"
            | "july"
            | "august"
            | "september"
            | "october"
            | "november"
            | "december"
            | "jan"
            | "feb"
            | "mar"
            | "apr"
            | "jun"
            | "jul"
            | "aug"
            | "sep"
            | "oct"
            | "nov"
            | "dec"
    )
}

fn month_name_to_num(s: &str) -> Option<u32> {
    match s
        .to_lowercase()
        .trim_matches(|c: char| !c.is_alphabetic())
    {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

fn extract_date(question: &str) -> Option<NaiveDate> {
    let words: Vec<&str> = question.split_whitespace().collect();

    // ISO format: "2025-03-15"
    for word in &words {
        let w = word.trim_matches(|c: char| c == ',' || c == '?');
        if let Ok(d) = NaiveDate::parse_from_str(w, "%Y-%m-%d") {
            return Some(d);
        }
    }

    // "Month Day, Year" or "Month Day Year"
    for (i, &word) in words.iter().enumerate() {
        if is_month_name(word) && i + 1 < words.len() {
            let day_str = words[i + 1].trim_matches(|c: char| c == ',' || !c.is_ascii_digit());
            let day: u32 = day_str.parse().ok()?;

            // Year: look at next token after day
            let year_str = words
                .get(i + 2)
                .map(|w| w.trim_matches(|c: char| c == ',' || !c.is_ascii_digit()))
                .unwrap_or("2025");
            let year: i32 = year_str.parse().unwrap_or(2025);

            let month = month_name_to_num(word)?;
            return NaiveDate::from_ymd_opt(year, month, day);
        }
    }

    None
}

/// Extract the first numeric value from a string, respecting a leading minus sign.
fn extract_first_number(s: &str) -> Option<f64> {
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let is_neg = (c == '-' || c == '\u{2212}') // ASCII or Unicode minus
            && i + 1 < chars.len()
            && chars[i + 1].is_ascii_digit();
        if c.is_ascii_digit() || is_neg {
            let mut num = String::new();
            if is_neg {
                num.push('-');
                i += 1;
            }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                num.push(chars[i]);
                i += 1;
            }
            return num.parse().ok();
        }
        i += 1;
    }
    None
}

/// Extract all numeric values from a string (handles negative numbers and ranges).
///
/// "15-16°C" → [15.0, 16.0]
/// "-5 to -2°C" → [-5.0, -2.0]
fn extract_all_numbers(s: &str) -> Vec<f64> {
    let chars: Vec<char> = s.chars().collect();
    let mut nums = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        // A minus/hyphen is a negative sign only when not immediately preceded by a digit
        let is_neg_sign = (c == '-' || c == '\u{2212}')
            && i + 1 < chars.len()
            && chars[i + 1].is_ascii_digit()
            && (i == 0 || !chars[i - 1].is_ascii_digit());

        if c.is_ascii_digit() || is_neg_sign {
            let mut num = String::new();
            if is_neg_sign {
                num.push('-');
                i += 1;
            }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                num.push(chars[i]);
                i += 1;
            }
            if let Ok(n) = num.parse::<f64>() {
                nums.push(n);
            }
        } else {
            i += 1;
        }
    }
    nums
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Bracket bounds use floor semantics: each integer label N covers [N, N+1).

    #[test]
    fn test_parse_bracket_label_upper_bounded() {
        // "14°C or below" → floor ≤ 14 ⇔ x < 15
        let (lower, upper) = parse_bracket_label("14°C or below").unwrap();
        assert_eq!(lower, None);
        assert_eq!(upper, Some(15.0));
    }

    #[test]
    fn test_parse_bracket_label_lower_bounded() {
        // "19°C or above" → floor ≥ 19 ⇔ x ≥ 19 (lower bound unchanged)
        let (lower, upper) = parse_bracket_label("19°C or above").unwrap();
        assert_eq!(lower, Some(19.0));
        assert_eq!(upper, None);
    }

    #[test]
    fn test_parse_bracket_label_range() {
        // "15-16°C" → floor ∈ {15, 16} ⇔ 15 ≤ x < 17
        let (lower, upper) = parse_bracket_label("15-16°C").unwrap();
        assert_eq!(lower, Some(15.0));
        assert_eq!(upper, Some(17.0));
    }

    #[test]
    fn test_parse_bracket_label_singleton() {
        // "16°C" → floor = 16 ⇔ 16 ≤ x < 17
        let (lower, upper) = parse_bracket_label("16°C").unwrap();
        assert_eq!(lower, Some(16.0));
        assert_eq!(upper, Some(17.0));
    }

    #[test]
    fn test_parse_bracket_label_negative() {
        // "-5°C or below" → floor ≤ -5 ⇔ x < -4
        let (lower, upper) = parse_bracket_label("-5°C or below").unwrap();
        assert_eq!(lower, None);
        assert_eq!(upper, Some(-4.0));
    }

    #[test]
    fn test_extract_location() {
        let loc = extract_location("Will the high temperature in New York City on March 15, 2025 exceed 10°C?");
        assert!(loc.is_some());
        let loc = loc.unwrap().to_lowercase();
        assert!(loc.contains("new york"), "got: {loc}");
    }

    #[test]
    fn test_extract_date() {
        let d = extract_date("What will the temperature in Chicago be on March 15, 2025?");
        assert_eq!(d, NaiveDate::from_ymd_opt(2025, 3, 15));
    }

    #[test]
    fn test_lookup_station_nyc() {
        let result = lookup_station("New York City");
        assert!(result.is_some());
        let (station, _, _) = result.unwrap();
        assert_eq!(station, "KLGA");
    }

    #[test]
    fn test_lookup_station_international() {
        let result = lookup_station("London");
        assert!(result.is_some());
        let (station, _, _) = result.unwrap();
        assert_eq!(station, ""); // Open-Meteo fallback
    }
}
