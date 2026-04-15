//! Wunderground HTML scraper for per-station running-max observations.
//!
//! Polymarket resolves temperature bracket markets from Wunderground daily-history
//! pages at URLs like `https://www.wunderground.com/history/daily/gb/london/EGLC`.
//! For live running-max data during the day we hit the station's "Today" page
//! (`/weather/{ICAO}`), which embeds a JSON blob containing:
//!
//! - `"temperatureMaxSince7Am"` — running daily max (the value that resolves the market)
//! - `"temperatureMin24Hour"` — 24h min
//! - `"temperature"` — current reading
//! - `"validTimeUtc"` — unix timestamp of the observation
//!
//! All values in the embedded data are Fahrenheit regardless of viewer locale,
//! so callers convert to the market's unit as needed.

use anyhow::{anyhow, Result};
use tracing::debug;

/// Observations scraped from Wunderground's station page. Always in Fahrenheit.
#[derive(Debug, Clone)]
pub struct WundergroundObservation {
    pub icao: String,
    pub current_f: f64,
    pub max_since_7am_f: f64,
    pub min_24h_f: f64,
    pub observation_time_utc: chrono::DateTime<chrono::Utc>,
}

/// Extract the ICAO station code from a Wunderground resolution URL.
///
/// Resolution URLs look like:
/// `https://www.wunderground.com/history/daily/gb/london/EGLC`
/// or occasionally with a `/date/...` suffix.
pub fn icao_from_resolution_url(url: &str) -> Option<String> {
    let trimmed = url.trim().trim_end_matches('/');
    // Strip any `/date/...` tail
    let without_date = trimmed.split("/date/").next().unwrap_or(trimmed);
    let last = without_date.rsplit('/').next()?;
    // Basic sanity: ICAO codes are 4 uppercase letters/digits
    if last.len() == 4 && last.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(last.to_ascii_uppercase())
    } else {
        None
    }
}

/// Scrape the running daily max for a station.
///
/// `http` must already be configured with a browser User-Agent — Wunderground
/// returns a challenge page to bare requests.
pub async fn fetch_wunderground(
    http: &reqwest::Client,
    icao: &str,
) -> Result<WundergroundObservation> {
    let url = format!("https://www.wunderground.com/weather/{icao}");
    debug!(url = %url, "Wunderground: fetching station page");

    let resp = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("Wunderground {icao} returned HTTP {}", resp.status()));
    }
    let html = resp.text().await?;

    let current_f = extract_num(&html, "\"temperature\":")
        .ok_or_else(|| anyhow!("no temperature field in Wunderground HTML for {icao}"))?;
    let max_since_7am_f = extract_num(&html, "\"temperatureMaxSince7Am\":")
        .ok_or_else(|| anyhow!("no temperatureMaxSince7Am for {icao}"))?;
    let min_24h_f = extract_num(&html, "\"temperatureMin24Hour\":").unwrap_or(current_f);

    let observation_time_utc = extract_num(&html, "\"validTimeUtc\":")
        .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, 0))
        .unwrap_or_else(chrono::Utc::now);

    Ok(WundergroundObservation {
        icao: icao.to_string(),
        current_f,
        max_since_7am_f,
        min_24h_f,
        observation_time_utc,
    })
}

/// Extract the first numeric literal (integer or float, possibly negative)
/// that appears immediately after a given marker string in the source text.
///
/// Used to pluck `"temperature":57` → 57.0 out of Wunderground's embedded JSON
/// without pulling in a full JSON parser (the blob is an Angular SSR payload
/// with many nested objects).
fn extract_num(src: &str, marker: &str) -> Option<f64> {
    let idx = src.find(marker)?;
    let rest = &src[idx + marker.len()..];
    let rest = rest.trim_start();
    let end = rest
        .char_indices()
        .take_while(|(_, c)| matches!(c, '0'..='9' | '.' | '-'))
        .last()
        .map(|(i, c)| i + c.len_utf8())?;
    rest[..end].parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icao_extraction() {
        assert_eq!(
            icao_from_resolution_url("https://www.wunderground.com/history/daily/gb/london/EGLC"),
            Some("EGLC".to_string())
        );
        assert_eq!(
            icao_from_resolution_url("https://www.wunderground.com/history/daily/us/wa/seatac/KSEA/"),
            Some("KSEA".to_string())
        );
        assert_eq!(
            icao_from_resolution_url(
                "https://www.wunderground.com/history/daily/gb/london/EGLC/date/2026-4-14"
            ),
            Some("EGLC".to_string())
        );
        assert_eq!(icao_from_resolution_url("not-a-url"), None);
    }

    #[test]
    fn num_extraction() {
        let src = r#"{"temperature":57,"temperatureMax24Hour":64,"temperatureMaxSince7Am":57.5,"cold":-3}"#;
        assert_eq!(extract_num(src, "\"temperature\":"), Some(57.0));
        assert_eq!(extract_num(src, "\"temperatureMaxSince7Am\":"), Some(57.5));
        assert_eq!(extract_num(src, "\"cold\":"), Some(-3.0));
        assert_eq!(extract_num(src, "\"missing\":"), None);
    }
}
