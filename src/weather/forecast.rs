//! Weather forecast fetching from NWS (US cities) and Open-Meteo (international fallback).
//!
//! NWS API returns forecasts tied to official observation stations, which is exactly what
//! Polymarket uses for settlement (e.g. LaGuardia for NYC, Midway for Chicago).
//! Open-Meteo provides a free, no-key-required fallback for non-US cities.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use tracing::{debug, warn};

/// Forecast distribution for a target day — the inputs to the edge calculator.
#[derive(Debug, Clone)]
pub struct ForecastDistribution {
    /// Expected high (or low) temperature in the market's unit (°C or °F).
    pub mean: f64,
    /// Standard deviation representing forecast uncertainty.
    /// NWS: ~2.0°, Open-Meteo: ~2.5° (conservative estimates).
    pub std_dev: f64,
    /// Which data source produced this forecast.
    pub source: ForecastSource,
}

#[derive(Debug, Clone, Copy)]
pub enum ForecastSource {
    Nws,
    OpenMeteo,
}

impl std::fmt::Display for ForecastSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nws => write!(f, "NWS"),
            Self::OpenMeteo => write!(f, "Open-Meteo"),
        }
    }
}

/// Fetch a temperature forecast for the given location and target date.
///
/// Tries NWS first when a US station ID is provided; falls back to Open-Meteo.
pub async fn fetch_forecast(
    client: &reqwest::Client,
    station_id: Option<&str>,
    lat: f64,
    lon: f64,
    target_date: chrono::NaiveDate,
    measure: super::TemperatureMeasure,
    unit: super::TemperatureUnit,
) -> Result<ForecastDistribution> {
    // Try NWS for US stations
    if let Some(sid) = station_id {
        if !sid.is_empty() {
            match fetch_nws_forecast(client, lat, lon, target_date, measure, unit).await {
                Ok(dist) => return Ok(dist),
                Err(e) => {
                    warn!(station = %sid, error = %e, "NWS forecast failed, falling back to Open-Meteo");
                }
            }
        }
    }

    fetch_open_meteo_forecast(client, lat, lon, target_date, measure, unit).await
}

// ---------------------------------------------------------------------------
// NWS (National Weather Service) API
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct NwsPointsResponse {
    properties: NwsPointsProperties,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsPointsProperties {
    forecast: String,
}

#[derive(Deserialize)]
struct NwsForecastResponse {
    properties: NwsForecastProperties,
}

#[derive(Deserialize)]
struct NwsForecastProperties {
    periods: Vec<NwsPeriod>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsPeriod {
    start_time: String,
    is_daytime: bool,
    temperature: f64,
    temperature_unit: String,
}

async fn fetch_nws_forecast(
    client: &reqwest::Client,
    lat: f64,
    lon: f64,
    target_date: chrono::NaiveDate,
    measure: super::TemperatureMeasure,
    unit: super::TemperatureUnit,
) -> Result<ForecastDistribution> {
    // Step 1: resolve lat/lon → NWS grid office + grid coords
    let points_url = format!(
        "https://api.weather.gov/points/{:.4},{:.4}",
        lat, lon
    );
    debug!(url = %points_url, "NWS: fetching grid point");

    let resp = client
        .get(&points_url)
        .header(
            "User-Agent",
            "surebet-polymarket-bot/0.2 (github.com/surebet)",
        )
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("NWS /points returned HTTP {}", resp.status()));
    }

    let points: NwsPointsResponse = resp.json().await?;
    let forecast_url = points.properties.forecast;

    // Step 2: fetch the gridpoint forecast (12-hour periods)
    debug!(url = %forecast_url, "NWS: fetching forecast");

    let resp = client
        .get(&forecast_url)
        .header(
            "User-Agent",
            "surebet-polymarket-bot/0.2 (github.com/surebet)",
        )
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("NWS forecast returned HTTP {}", resp.status()));
    }

    let forecast: NwsForecastResponse = resp.json().await?;

    // Step 3: find the period(s) matching the target date and measure
    let mut matched_temps: Vec<f64> = Vec::new();

    for period in &forecast.properties.periods {
        let dt = chrono::DateTime::parse_from_rfc3339(&period.start_time)
            .map_err(|e| anyhow!("bad NWS timestamp '{}': {}", period.start_time, e))?;
        if dt.date_naive() != target_date {
            continue;
        }
        let matches = match measure {
            super::TemperatureMeasure::High => period.is_daytime,
            super::TemperatureMeasure::Low => !period.is_daytime,
        };
        if !matches {
            continue;
        }

        let mut temp = period.temperature;
        // Convert to the market's unit if needed
        let nws_unit = period.temperature_unit.to_uppercase();
        match (nws_unit.as_str(), unit) {
            ("F", super::TemperatureUnit::Celsius) => temp = (temp - 32.0) * 5.0 / 9.0,
            ("C", super::TemperatureUnit::Fahrenheit) => temp = temp * 9.0 / 5.0 + 32.0,
            _ => {}
        }
        matched_temps.push(temp);
    }

    if matched_temps.is_empty() {
        return Err(anyhow!(
            "no NWS periods matched date={} measure={:?}",
            target_date,
            measure
        ));
    }

    let mean = matched_temps.iter().sum::<f64>() / matched_temps.len() as f64;
    // NWS doesn't publish ensemble spread; use 2.0° as a conservative std dev
    // (typical MAE for 1-7 day NWS high-temp forecasts is ~1.5-2.5°)
    let std_dev = 2.0_f64;

    Ok(ForecastDistribution {
        mean,
        std_dev,
        source: ForecastSource::Nws,
    })
}

// ---------------------------------------------------------------------------
// Observations (running max/min so far today)
// ---------------------------------------------------------------------------

/// Observed conditions for the resolution day so far.
///
/// For "Highest temperature" markets, `max_so_far` is the running max — daily
/// max can only stay or increase, so any bracket with `upper ≤ max_so_far`
/// is mathematically dead.
#[derive(Debug, Clone)]
pub struct ObservedConditions {
    /// Highest temperature observed so far on the target date (in market's unit).
    pub max_so_far: f64,
    /// Lowest temperature observed so far on the target date.
    pub min_so_far: f64,
    /// Most recent observed temperature.
    pub current: f64,
    /// Timestamp of the most recent observation (UTC).
    pub last_observation_at: chrono::DateTime<chrono::Utc>,
    /// IANA timezone of the station (used to interpret "today" correctly).
    pub timezone: String,
    pub source: ForecastSource,
}

#[derive(Deserialize)]
struct OpenMeteoObservationsResp {
    timezone: String,
    utc_offset_seconds: i64,
    hourly: OpenMeteoHourly,
}

#[derive(Deserialize)]
struct OpenMeteoHourly {
    time: Vec<String>,
    temperature_2m: Vec<Option<f64>>,
}

/// Forecast for the remaining hours of a calendar day — used by LOW markets
/// to detect "late-evening cooling may push a new daily min below the morning
/// trough" scenarios.
#[derive(Debug, Clone)]
pub struct RemainingDayForecast {
    /// Minimum forecasted temperature across all hours from `now` through
    /// 23:00 local on the target date (Open-Meteo's last sample for the day).
    pub min_temp: f64,
    /// Local time (station tz) at which `min_temp` is forecasted. Format: "HH:00".
    pub min_hour: String,
    /// Count of hourly samples that contributed (for confidence sanity-check).
    pub sample_count: usize,
}

/// Fetch Open-Meteo's hourly forecast for the target date and return the
/// minimum temperature across the *remaining* hours of that local day
/// (current_local_hour onward, inclusive of 23:00 which represents the
/// ~23:00–00:00 window). This is the "tail trajectory" input that tells us
/// whether evening cooling is expected to push a new daily low.
///
/// `target_date` and `current_local_hour` are in the station's local timezone.
pub async fn fetch_remaining_day_forecast(
    client: &reqwest::Client,
    lat: f64,
    lon: f64,
    target_date: chrono::NaiveDate,
    current_local_hour: f64,
    unit: super::TemperatureUnit,
) -> Result<RemainingDayForecast> {
    let temp_unit = match unit {
        super::TemperatureUnit::Celsius => "celsius",
        super::TemperatureUnit::Fahrenheit => "fahrenheit",
    };
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={:.4}&longitude={:.4}\
         &hourly=temperature_2m\
         &temperature_unit={}\
         &timezone=auto\
         &forecast_days=2",
        lat, lon, temp_unit
    );
    debug!(url = %url, "Open-Meteo: fetching remaining-day hourly forecast");

    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "Open-Meteo remaining-day forecast returned HTTP {}",
            resp.status()
        ));
    }
    let data: OpenMeteoObservationsResp = resp.json().await?;

    let target_prefix = target_date.format("%Y-%m-%d").to_string();
    // Floor so we compare "hour X >= current_hour" correctly (e.g., 14:20 → 14).
    let cur_hour_int = current_local_hour.floor() as i64;

    let mut samples: Vec<(String, f64)> = Vec::new();
    for (ts, temp_opt) in data.hourly.time.iter().zip(data.hourly.temperature_2m.iter()) {
        if !ts.starts_with(&target_prefix) {
            continue;
        }
        let Some(temp) = temp_opt else { continue };
        // Parse hour-of-day from "YYYY-MM-DDTHH:MM"
        let Some(hm) = ts.split('T').nth(1) else { continue };
        let Some(hh) = hm.split(':').next() else { continue };
        let Ok(hour_int) = hh.parse::<i64>() else { continue };
        if hour_int < cur_hour_int {
            continue; // already past, METAR owns this
        }
        samples.push((format!("{hh}:00"), *temp));
    }

    if samples.is_empty() {
        return Err(anyhow!(
            "no remaining-day forecast samples for {} starting at hour {}",
            target_prefix,
            cur_hour_int
        ));
    }

    let (min_hour, min_temp) = samples
        .iter()
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .cloned()
        .unwrap();

    Ok(RemainingDayForecast {
        min_temp,
        min_hour,
        sample_count: samples.len(),
    })
}

/// Fetch today's running max/min/current for a station from Open-Meteo's hourly
/// observations + nowcast (`past_days=1` returns recorded values).
///
/// `target_date` should be the resolution date in the station's local timezone.
pub async fn fetch_observations(
    client: &reqwest::Client,
    lat: f64,
    lon: f64,
    target_date: chrono::NaiveDate,
    unit: super::TemperatureUnit,
) -> Result<ObservedConditions> {
    let temp_unit = match unit {
        super::TemperatureUnit::Celsius => "celsius",
        super::TemperatureUnit::Fahrenheit => "fahrenheit",
    };

    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={:.4}&longitude={:.4}\
         &hourly=temperature_2m\
         &temperature_unit={}\
         &timezone=auto\
         &past_days=2&forecast_days=1",
        lat, lon, temp_unit
    );

    debug!(url = %url, "Open-Meteo: fetching observations");

    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("Open-Meteo observations returned HTTP {}", resp.status()));
    }

    let data: OpenMeteoObservationsResp = resp.json().await?;
    let target_prefix = target_date.format("%Y-%m-%d").to_string();
    let now_utc = chrono::Utc::now();
    let offset = data.utc_offset_seconds;

    // Open-Meteo's hourly response mixes past observations and forecast hours.
    // Filter to the target date AND drop any entry whose UTC time is in the
    // future — otherwise "max so far today" includes tomorrow's peak forecast
    // and produces a bogus high (e.g. 83.8°F at 4 AM in Dallas).
    let mut samples: Vec<(chrono::NaiveDateTime, f64)> = Vec::new();
    for (ts, temp_opt) in data.hourly.time.iter().zip(data.hourly.temperature_2m.iter()) {
        if !ts.starts_with(&target_prefix) {
            continue;
        }
        let Some(temp) = temp_opt else { continue };
        let Ok(local_dt) = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M") else {
            continue;
        };
        // Convert local hourly-slot time to UTC and skip anything still in the future.
        let entry_utc = local_dt.and_utc().timestamp() - offset;
        if entry_utc > now_utc.timestamp() {
            continue;
        }
        samples.push((local_dt, *temp));
    }

    if samples.is_empty() {
        return Err(anyhow!(
            "no past hourly observations for {} (timezone {})",
            target_prefix,
            data.timezone
        ));
    }

    // Latest sample is the "current" reading; max/min cover all samples for the day so far.
    samples.sort_by_key(|(dt, _)| *dt);
    let (latest_dt, latest_temp) = *samples.last().expect("non-empty after sort");
    let max_so_far = samples.iter().map(|(_, t)| *t).fold(f64::NEG_INFINITY, f64::max);
    let min_so_far = samples.iter().map(|(_, t)| *t).fold(f64::INFINITY, f64::min);

    // We don't know the station's UTC offset without parsing the IANA tz; approximate
    // last_observation_at as "now" since hourly data is updated within the last hour.
    // This is good enough for "how stale is this" UX.
    let _ = latest_dt; // placeholder until we wire proper tz parsing
    let last_observation_at = now_utc;

    Ok(ObservedConditions {
        max_so_far,
        min_so_far,
        current: latest_temp,
        last_observation_at,
        timezone: data.timezone,
        source: ForecastSource::OpenMeteo,
    })
}

// ---------------------------------------------------------------------------
// Open-Meteo API (free, no API key required)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct OpenMeteoResponse {
    daily: OpenMeteoDailyData,
}

#[derive(Deserialize)]
struct OpenMeteoDailyData {
    time: Vec<String>,
    temperature_2m_max: Option<Vec<Option<f64>>>,
    temperature_2m_min: Option<Vec<Option<f64>>>,
}

async fn fetch_open_meteo_forecast(
    client: &reqwest::Client,
    lat: f64,
    lon: f64,
    target_date: chrono::NaiveDate,
    measure: super::TemperatureMeasure,
    unit: super::TemperatureUnit,
) -> Result<ForecastDistribution> {
    let temp_unit = match unit {
        super::TemperatureUnit::Celsius => "celsius",
        super::TemperatureUnit::Fahrenheit => "fahrenheit",
    };

    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={:.4}&longitude={:.4}\
         &daily=temperature_2m_max,temperature_2m_min\
         &temperature_unit={}\
         &timezone=auto\
         &forecast_days=16",
        lat, lon, temp_unit
    );

    debug!(url = %url, "Open-Meteo: fetching forecast");

    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("Open-Meteo returned HTTP {}", resp.status()));
    }

    let data: OpenMeteoResponse = resp.json().await?;
    let target_str = target_date.format("%Y-%m-%d").to_string();

    let idx = data
        .daily
        .time
        .iter()
        .position(|d| d == &target_str)
        .ok_or_else(|| {
            anyhow!(
                "date {} not in Open-Meteo response ({} days available)",
                target_str,
                data.daily.time.len()
            )
        })?;

    let temp = match measure {
        super::TemperatureMeasure::High => data
            .daily
            .temperature_2m_max
            .as_ref()
            .and_then(|v| v.get(idx))
            .and_then(|v| *v)
            .ok_or_else(|| anyhow!("no max temperature at index {idx}"))?,
        super::TemperatureMeasure::Low => data
            .daily
            .temperature_2m_min
            .as_ref()
            .and_then(|v| v.get(idx))
            .and_then(|v| *v)
            .ok_or_else(|| anyhow!("no min temperature at index {idx}"))?,
    };

    // Open-Meteo deterministic model; use 2.5° std dev as uncertainty estimate
    Ok(ForecastDistribution {
        mean: temp,
        std_dev: 2.5,
        source: ForecastSource::OpenMeteo,
    })
}
