//! METAR observations from aviationweather.gov (NOAA).
//!
//! METAR is the international airport weather observation standard. For every
//! ICAO station we care about, aviationweather.gov returns structured JSON
//! with integer-°C temperature readings — the same data Wunderground reformats.
//! Fetching METAR directly avoids the °C → °F → °C round-trip rounding loss
//! that Wunderground's scraped HTML introduces.
//!
//! Polymarket's temperature bracket markets resolve to "the highest/lowest
//! temperature recorded at <ICAO> station on <date>", where <date> is the
//! station's local calendar day. Given a station ICAO and target date, this
//! module computes the running max/min over all METAR reports on that day.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use tracing::debug;

#[derive(Deserialize, Debug, Clone)]
pub struct MetarReport {
    #[serde(rename = "icaoId")]
    pub icao_id: String,
    /// Unix timestamp (seconds).
    #[serde(rename = "obsTime")]
    pub obs_time: i64,
    /// Native °C integer (station's reported temperature).
    pub temp: Option<f64>,
    #[serde(rename = "rawOb", default)]
    pub raw_ob: String,
}

/// Result of aggregating METAR reports for a station + day.
#[derive(Debug, Clone)]
pub struct MetarDailyAggregate {
    pub icao: String,
    /// Max temperature observed on the target day so far (°C).
    pub max_c: f64,
    /// Min temperature observed on the target day so far (°C).
    pub min_c: f64,
    /// Latest reading on that day (°C).
    pub current_c: f64,
    /// Timestamp of the latest report used.
    pub latest_obs_utc: chrono::DateTime<chrono::Utc>,
    /// Number of reports that contributed to the aggregate.
    pub report_count: usize,
}

/// Fetch the last `hours` of METAR reports for a station. Default caller
/// should pass something ≥ 24 so that a full calendar day is always covered
/// regardless of when during the day we're querying.
pub async fn fetch_metar_reports(
    http: &reqwest::Client,
    icao: &str,
    hours: u32,
) -> Result<Vec<MetarReport>> {
    let url = format!(
        "https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours={hours}"
    );
    debug!(url = %url, "METAR: fetching reports");
    let resp = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("METAR {icao} returned HTTP {}", resp.status()));
    }
    let reports: Vec<MetarReport> = resp.json().await?;
    Ok(reports)
}

/// UTC offset (seconds east of UTC) for a known airport ICAO code, used to
/// convert METAR `obsTime` into the station's local calendar day.
///
/// Hardcoded; DST is approximated from the current Northern-hemisphere season.
/// Unknown stations default to 0 (UTC) — acceptable for European capitals in
/// any season, wrong by 8–14 hours for East/Far-East stations.
pub fn tz_offset_for_icao(icao: &str) -> i64 {
    match icao {
        // UK & Ireland (currently BST)
        "EGLC" | "EGLL" | "EGSS" | "EGKK" | "EIDW" => 3600,
        // Central Europe (CEST)
        "LFPG" | "LFPO" | "LEMD" | "LEBL" | "EDDM" | "EDDT" | "EDDF"
        | "EHAM" | "EKCH" | "LOWW" | "EPWA" | "LKPR" | "LHBP" | "LIRF"
        | "LIMC" | "LIPZ" | "LSZH" | "LSGG" | "LEPA" | "LPPT" => 7200,
        // Eastern Europe / Turkey
        "EFHK" | "EETN" | "EVRA" | "EYVI" | "LTAC" | "LTBA" | "LTAI"
        | "LTFJ" => 10800,
        // Middle East
        "OEJN" | "OERK" | "OMDB" | "OMAA" | "OTHH" | "OJAI" => 10800,
        "OPKC" | "OPLA" => 18000, // Pakistan UTC+5
        // India
        "VILK" | "VIDP" | "VOBL" | "VOMM" | "VABB" => 19800, // UTC+5:30
        // Southeast Asia
        "VTBS" | "VTBD" => 25200, // Bangkok UTC+7
        "WSSS" | "WMKK" | "WIHH" | "ZSPD" | "ZBAA" | "ZSSS" | "ZUCK"
        | "ZHHH" | "ZUUU" | "RCSS" | "RCTP" | "RPLL" | "VHHH" | "RKSI"
        | "RKPK" => 28800, // UTC+8 (China / SEA / Korea / Philippines)
        "RJTT" | "RJAA" | "RJBB" | "RJGG" => 32400, // Japan UTC+9
        // Oceania (April = autumn, AEST UTC+10, NZST UTC+12)
        "YSSY" | "YMML" | "YBBN" | "YPPH" => 36000,
        "NZAA" | "NZWN" | "NZCH" => 43200,
        // Americas (EDT/CDT/MDT/PDT)
        "KJFK" | "KLGA" | "KEWR" | "KBOS" | "KIAD" | "KDCA" | "KBWI"
        | "KPHL" | "KATL" | "KRDU" | "KCLT" | "KJAX" | "KMIA" | "KRIC"
        | "CYYZ" | "KPIT" | "KCVG" | "KCMH" | "KIND" | "KBNA" | "KSDF"
        | "KDTW" | "KBUF" | "KBDL" | "KPVD" => -14400,
        "KMDW" | "KORD" | "KMSP" | "KMCI" | "KMEM" | "KSTL" | "KOMA"
        | "KMKE" | "KMSY" | "KAUS" | "KDFW" | "KDAL" | "KHOU" | "KSAT" => -18000,
        "KDEN" | "KBKF" | "KSLC" | "KABQ" | "KELP" | "KOKC" => -21600,
        "KPHX" | "KTUS" => -25200, // Arizona (no DST)
        "KSFO" | "KSJC" | "KSMF" | "KLAX" | "KSAN" | "KSEA" | "KPDX"
        | "KLAS" => -25200,
        "MMMX" => -21600,
        "MPMG" => -18000,
        // South America
        "SBGR" | "SBSP" | "SBRJ" | "SBBR" => -10800, // Brazil BRT
        "SAEZ" | "SAEB" => -10800, // Argentina ART
        "SCEL" => -14400, // Santiago
        // Africa
        "DNMM" | "DAAG" | "GMMN" => 3600, // Lagos, Algiers, Casablanca
        "FACT" | "FAJS" | "HECA" | "DTTA" => 7200, // Cape Town, Jo'burg, Cairo
        "HKJK" | "HAAB" => 10800, // Nairobi, Addis
        _ => 0,
    }
}

/// Aggregate METAR reports to running max/min for a specific calendar day.
///
/// `tz_offset_seconds`: the station's offset from UTC (e.g., +3600 for London
/// in BST). Reports with `obs_time` inside `[local_midnight, local_midnight+86400)`
/// at the given offset are included.
///
/// If `tz_offset_seconds` is 0 this simply filters by UTC calendar day — an
/// adequate approximation for stations in European timezones, but wrong for
/// far-east / far-west stations.
pub fn aggregate_by_local_day(
    reports: &[MetarReport],
    target_day: chrono::NaiveDate,
    tz_offset_seconds: i64,
) -> Option<MetarDailyAggregate> {
    if reports.is_empty() {
        return None;
    }
    let icao = reports[0].icao_id.clone();

    // Local midnight of target day expressed as a UTC timestamp.
    let local_midnight = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        target_day.and_hms_opt(0, 0, 0)?,
        chrono::Utc,
    )
    .timestamp()
        - tz_offset_seconds;
    let local_end = local_midnight + 86_400;

    let mut in_day: Vec<(i64, f64)> = reports
        .iter()
        .filter_map(|r| {
            let t = r.temp?;
            if r.obs_time >= local_midnight && r.obs_time < local_end {
                Some((r.obs_time, t))
            } else {
                None
            }
        })
        .collect();
    if in_day.is_empty() {
        return None;
    }
    in_day.sort_by_key(|(t, _)| *t);
    let latest = in_day.last().copied().unwrap();
    let max_c = in_day.iter().map(|(_, t)| *t).fold(f64::NEG_INFINITY, f64::max);
    let min_c = in_day.iter().map(|(_, t)| *t).fold(f64::INFINITY, f64::min);

    Some(MetarDailyAggregate {
        icao,
        max_c,
        min_c,
        current_c: latest.1,
        latest_obs_utc: chrono::DateTime::from_timestamp(latest.0, 0)
            .unwrap_or_else(chrono::Utc::now),
        report_count: in_day.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_report(icao: &str, epoch: i64, temp: f64) -> MetarReport {
        MetarReport {
            icao_id: icao.to_string(),
            obs_time: epoch,
            temp: Some(temp),
            raw_ob: String::new(),
        }
    }

    #[test]
    fn aggregate_filters_to_local_day() {
        // Target day: 2026-04-15 in UTC+1 (London BST)
        // Local midnight = 2026-04-14 23:00 UTC = 1776207600
        let tz = 3600;
        let target = chrono::NaiveDate::from_ymd_opt(2026, 4, 15).unwrap();
        let reports = vec![
            // In-day (local): 2026-04-15 00:30 BST = 2026-04-14 23:30 UTC
            mock_report("EGLC", 1776209400, 13.0),
            // In-day: 2026-04-15 09:20 BST = 2026-04-15 08:20 UTC
            mock_report("EGLC", 1776241200, 15.0),
            // Previous day (local): 2026-04-14 22:00 BST = 21:00 UTC
            mock_report("EGLC", 1776200400, 17.0),
            // Next day (local): 2026-04-16 01:00 BST = 2026-04-16 00:00 UTC
            mock_report("EGLC", 1776297600, 8.0),
        ];
        let agg = aggregate_by_local_day(&reports, target, tz).expect("has reports");
        assert_eq!(agg.report_count, 2);
        assert_eq!(agg.min_c, 13.0);
        assert_eq!(agg.max_c, 15.0);
        assert_eq!(agg.current_c, 15.0); // latest is the 15°C reading
    }

    #[test]
    fn aggregate_utc_offset_zero() {
        // tz=0 acts as pure UTC calendar day filter
        let target = chrono::NaiveDate::from_ymd_opt(2026, 4, 15).unwrap();
        let reports = vec![
            mock_report("EGLC", 1776211199, 10.0), // Apr 14 23:59:59 UTC - excluded
            mock_report("EGLC", 1776211200, 20.0), // Apr 15 00:00:00 UTC - included
            mock_report("EGLC", 1776297599, 5.0),  // Apr 15 23:59:59 UTC - included
            mock_report("EGLC", 1776297600, 30.0), // Apr 16 00:00:00 UTC - excluded
        ];
        let agg = aggregate_by_local_day(&reports, target, 0).expect("has reports");
        assert_eq!(agg.report_count, 2);
        assert_eq!(agg.min_c, 5.0);
        assert_eq!(agg.max_c, 20.0);
    }
}
