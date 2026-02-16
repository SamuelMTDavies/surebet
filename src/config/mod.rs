use serde::Deserialize;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("missing required env var: {0}")]
    MissingEnv(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub polymarket: PolymarketConfig,
    #[serde(default)]
    pub filters: FilterConfig,
    #[serde(default)]
    pub arb: ArbConfig,
    #[serde(default)]
    pub feeds: FeedsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketConfig {
    /// CLOB REST API base URL
    #[serde(default = "default_clob_url")]
    pub clob_url: String,
    /// CLOB WebSocket URL
    #[serde(default = "default_clob_ws_url")]
    pub clob_ws_url: String,
    /// RTDS WebSocket URL
    #[serde(default = "default_rtds_ws_url")]
    pub rtds_ws_url: String,
    /// Gamma API URL (market discovery)
    #[serde(default = "default_gamma_url")]
    pub gamma_url: String,
    /// API key (L2 auth) - loaded from env POLY_API_KEY
    #[serde(default)]
    pub api_key: String,
    /// API secret (L2 auth) - loaded from env POLY_API_SECRET
    #[serde(default)]
    pub api_secret: String,
    /// API passphrase (L2 auth) - loaded from env POLY_API_PASSPHRASE
    #[serde(default)]
    pub api_passphrase: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FilterConfig {
    /// Minimum 24h volume in USDC to consider a market
    #[serde(default = "default_min_volume")]
    pub min_volume_usd: f64,
    /// Minimum order book depth (sum of top 5 levels) in USDC
    #[serde(default = "default_min_depth")]
    pub min_depth_usd: f64,
    /// Maximum spread percentage to consider
    #[serde(default = "default_max_spread")]
    pub max_spread_pct: f64,
    /// Market categories to watch (empty = all)
    #[serde(default)]
    pub categories: Vec<String>,
    /// Market categories to exclude
    #[serde(default)]
    pub exclude_categories: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArbConfig {
    /// Enable the arb scanner.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Enable live execution (false = scan-only / paper mode).
    #[serde(default)]
    pub execute: bool,
    /// Minimum net edge after fees to report/execute.
    #[serde(default = "default_min_edge")]
    pub min_net_edge: f64,
    /// Scan interval in milliseconds.
    #[serde(default = "default_scan_interval_ms")]
    pub scan_interval_ms: u64,
    /// Max USDC per position.
    #[serde(default = "default_max_position")]
    pub max_position_usd: f64,
    /// Max total USDC exposure.
    #[serde(default = "default_max_exposure")]
    pub max_total_exposure: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeedsConfig {
    /// Enable external exchange feeds (Binance, Coinbase).
    #[serde(default)]
    pub enabled: bool,
    /// Binance symbols to track, e.g. ["btcusdt", "ethusdt"].
    #[serde(default = "default_binance_symbols")]
    pub binance_symbols: Vec<String>,
    /// Coinbase product IDs to track, e.g. ["BTC-USD", "ETH-USD"].
    #[serde(default = "default_coinbase_products")]
    pub coinbase_products: Vec<String>,
    /// Max observation age in seconds before considered stale.
    #[serde(default = "default_max_obs_age")]
    pub max_observation_age_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub json: bool,
}

fn default_clob_url() -> String {
    "https://clob.polymarket.com".to_string()
}
fn default_clob_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
}
fn default_rtds_ws_url() -> String {
    "wss://ws-live-data.polymarket.com".to_string()
}
fn default_gamma_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}
fn default_min_volume() -> f64 {
    10_000.0
}
fn default_min_depth() -> f64 {
    1_000.0
}
fn default_max_spread() -> f64 {
    5.0
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_true() -> bool {
    true
}
fn default_min_edge() -> f64 {
    0.005
}
fn default_scan_interval_ms() -> u64 {
    1000
}
fn default_max_position() -> f64 {
    100.0
}
fn default_max_exposure() -> f64 {
    500.0
}
fn default_binance_symbols() -> Vec<String> {
    vec!["btcusdt".to_string(), "ethusdt".to_string()]
}
fn default_coinbase_products() -> Vec<String> {
    vec!["BTC-USD".to_string(), "ETH-USD".to_string()]
}
fn default_max_obs_age() -> u64 {
    5
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            min_volume_usd: default_min_volume(),
            min_depth_usd: default_min_depth(),
            max_spread_pct: default_max_spread(),
            categories: Vec::new(),
            exclude_categories: Vec::new(),
        }
    }
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            execute: false,
            min_net_edge: default_min_edge(),
            scan_interval_ms: default_scan_interval_ms(),
            max_position_usd: default_max_position(),
            max_total_exposure: default_max_exposure(),
        }
    }
}

impl Default for FeedsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            binance_symbols: default_binance_symbols(),
            coinbase_products: default_coinbase_products(),
            max_observation_age_secs: default_max_obs_age(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            json: false,
        }
    }
}

impl Config {
    /// Load config from a TOML file, then overlay environment variables for secrets.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&contents)?;

        // Override secrets from environment variables (never store in config file)
        if let Ok(key) = std::env::var("POLY_API_KEY") {
            config.polymarket.api_key = key;
        }
        if let Ok(secret) = std::env::var("POLY_API_SECRET") {
            config.polymarket.api_secret = secret;
        }
        if let Ok(pass) = std::env::var("POLY_API_PASSPHRASE") {
            config.polymarket.api_passphrase = pass;
        }

        Ok(config)
    }

    /// Load a default config with env-only secrets (no file needed).
    pub fn from_env() -> Self {
        Config {
            polymarket: PolymarketConfig {
                clob_url: std::env::var("POLY_CLOB_URL")
                    .unwrap_or_else(|_| default_clob_url()),
                clob_ws_url: std::env::var("POLY_CLOB_WS_URL")
                    .unwrap_or_else(|_| default_clob_ws_url()),
                rtds_ws_url: std::env::var("POLY_RTDS_WS_URL")
                    .unwrap_or_else(|_| default_rtds_ws_url()),
                gamma_url: std::env::var("POLY_GAMMA_URL")
                    .unwrap_or_else(|_| default_gamma_url()),
                api_key: std::env::var("POLY_API_KEY").unwrap_or_default(),
                api_secret: std::env::var("POLY_API_SECRET").unwrap_or_default(),
                api_passphrase: std::env::var("POLY_API_PASSPHRASE").unwrap_or_default(),
            },
            filters: FilterConfig::default(),
            arb: ArbConfig::default(),
            feeds: FeedsConfig::default(),
            logging: LoggingConfig::default(),
        }
    }

    pub fn has_credentials(&self) -> bool {
        !self.polymarket.api_key.is_empty()
            && !self.polymarket.api_secret.is_empty()
            && !self.polymarket.api_passphrase.is_empty()
    }
}
