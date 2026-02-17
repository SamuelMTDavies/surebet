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
    pub maker: MakerStrategyConfig,
    #[serde(default)]
    pub feeds: FeedsConfig,
    #[serde(default)]
    pub valkey: ValkeyConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub anomaly: AnomalyConfig,
    #[serde(default)]
    pub crossbook: CrossbookConfig,
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
pub struct MakerStrategyConfig {
    /// Enable the passive maker strategy.
    #[serde(default)]
    pub enabled: bool,
    /// Enable live execution (false = paper mode, logs orders without placing them).
    #[serde(default)]
    pub execute: bool,
    /// Target bid sum as fraction of $1.00 (lower = more edge, less fill).
    #[serde(default = "default_target_bid_sum")]
    pub target_bid_sum: f64,
    /// Order size in shares per leg.
    #[serde(default = "default_order_size")]
    pub order_size: f64,
    /// Minimum spread to post into.
    #[serde(default = "default_maker_min_spread")]
    pub min_spread: f64,
    /// Maximum inventory imbalance before pausing.
    #[serde(default = "default_max_imbalance")]
    pub max_inventory_imbalance: f64,
    /// Requote interval in seconds.
    #[serde(default = "default_requote_interval")]
    pub requote_interval_secs: u64,
    /// Minimum price change to trigger requote.
    #[serde(default = "default_requote_threshold")]
    pub requote_threshold: f64,
    /// Seconds to wait for second leg after first fill before unwinding.
    #[serde(default = "default_fill_timeout_secs")]
    pub fill_timeout_secs: u64,
    /// Fraction of spread to chase on unfilled leg after partial fill (0.0-1.0).
    #[serde(default = "default_aggressive_reprice_pct")]
    pub aggressive_reprice_pct: f64,
    /// Max seconds since last trade on an outcome before skipping that market.
    #[serde(default = "default_min_activity_age_secs")]
    pub min_activity_age_secs: u64,
}

fn default_target_bid_sum() -> f64 {
    0.97
}
fn default_order_size() -> f64 {
    10.0
}
fn default_maker_min_spread() -> f64 {
    0.02
}
fn default_max_imbalance() -> f64 {
    50.0
}
fn default_requote_interval() -> u64 {
    5
}
fn default_requote_threshold() -> f64 {
    0.01
}
fn default_fill_timeout_secs() -> u64 {
    30
}
fn default_aggressive_reprice_pct() -> f64 {
    0.50
}
fn default_min_activity_age_secs() -> u64 {
    300
}

impl Default for MakerStrategyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            execute: false,
            target_bid_sum: default_target_bid_sum(),
            order_size: default_order_size(),
            min_spread: default_maker_min_spread(),
            max_inventory_imbalance: default_max_imbalance(),
            requote_interval_secs: default_requote_interval(),
            requote_threshold: default_requote_threshold(),
            fill_timeout_secs: default_fill_timeout_secs(),
            aggressive_reprice_pct: default_aggressive_reprice_pct(),
            min_activity_age_secs: default_min_activity_age_secs(),
        }
    }
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
pub struct ValkeyConfig {
    /// Valkey/Redis connection URL.
    #[serde(default = "default_valkey_url")]
    pub url: String,
    /// Key namespace prefix. Allows multiple instances (e.g. "surebet:paper"
    /// vs "surebet:live") to share one Valkey without key collisions.
    #[serde(default = "default_valkey_prefix")]
    pub prefix: String,
}

fn default_valkey_url() -> String {
    "redis://127.0.0.1:6379".to_string()
}

fn default_valkey_prefix() -> String {
    "surebet".to_string()
}

impl Default for ValkeyConfig {
    fn default() -> Self {
        Self {
            url: default_valkey_url(),
            prefix: default_valkey_prefix(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    /// Enable the HTTP dashboard.
    #[serde(default = "default_dashboard_enabled")]
    pub enabled: bool,
    /// Bind address for the dashboard server.
    #[serde(default = "default_dashboard_bind")]
    pub bind: String,
}

fn default_dashboard_enabled() -> bool {
    true
}

fn default_dashboard_bind() -> String {
    "127.0.0.1:3030".to_string()
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: default_dashboard_enabled(),
            bind: default_dashboard_bind(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub json: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnomalyConfig {
    /// Enable the anomaly detector.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Scan interval in milliseconds.
    #[serde(default = "default_anomaly_scan_interval")]
    pub scan_interval_ms: u64,
    /// Rolling window for trade history per asset (seconds).
    #[serde(default = "default_anomaly_window")]
    pub window_secs: u64,
    /// A single trade this many times the average trade size is a "whale" trade.
    #[serde(default = "default_whale_multiplier")]
    pub whale_size_multiplier: f64,
    /// Trade size as fraction of top-5 depth to flag as "thin-book aggression".
    #[serde(default = "default_depth_fraction")]
    pub depth_aggression_fraction: f64,
    /// Volume in window must exceed this multiple of the previous window to flag "volume spike".
    #[serde(default = "default_volume_spike")]
    pub volume_spike_multiplier: f64,
    /// Price must move this many percent from the rolling mean to flag "price dislocation".
    #[serde(default = "default_price_move_pct")]
    pub price_move_pct: f64,
    /// Bid/ask depth ratio threshold to flag "book imbalance" (e.g. 5.0 means one side 5x heavier).
    #[serde(default = "default_imbalance_ratio")]
    pub book_imbalance_ratio: f64,
    /// Maximum number of anomalies to keep in memory for the dashboard.
    #[serde(default = "default_max_anomalies")]
    pub max_anomalies: usize,
}

fn default_anomaly_scan_interval() -> u64 {
    5000
}
fn default_anomaly_window() -> u64 {
    300
}
fn default_whale_multiplier() -> f64 {
    10.0
}
fn default_depth_fraction() -> f64 {
    0.20
}
fn default_volume_spike() -> f64 {
    5.0
}
fn default_price_move_pct() -> f64 {
    10.0
}
fn default_imbalance_ratio() -> f64 {
    5.0
}
fn default_max_anomalies() -> usize {
    200
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            scan_interval_ms: default_anomaly_scan_interval(),
            window_secs: default_anomaly_window(),
            whale_size_multiplier: default_whale_multiplier(),
            depth_aggression_fraction: default_depth_fraction(),
            volume_spike_multiplier: default_volume_spike(),
            price_move_pct: default_price_move_pct(),
            book_imbalance_ratio: default_imbalance_ratio(),
            max_anomalies: default_max_anomalies(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrossbookConfig {
    /// Enable the cross-bookmaker arb scanner.
    #[serde(default)]
    pub enabled: bool,
    /// The Odds API key (loaded from env ODDS_API_KEY).
    #[serde(default)]
    pub odds_api_key: String,
    /// The Odds API base URL.
    #[serde(default = "default_odds_api_url")]
    pub odds_api_url: String,
    /// Regions to fetch odds for (comma-separated: us, uk, eu, au).
    #[serde(default = "default_odds_regions")]
    pub regions: String,
    /// Markets to fetch (comma-separated: h2h, h2h_lay, spreads, totals).
    #[serde(default = "default_odds_markets")]
    pub markets: String,
    /// Sports keys to monitor from The Odds API.
    #[serde(default = "default_sports_keys")]
    pub sports_keys: Vec<String>,
    /// How often to fetch fresh odds from The Odds API (seconds).
    #[serde(default = "default_crossbook_fetch_interval")]
    pub fetch_interval_secs: u64,
    /// How often to scan for cross-provider arbs (milliseconds).
    #[serde(default = "default_crossbook_scan_interval")]
    pub scan_interval_ms: u64,
    /// Minimum edge (%) to report a cross-provider arb.
    #[serde(default = "default_crossbook_min_edge")]
    pub min_edge_pct: f64,
    /// Maximum results to keep for dashboard.
    #[serde(default = "default_crossbook_max_results")]
    pub max_results: usize,
}

fn default_odds_api_url() -> String {
    "https://api.the-odds-api.com/v4/sports".to_string()
}
fn default_odds_regions() -> String {
    "uk,eu".to_string()
}
fn default_odds_markets() -> String {
    "h2h,h2h_lay".to_string()
}
fn default_sports_keys() -> Vec<String> {
    vec![
        "soccer_epl".to_string(),
        "soccer_efl_champ".to_string(),
        "soccer_england_league1".to_string(),
        "soccer_spain_la_liga".to_string(),
        "soccer_germany_bundesliga".to_string(),
        "soccer_italy_serie_a".to_string(),
        "soccer_france_ligue_one".to_string(),
        "soccer_uefa_champs_league".to_string(),
        "soccer_uefa_europa_league".to_string(),
    ]
}
fn default_crossbook_fetch_interval() -> u64 {
    120
}
fn default_crossbook_scan_interval() -> u64 {
    5000
}
fn default_crossbook_min_edge() -> f64 {
    1.0
}
fn default_crossbook_max_results() -> usize {
    100
}

impl Default for CrossbookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            odds_api_key: String::new(),
            odds_api_url: default_odds_api_url(),
            regions: default_odds_regions(),
            markets: default_odds_markets(),
            sports_keys: default_sports_keys(),
            fetch_interval_secs: default_crossbook_fetch_interval(),
            scan_interval_ms: default_crossbook_scan_interval(),
            min_edge_pct: default_crossbook_min_edge(),
            max_results: default_crossbook_max_results(),
        }
    }
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
        if let Ok(key) = std::env::var("ODDS_API_KEY") {
            config.crossbook.odds_api_key = key;
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
            maker: MakerStrategyConfig::default(),
            feeds: FeedsConfig::default(),
            valkey: ValkeyConfig {
                url: std::env::var("VALKEY_URL").unwrap_or_else(|_| default_valkey_url()),
                prefix: std::env::var("VALKEY_PREFIX").unwrap_or_else(|_| default_valkey_prefix()),
            },
            dashboard: DashboardConfig::default(),
            logging: LoggingConfig::default(),
            anomaly: AnomalyConfig::default(),
            crossbook: CrossbookConfig {
                odds_api_key: std::env::var("ODDS_API_KEY").unwrap_or_default(),
                ..CrossbookConfig::default()
            },
        }
    }

    pub fn has_credentials(&self) -> bool {
        !self.polymarket.api_key.is_empty()
            && !self.polymarket.api_secret.is_empty()
            && !self.polymarket.api_passphrase.is_empty()
    }
}
