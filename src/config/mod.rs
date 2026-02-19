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
    pub valkey: ValkeyConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub crossbook: CrossbookConfig,
    #[serde(default)]
    pub sniper: SniperConfig,
    #[serde(default)]
    pub onchain: OnChainConfig,
    #[serde(default)]
    pub harvester: HarvesterConfig,
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
pub struct ValkeyConfig {
    /// Valkey/Redis connection URL.
    #[serde(default = "default_valkey_url")]
    pub url: String,
    /// Key namespace prefix. Allows multiple instances (e.g. "surebet:paper"
    /// vs "surebet:live") to share one Valkey without key collisions.
    #[serde(default = "default_valkey_prefix")]
    pub prefix: String,
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

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub json: bool,
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
    /// Default: 21600 (6 hours). With 4 sports this gives ~480 req/month.
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
    /// Minimum Polymarket 24h volume (USDC) for crossbook matching.
    /// Lower than the main `[filters]` since sports markets may have thinner books.
    #[serde(default = "default_crossbook_poly_min_volume")]
    pub poly_min_volume_usd: f64,
    /// Minimum Polymarket depth (USDC) for crossbook matching.
    #[serde(default = "default_crossbook_poly_min_depth")]
    pub poly_min_depth_usd: f64,
    /// Monthly API request budget for The Odds API (free tier = 500).
    #[serde(default = "default_monthly_budget")]
    pub monthly_budget: u32,
    /// Stop fetching when remaining requests fall to this floor.
    /// Preserves a safety margin for manual/debugging requests.
    #[serde(default = "default_min_remaining")]
    pub min_remaining: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SniperConfig {
    /// Enable the resolution sniper.
    #[serde(default)]
    pub enabled: bool,
    /// Enable live execution (false = paper mode, logs opportunities).
    #[serde(default)]
    pub execute: bool,
    /// Minimum confidence (0.0-1.0) to act on a resolution signal.
    #[serde(default = "default_min_confidence")]
    pub min_confidence: f64,
    /// Maximum price to pay for winning outcome tokens ($0.99 = sweep nearly everything).
    #[serde(default = "default_max_buy_price")]
    pub max_buy_price: f64,
    /// Enable crypto price threshold watching (uses existing exchange feeds).
    #[serde(default = "default_true")]
    pub crypto_enabled: bool,
    /// Poll interval for HTTP-based sources in seconds.
    #[serde(default = "default_http_poll_secs")]
    pub http_poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OnChainConfig {
    /// Enable on-chain event monitoring (Polygon WebSocket).
    #[serde(default)]
    pub enabled: bool,
    /// Polygon WebSocket RPC endpoint (Alchemy, QuickNode, Infura, etc.).
    /// Set via POLYGON_WS_URL env var or in config.
    #[serde(default)]
    pub polygon_ws_url: String,
    /// CTF (Conditional Token Framework) contract address on Polygon.
    #[serde(default = "default_ctf_address")]
    pub ctf_address: String,
    /// Neg Risk CTF Exchange contract address.
    #[serde(default = "default_neg_risk_ctf_exchange")]
    pub neg_risk_ctf_exchange: String,
    /// Standard CTF Exchange contract address.
    #[serde(default = "default_ctf_exchange")]
    pub ctf_exchange: String,
    /// Neg Risk Adapter contract address.
    #[serde(default = "default_neg_risk_adapter")]
    pub neg_risk_adapter: String,
    /// UMA Optimistic Oracle V2 address (for ProposePrice/DisputePrice events).
    /// Note: events fire on the Oracle contract, NOT the UmaCtfAdapter.
    #[serde(default = "default_uma_oracle_adapter")]
    pub uma_oracle_adapter: String,
    /// Maximum latency (ms) before alerting. If event processing exceeds
    /// this threshold from block timestamp to signal emission, log a warning.
    #[serde(default = "default_max_latency_ms")]
    pub max_latency_ms: u64,
    /// File path to persist last processed block number for gap recovery.
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
    /// How many blocks back to scan on startup for missed events.
    #[serde(default = "default_startup_lookback")]
    pub startup_lookback_blocks: u64,
    /// Fallback Polygon WebSocket URLs to try when the primary is rate-limited
    /// or unavailable. Rotated through on consecutive failures.
    #[serde(default = "default_fallback_ws_urls")]
    pub fallback_ws_urls: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HarvesterConfig {
    /// Maximum price to pay for winning outcome (sweep asks up to this).
    #[serde(default = "default_harvester_max_buy")]
    pub max_buy_price: f64,
    /// Minimum price to sell losing outcomes (hit bids above this).
    #[serde(default = "default_harvester_min_sell")]
    pub min_sell_price: f64,
    /// End date window: markets ending within this many days from now
    /// (also includes markets whose end date is in the past, up to 24h ago).
    #[serde(default = "default_harvester_end_date_window")]
    pub end_date_window_days: i64,
    /// Maximum number of markets to display in the list.
    #[serde(default = "default_harvester_max_display")]
    pub max_display: usize,
}

// --- Default functions ---

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
fn default_valkey_url() -> String {
    "redis://127.0.0.1:6379".to_string()
}
fn default_valkey_prefix() -> String {
    "surebet".to_string()
}
fn default_dashboard_enabled() -> bool {
    true
}
fn default_dashboard_bind() -> String {
    "127.0.0.1:3030".to_string()
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
        "soccer_spain_la_liga".to_string(),
        "soccer_germany_bundesliga".to_string(),
        "soccer_uefa_champs_league".to_string(),
    ]
}
fn default_crossbook_fetch_interval() -> u64 {
    21600
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
fn default_monthly_budget() -> u32 {
    500
}
fn default_min_remaining() -> u32 {
    50
}
fn default_crossbook_poly_min_volume() -> f64 {
    100.0
}
fn default_crossbook_poly_min_depth() -> f64 {
    0.0
}
fn default_min_confidence() -> f64 {
    0.95
}
fn default_max_buy_price() -> f64 {
    0.99
}
fn default_http_poll_secs() -> u64 {
    30
}
fn default_harvester_max_buy() -> f64 {
    0.995
}
fn default_harvester_min_sell() -> f64 {
    0.005
}
fn default_harvester_end_date_window() -> i64 {
    1
}
fn default_harvester_max_display() -> usize {
    50
}
fn default_ctf_address() -> String {
    "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".to_string()
}
fn default_neg_risk_ctf_exchange() -> String {
    "0xC5d563A36AE78145C45a50134d48A1215220f80a".to_string()
}
fn default_ctf_exchange() -> String {
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".to_string()
}
fn default_neg_risk_adapter() -> String {
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296".to_string()
}
fn default_uma_oracle_adapter() -> String {
    "0xeE3Afe347D5C74317041E2618C49534dAf887c24".to_string()
}
fn default_max_latency_ms() -> u64 {
    2000
}
fn default_checkpoint_path() -> String {
    "onchain_checkpoint.txt".to_string()
}
fn default_startup_lookback() -> u64 {
    10
}
fn default_fallback_ws_urls() -> Vec<String> {
    vec![
        "wss://polygon-bor-rpc.publicnode.com".to_string(),
        "wss://polygon.drpc.org".to_string(),
    ]
}

// --- Default impls ---

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

impl Default for ValkeyConfig {
    fn default() -> Self {
        Self {
            url: default_valkey_url(),
            prefix: default_valkey_prefix(),
        }
    }
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: default_dashboard_enabled(),
            bind: default_dashboard_bind(),
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
            monthly_budget: default_monthly_budget(),
            min_remaining: default_min_remaining(),
            poly_min_volume_usd: default_crossbook_poly_min_volume(),
            poly_min_depth_usd: default_crossbook_poly_min_depth(),
        }
    }
}

impl Default for SniperConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            execute: false,
            min_confidence: default_min_confidence(),
            max_buy_price: default_max_buy_price(),
            crypto_enabled: true,
            http_poll_interval_secs: default_http_poll_secs(),
        }
    }
}

impl Default for OnChainConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            polygon_ws_url: String::new(),
            ctf_address: default_ctf_address(),
            neg_risk_ctf_exchange: default_neg_risk_ctf_exchange(),
            ctf_exchange: default_ctf_exchange(),
            neg_risk_adapter: default_neg_risk_adapter(),
            uma_oracle_adapter: default_uma_oracle_adapter(),
            max_latency_ms: default_max_latency_ms(),
            checkpoint_path: default_checkpoint_path(),
            startup_lookback_blocks: default_startup_lookback(),
            fallback_ws_urls: default_fallback_ws_urls(),
        }
    }
}

impl Default for HarvesterConfig {
    fn default() -> Self {
        Self {
            max_buy_price: default_harvester_max_buy(),
            min_sell_price: default_harvester_min_sell(),
            end_date_window_days: default_harvester_end_date_window(),
            max_display: default_harvester_max_display(),
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
            if !key.is_empty() {
                config.crossbook.enabled = true;
            }
            config.crossbook.odds_api_key = key;
        }
        if let Ok(ws_url) = std::env::var("POLYGON_WS_URL") {
            if !ws_url.is_empty() {
                config.onchain.polygon_ws_url = ws_url;
            }
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
            valkey: ValkeyConfig {
                url: std::env::var("VALKEY_URL").unwrap_or_else(|_| default_valkey_url()),
                prefix: std::env::var("VALKEY_PREFIX").unwrap_or_else(|_| default_valkey_prefix()),
            },
            dashboard: DashboardConfig::default(),
            logging: LoggingConfig::default(),
            crossbook: {
                let key = std::env::var("ODDS_API_KEY").unwrap_or_default();
                let auto_enable = !key.is_empty();
                CrossbookConfig {
                    enabled: auto_enable,
                    odds_api_key: key,
                    ..CrossbookConfig::default()
                }
            },
            sniper: SniperConfig::default(),
            harvester: HarvesterConfig::default(),
            onchain: {
                let ws_url = std::env::var("POLYGON_WS_URL").unwrap_or_default();
                let auto_enable = !ws_url.is_empty();
                OnChainConfig {
                    enabled: auto_enable,
                    polygon_ws_url: ws_url,
                    ..OnChainConfig::default()
                }
            },
        }
    }

    pub fn has_credentials(&self) -> bool {
        !self.polymarket.api_key.is_empty()
            && !self.polymarket.api_secret.is_empty()
            && !self.polymarket.api_passphrase.is_empty()
    }
}
