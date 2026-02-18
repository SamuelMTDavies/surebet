mod anomaly;
mod arb;
mod crossbook;
mod auth;
mod config;
mod dashboard;
mod feeds;
mod lifecycle;
mod maker;
mod market;
mod metrics;
mod onchain;
mod orderbook;
mod sniper;
mod split;
mod store;
mod ws;

use crate::anomaly::{AnomalyDetector, AnomalyEvent};
use crate::arb::executor::{ArbExecutor, RiskLimits};
use crate::crossbook::{CrossbookEvent, CrossbookScanner};
use crate::lifecycle::{LifecycleEvent, LifecycleManager};
use crate::maker::{MakerConfig, MakerEvent, MakerStrategy};
use crate::metrics::MetricsTracker;
use crate::arb::{ArbEvent, ArbScanner, TrackedMarket};
use crate::auth::{ClobApiClient, L2Credentials};
use crate::config::Config;
use crate::dashboard::DashboardState;
use crate::feeds::aggregator::{AggregatorEvent, PriceAggregator};
use crate::feeds::binance::{BinanceEvent, BinanceFeed};
use crate::feeds::coinbase::{CoinbaseEvent, CoinbaseFeed};
use crate::market::{classify_edge_profile, DiscoveredMarket, MarketDiscovery};
use crate::onchain::{MarketCache, OnChainMonitor, OnChainSignal};
use crate::orderbook::OrderBookStore;
use crate::sniper::{SniperEngine, SniperEvent, auto_map_markets};
use crate::sniper::executor::SniperExecutor;
use crate::sniper::sources::CryptoThresholdWatcher;
use crate::split::{SplitEvent, SplitScanner};
use crate::split::executor::{SplitExecutor, SplitRiskLimits};
use crate::store::{FillRecord, OrderRecord, OrderStatus, PositionLeg, PositionRecord, StateStore};
use crate::ws::clob::{ClobEvent, start_clob_ws};
use crate::ws::rtds::{RtdsEvent, RtdsSubscription, RtdsWsClient};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install rustls crypto provider before any TLS usage.
    // The SDK's WS client needs this to establish TLS connections.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Load .env if present
    let _ = dotenvy::dotenv();

    // Load config
    let config = if Path::new("surebet.toml").exists() {
        Config::load(Path::new("surebet.toml"))?
    } else {
        info!("no surebet.toml found, using env-only config");
        Config::from_env()
    };

    // Initialize logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.logging.level));

    if config.logging.json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .init();
    }

    info!("surebet v{} starting", env!("CARGO_PKG_VERSION"));

    // --- Valkey State Store ---
    let activity_ttl = Duration::from_secs(config.maker.min_activity_age_secs);
    let state_store = match StateStore::connect(&config.valkey.url, activity_ttl, &config.valkey.prefix).await {
        Ok(mut s) => {
            if let Err(e) = s.ping().await {
                error!(error = %e, "Valkey ping failed — continuing without state store");
                None
            } else {
                info!(url = %config.valkey.url, "Valkey state store connected");
                Some(Arc::new(Mutex::new(s)))
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                url = %config.valkey.url,
                "failed to connect to Valkey — running without persistent state"
            );
            None
        }
    };

    // Dashboard is launched after anomaly detector is created (see below).

    // --- L2 Auth Setup ---
    let creds = L2Credentials::from_config(
        &config.polymarket.api_key,
        &config.polymarket.api_secret,
        &config.polymarket.api_passphrase,
    );

    if creds.is_none() {
        warn!(
            "no API credentials configured - running in read-only mode \
             (set POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE for trading)"
        );
    }

    let api_client = creds.as_ref().map(|c| {
        Arc::new(ClobApiClient::new(
            config.polymarket.clob_url.clone(),
            c.clone(),
        ))
    });

    // --- Market Discovery ---
    info!("discovering active markets...");
    let discovery = MarketDiscovery::new(
        config.polymarket.gamma_url.clone(),
        config.filters.clone(),
    );

    // Fetch all markets once, then filter twice: strict for arb/maker,
    // relaxed for crossbook (sports markets often have lower Polymarket volume).
    let all_gamma_markets = discovery.fetch_active_markets().await?;
    let markets = discovery.filter_markets(&all_gamma_markets);

    let mut asset_ids: Vec<String> = markets
        .iter()
        .flat_map(|m| m.clob_token_ids.clone())
        .collect();

    if asset_ids.is_empty() {
        error!("no markets found matching filters, exiting");
        return Ok(());
    }

    // Crossbook gets a second filter pass with relaxed thresholds so that
    // thin sports markets aren't dropped before the matcher sees them.
    let crossbook_discovered = if config.crossbook.enabled
        && !config.crossbook.odds_api_key.is_empty()
    {
        let crossbook_filters = crate::config::FilterConfig {
            min_volume_usd: config.crossbook.poly_min_volume_usd,
            min_depth_usd: config.crossbook.poly_min_depth_usd,
            ..config.filters.clone()
        };
        let crossbook_discovery = MarketDiscovery::new(
            config.polymarket.gamma_url.clone(),
            crossbook_filters,
        );
        let cb_markets = crossbook_discovery.filter_markets(&all_gamma_markets);

        // Merge extra asset IDs into the CLOB WS subscription
        let extra_ids: Vec<String> = cb_markets
            .iter()
            .flat_map(|m| m.clob_token_ids.clone())
            .filter(|id| !asset_ids.contains(id))
            .collect();
        info!(
            crossbook_markets = cb_markets.len(),
            extra_assets = extra_ids.len(),
            "crossbook discovery (relaxed filters)"
        );
        asset_ids.extend(extra_ids);
        cb_markets
    } else {
        Vec::new()
    };

    // Build tracked markets for arb scanner
    let tracked_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();

    info!(markets = markets.len(), "market edge profiles loaded");
    for m in &markets {
        let profile = classify_edge_profile(m);
        debug!(
            question = %m.question,
            category = %m.category,
            volume_24h = %m.volume_24hr,
            liquidity = %m.liquidity,
            profile = %profile,
            tokens = m.clob_token_ids.len(),
            "market"
        );
    }

    // --- Order Book Store ---
    let store = OrderBookStore::new();

    // --- CLOB WebSocket ---
    let (clob_tx, mut clob_rx) = mpsc::unbounded_channel::<ClobEvent>();
    info!(assets = asset_ids.len(), "subscribing to CLOB market channel (SDK)");
    start_clob_ws(store.clone(), clob_tx, asset_ids)?;

    // --- RTDS WebSocket ---
    let (rtds_tx, mut rtds_rx) = mpsc::unbounded_channel::<RtdsEvent>();
    let rtds_client = RtdsWsClient::new(config.polymarket.rtds_ws_url.clone(), rtds_tx);
    let rtds_subs = vec![
        RtdsSubscription {
            topic: "activity".to_string(),
            event_type: "*".to_string(),
            filters: None,
        },
        RtdsSubscription {
            topic: "crypto_prices".to_string(),
            event_type: "*".to_string(),
            filters: None,
        },
    ];
    info!("subscribing to RTDS activity feed");
    rtds_client.subscribe(rtds_subs).await?;

    // --- Arb Scanner ---
    let (arb_tx, mut arb_rx) = mpsc::unbounded_channel::<ArbEvent>();
    let min_net_edge =
        Decimal::from_str(&config.arb.min_net_edge.to_string()).unwrap_or(Decimal::ZERO);
    let mut arb_scanner = ArbScanner::new(
        store.clone(),
        tracked_markets,
        arb_tx,
        min_net_edge,
    );

    // --- Arb Executor ---
    let executor = api_client.as_ref().map(|api| {
        let limits = RiskLimits {
            max_position_usd: Decimal::from_str(&config.arb.max_position_usd.to_string())
                .unwrap_or(Decimal::from(100)),
            max_total_exposure: Decimal::from_str(&config.arb.max_total_exposure.to_string())
                .unwrap_or(Decimal::from(500)),
            min_net_edge,
            ..RiskLimits::default()
        };
        Arc::new(ArbExecutor::new(api.clone(), limits))
    });

    if config.arb.execute {
        if let Some(ref exec) = executor {
            exec.enable();
            info!("arb executor enabled - LIVE EXECUTION MODE");
        } else {
            warn!("arb.execute=true but no API credentials - staying in paper mode");
        }
    } else {
        info!("arb executor in paper mode (set arb.execute=true to go live)");
    }

    // --- Maker Strategy ---
    let (maker_tx, mut maker_rx) = mpsc::unbounded_channel::<MakerEvent>();

    // The maker needs its own tracked_markets clone since it moves into a task
    let maker_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();

    // Shared maker: Arc<Mutex> so both the tick task and RTDS fill handler can access it
    let maker: Option<Arc<Mutex<MakerStrategy>>> = if config.maker.enabled {
        let mc = &config.maker;
        let maker_config = MakerConfig {
            execute: mc.execute,
            target_bid_sum: Decimal::from_str(&mc.target_bid_sum.to_string())
                .unwrap_or(Decimal::from_str("0.97").unwrap()),
            order_size: Decimal::from_str(&mc.order_size.to_string())
                .unwrap_or(Decimal::from(10)),
            min_spread: Decimal::from_str(&mc.min_spread.to_string())
                .unwrap_or(Decimal::from_str("0.02").unwrap()),
            max_inventory_imbalance: Decimal::from_str(&mc.max_inventory_imbalance.to_string())
                .unwrap_or(Decimal::from(50)),
            requote_interval: Duration::from_secs(mc.requote_interval_secs),
            requote_threshold: Decimal::from_str(&mc.requote_threshold.to_string())
                .unwrap_or(Decimal::from_str("0.01").unwrap()),
            fill_timeout: Duration::from_secs(mc.fill_timeout_secs),
            aggressive_reprice_pct: Decimal::from_str(&mc.aggressive_reprice_pct.to_string())
                .unwrap_or(Decimal::from_str("0.50").unwrap()),
            min_activity_age: Duration::from_secs(mc.min_activity_age_secs),
        };

        // Maker can run without API credentials (paper mode)
        let mut strategy = MakerStrategy::new(
            api_client.clone(),
            store.clone(),
            maker_config,
            maker_tx,
        );
        strategy.add_markets(&maker_markets);

        let shared = Arc::new(Mutex::new(strategy));

        // Spawn the tick task with a clone of the shared maker
        let maker_for_tick = shared.clone();
        let tick_interval = Duration::from_secs(mc.requote_interval_secs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            loop {
                interval.tick().await;
                maker_for_tick.lock().await.tick().await;
            }
        });

        let mode = if mc.execute && api_client.is_some() {
            "LIVE EXECUTION"
        } else if api_client.is_some() {
            "PAPER (set maker.execute=true to go live)"
        } else {
            "PAPER (no API credentials)"
        };

        info!(
            markets = maker_markets.len(),
            target_sum = %config.maker.target_bid_sum,
            fill_timeout_secs = mc.fill_timeout_secs,
            reprice_pct = %mc.aggressive_reprice_pct,
            activity_age_secs = mc.min_activity_age_secs,
            mode = mode,
            "maker strategy enabled"
        );
        Some(shared)
    } else {
        info!("maker strategy disabled (set maker.enabled=true in config)");
        None
    };

    // --- Anomaly Detector ---
    let (anomaly_tx, mut anomaly_rx) = mpsc::unbounded_channel::<AnomalyEvent>();
    let anomaly_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();
    let anomaly: Option<Arc<Mutex<AnomalyDetector>>> = if config.anomaly.enabled {
        let detector = AnomalyDetector::new(
            store.clone(),
            &anomaly_markets,
            config.anomaly.clone(),
            anomaly_tx,
        );
        let shared = Arc::new(Mutex::new(detector));

        // Spawn periodic scan task
        let detector_for_scan = shared.clone();
        let scan_interval = Duration::from_millis(config.anomaly.scan_interval_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                interval.tick().await;
                detector_for_scan.lock().await.scan();
            }
        });

        info!(
            scan_interval_ms = config.anomaly.scan_interval_ms,
            window_secs = config.anomaly.window_secs,
            whale_multiplier = config.anomaly.whale_size_multiplier,
            volume_spike = config.anomaly.volume_spike_multiplier,
            "anomaly detector enabled"
        );
        Some(shared)
    } else {
        info!("anomaly detector disabled (set anomaly.enabled=true in config)");
        None
    };

    // --- Cross-Bookmaker Scanner ---
    let (crossbook_tx, mut crossbook_rx) = mpsc::unbounded_channel::<CrossbookEvent>();
    let crossbook_markets: Vec<TrackedMarket> = if crossbook_discovered.is_empty() {
        markets
            .iter()
            .filter_map(TrackedMarket::from_discovered)
            .collect()
    } else {
        crossbook_discovered
            .iter()
            .filter_map(TrackedMarket::from_discovered)
            .collect()
    };
    let crossbook: Option<Arc<Mutex<CrossbookScanner>>> = if config.crossbook.enabled
        && !config.crossbook.odds_api_key.is_empty()
    {
        let scanner = CrossbookScanner::new(
            config.crossbook.clone(),
            store.clone(),
            crossbook_markets,
            crossbook_tx,
        );
        let shared = Arc::new(Mutex::new(scanner));

        // Spawn periodic fetch task (every fetch_interval_secs)
        let scanner_for_fetch = shared.clone();
        let fetch_interval = Duration::from_secs(config.crossbook.fetch_interval_secs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(fetch_interval);
            loop {
                interval.tick().await;
                scanner_for_fetch.lock().await.fetch_odds().await;
            }
        });

        // Spawn periodic scan task (every scan_interval_ms)
        let scanner_for_scan = shared.clone();
        let scan_interval = Duration::from_millis(config.crossbook.scan_interval_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                interval.tick().await;
                scanner_for_scan.lock().await.scan();
            }
        });

        let est_daily = (86400 / config.crossbook.fetch_interval_secs as u32)
            * config.crossbook.sports_keys.len() as u32;
        let est_monthly = est_daily * 30;
        info!(
            fetch_interval_secs = config.crossbook.fetch_interval_secs,
            scan_interval_ms = config.crossbook.scan_interval_ms,
            sports = config.crossbook.sports_keys.len(),
            min_edge_pct = config.crossbook.min_edge_pct,
            monthly_budget = config.crossbook.monthly_budget,
            min_remaining = config.crossbook.min_remaining,
            est_daily_requests = est_daily,
            est_monthly_requests = est_monthly,
            "crossbook scanner enabled"
        );
        Some(shared)
    } else {
        if config.crossbook.enabled && config.crossbook.odds_api_key.is_empty() {
            warn!("crossbook.enabled=true but ODDS_API_KEY not set — disabled");
        } else {
            info!("crossbook scanner disabled (set crossbook.enabled=true + ODDS_API_KEY)");
        }
        None
    };

    // --- Split Arb Scanner (Strategy 1A) ---
    let (split_tx, mut split_rx) = mpsc::unbounded_channel::<SplitEvent>();
    let split_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();

    if config.split.enabled {
        let split_min_edge =
            Decimal::from_str(&config.split.min_net_edge.to_string()).unwrap_or(Decimal::ZERO);
        let mut split_scanner = SplitScanner::new(
            store.clone(),
            split_markets,
            split_tx,
            split_min_edge,
        );

        let split_interval = Duration::from_millis(config.split.scan_interval_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(split_interval);
            loop {
                interval.tick().await;
                split_scanner.scan_all();
            }
        });

        info!(
            scan_interval_ms = config.split.scan_interval_ms,
            min_edge = config.split.min_net_edge,
            execute = config.split.execute,
            "split arb scanner enabled"
        );
    } else {
        info!("split arb scanner disabled (set split.enabled=true in config)");
    }

    // --- Strategy Snapshot (shared between event loop and dashboard) ---
    // Created early so sniper/metrics/lifecycle setup can populate initial values.
    let strategy_snapshot = Arc::new(Mutex::new(dashboard::StrategySnapshot {
        split: dashboard::SplitSnapshot {
            enabled: config.split.enabled,
            ..Default::default()
        },
        sniper: dashboard::SniperSnapshot {
            enabled: config.sniper.enabled,
            ..Default::default()
        },
        lifecycle: dashboard::LifecycleSnapshot {
            enabled: config.lifecycle.enabled,
            ..Default::default()
        },
        metrics: dashboard::MetricsSnapshot::default(),
    }));

    // --- Resolution Sniper (Strategy 1B) ---
    let (sniper_tx, mut sniper_rx) = mpsc::unbounded_channel::<SniperEvent>();
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel::<sniper::ResolutionSignal>();

    let sniper_executor = SniperExecutor::new(store.clone(), api_client.clone(), config.sniper.execute);

    let sniper_engine: Option<Arc<Mutex<SniperEngine>>> = if config.sniper.enabled {
        let mut engine = SniperEngine::new(
            store.clone(),
            sniper_tx,
            config.sniper.min_confidence,
        );

        // Auto-map markets to resolution sources
        let sniper_markets: Vec<TrackedMarket> = markets
            .iter()
            .filter_map(TrackedMarket::from_discovered)
            .collect();
        let mappings = auto_map_markets(&sniper_markets);
        for mapping in &mappings {
            info!(
                cid = %mapping.condition_id,
                source = %mapping.source_type,
                question = %mapping.question,
                "sniper: mapped market"
            );
            engine.add_mapping(mapping.clone());
        }

        info!(
            mapped = mappings.len(),
            total = sniper_markets.len(),
            confidence = config.sniper.min_confidence,
            execute = config.sniper.execute,
            "resolution sniper enabled"
        );

        // Update snapshot with mapped market count
        strategy_snapshot.lock().await.sniper.mapped_markets = mappings.len();

        Some(Arc::new(Mutex::new(engine)))
    } else {
        info!("resolution sniper disabled (set sniper.enabled=true in config)");
        None
    };

    // Crypto threshold watcher (feeds into sniper)
    let crypto_watcher: Option<Arc<Mutex<CryptoThresholdWatcher>>> =
        if config.sniper.enabled && config.sniper.crypto_enabled {
            let watcher = CryptoThresholdWatcher::new();
            // Watches are auto-populated from sniper mappings with crypto source type
            let shared = Arc::new(Mutex::new(watcher));
            info!("crypto threshold watcher enabled for sniper");
            Some(shared)
        } else {
            None
        };

    // --- Lifecycle Manager (Strategy 1C) ---
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel::<LifecycleEvent>();

    let lifecycle: Option<Arc<Mutex<LifecycleManager>>> = if config.lifecycle.enabled {
        let stale_threshold = Duration::from_secs(config.lifecycle.stale_threshold_hours * 3600);
        let manager = LifecycleManager::new(store.clone(), lifecycle_tx, stale_threshold);
        let shared = Arc::new(Mutex::new(manager));

        let lc_for_tick = shared.clone();
        let tick_interval = Duration::from_secs(config.lifecycle.tick_interval_secs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            loop {
                interval.tick().await;
                lc_for_tick.lock().await.tick();
            }
        });

        info!(
            stale_hours = config.lifecycle.stale_threshold_hours,
            drip_batch = config.lifecycle.drip_sell_batch,
            drip_interval = config.lifecycle.drip_sell_interval_secs,
            "lifecycle manager enabled"
        );
        Some(shared)
    } else {
        info!("lifecycle manager disabled (set lifecycle.enabled=true in config)");
        None
    };

    // --- Metrics Tracker ---
    let metrics = Arc::new(Mutex::new(MetricsTracker::new(
        Decimal::from_str(&config.arb.max_total_exposure.to_string()).unwrap_or(Decimal::from(500)),
    )));

    // Metrics summary every 60 seconds (update dashboard snapshot) + full log every 5 minutes
    let metrics_for_summary = metrics.clone();
    let snapshot_for_metrics = strategy_snapshot.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let mut tick_count = 0u64;
        loop {
            interval.tick().await;
            tick_count += 1;
            let m = metrics_for_summary.lock().await;

            // Update dashboard snapshot every tick (60s)
            let m5 = m.window_metrics(Duration::from_secs(300));
            let m60 = m.window_metrics(Duration::from_secs(3600));
            {
                let mut snap = snapshot_for_metrics.lock().await;
                snap.metrics = dashboard::MetricsSnapshot {
                    trades_5m: m5.count,
                    profit_5m: m5.total_profit.to_string(),
                    avg_edge_5m: m5.avg_edge.to_string(),
                    avg_fill_rate_5m: format!("{:.1}%", m5.avg_fill_rate * 100.0),
                    avg_latency_ms_5m: format!("{:.1}ms", m5.avg_latency_ms),
                    trades_1h: m60.count,
                    profit_1h: m60.total_profit.to_string(),
                    utilisation_pct: format!("{:.1}%", m60.utilisation_pct),
                    capital_deployed: m60.capital_deployed.to_string(),
                    capital_available: m60.capital_available.to_string(),
                };
            }

            // Full log summary every 5 ticks (300s)
            if tick_count % 5 == 0 {
                m.log_summary();
            }
        }
    });

    // --- Dashboard (launched after all modules so it can reference them) ---
    // Dashboard now works without Valkey — strategy snapshots are always available.
    if config.dashboard.enabled {
        let dash_state = DashboardState {
            store: state_store.clone(),
            anomaly_detector: anomaly.clone(),
            crossbook_scanner: crossbook.clone(),
            strategy_snapshot: strategy_snapshot.clone(),
        };
        let bind = config.dashboard.bind.clone();
        if state_store.is_none() {
            info!("dashboard starting without Valkey — Valkey-backed data will be empty");
        }
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(dash_state, &bind).await {
                error!(error = %e, "dashboard server error");
            }
        });
    } else {
        info!("dashboard disabled (set dashboard.enabled=true in config)");
    }

    // --- Exchange Feeds (Binance + Coinbase) ---
    let (binance_tx, mut binance_rx) = mpsc::unbounded_channel::<BinanceEvent>();
    let (coinbase_tx, mut coinbase_rx) = mpsc::unbounded_channel::<CoinbaseEvent>();
    let (agg_tx, mut agg_rx) = mpsc::unbounded_channel::<AggregatorEvent>();

    let tracked_symbols = vec!["BTCUSD".to_string(), "ETHUSD".to_string()];
    let aggregator = Arc::new(PriceAggregator::new(
        agg_tx,
        tracked_symbols,
        Duration::from_secs(config.feeds.max_observation_age_secs),
    ));

    if config.feeds.enabled {
        info!("starting exchange price feeds");

        let binance_feed = BinanceFeed::new(binance_tx);
        binance_feed.subscribe(config.feeds.binance_symbols.clone());

        let coinbase_feed = CoinbaseFeed::new(coinbase_tx);
        coinbase_feed.subscribe(config.feeds.coinbase_products.clone());
    } else {
        info!("exchange feeds disabled (set feeds.enabled=true in config)");
    }

    // --- On-Chain Event Monitor (Polygon WebSocket) ---
    let (onchain_tx, mut onchain_rx) = mpsc::unbounded_channel::<OnChainSignal>();

    if config.onchain.enabled && !config.onchain.polygon_ws_url.is_empty() {
        // Build the market cache from Gamma API
        let market_cache = MarketCache::new(config.polymarket.gamma_url.clone());
        match market_cache.load_active_markets().await {
            Ok(count) => info!(markets = count, "on-chain market cache loaded"),
            Err(e) => warn!(error = %e, "failed to load market cache (will populate on demand)"),
        }

        let monitor = OnChainMonitor::new(
            config.onchain.clone(),
            onchain_tx.clone(),
            market_cache,
        );
        monitor.start();
        info!(
            ctf = %config.onchain.ctf_address,
            ctf_exchange = %config.onchain.ctf_exchange,
            neg_risk_exchange = %config.onchain.neg_risk_ctf_exchange,
            neg_risk_adapter = %config.onchain.neg_risk_adapter,
            max_latency_ms = config.onchain.max_latency_ms,
            "on-chain event monitor enabled"
        );
    } else {
        if config.onchain.enabled && config.onchain.polygon_ws_url.is_empty() {
            warn!("onchain.enabled=true but POLYGON_WS_URL not set — disabled");
        } else {
            info!("on-chain monitor disabled (set onchain.enabled=true + POLYGON_WS_URL)");
        }
    }

    // --- Periodic Tasks ---

    // Order book summary every 30s
    let store_for_summary = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let summaries = store_for_summary.summary();
            if !summaries.is_empty() {
                debug!("--- Order Book Summary ({} assets) ---", summaries.len());
                for s in &summaries {
                    debug!("{}", s);
                }
            }
        }
    });

    // Arb scanner on configured interval
    let scan_interval = Duration::from_millis(config.arb.scan_interval_ms);
    let arb_enabled = config.arb.enabled;
    tokio::spawn(async move {
        if !arb_enabled {
            info!("arb scanner disabled");
            return;
        }
        let mut interval = tokio::time::interval(scan_interval);
        loop {
            interval.tick().await;
            arb_scanner.scan_all();
        }
    });

    // Aggregator price snapshot every 10s
    let agg_for_snapshot = aggregator.clone();
    let feeds_enabled = config.feeds.enabled;
    tokio::spawn(async move {
        if !feeds_enabled {
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let snap = agg_for_snapshot.snapshot();
            for agg in &snap {
                debug!(
                    symbol = %agg.symbol,
                    price = %agg.price,
                    sources = agg.source_count,
                    spread = %agg.source_spread,
                    confidence = %agg.confidence,
                    "aggregated price"
                );
            }
        }
    });

    // Executor position cleanup every 60s
    if let Some(ref exec) = executor {
        let exec_cleanup = exec.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                exec_cleanup.cleanup_stale(Duration::from_secs(300)).await;
                let summary = exec_cleanup.exposure_summary().await;
                debug!("executor: {}", summary);
            }
        });
    }

    // --- WebSocket Connection Tracker ---
    let mut ws_expected: Vec<&str> = vec!["CLOB", "RTDS"];
    if config.feeds.enabled {
        ws_expected.push("Binance");
        ws_expected.push("Coinbase");
    }
    if config.onchain.enabled && !config.onchain.polygon_ws_url.is_empty() {
        ws_expected.push("Polygon");
    }
    let mut ws_connected: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let ws_total = ws_expected.len();
    info!(expected = ?ws_expected, "waiting for WebSocket connections...");

    // --- Main Event Loop ---
    info!("entering main event loop - press Ctrl+C to stop");

    loop {
        tokio::select! {
            Some(clob_event) = clob_rx.recv() => {
                match clob_event {
                    ClobEvent::Connected => {
                        ws_connected.insert("CLOB");
                        info!(
                            "{}/{} WebSockets up [CLOB connected] {:?}",
                            ws_connected.len(), ws_total, ws_connected
                        );
                    }
                    ClobEvent::Disconnected => {
                        ws_connected.remove("CLOB");
                        warn!("CLOB disconnected ({}/{} WebSockets up)", ws_connected.len(), ws_total);
                    }
                    ClobEvent::BookSnapshot { asset_id, bid_levels, ask_levels } => {
                        debug!(
                            asset = %asset_id[..12.min(asset_id.len())],
                            bids = bid_levels,
                            asks = ask_levels,
                            "book snapshot"
                        );
                    }
                    ClobEvent::PriceChange { asset_id, best_bid, best_ask } => {
                        debug!(
                            asset = %asset_id[..12.min(asset_id.len())],
                            bid = ?best_bid,
                            ask = ?best_ask,
                            "price change"
                        );
                    }
                    ClobEvent::LastTradePrice { asset_id, price } => {
                        debug!(
                            asset = %asset_id[..12.min(asset_id.len())],
                            price = %price,
                            "last trade"
                        );
                    }
                }
            }

            Some(rtds_event) = rtds_rx.recv() => {
                match rtds_event {
                    RtdsEvent::Connected => {
                        ws_connected.insert("RTDS");
                        info!(
                            "{}/{} WebSockets up [RTDS connected] {:?}",
                            ws_connected.len(), ws_total, ws_connected
                        );
                    }
                    RtdsEvent::Disconnected => {
                        ws_connected.remove("RTDS");
                        warn!("RTDS disconnected ({}/{} WebSockets up)", ws_connected.len(), ws_total);
                    }
                    RtdsEvent::Trade(t) => {
                        debug!(
                            asset = %t.asset_id,
                            price = %t.price,
                            size = %t.size,
                            side = %t.side,
                            "RTDS trade"
                        );
                        // Feed trade to anomaly detector
                        if let Some(ref det) = anomaly {
                            det.lock().await.record_trade(&t.asset_id, &t.price, &t.size, &t.side);
                        }
                        // Feed trade activity to maker + Valkey
                        if let Some(ref m) = maker {
                            let mut mkr = m.lock().await;
                            mkr.record_activity(&t.asset_id);

                            // Paper fill simulation: check if this trade would
                            // have filled one of our resting paper bids.
                            if let (Ok(price), Ok(size)) = (
                                Decimal::from_str(&t.price),
                                Decimal::from_str(&t.size),
                            ) {
                                mkr.check_paper_fill(&t.asset_id, price, size, &t.side).await;
                            }
                        }
                        if let Some(ref ss) = state_store {
                            let _ = ss.lock().await.record_activity(&t.asset_id).await;
                        }
                    }
                    RtdsEvent::OrderMatch(m) => {
                        debug!(
                            asset = %m.asset_id,
                            price = %m.price,
                            size = %m.size,
                            "RTDS match"
                        );
                        if let Some(ref mkr) = maker {
                            mkr.lock().await.record_activity(&m.asset_id);
                        }
                        if let Some(ref ss) = state_store {
                            let _ = ss.lock().await.record_activity(&m.asset_id).await;
                        }
                    }
                    RtdsEvent::CryptoPrice { symbol, price } => {
                        debug!(symbol = %symbol, price = %price, "crypto price");
                        // Feed RTDS crypto prices to sniper threshold watcher
                        if let Some(ref watcher) = crypto_watcher {
                            if let Ok(dec_price) = Decimal::from_str(&price) {
                                let signals = watcher.lock().await.check_price(&symbol, dec_price);
                                for sig in signals {
                                    let _ = signal_tx.send(sig);
                                }
                            }
                        }
                    }
                    RtdsEvent::RawEvent(_) => {}
                }
            }

            Some(arb_event) = arb_rx.recv() => {
                match arb_event {
                    ArbEvent::OpportunityDetected(opp) => {
                        info!(
                            market = %opp.question,
                            ask_sum = %opp.ask_sum,
                            gross = %opp.gross_edge,
                            net = %opp.net_edge,
                            fillable = %opp.max_fillable_size,
                            "ARB OPPORTUNITY"
                        );

                        let mut executed_live = false;
                        if let Some(ref exec) = executor {
                            match exec.try_execute(&opp).await {
                                Ok(true) => {
                                    info!(market = %opp.question, "arb executed");
                                    executed_live = true;
                                }
                                Ok(false) => {}
                                Err(e) => error!(error = %e, "arb execution error"),
                            }
                        }

                        // Paper trade: simulate the fill and persist to Valkey
                        // so the dashboard has something to show.
                        if !executed_live {
                            if let Some(ref ss) = state_store {
                                let mut vk = ss.lock().await;
                                let now = chrono::Utc::now().to_rfc3339();
                                let paper_id = format!("paper-{}", chrono::Utc::now().timestamp_millis());
                                let size = opp.max_fillable_size.min(
                                    Decimal::from(100) / opp.ask_sum
                                );

                                // Record simulated position
                                let legs: Vec<PositionLeg> = opp.legs.iter().map(|l| PositionLeg {
                                    label: l.label.clone(),
                                    token_id: l.token_id.clone(),
                                    inventory: size.to_string(),
                                    avg_price: l.best_ask.to_string(),
                                }).collect();

                                let pos = PositionRecord {
                                    condition_id: opp.condition_id.clone(),
                                    question: opp.question.clone(),
                                    legs,
                                    bid_sum: opp.ask_sum.to_string(),
                                    edge: opp.net_edge.to_string(),
                                    pending_fill: false,
                                    updated_at: now.clone(),
                                };
                                let _ = vk.set_position(&pos).await;

                                // Record simulated fills for each leg
                                for leg in &opp.legs {
                                    let fill = FillRecord {
                                        order_id: format!("{}-{}", paper_id, &leg.token_id[..8.min(leg.token_id.len())]),
                                        condition_id: opp.condition_id.clone(),
                                        token_id: leg.token_id.clone(),
                                        label: leg.label.clone(),
                                        side: "BUY".to_string(),
                                        price: leg.best_ask.to_string(),
                                        size: size.to_string(),
                                        timestamp: now.clone(),
                                        strategy: "arb-paper".to_string(),
                                    };
                                    let _ = vk.record_fill(&fill).await;
                                }

                                // Record simulated PnL (net_edge * size)
                                let paper_pnl = opp.net_edge * size;
                                let _ = vk.add_pnl(paper_pnl).await;

                                info!(
                                    market = %opp.question,
                                    size = %size,
                                    pnl = %paper_pnl,
                                    "PAPER TRADE simulated"
                                );
                            }
                        }
                    }
                    ArbEvent::OpportunityGone { condition_id } => {
                        debug!(condition_id = %condition_id, "arb opportunity gone");
                    }
                    ArbEvent::ScanComplete { markets_scanned, opportunities } => {
                        if opportunities > 0 {
                            info!(scanned = markets_scanned, opps = opportunities, "arb scan");
                        }
                    }
                    ArbEvent::ScanDiagnostics {
                        total_markets,
                        markets_with_books,
                        tightest_ask_sum,
                        tightest_market,
                        near_misses,
                    } => {
                        info!(
                            total = total_markets,
                            with_books = markets_with_books,
                            tightest_sum = ?tightest_ask_sum,
                            tightest = ?tightest_market.as_deref().map(|s| &s[..s.len().min(50)]),
                            near_misses = near_misses,
                            "ARB SCAN diagnostics"
                        );
                    }
                }
            }

            Some(binance_event) = binance_rx.recv() => {
                aggregator.handle_binance(&binance_event);
                if let BinanceEvent::Connected = binance_event {
                    ws_connected.insert("Binance");
                    info!(
                        "{}/{} WebSockets up [Binance connected] {:?}",
                        ws_connected.len(), ws_total, ws_connected
                    );
                }
            }

            Some(coinbase_event) = coinbase_rx.recv() => {
                aggregator.handle_coinbase(&coinbase_event);
                if let CoinbaseEvent::Connected = coinbase_event {
                    ws_connected.insert("Coinbase");
                    info!(
                        "{}/{} WebSockets up [Coinbase connected] {:?}",
                        ws_connected.len(), ws_total, ws_connected
                    );
                }
            }

            Some(agg_event) = agg_rx.recv() => {
                if let AggregatorEvent::PriceUpdate(ref agg) = agg_event {
                    debug!(
                        symbol = %agg.symbol,
                        price = %agg.price,
                        sources = agg.source_count,
                        confidence = %agg.confidence,
                        "price signal"
                    );

                    // Feed price to crypto threshold watcher for sniper
                    if let Some(ref watcher) = crypto_watcher {
                        let signals = watcher.lock().await.check_price(&agg.symbol, agg.price);
                        for sig in signals {
                            let _ = signal_tx.send(sig);
                        }
                    }
                }
            }

            Some(maker_event) = maker_rx.recv() => {
                // Persist maker events to Valkey
                if let Some(ref ss) = state_store {
                    let mut vk = ss.lock().await;
                    match &maker_event {
                        MakerEvent::QuotesPosted { condition_id, question, bid_sum, edge } => {
                            let pos = PositionRecord {
                                condition_id: condition_id.clone(),
                                question: question.clone(),
                                legs: vec![], // updated when fills come in
                                bid_sum: bid_sum.to_string(),
                                edge: edge.to_string(),
                                pending_fill: false,
                                updated_at: chrono::Utc::now().to_rfc3339(),
                            };
                            let _ = vk.set_position(&pos).await;
                        }
                        MakerEvent::LegFilled { order_id, condition_id, token_id, label, price, size } => {
                            let fill = FillRecord {
                                order_id: order_id.clone(),
                                condition_id: condition_id.clone(),
                                token_id: token_id.clone(),
                                label: label.clone(),
                                side: "BUY".to_string(),
                                price: price.to_string(),
                                size: size.to_string(),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                strategy: "maker".to_string(),
                            };
                            let _ = vk.record_fill(&fill).await;

                            // Update position with leg inventory so dashboard shows fills
                            if let Ok(Some(mut pos)) = vk.get_position(condition_id).await {
                                // Upsert the leg
                                if let Some(leg) = pos.legs.iter_mut().find(|l| l.token_id == *token_id) {
                                    let prev: Decimal = Decimal::from_str(&leg.inventory).unwrap_or(Decimal::ZERO);
                                    leg.inventory = (prev + size).to_string();
                                    leg.avg_price = price.to_string();
                                } else {
                                    pos.legs.push(PositionLeg {
                                        label: label.clone(),
                                        token_id: token_id.clone(),
                                        inventory: size.to_string(),
                                        avg_price: price.to_string(),
                                    });
                                }
                                pos.pending_fill = true;
                                pos.updated_at = chrono::Utc::now().to_rfc3339();
                                let _ = vk.set_position(&pos).await;
                            }
                        }
                        MakerEvent::RoundTripComplete { condition_id, profit } => {
                            let _ = vk.add_pnl(*profit).await;

                            // Clear pending_fill and zero out legs on completed round-trip
                            if let Ok(Some(mut pos)) = vk.get_position(condition_id).await {
                                pos.pending_fill = false;
                                for leg in &mut pos.legs {
                                    leg.inventory = "0".to_string();
                                }
                                pos.updated_at = chrono::Utc::now().to_rfc3339();
                                let _ = vk.set_position(&pos).await;
                            }
                        }
                        MakerEvent::FillTimeout { condition_id, .. } => {
                            // Mark position as unwinding
                            if let Ok(Some(mut pos)) = vk.get_position(condition_id).await {
                                pos.pending_fill = false;
                                pos.edge = "UNWOUND".to_string();
                                pos.updated_at = chrono::Utc::now().to_rfc3339();
                                let _ = vk.set_position(&pos).await;
                            }
                        }
                        _ => {}
                    }
                }

                // Log maker events
                match maker_event {
                    MakerEvent::QuotesPosted { question, bid_sum, edge, .. } => {
                        info!(
                            market = %question,
                            bid_sum = %bid_sum,
                            edge = %edge,
                            "maker quotes posted"
                        );
                    }
                    MakerEvent::QuotesUpdated { condition_id, bid_sum } => {
                        debug!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            bid_sum = %bid_sum,
                            "maker quotes updated"
                        );
                    }
                    MakerEvent::QuotesCancelled { condition_id, reason } => {
                        warn!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            reason = %reason,
                            "maker quotes cancelled"
                        );
                    }
                    MakerEvent::LegFilled { condition_id, label, price, size, .. } => {
                        info!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            label = %label,
                            price = %price,
                            size = %size,
                            "maker leg filled"
                        );
                    }
                    MakerEvent::InventoryAlert { condition_id, imbalance } => {
                        warn!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            imbalance = %imbalance,
                            "maker inventory alert"
                        );
                    }
                    MakerEvent::RoundTripComplete { condition_id, profit } => {
                        info!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            profit = %profit,
                            "ROUND-TRIP COMPLETE"
                        );
                    }
                    MakerEvent::AggressiveReprice { condition_id, token_id, old_price, new_price } => {
                        warn!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            token = %token_id[..12.min(token_id.len())],
                            old = %old_price,
                            new = %new_price,
                            "aggressive reprice — chasing fill"
                        );
                    }
                    MakerEvent::FillTimeout { condition_id, filled_token, unfilled_tokens } => {
                        error!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            filled = %filled_token[..12.min(filled_token.len())],
                            unfilled = ?unfilled_tokens,
                            "FILL TIMEOUT — unwinding position"
                        );
                    }
                    MakerEvent::Unwinding { condition_id, token_id, size } => {
                        warn!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            token = %token_id[..12.min(token_id.len())],
                            size = %size,
                            "unwinding filled leg"
                        );
                    }
                    MakerEvent::MarketSkipped { condition_id, reason } => {
                        debug!(
                            condition = %condition_id[..12.min(condition_id.len())],
                            reason = %reason,
                            "market skipped — insufficient activity"
                        );
                    }
                }
            }

            Some(anomaly_event) = anomaly_rx.recv() => {
                match anomaly_event {
                    AnomalyEvent::Detected(a) => {
                        info!(
                            kind = %a.kind,
                            severity = %a.severity,
                            market = %a.market_question[..a.market_question.len().min(50)],
                            price = ?a.price,
                            size = ?a.size,
                            "anomaly alert"
                        );
                    }
                    AnomalyEvent::ScanComplete { assets_scanned, anomalies_found } => {
                        if anomalies_found > 0 {
                            info!(
                                scanned = assets_scanned,
                                found = anomalies_found,
                                "anomaly scan"
                            );
                        }
                    }
                }
            }

            Some(crossbook_event) = crossbook_rx.recv() => {
                match crossbook_event {
                    CrossbookEvent::OpportunityDetected(opp) => {
                        info!(
                            event = %opp.event,
                            arb_pct = format!("{:.2}%", opp.arb_return_pct),
                            legs = opp.legs.len(),
                            poly = ?opp.poly_question.as_deref().map(|s| &s[..s.len().min(40)]),
                            "CROSSBOOK ARB"
                        );
                    }
                    CrossbookEvent::FetchComplete { matches_fetched, poly_links } => {
                        debug!(
                            matches = matches_fetched,
                            poly_links = poly_links,
                            "crossbook odds fetched"
                        );
                    }
                    CrossbookEvent::ScanComplete { events_scanned, opportunities, best_edge_pct } => {
                        if opportunities > 0 || best_edge_pct.map_or(false, |e| e > -5.0) {
                            info!(
                                scanned = events_scanned,
                                opps = opportunities,
                                best_edge = ?best_edge_pct.map(|e| format!("{:.2}%", e)),
                                "crossbook scan"
                            );
                        }
                    }
                    _ => {}
                }
            }

            Some(split_event) = split_rx.recv() => {
                match split_event {
                    SplitEvent::OpportunityDetected(ref opp) => {
                        info!(
                            market = %opp.question,
                            dir = %opp.direction,
                            outcomes = opp.num_outcomes,
                            sum = %opp.price_sum,
                            gross = %opp.gross_edge,
                            net_tob = %opp.net_edge_tob,
                            fillable = %opp.min_fillable_shares,
                            tiers = opp.tier_analyses.len(),
                            priority = opp.high_priority,
                            "SPLIT ARB OPPORTUNITY"
                        );
                        // Update dashboard snapshot
                        let best_tier = opp.tier_analyses.iter()
                            .filter(|t| t.expected_profit_usd > Decimal::ZERO)
                            .last()
                            .map(|t| format!("${}", t.expected_profit_usd));
                        let entry = dashboard::SplitOppEntry {
                            question: opp.question.clone(),
                            direction: opp.direction.to_string(),
                            num_outcomes: opp.num_outcomes,
                            price_sum: opp.price_sum.to_string(),
                            gross_edge: opp.gross_edge.to_string(),
                            net_edge_tob: opp.net_edge_tob.to_string(),
                            min_fillable: opp.min_fillable_shares.to_string(),
                            high_priority: opp.high_priority,
                            best_tier_profit: best_tier,
                        };
                        let mut snap = strategy_snapshot.lock().await;
                        // Replace existing or push new
                        snap.split.active_opps.retain(|e| !(e.question == entry.question && e.direction == entry.direction));
                        snap.split.active_opps.push(entry);
                    }
                    SplitEvent::OpportunityGone { ref condition_id, direction } => {
                        debug!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            dir = %direction,
                            "split opp gone"
                        );
                        let dir_str = direction.to_string();
                        let mut snap = strategy_snapshot.lock().await;
                        snap.split.active_opps.retain(|e| e.direction != dir_str);
                    }
                    SplitEvent::ScanComplete { markets_scanned, buy_merge_opps, split_sell_opps } => {
                        if buy_merge_opps > 0 || split_sell_opps > 0 {
                            info!(
                                scanned = markets_scanned,
                                buy_merge = buy_merge_opps,
                                split_sell = split_sell_opps,
                                "split scan"
                            );
                        }
                        strategy_snapshot.lock().await.split.total_scans += 1;
                    }
                    SplitEvent::ScanDiagnostics { total_markets, multi_outcome_markets, tightest_buy_sum, tightest_sell_sum, near_misses } => {
                        info!(
                            total = total_markets,
                            multi = multi_outcome_markets,
                            buy_tightest = ?tightest_buy_sum,
                            sell_tightest = ?tightest_sell_sum,
                            near_misses = near_misses,
                            "SPLIT SCAN diagnostics"
                        );
                        strategy_snapshot.lock().await.split.last_diagnostics = Some(dashboard::SplitDiagnostics {
                            total_markets,
                            multi_outcome: multi_outcome_markets,
                            tightest_buy: tightest_buy_sum.map(|d| d.to_string()),
                            tightest_sell: tightest_sell_sum.map(|d| d.to_string()),
                            near_misses,
                        });
                    }
                }
            }

            Some(sniper_event) = sniper_rx.recv() => {
                match sniper_event {
                    SniperEvent::SignalReceived { ref condition_id, ref source, confidence } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            source = %source,
                            confidence = confidence,
                            "SNIPER signal received"
                        );
                        let now = chrono::Utc::now().format("%H:%M:%S").to_string();
                        strategy_snapshot.lock().await.push_sniper_signal(dashboard::SniperSignalEntry {
                            condition_id: condition_id.clone(),
                            source: source.clone(),
                            confidence,
                            action: "received".to_string(),
                            time: now,
                        });
                    }
                    SniperEvent::SnipeDispatched { ref condition_id, ref question, num_orders } => {
                        info!(
                            market = %question,
                            orders = num_orders,
                            "SNIPER dispatched"
                        );
                        let now = chrono::Utc::now().format("%H:%M:%S").to_string();
                        let mut snap = strategy_snapshot.lock().await;
                        snap.sniper.sniped_count += 1;
                        snap.push_sniper_signal(dashboard::SniperSignalEntry {
                            condition_id: condition_id.clone(),
                            source: format!("{} orders", num_orders),
                            confidence: 1.0,
                            action: "dispatched".to_string(),
                            time: now,
                        });
                    }
                    SniperEvent::OnChainResolution { ref condition_id, ref winning_outcome } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            winner = %winning_outcome,
                            "on-chain resolution"
                        );
                        let now = chrono::Utc::now().format("%H:%M:%S").to_string();
                        strategy_snapshot.lock().await.push_sniper_signal(dashboard::SniperSignalEntry {
                            condition_id: condition_id.clone(),
                            source: format!("on-chain → {}", winning_outcome),
                            confidence: 1.0,
                            action: "on_chain".to_string(),
                            time: now,
                        });
                        // Notify lifecycle manager
                        if let Some(ref lc) = lifecycle {
                            lc.lock().await.on_resolution(condition_id, winning_outcome);
                            lc.lock().await.on_chain_resolution_confirmed(condition_id);
                        }
                    }
                    SniperEvent::SignalIgnored { ref condition_id, confidence, ref reason } => {
                        debug!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            confidence = confidence,
                            reason = %reason,
                            "sniper signal ignored"
                        );
                        let now = chrono::Utc::now().format("%H:%M:%S").to_string();
                        strategy_snapshot.lock().await.push_sniper_signal(dashboard::SniperSignalEntry {
                            condition_id: condition_id.clone(),
                            source: reason.clone(),
                            confidence,
                            action: "ignored".to_string(),
                            time: now,
                        });
                    }
                }
            }

            Some(signal) = signal_rx.recv() => {
                // Resolution signal from an external source — feed to sniper engine
                if let Some(ref engine) = sniper_engine {
                    let action = engine.lock().await.process_signal(signal);
                    if let Some(action) = action {
                        let profit = sniper_executor.execute(&action).await;
                        // Record in metrics
                        metrics.lock().await.record_trade(metrics::TradeRecord {
                            condition_id: action.condition_id.clone(),
                            strategy: "snipe".to_string(),
                            direction: "buy_winner".to_string(),
                            gross_edge: profit,
                            fees: Decimal::ZERO,
                            net_edge: profit,
                            size_usd: profit,
                            profit_usd: profit,
                            fill_rate: 1.0, // Paper mode assumes full fill
                            recorded_at: std::time::Instant::now(),
                        });
                    }
                }
            }

            Some(lc_event) = lifecycle_rx.recv() => {
                let now = chrono::Utc::now().format("%H:%M:%S").to_string();
                match lc_event {
                    LifecycleEvent::PositionTracked { ref condition_id, strategy, legs, cost_basis } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            strategy = %strategy,
                            legs = legs,
                            cost = %cost_basis,
                            "lifecycle: position tracked"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "tracked".to_string(),
                            detail: format!("{} ({} legs, cost ${})", strategy, legs, cost_basis),
                            time: now,
                        });
                    }
                    LifecycleEvent::StateChanged { ref condition_id, from, to } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            from = %from,
                            to = %to,
                            "lifecycle: state change"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "state_change".to_string(),
                            detail: format!("{} → {}", from, to),
                            time: now,
                        });
                    }
                    LifecycleEvent::RedemptionQueued { ref condition_id, shares, expected_payout, .. } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            shares = %shares,
                            payout = %expected_payout,
                            "lifecycle: PAPER redemption queued"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "redemption".to_string(),
                            detail: format!("{} shares → ${} payout", shares, expected_payout),
                            time: now,
                        });
                    }
                    LifecycleEvent::DripSellQueued { ref condition_id, shares, target_price, .. } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            shares = %shares,
                            target = %target_price,
                            "lifecycle: PAPER drip sell"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "drip_sell".to_string(),
                            detail: format!("{} shares @ ${}", shares, target_price),
                            time: now,
                        });
                    }
                    LifecycleEvent::StalePositionSell { ref condition_id, age_hours, discount_pct } => {
                        warn!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            age_h = format!("{:.1}", age_hours),
                            discount = format!("{:.1}%", discount_pct),
                            "lifecycle: stale position"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "stale".to_string(),
                            detail: format!("{:.1}h old, selling at {:.1}% discount", age_hours, discount_pct),
                            time: now,
                        });
                    }
                    LifecycleEvent::PositionClosed { ref condition_id, realized_pnl } => {
                        info!(
                            cid = %condition_id[..12.min(condition_id.len())],
                            pnl = %realized_pnl,
                            "lifecycle: position closed"
                        );
                        strategy_snapshot.lock().await.push_lifecycle_event(dashboard::LifecycleEventEntry {
                            condition_id: condition_id.clone(),
                            event_type: "closed".to_string(),
                            detail: format!("PnL: ${}", realized_pnl),
                            time: now,
                        });
                    }
                    LifecycleEvent::PortfolioSummary { total_positions, active, exiting, redeemable, stale, total_cost_basis, total_mark_value, unrealized_pnl } => {
                        if total_positions > 0 {
                            info!(
                                positions = total_positions,
                                active = active,
                                exiting = exiting,
                                redeemable = redeemable,
                                stale = stale,
                                cost = %total_cost_basis,
                                mark = %total_mark_value,
                                unrealized = %unrealized_pnl,
                                "PORTFOLIO SUMMARY"
                            );
                        }
                        // Update lifecycle snapshot with portfolio summary
                        let mut snap = strategy_snapshot.lock().await;
                        snap.lifecycle.total_positions = total_positions;
                        snap.lifecycle.active = active;
                        snap.lifecycle.exiting = exiting;
                        snap.lifecycle.redeemable = redeemable;
                        snap.lifecycle.stale = stale;
                        snap.lifecycle.total_cost_basis = total_cost_basis.to_string();
                        snap.lifecycle.total_mark_value = total_mark_value.to_string();
                        snap.lifecycle.unrealized_pnl = unrealized_pnl.to_string();
                    }
                }
            }

            Some(onchain_event) = onchain_rx.recv() => {
                match onchain_event {
                    OnChainSignal::Connected => {
                        ws_connected.insert("Polygon");
                        info!(
                            "{}/{} WebSockets up [Polygon connected] {:?}",
                            ws_connected.len(), ws_total, ws_connected
                        );
                    }
                    OnChainSignal::Disconnected { reason } => {
                        ws_connected.remove("Polygon");
                        warn!(reason = %reason, "Polygon disconnected ({}/{} WebSockets up)", ws_connected.len(), ws_total);
                    }
                    OnChainSignal::GapRecovered { from_block, to_block, events_found } => {
                        info!(
                            from = from_block,
                            to = to_block,
                            events = events_found,
                            "on-chain gap recovery complete"
                        );
                    }
                    OnChainSignal::NewMarket {
                        condition_id,
                        outcome_count,
                        is_multi_outcome,
                        question_id,
                        block_number,
                        ..
                    } => {
                        info!(
                            condition_id = %condition_id,
                            question_id = %question_id,
                            outcomes = outcome_count,
                            multi = is_multi_outcome,
                            block = block_number,
                            "ON-CHAIN: new market detected"
                        );
                        if is_multi_outcome {
                            info!(
                                condition_id = %condition_id,
                                outcomes = outcome_count,
                                "SPLIT ARB TARGET: multi-outcome market created on-chain"
                            );
                        }
                    }
                    OnChainSignal::ResolutionProposed {
                        question_id,
                        proposed_price,
                        proposer,
                        block_number,
                        ..
                    } => {
                        let outcome = if proposed_price > 0 { "YES" } else { "NO" };
                        warn!(
                            question_id = %question_id,
                            proposed = outcome,
                            proposer = %proposer,
                            block = block_number,
                            "ON-CHAIN: RESOLUTION PROPOSED — sniping window open"
                        );
                        // Feed to sniper as on-chain resolution signal
                        if let Some(ref engine) = sniper_engine {
                            let qid_str = format!("{:x}", question_id);
                            let action = engine.lock().await.on_chain_resolution(
                                &qid_str,
                                outcome,
                            );
                            if let Some(action) = action {
                                let profit = sniper_executor.execute(&action).await;
                                metrics.lock().await.record_trade(metrics::TradeRecord {
                                    condition_id: action.condition_id.clone(),
                                    strategy: "snipe-onchain".to_string(),
                                    direction: "buy_winner".to_string(),
                                    gross_edge: profit,
                                    fees: Decimal::ZERO,
                                    net_edge: profit,
                                    size_usd: profit,
                                    profit_usd: profit,
                                    fill_rate: 1.0,
                                    recorded_at: std::time::Instant::now(),
                                });
                            }
                        }
                    }
                    OnChainSignal::ResolutionDisputed {
                        question_id,
                        block_number,
                        ..
                    } => {
                        error!(
                            question_id = %question_id,
                            block = block_number,
                            "ON-CHAIN: RESOLUTION DISPUTED — cancel any snipe orders!"
                        );
                    }
                    OnChainSignal::ResolutionFinalised {
                        condition_id,
                        question_id,
                        payout_numerators,
                        block_number,
                        ..
                    } => {
                        // Determine winning outcome from payouts
                        let winner_idx = payout_numerators
                            .iter()
                            .position(|p| *p > alloy::primitives::U256::ZERO)
                            .unwrap_or(0);
                        info!(
                            condition_id = %condition_id,
                            question_id = %question_id,
                            winner_index = winner_idx,
                            payouts = ?payout_numerators,
                            block = block_number,
                            "ON-CHAIN: resolution finalised — trigger redemption"
                        );
                        // Notify lifecycle manager
                        if let Some(ref lc) = lifecycle {
                            let cid_str = format!("{:x}", condition_id);
                            let winner_label = if winner_idx == 0 { "Yes" } else { "No" };
                            lc.lock().await.on_resolution(&cid_str, winner_label);
                            lc.lock().await.on_chain_resolution_confirmed(&cid_str);
                        }
                    }
                    OnChainSignal::TokenRegistered {
                        token_id,
                        exchange,
                        block_number,
                        ..
                    } => {
                        debug!(
                            token_id = %token_id,
                            exchange = %exchange,
                            block = block_number,
                            "ON-CHAIN: token registered on exchange"
                        );
                    }
                    OnChainSignal::PositionSplit {
                        condition_id,
                        collateral_amount,
                        block_number,
                        ..
                    } => {
                        debug!(
                            condition_id = %condition_id,
                            amount = %collateral_amount,
                            block = block_number,
                            "ON-CHAIN: position split (neg risk)"
                        );
                    }
                    OnChainSignal::PositionsMerged {
                        condition_id,
                        merge_amount,
                        block_number,
                        ..
                    } => {
                        debug!(
                            condition_id = %condition_id,
                            amount = %merge_amount,
                            block = block_number,
                            "ON-CHAIN: positions merged (neg risk)"
                        );
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                info!("shutting down...");
                if let Some(ref exec) = executor {
                    if exec.is_enabled() {
                        info!("cancelling all arb orders...");
                        if let Err(e) = exec.cancel_all().await {
                            error!(error = %e, "failed to cancel arb orders on shutdown");
                        }
                    }
                }
                if let Some(ref m) = maker {
                    info!("cancelling all maker orders...");
                    if let Err(e) = m.lock().await.cancel_all().await {
                        error!(error = %e, "failed to cancel maker orders on shutdown");
                    }
                }
                break;
            }
        }
    }

    Ok(())
}
