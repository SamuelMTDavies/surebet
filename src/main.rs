mod anomaly;
mod arb;
mod crossbook;
mod auth;
mod config;
mod dashboard;
mod feeds;
mod maker;
mod market;
mod orderbook;
mod store;
mod ws;

use crate::anomaly::{AnomalyDetector, AnomalyEvent};
use crate::arb::executor::{ArbExecutor, RiskLimits};
use crate::crossbook::{CrossbookEvent, CrossbookScanner};
use crate::maker::{MakerConfig, MakerEvent, MakerStrategy};
use crate::arb::{ArbEvent, ArbScanner, TrackedMarket};
use crate::auth::{ClobApiClient, L2Credentials};
use crate::config::Config;
use crate::dashboard::DashboardState;
use crate::feeds::aggregator::{AggregatorEvent, PriceAggregator};
use crate::feeds::binance::{BinanceEvent, BinanceFeed};
use crate::feeds::coinbase::{CoinbaseEvent, CoinbaseFeed};
use crate::market::{classify_edge_profile, DiscoveredMarket, MarketDiscovery};
use crate::orderbook::OrderBookStore;
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

    let (asset_ids, markets) = discovery.discover_asset_ids().await?;

    if asset_ids.is_empty() {
        error!("no markets found matching filters, exiting");
        return Ok(());
    }

    // Build tracked markets for arb scanner
    let tracked_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();

    info!("--- Market Edge Profiles ({} markets) ---", markets.len());
    for m in &markets {
        let profile = classify_edge_profile(m);
        info!(
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
    let crossbook_markets: Vec<TrackedMarket> = markets
        .iter()
        .filter_map(TrackedMarket::from_discovered)
        .collect();
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

    // --- Dashboard (launched after all modules so it can reference them) ---
    if config.dashboard.enabled {
        if let Some(ref store_ref) = state_store {
            let dash_state = DashboardState {
                store: store_ref.clone(),
                anomaly_detector: anomaly.clone(),
                crossbook_scanner: crossbook.clone(),
            };
            let bind = config.dashboard.bind.clone();
            tokio::spawn(async move {
                if let Err(e) = dashboard::serve(dash_state, &bind).await {
                    error!(error = %e, "dashboard server error");
                }
            });
        } else {
            warn!("dashboard enabled but Valkey not available — dashboard disabled");
        }
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

    // --- Periodic Tasks ---

    // Order book summary every 30s
    let store_for_summary = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let summaries = store_for_summary.summary();
            if !summaries.is_empty() {
                info!("--- Order Book Summary ({} assets) ---", summaries.len());
                for s in &summaries {
                    info!("{}", s);
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
                info!(
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
                info!("executor: {}", summary);
            }
        });
    }

    // --- Main Event Loop ---
    info!("entering main event loop - press Ctrl+C to stop");

    loop {
        tokio::select! {
            Some(clob_event) = clob_rx.recv() => {
                match clob_event {
                    ClobEvent::Connected => info!("CLOB connected"),
                    ClobEvent::Disconnected => warn!("CLOB disconnected"),
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
                    RtdsEvent::Connected => info!("RTDS connected"),
                    RtdsEvent::Disconnected => warn!("RTDS disconnected"),
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
                        info!(condition_id = %condition_id, "arb opportunity gone");
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
                    info!("Binance feed connected");
                }
            }

            Some(coinbase_event) = coinbase_rx.recv() => {
                aggregator.handle_coinbase(&coinbase_event);
                if let CoinbaseEvent::Connected = coinbase_event {
                    info!("Coinbase feed connected");
                }
            }

            Some(agg_event) = agg_rx.recv() => {
                if let AggregatorEvent::PriceUpdate(agg) = agg_event {
                    info!(
                        symbol = %agg.symbol,
                        price = %agg.price,
                        sources = agg.source_count,
                        confidence = %agg.confidence,
                        "price signal"
                    );
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
                        info!(
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
                        info!(
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
