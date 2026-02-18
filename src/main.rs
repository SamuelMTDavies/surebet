mod crossbook;
mod auth;
mod config;
mod dashboard;
mod market;
mod metrics;
mod onchain;
mod orderbook;
mod sniper;
mod store;
mod ws;

use crate::crossbook::{CrossbookEvent, CrossbookScanner};
use crate::metrics::MetricsTracker;
use crate::market::TrackedMarket;
use crate::auth::{ClobApiClient, L2Credentials};
use crate::config::Config;
use crate::dashboard::DashboardState;
use crate::market::{classify_edge_profile, MarketDiscovery};
use crate::onchain::{MarketCache, OnChainMonitor, OnChainSignal};
use crate::orderbook::OrderBookStore;
use crate::sniper::{SniperEngine, SniperEvent, auto_map_markets};
use crate::sniper::executor::SniperExecutor;
use crate::sniper::sources::CryptoThresholdWatcher;
use crate::store::StateStore;
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
    let activity_ttl = Duration::from_secs(300u64);
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

    // --- Strategy Snapshot (shared between event loop and dashboard) ---
    // Created early so sniper/metrics setup can populate initial values.
    let strategy_snapshot = Arc::new(Mutex::new(dashboard::StrategySnapshot {
        sniper: dashboard::SniperSnapshot {
            enabled: config.sniper.enabled,
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

    // --- Metrics Tracker ---
    let metrics = Arc::new(Mutex::new(MetricsTracker::new(
        Decimal::from(500),
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

    // --- On-Chain Event Monitor (Polygon WebSocket) ---
    let (onchain_tx, mut onchain_rx) = mpsc::unbounded_channel::<OnChainSignal>();
    let mut event_market_cache: Option<MarketCache> = None;

    if config.onchain.enabled && !config.onchain.polygon_ws_url.is_empty() {
        // Build the market cache from Gamma API
        let market_cache = MarketCache::new(config.polymarket.gamma_url.clone());
        match market_cache.load_active_markets().await {
            Ok(count) => info!(markets = count, "on-chain market cache loaded"),
            Err(e) => warn!(error = %e, "failed to load market cache (will populate on demand)"),
        }

        // Keep a clone for the event handler to map question_id → market tokens
        event_market_cache = Some(market_cache.clone());

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

    // --- WebSocket Connection Tracker ---
    let mut ws_expected: Vec<&str> = vec!["CLOB", "RTDS"];
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
                    let signal_received = std::time::Instant::now();
                    let action = engine.lock().await.process_signal(signal);
                    if let Some(action) = action {
                        let profit = sniper_executor.execute(&action, signal_received).await;
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

                        // Eagerly populate the event market cache so resolution
                        // lookups later will hit instantly (the monitor's cache is
                        // shared via Arc<DashMap>, but this fires an extra lookup
                        // from the event handler side for redundancy).
                        if let Some(ref cache) = event_market_cache {
                            let cache = cache.clone();
                            let cid_hex = format!("{:x}", condition_id);
                            tokio::spawn(async move {
                                cache.lookup_condition(&cid_hex).await;
                            });
                        }
                    }
                    OnChainSignal::ResolutionProposed {
                        question_id,
                        proposed_price,
                        proposer,
                        block_number,
                        market_id,
                        detected_at,
                        chain_latency_ms,
                        ..
                    } => {
                        let outcome = if proposed_price > 0 { "YES" } else { "NO" };
                        let receive_elapsed_ms = detected_at.elapsed().as_millis() as u64;
                        warn!(
                            question_id = %question_id,
                            market_id = ?market_id,
                            proposed = outcome,
                            proposer = %proposer,
                            block = block_number,
                            chain_ms = chain_latency_ms,
                            channel_ms = receive_elapsed_ms,
                            "ON-CHAIN: RESOLUTION PROPOSED — sniping window open"
                        );

                        // Scan ONLY the matched market's order book for snipeable orders.
                        // Use the Gamma numeric market_id parsed from the UMA ancillary data
                        // to look up the market via cache or direct Gamma API call.
                        // Fast path: in-memory DashMap (nanoseconds).
                        // Fallback: GET /markets/{id} (single attempt, ~100ms).
                        {
                            let cached = match (event_market_cache.as_ref(), market_id.as_ref()) {
                                (Some(cache), Some(mid)) => {
                                    let sync_hit = cache.get_by_market_id(mid);
                                    if sync_hit.is_some() {
                                        sync_hit
                                    } else {
                                        warn!(
                                            market_id = %mid,
                                            "market_id not in cache, fetching from Gamma API"
                                        );
                                        cache.lookup_by_market_id(mid).await
                                    }
                                }
                                (_, None) => {
                                    warn!(
                                        question_id = %question_id,
                                        "no market_id in ancillary data — cannot map to market"
                                    );
                                    None
                                }
                                (None, _) => None,
                            };

                            if let Some(market) = cached {
                                let one = Decimal::from(1);
                                let mut grand_total_profit = Decimal::ZERO;
                                let token_count = market.clob_token_ids.len();

                                warn!(
                                    market_id = ?market_id,
                                    condition_id = %market.condition_id,
                                    question = %market.question,
                                    outcomes = ?market.outcomes,
                                    tokens = token_count,
                                    "MATCHED MARKET for resolution"
                                );

                                for (idx, token_id) in market.clob_token_ids.iter().enumerate() {
                                    let outcome_label = market.outcomes.get(idx)
                                        .map(|s| s.as_str())
                                        .unwrap_or("?");

                                    if let Some(book) = store.get_book(token_id) {
                                        // Asks ≤ $0.95 = winning outcome shares to buy cheap
                                        let mut ask_shares = Decimal::ZERO;
                                        let mut ask_profit = Decimal::ZERO;
                                        let mut ask_details = Vec::new();
                                        let threshold = Decimal::from_str("0.95").unwrap();
                                        for (&price, &size) in book.asks.levels.iter() {
                                            if price <= threshold {
                                                ask_shares += size;
                                                ask_profit += (one - price) * size;
                                                ask_details.push(format!("{}@${}", size, price));
                                            }
                                        }
                                        // Bids ≥ $0.05 = losing outcome shares to sell into
                                        let mut bid_shares = Decimal::ZERO;
                                        let mut bid_profit = Decimal::ZERO;
                                        let mut bid_details = Vec::new();
                                        let floor = Decimal::from_str("0.05").unwrap();
                                        for (&price, &size) in book.bids.levels.iter().rev() {
                                            if price >= floor {
                                                bid_shares += size;
                                                bid_profit += price * size;
                                                bid_details.push(format!("{}@${}", size, price));
                                            }
                                        }

                                        let total = ask_profit + bid_profit;
                                        grand_total_profit += total;

                                        warn!(
                                            outcome = outcome_label,
                                            token = %&token_id[..16.min(token_id.len())],
                                            best_bid = %book.bids.best(true).map(|(p,_)| p.to_string()).unwrap_or_else(|| "-".into()),
                                            best_ask = %book.asks.best(false).map(|(p,_)| p.to_string()).unwrap_or_else(|| "-".into()),
                                            ask_snipe_shares = %ask_shares,
                                            ask_snipe_profit = %format!("${:.2}", ask_profit),
                                            ask_levels = %ask_details.join(" | "),
                                            bid_snipe_shares = %bid_shares,
                                            bid_snipe_profit = %format!("${:.2}", bid_profit),
                                            bid_levels = %bid_details.join(" | "),
                                            "SNIPE BOOK: {} outcome", outcome_label
                                        );
                                    } else {
                                        warn!(
                                            outcome = outcome_label,
                                            token = %&token_id[..16.min(token_id.len())],
                                            "SNIPE BOOK: no order book data for this token"
                                        );
                                    }
                                }

                                warn!(
                                    question = %market.question,
                                    proposed = outcome,
                                    tokens_scanned = token_count,
                                    total_potential_profit = %format!("${:.2}", grand_total_profit),
                                    "SNIPE SCAN COMPLETE"
                                );
                            } else {
                                warn!(
                                    question_id = %question_id,
                                    market_id = ?market_id,
                                    proposed = outcome,
                                    "SNIPE SCAN: could not resolve market — no cache hit or API match"
                                );
                            }
                        }

                        // Feed to sniper engine for execution (if market mapping exists)
                        if let Some(ref engine) = sniper_engine {
                            let qid_str = format!("{:x}", question_id);
                            let action = engine.lock().await.on_chain_resolution(
                                &qid_str,
                                outcome,
                                detected_at,
                            );
                            if let Some(ref action) = action {
                                let profit = sniper_executor.execute(action, detected_at).await;
                                let order_ready_ms = detected_at.elapsed().as_millis() as u64;
                                info!(
                                    chain_ms = chain_latency_ms,
                                    pipeline_ms = order_ready_ms,
                                    total_ms = chain_latency_ms + order_ready_ms,
                                    profit = %profit,
                                    "SNIPE LATENCY: chain={}ms | pipeline={}ms | total={}ms",
                                    chain_latency_ms, order_ready_ms, chain_latency_ms + order_ready_ms
                                );
                                metrics.lock().await.record_latency(
                                    "snipe-onchain",
                                    &action.condition_id,
                                    Duration::from_millis(chain_latency_ms + order_ready_ms),
                                );
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
                        chain_latency_ms,
                        ..
                    } => {
                        error!(
                            question_id = %question_id,
                            block = block_number,
                            chain_ms = chain_latency_ms,
                            "ON-CHAIN: RESOLUTION DISPUTED — cancel any snipe orders!"
                        );
                    }
                    OnChainSignal::ResolutionFinalised {
                        condition_id,
                        question_id,
                        payout_numerators,
                        block_number,
                        chain_latency_ms,
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
                            chain_ms = chain_latency_ms,
                            "ON-CHAIN: resolution finalised — trigger redemption"
                        );
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
                break;
            }
        }
    }

    Ok(())
}
