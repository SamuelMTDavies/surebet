mod arb;
mod auth;
mod config;
mod feeds;
mod market;
mod orderbook;
mod ws;

use crate::arb::executor::{ArbExecutor, RiskLimits};
use crate::arb::maker::{MakerConfig, MakerEvent, MakerStrategy};
use crate::arb::{ArbEvent, ArbScanner, TrackedMarket};
use crate::auth::{ClobApiClient, L2Credentials};
use crate::config::Config;
use crate::feeds::aggregator::{AggregatorEvent, PriceAggregator};
use crate::feeds::binance::{BinanceEvent, BinanceFeed};
use crate::feeds::coinbase::{CoinbaseEvent, CoinbaseFeed};
use crate::market::{classify_edge_profile, MarketDiscovery};
use crate::orderbook::OrderBookStore;
use crate::ws::clob::{ClobEvent, ClobWsClient};
use crate::ws::rtds::{RtdsEvent, RtdsSubscription, RtdsWsClient};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        .filter_map(TrackedMarket::from_gamma)
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
    let clob_client = ClobWsClient::new(
        config.polymarket.clob_ws_url.clone(),
        store.clone(),
        clob_tx,
    );
    info!(assets = asset_ids.len(), "subscribing to CLOB market channel");
    clob_client.subscribe(asset_ids).await?;

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
        .filter_map(TrackedMarket::from_gamma)
        .collect();

    if config.maker.enabled {
        if let Some(ref api) = api_client {
            let mc = &config.maker;
            let maker_config = MakerConfig {
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
            };

            let mut maker = MakerStrategy::new(
                api.clone(),
                store.clone(),
                maker_config,
                maker_tx,
            );
            maker.add_markets(&maker_markets);

            let tick_interval = Duration::from_secs(mc.requote_interval_secs);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tick_interval);
                loop {
                    interval.tick().await;
                    maker.tick().await;
                }
            });

            info!(
                markets = maker_markets.len(),
                target_sum = %config.maker.target_bid_sum,
                "maker strategy enabled"
            );
        } else {
            warn!("maker.enabled=true but no API credentials");
        }
    } else {
        info!("maker strategy disabled (set maker.enabled=true in config)");
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
                        info!(
                            asset = %asset_id[..12.min(asset_id.len())],
                            bids = bid_levels,
                            asks = ask_levels,
                            "book snapshot"
                        );
                    }
                    ClobEvent::PriceChange { asset_id, best_bid, best_ask } => {
                        info!(
                            asset = %asset_id[..12.min(asset_id.len())],
                            bid = ?best_bid,
                            ask = ?best_ask,
                            "price change"
                        );
                    }
                    ClobEvent::LastTradePrice { asset_id, price } => {
                        info!(
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
                        info!(
                            asset = %t.asset_id,
                            price = %t.price,
                            size = %t.size,
                            side = %t.side,
                            "RTDS trade"
                        );
                    }
                    RtdsEvent::OrderMatch(m) => {
                        info!(
                            asset = %m.asset_id,
                            price = %m.price,
                            size = %m.size,
                            "RTDS match"
                        );
                    }
                    RtdsEvent::CryptoPrice { symbol, price } => {
                        info!(symbol = %symbol, price = %price, "crypto price");
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
                        if let Some(ref exec) = executor {
                            match exec.try_execute(&opp).await {
                                Ok(true) => info!(market = %opp.question, "arb executed"),
                                Ok(false) => {}
                                Err(e) => error!(error = %e, "arb execution error"),
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
                }
            }

            _ = tokio::signal::ctrl_c() => {
                info!("shutting down...");
                if let Some(ref exec) = executor {
                    if exec.is_enabled() {
                        info!("cancelling all open orders...");
                        if let Err(e) = exec.cancel_all().await {
                            error!(error = %e, "failed to cancel orders on shutdown");
                        }
                    }
                }
                break;
            }
        }
    }

    Ok(())
}
