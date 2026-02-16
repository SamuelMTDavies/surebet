mod config;
mod market;
mod orderbook;
mod ws;

use crate::config::Config;
use crate::market::{classify_edge_profile, MarketDiscovery};
use crate::orderbook::OrderBookStore;
use crate::ws::clob::{ClobEvent, ClobWsClient};
use crate::ws::rtds::{RtdsEvent, RtdsSubscription, RtdsWsClient};
use std::path::Path;
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

    if !config.has_credentials() {
        warn!(
            "no API credentials configured - running in read-only mode \
             (set POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE for trading)"
        );
    }

    // Phase 1: Discover markets
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

    // Log market profiles
    info!("--- Market Edge Profiles ---");
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

    // Phase 2: Set up order book store
    let store = OrderBookStore::new();

    // Phase 3: Connect CLOB WebSocket for order book data
    let (clob_tx, mut clob_rx) = mpsc::unbounded_channel::<ClobEvent>();
    let clob_client = ClobWsClient::new(
        config.polymarket.clob_ws_url.clone(),
        store.clone(),
        clob_tx,
    );

    info!(
        assets = asset_ids.len(),
        "subscribing to CLOB market channel"
    );
    clob_client.subscribe(asset_ids).await?;

    // Phase 4: Connect RTDS WebSocket for trade activity
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

    // Phase 5: Event processing loop
    info!("entering main event loop - press Ctrl+C to stop");

    let store_for_summary = store.clone();

    // Periodic summary printer
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
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

    // Main event loop
    loop {
        tokio::select! {
            Some(clob_event) = clob_rx.recv() => {
                match clob_event {
                    ClobEvent::Connected => {
                        info!("CLOB WebSocket connected");
                    }
                    ClobEvent::Disconnected => {
                        warn!("CLOB WebSocket disconnected");
                    }
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
                    RtdsEvent::Connected => {
                        info!("RTDS WebSocket connected");
                    }
                    RtdsEvent::Disconnected => {
                        warn!("RTDS WebSocket disconnected");
                    }
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
                            "RTDS order match"
                        );
                    }
                    RtdsEvent::CryptoPrice { symbol, price } => {
                        info!(symbol = %symbol, price = %price, "crypto price");
                    }
                    RtdsEvent::RawEvent(_) => {}
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
