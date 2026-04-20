//! On-chain event correlation.
//!
//! For each market the trader has traded, queries Polygon RPC for historical
//! ConditionPreparation, ProposePrice, and ConditionResolution events.
//! Each trade is annotated with the nearest event of each type and the
//! time delta (trade_timestamp - event_timestamp).

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use alloy::primitives::{b256, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Filter;
use polymarket_client_sdk::data::types::Side;
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use super::enrich::EnrichedTraderData;

// ── Event topic hashes (copied from onchain::abi to avoid lib/bin module conflicts) ─

/// keccak256("ConditionPreparation(bytes32,address,bytes32,uint256)")
const CONDITION_PREPARATION_TOPIC: B256 =
    b256!("abf28353011ab5adfa12894e9da498afb8e102520e71ba8e12acd979f2753e23");

/// keccak256("ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)")
const PROPOSE_PRICE_TOPIC: B256 =
    b256!("6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1");

/// keccak256("ConditionResolution(bytes32,address,bytes32,uint256,uint256[])")
const CONDITION_RESOLUTION_TOPIC: B256 =
    b256!("b3a26bab9bbcd2aabece9cb56a3bcc47b9cfee7ecef7e3d4ab4455f3afe4d53f");

/// A nearby on-chain event relative to a trade.
#[derive(Debug, Clone)]
pub struct NearbyEvent {
    pub event_type: EventType,
    pub block_number: u64,
    pub block_timestamp: u64,
    /// trade_timestamp - event_timestamp.
    /// Negative = trade happened BEFORE the event (anticipatory).
    /// Positive = trade happened AFTER the event (reactive).
    pub delta_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    ConditionPreparation,
    ProposePrice,
    ConditionResolution,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::ConditionPreparation => write!(f, "ConditionPreparation"),
            EventType::ProposePrice => write!(f, "ProposePrice"),
            EventType::ConditionResolution => write!(f, "ConditionResolution"),
        }
    }
}

/// A single trade annotated with correlated on-chain events.
#[derive(Debug, Clone)]
pub struct TradeEventCorrelation {
    pub trade_timestamp: i64,
    pub trade_condition_id: String,
    pub trade_side: Side,
    pub trade_price: Decimal,
    pub trade_size: Decimal,
    pub trade_title: String,
    pub trade_outcome: String,

    /// Nearest ConditionPreparation (market creation) event.
    pub nearest_creation: Option<NearbyEvent>,
    /// Nearest ProposePrice (resolution proposal) event.
    pub nearest_propose: Option<NearbyEvent>,
    /// Nearest ConditionResolution (final settlement) event.
    pub nearest_resolution: Option<NearbyEvent>,
}

/// Enriched data with on-chain event correlations added.
#[derive(Debug, Clone)]
pub struct CorrelatedTraderData {
    pub enriched: EnrichedTraderData,
    pub trade_events: Vec<TradeEventCorrelation>,
}

/// On-chain event timestamps for a single market.
#[derive(Debug, Default)]
struct MarketEvents {
    creation_timestamps: Vec<u64>,
    propose_timestamps: Vec<u64>,
    resolution_timestamps: Vec<u64>,
}

/// Known contract addresses on Polygon.
const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const UMA_ORACLE_ADDRESS: &str = "0xeE3Afe347D5C74317041E2618C49534dAf887c24";

/// Correlate trades with on-chain events.
///
/// If `rpc_url` is None, returns correlations with no on-chain events.
pub async fn correlate(
    enriched: &EnrichedTraderData,
    rpc_url: Option<&str>,
) -> Result<CorrelatedTraderData> {
    let trades = &enriched.raw.trades;

    if trades.is_empty() {
        return Ok(CorrelatedTraderData {
            enriched: enriched.clone(),
            trade_events: vec![],
        });
    }

    // Group trades by condition_id
    let mut trades_by_cid: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, t) in trades.iter().enumerate() {
        let cid = format!("{:#x}", t.condition_id);
        trades_by_cid.entry(cid).or_default().push(idx);
    }

    info!(
        unique_markets = trades_by_cid.len(),
        total_trades = trades.len(),
        "correlating trades with on-chain events"
    );

    // Fetch on-chain events per market
    let mut market_events: HashMap<String, MarketEvents> = HashMap::new();

    if let Some(rpc_url) = rpc_url {
        match fetch_all_market_events(rpc_url, &trades_by_cid, trades).await {
            Ok(events) => {
                market_events = events;
            }
            Err(e) => {
                warn!(rpc_url, error = %e, "failed to fetch on-chain events, proceeding without correlation");
                eprintln!("  WARNING: On-chain correlation failed ({rpc_url}): {e:#}");
                eprintln!("  Phases 3-4 (creation/resolution timing) will be empty.");
                eprintln!("  Try a different RPC: --rpc-http https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY\n");
            }
        }
    } else {
        info!("no RPC URL provided, skipping on-chain event correlation");
    }

    // Build correlations
    let mut trade_events = Vec::with_capacity(trades.len());
    for t in trades.iter() {
        let cid = format!("{:#x}", t.condition_id);
        let events = market_events.get(&cid);

        let nearest_creation = events.and_then(|e| find_nearest(&e.creation_timestamps, t.timestamp));
        let nearest_propose = events.and_then(|e| find_nearest(&e.propose_timestamps, t.timestamp));
        let nearest_resolution =
            events.and_then(|e| find_nearest(&e.resolution_timestamps, t.timestamp));

        trade_events.push(TradeEventCorrelation {
            trade_timestamp: t.timestamp,
            trade_condition_id: cid,
            trade_side: t.side.clone(),
            trade_price: t.price,
            trade_size: t.size,
            trade_title: t.title.clone(),
            trade_outcome: t.outcome.clone(),
            nearest_creation: nearest_creation.map(|(ts, delta)| NearbyEvent {
                event_type: EventType::ConditionPreparation,
                block_number: 0,
                block_timestamp: ts,
                delta_seconds: delta,
            }),
            nearest_propose: nearest_propose.map(|(ts, delta)| NearbyEvent {
                event_type: EventType::ProposePrice,
                block_number: 0,
                block_timestamp: ts,
                delta_seconds: delta,
            }),
            nearest_resolution: nearest_resolution.map(|(ts, delta)| NearbyEvent {
                event_type: EventType::ConditionResolution,
                block_number: 0,
                block_timestamp: ts,
                delta_seconds: delta,
            }),
        });
    }

    let correlated_count = trade_events
        .iter()
        .filter(|t| {
            t.nearest_creation.is_some()
                || t.nearest_propose.is_some()
                || t.nearest_resolution.is_some()
        })
        .count();

    info!(
        total_trades = trade_events.len(),
        correlated = correlated_count,
        "event correlation complete"
    );

    Ok(CorrelatedTraderData {
        enriched: enriched.clone(),
        trade_events,
    })
}

/// Find the nearest event timestamp to a trade timestamp.
/// Returns (event_timestamp, delta_seconds) where delta = trade_ts - event_ts.
fn find_nearest(event_timestamps: &[u64], trade_ts: i64) -> Option<(u64, i64)> {
    if event_timestamps.is_empty() {
        return None;
    }

    let mut best_ts = event_timestamps[0];
    let mut best_delta = (trade_ts - best_ts as i64).abs();

    for &ets in &event_timestamps[1..] {
        let delta = (trade_ts - ets as i64).abs();
        if delta < best_delta {
            best_delta = delta;
            best_ts = ets;
        }
    }

    let delta = trade_ts - best_ts as i64;
    Some((best_ts, delta))
}

/// Fetch on-chain events for all markets the trader has traded.
async fn fetch_all_market_events(
    rpc_url: &str,
    trades_by_cid: &HashMap<String, Vec<usize>>,
    trades: &[polymarket_client_sdk::data::types::response::Trade],
) -> Result<HashMap<String, MarketEvents>> {
    // Connect to RPC
    info!(rpc_url, "connecting to Polygon RPC");
    let provider = if rpc_url.starts_with("wss://") || rpc_url.starts_with("ws://") {
        ProviderBuilder::new()
            .connect_ws(WsConnect::new(rpc_url))
            .await
            .with_context(|| format!("failed to connect to WS RPC: {rpc_url}"))?
    } else {
        ProviderBuilder::new()
            .connect_http(rpc_url.parse().with_context(|| format!("invalid RPC URL: {rpc_url}"))?)
    };

    // Get reference block for timestamp estimation (retry once on failure)
    let latest_block_num = match provider.get_block_number().await {
        Ok(n) => n,
        Err(e) => {
            warn!(error = %e, "first attempt to get block number failed, retrying...");
            tokio::time::sleep(Duration::from_secs(2)).await;
            provider
                .get_block_number()
                .await
                .with_context(|| format!("failed to get latest block from {rpc_url} — is this a valid Polygon RPC endpoint?"))?
        }
    };
    let latest_block = provider
        .get_block_by_number(latest_block_num.into())
        .await
        .context("failed to get latest block details")?
        .ok_or_else(|| anyhow::anyhow!("latest block not found"))?;
    let ref_ts = latest_block.header.timestamp;
    let ref_block = latest_block_num;

    info!(
        ref_block,
        ref_ts,
        markets = trades_by_cid.len(),
        "querying on-chain events"
    );

    let ctf_addr: alloy::primitives::Address = CTF_ADDRESS.parse().unwrap();
    let uma_addr: alloy::primitives::Address = UMA_ORACLE_ADDRESS.parse().unwrap();

    // ── Phase A: Batch query ProposePrice events across full trade time range ──
    // ProposePrice doesn't have condition_id in topics (topics are: event sig,
    // requester address, proposer address). We must query broadly, then decode
    // the ancillary data to match to specific markets.
    //
    // Instead of per-market queries (297 × 3 = 891 RPC calls), query ProposePrice
    // ONCE for the entire trade date range, then distribute events to markets
    // by decoding the ancillary data.

    let global_min_ts = trades.iter().map(|t| t.timestamp).min().unwrap_or(0);
    let global_max_ts = trades.iter().map(|t| t.timestamp).max().unwrap_or(0);

    // Expand by 24 hours each side to catch proposals before first trade / after last
    let global_from_ts = (global_min_ts - 86400).max(0) as u64;
    let global_to_ts = (global_max_ts + 86400) as u64;
    let global_from_block = ts_to_block(global_from_ts, ref_ts, ref_block);
    let global_to_block = ts_to_block(global_to_ts, ref_ts, ref_block);

    info!(
        from_block = global_from_block,
        to_block = global_to_block,
        span_blocks = global_to_block - global_from_block,
        "querying ProposePrice events (single batch for entire range)"
    );

    // ProposePrice: batch query across the full range, chunked to avoid
    // RPC response size limits. Use 50K blocks per chunk (Infura supports
    // up to ~100K for filtered queries; 50K is conservative).
    //
    // ProposePrice doesn't have condition_id in indexed topics (topics are:
    // event sig, requester, proposer). However, the ancillaryData field
    // contains "market_id: <digits>" which is the Gamma API numeric ID.
    // We extract that, then batch-resolve market_id → condition_id via Gamma.
    let mut raw_propose_events: Vec<(u64, Vec<u8>)> = Vec::new(); // (timestamp, log_data)
    {
        let chunk_size = 50_000u64;
        let mut chunk_from = global_from_block;
        while chunk_from < global_to_block {
            let chunk_to = (chunk_from + chunk_size).min(global_to_block);
            let mut success = false;
            for attempt in 0..3 {
                match provider
                    .get_logs(
                        &Filter::new()
                            .address(uma_addr)
                            .event_signature(PROPOSE_PRICE_TOPIC)
                            .from_block(chunk_from)
                            .to_block(chunk_to),
                    )
                    .await
                {
                    Ok(logs) => {
                        for log in &logs {
                            let ts = if let Some(bt) = log.block_timestamp {
                                bt
                            } else if let Some(bn) = log.block_number {
                                block_to_ts(bn, ref_ts, ref_block)
                            } else {
                                continue;
                            };
                            let data = log.data().data.to_vec();
                            raw_propose_events.push((ts, data));
                        }
                        success = true;
                        break;
                    }
                    Err(e) => {
                        let delay = Duration::from_millis(500 * (1 << attempt));
                        warn!(
                            from = chunk_from,
                            to = chunk_to,
                            attempt = attempt + 1,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "ProposePrice chunk query failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
            if !success {
                warn!(
                    from = chunk_from,
                    to = chunk_to,
                    "ProposePrice chunk failed after 3 attempts, skipping"
                );
            }
            chunk_from = chunk_to + 1;
            // Delay between chunks to respect rate limits
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    info!(
        raw_events = raw_propose_events.len(),
        "ProposePrice batch query complete, extracting market_ids"
    );

    // ── Phase B: Extract market_id from ancillaryData, resolve to condition_id ──
    // Each ProposePrice event's ancillaryData contains "market_id: <digits>".
    // We collect unique market_ids, batch-resolve via Gamma API, then build
    // a condition_id-keyed map of propose timestamps.
    let mut propose_by_condition_id: HashMap<String, Vec<u64>> = HashMap::new();
    {
        // Step 1: Parse market_id from each event
        let mut events_with_market_id: Vec<(u64, String)> = Vec::new(); // (timestamp, market_id)
        let mut unique_market_ids: HashSet<String> = HashSet::new();
        let mut no_market_id = 0;

        for (ts, data) in &raw_propose_events {
            if let Some(mid) = extract_market_id_from_ancillary(data) {
                unique_market_ids.insert(mid.clone());
                events_with_market_id.push((*ts, mid));
            } else {
                no_market_id += 1;
            }
        }

        info!(
            parsed = events_with_market_id.len(),
            unique_market_ids = unique_market_ids.len(),
            no_market_id,
            "extracted market_ids from ProposePrice ancillary data"
        );

        // Step 2: Filter to only market_ids for condition_ids we care about
        // (the trader's markets). First resolve all unique market_ids via Gamma.
        let client = reqwest::Client::new();
        let mut market_id_to_cid: HashMap<String, String> = HashMap::new();
        let market_id_list: Vec<String> = unique_market_ids.into_iter().collect();

        for (i, mid) in market_id_list.iter().enumerate() {
            for attempt in 0..3 {
                match client
                    .get(format!("https://gamma-api.polymarket.com/markets/{mid}"))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(body) = resp.json::<serde_json::Value>().await {
                            if let Some(cid) = body.get("conditionId").and_then(|v| v.as_str()) {
                                let cid_lower = cid.to_lowercase();
                                // Only keep if this condition_id is one of the trader's markets
                                if trades_by_cid.contains_key(&cid_lower) {
                                    market_id_to_cid.insert(mid.clone(), cid_lower);
                                }
                            }
                        }
                        break;
                    }
                    Ok(_) => break, // 404 etc — market doesn't exist
                    Err(_) if attempt < 2 => {
                        tokio::time::sleep(Duration::from_millis(300 * (1 << attempt))).await;
                    }
                    Err(_) => break,
                }
            }

            // Rate limit: Gamma API
            if (i + 1) % 20 == 0 {
                info!(
                    resolved = market_id_to_cid.len(),
                    queried = i + 1,
                    total = market_id_list.len(),
                    "resolving market_id → condition_id"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        info!(
            resolved = market_id_to_cid.len(),
            total_market_ids = market_id_list.len(),
            "market_id → condition_id resolution complete"
        );

        // Step 3: Build per-condition_id propose timestamp lists
        for (ts, mid) in &events_with_market_id {
            if let Some(cid) = market_id_to_cid.get(mid) {
                propose_by_condition_id
                    .entry(cid.clone())
                    .or_default()
                    .push(*ts);
            }
        }

        info!(
            markets_with_proposals = propose_by_condition_id.len(),
            "ProposePrice events matched to trader's markets"
        );
    }

    let mut result: HashMap<String, MarketEvents> = HashMap::new();
    let mut queried = 0;

    for (cid, trade_indices) in trades_by_cid {
        let min_ts = trade_indices
            .iter()
            .map(|&i| trades[i].timestamp)
            .min()
            .unwrap_or(0);
        let max_ts = trade_indices
            .iter()
            .map(|&i| trades[i].timestamp)
            .max()
            .unwrap_or(0);

        // Expand by 24 hours each side (up from 2 hours) for better coverage
        let from_ts = (min_ts - 86400).max(0) as u64;
        let to_ts = (max_ts + 86400) as u64;

        let from_block = ts_to_block(from_ts, ref_ts, ref_block);
        let to_block = ts_to_block(to_ts, ref_ts, ref_block);

        let mut events = MarketEvents::default();

        // Parse condition_id as B256 for topic filtering
        let cid_bytes: Option<B256> = cid
            .strip_prefix("0x")
            .and_then(|hex| hex.parse::<B256>().ok());

        // ── ConditionPreparation: filter by condition_id as topic1 ──
        // Search a wide range — creation must precede trades.
        if let Some(cid_b256) = cid_bytes {
            for attempt in 0..3 {
                match provider
                    .get_logs(
                        &Filter::new()
                            .address(ctf_addr)
                            .event_signature(CONDITION_PREPARATION_TOPIC)
                            .topic1(cid_b256)
                            .from_block(from_block.saturating_sub(1_000_000))
                            .to_block(to_block),
                    )
                    .await
                {
                    Ok(logs) => {
                        for log in &logs {
                            if let Some(ts) = log.block_timestamp {
                                events.creation_timestamps.push(ts);
                            } else if let Some(bn) = log.block_number {
                                events.creation_timestamps.push(block_to_ts(bn, ref_ts, ref_block));
                            }
                        }
                        break;
                    }
                    Err(_) if attempt < 2 => {
                        tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
                    }
                    Err(_) => break,
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // ── ProposePrice: use pre-resolved condition_id-keyed data ──
        // Each ProposePrice event was decoded to extract its market_id from
        // ancillaryData, resolved to condition_id via Gamma, and only events
        // matching THIS market's condition_id are included.
        if let Some(propose_ts) = propose_by_condition_id.get(cid) {
            events.propose_timestamps.extend(propose_ts);
        }

        // ── ConditionResolution: filter by condition_id as topic1 ──
        if let Some(cid_b256) = cid_bytes {
            for attempt in 0..3 {
                match provider
                    .get_logs(
                        &Filter::new()
                            .address(ctf_addr)
                            .event_signature(CONDITION_RESOLUTION_TOPIC)
                            .topic1(cid_b256)
                            .from_block(from_block)
                            .to_block(to_block),
                    )
                    .await
                {
                    Ok(logs) => {
                        for log in &logs {
                            if let Some(ts) = log.block_timestamp {
                                events.resolution_timestamps.push(ts);
                            } else if let Some(bn) = log.block_number {
                                events.resolution_timestamps.push(block_to_ts(bn, ref_ts, ref_block));
                            }
                        }
                        break;
                    }
                    Err(_) if attempt < 2 => {
                        tokio::time::sleep(Duration::from_millis(500 * (1 << attempt))).await;
                    }
                    Err(_) => break,
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let has_any = !events.creation_timestamps.is_empty()
            || !events.propose_timestamps.is_empty()
            || !events.resolution_timestamps.is_empty();

        if has_any {
            debug!(
                cid = %&cid[..12.min(cid.len())],
                creation = events.creation_timestamps.len(),
                propose = events.propose_timestamps.len(),
                resolution = events.resolution_timestamps.len(),
                "events found"
            );
        }

        result.insert(cid.clone(), events);
        queried += 1;

        // Rate limit: delay every 10 markets (2 RPC calls per market)
        if queried % 10 == 0 {
            info!(queried, total = trades_by_cid.len(), "correlation progress");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(result)
}

/// Estimate a block number from a unix timestamp, using a reference block.
/// Polygon produces blocks roughly every 2 seconds.
fn ts_to_block(target_ts: u64, ref_ts: u64, ref_block: u64) -> u64 {
    if target_ts >= ref_ts {
        ref_block + (target_ts - ref_ts) / 2
    } else {
        let blocks_back = (ref_ts - target_ts) / 2;
        ref_block.saturating_sub(blocks_back)
    }
}

/// Estimate a unix timestamp from a block number, using a reference block.
fn block_to_ts(block: u64, ref_ts: u64, ref_block: u64) -> u64 {
    if block >= ref_block {
        ref_ts + (block - ref_block) * 2
    } else {
        ref_ts.saturating_sub((ref_block - block) * 2)
    }
}

/// Extract the Gamma API numeric `market_id` from UMA ProposePrice event data.
///
/// The event data layout:
///   [0..32]    identifier (bytes32)
///   [32..64]   timestamp (uint256)
///   [64..96]   offset to ancillaryData (uint256)
///   [96..128]  proposedPrice (int256)
///   [128..160] expirationTimestamp (uint256)
///   [160..192] currency (address)
///   [offset..] ancillaryData dynamic bytes
///
/// The ancillaryData is UTF-8 text containing "market_id: <digits>".
fn extract_market_id_from_ancillary(data: &[u8]) -> Option<String> {
    if data.len() < 96 {
        return None;
    }

    // Read offset to ancillaryData at data[64..96]
    let offset_bytes: [u8; 32] = data[64..96].try_into().ok()?;
    let offset: usize = U256::from_be_bytes(offset_bytes).try_into().ok()?;

    if offset == 0 || offset + 32 > data.len() {
        return None;
    }

    // Read length of ancillaryData
    let len_bytes: [u8; 32] = data[offset..offset + 32].try_into().ok()?;
    let len: usize = U256::from_be_bytes(len_bytes).try_into().ok()?;

    if len == 0 || offset + 32 + len > data.len() {
        return None;
    }

    let ancillary = &data[offset + 32..offset + 32 + len];
    let text = std::str::from_utf8(ancillary).ok()?;

    // Look for "market_id: <digits>"
    if let Some(pos) = text.find("market_id: ") {
        let after = &text[pos + "market_id: ".len()..];
        let id: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !id.is_empty() {
            return Some(id);
        }
    }

    // Fallback: "market_id:" (no space)
    if let Some(pos) = text.find("market_id:") {
        let after = &text[pos + "market_id:".len()..];
        let trimmed = after.trim_start();
        let id: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !id.is_empty() {
            return Some(id);
        }
    }

    None
}
