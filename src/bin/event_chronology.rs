//! Event chronology tool: queries on-chain ProposePrice, OrderFilled, and
//! ConditionResolution events to build a timeline around a specific trade.
//!
//! Usage:
//!   cargo run --bin event_chronology
//!
//! Uses public Polygon HTTP RPC — no API key required.

use alloy::primitives::{address, b256, Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

// ─── Constants ───────────────────────────────────────────────────────────────

// Contract addresses (from surebet.toml)
const UMA_ORACLE: Address = address!("eE3Afe347D5C74317041E2618C49534dAf887c24");
const CTF_EXCHANGE: Address = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const CTF: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");

// Event topic0 hashes (from onchain/abi.rs)
const PROPOSE_PRICE_TOPIC: B256 =
    b256!("6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1");
const ORDER_FILLED_TOPIC: B256 =
    b256!("d0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6");
const CONDITION_RESOLUTION_TOPIC: B256 =
    b256!("b3a26bab9bbcd2aabece9cb56a3bcc47b9cfee7ecef7e3d4ab4455f3afe4d53f");

// Target parameters
const USER_ADDR: Address = address!("bb015bb4009b6a48bfb9363d9c9b1d54e9ab02e5");
const TARGET_MARKET_ID: &str = "1230800";
const TRADE_BLOCK: u64 = 83_151_527; // The user's trade tx block
const TRADE_TX: &str = "0x13ce61f2ff8e0dbeefa2e1ee892f831704c6400674efcb39434792050e56f8a2";

// Target token IDs for this market
const YES_TOKEN: &str =
    "75629190769921177619892210337233452141398201777110655238134748084828418116837";
const NO_TOKEN: &str =
    "11527205430017017451951047787027804053475980774422934644474666529487295795343";

// Search range for ProposePrice: ~48 hours before trade, ~2h after
// For OrderFilled/Resolution: ~2 hours around trade
const PROPOSE_LOOKBACK: u64 = 86_400; // ~48 hours before trade
const PROPOSE_LOOKAHEAD: u64 = 3_600; // ~2 hours after trade
const TRADE_RANGE: u64 = 1_800; // ~1 hour around trade for OrderFilled

// Max blocks per getLogs request (free tier limit)
const CHUNK_SIZE: u64 = 9_999;

// Public Polygon HTTP RPC endpoints (no key needed)
const RPC_URLS: &[&str] = &[
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.drpc.org",
    "https://polygon-bor-rpc.publicnode.com",
];

// ─── Timeline entry ──────────────────────────────────────────────────────────

#[derive(Debug)]
struct TimelineEntry {
    block: u64,
    timestamp: Option<u64>,
    tx_hash: B256,
    event_type: String,
    details: String,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Event Chronology: Market {} ===", TARGET_MARKET_ID);
    println!("Target trade: {} (block {})", TRADE_TX, TRADE_BLOCK);
    println!("User: {:?}", USER_ADDR);
    println!();

    // Connect to a public RPC
    let provider = connect_rpc().await?;
    println!("[+] Connected to Polygon RPC");

    let propose_from = TRADE_BLOCK.saturating_sub(PROPOSE_LOOKBACK);
    let propose_to = TRADE_BLOCK + PROPOSE_LOOKAHEAD;
    let trade_from = TRADE_BLOCK.saturating_sub(TRADE_RANGE);
    let trade_to = TRADE_BLOCK + TRADE_RANGE;
    println!(
        "[+] ProposePrice scan: blocks {} .. {} (~48h before to ~2h after trade)",
        propose_from, propose_to
    );
    println!(
        "[+] OrderFilled scan:  blocks {} .. {} (~1h around trade)",
        trade_from, trade_to
    );
    println!();

    let mut timeline: BTreeMap<(u64, u32), TimelineEntry> = BTreeMap::new();

    // ── 1. Query ProposePrice events on UMA Oracle ───────────────────────
    println!("[1/3] Querying ProposePrice events on UMA Oracle...");
    let propose_filter = Filter::new()
        .address(UMA_ORACLE)
        .event_signature(PROPOSE_PRICE_TOPIC);

    let propose_logs = get_logs_chunked(&provider, &propose_filter, propose_from, propose_to).await?;
    println!("      Found {} ProposePrice events in range", propose_logs.len());

    // Also dump the ancillary text for any proposals that DON'T have a market_id
    // to understand non-neg-risk market format
    let mut matched_proposals = 0u32;
    let mut no_market_id_count = 0u32;
    // Track unique requester addresses to see if different adapters use this oracle
    let mut requesters: std::collections::HashMap<Address, u32> = std::collections::HashMap::new();
    for log in &propose_logs {
        let data = &log.data().data;
        let market_id_opt = extract_market_id(data);

        // Track requester addresses
        if let Some(t) = log.topics().get(1) {
            let requester = Address::from_slice(&t.0[12..]);
            *requesters.entry(requester).or_insert(0) += 1;
        }

        // For debugging: dump ancillary text from proposals with no market_id
        if market_id_opt.is_none() && no_market_id_count < 5 {
            no_market_id_count += 1;
            if let Some(text) = extract_ancillary_text(data) {
                let block = log.block_number.unwrap_or(0);
                let snippet: String = text.chars().take(200).collect();
                println!("  [no market_id] block={} ancillary: {}...", block, snippet);
            }
        }

        // Check if ancillary text contains our question keywords
        if let Some(text) = extract_ancillary_text(data) {
            let lower = text.to_lowercase();
            if lower.contains("liberal democratic") || lower.contains("ldp") || lower.contains("1230800") || lower.contains("governing coalition") {
                let block = log.block_number.unwrap_or(0);
                let tx = log.transaction_hash.unwrap_or_default();
                println!("  !!! KEYWORD MATCH: block={} tx={:?}", block, tx);
                println!("      ancillary: {}", &text[..text.len().min(300)]);
            }
        }

        if let Some(ref market_id) = market_id_opt {
            if market_id == TARGET_MARKET_ID {
                matched_proposals += 1;
                let block = log.block_number.unwrap_or(0);
                let tx = log.transaction_hash.unwrap_or_default();
                let proposed_price = extract_proposed_price(data);
                let proposer = log
                    .topics()
                    .get(2)
                    .map(|t| Address::from_slice(&t.0[12..]))
                    .unwrap_or_default();
                let log_idx = log.log_index.unwrap_or(0) as u32;

                let details = format!(
                    "ProposePrice for market {} | proposed_price={} | proposer={:?}",
                    TARGET_MARKET_ID, proposed_price, proposer
                );
                println!("  >>> MATCH: block={} tx={:?} {}", block, tx, details);

                timeline.insert(
                    (block, log_idx),
                    TimelineEntry {
                        block,
                        timestamp: log.block_timestamp,
                        tx_hash: tx,
                        event_type: "PROPOSE_PRICE".to_string(),
                        details,
                    },
                );
            }
        }
    }
    println!("      Matched {} proposals for market {}", matched_proposals, TARGET_MARKET_ID);
    println!("      No-market-id events: {}", no_market_id_count);
    println!("      Requester addresses seen:");
    for (addr, count) in &requesters {
        println!("        {:?} => {} events", addr, count);
    }

    // Collect all ProposePrice market_ids with their block/timestamp for cross-referencing
    // Focus on proposals in the 30min before the trade
    let cutoff_block = TRADE_BLOCK.saturating_sub(600); // ~20 min before
    println!();
    println!("      ProposePrice events in 20min before trade (block >= {}):", cutoff_block);
    let mut propose_market_ids: Vec<(String, u64, Option<u64>, i64)> = Vec::new();
    for log in &propose_logs {
        let block = log.block_number.unwrap_or(0);
        if block < cutoff_block {
            continue;
        }
        let data = &log.data().data;
        if let Some(mid) = extract_market_id(data) {
            let proposed_price = extract_proposed_price(data);
            let ts = log.block_timestamp;
            propose_market_ids.push((mid.clone(), block, ts, proposed_price));
        }
    }
    // Deduplicate by market_id (keep first occurrence)
    let mut seen_mids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (mid, block, ts, price) in &propose_market_ids {
        if seen_mids.insert(mid.clone()) {
            let ts_str = ts
                .map(|t| fmt_ts(t))
                .unwrap_or_else(|| "?".to_string());
            let outcome = if *price > 0 { "YES" } else { "NO" };
            println!("        market_id={} block={} time={} proposed={}", mid, block, ts_str, outcome);
        }
    }
    println!("      ({} unique market_ids proposed in window)", seen_mids.len());
    println!();

    // ── 2. Query OrderFilled events for the user on CTF Exchange ─────────
    println!("[2/3] Querying OrderFilled events for user on CTF Exchange...");

    // OrderFilled: topic0=sig, topic1=orderHash, topic2=maker, topic3=taker
    // The user could be either maker or taker. Query both.
    // User as taker (topic3):
    let user_b256 = B256::left_padding_from(USER_ADDR.as_slice());

    // Query with user as taker (topic[3])
    let fill_taker_filter = Filter::new()
        .address(CTF_EXCHANGE)
        .event_signature(ORDER_FILLED_TOPIC)
        .topic3(user_b256);

    let fill_taker_logs = get_logs_chunked(&provider, &fill_taker_filter, trade_from, trade_to).await?;
    println!("      Found {} OrderFilled events (user as taker)", fill_taker_logs.len());

    // Query with user as maker (topic[2])
    let fill_maker_filter = Filter::new()
        .address(CTF_EXCHANGE)
        .event_signature(ORDER_FILLED_TOPIC)
        .topic2(user_b256);

    let fill_maker_logs = get_logs_chunked(&provider, &fill_maker_filter, trade_from, trade_to).await?;
    println!("      Found {} OrderFilled events (user as maker)", fill_maker_logs.len());

    for log in fill_taker_logs.iter().chain(fill_maker_logs.iter()) {
        let block = log.block_number.unwrap_or(0);
        let tx = log.transaction_hash.unwrap_or_default();
        let log_idx = log.log_index.unwrap_or(0) as u32;
        let data = &log.data().data;

        // OrderFilled data layout:
        // [0..32]   = makerAssetId (uint256)
        // [32..64]  = takerAssetId (uint256)
        // [64..96]  = makerAmountFilled (uint256)
        // [96..128] = takerAmountFilled (uint256)
        let (maker_asset, taker_asset, maker_amount, taker_amount) = if data.len() >= 128 {
            let ma_id = U256::from_be_bytes::<32>(data[0..32].try_into().unwrap_or([0u8; 32]));
            let ta_id = U256::from_be_bytes::<32>(data[32..64].try_into().unwrap_or([0u8; 32]));
            let ma_amt = U256::from_be_bytes::<32>(data[64..96].try_into().unwrap_or([0u8; 32]));
            let ta_amt = U256::from_be_bytes::<32>(data[96..128].try_into().unwrap_or([0u8; 32]));
            (ma_id, ta_id, ma_amt, ta_amt)
        } else {
            (U256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO)
        };

        let maker = log
            .topics()
            .get(2)
            .map(|t| Address::from_slice(&t.0[12..]))
            .unwrap_or_default();
        let taker = log
            .topics()
            .get(3)
            .map(|t| Address::from_slice(&t.0[12..]))
            .unwrap_or_default();

        let is_user_taker = taker == USER_ADDR;
        let role = if is_user_taker { "TAKER" } else { "MAKER" };

        // Check if this involves our target token
        let ma_str = maker_asset.to_string();
        let ta_str = taker_asset.to_string();
        let involves_target = ma_str == YES_TOKEN
            || ma_str == NO_TOKEN
            || ta_str == YES_TOKEN
            || ta_str == NO_TOKEN;

        // Compute price: makerAssetId=0 means maker pays USDC, taker gets tokens
        // makerAssetId=tokenId means maker pays tokens, taker gets USDC
        let (side, price_str, token_label) = if maker_asset == U256::ZERO {
            // Maker is paying USDC (makerAmount), taker receives tokens (takerAmount)
            let usdc = maker_amount;
            let shares = taker_amount;
            let price = if shares > U256::ZERO {
                format!(
                    "{:.4}",
                    usdc.to::<u128>() as f64 / shares.to::<u128>() as f64
                )
            } else {
                "?".to_string()
            };
            let tok = if ta_str == YES_TOKEN {
                "YES"
            } else if ta_str == NO_TOKEN {
                "NO"
            } else {
                "?"
            };
            ("BUY", price, tok)
        } else {
            // Maker is paying tokens, taker pays USDC
            let shares = maker_amount;
            let usdc = taker_amount;
            let price = if shares > U256::ZERO {
                format!(
                    "{:.4}",
                    usdc.to::<u128>() as f64 / shares.to::<u128>() as f64
                )
            } else {
                "?".to_string()
            };
            let tok = if ma_str == YES_TOKEN {
                "YES"
            } else if ma_str == NO_TOKEN {
                "NO"
            } else {
                "?"
            };
            ("SELL", price, tok)
        };

        let marker = if involves_target { ">>>" } else { "   " };
        let usdc_val = if maker_asset == U256::ZERO {
            maker_amount.to::<u128>() as f64 / 1_000_000.0
        } else {
            taker_amount.to::<u128>() as f64 / 1_000_000.0
        };

        let details = format!(
            "OrderFilled {} {} {} token @ {} | ${:.2} USDC | role={} counterparty={:?}",
            side, token_label, if involves_target { "(TARGET MARKET)" } else { "" },
            price_str, usdc_val, role,
            if is_user_taker { maker } else { taker }
        );
        println!("  {} block={} tx={:?}", marker, block, tx);
        if involves_target {
            println!("      {}", details);
        }

        timeline.insert(
            (block, log_idx),
            TimelineEntry {
                block,
                timestamp: log.block_timestamp,
                tx_hash: tx,
                event_type: if involves_target {
                    format!("ORDER_FILLED_{}", side)
                } else {
                    "ORDER_FILLED_OTHER".to_string()
                },
                details,
            },
        );
    }
    println!();

    // ── 3. Query ConditionResolution for this market's condition ──────────
    println!("[3/3] Querying ConditionResolution events...");
    let condition_id =
        b256!("343ba1d1a05b213bf0a131bc00670fcc64274ce464dc9becc6b214063f52a318");

    let resolution_filter = Filter::new()
        .address(CTF)
        .event_signature(CONDITION_RESOLUTION_TOPIC)
        .topic1(condition_id);

    let resolution_logs = get_logs_chunked(&provider, &resolution_filter, propose_from, propose_to).await?;
    println!(
        "      Found {} ConditionResolution events for this condition",
        resolution_logs.len()
    );

    for log in &resolution_logs {
        let block = log.block_number.unwrap_or(0);
        let tx = log.transaction_hash.unwrap_or_default();
        let log_idx = log.log_index.unwrap_or(0) as u32;
        let details = format!(
            "ConditionResolution for condition {:?}",
            condition_id
        );
        println!("  >>> block={} tx={:?}", block, tx);

        timeline.insert(
            (block, log_idx),
            TimelineEntry {
                block,
                timestamp: log.block_timestamp,
                tx_hash: tx,
                event_type: "CONDITION_RESOLUTION".to_string(),
                details,
            },
        );
    }
    println!();

    // ── Print chronological timeline ─────────────────────────────────────
    println!("=====================================================");
    println!("  CHRONOLOGICAL TIMELINE");
    println!("=====================================================");
    println!();

    let mut prev_block = 0u64;
    for ((block, _), entry) in &timeline {
        // Only show target-related events and a separator for gaps
        let is_target = entry.event_type.contains("PROPOSE_PRICE")
            || entry.event_type.contains("TARGET")
            || entry.event_type.contains("ORDER_FILLED_BUY")
            || entry.event_type.contains("ORDER_FILLED_SELL")
            || entry.event_type.contains("CONDITION_RESOLUTION");

        if !is_target && !entry.event_type.contains("OTHER") {
            continue;
        }

        if *block > prev_block + 100 && prev_block > 0 {
            let gap_secs = (*block - prev_block) * 2; // ~2s per block
            println!("  ... gap of ~{} seconds ({} blocks) ...", gap_secs, block - prev_block);
            println!();
        }

        let ts_str = if let Some(ts) = entry.timestamp {
            let dt = DateTime::<Utc>::from_timestamp(ts as i64, 0)
                .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| format!("ts={}", ts));
            dt
        } else {
            format!("block {}", block)
        };

        let marker = if entry.event_type.contains("PROPOSE") {
            "[PROPOSE]"
        } else if entry.event_type.contains("ORDER_FILLED") && !entry.event_type.contains("OTHER") {
            "[TRADE]  "
        } else if entry.event_type.contains("CONDITION") {
            "[RESOLVE]"
        } else {
            "[other]  "
        };

        // Only print target-relevant events in the summary
        if is_target {
            println!("  {} {} | block {} | tx {:.16}...", marker, ts_str, block, entry.tx_hash);
            println!("           {}", entry.details);
            println!();
        }

        prev_block = *block;
    }

    // ── Summary ──────────────────────────────────────────────────────────
    println!("=====================================================");
    println!("  SUMMARY");
    println!("=====================================================");

    let proposal_entries: Vec<_> = timeline
        .values()
        .filter(|e| e.event_type == "PROPOSE_PRICE")
        .collect();
    let trade_entries: Vec<_> = timeline
        .values()
        .filter(|e| e.event_type.starts_with("ORDER_FILLED_") && !e.event_type.contains("OTHER"))
        .collect();
    let resolution_entries: Vec<_> = timeline
        .values()
        .filter(|e| e.event_type == "CONDITION_RESOLUTION")
        .collect();

    if let Some(proposal) = proposal_entries.first() {
        println!("  ProposePrice block:   {}", proposal.block);
        if let Some(ts) = proposal.timestamp {
            println!("  ProposePrice time:    {}", fmt_ts(ts));
        }
    } else {
        println!("  ProposePrice:         NOT FOUND in range (try wider block range)");
    }

    for trade in &trade_entries {
        println!("  User trade block:     {}", trade.block);
        if let Some(ts) = trade.timestamp {
            println!("  User trade time:      {}", fmt_ts(ts));
        }
    }

    if let Some(resolution) = resolution_entries.first() {
        println!("  Resolution block:     {}", resolution.block);
        if let Some(ts) = resolution.timestamp {
            println!("  Resolution time:      {}", fmt_ts(ts));
        }
    } else {
        println!("  Resolution:           NOT FOUND in range");
    }

    // Compute deltas
    if let (Some(proposal), Some(trade)) = (proposal_entries.first(), trade_entries.first()) {
        let block_delta = trade.block as i64 - proposal.block as i64;
        let time_delta = match (proposal.timestamp, trade.timestamp) {
            (Some(pt), Some(tt)) => Some(tt as i64 - pt as i64),
            _ => None,
        };
        println!();
        println!(
            "  ProposePrice -> Trade: {} blocks ({} seconds)",
            block_delta,
            time_delta
                .map(|d| d.to_string())
                .unwrap_or_else(|| "?".to_string())
        );
    }

    if let (Some(trade), Some(resolution)) = (trade_entries.first(), resolution_entries.first()) {
        let block_delta = resolution.block as i64 - trade.block as i64;
        let time_delta = match (trade.timestamp, resolution.timestamp) {
            (Some(tt), Some(rt)) => Some(rt as i64 - tt as i64),
            _ => None,
        };
        println!(
            "  Trade -> Resolution:   {} blocks ({} seconds)",
            block_delta,
            time_delta
                .map(|d| d.to_string())
                .unwrap_or_else(|| "?".to_string())
        );
    }

    println!();
    println!("  Total OrderFilled events for user in range: {}",
        fill_taker_logs.len() + fill_maker_logs.len());
    println!("  Total ProposePrice events in range: {}", propose_logs.len());

    // Collect unique token IDs from user's non-target fills for cross-reference
    println!();
    println!("  --- User's token IDs from fills (for Gamma lookup) ---");
    let mut user_token_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for log in fill_taker_logs.iter().chain(fill_maker_logs.iter()) {
        let data = &log.data().data;
        if data.len() >= 64 {
            let ma_id = U256::from_be_bytes::<32>(data[0..32].try_into().unwrap_or([0u8; 32]));
            let ta_id = U256::from_be_bytes::<32>(data[32..64].try_into().unwrap_or([0u8; 32]));
            // Skip USDC (id=0) and our known target tokens
            for id in [ma_id, ta_id] {
                let s = id.to_string();
                if id != U256::ZERO && s != YES_TOKEN && s != NO_TOKEN {
                    user_token_ids.insert(s);
                }
            }
        }
    }
    // Print first 20 unique token IDs
    for (i, tid) in user_token_ids.iter().take(20).enumerate() {
        println!("  [{}] {}", i, tid);
    }
    println!("  Total unique non-target token IDs: {}", user_token_ids.len());

    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn connect_rpc() -> anyhow::Result<alloy::providers::RootProvider> {
    // Try env var first, then fallback to public RPCs
    if let Ok(url) = std::env::var("POLYGON_HTTP_URL") {
        if !url.is_empty() {
            println!("  Using POLYGON_HTTP_URL from env");
            let provider = alloy::providers::RootProvider::new_http(url.parse()?);
            provider.get_block_number().await?;
            return Ok(provider);
        }
    }

    for url in RPC_URLS {
        println!("  Trying {}...", url);
        match try_connect(url).await {
            Ok(p) => return Ok(p),
            Err(e) => {
                println!("  Failed: {}", e);
                continue;
            }
        }
    }
    anyhow::bail!("Could not connect to any Polygon RPC")
}

async fn try_connect(url: &str) -> anyhow::Result<alloy::providers::RootProvider> {
    let provider = alloy::providers::RootProvider::new_http(url.parse()?);
    let block = provider.get_block_number().await?;
    println!("  Connected! Current block: {}", block);
    Ok(provider)
}

/// Query logs in chunks to respect free-tier block range limits.
async fn get_logs_chunked(
    provider: &alloy::providers::RootProvider,
    base_filter: &Filter,
    from: u64,
    to: u64,
) -> anyhow::Result<Vec<alloy::rpc::types::Log>> {
    let mut all_logs = Vec::new();
    let mut start = from;
    let total_chunks = ((to - from) / CHUNK_SIZE) + 1;
    let mut chunk_num = 0u64;
    while start <= to {
        let end = (start + CHUNK_SIZE).min(to);
        chunk_num += 1;
        if total_chunks > 1 {
            print!("      chunk {}/{} (blocks {}..{})...", chunk_num, total_chunks, start, end);
        }
        let filter = base_filter.clone().from_block(start).to_block(end);
        let logs = provider.get_logs(&filter).await?;
        if total_chunks > 1 {
            println!(" {} logs", logs.len());
        }
        all_logs.extend(logs);
        start = end + 1;
    }
    Ok(all_logs)
}

/// Extract raw ancillary text from ProposePrice event data.
fn extract_ancillary_text(data: &[u8]) -> Option<String> {
    if data.len() < 96 {
        return None;
    }
    let offset_bytes: [u8; 32] = data[64..96].try_into().ok()?;
    let offset: usize = U256::from_be_bytes(offset_bytes).try_into().ok()?;
    if offset == 0 || offset + 32 > data.len() {
        return None;
    }
    let len_bytes: [u8; 32] = data[offset..offset + 32].try_into().ok()?;
    let len: usize = U256::from_be_bytes(len_bytes).try_into().ok()?;
    if len == 0 || offset + 32 + len > data.len() {
        return None;
    }
    let ancillary = &data[offset + 32..offset + 32 + len];
    std::str::from_utf8(ancillary).ok().map(|s| s.to_string())
}

/// Extract market_id from ancillary data in ProposePrice event data.
/// Replicates logic from onchain/monitor.rs extract_market_id_from_ancillary
fn extract_market_id(data: &[u8]) -> Option<String> {
    if data.len() < 96 {
        return None;
    }

    let offset_bytes: [u8; 32] = data[64..96].try_into().ok()?;
    let offset: usize = U256::from_be_bytes(offset_bytes).try_into().ok()?;

    if offset == 0 || offset + 32 > data.len() {
        return None;
    }

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

/// Extract proposed_price from ProposePrice event data
fn extract_proposed_price(data: &[u8]) -> i64 {
    if data.len() >= 128 {
        let bytes: [u8; 32] = data[96..128].try_into().unwrap_or([0u8; 32]);
        let val = U256::from_be_bytes(bytes);
        if val > U256::ZERO { 1 } else { 0 }
    } else {
        0
    }
}

fn fmt_ts(ts: u64) -> String {
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("ts={}", ts))
}
