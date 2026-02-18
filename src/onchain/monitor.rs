//! Core on-chain event monitor.
//!
//! Subscribes to Polygon WebSocket RPC for log events on Polymarket's
//! CTF, Exchange, UMA Oracle, and Neg Risk Adapter contracts.
//! Decodes events and emits typed `OnChainSignal`s via a tokio channel.
//!
//! Features:
//! - Automatic reconnection with exponential backoff
//! - Block gap recovery on reconnect (queries eth_getLogs for missed range)
//! - Persistent checkpoint of last processed block
//! - Latency monitoring (block timestamp → signal emission)

use crate::config::OnChainConfig;
use crate::onchain::abi;
use crate::onchain::cache::MarketCache;
use crate::onchain::types::OnChainSignal;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{Filter, Log};
use futures_util::StreamExt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// The on-chain monitor that subscribes to Polygon events.
pub struct OnChainMonitor {
    config: OnChainConfig,
    signal_tx: mpsc::UnboundedSender<OnChainSignal>,
    cache: MarketCache,
    /// Last processed block, persisted to disk for gap recovery.
    last_block: Arc<AtomicU64>,
}

impl OnChainMonitor {
    pub fn new(
        config: OnChainConfig,
        signal_tx: mpsc::UnboundedSender<OnChainSignal>,
        cache: MarketCache,
    ) -> Self {
        // Load checkpoint from disk if available
        let last_block = Arc::new(AtomicU64::new(0));
        if let Ok(contents) = std::fs::read_to_string(&config.checkpoint_path) {
            if let Ok(block) = contents.trim().parse::<u64>() {
                last_block.store(block, Ordering::SeqCst);
                info!(block = block, "loaded on-chain checkpoint");
            }
        }

        Self {
            config,
            signal_tx,
            cache,
            last_block,
        }
    }

    /// Start the monitor in a background task. Returns immediately.
    /// The monitor will reconnect automatically on WebSocket failures.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_forever().await;
        })
    }

    /// Build the list of WebSocket URLs to rotate through: primary first, then fallbacks.
    fn ws_urls(&self) -> Vec<&str> {
        let mut urls: Vec<&str> = Vec::new();
        if !self.config.polygon_ws_url.is_empty() {
            urls.push(&self.config.polygon_ws_url);
        }
        for url in &self.config.fallback_ws_urls {
            if !url.is_empty() && urls.iter().all(|u| *u != url.as_str()) {
                urls.push(url);
            }
        }
        urls
    }

    /// Main loop: connect, subscribe, process events, reconnect on failure.
    /// Rotates through primary + fallback WebSocket URLs on consecutive failures.
    async fn run_forever(&self) {
        let max_backoff = Duration::from_secs(60);
        let urls = self.ws_urls();
        if urls.is_empty() {
            error!("no Polygon WebSocket URLs configured (primary or fallback)");
            return;
        }
        let mut url_index = 0;
        let mut consecutive_failures: usize = 0;

        loop {
            let url = urls[url_index];
            info!(url = %url, provider = url_index + 1, total = urls.len(), "connecting to Polygon WebSocket");

            match self.run_session_with_url(url).await {
                Ok(()) => {
                    info!("Polygon WebSocket session ended cleanly");
                    consecutive_failures = 0;
                    // On clean disconnect, stay on the same provider
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let is_rate_limited = err_str.contains("429")
                        || err_str.contains("Too Many Requests")
                        || err_str.contains("Space limit exceeded");

                    if is_rate_limited {
                        warn!(
                            url = %url,
                            "provider rate limited / quota exceeded, rotating"
                        );
                    } else {
                        error!(url = %url, error = %e, "Polygon WebSocket session error");
                    }

                    let _ = self.signal_tx.send(OnChainSignal::Disconnected {
                        reason: err_str,
                    });

                    consecutive_failures += 1;
                    url_index = (url_index + 1) % urls.len();
                }
            }

            // Backoff scales with how many providers have failed in a row.
            // If we still have untried providers in this rotation, try them
            // quickly (1s). Only do a real backoff once we've cycled through all.
            let backoff = if consecutive_failures == 0 {
                Duration::from_secs(1)
            } else if consecutive_failures < urls.len() {
                // Still have providers to try — rotate fast
                Duration::from_secs(2)
            } else {
                // All providers failed this cycle — back off harder
                let cycle = consecutive_failures / urls.len();
                let secs = (2u64).pow(cycle.min(5) as u32).min(max_backoff.as_secs());
                Duration::from_secs(secs)
            };

            info!(
                backoff_secs = backoff.as_secs(),
                next_url = %urls[url_index],
                failures = consecutive_failures,
                "reconnecting to Polygon WebSocket"
            );
            tokio::time::sleep(backoff).await;
        }
    }

    /// A single WebSocket session: connect, subscribe, process live events.
    async fn run_session_with_url(&self, url: &str) -> anyhow::Result<()> {
        let ws = WsConnect::new(url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let _ = self.signal_tx.send(OnChainSignal::Connected);
        info!("Polygon WebSocket connected");

        // Log current block for reference (no gap recovery — forward-only)
        let current_block = provider.get_block_number().await?;
        self.update_checkpoint(current_block);
        info!(block = current_block, "current Polygon block, streaming forward");

        // Build the subscription filter
        let filter = self.build_filter();

        // Subscribe to new logs
        let sub = provider.subscribe_logs(&filter).await?;
        let mut stream = sub.into_stream();

        info!("subscribed to on-chain events");

        while let Some(log) = stream.next().await {
            let receive_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            if let Err(e) = self.process_log(&log, receive_time).await {
                warn!(error = %e, "failed to process log");
            }

            // Update checkpoint
            if let Some(block_num) = log.block_number {
                self.update_checkpoint(block_num);
            }
        }

        // Stream ended — will reconnect
        Ok(())
    }

    /// Build the log filter covering all contracts and event topics.
    fn build_filter(&self) -> Filter {
        let mut addresses: Vec<Address> = Vec::new();

        // CTF contract (ConditionPreparation, ConditionResolution)
        if let Ok(addr) = Address::from_str(&self.config.ctf_address) {
            addresses.push(addr);
        }

        // CTF Exchange (TokenRegistered / TransferSingle)
        if let Ok(addr) = Address::from_str(&self.config.ctf_exchange) {
            addresses.push(addr);
        }

        // Neg Risk CTF Exchange
        if let Ok(addr) = Address::from_str(&self.config.neg_risk_ctf_exchange) {
            addresses.push(addr);
        }

        // Neg Risk Adapter (PositionSplit, PositionsMerge)
        if let Ok(addr) = Address::from_str(&self.config.neg_risk_adapter) {
            addresses.push(addr);
        }

        // UMA Optimistic Oracle V2 (ProposePrice, DisputePrice)
        if !self.config.uma_oracle_adapter.is_empty() {
            if let Ok(addr) = Address::from_str(&self.config.uma_oracle_adapter) {
                addresses.push(addr);
            }
        }

        // Event topics we want to subscribe to
        let topics = vec![
            abi::CONDITION_PREPARATION_TOPIC,
            abi::CONDITION_RESOLUTION_TOPIC,
            abi::PROPOSE_PRICE_TOPIC,
            abi::DISPUTE_PRICE_TOPIC,
            abi::TRANSFER_SINGLE_ERC1155_TOPIC,
            abi::POSITION_SPLIT_TOPIC,
            abi::POSITIONS_MERGE_TOPIC,
        ];

        Filter::new().address(addresses).event_signature(topics)
    }

    /// Process a single log entry, decode the event, and emit a signal.
    async fn process_log(&self, log: &Log, receive_time_ms: u64) -> anyhow::Result<()> {
        let topic0 = log
            .topic0()
            .ok_or_else(|| anyhow::anyhow!("log has no topic0"))?;

        let block_number = log.block_number.unwrap_or(0);

        // Estimate block timestamp (Polygon ~2s blocks).
        // For precise timestamp we'd need to fetch the block, but that adds
        // latency. We'll use the receive_time as our reference and log the
        // delta separately.
        let block_timestamp = log.block_timestamp.unwrap_or(0);

        // Measure latency
        if block_timestamp > 0 {
            let latency_ms = receive_time_ms.saturating_sub(block_timestamp * 1000);
            if latency_ms > self.config.max_latency_ms {
                warn!(
                    latency_ms = latency_ms,
                    block = block_number,
                    threshold_ms = self.config.max_latency_ms,
                    "on-chain event latency exceeds threshold"
                );
            }
            debug!(
                latency_ms = latency_ms,
                block = block_number,
                "event latency"
            );
        }

        match *topic0 {
            t if t == abi::CONDITION_PREPARATION_TOPIC => {
                self.handle_condition_preparation(log, block_number, block_timestamp)
                    .await?;
            }
            t if t == abi::CONDITION_RESOLUTION_TOPIC => {
                self.handle_condition_resolution(log, block_number, block_timestamp, receive_time_ms)?;
            }
            t if t == abi::PROPOSE_PRICE_TOPIC => {
                self.handle_propose_price(log, block_number, block_timestamp, receive_time_ms)?;
            }
            t if t == abi::DISPUTE_PRICE_TOPIC => {
                self.handle_dispute_price(log, block_number, block_timestamp, receive_time_ms)?;
            }
            t if t == abi::TRANSFER_SINGLE_ERC1155_TOPIC => {
                self.handle_transfer_single(log, block_number, block_timestamp)?;
            }
            t if t == abi::POSITION_SPLIT_TOPIC => {
                self.handle_position_split(log, block_number, block_timestamp)?;
            }
            t if t == abi::POSITIONS_MERGE_TOPIC => {
                self.handle_positions_merge(log, block_number, block_timestamp)?;
            }
            _ => {
                debug!(topic = %topic0, "unrecognised event topic");
            }
        }

        Ok(())
    }

    /// ConditionPreparation(bytes32 indexed conditionId, address indexed oracle,
    ///                      bytes32 indexed questionId, uint256 outcomeSlotCount)
    async fn handle_condition_preparation(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        // Indexed params are in topics[1..4]
        let condition_id = log
            .topics()
            .get(1)
            .copied()
            .unwrap_or_default();
        let oracle = Address::from_slice(
            &log.topics()
                .get(2)
                .copied()
                .unwrap_or_default()
                .0[12..],
        );
        let question_id = log
            .topics()
            .get(3)
            .copied()
            .unwrap_or_default();

        // outcomeSlotCount is in the data field (non-indexed)
        let outcome_count = if log.data().data.len() >= 32 {
            let bytes: [u8; 32] = log.data().data[0..32].try_into().unwrap_or([0u8; 32]);
            U256::from_be_bytes(bytes)
                .try_into()
                .unwrap_or(0u32)
        } else {
            0u32
        };

        let is_multi = outcome_count > 2;

        info!(
            condition_id = %condition_id,
            oracle = %oracle,
            question_id = %question_id,
            outcomes = outcome_count,
            multi_outcome = is_multi,
            block = block_number,
            "NEW MARKET: ConditionPreparation"
        );

        let _ = self.signal_tx.send(OnChainSignal::NewMarket {
            condition_id,
            oracle,
            question_id,
            outcome_count,
            is_multi_outcome: is_multi,
            block_number,
            timestamp,
        });

        // Async: query Gamma API to populate cache for this new condition
        let cache = self.cache.clone();
        let cid_hex = format!("{:x}", condition_id);
        tokio::spawn(async move {
            cache.lookup_condition(&cid_hex).await;
        });

        Ok(())
    }

    /// ConditionResolution(bytes32 indexed conditionId, address indexed oracle,
    ///                     bytes32 indexed questionId, uint256 outcomeSlotCount,
    ///                     uint256[] payoutNumerators)
    fn handle_condition_resolution(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
        receive_time_ms: u64,
    ) -> anyhow::Result<()> {
        let condition_id = log.topics().get(1).copied().unwrap_or_default();
        let oracle = Address::from_slice(
            &log.topics().get(2).copied().unwrap_or_default().0[12..],
        );
        let question_id = log.topics().get(3).copied().unwrap_or_default();

        // Data contains: outcomeSlotCount (uint256), then dynamic array payoutNumerators
        let data = &log.data().data;
        let outcome_count = if data.len() >= 32 {
            let bytes: [u8; 32] = data[0..32].try_into().unwrap_or([0u8; 32]);
            U256::from_be_bytes(bytes)
                .try_into()
                .unwrap_or(0u32)
        } else {
            0u32
        };

        // Decode payoutNumerators dynamic array
        // ABI encoding: offset (32 bytes), then length (32 bytes), then elements
        let payout_numerators = if data.len() >= 96 {
            // offset at data[32..64], then array starts at that offset
            let offset_bytes: [u8; 32] = data[32..64].try_into().unwrap_or([0u8; 32]);
            let offset = U256::from_be_bytes(offset_bytes)
                .try_into()
                .unwrap_or(0usize);

            if offset + 32 <= data.len() {
                let len_bytes: [u8; 32] = data[offset..offset + 32]
                    .try_into()
                    .unwrap_or([0u8; 32]);
                let len: usize = U256::from_be_bytes(len_bytes)
                    .try_into()
                    .unwrap_or(0);

                let mut payouts = Vec::with_capacity(len);
                for i in 0..len {
                    let start = offset + 32 + i * 32;
                    if start + 32 <= data.len() {
                        let elem_bytes: [u8; 32] =
                            data[start..start + 32].try_into().unwrap_or([0u8; 32]);
                        payouts.push(U256::from_be_bytes(elem_bytes));
                    }
                }
                payouts
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        info!(
            condition_id = %condition_id,
            question_id = %question_id,
            outcomes = outcome_count,
            payouts = ?payout_numerators,
            block = block_number,
            "RESOLUTION FINALISED: ConditionResolution"
        );

        let detected_at = std::time::Instant::now();
        let chain_latency_ms = if timestamp > 0 {
            receive_time_ms.saturating_sub(timestamp * 1000)
        } else {
            0
        };
        let _ = self.signal_tx.send(OnChainSignal::ResolutionFinalised {
            condition_id,
            oracle,
            question_id,
            outcome_count,
            payout_numerators,
            block_number,
            timestamp,
            detected_at,
            chain_latency_ms,
        });

        Ok(())
    }

    /// ProposePrice(address indexed requester, address indexed proposer,
    ///              bytes32 identifier, uint256 timestamp, bytes ancillaryData,
    ///              int256 proposedPrice, uint256 expirationTimestamp, address currency)
    ///
    /// Emitted by UMA Optimistic Oracle V2 (NOT the adapter).
    /// Topics: [sig, requester, proposer]
    /// Data:   [identifier(32), timestamp(32), ancillaryData_offset(32),
    ///          proposedPrice(32), expirationTimestamp(32), currency(32),
    ///          ancillaryData_length(32), ancillaryData_bytes(...)]
    fn handle_propose_price(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
        receive_time_ms: u64,
    ) -> anyhow::Result<()> {
        // topic[1] = requester (the UmaCtfAdapter that requested the price)
        let _requester = if let Some(t) = log.topics().get(1) {
            Address::from_slice(&t.0[12..])
        } else {
            Address::ZERO
        };

        // topic[2] = proposer (the address that proposed the resolution)
        let proposer = if let Some(t) = log.topics().get(2) {
            Address::from_slice(&t.0[12..])
        } else {
            Address::ZERO
        };

        let data = &log.data().data;

        // data[0..32]   = identifier (bytes32, e.g. "YES_OR_NO_QUERY")
        // data[32..64]  = timestamp (uint256)
        // data[64..96]  = offset to ancillaryData (uint256, typically 0xC0 = 192)
        // data[96..128] = proposedPrice (int256)
        // data[128..160] = expirationTimestamp (uint256)
        // data[160..192] = currency (address)
        // data[192..224] = ancillaryData length
        // data[224..]    = ancillaryData bytes

        let proposed_price = if data.len() >= 128 {
            let bytes: [u8; 32] = data[96..128].try_into().unwrap_or([0u8; 32]);
            // int256 — for Polymarket: 1e18 = Yes, 0 = No
            let val = U256::from_be_bytes(bytes);
            if val > U256::ZERO { 1i64 } else { 0i64 }
        } else {
            0i64
        };

        let expiration_ts = if data.len() >= 160 {
            let bytes: [u8; 32] = data[128..160].try_into().unwrap_or([0u8; 32]);
            U256::from_be_bytes(bytes).try_into().unwrap_or(0u64)
        } else {
            0u64
        };

        // Extract questionId from ancillaryData (contains market_id and question text)
        let question_id = extract_question_id_from_ancillary(data);

        info!(
            question_id = %question_id,
            proposed_price = proposed_price,
            proposer = %proposer,
            expiration_ts = expiration_ts,
            block = block_number,
            "RESOLUTION PROPOSED: ProposePrice on UMA Oracle"
        );

        let detected_at = std::time::Instant::now();
        let chain_latency_ms = if timestamp > 0 {
            receive_time_ms.saturating_sub(timestamp * 1000)
        } else {
            0
        };
        let _ = self.signal_tx.send(OnChainSignal::ResolutionProposed {
            question_id,
            proposed_price,
            proposer,
            block_number,
            timestamp,
            detected_at,
            chain_latency_ms,
        });

        Ok(())
    }

    /// DisputePrice(address indexed requester, address indexed proposer,
    ///              address indexed disputer, bytes32 identifier,
    ///              uint256 timestamp, bytes ancillaryData, int256 proposedPrice)
    ///
    /// Emitted by UMA Optimistic Oracle V2 when a proposal is disputed.
    /// Topics: [sig, requester, proposer, disputer]
    /// Data:   [identifier(32), timestamp(32), ancillaryData_offset(32),
    ///          proposedPrice(32), ancillaryData_length(32), ancillaryData_bytes(...)]
    fn handle_dispute_price(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
        receive_time_ms: u64,
    ) -> anyhow::Result<()> {
        let question_id = extract_question_id_from_ancillary(&log.data().data);

        warn!(
            question_id = %question_id,
            block = block_number,
            "RESOLUTION DISPUTED: DisputePrice on UMA Oracle"
        );

        let detected_at = std::time::Instant::now();
        let chain_latency_ms = if timestamp > 0 {
            receive_time_ms.saturating_sub(timestamp * 1000)
        } else {
            0
        };
        let _ = self.signal_tx.send(OnChainSignal::ResolutionDisputed {
            question_id,
            block_number,
            timestamp,
            detected_at,
            chain_latency_ms,
        });

        Ok(())
    }

    /// TransferSingle — ERC-1155 transfer that indicates token registration
    /// on a CTF Exchange contract.
    fn handle_transfer_single(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        // TransferSingle(address operator, address from, address to, uint256 id, uint256 value)
        // Topics: [sig, operator_indexed]
        // Data: [from, to, id, value] or topics may include from/to
        let data = &log.data().data;

        // For minting (from = 0x0), the token is being created/registered.
        // data layout: from(32), to(32), id(32), value(32)
        if data.len() >= 128 {
            let from_bytes: [u8; 32] = data[0..32].try_into().unwrap_or([0u8; 32]);
            let from = Address::from_slice(&from_bytes[12..]);

            // Only care about mints (from zero address)
            if from == Address::ZERO {
                let id_bytes: [u8; 32] = data[64..96].try_into().unwrap_or([0u8; 32]);
                let token_id = U256::from_be_bytes(id_bytes);

                // The token_id in Polymarket CTF encodes the conditionId
                // We can extract it or look it up in the cache
                let condition_id = token_id_to_condition_hint(token_id);

                let exchange = log.address();

                debug!(
                    token_id = %token_id,
                    exchange = %exchange,
                    block = block_number,
                    "TOKEN REGISTERED: TransferSingle mint"
                );

                let _ = self.signal_tx.send(OnChainSignal::TokenRegistered {
                    token_id,
                    condition_id,
                    exchange,
                    block_number,
                    timestamp,
                });
            }
        }

        Ok(())
    }

    /// PositionSplit(address indexed stakeholder, bytes32 conditionId, uint256 amount)
    fn handle_position_split(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        let data = &log.data().data;

        // conditionId and amount are in data (non-indexed after stakeholder)
        let condition_id = if data.len() >= 32 {
            B256::from_slice(&data[0..32])
        } else {
            B256::ZERO
        };

        let amount = if data.len() >= 64 {
            let bytes: [u8; 32] = data[32..64].try_into().unwrap_or([0u8; 32]);
            U256::from_be_bytes(bytes)
        } else {
            U256::ZERO
        };

        debug!(
            condition_id = %condition_id,
            amount = %amount,
            block = block_number,
            "POSITION SPLIT on Neg Risk Adapter"
        );

        let _ = self.signal_tx.send(OnChainSignal::PositionSplit {
            condition_id,
            collateral_amount: amount,
            block_number,
            timestamp,
        });

        Ok(())
    }

    /// PositionsMerge(address indexed stakeholder, bytes32 conditionId, uint256 amount)
    fn handle_positions_merge(
        &self,
        log: &Log,
        block_number: u64,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        let data = &log.data().data;

        let condition_id = if data.len() >= 32 {
            B256::from_slice(&data[0..32])
        } else {
            B256::ZERO
        };

        let amount = if data.len() >= 64 {
            let bytes: [u8; 32] = data[32..64].try_into().unwrap_or([0u8; 32]);
            U256::from_be_bytes(bytes)
        } else {
            U256::ZERO
        };

        debug!(
            condition_id = %condition_id,
            amount = %amount,
            block = block_number,
            "POSITIONS MERGED on Neg Risk Adapter"
        );

        let _ = self.signal_tx.send(OnChainSignal::PositionsMerged {
            condition_id,
            merge_amount: amount,
            block_number,
            timestamp,
        });

        Ok(())
    }

    /// Persist the last processed block number to disk.
    fn update_checkpoint(&self, block_number: u64) {
        let prev = self.last_block.load(Ordering::SeqCst);
        if block_number > prev {
            self.last_block.store(block_number, Ordering::SeqCst);
            // Write to disk (best-effort, don't block on I/O failure)
            if let Err(e) =
                std::fs::write(&self.config.checkpoint_path, block_number.to_string())
            {
                debug!(error = %e, "failed to write on-chain checkpoint");
            }
        }
    }
}

/// Extract a questionId from UMA Oracle event data by parsing ancillaryData.
///
/// ProposePrice data layout:
///   [0..32]    identifier (bytes32)
///   [32..64]   timestamp (uint256)
///   [64..96]   offset to ancillaryData (relative to data start)
///   [96..128]  proposedPrice (int256)
///   [128..160] expirationTimestamp (uint256)  — ProposePrice only
///   [160..192] currency (address)             — ProposePrice only
///   [offset..offset+32] ancillaryData length
///   [offset+32..] ancillaryData bytes
///
/// DisputePrice data layout:
///   [0..32]    identifier (bytes32)
///   [32..64]   timestamp (uint256)
///   [64..96]   offset to ancillaryData
///   [96..128]  proposedPrice (int256)
///   [offset..offset+32] ancillaryData length
///   [offset+32..] ancillaryData bytes
///
/// The ancillaryData for Polymarket contains UTF-8 text with an `initializer:`
/// suffix that holds the questionId as a hex string. Format:
///   "q: title: ..., description: ..., market_id: ..., initializer:<40-char hex>"
///
/// The last 40 hex chars after "initializer:" are the questionId's initializer
/// address, but for unique identification we hash the full ancillaryData to
/// derive a stable questionId matching what the adapter uses.
fn extract_question_id_from_ancillary(data: &[u8]) -> B256 {
    if data.len() < 96 {
        return B256::ZERO;
    }

    // The offset to ancillaryData is at data[64..96]
    let offset_bytes: [u8; 32] = data[64..96].try_into().unwrap_or([0u8; 32]);
    let offset: usize = U256::from_be_bytes(offset_bytes)
        .try_into()
        .unwrap_or(0);

    if offset == 0 || offset + 32 > data.len() {
        return B256::ZERO;
    }

    let len_bytes: [u8; 32] = data[offset..offset + 32]
        .try_into()
        .unwrap_or([0u8; 32]);
    let len: usize = U256::from_be_bytes(len_bytes)
        .try_into()
        .unwrap_or(0);

    if len == 0 || offset + 32 + len > data.len() {
        return B256::ZERO;
    }

    let ancillary = &data[offset + 32..offset + 32 + len];

    // Log the ancillary text for debugging
    if let Ok(text) = std::str::from_utf8(ancillary) {
        debug!(ancillary_len = len, "decoded ancillary data from UMA Oracle event");

        // Try to extract initializer from the tail — Polymarket appends it as
        // "initializer:<hex>" at the very end of the ancillary string.
        if let Some(pos) = text.rfind("initializer:") {
            let hex_str = &text[pos + "initializer:".len()..];
            debug!(initializer = %hex_str, "found initializer in ancillary data");
        }
    }

    // Derive a stable questionId by hashing the full ancillary data.
    // This matches how the UmaCtfAdapter computes questionId from the
    // ancillaryData + timestamp combination.
    use crate::onchain::abi::keccak256;
    keccak256(ancillary)
}

/// Attempt to derive a conditionId hint from a CTF token ID.
/// In the Gnosis CTF, token IDs are computed as:
///   tokenId = uint256(keccak256(abi.encodePacked(conditionId, indexSet)))
/// We can't reverse keccak256, so this returns a zero hash as placeholder.
/// The cache will be used for actual lookups.
fn token_id_to_condition_hint(_token_id: U256) -> B256 {
    // Cannot reverse keccak256 — the cache handles the actual mapping.
    B256::ZERO
}
