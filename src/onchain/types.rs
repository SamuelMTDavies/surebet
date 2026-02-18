//! On-chain signal types emitted by the Polygon event monitor.

use alloy::primitives::{Address, B256, U256};

/// Signals emitted by the on-chain monitor, consumed by the main event loop.
#[derive(Debug, Clone)]
pub enum OnChainSignal {
    /// A new condition was prepared on the CTF contract (new market created).
    NewMarket {
        condition_id: B256,
        oracle: Address,
        question_id: B256,
        outcome_count: u32,
        /// True if outcome_count > 2 (prime split arb target).
        is_multi_outcome: bool,
        block_number: u64,
        timestamp: u64,
    },

    /// A resolution was proposed on the UMA Oracle Adapter.
    /// This is the critical sniping window — stale orders still sit on the CLOB.
    ResolutionProposed {
        question_id: B256,
        proposed_price: i64,
        proposer: Address,
        block_number: u64,
        timestamp: u64,
    },

    /// A previously proposed resolution was disputed.
    /// Cancel any orders placed based on the now-disputed proposal.
    ResolutionDisputed {
        question_id: B256,
        block_number: u64,
        timestamp: u64,
    },

    /// A condition was resolved on-chain (final settlement).
    /// Triggers redemption logic.
    ResolutionFinalised {
        condition_id: B256,
        oracle: Address,
        question_id: B256,
        outcome_count: u32,
        payout_numerators: Vec<U256>,
        block_number: u64,
        timestamp: u64,
    },

    /// A new token was registered on a CTF Exchange, confirming
    /// the market is live for trading.
    TokenRegistered {
        token_id: U256,
        condition_id: B256,
        exchange: Address,
        block_number: u64,
        timestamp: u64,
    },

    /// A position was split on the Neg Risk Adapter.
    /// Detects when others are splitting, which temporarily creates
    /// order book imbalances.
    PositionSplit {
        condition_id: B256,
        collateral_amount: U256,
        block_number: u64,
        timestamp: u64,
    },

    /// Positions were merged on the Neg Risk Adapter.
    PositionsMerged {
        condition_id: B256,
        merge_amount: U256,
        block_number: u64,
        timestamp: u64,
    },

    /// The WebSocket connection to the Polygon node was established.
    Connected,

    /// The WebSocket connection was lost (will auto-reconnect).
    Disconnected {
        reason: String,
    },

    /// Events were recovered after a gap (reconnection or startup lookback).
    GapRecovered {
        from_block: u64,
        to_block: u64,
        events_found: usize,
    },
}

impl std::fmt::Display for OnChainSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewMarket { condition_id, outcome_count, .. } => {
                write!(f, "NewMarket(cid={}, outcomes={})", &format!("{condition_id}")[..14], outcome_count)
            }
            Self::ResolutionProposed { question_id, proposed_price, .. } => {
                write!(f, "ResolutionProposed(qid={}, price={})", &format!("{question_id}")[..14], proposed_price)
            }
            Self::ResolutionDisputed { question_id, .. } => {
                write!(f, "ResolutionDisputed(qid={})", &format!("{question_id}")[..14])
            }
            Self::ResolutionFinalised { condition_id, .. } => {
                write!(f, "ResolutionFinalised(cid={})", &format!("{condition_id}")[..14])
            }
            Self::TokenRegistered { token_id, .. } => {
                write!(f, "TokenRegistered(tid={})", token_id)
            }
            Self::PositionSplit { condition_id, .. } => {
                write!(f, "PositionSplit(cid={})", &format!("{condition_id}")[..14])
            }
            Self::PositionsMerged { condition_id, .. } => {
                write!(f, "PositionsMerged(cid={})", &format!("{condition_id}")[..14])
            }
            Self::Connected => write!(f, "Connected"),
            Self::Disconnected { reason } => write!(f, "Disconnected({})", reason),
            Self::GapRecovered { from_block, to_block, events_found } => {
                write!(f, "GapRecovered({}..{}, {} events)", from_block, to_block, events_found)
            }
        }
    }
}
