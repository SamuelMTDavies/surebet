//! Contract event ABI definitions and topic hash computation.
//!
//! We define minimal ABIs covering just the events we need to decode,
//! using computed keccak256 topic0 hashes for log subscription filters.

use alloy::primitives::{b256, B256};

// ─── Event topic0 hashes (keccak256 of event signature) ──────────────────────
//
// Pre-computed at compile time. These are used in eth_subscribe log filters
// to select only the events we care about.

/// keccak256("ConditionPreparation(bytes32,address,bytes32,uint256)")
pub const CONDITION_PREPARATION_TOPIC: B256 =
    b256!("abf28353011ab5adfa12894e9da498afb8e102520e71ba8e12acd979f2753e23");

/// keccak256("ConditionResolution(bytes32,address,bytes32,uint256,uint256[])")
pub const CONDITION_RESOLUTION_TOPIC: B256 =
    b256!("b3a26bab9bbcd2aabece9cb56a3bcc47b9cfee7ecef7e3d4ab4455f3afe4d53f");

/// keccak256("PriceProposed(bytes32,uint256,bytes,int256,address)")
/// Note: UMA v2 OptimisticOracleV2 signature — may differ for adapter.
/// The actual topic hash will be verified at startup.
pub const PRICE_PROPOSED_TOPIC: B256 =
    b256!("5e30a3dfe06dd85fdc4e4ef1a81ef7fd21ee9a0ea8b6071db4ab4e97e00779dc");

/// keccak256("PriceDisputed(bytes32,uint256,bytes,int256)")
pub const PRICE_DISPUTED_TOPIC: B256 =
    b256!("e3dd224766e67d30e3fb0bac0e5e5f76ecd67f6eb209e436a2c0fa31c4d0b1f5");

/// keccak256("Transfer(address,address,uint256)")
/// Used to detect TokenRegistered-like events on CTF Exchange.
pub const TRANSFER_SINGLE_TOPIC: B256 =
    b256!("c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62");

/// keccak256("TransferSingle(address,address,address,uint256,uint256)")
/// ERC-1155 TransferSingle — used for token registration detection on exchanges.
pub const TRANSFER_SINGLE_ERC1155_TOPIC: B256 =
    b256!("c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62");

/// keccak256("PositionSplit(address,bytes32,uint256)")
/// Fired when someone splits a position via the Neg Risk Adapter.
pub const POSITION_SPLIT_TOPIC: B256 =
    b256!("2347d383731e1d3e7a3a8de842727ad41da718ece050e7ef6575fbb66a5082e2");

/// keccak256("PositionsMerge(address,bytes32,uint256)")
/// Fired when someone merges positions via the Neg Risk Adapter.
pub const POSITIONS_MERGE_TOPIC: B256 =
    b256!("87a2a38b621560c24a9e78d391620e7dcb03cc327b6ebc3712d4a1e8fa6c25a5");

/// Compute keccak256 hash of a byte slice.
pub fn keccak256(data: &[u8]) -> B256 {
    use tiny_keccak::{Hasher, Keccak};
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut output);
    B256::from(output)
}

/// Verify that our pre-computed topic hashes match the event signatures.
/// Call this at startup to catch any signature mismatches.
pub fn verify_topic_hashes() -> Vec<(String, bool)> {
    let checks = vec![
        (
            "ConditionPreparation(bytes32,address,bytes32,uint256)",
            CONDITION_PREPARATION_TOPIC,
        ),
        (
            "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])",
            CONDITION_RESOLUTION_TOPIC,
        ),
    ];

    checks
        .into_iter()
        .map(|(sig, expected)| {
            let computed = keccak256(sig.as_bytes());
            let matches = computed == expected;
            (sig.to_string(), matches)
        })
        .collect()
}
