//! On-chain event monitoring for Polymarket on Polygon.
//!
//! Subscribes to blockchain events via WebSocket RPC to detect:
//! 1. New market creation (ConditionPreparation) — earliest signal for split arb
//! 2. Resolution proposals (PriceProposed on UMA Oracle) — sniping window
//! 3. Resolution disputes (PriceDisputed) — cancel snipe orders
//! 4. Final settlement (ConditionResolution) — trigger redemptions
//! 5. Neg Risk splits/merges — conversion opportunity detection
//!
//! Architecture:
//! - `OnChainMonitor`: spawns a background task that maintains a WebSocket
//!   connection to a Polygon RPC node, with automatic reconnection
//! - `MarketCache`: maps on-chain identifiers to Polymarket API data
//! - Signals are emitted via `tokio::sync::mpsc` channel as `OnChainSignal` variants
//! - The main event loop in `main.rs` consumes these signals and routes them
//!   to the appropriate strategy engines

pub mod abi;
pub mod cache;
pub mod monitor;
pub mod types;

pub use cache::MarketCache;
pub use monitor::OnChainMonitor;
pub use types::OnChainSignal;
