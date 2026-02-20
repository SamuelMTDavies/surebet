//! Shared library modules for the surebet trading engine.
//!
//! Re-exports modules needed by standalone binaries (e.g. `harvester`)
//! without duplicating code from the main binary.

pub mod auth;
pub mod config;
pub mod harvester;
pub mod orderbook;
pub mod ws;
