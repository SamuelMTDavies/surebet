//! CTF admin CLI: merge, redeem, or inspect CTF positions on-chain.
//!
//! All operations are direct on-chain calls — they bypass Polymarket's CLOB
//! API entirely (so they work even when the CLOB is geoblocked or down).
//!
//! Usage:
//!   cargo run --bin ctf_admin -- merge   --condition-id 0x... --amount 2.0
//!   cargo run --bin ctf_admin -- redeem  --condition-id 0x...
//!   cargo run --bin ctf_admin -- balance --condition-id 0x...
//!
//! `merge`   burns equal amounts of YES + NO outcome tokens to recover USDC.e
//!           before resolution.
//! `redeem`  claims winnings from a resolved market.
//! `balance` reads on-chain ERC-1155 balances for the YES and NO position
//!           tokens of a given condition (read-only, no gas).
//!
//! Required env:
//!   POLYMARKET_PRIVATE_KEY   — wallet that holds the tokens
//!   POLYGON_HTTP_URL         — HTTPS Polygon RPC (falls back to polygon-rpc.com)

use std::str::FromStr;

use alloy::primitives::{Address, B256, U256, address};
use alloy::providers::ProviderBuilder;
use alloy::signers::Signer;
use alloy::signers::local::LocalSigner;
use alloy::sol;
use anyhow::{Context, Result, anyhow, bail};
use polymarket_client_sdk::ctf::types::{
    CollectionIdRequest, MergePositionsRequest, PositionIdRequest, RedeemPositionsRequest,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

const CTF_CONTRACT: Address = address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");

sol! {
    #[sol(rpc)]
    interface IConditionalTokensERC1155 {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
    }
}

const USDC: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
const POLYGON_CHAIN_ID: u64 = 137;
const DEFAULT_RPC: &str = "https://polygon-rpc.com";

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  ctf_admin merge      --condition-id 0x... --amount <USDC>");
    eprintln!("  ctf_admin redeem     --condition-id 0x...");
    eprintln!("  ctf_admin balance    --condition-id 0x...");
    eprintln!("  ctf_admin resolution --condition-id 0x...");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  ctf_admin merge      --condition-id 0xd387bfe6... --amount 2.0");
    eprintln!("  ctf_admin redeem     --condition-id 0xd387bfe6...");
    eprintln!("  ctf_admin balance    --condition-id 0xd387bfe6...");
    eprintln!("  ctf_admin resolution --condition-id 0xd387bfe6...");
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        bail!("missing subcommand");
    }

    let cmd = args[1].as_str();
    let condition_id_str = arg_value(&args, "--condition-id")
        .ok_or_else(|| anyhow!("--condition-id is required"))?;
    let condition_id =
        B256::from_str(&condition_id_str).context("invalid --condition-id (expected 0x...)")?;

    let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
        .context("POLYMARKET_PRIVATE_KEY env var not set")?;
    let rpc_url = std::env::var("POLYGON_HTTP_URL").unwrap_or_else(|_| DEFAULT_RPC.to_string());

    let signer = LocalSigner::from_str(&private_key)
        .context("invalid POLYMARKET_PRIVATE_KEY")?
        .with_chain_id(Some(POLYGON_CHAIN_ID));
    let wallet_addr = signer.address();
    println!("wallet:    {wallet_addr}");
    println!("rpc:       {rpc_url}");
    println!("condition: {condition_id}");

    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(&rpc_url)
        .await
        .context("failed to connect to Polygon RPC")?;

    let ctf_client = polymarket_client_sdk::ctf::Client::new(provider, POLYGON_CHAIN_ID)
        .context("failed to init CTF client")?;

    match cmd {
        "merge" => {
            let amount_str = arg_value(&args, "--amount")
                .ok_or_else(|| anyhow!("--amount is required for merge (in USDC)"))?;
            let amount_dec = Decimal::from_str(&amount_str).context("invalid --amount")?;
            if amount_dec <= Decimal::ZERO {
                bail!("--amount must be > 0");
            }
            let amount_units = decimal_to_usdc_units(amount_dec)?;
            println!("action:    merge");
            println!("amount:    ${amount_dec} ({amount_units} units)");
            println!();

            let req = MergePositionsRequest::for_binary_market(USDC, condition_id, amount_units);
            let resp = ctf_client
                .merge_positions(&req)
                .await
                .map_err(|e| anyhow!("merge failed: {e}"))?;
            println!("✓ merge submitted");
            println!("  tx_hash: {:?}", resp.transaction_hash);
            println!("  block:   {}", resp.block_number);
        }
        "redeem" => {
            println!("action:    redeem");
            println!();

            let req = RedeemPositionsRequest::for_binary_market(USDC, condition_id);
            let resp = ctf_client
                .redeem_positions(&req)
                .await
                .map_err(|e| anyhow!("redeem failed: {e}"))?;
            println!("✓ redeem submitted");
            println!("  tx_hash: {:?}", resp.transaction_hash);
            println!("  block:   {}", resp.block_number);
        }
        "balance" => {
            println!("action:    balance (read-only)");
            println!();

            // Compute the YES and NO position IDs by chaining
            // getCollectionId → getPositionId on the CTF contract.
            let mut holdings: Vec<(&str, U256, U256)> = Vec::new();
            for (label, index_set) in [("YES", U256::from(1u64)), ("NO", U256::from(2u64))] {
                let coll_req = CollectionIdRequest::builder()
                    .parent_collection_id(B256::ZERO)
                    .condition_id(condition_id)
                    .index_set(index_set)
                    .build();
                let coll = ctf_client
                    .collection_id(&coll_req)
                    .await
                    .map_err(|e| anyhow!("collection_id({label}) failed: {e}"))?;
                let pos_req = PositionIdRequest::builder()
                    .collateral_token(USDC)
                    .collection_id(coll.collection_id)
                    .build();
                let pos = ctf_client
                    .position_id(&pos_req)
                    .await
                    .map_err(|e| anyhow!("position_id({label}) failed: {e}"))?;

                // Read the wallet's ERC-1155 balance for this position id
                let erc1155 =
                    IConditionalTokensERC1155::new(CTF_CONTRACT, ctf_client.provider().clone());
                let balance = erc1155
                    .balanceOf(wallet_addr, pos.position_id)
                    .call()
                    .await
                    .map_err(|e| anyhow!("balanceOf({label}) failed: {e}"))?;
                holdings.push((label, pos.position_id, balance));
            }

            for (label, pos_id, raw) in &holdings {
                // CTF outcome tokens use the same 6-decimal scale as USDC.
                let display = Decimal::from(raw.to::<u128>())
                    / Decimal::from(1_000_000u64);
                println!("  {label}");
                println!("    position_id: {pos_id}");
                println!("    raw balance: {raw}");
                println!("    tokens:      {display}");
                println!();
            }

            // Quick summary of what's actionable
            let yes_raw = holdings[0].2;
            let no_raw = holdings[1].2;
            if yes_raw == U256::ZERO && no_raw == U256::ZERO {
                println!("→ no holdings for this condition");
            } else if yes_raw > U256::ZERO && no_raw > U256::ZERO {
                let mergeable = yes_raw.min(no_raw);
                let merge_dec = Decimal::from(mergeable.to::<u128>())
                    / Decimal::from(1_000_000u64);
                println!("→ MERGEABLE: {merge_dec} USDC.e (burn {mergeable} of each)");
                println!("  After resolution, REDEEMABLE for whichever side wins");
            } else if yes_raw > U256::ZERO {
                let dec = Decimal::from(yes_raw.to::<u128>()) / Decimal::from(1_000_000u64);
                println!("→ YES-only: {dec} tokens");
                println!("  Cannot merge (need matching NO). Wait for resolution + redeem.");
            } else {
                let dec = Decimal::from(no_raw.to::<u128>()) / Decimal::from(1_000_000u64);
                println!("→ NO-only: {dec} tokens");
                println!("  Cannot merge (need matching YES). Wait for resolution + redeem.");
            }
        }
        "resolution" => {
            println!("action:    resolution (read-only)");
            println!();

            let provider = ctf_client.provider().clone();
            let ctf = IConditionalTokensERC1155::new(CTF_CONTRACT, provider);

            let denom = ctf
                .payoutDenominator(condition_id)
                .call()
                .await
                .map_err(|e| anyhow!("payoutDenominator failed: {e}"))?;
            let denom_u128: u128 = denom.try_into().unwrap_or(0);

            if denom_u128 == 0 {
                println!("→ UNRESOLVED");
                println!("  payoutDenominator = 0 (UMA has not reported the outcome yet)");
                println!("  Cannot redeem until the market resolves.");
            } else {
                let yes = ctf
                    .payoutNumerators(condition_id, U256::from(0u64))
                    .call()
                    .await
                    .map_err(|e| anyhow!("payoutNumerators(0) failed: {e}"))?;
                let no = ctf
                    .payoutNumerators(condition_id, U256::from(1u64))
                    .call()
                    .await
                    .map_err(|e| anyhow!("payoutNumerators(1) failed: {e}"))?;
                let yes_u128: u128 = yes.try_into().unwrap_or(0);
                let no_u128: u128 = no.try_into().unwrap_or(0);
                println!("→ RESOLVED");
                println!("  payoutDenominator: {denom_u128}");
                println!("  YES payout:        {yes_u128}");
                println!("  NO  payout:        {no_u128}");
                println!();
                if yes_u128 > 0 && no_u128 == 0 {
                    println!("  Winner: YES");
                } else if no_u128 > 0 && yes_u128 == 0 {
                    println!("  Winner: NO");
                } else {
                    println!("  Outcome: tied / split payout");
                }
                println!("  → Run `ctf_admin redeem --condition-id {condition_id}` to claim.");
            }
        }
        other => {
            print_usage();
            bail!("unknown subcommand: {other}");
        }
    }

    Ok(())
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Convert a Decimal USDC amount (e.g. 2.0) to 6-decimal base units (2_000_000).
fn decimal_to_usdc_units(amount: Decimal) -> Result<U256> {
    let scaled = (amount * Decimal::from(1_000_000u64)).round();
    let units = scaled
        .to_u128()
        .ok_or_else(|| anyhow!("amount overflow"))?;
    Ok(U256::from(units))
}