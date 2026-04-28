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
use polymarket_client_sdk_v2::ctf::types::{
    CollectionIdRequest, MergePositionsRequest, PositionIdRequest, RedeemNegRiskRequest,
    RedeemPositionsRequest,
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

    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    // Polymarket V2 CollateralOnramp / Offramp.
    // Onramp.wrap   pulls USDC.e from msg.sender (after approve) and mints pUSD 1:1 to `_to`.
    // Offramp.unwrap burns pUSD from msg.sender (after approve) and returns USDC.e 1:1 to `_to`.
    #[sol(rpc)]
    interface ICollateralRamp {
        function wrap(address _asset, address _to, uint256 _amount) external;
        function unwrap(address _asset, address _to, uint256 _amount) external;
    }
}

// Legacy bridged USDC. After Polymarket's V2 migration this is no longer the
// protocol's collateral — it's only relevant for one-time migration via the
// CollateralOnramp (wrap/unwrap subcommands).
const USDCE: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
// V2 collateral. Used for every position-related CTF call (split / merge /
// redeem / position-id derivation). Imported as `COLLATERAL` so the call sites
// read as the role rather than the specific token.
const PUSD: Address = address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");
const COLLATERAL: Address = PUSD;
const ONRAMP: Address = address!("0x93070a847efEf7F70739046A929D47a521F5B8ee");
// V2 exchange contracts. Both must be approved on pUSD so they can move the
// wallet's collateral when filling buys / locking quote-side liquidity. The
// V1 addresses (`0x4bFb41…8982E`, `0xC5d563…20f80a`) are deliberately omitted
// — they reject post-V2 signatures, so leaving them un-approved is harmless.
const CTF_EXCHANGE_V2: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const NEG_RISK_CTF_EXCHANGE_V2: Address =
    address!("0xe2222d279d744050d28e00520010520000310F59");
const NEG_RISK_ADAPTER: Address = address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296");
const POLYGON_CHAIN_ID: u64 = 137;
const DEFAULT_RPC: &str = "https://polygon-rpc.com";

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  ctf_admin merge           --condition-id 0x... --amount <USDC>");
    eprintln!("  ctf_admin redeem          --condition-id 0x...");
    eprintln!("  ctf_admin redeem-neg-risk --condition-id 0x... [--yes-amount <USDC>] [--no-amount <USDC>]");
    eprintln!("  ctf_admin balance         --condition-id 0x...");
    eprintln!("  ctf_admin resolution      --condition-id 0x...");
    eprintln!("  ctf_admin wrap            [--amount <USDC>]    # USDC.e → pUSD (omit --amount for full balance)");
    eprintln!("  ctf_admin unwrap          --amount <pUSD>       # pUSD → USDC.e");
    eprintln!("  ctf_admin approve-collateral                     # one-time pUSD approval for V2 trading");
    eprintln!();
    eprintln!("Notes:");
    eprintln!("  redeem          — standard CTF binary markets (single-question YES/NO).");
    eprintln!("  redeem-neg-risk — Polymarket negative-risk markets (multi-outcome events).");
    eprintln!("                    Pass the on-chain token amounts to redeem; defaults to 0 if omitted.");
    eprintln!("  wrap            — V2 collateral migration. Calls approve(USDC.e → Onramp) then");
    eprintln!("                    Onramp.wrap(USDC.e, wallet, amount). Mints pUSD 1:1 to the wallet.");
    eprintln!("  approve-collateral — Sets max-uint pUSD allowance on the four V2 spenders the");
    eprintln!("                    dashboard's mint+sell needs: ConditionalTokens, NegRiskAdapter,");
    eprintln!("                    CtfExchange V2, NegRiskCtfExchange V2. Idempotent — already-set");
    eprintln!("                    allowances are skipped. Run once after wrapping USDC.e → pUSD.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  ctf_admin merge           --condition-id 0xd387bfe6... --amount 2.0");
    eprintln!("  ctf_admin redeem          --condition-id 0xd387bfe6...");
    eprintln!("  ctf_admin redeem-neg-risk --condition-id 0x35b47285... --no-amount 10.0");
    eprintln!("  ctf_admin balance         --condition-id 0xd387bfe6...");
    eprintln!("  ctf_admin resolution      --condition-id 0xd387bfe6...");
    eprintln!("  ctf_admin wrap                              # wrap entire USDC.e balance");
    eprintln!("  ctf_admin wrap            --amount 10.0     # wrap exactly $10.00");
    eprintln!("  ctf_admin unwrap          --amount 5.0      # unwrap $5.00 pUSD back to USDC.e");
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
    // condition-id is required for every CTF subcommand; wrap/unwrap operate on
    // collateral balances and don't reference any market.
    let needs_condition = !matches!(cmd, "wrap" | "unwrap" | "approve-collateral");
    let condition_id = if needs_condition {
        let s = arg_value(&args, "--condition-id")
            .ok_or_else(|| anyhow!("--condition-id is required"))?;
        B256::from_str(&s).context("invalid --condition-id (expected 0x...)")?
    } else {
        B256::ZERO
    };

    let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
        .context("POLYMARKET_PRIVATE_KEY env var not set")?;
    let rpc_url = std::env::var("POLYGON_HTTP_URL").unwrap_or_else(|_| DEFAULT_RPC.to_string());

    let signer = LocalSigner::from_str(&private_key)
        .context("invalid POLYMARKET_PRIVATE_KEY")?
        .with_chain_id(Some(POLYGON_CHAIN_ID));
    let wallet_addr = signer.address();
    println!("wallet:    {wallet_addr}");
    println!("rpc:       {rpc_url}");
    if needs_condition {
        println!("condition: {condition_id}");
    }

    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(&rpc_url)
        .await
        .context("failed to connect to Polygon RPC")?;

     // Use `with_neg_risk` so the same client handles both standard CTF and
    // Polymarket negative-risk markets. The standard CTF entry points still
    // work — the only difference is that `redeem_neg_risk()` is now available.
    let ctf_client =
        polymarket_client_sdk_v2::ctf::Client::with_neg_risk(provider, POLYGON_CHAIN_ID)
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

            let req = MergePositionsRequest::for_binary_market(COLLATERAL, condition_id, amount_units);
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

            let req = RedeemPositionsRequest::for_binary_market(COLLATERAL, condition_id);
            let resp = ctf_client
                .redeem_positions(&req)
                .await
                .map_err(|e| anyhow!("redeem failed: {e}"))?;
            println!("✓ redeem submitted");
            println!("  tx_hash: {:?}", resp.transaction_hash);
            println!("  block:   {}", resp.block_number);
        }
        "redeem-neg-risk" => {
            // Polymarket negative-risk markets are redeemed through the
            // NegRiskAdapter contract, which takes per-outcome token amounts
            // rather than index sets. amounts = [yesUnits, noUnits] in 6-decimal
            // base units (same scale as USDC).
            let yes_dec = arg_value(&args, "--yes-amount")
                .map(|s| Decimal::from_str(&s).context("invalid --yes-amount"))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            let no_dec = arg_value(&args, "--no-amount")
                .map(|s| Decimal::from_str(&s).context("invalid --no-amount"))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            if yes_dec < Decimal::ZERO || no_dec < Decimal::ZERO {
                bail!("amounts must be >= 0");
            }
            if yes_dec == Decimal::ZERO && no_dec == Decimal::ZERO {
                bail!("at least one of --yes-amount / --no-amount must be > 0");
            }
            let yes_units = decimal_to_usdc_units(yes_dec)?;
            let no_units = decimal_to_usdc_units(no_dec)?;

            println!("action:    redeem-neg-risk");
            println!("yes:       {yes_dec} ({yes_units} units)");
            println!("no:        {no_dec} ({no_units} units)");
            println!();

            let req = RedeemNegRiskRequest::builder()
                .condition_id(condition_id)
                .amounts(vec![yes_units, no_units])
                .build();
            let resp = ctf_client
                .redeem_neg_risk(&req)
                .await
                .map_err(|e| anyhow!("redeem-neg-risk failed: {e}"))?;
            println!("✓ redeem-neg-risk submitted");
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
                    .collateral_token(COLLATERAL)
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
        "wrap" => {
            // V2 collateral migration: USDC.e → pUSD via CollateralOnramp.
            // Two-step: approve(USDC.e → onramp), then onramp.wrap(USDC.e, wallet, amount).
            // pUSD is minted 1:1 to the wallet; backing is enforced on-chain.
            let provider = ctf_client.provider().clone();
            let usdce = IERC20::new(USDCE, provider.clone());
            let pusd = IERC20::new(PUSD, provider.clone());
            let onramp = ICollateralRamp::new(ONRAMP, provider.clone());

            let bal_raw = usdce
                .balanceOf(wallet_addr)
                .call()
                .await
                .map_err(|e| anyhow!("USDC.e balanceOf failed: {e}"))?;
            let bal_units: u128 = bal_raw.try_into().unwrap_or(0);
            let bal_dec = Decimal::from(bal_units) / Decimal::from(1_000_000u64);

            let amount_dec = match arg_value(&args, "--amount") {
                Some(s) => Decimal::from_str(&s).context("invalid --amount")?,
                None => bal_dec,
            };
            if amount_dec <= Decimal::ZERO {
                bail!("nothing to wrap (USDC.e balance is 0 and no --amount specified)");
            }
            let amount_units = decimal_to_usdc_units(amount_dec)?;
            if amount_units > bal_raw {
                bail!(
                    "amount ${amount_dec} exceeds USDC.e balance ${bal_dec}"
                );
            }

            let pusd_before_raw = pusd
                .balanceOf(wallet_addr)
                .call()
                .await
                .map_err(|e| anyhow!("pUSD balanceOf failed: {e}"))?;
            let pusd_before: u128 = pusd_before_raw.try_into().unwrap_or(0);

            println!("action:    wrap (USDC.e → pUSD)");
            println!("amount:    ${amount_dec} ({amount_units} units)");
            println!("usdce_bal: ${bal_dec}");
            println!("pusd_bal:  ${}", Decimal::from(pusd_before) / Decimal::from(1_000_000u64));
            println!("onramp:    {ONRAMP}");
            println!();

            // Step 1: approve onramp to pull USDC.e from the wallet.
            // Set exactly the wrap amount — using max-uint would leave a dangling
            // approval on a contract we only need to interact with once per wrap.
            let allowance_raw = usdce
                .allowance(wallet_addr, ONRAMP)
                .call()
                .await
                .map_err(|e| anyhow!("USDC.e allowance failed: {e}"))?;
            if allowance_raw < amount_units {
                println!("step 1/2: approve onramp for {amount_units} units...");
                let approve_tx = usdce
                    .approve(ONRAMP, amount_units)
                    .send()
                    .await
                    .map_err(|e| anyhow!("approve send failed: {e}"))?;
                let approve_hash = *approve_tx.tx_hash();
                let _approve_receipt = approve_tx
                    .get_receipt()
                    .await
                    .map_err(|e| anyhow!("approve receipt failed: {e}"))?;
                println!("  ✓ approve tx: {approve_hash}");
            } else {
                println!("step 1/2: approve already covers amount (allowance OK)");
            }

            // Step 2: wrap. Onramp pulls USDC.e and mints pUSD 1:1 to `_to`.
            println!("step 2/2: onramp.wrap(USDC.e, wallet, {amount_units})...");
            let wrap_tx = onramp
                .wrap(USDCE, wallet_addr, amount_units)
                .send()
                .await
                .map_err(|e| anyhow!("wrap send failed: {e}"))?;
            let wrap_hash = *wrap_tx.tx_hash();
            let receipt = wrap_tx
                .get_receipt()
                .await
                .map_err(|e| anyhow!("wrap receipt failed: {e}"))?;
            println!("  ✓ wrap tx:    {wrap_hash}");
            println!("    block:      {}", receipt.block_number.unwrap_or(0));

            // Verify the 1:1 mint actually happened.
            let pusd_after_raw = pusd
                .balanceOf(wallet_addr)
                .call()
                .await
                .map_err(|e| anyhow!("pUSD balanceOf (after) failed: {e}"))?;
            let pusd_after: u128 = pusd_after_raw.try_into().unwrap_or(0);
            let minted = pusd_after.saturating_sub(pusd_before);
            let minted_dec = Decimal::from(minted) / Decimal::from(1_000_000u64);
            println!();
            println!("→ minted ${minted_dec} pUSD (new balance: ${})",
                Decimal::from(pusd_after) / Decimal::from(1_000_000u64));
        }
        "unwrap" => {
            // Reverse direction: pUSD → USDC.e. Must approve onramp on pUSD,
            // then call onramp.unwrap(USDC.e, wallet, amount).
            let amount_str = arg_value(&args, "--amount")
                .ok_or_else(|| anyhow!("--amount is required for unwrap (in pUSD)"))?;
            let amount_dec = Decimal::from_str(&amount_str).context("invalid --amount")?;
            if amount_dec <= Decimal::ZERO {
                bail!("--amount must be > 0");
            }
            let amount_units = decimal_to_usdc_units(amount_dec)?;

            let provider = ctf_client.provider().clone();
            let pusd = IERC20::new(PUSD, provider.clone());
            let onramp = ICollateralRamp::new(ONRAMP, provider.clone());

            let bal_raw = pusd
                .balanceOf(wallet_addr)
                .call()
                .await
                .map_err(|e| anyhow!("pUSD balanceOf failed: {e}"))?;
            if amount_units > bal_raw {
                let bal_dec = Decimal::from(bal_raw.to::<u128>()) / Decimal::from(1_000_000u64);
                bail!("amount ${amount_dec} exceeds pUSD balance ${bal_dec}");
            }

            println!("action:    unwrap (pUSD → USDC.e)");
            println!("amount:    ${amount_dec} ({amount_units} units)");
            println!();

            let allowance_raw = pusd
                .allowance(wallet_addr, ONRAMP)
                .call()
                .await
                .map_err(|e| anyhow!("pUSD allowance failed: {e}"))?;
            if allowance_raw < amount_units {
                println!("step 1/2: approve onramp on pUSD...");
                let approve_tx = pusd
                    .approve(ONRAMP, amount_units)
                    .send()
                    .await
                    .map_err(|e| anyhow!("approve send failed: {e}"))?;
                let _ = approve_tx
                    .get_receipt()
                    .await
                    .map_err(|e| anyhow!("approve receipt failed: {e}"))?;
                println!("  ✓ approve done");
            }

            println!("step 2/2: onramp.unwrap(USDC.e, wallet, {amount_units})...");
            let unwrap_tx = onramp
                .unwrap(USDCE, wallet_addr, amount_units)
                .send()
                .await
                .map_err(|e| anyhow!("unwrap send failed: {e}"))?;
            let unwrap_hash = *unwrap_tx.tx_hash();
            let receipt = unwrap_tx
                .get_receipt()
                .await
                .map_err(|e| anyhow!("unwrap receipt failed: {e}"))?;
            println!("  ✓ unwrap tx:  {unwrap_hash}");
            println!("    block:      {}", receipt.block_number.unwrap_or(0));
        }
        "approve-collateral" => {
            // After the V1 → V2 collateral switch (USDC.e → pUSD), every
            // existing on-chain allowance the wallet had set on USDC.e is
            // useless to the V2 protocol. Re-approve pUSD against the four
            // spenders the dashboard's mint / sell / redeem paths use.
            //
            // We use max-uint as the allowance. That matches Polymarket's UI
            // behavior on first deposit and avoids re-approving every time
            // the wallet runs out of headroom. The trade-off is that an
            // exploit on any of these contracts could drain unbounded pUSD
            // from the wallet — acceptable here because all four are core
            // Polymarket protocol contracts the wallet already trusts.
            let provider = ctf_client.provider().clone();
            let pusd = IERC20::new(PUSD, provider.clone());

            let spenders: [(Address, &str); 4] = [
                (CTF_CONTRACT, "ConditionalTokens (split / merge / redeem)"),
                (NEG_RISK_ADAPTER, "NegRiskAdapter (neg-risk split / merge)"),
                (CTF_EXCHANGE_V2, "CtfExchange V2 (binary orders)"),
                (NEG_RISK_CTF_EXCHANGE_V2, "NegRiskCtfExchange V2 (neg-risk orders)"),
            ];

            println!("action:    approve-collateral");
            println!("collateral: {PUSD} (pUSD)");
            println!();

            for (spender, label) in spenders {
                let current = pusd
                    .allowance(wallet_addr, spender)
                    .call()
                    .await
                    .map_err(|e| anyhow!("allowance({label}) failed: {e}"))?;
                if current == U256::MAX {
                    println!("✓ {spender}  {label}");
                    println!("    already at max — skipping");
                    continue;
                }
                println!("→ {spender}  {label}");
                let tx = pusd
                    .approve(spender, U256::MAX)
                    .send()
                    .await
                    .map_err(|e| anyhow!("approve({label}) send failed: {e}"))?;
                let hash = *tx.tx_hash();
                let _ = tx
                    .get_receipt()
                    .await
                    .map_err(|e| anyhow!("approve({label}) receipt failed: {e}"))?;
                println!("    ✓ tx: {hash}");
            }

            println!();
            println!("→ pUSD allowances set. Restart the dashboard to clear");
            println!("  the cached \"USDC not approved\" warnings on startup.");
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