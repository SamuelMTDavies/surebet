//! Polymarket trader investigation tool.
//!
//! Usage: cargo run --bin investigate -- <address> [--rpc-http <url>] [--from YYYY-MM-DD] [--to YYYY-MM-DD]

use anyhow::{bail, Context, Result};
use polymarket_client_sdk_v2::types::Address;
use std::io::Write as _;
use std::str::FromStr;

use surebet::investigate::{analyze, classify, correlate, enrich, fetch, report};

fn main() {
    // Install rustls provider (needed by SDK)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls provider");
    let _ = dotenvy::dotenv();

    // Init tracing — only show investigate logs, suppress SDK noise
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(
                    "surebet::investigate=info,warn"
                )),
        )
        .with_writer(std::io::stderr)
        .init();

    // Parse CLI args
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {e:#}");
            std::process::exit(1);
        }
    };

    // Run async main
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(run(args));

    if let Err(e) = result {
        eprintln!("\nError: {e:#}");
        std::process::exit(1);
    }
}

struct Args {
    address: Address,
    rpc_http: Option<String>,
}

fn parse_args() -> Result<Args> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        bail!(
            "Usage: {} <address> [--rpc-http <url>]\n\n  \
             address: Ethereum address to investigate (0x...)\n  \
             --rpc-http: Polygon HTTP RPC URL (or set POLYGON_HTTP_URL env)\n",
            args.first().map(|s| s.as_str()).unwrap_or("investigate")
        );
    }

    let address = Address::from_str(&args[1]).context("invalid Ethereum address")?;

    let mut rpc_http = std::env::var("POLYGON_HTTP_URL").ok();

    // Parse optional flags
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--rpc-http" => {
                i += 1;
                rpc_http = args.get(i).cloned();
            }
            _ => {}
        }
        i += 1;
    }

    // Fallback: convert POLYGON_WS_URL to HTTP
    if rpc_http.is_none() {
        if let Ok(ws_url) = std::env::var("POLYGON_WS_URL") {
            // Infura uses /ws/v3/ for WS and /v3/ for HTTP — strip the /ws/ segment
            let http_url = ws_url
                .replace("wss://", "https://")
                .replace("ws://", "http://")
                .replace("/ws/v3/", "/v3/");
            rpc_http = Some(http_url);
        }
    }

    Ok(Args { address, rpc_http })
}

async fn run(args: Args) -> Result<()> {
    let addr_str = format!("{:#x}", args.address);
    report::print_header(&addr_str);
    flush();

    eprintln!("[1/6] Fetching trader data...");

    // Phase 1: Fetch all data
    let raw = fetch::fetch_all(&args.address).await?;
    report::print_phase1(&raw);
    flush();

    if raw.trades.is_empty() {
        println!("  No trades found for this address. Exiting.\n");
        return Ok(());
    }

    eprintln!("[2/6] Enriching with market metadata...");

    // Phase 2: Enrich with Gamma metadata
    let enriched = enrich::enrich(&raw).await?;

    eprintln!("[3/6] Correlating with on-chain events...");

    // Phase 3: Correlate with on-chain events
    let correlated = correlate::correlate(&enriched, args.rpc_http.as_deref()).await?;

    eprintln!("[4/6] Analyzing patterns...");

    // Phase 4: Analyze
    let analysis = analyze::analyze(&correlated);

    // Print all report sections
    report::print_phase2_lifecycles(&analysis);
    flush();
    report::print_phase3_creation(&analysis);
    flush();
    report::print_phase4_resolution(&analysis);
    flush();
    report::print_phase5_patterns(&analysis);
    flush();

    eprintln!("[5/6] Classifying strategy...");

    // Phase 5: Classify
    let classification = classify::classify(&analysis);
    report::print_phase6_classification(&classification);
    flush();

    eprintln!("[6/6] Done.");

    Ok(())
}

fn flush() {
    let _ = std::io::stdout().flush();
}
