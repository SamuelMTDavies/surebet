//! Stale Order Harvester — scans for fee-free Polymarket markets near their
//! end date that still accept orders, lets the operator pick a winner, and
//! sweeps the book.  After the initial sweep it enters "hoover mode" —
//! subscribes to the WebSocket book stream and auto-buys any new asks that
//! appear below the limit price until Ctrl-C.
//!
//! Navigation: type 'b' to go back, 'q' to quit at any prompt.
//!
//! Usage:
//!   cargo run --bin harvester              # paper mode (default)
//!   cargo run --bin harvester -- --live    # live execution

use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use std::io::{self, Write};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;

use surebet::auth::{ClobApiClient, L2Credentials, OrderSide};
use surebet::config::Config;
use surebet::harvester::{build_outcome_info, scan_markets, OutcomeInfo};
use surebet::orderbook::OrderBookStore;
use surebet::ws::clob::{start_clob_ws, ClobEvent};

// ─── CLI-only types ──────────────────────────────────────────────────────────

#[derive(Debug)]
struct PlannedOrder {
    token_id: String,
    label: String,
    price: Decimal,
    shares: Decimal,
    cost: Decimal,
    profit: Decimal,
}

/// User input result — a selection, go back, or quit.
enum Selection {
    Index(usize),
    Back,
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let _ = dotenvy::dotenv();

    let args: Vec<String> = std::env::args().collect();
    let live_mode = args.iter().any(|a| a == "--live");

    let config = match Config::load(std::path::Path::new("surebet.toml")) {
        Ok(c) => c,
        Err(_) => Config::from_env(),
    };

    let gamma_url = &config.polymarket.gamma_url;
    let clob_url = &config.polymarket.clob_url;
    let harvester = &config.harvester;

    let max_buy = Decimal::from_str(&format!("{}", harvester.max_buy_price))
        .unwrap_or_else(|_| Decimal::from_str("0.995").unwrap());
    let min_sell = Decimal::from_str(&format!("{}", harvester.min_sell_price))
        .unwrap_or_else(|_| Decimal::from_str("0.005").unwrap());

    // Build API client once (live mode)
    let api = if live_mode {
        let creds = L2Credentials::from_config(
            &config.polymarket.api_key,
            &config.polymarket.api_secret,
            &config.polymarket.api_passphrase,
        );
        match creds {
            Some(c) => Some(ClobApiClient::new(clob_url.clone(), c)),
            None => {
                println!("WARNING: --live but missing credentials, falling back to paper mode");
                None
            }
        }
    } else {
        None
    };

    println!("=== Stale Order Harvester ===");
    println!(
        "Mode: {}  |  Buy limit: ${}  |  Window: {}d",
        if api.is_some() { "LIVE" } else { "PAPER" },
        max_buy,
        harvester.end_date_window_days,
    );
    println!("  Type 'b' to go back, 'q' to quit at any prompt.");
    println!();

    // ── Step 1: Scan Gamma API ──────────────────────────────────────────────

    println!("Scanning Gamma API for markets near end date...");
    let markets = scan_markets(gamma_url, harvester.end_date_window_days, harvester.max_display).await?;

    if markets.is_empty() {
        println!("No markets found matching criteria.");
        return Ok(());
    }

    let book_store = Arc::new(OrderBookStore::new());

    // ── Main loop: market selection → book → winner → execute → repeat ──

    'market_loop: loop {
        display_markets(&markets);

        // ── Select market ────────────────────────────────────────────────
        let selected_idx = match prompt_market_selection(markets.len())? {
            Selection::Index(i) => i,
            Selection::Back => {
                println!("Already at the top level. Type 'q' to quit.");
                continue;
            }
        };
        let market = &markets[selected_idx];

        // ── Fetch book ───────────────────────────────────────────────────
        'outcome_loop: loop {
            println!();
            println!("Fetching order books for: {}", market.question);

            let mut outcome_infos = Vec::new();
            for (i, token_id) in market.clob_token_ids.iter().enumerate() {
                let label = market.outcomes.get(i).cloned().unwrap_or_else(|| format!("Outcome {}", i));
                let book = book_store.fetch_rest_book(clob_url, token_id).await;

                let info = match book {
                    Some(ref b) => build_outcome_info(&label, token_id, b, max_buy, min_sell),
                    None => OutcomeInfo {
                        label: label.clone(),
                        token_id: token_id.clone(),
                        best_ask: None,
                        sweepable_shares: Decimal::ZERO,
                        sweepable_cost: Decimal::ZERO,
                        sweepable_profit: Decimal::ZERO,
                        best_bid: None,
                        sellable_shares: Decimal::ZERO,
                        sellable_revenue: Decimal::ZERO,
                    },
                };
                outcome_infos.push(info);
            }

            // Display outcomes
            println!();
            println!("Market: \"{}\"", market.question);
            println!();
            for (i, info) in outcome_infos.iter().enumerate() {
                let ask_str = info
                    .best_ask
                    .map(|p| format!("${}", p))
                    .unwrap_or_else(|| "none".to_string());
                let bid_str = info
                    .best_bid
                    .map(|p| format!("${}", p))
                    .unwrap_or_else(|| "none".to_string());
                let buy_pct = if info.sweepable_cost > Decimal::ZERO {
                    let pct_val = (info.sweepable_profit / info.sweepable_cost)
                        * Decimal::from(100);
                    format!("{:.2}%", pct_val)
                } else {
                    "-".to_string()
                };

                let sell_losers_rev: Decimal = outcome_infos
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, o)| o.sellable_revenue)
                    .sum();
                let combined = info.sweepable_profit + sell_losers_rev;

                println!(
                    "  [{}] {:<20} ask: {:<8} bid: {:<8}",
                    i, info.label, ask_str, bid_str,
                );
                println!(
                    "      BUY:  {} shares ≤${} | cost ${:.4} → profit ${:.4} ({})",
                    info.sweepable_shares, max_buy, info.sweepable_cost, info.sweepable_profit, buy_pct,
                );
                if sell_losers_rev > Decimal::ZERO {
                    println!(
                        "      SELL: split+sell losers → ${:.4} revenue",
                        sell_losers_rev,
                    );
                    println!(
                        "      COMBINED: ${:.4}",
                        combined,
                    );
                }
                println!();
            }

            // ── Select winner ────────────────────────────────────────────
            let winner_idx = match prompt_winner_selection(outcome_infos.len())? {
                Selection::Index(i) => i,
                Selection::Back => continue 'market_loop,
            };

            let winner = &outcome_infos[winner_idx];

            let sell_losers_revenue: Decimal = outcome_infos
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != winner_idx)
                .map(|(_, o)| o.sellable_revenue)
                .sum();

            if winner.sweepable_shares == Decimal::ZERO && sell_losers_revenue == Decimal::ZERO {
                println!("\nNo opportunity for \"{}\" — no asks to buy, no loser bids to sell into.", winner.label);
                println!("Press enter to go back to outcomes, or 'b' for market list.");
                let mut buf = String::new();
                io::stdin().read_line(&mut buf)?;
                if buf.trim().eq_ignore_ascii_case("b") {
                    continue 'market_loop;
                }
                continue 'outcome_loop;
            }

            let order = PlannedOrder {
                token_id: winner.token_id.clone(),
                label: winner.label.clone(),
                price: max_buy,
                shares: winner.sweepable_shares,
                cost: winner.sweepable_cost,
                profit: winner.sweepable_profit,
            };

            let combined_profit = order.profit + sell_losers_revenue;
            let pct = if order.cost > Decimal::ZERO {
                let v = (order.profit / order.cost) * Decimal::from(100);
                format!("{:.2}%", v)
            } else {
                "-".to_string()
            };

            println!();
            println!("=== Execution Plan ===");
            println!(
                "  BUY \"{}\"  {} shares @ limit ${}  |  cost ${}  |  profit ${} ({})",
                order.label, order.shares, order.price, order.cost, order.profit, pct,
            );
            if sell_losers_revenue > Decimal::ZERO {
                println!(
                    "  SELL losers (split+sell)  |  revenue ${}",
                    sell_losers_revenue,
                );
                println!(
                    "  COMBINED PROFIT: ${}",
                    combined_profit,
                );
            }
            println!(
                "  Mode: {}",
                if api.is_some() {
                    "LIVE — order WILL be placed"
                } else {
                    "PAPER (use --live for real execution)"
                }
            );

            // ── Confirm ──────────────────────────────────────────────────
            print!("\nProceed? [y/N/b]: ");
            io::stdout().flush()?;
            let mut confirm = String::new();
            io::stdin().read_line(&mut confirm)?;
            let trimmed = confirm.trim();
            if trimmed.eq_ignore_ascii_case("b") {
                continue 'outcome_loop;
            }
            if !trimmed.eq_ignore_ascii_case("y") {
                println!("Skipped.");
                continue 'outcome_loop;
            }

            // ── Execute ──────────────────────────────────────────────────

            if let Some(ref api) = api {
                println!();
                println!("Placing initial sweep order...");

                let resp = api
                    .place_order(
                        &order.token_id,
                        &order.price.to_string(),
                        &order.shares.to_string(),
                        OrderSide::Buy,
                    )
                    .await;

                match resp {
                    Ok(r) if r.success => {
                        println!(
                            "  OK  BUY \"{}\" — order_id: {}, status: {}",
                            order.label, r.order_id, r.status,
                        );
                    }
                    Ok(r) => {
                        println!("  FAIL BUY \"{}\" — error: {}", order.label, r.error_msg);
                    }
                    Err(e) => {
                        println!("  ERR  BUY \"{}\" — {}", order.label, e);
                    }
                }
            } else {
                println!();
                println!("Paper mode — no order placed.");
            }

            // ── Post-trade menu ──────────────────────────────────────────
            println!();
            println!("Trade done. What next?");
            println!("  [h] Enter hoover mode for \"{}\"", order.label);
            println!("  [b] Back to market list");
            println!("  [r] Re-fetch this market's book");
            println!("  [q] Quit");
            print!("> ");
            io::stdout().flush()?;

            let mut choice = String::new();
            io::stdin().read_line(&mut choice)?;
            match choice.trim().to_lowercase().as_str() {
                "h" => {
                    println!();
                    println!("=== Hoover Mode ===");
                    println!("Monitoring book for new asks on \"{}\" ≤ ${}", order.label, max_buy);
                    println!("Press Ctrl-C to stop and return to market list.");
                    println!();

                    hoover_loop(&book_store, &order.token_id, &order.label, max_buy, api.as_ref()).await?;

                    println!();
                    println!("Returning to market list...");
                    continue 'market_loop;
                }
                "r" => {
                    // Re-fetch book and show outcomes again
                    continue 'outcome_loop;
                }
                "q" => {
                    println!("Goodbye.");
                    return Ok(());
                }
                _ => {
                    // Default: back to market list
                    continue 'market_loop;
                }
            }
        }
    }
}

// ─── Display ────────────────────────────────────────────────────────────────

fn display_markets(markets: &[surebet::harvester::HarvestableMarket]) {
    let now = Utc::now();
    println!();
    println!(
        " {:<3} | {:<12} | {:<3} | {:<8} | {:<12} | Question",
        "#", "End Date", "Out", "NegRisk", "Category"
    );
    println!("{}", "-".repeat(90));

    for (i, m) in markets.iter().enumerate() {
        let end_str = match m.end_date {
            Some(ed) if ed < now => "PAST".to_string(),
            Some(ed) => {
                let diff = ed - now;
                if diff.num_hours() < 24 {
                    format!("in {}h", diff.num_hours())
                } else {
                    ed.format("%Y-%m-%d").to_string()
                }
            }
            None => "???".to_string(),
        };

        let question_trunc = if m.question.len() > 50 {
            format!("{}...", &m.question[..47])
        } else {
            m.question.clone()
        };

        println!(
            " {:<3} | {:<12} | {:<3} | {:<8} | {:<12} | {}",
            i + 1,
            end_str,
            m.outcomes.len(),
            if m.is_neg_risk { "yes" } else { "no" },
            if m.category.len() > 12 {
                &m.category[..12]
            } else {
                &m.category
            },
            question_trunc,
        );
    }
    println!();
}

// ─── Prompts ────────────────────────────────────────────────────────────────

fn prompt_market_selection(count: usize) -> Result<Selection> {
    loop {
        print!("Enter market number (1-{}), 'b' back, 'q' quit: ", count);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();

        if trimmed.eq_ignore_ascii_case("q") {
            std::process::exit(0);
        }
        if trimmed.eq_ignore_ascii_case("b") {
            return Ok(Selection::Back);
        }

        if let Ok(n) = trimmed.parse::<usize>() {
            if n >= 1 && n <= count {
                return Ok(Selection::Index(n - 1));
            }
        }
        println!("Invalid selection. Try again.");
    }
}

fn prompt_winner_selection(count: usize) -> Result<Selection> {
    loop {
        print!("Select winning outcome (0-{}), 'b' back: ", count - 1);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();

        if trimmed.eq_ignore_ascii_case("q") {
            std::process::exit(0);
        }
        if trimmed.eq_ignore_ascii_case("b") {
            return Ok(Selection::Back);
        }

        if let Ok(n) = trimmed.parse::<usize>() {
            if n < count {
                return Ok(Selection::Index(n));
            }
        }
        println!("Invalid selection. Try again.");
    }
}

// ─── Hoover Mode ─────────────────────────────────────────────────────────────

async fn hoover_loop(
    book_store: &Arc<OrderBookStore>,
    token_id: &str,
    label: &str,
    max_buy: Decimal,
    api: Option<&ClobApiClient>,
) -> Result<()> {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ClobEvent>();
    let min_order = Decimal::from(5);

    start_clob_ws(
        (**book_store).clone(),
        event_tx,
        vec![token_id.to_string()],
    )
    .map_err(|e| anyhow::anyhow!("WebSocket start failed: {}", e))?;

    let mut total_hoovered_shares = Decimal::ZERO;
    let mut total_hoovered_cost = Decimal::ZERO;
    let mode = if api.is_some() { "LIVE" } else { "PAPER" };
    let mut last_ordered_depth = Decimal::ZERO;

    loop {
        tokio::select! {
            event = event_rx.recv() => {
                match event {
                    Some(ClobEvent::BookSnapshot { asset_id, .. }) => {
                        if asset_id != token_id {
                            continue;
                        }

                        let book = match book_store.get_book(&asset_id) {
                            Some(b) => b,
                            None => continue,
                        };

                        let mut sweepable_shares = Decimal::ZERO;
                        let mut sweepable_cost = Decimal::ZERO;
                        for (&price, &size) in book.asks.levels.iter() {
                            if price > max_buy {
                                break;
                            }
                            sweepable_shares += size;
                            sweepable_cost += price * size;
                        }

                        if sweepable_shares <= Decimal::ZERO || sweepable_cost < min_order {
                            if sweepable_shares < last_ordered_depth {
                                last_ordered_depth = sweepable_shares;
                            }
                            continue;
                        }

                        if sweepable_shares <= last_ordered_depth {
                            continue;
                        }

                        let profit = sweepable_shares - sweepable_cost;
                        let pct = if sweepable_cost > Decimal::ZERO {
                            (profit / sweepable_cost) * Decimal::from(100)
                        } else {
                            Decimal::ZERO
                        };

                        let now = Utc::now().format("%H:%M:%S");
                        println!(
                            "[{}] Asks available: {} shares @ ≤${} | cost ${:.4} | profit ${:.4} ({:.2}%)",
                            now, sweepable_shares, max_buy, sweepable_cost, profit, pct,
                        );

                        if let Some(api) = api {
                            let resp = api
                                .place_order(
                                    token_id,
                                    &max_buy.to_string(),
                                    &sweepable_shares.to_string(),
                                    OrderSide::Buy,
                                )
                                .await;

                            match resp {
                                Ok(r) if r.success => {
                                    total_hoovered_shares += sweepable_shares;
                                    total_hoovered_cost += sweepable_cost;
                                    last_ordered_depth = sweepable_shares;
                                    println!(
                                        "  [{}] OK  BUY {} shares — id: {} | total: {} shares, ${:.4}",
                                        mode, sweepable_shares, r.order_id,
                                        total_hoovered_shares, total_hoovered_cost,
                                    );
                                }
                                Ok(r) => {
                                    println!("  [{}] FAIL — {}", mode, r.error_msg);
                                }
                                Err(e) => {
                                    println!("  [{}] ERR  — {}", mode, e);
                                }
                            }
                        } else {
                            total_hoovered_shares += sweepable_shares;
                            total_hoovered_cost += sweepable_cost;
                            last_ordered_depth = sweepable_shares;
                            println!(
                                "  [{}] Would BUY {} shares @ ${} | total: {} shares, ${:.4}",
                                mode, sweepable_shares, max_buy,
                                total_hoovered_shares, total_hoovered_cost,
                            );
                        }
                    }
                    Some(ClobEvent::Connected) => {
                        println!("[ws] Connected — monitoring \"{}\"", label);
                    }
                    Some(ClobEvent::Disconnected) => {
                        println!("[ws] Disconnected — will attempt reconnect...");
                    }
                    Some(_) => {}
                    None => {
                        println!("[ws] Event channel closed.");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!();
                println!("=== Hoover Summary ===");
                let total_profit = total_hoovered_shares - total_hoovered_cost;
                let total_pct = if total_hoovered_cost > Decimal::ZERO {
                    (total_profit / total_hoovered_cost) * Decimal::from(100)
                } else {
                    Decimal::ZERO
                };
                println!(
                    "  Total hoovered: {} shares | cost ${:.4} | est. profit ${:.4} ({:.2}%)",
                    total_hoovered_shares, total_hoovered_cost, total_profit, total_pct,
                );
                break;
            }
        }
    }

    Ok(())
}
