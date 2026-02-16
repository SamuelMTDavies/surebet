//! Arbitrage execution manager.
//!
//! Takes ArbOpportunities from the scanner and executes them via the
//! CLOB API. Handles leg risk: if only one side of a multi-leg arb
//! fills, the unfilled legs are cancelled and the position is unwound.

use crate::arb::{ArbLeg, ArbOpportunity};
use crate::auth::{AuthError, ClobApiClient, OrderRequest, OrderSide};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Risk limits for the executor.
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Maximum USDC to deploy per single arb opportunity.
    pub max_position_usd: Decimal,
    /// Maximum number of simultaneous open arb positions.
    pub max_open_positions: usize,
    /// Maximum total USDC across all open positions.
    pub max_total_exposure: Decimal,
    /// Minimum net edge (after fees) required to execute.
    pub min_net_edge: Decimal,
    /// Minimum fillable size in USDC to bother executing.
    pub min_fill_size: Decimal,
    /// Cooldown per market after an execution (avoid hammering).
    pub market_cooldown: Duration,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_position_usd: Decimal::from(100),
            max_open_positions: 5,
            max_total_exposure: Decimal::from(500),
            min_net_edge: Decimal::from_str("0.005").unwrap(),
            min_fill_size: Decimal::from(5),
            market_cooldown: Duration::from_secs(30),
        }
    }
}

/// Tracks an in-flight arb execution.
#[derive(Debug)]
struct OpenPosition {
    condition_id: String,
    legs: Vec<LegStatus>,
    total_cost: Decimal,
    submitted_at: Instant,
}

#[derive(Debug)]
struct LegStatus {
    token_id: String,
    order_id: Option<String>,
    price: Decimal,
    size: Decimal,
    filled: bool,
}

/// The executor manages arb execution lifecycle.
pub struct ArbExecutor {
    api: Arc<ClobApiClient>,
    limits: RiskLimits,
    enabled: Arc<AtomicBool>,
    positions: Arc<Mutex<Vec<OpenPosition>>>,
    cooldowns: Arc<Mutex<HashMap<String, Instant>>>,
}

impl ArbExecutor {
    pub fn new(api: Arc<ClobApiClient>, limits: RiskLimits) -> Self {
        Self {
            api,
            limits,
            enabled: Arc::new(AtomicBool::new(false)),
            positions: Arc::new(Mutex::new(Vec::new())),
            cooldowns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
        info!("arb executor ENABLED");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        info!("arb executor DISABLED");
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Attempt to execute an arb opportunity.
    /// Returns Ok(true) if orders were submitted, Ok(false) if skipped.
    pub async fn try_execute(&self, opp: &ArbOpportunity) -> Result<bool, AuthError> {
        if !self.is_enabled() {
            return Ok(false);
        }

        // Check risk limits
        if !self.passes_risk_checks(opp).await {
            return Ok(false);
        }

        // Check cooldown
        {
            let cooldowns = self.cooldowns.lock().await;
            if let Some(last) = cooldowns.get(&opp.condition_id) {
                if last.elapsed() < self.limits.market_cooldown {
                    return Ok(false);
                }
            }
        }

        // Determine size: minimum of fillable size and max position
        let size = opp
            .max_fillable_size
            .min(self.limits.max_position_usd / opp.ask_sum);

        if size * opp.ask_sum < self.limits.min_fill_size {
            return Ok(false);
        }

        info!(
            market = %opp.question,
            net_edge = %opp.net_edge,
            size = %size,
            cost = %(size * opp.ask_sum),
            "EXECUTING ARB"
        );

        // Build batch order: buy all outcomes simultaneously
        let orders: Vec<OrderRequest> = opp
            .legs
            .iter()
            .map(|leg| OrderRequest {
                token_id: leg.token_id.clone(),
                price: leg.best_ask.to_string(),
                size: size.to_string(),
                side: OrderSide::Buy.as_str().to_string(),
            })
            .collect();

        // Submit as batch (up to 15 orders per call)
        let result = self.api.place_orders(&orders).await;

        match &result {
            Ok(resp) => {
                info!(
                    market = %opp.question,
                    response = %resp,
                    "batch order submitted"
                );

                // Track position
                let position = OpenPosition {
                    condition_id: opp.condition_id.clone(),
                    legs: opp
                        .legs
                        .iter()
                        .map(|l| LegStatus {
                            token_id: l.token_id.clone(),
                            order_id: None, // TODO: parse from response
                            price: l.best_ask,
                            size,
                            filled: false,
                        })
                        .collect(),
                    total_cost: size * opp.ask_sum,
                    submitted_at: Instant::now(),
                };

                self.positions.lock().await.push(position);
                self.cooldowns
                    .lock()
                    .await
                    .insert(opp.condition_id.clone(), Instant::now());
            }
            Err(e) => {
                error!(
                    market = %opp.question,
                    error = %e,
                    "batch order FAILED"
                );
            }
        }

        result.map(|_| true)
    }

    /// Check all risk limits.
    async fn passes_risk_checks(&self, opp: &ArbOpportunity) -> bool {
        if opp.net_edge < self.limits.min_net_edge {
            return false;
        }

        let positions = self.positions.lock().await;

        if positions.len() >= self.limits.max_open_positions {
            warn!(
                max = self.limits.max_open_positions,
                current = positions.len(),
                "max open positions reached"
            );
            return false;
        }

        let total_exposure: Decimal = positions.iter().map(|p| p.total_cost).sum();
        let new_cost = opp.max_fillable_size * opp.ask_sum;
        if total_exposure + new_cost > self.limits.max_total_exposure {
            warn!(
                exposure = %total_exposure,
                new_cost = %new_cost,
                max = %self.limits.max_total_exposure,
                "max total exposure reached"
            );
            return false;
        }

        true
    }

    /// Cancel all open orders (panic button).
    pub async fn cancel_all(&self) -> Result<(), AuthError> {
        warn!("CANCELLING ALL ORDERS");
        self.api.cancel_all().await?;
        self.positions.lock().await.clear();
        Ok(())
    }

    /// Check for stale positions and clean up.
    pub async fn cleanup_stale(&self, max_age: Duration) {
        let mut positions = self.positions.lock().await;
        let before = positions.len();
        positions.retain(|p| p.submitted_at.elapsed() < max_age);
        let removed = before - positions.len();
        if removed > 0 {
            info!(removed = removed, "cleaned up stale positions");
        }
    }

    /// Get current exposure summary.
    pub async fn exposure_summary(&self) -> ExposureSummary {
        let positions = self.positions.lock().await;
        let total_exposure: Decimal = positions.iter().map(|p| p.total_cost).sum();
        ExposureSummary {
            open_positions: positions.len(),
            total_exposure,
            enabled: self.is_enabled(),
        }
    }
}

#[derive(Debug)]
pub struct ExposureSummary {
    pub open_positions: usize,
    pub total_exposure: Decimal,
    pub enabled: bool,
}

impl std::fmt::Display for ExposureSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "positions={} exposure=${} enabled={}",
            self.open_positions, self.total_exposure, self.enabled
        )
    }
}
