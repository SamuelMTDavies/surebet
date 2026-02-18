//! Split arbitrage executor.
//!
//! Selects the best fillable tier, then places simultaneous limit orders
//! for every leg. Uses the existing ClobApiClient for order submission.

use crate::auth::{AuthError, ClobApiClient, OrderRequest, OrderSide};
use crate::split::{SplitDirection, SplitOpportunity, TierAnalysis};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Risk limits for split arb execution.
#[derive(Debug, Clone)]
pub struct SplitRiskLimits {
    pub max_position_usd: Decimal,
    pub max_open_positions: usize,
    pub max_total_exposure: Decimal,
    pub min_net_edge: Decimal,
    pub min_fill_size_usd: Decimal,
    pub market_cooldown: Duration,
}

impl Default for SplitRiskLimits {
    fn default() -> Self {
        Self {
            max_position_usd: Decimal::from(500),
            max_open_positions: 5,
            max_total_exposure: Decimal::from(2000),
            min_net_edge: Decimal::from_str("0.005").unwrap(),
            min_fill_size_usd: Decimal::from(10),
            market_cooldown: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
struct OpenSplitPosition {
    condition_id: String,
    direction: SplitDirection,
    order_ids: Vec<String>,
    total_cost: Decimal,
    submitted_at: Instant,
}

/// Executor for split arbitrage opportunities.
pub struct SplitExecutor {
    api: Arc<ClobApiClient>,
    limits: SplitRiskLimits,
    enabled: Arc<AtomicBool>,
    positions: Arc<Mutex<Vec<OpenSplitPosition>>>,
    cooldowns: Arc<Mutex<HashMap<String, Instant>>>,
}

impl SplitExecutor {
    pub fn new(api: Arc<ClobApiClient>, limits: SplitRiskLimits) -> Self {
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
        info!("split executor ENABLED");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        info!("split executor DISABLED");
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Attempt to execute a split opportunity. Returns Ok(true) if orders submitted.
    pub async fn try_execute(&self, opp: &SplitOpportunity) -> Result<bool, AuthError> {
        if !self.is_enabled() {
            return Ok(false);
        }

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

        // Select the best tier: largest tier that is fully fillable and profitable
        let best_tier = self.select_tier(opp);
        let (shares, tier_label) = match best_tier {
            Some(t) => (t.executable_shares, t.tier_usd),
            None => {
                // Fall back to top-of-book min fillable
                let max_shares = opp.min_fillable_shares.min(
                    self.limits.max_position_usd / opp.price_sum.max(Decimal::ONE),
                );
                if max_shares * opp.price_sum < self.limits.min_fill_size_usd {
                    return Ok(false);
                }
                (max_shares, max_shares * opp.price_sum)
            }
        };

        if shares <= Decimal::ZERO {
            return Ok(false);
        }

        let side = match opp.direction {
            SplitDirection::BuyAndMerge => OrderSide::Buy,
            SplitDirection::SplitAndSell => OrderSide::Sell,
        };

        info!(
            market = %opp.question,
            dir = %opp.direction,
            shares = %shares,
            tier = %tier_label,
            net_edge = %opp.net_edge_tob,
            "EXECUTING SPLIT"
        );

        // Build batch: one order per leg at best price (or slightly better for fill)
        let orders: Vec<OrderRequest> = opp
            .legs
            .iter()
            .map(|leg| OrderRequest {
                token_id: leg.token_id.clone(),
                price: leg.best_price.to_string(),
                size: shares.to_string(),
                side: side.as_str().to_string(),
            })
            .collect();

        let result = self.api.place_orders(&orders).await;

        match &result {
            Ok(resps) => {
                let order_ids: Vec<String> = resps.iter().map(|r| r.order_id.clone()).collect();
                info!(
                    market = %opp.question,
                    orders = resps.len(),
                    "split batch submitted"
                );

                let cost = shares * opp.price_sum;
                self.positions.lock().await.push(OpenSplitPosition {
                    condition_id: opp.condition_id.clone(),
                    direction: opp.direction,
                    order_ids,
                    total_cost: cost,
                    submitted_at: Instant::now(),
                });
                self.cooldowns
                    .lock()
                    .await
                    .insert(opp.condition_id.clone(), Instant::now());
            }
            Err(e) => {
                error!(market = %opp.question, error = %e, "split batch FAILED");
            }
        }

        result.map(|_| true)
    }

    /// Pick the best tier: largest fully-fillable tier with positive edge,
    /// capped by risk limits.
    fn select_tier<'a>(&self, opp: &'a SplitOpportunity) -> Option<&'a TierAnalysis> {
        opp.tier_analyses
            .iter()
            .rev() // largest first
            .find(|t| {
                t.fully_fillable
                    && t.net_edge >= self.limits.min_net_edge
                    && t.tier_usd <= self.limits.max_position_usd
                    && t.expected_profit_usd > Decimal::ZERO
            })
    }

    async fn passes_risk_checks(&self, opp: &SplitOpportunity) -> bool {
        if opp.net_edge_tob < self.limits.min_net_edge {
            return false;
        }

        let positions = self.positions.lock().await;
        if positions.len() >= self.limits.max_open_positions {
            warn!(max = self.limits.max_open_positions, "split: max positions");
            return false;
        }

        let total_exposure: Decimal = positions.iter().map(|p| p.total_cost).sum();
        let est_cost = opp.min_fillable_shares * opp.price_sum;
        if total_exposure + est_cost > self.limits.max_total_exposure {
            warn!(
                exposure = %total_exposure,
                max = %self.limits.max_total_exposure,
                "split: max exposure"
            );
            return false;
        }

        true
    }

    pub async fn cancel_all(&self) -> Result<(), AuthError> {
        warn!("cancelling all split orders");
        self.api.cancel_all().await?;
        self.positions.lock().await.clear();
        Ok(())
    }

    pub async fn cleanup_stale(&self, max_age: Duration) {
        let mut positions = self.positions.lock().await;
        let before = positions.len();
        positions.retain(|p| p.submitted_at.elapsed() < max_age);
        let removed = before - positions.len();
        if removed > 0 {
            info!(removed, "cleaned stale split positions");
        }
    }
}
