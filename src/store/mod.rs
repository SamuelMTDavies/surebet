//! Valkey (Redis-compatible) state store for order tracking, positions, and PnL.
//!
//! Data model:
//!   order:{order_id}              → JSON OrderRecord      (TTL: 24h)
//!   market_orders:{condition_id}  → SET of order_ids
//!   position:{condition_id}       → JSON PositionRecord
//!   fills                         → LIST of JSON FillRecord (append-only, capped)
//!   pnl:total                     → running Decimal string
//!   pnl:daily:{YYYY-MM-DD}       → daily Decimal string
//!   activity:{token_id}           → "1"                   (TTL: activity_age)

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const ORDER_TTL_SECS: u64 = 86400; // 24 hours
const MAX_FILL_LOG: isize = 10_000; // keep last 10k fills

/// A tracked order on the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub status: OrderStatus,
    pub created_at: String,
    pub updated_at: String,
    pub question: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Open => write!(f, "open"),
            OrderStatus::PartiallyFilled => write!(f, "partially_filled"),
            OrderStatus::Filled => write!(f, "filled"),
            OrderStatus::Cancelled => write!(f, "cancelled"),
            OrderStatus::Expired => write!(f, "expired"),
        }
    }
}

/// A position across all outcomes of a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRecord {
    pub condition_id: String,
    pub question: String,
    /// outcome_label → (token_id, inventory_size, avg_price)
    pub legs: Vec<PositionLeg>,
    pub bid_sum: String,
    pub edge: String,
    pub pending_fill: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionLeg {
    pub label: String,
    pub token_id: String,
    pub inventory: String,
    pub avg_price: String,
}

/// A single fill event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillRecord {
    pub order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub label: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub timestamp: String,
    pub strategy: String, // "maker" or "arb"
}

/// Aggregate exposure summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposureSummary {
    pub total_open_orders: u64,
    pub total_positions: u64,
    pub total_exposure_usd: String,
    pub total_pnl: String,
    pub daily_pnl: String,
    pub recent_fills: Vec<FillRecord>,
}

/// Valkey-backed state store.
///
/// All keys are namespaced under a configurable prefix to allow multiple
/// instances (e.g. paper vs live) to share a single Valkey without collisions.
/// Default prefix: "surebet" → keys like "surebet:order:{id}", "surebet:fills", etc.
#[derive(Clone)]
pub struct StateStore {
    conn: MultiplexedConnection,
    activity_ttl: Duration,
    prefix: String,
}

impl StateStore {
    /// Connect to Valkey/Redis.
    pub async fn connect(url: &str, activity_ttl: Duration, prefix: &str) -> anyhow::Result<Self> {
        let client = Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        info!(url = url, prefix = prefix, "connected to Valkey");
        Ok(Self {
            conn,
            activity_ttl,
            prefix: prefix.to_string(),
        })
    }

    /// Build a namespaced key: "{prefix}:{suffix}"
    fn key(&self, suffix: &str) -> String {
        format!("{}:{}", self.prefix, suffix)
    }

    /// Test connectivity.
    pub async fn ping(&mut self) -> anyhow::Result<()> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut self.conn)
            .await?;
        debug!(response = %pong, "Valkey ping");
        Ok(())
    }

    // --- Orders ---

    /// Store an order. Sets TTL of 24h.
    pub async fn set_order(&mut self, order: &OrderRecord) -> anyhow::Result<()> {
        let key = self.key(&format!("order:{}", order.order_id));
        let json = serde_json::to_string(order)?;
        self.conn
            .set_ex::<_, _, ()>(&key, &json, ORDER_TTL_SECS)
            .await?;

        // Add to market index
        let market_key = self.key(&format!("market_orders:{}", order.condition_id));
        self.conn
            .sadd::<_, _, ()>(&market_key, &order.order_id)
            .await?;

        debug!(order_id = %order.order_id, status = %order.status, "stored order");
        Ok(())
    }

    /// Get a single order.
    pub async fn get_order(&mut self, order_id: &str) -> anyhow::Result<Option<OrderRecord>> {
        let key = self.key(&format!("order:{}", order_id));
        let json: Option<String> = self.conn.get(&key).await?;
        match json {
            Some(j) => Ok(Some(serde_json::from_str(&j)?)),
            None => Ok(None),
        }
    }

    /// Update order status.
    pub async fn update_order_status(
        &mut self,
        order_id: &str,
        status: OrderStatus,
    ) -> anyhow::Result<()> {
        if let Some(mut order) = self.get_order(order_id).await? {
            order.status = status;
            order.updated_at = chrono::Utc::now().to_rfc3339();
            self.set_order(&order).await?;
        }
        Ok(())
    }

    /// Get all order IDs for a market.
    pub async fn get_market_order_ids(
        &mut self,
        condition_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let key = self.key(&format!("market_orders:{}", condition_id));
        let ids: Vec<String> = self.conn.smembers(&key).await?;
        Ok(ids)
    }

    /// Get all open orders across all markets.
    pub async fn get_all_open_orders(&mut self) -> anyhow::Result<Vec<OrderRecord>> {
        // Scan for namespaced order:* keys
        let pattern = self.key("order:*");
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut self.conn)
            .await?;

        let mut orders = Vec::new();
        for key in keys {
            let json: Option<String> = self.conn.get(&key).await?;
            if let Some(j) = json {
                if let Ok(order) = serde_json::from_str::<OrderRecord>(&j) {
                    if order.status == OrderStatus::Open
                        || order.status == OrderStatus::PartiallyFilled
                    {
                        orders.push(order);
                    }
                }
            }
        }
        Ok(orders)
    }

    /// Remove order from market index (on cancel/fill).
    pub async fn remove_from_market_index(
        &mut self,
        condition_id: &str,
        order_id: &str,
    ) -> anyhow::Result<()> {
        let key = self.key(&format!("market_orders:{}", condition_id));
        self.conn.srem::<_, _, ()>(&key, order_id).await?;
        Ok(())
    }

    // --- Positions ---

    /// Update position for a market.
    pub async fn set_position(&mut self, position: &PositionRecord) -> anyhow::Result<()> {
        let key = self.key(&format!("position:{}", position.condition_id));
        let json = serde_json::to_string(position)?;
        self.conn.set::<_, _, ()>(&key, &json).await?;
        Ok(())
    }

    /// Get position for a market.
    pub async fn get_position(
        &mut self,
        condition_id: &str,
    ) -> anyhow::Result<Option<PositionRecord>> {
        let key = self.key(&format!("position:{}", condition_id));
        let json: Option<String> = self.conn.get(&key).await?;
        match json {
            Some(j) => Ok(Some(serde_json::from_str(&j)?)),
            None => Ok(None),
        }
    }

    /// Get all positions.
    pub async fn get_all_positions(&mut self) -> anyhow::Result<Vec<PositionRecord>> {
        let pattern = self.key("position:*");
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut self.conn)
            .await?;

        let mut positions = Vec::new();
        for key in keys {
            let json: Option<String> = self.conn.get(&key).await?;
            if let Some(j) = json {
                if let Ok(pos) = serde_json::from_str::<PositionRecord>(&j) {
                    positions.push(pos);
                }
            }
        }
        Ok(positions)
    }

    // --- Fills ---

    /// Record a fill. Appends to the fill log and updates PnL.
    pub async fn record_fill(&mut self, fill: &FillRecord) -> anyhow::Result<()> {
        let json = serde_json::to_string(fill)?;
        let fills_key = self.key("fills");

        // Append to fill log (left-push so newest is first)
        self.conn.lpush::<_, _, ()>(&fills_key, &json).await?;
        // Cap the list
        self.conn
            .ltrim::<_, ()>(&fills_key, 0, MAX_FILL_LOG - 1)
            .await?;

        debug!(
            order_id = %fill.order_id,
            price = %fill.price,
            size = %fill.size,
            "fill recorded"
        );
        Ok(())
    }

    /// Get recent fills (newest first).
    pub async fn get_recent_fills(&mut self, count: isize) -> anyhow::Result<Vec<FillRecord>> {
        let fills_key = self.key("fills");
        let jsons: Vec<String> = self.conn.lrange(&fills_key, 0, count - 1).await?;
        let mut fills = Vec::new();
        for j in jsons {
            if let Ok(f) = serde_json::from_str::<FillRecord>(&j) {
                fills.push(f);
            }
        }
        Ok(fills)
    }

    // --- PnL ---

    /// Add to running PnL total.
    pub async fn add_pnl(&mut self, amount: Decimal) -> anyhow::Result<()> {
        // Get current total
        let pnl_key = self.key("pnl:total");
        let current: String = self.conn.get(&pnl_key).await.unwrap_or("0".to_string());
        let current_dec = Decimal::from_str(&current).unwrap_or(Decimal::ZERO);
        let new_total = current_dec + amount;
        self.conn
            .set::<_, _, ()>(&pnl_key, new_total.to_string())
            .await?;

        // Daily PnL
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let daily_key = self.key(&format!("pnl:daily:{}", today));
        let daily: String = self.conn.get(&daily_key).await.unwrap_or("0".to_string());
        let daily_dec = Decimal::from_str(&daily).unwrap_or(Decimal::ZERO);
        let new_daily = daily_dec + amount;
        self.conn
            .set_ex::<_, _, ()>(&daily_key, new_daily.to_string(), 172800) // 48h TTL
            .await?;

        info!(
            amount = %amount,
            total = %new_total,
            daily = %new_daily,
            "PnL updated"
        );
        Ok(())
    }

    /// Get total PnL.
    pub async fn get_total_pnl(&mut self) -> anyhow::Result<Decimal> {
        let key = self.key("pnl:total");
        let val: String = self.conn.get(&key).await.unwrap_or("0".to_string());
        Ok(Decimal::from_str(&val).unwrap_or(Decimal::ZERO))
    }

    /// Get daily PnL.
    pub async fn get_daily_pnl(&mut self) -> anyhow::Result<Decimal> {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let key = self.key(&format!("pnl:daily:{}", today));
        let val: String = self.conn.get(&key).await.unwrap_or("0".to_string());
        Ok(Decimal::from_str(&val).unwrap_or(Decimal::ZERO))
    }

    // --- Activity Tracking ---

    /// Record trade activity on a token. Sets a key with TTL.
    pub async fn record_activity(&mut self, token_id: &str) -> anyhow::Result<()> {
        let key = self.key(&format!("activity:{}", token_id));
        self.conn
            .set_ex::<_, _, ()>(&key, "1", self.activity_ttl.as_secs())
            .await?;
        Ok(())
    }

    /// Check if a token has recent activity (key exists = active).
    pub async fn has_recent_activity(&mut self, token_id: &str) -> anyhow::Result<bool> {
        let key = self.key(&format!("activity:{}", token_id));
        let exists: bool = self.conn.exists(&key).await?;
        Ok(exists)
    }

    // --- Aggregate ---

    /// Build a full exposure summary for the dashboard.
    pub async fn get_exposure_summary(&mut self) -> anyhow::Result<ExposureSummary> {
        let open_orders = self.get_all_open_orders().await?;
        let positions = self.get_all_positions().await?;
        let recent_fills = self.get_recent_fills(50).await?;
        let total_pnl = self.get_total_pnl().await?;
        let daily_pnl = self.get_daily_pnl().await?;

        // Calculate total exposure from open orders
        let total_exposure: Decimal = open_orders
            .iter()
            .filter_map(|o| {
                let price = Decimal::from_str(&o.price).ok()?;
                let size = Decimal::from_str(&o.size).ok()?;
                Some(price * size)
            })
            .sum();

        Ok(ExposureSummary {
            total_open_orders: open_orders.len() as u64,
            total_positions: positions.len() as u64,
            total_exposure_usd: total_exposure.to_string(),
            total_pnl: total_pnl.to_string(),
            daily_pnl: daily_pnl.to_string(),
            recent_fills,
        })
    }

    /// Flush all keys under this instance's namespace (not the whole DB).
    pub async fn flush_all(&mut self) -> anyhow::Result<()> {
        let pattern = self.key("*");
        warn!(prefix = %self.prefix, "flushing all keys under namespace");
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut self.conn)
            .await?;
        if !keys.is_empty() {
            let mut cmd = redis::cmd("DEL");
            for k in &keys {
                cmd.arg(k);
            }
            cmd.query_async::<()>(&mut self.conn).await?;
            info!(count = keys.len(), "flushed namespaced keys");
        }
        Ok(())
    }
}
