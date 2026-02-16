//! L2 HMAC-SHA256 authentication for Polymarket CLOB API.
//!
//! Polymarket uses a two-level auth system:
//! - L1: EIP-712 wallet signatures (used to derive API credentials)
//! - L2: HMAC-SHA256 signed requests (used for all trading operations)
//!
//! This module implements L2. Credentials (api_key, secret, passphrase)
//! must be derived externally using py-clob-client or rs-clob-client.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::Sha256;
use thiserror::Error;
use tracing::debug;

type HmacSha256 = Hmac<Sha256>;

const HEADER_API_KEY: &str = "POLY_API_KEY";
const HEADER_SIGNATURE: &str = "POLY_SIGNATURE";
const HEADER_TIMESTAMP: &str = "POLY_TIMESTAMP";
const HEADER_PASSPHRASE: &str = "POLY_PASSPHRASE";

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("missing API credentials")]
    MissingCredentials,
    #[error("HMAC key error: {0}")]
    HmacKey(String),
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("API error {status}: {body}")]
    ApiError { status: u16, body: String },
}

#[derive(Debug, Clone)]
pub struct L2Credentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

impl L2Credentials {
    pub fn from_config(api_key: &str, secret: &str, passphrase: &str) -> Option<Self> {
        if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
            return None;
        }
        Some(Self {
            api_key: api_key.to_string(),
            secret: secret.to_string(),
            passphrase: passphrase.to_string(),
        })
    }
}

/// Build L2 auth headers for a CLOB API request.
///
/// The signature is HMAC-SHA256(secret, timestamp + method + path + body)
/// encoded as base64.
pub fn build_l2_headers(
    creds: &L2Credentials,
    method: &str,
    path: &str,
    body: &str,
) -> Result<HeaderMap, AuthError> {
    let timestamp = chrono::Utc::now().timestamp().to_string();

    let message = format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body);

    let secret_bytes = BASE64
        .decode(&creds.secret)
        .map_err(|e| AuthError::HmacKey(e.to_string()))?;

    let mut mac = HmacSha256::new_from_slice(&secret_bytes)
        .map_err(|e| AuthError::HmacKey(e.to_string()))?;
    mac.update(message.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());

    debug!(
        method = method,
        path = path,
        timestamp = %timestamp,
        "built L2 auth headers"
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        HEADER_API_KEY,
        HeaderValue::from_str(&creds.api_key).unwrap(),
    );
    headers.insert(
        HEADER_SIGNATURE,
        HeaderValue::from_str(&signature).unwrap(),
    );
    headers.insert(
        HEADER_TIMESTAMP,
        HeaderValue::from_str(&timestamp).unwrap(),
    );
    headers.insert(
        HEADER_PASSPHRASE,
        HeaderValue::from_str(&creds.passphrase).unwrap(),
    );

    Ok(headers)
}

/// Authenticated HTTP client for Polymarket CLOB REST API.
pub struct ClobApiClient {
    client: reqwest::Client,
    base_url: String,
    creds: L2Credentials,
}

impl ClobApiClient {
    pub fn new(base_url: String, creds: L2Credentials) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            creds,
        }
    }

    /// GET with L2 auth.
    pub async fn get(&self, path: &str) -> Result<serde_json::Value, AuthError> {
        let headers = build_l2_headers(&self.creds, "GET", path, "")?;
        let url = format!("{}{}", self.base_url, path);

        let resp = self.client.get(&url).headers(headers).send().await?;
        let status = resp.status().as_u16();
        if status >= 400 {
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::ApiError { status, body });
        }
        Ok(resp.json().await?)
    }

    /// POST with L2 auth and JSON body.
    pub async fn post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, AuthError> {
        let body_str = serde_json::to_string(body).unwrap_or_default();
        let headers = build_l2_headers(&self.creds, "POST", path, &body_str)?;
        let url = format!("{}{}", self.base_url, path);

        let resp = self
            .client
            .post(&url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::ApiError { status, body });
        }
        Ok(resp.json().await?)
    }

    /// DELETE with L2 auth and JSON body.
    pub async fn delete(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, AuthError> {
        let body_str = serde_json::to_string(body).unwrap_or_default();
        let headers = build_l2_headers(&self.creds, "DELETE", path, &body_str)?;
        let url = format!("{}{}", self.base_url, path);

        let resp = self
            .client
            .delete(&url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::ApiError { status, body });
        }
        Ok(resp.json().await?)
    }

    /// Place a single limit order. Returns a typed response with order_id.
    pub async fn place_order(
        &self,
        token_id: &str,
        price: &str,
        size: &str,
        side: OrderSide,
    ) -> Result<PlaceOrderResponse, AuthError> {
        let body = serde_json::json!({
            "tokenID": token_id,
            "price": price,
            "size": size,
            "side": side.as_str(),
        });
        let resp = self.post("/order", &body).await?;
        let parsed: PlaceOrderResponse =
            serde_json::from_value(resp).unwrap_or_else(|_| PlaceOrderResponse {
                success: false,
                error_msg: "failed to parse response".to_string(),
                order_id: String::new(),
                status: String::new(),
            });
        Ok(parsed)
    }

    /// Place a batch of orders (up to 15). Returns typed responses with order_ids.
    pub async fn place_orders(
        &self,
        orders: &[OrderRequest],
    ) -> Result<Vec<PlaceOrderResponse>, AuthError> {
        let body = serde_json::json!({
            "orders": orders,
        });
        let resp = self.post("/orders", &body).await?;

        // Batch response can be an array or an object wrapping an array
        let responses = if let Some(arr) = resp.as_array() {
            arr.iter()
                .filter_map(|v| serde_json::from_value::<PlaceOrderResponse>(v.clone()).ok())
                .collect()
        } else if let Some(arr) = resp.get("orderResponses").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| serde_json::from_value::<PlaceOrderResponse>(v.clone()).ok())
                .collect()
        } else {
            // Single response wrapped
            match serde_json::from_value::<PlaceOrderResponse>(resp) {
                Ok(r) => vec![r],
                Err(_) => vec![],
            }
        };
        Ok(responses)
    }

    /// Cancel a single order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<serde_json::Value, AuthError> {
        let body = serde_json::json!({ "orderID": order_id });
        self.delete("/order", &body).await
    }

    /// Cancel all open orders.
    pub async fn cancel_all(&self) -> Result<serde_json::Value, AuthError> {
        self.delete("/cancel-all", &serde_json::json!({})).await
    }

    /// Cancel all orders for a specific market.
    pub async fn cancel_market_orders(
        &self,
        market: &str,
    ) -> Result<serde_json::Value, AuthError> {
        let body = serde_json::json!({ "market": market });
        self.delete("/cancel-market-orders", &body).await
    }

    /// Get open orders.
    pub async fn get_orders(&self) -> Result<serde_json::Value, AuthError> {
        self.get("/orders").await
    }

    /// Get trade history.
    pub async fn get_trades(&self) -> Result<serde_json::Value, AuthError> {
        self.get("/trades").await
    }

    /// Send heartbeat (dead man's switch). If heartbeats stop,
    /// Polymarket auto-cancels all open orders.
    pub async fn send_heartbeat(&self) -> Result<serde_json::Value, AuthError> {
        self.get("/heartbeat").await
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderRequest {
    #[serde(rename = "tokenID")]
    pub token_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
}

/// Response from the CLOB API when placing an order.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PlaceOrderResponse {
    #[serde(default)]
    pub success: bool,
    #[serde(default, rename = "errorMsg")]
    pub error_msg: String,
    #[serde(default, rename = "orderID")]
    pub order_id: String,
    #[serde(default)]
    pub status: String,
}
