pub mod clob;
pub mod rtds;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum WsError {
    #[error("websocket connection failed: {0}")]
    Connection(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("connection closed unexpectedly")]
    Closed,
    #[error("ping timeout - no pong received")]
    PingTimeout,
}
