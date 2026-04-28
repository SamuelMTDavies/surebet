//! Paginated data fetching from the Polymarket Data API.
//!
//! Fetches a trader's complete history: trades, activity, positions,
//! closed positions, portfolio value, and market count.

use anyhow::{Context, Result};
use polymarket_client_sdk_v2::data;
use polymarket_client_sdk_v2::data::types::request::{
    ActivityRequest, ClosedPositionsRequest, PositionsRequest, TradesRequest, TradedRequest,
    ValueRequest,
};
use polymarket_client_sdk_v2::data::types::response::{
    Activity, ClosedPosition, Position, Trade, Traded, Value,
};
use polymarket_client_sdk_v2::data::types::SortDirection;
use polymarket_client_sdk_v2::types::Address;
use rust_decimal::Decimal;
use tracing::{info, warn};

/// All raw data fetched for a single trader address.
#[derive(Debug, Clone)]
pub struct RawTraderData {
    pub address: Address,
    pub trades: Vec<Trade>,
    pub activity: Vec<Activity>,
    pub open_positions: Vec<Position>,
    pub closed_positions: Vec<ClosedPosition>,
    pub portfolio_value: Decimal,
    pub markets_traded: i32,
}

/// Fetch all available data for a trader address.
pub async fn fetch_all(address: &Address) -> Result<RawTraderData> {
    let client = data::Client::default();

    info!(%address, "fetching trader data from Data API");

    // Fetch all endpoints concurrently
    let (trades, activity, open_positions, closed_positions, value_resp, traded_resp) = tokio::try_join!(
        paginate_trades(&client, address),
        paginate_activity(&client, address),
        paginate_positions(&client, address),
        paginate_closed_positions(&client, address),
        fetch_value(&client, address),
        fetch_traded(&client, address),
    )?;

    let portfolio_value = value_resp
        .first()
        .map(|v| v.value)
        .unwrap_or(Decimal::ZERO);
    let markets_traded = traded_resp.traded;

    info!(
        trades = trades.len(),
        activity = activity.len(),
        open_positions = open_positions.len(),
        closed_positions = closed_positions.len(),
        portfolio_value = %portfolio_value,
        markets_traded,
        "fetched all trader data"
    );

    Ok(RawTraderData {
        address: *address,
        trades,
        activity,
        open_positions,
        closed_positions,
        portfolio_value,
        markets_traded,
    })
}

async fn paginate_trades(client: &data::Client, addr: &Address) -> Result<Vec<Trade>> {
    let mut all = Vec::new();
    let page_size = 10_000i32;
    let mut offset = 0i32;

    loop {
        let req = TradesRequest::builder()
            .user(*addr)
            .limit(page_size)?
            .offset(offset)?
            .taker_only(false)
            .build();

        let page = client
            .trades(&req)
            .await
            .context("failed to fetch trades")?;
        let count = page.len() as i32;
        info!(offset, count, "fetched trades page");
        all.extend(page);

        if count < page_size {
            break;
        }
        offset += count;
        if offset >= 10_000 {
            warn!("trades pagination hit offset limit at {offset}, may be missing older trades");
            break;
        }
    }

    // Sort by timestamp ascending for chronological analysis
    all.sort_by_key(|t| t.timestamp);
    Ok(all)
}

async fn paginate_activity(client: &data::Client, addr: &Address) -> Result<Vec<Activity>> {
    let mut all = Vec::new();
    let page_size = 500i32;
    // The API's real max offset for activity is ~3000 (not the SDK's stated 10000).
    // When we hit that ceiling we switch to time-range windowing using the `end`
    // parameter to fetch older data.
    const MAX_OFFSET: i32 = 2500; // stay safely below 3000

    let mut window_end: Option<u64> = None; // None = no upper bound (latest first)

    'outer: loop {
        let mut offset = 0i32;

        loop {
            let req = match window_end {
                Some(end_ts) => ActivityRequest::builder()
                    .user(*addr)
                    .limit(page_size)?
                    .offset(offset)?
                    .sort_direction(SortDirection::Desc)
                    .end(end_ts)
                    .build(),
                None => ActivityRequest::builder()
                    .user(*addr)
                    .limit(page_size)?
                    .offset(offset)?
                    .sort_direction(SortDirection::Desc)
                    .build(),
            };

            let page = client
                .activity(&req)
                .await
                .context("failed to fetch activity")?;
            let count = page.len() as i32;
            info!(offset, count, window_end, "fetched activity page");
            all.extend(page);

            if count < page_size {
                // No more data in this window (or overall)
                break 'outer;
            }

            offset += count;
            if offset >= MAX_OFFSET {
                // Hit the offset ceiling — need to start a new time window.
                // Use the oldest timestamp we've seen so far as the new upper bound.
                if let Some(oldest) = all.iter().map(|a| a.timestamp).min() {
                    let new_end = (oldest as u64).saturating_sub(1);
                    if window_end == Some(new_end) {
                        // No progress — bail to avoid infinite loop
                        warn!("activity time-range windowing stalled at ts={oldest}");
                        break 'outer;
                    }
                    window_end = Some(new_end);
                    info!(new_window_end = new_end, total_so_far = all.len(), "activity: sliding time window");
                    break; // restart offset=0 with the new window
                } else {
                    break 'outer;
                }
            }
        }
    }

    all.sort_by_key(|a| a.timestamp);
    // Deduplicate by transaction_hash + timestamp (in case windows overlap)
    all.dedup_by(|a, b| a.transaction_hash == b.transaction_hash && a.timestamp == b.timestamp);
    Ok(all)
}

async fn paginate_positions(client: &data::Client, addr: &Address) -> Result<Vec<Position>> {
    let mut all = Vec::new();
    let page_size = 500i32;
    let mut offset = 0i32;

    loop {
        let req = PositionsRequest::builder()
            .user(*addr)
            .limit(page_size)?
            .offset(offset)?
            .build();

        let page = client
            .positions(&req)
            .await
            .context("failed to fetch positions")?;
        let count = page.len() as i32;
        info!(offset, count, "fetched positions page");
        all.extend(page);

        if count < page_size {
            break;
        }
        offset += count;
        if offset >= 10_000 {
            break;
        }
    }

    Ok(all)
}

async fn paginate_closed_positions(
    client: &data::Client,
    addr: &Address,
) -> Result<Vec<ClosedPosition>> {
    let mut all = Vec::new();
    let page_size = 50i32;
    let mut offset = 0i32;

    loop {
        let req = ClosedPositionsRequest::builder()
            .user(*addr)
            .limit(page_size)?
            .offset(offset)?
            .build();

        let page = client
            .closed_positions(&req)
            .await
            .context("failed to fetch closed positions")?;
        let count = page.len() as i32;
        info!(offset, count, "fetched closed positions page");
        all.extend(page);

        if count < page_size {
            break;
        }
        offset += count;
        if offset >= 100_000 {
            warn!("closed positions pagination hit offset limit at {offset}");
            break;
        }
    }

    Ok(all)
}

async fn fetch_value(client: &data::Client, addr: &Address) -> Result<Vec<Value>> {
    client
        .value(&ValueRequest::builder().user(*addr).build())
        .await
        .context("failed to fetch portfolio value")
}

async fn fetch_traded(client: &data::Client, addr: &Address) -> Result<Traded> {
    client
        .traded(&TradedRequest::builder().user(*addr).build())
        .await
        .context("failed to fetch traded count")
}
