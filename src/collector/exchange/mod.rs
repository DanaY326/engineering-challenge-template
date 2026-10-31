//! The exchange layer: the Kalshi codec, and the normalized [`FeedEvent`]s
//! it decodes frames into.

pub mod kalshi;

use chrono::{DateTime, Utc};

use super::book::LevelChange;
use crate::Side;

/// An exchange message, normalized to fixed point and the bid/ask convention
/// of the output schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEvent {
    /// The full book for `symbol`, replacing anything known before.
    Snapshot {
        symbol: String,
        exchange_ts: Option<DateTime<Utc>>,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
    },
    /// A change to a single price level.
    Level {
        symbol: String,
        exchange_ts: Option<DateTime<Utc>>,
        side: Side,
        price: i64,
        change: LevelChange,
    },
    /// This connection lost data for `symbols` (e.g. a sequence gap); their
    /// books on this connection are invalid until the next snapshot.
    Desync { symbols: Vec<String>, reason: String },
}

/// GET a JSON page, retrying rate limits (429) and server errors with
/// exponential backoff. Symbol discovery pages through thousands of markets,
/// so being rate limited partway through is expected.
pub(crate) async fn get_json<T: serde::de::DeserializeOwned>(
    request: reqwest::RequestBuilder,
) -> anyhow::Result<T> {
    let mut backoff = std::time::Duration::from_secs(1);
    for attempt in 0.. {
        let response = request
            .try_clone()
            .expect("GET requests have no streaming body")
            .send()
            .await?;
        let status = response.status();
        if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
            && attempt < 6
        {
            tracing::debug!("{status} from {}; retrying in {backoff:?}", response.url());
            tokio::time::sleep(backoff).await;
            backoff *= 2;
            continue;
        }
        return Ok(response.error_for_status()?.json().await?);
    }
    unreachable!()
}

pub(crate) fn millis_to_utc(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}
