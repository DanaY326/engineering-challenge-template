//! The exchange-specific layer (Extension 3).
//!
//! Everything exchange-specific lives behind [`Exchange`] and [`Session`]:
//! how to open an (authenticated) websocket, how to encode subscription
//! commands, how to decode frames into normalized [`FeedEvent`]s, and how to
//! list the symbols that should currently be collected. Sharding, redundancy,
//! failover, monitoring, and output are generic over these traits.

pub mod kalshi;
pub mod polymarket;

use std::future::Future;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::handshake::client::Request;

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
        /// Identifies the same exchange update across connections, so we can
        /// measure which connection received it first (Extension 4).
        match_key: Option<u64>,
    },
    /// This connection lost data for `symbols` (e.g. a sequence gap); their
    /// books on this connection are invalid until the next snapshot.
    Desync { symbols: Vec<String>, reason: String },
    /// An update for `symbol` that was received but deliberately not applied
    /// (e.g. already included in the latest snapshot). It still proves the
    /// subscription is live, for cross-replica monitoring.
    Seen { symbol: String, match_key: Option<u64> },
}

/// Static, shareable exchange configuration and REST access.
pub trait Exchange: Send + Sync + 'static {
    type Session: Session;

    fn name(&self) -> &'static str;

    /// Build the websocket handshake request, including any auth headers.
    /// Called for every (re)connect, so signatures are always fresh.
    fn connect_request(&self) -> anyhow::Result<Request>;

    /// Fresh protocol state for one websocket connection.
    fn new_session(&self) -> Self::Session;

    /// Every symbol that should currently be collected. Each page is also
    /// sent on `pages` as soon as it arrives, so collection can start before
    /// a long listing finishes. Implementations may stop paginating once
    /// they have `limit` symbols.
    fn list_symbols(
        &self,
        http: &reqwest::Client,
        limit: Option<usize>,
        pages: UnboundedSender<Vec<String>>,
    ) -> impl Future<Output = anyhow::Result<Vec<String>>> + Send;

    /// An application-level text heartbeat to send periodically, for
    /// exchanges that do not rely on websocket ping frames.
    fn app_heartbeat(&self) -> Option<&'static str> {
        None
    }

    /// The most websockets the exchange allows us to hold open at once.
    fn max_connections(&self) -> Option<usize> {
        None
    }
}

/// Per-connection protocol state. Methods push the text frames that must be
/// sent to the exchange onto `out`.
pub trait Session: Send + 'static {
    fn subscribe(&mut self, symbols: &[String], out: &mut Vec<String>);

    fn unsubscribe(&mut self, symbols: &[String], out: &mut Vec<String>);

    /// Unsubscribe and subscribe again, forcing a fresh snapshot. Emits a
    /// [`FeedEvent::Desync`] for the symbol, since updates between the two
    /// commands are lost.
    fn resubscribe(&mut self, symbol: &str, events: &mut Vec<FeedEvent>, out: &mut Vec<String>);

    /// Decode one text frame. Returning an error tears down the connection.
    fn decode(
        &mut self,
        text: &str,
        events: &mut Vec<FeedEvent>,
        out: &mut Vec<String>,
    ) -> anyhow::Result<()>;
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

/// Stable within a process; only used to match updates across connections.
pub(crate) fn hash_key(parts: impl std::hash::Hash) -> u64 {
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    BuildHasherDefault::<DefaultHasher>::default().hash_one(parts)
}

pub(crate) fn millis_to_utc(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}
