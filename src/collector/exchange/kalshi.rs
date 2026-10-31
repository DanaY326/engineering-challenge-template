//! Kalshi `orderbook_delta` websocket channel.
//!
//! Book convention: Kalshi publishes resting YES bids and resting NO bids. A
//! NO bid at price `p` is economically a YES ask at `1 - p`, so we emit YES
//! bids as bids and NO bids as asks at `SCALE - p`. The symbol is the market
//! ticker.
//!
//! Each `subscribe` command creates a subscription (`sid`) whose messages carry
//! a gapless `seq`. We subscribe in chunks so that a gap only invalidates the
//! tickers of one chunk, which we then resubscribe from scratch.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use rsa::sha2::Sha256;
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{FeedEvent, get_json, millis_to_utc};
use crate::Side;
use crate::collector::book::LevelChange;
use crate::collector::fixed::{SCALE, parse_fixed};

pub const PROD_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const PROD_REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const DEMO_WS_URL: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";
pub const DEMO_REST_URL: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
const WS_PATH: &str = "/trade-api/ws/v2";

/// Tickers per `subscribe` command (and therefore per `sid`).
const SUBSCRIBE_CHUNK: usize = 100;

enum Signer {
    Rsa(rsa::pss::BlindedSigningKey<Sha256>),
    Ed25519(ed25519_dalek::SigningKey),
}

impl Signer {
    /// Kalshi accepts RSA (PKCS#1 or PKCS#8 PEM, signed with RSA-PSS/SHA-256,
    /// salt = digest length) or Ed25519 (PKCS#8 PEM) keys.
    fn from_pem(pem: &str) -> anyhow::Result<Self> {
        // rsa and ed25519-dalek share the `pkcs8` crate's DecodePrivateKey.
        use ed25519_dalek::pkcs8::DecodePrivateKey as _;
        use rsa::pkcs1::DecodeRsaPrivateKey as _;

        if let Ok(key) = rsa::RsaPrivateKey::from_pkcs1_pem(pem) {
            return Ok(Self::Rsa(rsa::pss::BlindedSigningKey::new(key)));
        }
        if let Ok(key) = rsa::RsaPrivateKey::from_pkcs8_pem(pem) {
            return Ok(Self::Rsa(rsa::pss::BlindedSigningKey::new(key)));
        }
        if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self::Ed25519(key));
        }
        bail!("private key is not an RSA (PKCS#1/PKCS#8) or Ed25519 (PKCS#8) PEM")
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        match self {
            Self::Rsa(key) => {
                use rsa::signature::{RandomizedSigner as _, SignatureEncoding as _};
                key.sign_with_rng(&mut rand::thread_rng(), message)
                    .to_vec()
            }
            Self::Ed25519(key) => {
                use ed25519_dalek::Signer as _;
                key.sign(message).to_bytes().to_vec()
            }
        }
    }
}

pub struct Credentials {
    key_id: String,
    signer: Signer,
}

impl Credentials {
    pub fn new(key_id: String, private_key_pem: &str) -> anyhow::Result<Self> {
        Ok(Self {
            key_id,
            signer: Signer::from_pem(private_key_pem)?,
        })
    }
}

pub struct Kalshi {
    pub ws_url: String,
    pub rest_url: String,
    pub credentials: Option<Credentials>,
}

impl Kalshi {
    pub fn new(demo: bool, credentials: Option<Credentials>) -> Self {
        let (ws_url, rest_url) = if demo {
            (DEMO_WS_URL, DEMO_REST_URL)
        } else {
            (PROD_WS_URL, PROD_REST_URL)
        };
        Self {
            ws_url: ws_url.to_owned(),
            rest_url: rest_url.to_owned(),
            credentials,
        }
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<MarketInfo>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct MarketInfo {
    ticker: String,
    #[serde(default)]
    status: String,
}

impl Kalshi {
    pub fn name(&self) -> &'static str {
        "kalshi"
    }

    pub fn connect_request(&self) -> anyhow::Result<Request> {
        let mut request = self.ws_url.as_str().into_client_request()?;
        if let Some(creds) = &self.credentials {
            let timestamp = Utc::now().timestamp_millis().to_string();
            let signature = creds
                .signer
                .sign(format!("{timestamp}GET{WS_PATH}").as_bytes());
            let headers = request.headers_mut();
            headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&creds.key_id)?);
            headers.insert(
                "KALSHI-ACCESS-SIGNATURE",
                HeaderValue::from_str(&BASE64.encode(signature))?,
            );
            headers.insert(
                "KALSHI-ACCESS-TIMESTAMP",
                HeaderValue::from_str(&timestamp)?,
            );
        }
        Ok(request)
    }

    pub fn new_session(&self) -> KalshiSession {
        KalshiSession::default()
    }

    /// Not documented; measured on 2026-10-30: the 201st concurrent socket
    /// on one account is refused with HTTP 429.
    pub fn max_connections(&self) -> Option<usize> {
        Some(200)
    }

    /// All open, non-multivariate markets.
    pub async fn list_symbols(
        &self,
        http: &reqwest::Client,
        limit: Option<usize>,
        pages: UnboundedSender<Vec<String>>,
    ) -> anyhow::Result<Vec<String>> {
        let mut tickers = Vec::new();
        let mut cursor = String::new();
        loop {
            let mut request = http.get(format!("{}/markets", self.rest_url)).query(&[
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", "1000"),
            ]);
            if !cursor.is_empty() {
                request = request.query(&[("cursor", &cursor)]);
            }
            let page: MarketsPage = get_json(request)
                .await
                .context("fetching Kalshi markets page")?;
            let done = page.markets.is_empty();
            let page_tickers: Vec<String> = page
                .markets
                .into_iter()
                .filter(|m| m.status == "active" || m.status == "open")
                .map(|m| m.ticker)
                .collect();
            let _ = pages.send(page_tickers.clone());
            tickers.extend(page_tickers);
            let enough = limit.is_some_and(|limit| tickers.len() >= limit);
            match page.cursor {
                Some(next) if !next.is_empty() && !done && !enough => cursor = next,
                _ => break,
            }
            // Pace pages to stay under the unauthenticated REST rate limit.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(tickers)
    }
}

/// An outstanding command, keyed by its request `id`.
enum Pending {
    Subscribe(Vec<String>),
    /// `delete_markets` or `unsubscribe`; resubscribe the tickers once acked.
    Remove { tickers: Vec<String>, resubscribe: bool },
}

struct Subscription {
    tickers: BTreeSet<String>,
    last_seq: Option<u64>,
}

#[derive(Default)]
pub struct KalshiSession {
    next_id: u64,
    pending: HashMap<u64, Pending>,
    subs: HashMap<u64, Subscription>,
    ticker_sid: HashMap<String, u64>,
    /// Tickers in an in-flight `subscribe`, mapped to that command's id.
    ticker_pending: HashMap<String, u64>,
    /// Tickers unsubscribed while their `subscribe` was still in flight.
    cancelled: BTreeSet<String>,
    /// Clock of each ticker's latest snapshot, until a newer delta arrives.
    /// The snapshot already includes every change at or below its clock, so
    /// deltas up to it must be skipped or they would be applied twice.
    snapshot_clock: HashMap<String, i64>,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: String,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Subscribed {
    sid: u64,
}

#[derive(Deserialize)]
struct ErrorMsg {
    code: i64,
    #[serde(default)]
    msg: String,
}

#[derive(Deserialize)]
struct SnapshotMsg {
    market_ticker: String,
    #[serde(default)]
    clock: Option<i64>,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct DeltaMsg {
    market_ticker: String,
    #[serde(default)]
    clock: Option<i64>,
    price_dollars: String,
    delta_fp: String,
    side: String,
    #[serde(default)]
    ts_ms: Option<i64>,
}

/// Kalshi error code for "subscription buffer overflowed during a burst".
const ERR_BUFFER_OVERFLOW: i64 = 25;

impl KalshiSession {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Remove tickers from their subscriptions. If that empties a
    /// subscription we unsubscribe it entirely.
    fn remove(&mut self, tickers: &[String], resubscribe: bool, out: &mut Vec<String>) {
        let mut by_sid: HashMap<u64, Vec<String>> = HashMap::new();
        for ticker in tickers {
            if let Some(sid) = self.ticker_sid.remove(ticker) {
                by_sid.entry(sid).or_default().push(ticker.clone());
            } else if self.ticker_pending.contains_key(ticker) {
                self.cancelled.insert(ticker.clone());
            } else if resubscribe {
                // Nothing to remove (e.g. the original subscribe failed).
                self.subscribe(std::slice::from_ref(ticker), out);
            }
        }
        for (sid, tickers) in by_sid {
            let Some(sub) = self.subs.get_mut(&sid) else {
                continue;
            };
            for ticker in &tickers {
                sub.tickers.remove(ticker);
            }
            let emptied = sub.tickers.is_empty();
            let id = self.next_id();
            let frame = if emptied {
                self.subs.remove(&sid);
                json!({"id": id, "cmd": "unsubscribe", "params": {"sids": [sid]}})
            } else {
                json!({"id": id, "cmd": "update_subscription", "params": {
                    "sids": [sid],
                    "market_tickers": tickers,
                    "action": "delete_markets",
                }})
            };
            out.push(frame.to_string());
            self.pending.insert(
                id,
                Pending::Remove {
                    tickers,
                    resubscribe,
                },
            );
        }
    }

    /// Validate `seq` for subscription `sid`. On a gap, drop the subscription
    /// and resubscribe all of its tickers.
    fn check_seq(
        &mut self,
        sid: u64,
        seq: u64,
        events: &mut Vec<FeedEvent>,
        out: &mut Vec<String>,
    ) -> bool {
        let Some(sub) = self.subs.get_mut(&sid) else {
            return false;
        };
        match sub.last_seq {
            Some(last) if seq != last + 1 => {
                let tickers: Vec<String> = sub.tickers.iter().cloned().collect();
                self.subs.remove(&sid);
                for ticker in &tickers {
                    self.ticker_sid.remove(ticker);
                }
                let id = self.next_id();
                out.push(
                    json!({"id": id, "cmd": "unsubscribe", "params": {"sids": [sid]}})
                        .to_string(),
                );
                events.push(FeedEvent::Desync {
                    symbols: tickers.clone(),
                    reason: format!("sid {sid} seq gap: expected {}, got {seq}", last + 1),
                });
                self.subscribe(&tickers, out);
                false
            }
            _ => {
                sub.last_seq = Some(seq);
                true
            }
        }
    }

    fn on_ack(&mut self, id: u64, sid: Option<u64>, out: &mut Vec<String>) {
        match self.pending.remove(&id) {
            Some(Pending::Subscribe(tickers)) => {
                let Some(sid) = sid else { return };
                let mut kept = BTreeSet::new();
                let mut cancelled = Vec::new();
                for ticker in tickers {
                    self.ticker_pending.remove(&ticker);
                    if self.cancelled.remove(&ticker) {
                        cancelled.push(ticker.clone());
                    }
                    self.ticker_sid.insert(ticker.clone(), sid);
                    kept.insert(ticker);
                }
                self.subs.insert(
                    sid,
                    Subscription {
                        tickers: kept,
                        last_seq: None,
                    },
                );
                if !cancelled.is_empty() {
                    self.remove(&cancelled, false, out);
                }
            }
            Some(Pending::Remove {
                tickers,
                resubscribe: true,
            }) => self.subscribe(&tickers, out),
            Some(Pending::Remove { .. }) | None => {}
        }
    }

    fn on_error(&mut self, id: Option<u64>, err: ErrorMsg, out: &mut Vec<String>) -> anyhow::Result<()> {
        let pending = id.and_then(|id| self.pending.remove(&id));
        match pending {
            Some(Pending::Subscribe(tickers)) => {
                // Leave these unsubscribed until the socket reconnects.
                tracing::warn!(
                    code = err.code,
                    "Kalshi rejected subscribe for {} tickers (first {}): {}",
                    tickers.len(),
                    tickers[0],
                    err.msg
                );
                for ticker in &tickers {
                    self.ticker_pending.remove(ticker);
                    self.cancelled.remove(ticker);
                }
            }
            Some(Pending::Remove {
                tickers,
                resubscribe,
            }) => {
                tracing::debug!(code = err.code, "Kalshi rejected removal: {}", err.msg);
                if resubscribe {
                    self.subscribe(&tickers, out);
                }
            }
            None if err.code == ERR_BUFFER_OVERFLOW => {
                bail!("Kalshi subscription buffer overflow: {}", err.msg)
            }
            None => tracing::warn!(code = err.code, "Kalshi error: {}", err.msg),
        }
        Ok(())
    }
}

/// `[(price_dollars, contracts)]` → fixed-point levels, mapping NO prices to
/// YES asks when `invert` is set.
fn levels(raw: &[(String, String)], invert: bool) -> anyhow::Result<Vec<(i64, i64)>> {
    raw.iter()
        .map(|(price, qty)| {
            let price = parse_fixed(price)?;
            Ok((if invert { SCALE - price } else { price }, parse_fixed(qty)?))
        })
        .collect()
}

impl KalshiSession {
    pub fn subscribe(&mut self, symbols: &[String], out: &mut Vec<String>) {
        let fresh: Vec<String> = symbols
            .iter()
            .filter(|t| !self.ticker_sid.contains_key(*t) && !self.ticker_pending.contains_key(*t))
            .cloned()
            .collect();
        for ticker in &fresh {
            self.cancelled.remove(ticker);
        }
        for chunk in fresh.chunks(SUBSCRIBE_CHUNK) {
            let id = self.next_id();
            out.push(
                json!({"id": id, "cmd": "subscribe", "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": chunk,
                    // NO levels at NO prices; we invert them ourselves. Set
                    // explicitly because Kalshi plans to flip the default.
                    "use_yes_price": false,
                }})
                .to_string(),
            );
            for ticker in chunk {
                self.ticker_pending.insert(ticker.clone(), id);
            }
            self.pending.insert(id, Pending::Subscribe(chunk.to_vec()));
        }
    }

    pub fn unsubscribe(&mut self, symbols: &[String], out: &mut Vec<String>) {
        self.remove(symbols, false, out);
    }

    pub fn resubscribe(&mut self, symbol: &str, events: &mut Vec<FeedEvent>, out: &mut Vec<String>) {
        if self.ticker_pending.contains_key(symbol) {
            // A subscribe is already in flight; its snapshot will resync us.
            return;
        }
        events.push(FeedEvent::Desync {
            symbols: vec![symbol.to_owned()],
            reason: "resubscribing".to_owned(),
        });
        self.remove(&[symbol.to_owned()], true, out);
    }

    pub fn decode(
        &mut self,
        text: &str,
        events: &mut Vec<FeedEvent>,
        out: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        let env: Envelope = serde_json::from_str(text).context("decoding Kalshi envelope")?;

        // Every message on a subscription (data and acks) consumes a seq.
        if let (Some(sid), Some(seq)) = (env.sid, env.seq)
            && !self.check_seq(sid, seq, events, out)
            && matches!(env.kind.as_str(), "orderbook_snapshot" | "orderbook_delta")
        {
            return Ok(());
        }

        let msg = env.msg.map(RawValue::get).unwrap_or("null");
        match env.kind.as_str() {
            "orderbook_snapshot" => {
                let snap: SnapshotMsg = serde_json::from_str(msg)?;
                if self.ticker_sid.get(&snap.market_ticker) != env.sid.as_ref() {
                    return Ok(());
                }
                match snap.clock {
                    Some(clock) => self.snapshot_clock.insert(snap.market_ticker.clone(), clock),
                    None => self.snapshot_clock.remove(&snap.market_ticker),
                };
                events.push(FeedEvent::Snapshot {
                    exchange_ts: env.sending_ts_ms.and_then(millis_to_utc),
                    bids: levels(&snap.yes_dollars_fp, false)?,
                    asks: levels(&snap.no_dollars_fp, true)?,
                    symbol: snap.market_ticker,
                });
            }
            "orderbook_delta" => {
                let delta: DeltaMsg = serde_json::from_str(msg)?;
                if self.ticker_sid.get(&delta.market_ticker) != env.sid.as_ref() {
                    return Ok(());
                }
                let price = parse_fixed(&delta.price_dollars)?;
                let qty = parse_fixed(&delta.delta_fp)?;
                let (side, price) = match delta.side.as_str() {
                    "yes" => (Side::Buy, price),
                    "no" => (Side::Sell, SCALE - price),
                    other => bail!("unknown Kalshi side {other:?}"),
                };
                if let (Some(clock), Some(&snapshot)) =
                    (delta.clock, self.snapshot_clock.get(&delta.market_ticker))
                {
                    if clock <= snapshot {
                        return Ok(());
                    }
                    // Only the first deltas after a snapshot can overlap it;
                    // forgetting the clock also keeps us safe if Kalshi's
                    // clock is reset (e.g. during maintenance).
                    self.snapshot_clock.remove(&delta.market_ticker);
                }
                events.push(FeedEvent::Level {
                    exchange_ts: delta
                        .ts_ms
                        .or(env.sending_ts_ms)
                        .and_then(millis_to_utc),
                    side,
                    price,
                    change: LevelChange::Delta(qty),
                    symbol: delta.market_ticker,
                });
            }
            "subscribed" => {
                let sub: Subscribed = serde_json::from_str(msg)?;
                if let Some(id) = env.id {
                    self.on_ack(id, Some(sub.sid), out);
                }
            }
            "ok" | "unsubscribed" => {
                if let Some(id) = env.id {
                    self.on_ack(id, env.sid, out);
                }
            }
            "error" => {
                let err: ErrorMsg = serde_json::from_str(msg)?;
                self.on_error(env.id, err, out)?;
            }
            other => tracing::trace!("ignoring Kalshi message type {other}"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(s: &mut KalshiSession, text: &str) -> (Vec<FeedEvent>, Vec<String>) {
        let (mut events, mut out) = (Vec::new(), Vec::new());
        s.decode(text, &mut events, &mut out).unwrap();
        (events, out)
    }

    fn subscribed_session() -> KalshiSession {
        let mut s = KalshiSession::default();
        let mut out = Vec::new();
        s.subscribe(&["FED".to_owned(), "CPI".to_owned()], &mut out);
        assert_eq!(out.len(), 1);
        assert!(out[0].contains(r#""cmd":"subscribe""#));
        decode(
            &mut s,
            r#"{"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 2}}"#,
        );
        s
    }

    #[test]
    fn snapshot_and_delta() {
        let mut s = subscribed_session();
        let (events, _) = decode(
            &mut s,
            r#"{"type": "orderbook_snapshot", "sending_ts_ms": 1669149841234, "sid": 2, "seq": 1, "msg": {"market_ticker": "FED", "clock": 4182057, "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]], "no_dollars_fp": [["0.5400", "20.00"], ["0.5600", "146.00"]]}}"#,
        );
        let FeedEvent::Snapshot { symbol, bids, asks, .. } = &events[0] else {
            panic!("{events:?}")
        };
        assert_eq!(symbol, "FED");
        assert_eq!(bids, &[(80_000, 300_000_000), (220_000, 333_000_000)]);
        assert_eq!(asks, &[(460_000, 20_000_000), (440_000, 146_000_000)]);

        let (events, _) = decode(
            &mut s,
            r#"{"type": "orderbook_delta", "sending_ts_ms": 1669149841123, "sid": 2, "seq": 2, "msg": {"market_ticker": "FED", "clock": 4182061, "price_dollars": "0.9600", "delta_fp": "-54.00", "side": "no", "ts": "2022-11-22T20:44:01Z", "ts_ms": 1669149841000}}"#,
        );
        let FeedEvent::Level { side, price, change, exchange_ts, .. } = &events[0] else {
            panic!("{events:?}")
        };
        assert_eq!(*side, Side::Sell);
        assert_eq!(*price, 40_000);
        assert_eq!(*change, LevelChange::Delta(-54_000_000));
        assert_eq!(exchange_ts.unwrap().timestamp_millis(), 1669149841000);
    }

    #[test]
    fn skips_deltas_already_in_snapshot() {
        let mut s = subscribed_session();
        let delta = |seq: u64, clock: i64| {
            format!(r#"{{"type": "orderbook_delta", "sid": 2, "seq": {seq}, "msg": {{"market_ticker": "FED", "clock": {clock}, "price_dollars": "0.5", "delta_fp": "-1", "side": "yes"}}}}"#)
        };
        decode(&mut s, r#"{"type": "orderbook_snapshot", "sid": 2, "seq": 1, "msg": {"market_ticker": "FED", "clock": 100, "yes_dollars_fp": [["0.5", "1"]]}}"#);
        assert!(decode(&mut s, &delta(2, 99)).0.is_empty());
        assert!(decode(&mut s, &delta(3, 100)).0.is_empty());
        assert_eq!(decode(&mut s, &delta(4, 101)).0.len(), 1);
        // After the first newer delta, clocks are no longer compared.
        assert_eq!(decode(&mut s, &delta(5, 7)).0.len(), 1);
    }

    #[test]
    fn seq_gap_resubscribes() {
        let mut s = subscribed_session();
        decode(&mut s, r#"{"type": "orderbook_snapshot", "sid": 2, "seq": 1, "msg": {"market_ticker": "FED"}}"#);
        let (events, out) = decode(
            &mut s,
            r#"{"type": "orderbook_delta", "sid": 2, "seq": 3, "msg": {"market_ticker": "FED", "price_dollars": "0.5", "delta_fp": "1", "side": "yes"}}"#,
        );
        assert!(matches!(&events[0], FeedEvent::Desync { symbols, .. } if symbols.len() == 2));
        assert!(out[0].contains(r#""cmd":"unsubscribe""#));
        assert!(out[1].contains(r#""cmd":"subscribe""#));
        // Late messages on the dead sid are ignored.
        let (events, _) = decode(
            &mut s,
            r#"{"type": "orderbook_delta", "sid": 2, "seq": 4, "msg": {"market_ticker": "FED", "price_dollars": "0.5", "delta_fp": "1", "side": "yes"}}"#,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn resubscribe_waits_for_ack() {
        let mut s = subscribed_session();
        let (mut events, mut out) = (Vec::new(), Vec::new());
        s.resubscribe("FED", &mut events, &mut out);
        assert!(matches!(&events[0], FeedEvent::Desync { .. }));
        assert!(out[0].contains("delete_markets"));
        let (_, out) = decode(&mut s, r#"{"id": 2, "sid": 2, "seq": 1, "type": "ok", "msg": {}}"#);
        assert!(out[0].contains(r#""cmd":"subscribe""#) && out[0].contains("FED"));
    }

    #[test]
    fn signs_with_ed25519() {
        let pem = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEINTuctv5E1hK1bbY8fdp+K06/nwoy/HU++CXqI9EdVhC\n-----END PRIVATE KEY-----\n";
        let k = Kalshi::new(true, Some(Credentials::new("id".into(), pem).unwrap()));
        let req = k.connect_request().unwrap();
        assert!(req.headers().contains_key("KALSHI-ACCESS-SIGNATURE"));
    }
}
