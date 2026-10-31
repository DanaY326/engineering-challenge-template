//! Polymarket CLOB `market` websocket channel.
//!
//! The symbol is the CLOB token (asset) id; each outcome token has its own
//! book. `book` messages are full snapshots (sent on subscribe and after
//! trades) and `price_change` carries the new absolute size of a level.
//! There is no sequence number, so gaps can only be detected by the shard's
//! cross-connection and activity monitoring.

use std::collections::HashSet;

use anyhow::Context;
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::{Exchange, FeedEvent, Session, get_json, hash_key, millis_to_utc};
use crate::Side;
use crate::collector::book::LevelChange;
use crate::collector::fixed::parse_fixed;

pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

/// Asset ids per subscribe frame.
const SUBSCRIBE_CHUNK: usize = 200;

pub struct Polymarket {
    pub ws_url: String,
    pub gamma_url: String,
}

impl Default for Polymarket {
    fn default() -> Self {
        Self {
            ws_url: WS_URL.to_owned(),
            gamma_url: GAMMA_URL.to_owned(),
        }
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<MarketInfo>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketInfo {
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    enable_order_book: Option<bool>,
    #[serde(default)]
    accepting_orders: Option<bool>,
    /// A JSON-encoded array of token ids, inside a string.
    #[serde(default)]
    clob_token_ids: Option<String>,
}

impl Exchange for Polymarket {
    type Session = PolymarketSession;

    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn connect_request(&self) -> anyhow::Result<Request> {
        Ok(self.ws_url.as_str().into_client_request()?)
    }

    fn new_session(&self) -> PolymarketSession {
        PolymarketSession::default()
    }

    async fn list_symbols(
        &self,
        http: &reqwest::Client,
        limit: Option<usize>,
        pages: UnboundedSender<Vec<String>>,
    ) -> anyhow::Result<Vec<String>> {
        let mut tokens = Vec::new();
        let mut cursor = String::new();
        loop {
            let mut request = http
                .get(format!("{}/markets/keyset", self.gamma_url))
                .query(&[("closed", "false"), ("limit", "100")]);
            if !cursor.is_empty() {
                request = request.query(&[("after_cursor", &cursor)]);
            }
            let page: MarketsPage = get_json(request)
                .await
                .context("fetching Polymarket markets page")?;
            let done = page.markets.is_empty();
            let mut page_tokens = Vec::new();
            for market in page.markets {
                let tradable = market.active == Some(true)
                    && market.closed != Some(true)
                    && market.enable_order_book == Some(true)
                    && market.accepting_orders == Some(true);
                if let (true, Some(ids)) = (tradable, market.clob_token_ids) {
                    let ids: Vec<String> =
                        serde_json::from_str(&ids).context("decoding clobTokenIds")?;
                    page_tokens.extend(ids);
                }
            }
            let _ = pages.send(page_tokens.clone());
            tokens.extend(page_tokens);
            let enough = limit.is_some_and(|limit| tokens.len() >= limit);
            match page.next_cursor {
                Some(next) if !next.is_empty() && next != "LTE=" && !done && !enough => {
                    cursor = next
                }
                _ => break,
            }
        }
        Ok(tokens)
    }

    fn app_heartbeat(&self) -> Option<&'static str> {
        Some("PING")
    }
}

#[derive(Default)]
pub struct PolymarketSession {
    subscribed: HashSet<String>,
    /// The first frame on a connection must be the `"type": "market"` form.
    initialized: bool,
}

#[derive(Deserialize)]
struct Level {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    #[serde(default)]
    hash: Option<String>,
}

#[derive(Deserialize)]
struct Message {
    event_type: String,
    #[serde(default)]
    asset_id: Option<String>,
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
    #[serde(default)]
    price_changes: Vec<PriceChange>,
    #[serde(default)]
    timestamp: Option<String>,
}

fn levels(raw: &[Level]) -> anyhow::Result<Vec<(i64, i64)>> {
    raw.iter()
        .map(|l| Ok((parse_fixed(&l.price)?, parse_fixed(&l.size)?)))
        .collect()
}

impl PolymarketSession {
    fn decode_one(&self, msg: Message, events: &mut Vec<FeedEvent>) -> anyhow::Result<()> {
        let exchange_ts = msg
            .timestamp
            .as_deref()
            .and_then(|t| t.parse().ok())
            .and_then(millis_to_utc);
        match msg.event_type.as_str() {
            "book" => {
                let Some(symbol) = msg.asset_id.filter(|a| self.subscribed.contains(a)) else {
                    return Ok(());
                };
                events.push(FeedEvent::Snapshot {
                    symbol,
                    exchange_ts,
                    bids: levels(&msg.bids)?,
                    asks: levels(&msg.asks)?,
                });
            }
            "price_change" => {
                for change in msg.price_changes {
                    if !self.subscribed.contains(&change.asset_id) {
                        continue;
                    }
                    let side = match change.side.as_str() {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        other => anyhow::bail!("unknown Polymarket side {other:?}"),
                    };
                    let price = parse_fixed(&change.price)?;
                    let size = parse_fixed(&change.size)?;
                    events.push(FeedEvent::Level {
                        match_key: Some(hash_key((
                            &change.asset_id,
                            &change.hash,
                            side as u8,
                            price,
                            size,
                        ))),
                        symbol: change.asset_id,
                        exchange_ts,
                        side,
                        price,
                        change: LevelChange::Absolute(size),
                    });
                }
            }
            other => tracing::trace!("ignoring Polymarket message type {other}"),
        }
        Ok(())
    }
}

impl Session for PolymarketSession {
    fn subscribe(&mut self, symbols: &[String], out: &mut Vec<String>) {
        let fresh: Vec<&String> = symbols
            .iter()
            .filter(|s| self.subscribed.insert((*s).clone()))
            .collect();
        for chunk in fresh.chunks(SUBSCRIBE_CHUNK) {
            let frame = if self.initialized {
                json!({"assets_ids": chunk, "operation": "subscribe"})
            } else {
                self.initialized = true;
                json!({"assets_ids": chunk, "type": "market", "initial_dump": true})
            };
            out.push(frame.to_string());
        }
    }

    fn unsubscribe(&mut self, symbols: &[String], out: &mut Vec<String>) {
        let gone: Vec<&String> = symbols
            .iter()
            .filter(|s| self.subscribed.remove(*s))
            .collect();
        if self.initialized {
            for chunk in gone.chunks(SUBSCRIBE_CHUNK) {
                out.push(json!({"assets_ids": chunk, "operation": "unsubscribe"}).to_string());
            }
        }
    }

    fn resubscribe(&mut self, symbol: &str, events: &mut Vec<FeedEvent>, out: &mut Vec<String>) {
        events.push(FeedEvent::Desync {
            symbols: vec![symbol.to_owned()],
            reason: "resubscribing".to_owned(),
        });
        let symbol = [symbol.to_owned()];
        self.unsubscribe(&symbol, out);
        self.subscribe(&symbol, out);
    }

    fn decode(
        &mut self,
        text: &str,
        events: &mut Vec<FeedEvent>,
        _out: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        let text = text.trim();
        if text == "PONG" || text.is_empty() {
            return Ok(());
        }
        // The initial dump may arrive as an array of messages.
        if text.starts_with('[') {
            let batch: Vec<&RawValue> = serde_json::from_str(text)?;
            for raw in batch {
                self.decode_one(serde_json::from_str(raw.get())?, events)?;
            }
        } else {
            self.decode_one(serde_json::from_str(text)?, events)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_book_and_price_change() {
        let mut s = PolymarketSession::default();
        let mut out = Vec::new();
        s.subscribe(&["A".to_owned()], &mut out);
        assert!(out[0].contains(r#""type":"market""#));
        s.subscribe(&["B".to_owned()], &mut out);
        assert!(out[1].contains(r#""operation":"subscribe""#));

        let mut events = Vec::new();
        s.decode(
            r#"[{"event_type": "book", "asset_id": "A", "market": "0x", "bids": [{"price": "0.48", "size": "30"}], "asks": [{"price": "0.52", "size": "25.5"}], "timestamp": "1757908892351", "hash": "0x"}]"#,
            &mut events,
            &mut out,
        )
        .unwrap();
        assert_eq!(
            events[0],
            FeedEvent::Snapshot {
                symbol: "A".into(),
                exchange_ts: millis_to_utc(1757908892351),
                bids: vec![(480_000, 30_000_000)],
                asks: vec![(520_000, 25_500_000)],
            }
        );

        events.clear();
        s.decode(
            r#"{"event_type": "price_change", "market": "0x", "price_changes": [{"asset_id": "A", "price": "0.5", "size": "0", "side": "SELL", "hash": "h"}, {"asset_id": "Z", "price": "0.5", "size": "1", "side": "BUY", "hash": "h"}], "timestamp": "1757908892351"}"#,
            &mut events,
            &mut out,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            FeedEvent::Level { side: Side::Sell, price: 500_000, change: LevelChange::Absolute(0), .. }
        ));
    }
}
