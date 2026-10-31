//! A shard: a set of symbols served by one websocket.
//!
//! The shard keeps a book per symbol, writes every level update, and writes a
//! snapshot of every synced book at each wall-clock minute.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Config;
use super::book::Book;
use super::connection::{ConnCommand, ConnMsg, Connection};
use super::exchange::FeedEvent;
use super::exchange::kalshi::Kalshi;
use super::output::OutMsg;
use crate::Side;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;

pub enum ShardCommand {
    Add(Vec<String>),
    Remove(Vec<String>),
}

struct SyncedBook {
    book: Book,
    exchange_ts: DateTime<Utc>,
}

#[derive(Default)]
struct SymbolState {
    book: Option<SyncedBook>,
    /// The latest timestamp written for this symbol; output timestamps are
    /// kept monotonic per symbol.
    last_emitted: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct Counters {
    updates: u64,
    snapshots: u64,
    resyncs: u64,
}

pub struct Shard {
    id: usize,
    out: UnboundedSender<OutMsg>,
    tx: UnboundedSender<ConnCommand>,
    symbols: HashMap<String, SymbolState>,
    counters: Counters,
    /// Everything received up to this time has been applied: the latest
    /// receive time processed, or "now" when no events are queued. Minute
    /// snapshots are stamped with it, since stamping them with the wall clock
    /// would put them after queued events that were received earlier.
    as_of: DateTime<Utc>,
}

/// Spawn a shard and its websocket connection.
pub fn spawn(
    id: usize,
    exchange: Arc<Kalshi>,
    symbols: Vec<String>,
    cfg: Arc<Config>,
    out: UnboundedSender<OutMsg>,
    cancel: CancellationToken,
) -> (UnboundedSender<ShardCommand>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (tx, commands) = mpsc::unbounded_channel();
    let connection = Connection {
        exchange,
        cfg,
        shard: id,
        symbols: symbols.iter().cloned().collect::<BTreeSet<_>>(),
        commands,
        events: event_tx,
        cancel: cancel.clone(),
    };
    let connection = tokio::spawn(connection.run());

    let shard = Shard::new(id, out, tx, symbols);
    let handle = tokio::spawn(async move {
        shard.run(cmd_rx, event_rx, cancel).await;
        // Wait for the websocket to close before the output is closed.
        let _ = connection.await;
    });
    (cmd_tx, handle)
}

fn next_minute() -> tokio::time::Instant {
    let ms = 60_000 - Utc::now().timestamp_millis().rem_euclid(60_000);
    tokio::time::Instant::now() + Duration::from_millis(ms as u64)
}

impl Shard {
    fn new(
        id: usize,
        out: UnboundedSender<OutMsg>,
        tx: UnboundedSender<ConnCommand>,
        symbols: Vec<String>,
    ) -> Self {
        Shard {
            id,
            symbols: symbols
                .into_iter()
                .map(|s| (s, SymbolState::default()))
                .collect(),
            tx,
            out,
            counters: Counters::default(),
            as_of: Utc::now(),
        }
    }

    async fn run(
        mut self,
        mut commands: UnboundedReceiver<ShardCommand>,
        mut events: UnboundedReceiver<ConnMsg>,
        cancel: CancellationToken,
    ) {
        let minute = tokio::time::sleep_until(next_minute());
        tokio::pin!(minute);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(msg) = events.recv() => self.on_conn_msg(msg),
                command = commands.recv() => match command {
                    Some(ShardCommand::Add(symbols)) => self.add(symbols),
                    Some(ShardCommand::Remove(symbols)) => self.remove(symbols),
                    None => break,
                },
                _ = &mut minute => {
                    self.catch_up(&events);
                    self.minute_tick();
                    minute.as_mut().reset(next_minute());
                }
            }
        }
    }

    fn add(&mut self, symbols: Vec<String>) {
        let fresh: Vec<String> = symbols
            .into_iter()
            .filter(|s| !self.symbols.contains_key(s))
            .collect();
        for symbol in &fresh {
            self.symbols.insert(symbol.clone(), SymbolState::default());
        }
        let _ = self.tx.send(ConnCommand::Subscribe(fresh));
    }

    fn remove(&mut self, symbols: Vec<String>) {
        for symbol in &symbols {
            self.symbols.remove(symbol);
        }
        let _ = self.tx.send(ConnCommand::Unsubscribe(symbols));
    }

    /// With no events queued, everything received so far has been applied.
    fn catch_up(&mut self, events: &UnboundedReceiver<ConnMsg>) {
        if events.is_empty() {
            self.as_of = self.as_of.max(Utc::now());
        }
    }

    /// Clamp `ts` so this symbol's output timestamps never go backwards.
    fn monotonic(&mut self, symbol: &str, ts: DateTime<Utc>) -> DateTime<Utc> {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return ts;
        };
        let ts = state.last_emitted.map_or(ts, |last| last.max(ts));
        state.last_emitted = Some(ts);
        ts
    }

    fn emit_snapshot(&mut self, symbol: &str, received_at: DateTime<Utc>) {
        if self.symbols[symbol].book.is_none() {
            return;
        }
        let received_at = self.monotonic(symbol, received_at);
        let sb = self.symbols[symbol].book.as_ref().unwrap();
        let snapshot = sb.book.to_snapshot(symbol, received_at, sb.exchange_ts);
        self.counters.snapshots += 1;
        let _ = self.out.send(OutMsg::Snapshot(snapshot));
    }

    fn emit_update(
        &mut self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
    ) {
        self.counters.updates += 1;
        let received_at = self.monotonic(symbol, received_at);
        let _ = self.out.send(OutMsg::Update(PriceLevelUpdateDomain {
            backtest_timestamp: received_at,
            exchange_timestamp: exchange_ts,
            price_level: price,
            quantity,
            side,
            symbol: symbol.to_owned(),
        }));
    }

    /// Invalidate the symbol's book and ask the socket to resubscribe it, so
    /// the next snapshot resyncs us.
    fn resubscribe(&mut self, symbol: &str, reason: &str) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        state.book = None;
        self.counters.resyncs += 1;
        tracing::debug!(shard = self.id, symbol, "resubscribing: {reason}");
        let _ = self.tx.send(ConnCommand::Resubscribe(symbol.to_owned()));
    }

    fn on_conn_msg(&mut self, msg: ConnMsg) {
        match msg {
            ConnMsg::Connected => {}
            ConnMsg::Disconnected => {
                // Every book is stale until the reconnect's snapshots arrive.
                for state in self.symbols.values_mut() {
                    state.book = None;
                }
            }
            ConnMsg::Event { received_at, event } => {
                self.as_of = self.as_of.max(received_at);
                self.on_event(received_at, event);
            }
        }
    }

    fn on_event(&mut self, received_at: DateTime<Utc>, event: FeedEvent) {
        match event {
            FeedEvent::Snapshot {
                symbol,
                exchange_ts,
                bids,
                asks,
            } => {
                let Some(state) = self.symbols.get_mut(&symbol) else {
                    return;
                };
                let book = Book::from_levels(&bids, &asks);
                if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask())
                    && bid >= ask
                {
                    tracing::debug!(shard = self.id, symbol, bid, ask, "crossed book in snapshot");
                }
                state.book = Some(SyncedBook {
                    book,
                    exchange_ts: exchange_ts.unwrap_or(received_at),
                });
                self.emit_snapshot(&symbol, received_at);
            }
            FeedEvent::Level {
                symbol,
                exchange_ts,
                side,
                price,
                change,
            } => {
                let Some(state) = self.symbols.get_mut(&symbol) else {
                    return;
                };
                let Some(sb) = state.book.as_mut() else {
                    return;
                };
                match sb.book.apply(side, price, change) {
                    Ok(quantity) => {
                        let exchange_ts = exchange_ts.unwrap_or(received_at);
                        sb.exchange_ts = exchange_ts;
                        self.emit_update(&symbol, received_at, exchange_ts, side, price, quantity);
                    }
                    Err(e) => {
                        tracing::warn!(shard = self.id, symbol, "book diverged: {e}");
                        self.resubscribe(&symbol, "book diverged");
                    }
                }
            }
            FeedEvent::Desync { symbols, reason } => {
                if symbols.len() > 1 {
                    tracing::info!(shard = self.id, "{} symbols desynced: {reason}", symbols.len());
                }
                for symbol in symbols {
                    if let Some(state) = self.symbols.get_mut(&symbol) {
                        state.book = None;
                    }
                }
            }
        }
    }

    fn minute_tick(&mut self) {
        let received_at = self.as_of;
        let synced: Vec<String> = self
            .symbols
            .iter()
            .filter(|(_, state)| state.book.is_some())
            .map(|(symbol, _)| symbol.clone())
            .collect();
        for symbol in &synced {
            self.emit_snapshot(symbol, received_at);
        }

        let c = &self.counters;
        tracing::info!(
            "shard {}: {} symbols, synced {}, updates {}, snapshots {}, resyncs {}",
            self.id,
            self.symbols.len(),
            synced.len(),
            c.updates,
            c.snapshots,
            c.resyncs,
        );
        self.counters = Counters::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::book::LevelChange;

    const SYM: &str = "A";

    struct Harness {
        shard: Shard,
        out: UnboundedReceiver<OutMsg>,
        commands: UnboundedReceiver<ConnCommand>,
    }

    impl Harness {
        fn new() -> Self {
            let (out_tx, out) = mpsc::unbounded_channel();
            let (tx, commands) = mpsc::unbounded_channel();
            let mut shard = Shard::new(0, out_tx, tx, vec![SYM.to_owned()]);
            shard.on_conn_msg(ConnMsg::Connected);
            Self {
                shard,
                out,
                commands,
            }
        }

        fn event_at(&mut self, received_at: DateTime<Utc>, event: FeedEvent) {
            self.shard.on_conn_msg(ConnMsg::Event { received_at, event });
        }

        fn event(&mut self, event: FeedEvent) {
            self.event_at(Utc::now(), event);
        }

        fn snapshot(&mut self, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
            self.event(FeedEvent::Snapshot {
                symbol: SYM.to_owned(),
                exchange_ts: None,
                bids: bids.to_vec(),
                asks: asks.to_vec(),
            });
        }

        fn delta(&mut self, side: Side, price: i64, delta: i64) {
            self.event(level(side, price, delta));
        }

        fn output(&mut self) -> Vec<OutMsg> {
            std::iter::from_fn(|| self.out.try_recv().ok()).collect()
        }

        fn commands(&mut self) -> Vec<ConnCommand> {
            std::iter::from_fn(|| self.commands.try_recv().ok()).collect()
        }
    }

    fn level(side: Side, price: i64, delta: i64) -> FeedEvent {
        FeedEvent::Level {
            symbol: SYM.to_owned(),
            exchange_ts: None,
            side,
            price,
            change: LevelChange::Delta(delta),
        }
    }

    fn snapshot_bids(msg: &OutMsg) -> Vec<(i64, i64)> {
        match msg {
            OutMsg::Snapshot(s) => s.bids.iter().map(|l| (l.price_level, l.quantity)).collect(),
            OutMsg::Update(u) => panic!("expected snapshot, got update {u:?}"),
        }
    }

    fn update(msg: &OutMsg) -> (Side, i64, i64) {
        match msg {
            OutMsg::Update(u) => (u.side, u.price_level, u.quantity),
            OutMsg::Snapshot(s) => panic!("expected update, got snapshot {s:?}"),
        }
    }

    #[test]
    fn snapshot_then_updates_are_written_as_absolute_quantities() {
        let mut h = Harness::new();
        h.snapshot(&[(400_000, 10)], &[]);
        let out = h.output();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(400_000, 10)]);

        h.delta(Side::Buy, 400_000, 5);
        assert_eq!(update(&h.output()[0]), (Side::Buy, 400_000, 15));
    }

    #[test]
    fn desync_goes_dark_until_next_snapshot() {
        let mut h = Harness::new();
        h.snapshot(&[(400_000, 10)], &[]);
        h.output();
        h.event(FeedEvent::Desync {
            symbols: vec![SYM.to_owned()],
            reason: "seq gap".to_owned(),
        });
        // Updates on a desynced book are dropped, not written.
        h.delta(Side::Buy, 400_000, 1);
        assert!(h.output().is_empty());

        h.snapshot(&[(400_000, 7)], &[]);
        assert_eq!(snapshot_bids(&h.output()[0]), vec![(400_000, 7)]);
    }

    #[test]
    fn diverged_book_is_resubscribed() {
        let mut h = Harness::new();
        h.snapshot(&[(400_000, 10)], &[]);
        h.output();
        // A delta that would take a level negative proves divergence.
        h.delta(Side::Buy, 400_000, -11);
        assert!(matches!(
            h.commands().as_slice(),
            [ConnCommand::Resubscribe(s)] if s == SYM
        ));
        assert!(h.output().is_empty());
        h.delta(Side::Buy, 400_000, 1);
        assert!(h.output().is_empty());
    }

    #[test]
    fn output_timestamps_never_go_backwards() {
        let mut h = Harness::new();
        let t0 = Utc::now();
        let at = |ms| t0 + chrono::Duration::milliseconds(ms);
        let snapshot = FeedEvent::Snapshot {
            symbol: SYM.to_owned(),
            exchange_ts: None,
            bids: vec![(400_000, 10)],
            asks: vec![],
        };
        h.event_at(at(1), snapshot);
        h.event_at(at(10), level(Side::Buy, 400_000, 1));
        // The minute snapshot is stamped with the latest receive time
        // processed, not the wall clock.
        h.shard.minute_tick();
        h.event_at(at(12), level(Side::Buy, 400_000, 1));
        let stamps: Vec<DateTime<Utc>> = h
            .output()
            .iter()
            .map(|m| match m {
                OutMsg::Snapshot(s) => s.received_at,
                OutMsg::Update(u) => u.backtest_timestamp,
            })
            .collect();
        assert_eq!(stamps, vec![at(1), at(10), at(10), at(12)]);
    }

    #[test]
    fn minute_tick_snapshots_every_synced_symbol() {
        let mut h = Harness::new();
        h.shard.add(vec!["B".to_owned()]);
        h.snapshot(&[(400_000, 10)], &[]);
        h.output();
        h.shard.minute_tick();
        // "A" is synced, "B" never synced.
        let out = h.output();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(400_000, 10)]);
    }
}
