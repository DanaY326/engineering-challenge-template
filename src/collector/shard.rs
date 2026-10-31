//! A shard: a set of symbols served by one websocket.
//!
//! The shard keeps a book per symbol, writes every level update, and writes a
//! snapshot of every synced book at each wall-clock minute.
//!
//! It also monitors subscriptions (Extension 2):
//! - a symbol that never syncs is resubscribed, with backoff;
//! - a symbol that is quiet compared to its own history is resubscribed, and
//!   the fresh snapshot is checked against our book;
//! - the socket is recycled periodically.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rand::Rng;
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

struct SymbolState {
    book: Option<SyncedBook>,
    last_event: Option<Instant>,
    /// When we last (re)subscribed; the sync deadline is measured from here.
    requested_at: Instant,
    /// No monitoring-triggered resubscribe before this.
    retry_at: Instant,
    retry_backoff: Duration,
    /// After a quiet-check resubscribe: the book to compare the fresh
    /// snapshot against.
    verify: Option<Book>,
    /// EWMA of the time between updates, in seconds.
    ewma_gap: Option<f64>,
    last_update: Option<Instant>,
    quiet_check_at: Instant,
    quiet_backoff: Duration,
    /// The latest timestamp written for this symbol; output timestamps are
    /// kept monotonic per symbol.
    last_emitted: Option<DateTime<Utc>>,
}

impl SymbolState {
    fn new(cfg: &Config, now: Instant) -> Self {
        Self {
            book: None,
            last_event: None,
            requested_at: now,
            retry_at: now + cfg.sync_timeout,
            retry_backoff: cfg.sync_timeout,
            verify: None,
            ewma_gap: None,
            last_update: None,
            quiet_check_at: now + cfg.min_quiet,
            quiet_backoff: cfg.min_quiet,
            last_emitted: None,
        }
    }
}

/// Most monitoring-triggered resubscribes per health check (1 s).
const MAX_RESUBSCRIBES_PER_TICK: usize = 20;

#[derive(Default)]
struct Counters {
    updates: u64,
    snapshots: u64,
    resubscribes: u64,
    recovered: u64,
}

pub struct Shard {
    id: usize,
    cfg: Arc<Config>,
    out: UnboundedSender<OutMsg>,
    tx: UnboundedSender<ConnCommand>,
    connected_since: Option<Instant>,
    recycle_after: Duration,
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
        cfg: cfg.clone(),
        shard: id,
        symbols: symbols.iter().cloned().collect::<BTreeSet<_>>(),
        commands,
        events: event_tx,
        cancel: cancel.clone(),
    };
    let connection = tokio::spawn(connection.run());

    let shard = Shard::new(id, cfg, out, tx, symbols);
    let handle = tokio::spawn(async move {
        shard.run(cmd_rx, event_rx, cancel).await;
        // Wait for the websocket to close before the output is closed.
        let _ = connection.await;
    });
    (cmd_tx, handle)
}

/// Spread recycles out so sockets don't reconnect in lockstep.
fn jittered(base: Duration) -> Duration {
    base.mul_f64(rand::thread_rng().gen_range(0.75..1.25))
}

fn next_minute() -> tokio::time::Instant {
    let ms = 60_000 - Utc::now().timestamp_millis().rem_euclid(60_000);
    tokio::time::Instant::now() + Duration::from_millis(ms as u64)
}

impl Shard {
    fn new(
        id: usize,
        cfg: Arc<Config>,
        out: UnboundedSender<OutMsg>,
        tx: UnboundedSender<ConnCommand>,
        symbols: Vec<String>,
    ) -> Self {
        let now = Instant::now();
        Shard {
            id,
            symbols: symbols
                .into_iter()
                .map(|s| (s, SymbolState::new(&cfg, now)))
                .collect(),
            tx,
            connected_since: None,
            recycle_after: jittered(cfg.recycle_interval),
            cfg,
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
        let mut health = tokio::time::interval(Duration::from_secs(1));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                _ = health.tick() => {
                    self.catch_up(&events);
                    self.health_check();
                }
                _ = &mut minute => {
                    self.catch_up(&events);
                    self.minute_tick();
                    minute.as_mut().reset(next_minute());
                }
            }
        }
    }

    fn add(&mut self, symbols: Vec<String>) {
        let now = Instant::now();
        let fresh: Vec<String> = symbols
            .into_iter()
            .filter(|s| !self.symbols.contains_key(s))
            .collect();
        for symbol in &fresh {
            self.symbols
                .insert(symbol.clone(), SymbolState::new(&self.cfg, now));
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

    /// Invalidate the symbol's book and ask the socket to resubscribe it.
    fn resubscribe(&mut self, symbol: &str, verify: bool, reason: &str) {
        let now = Instant::now();
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let old = state.book.take();
        state.verify = old.filter(|_| verify).map(|sb| sb.book);
        state.requested_at = now;
        state.retry_at = now + state.retry_backoff;
        state.retry_backoff = (state.retry_backoff * 2).min(self.cfg.max_retry_backoff);
        self.counters.resubscribes += 1;
        tracing::debug!(shard = self.id, symbol, "resubscribing: {reason}");
        let _ = self.tx.send(ConnCommand::Resubscribe(symbol.to_owned()));
    }

    fn on_conn_msg(&mut self, msg: ConnMsg) {
        let now = Instant::now();
        match msg {
            ConnMsg::Connected => {
                self.connected_since = Some(now);
                for state in self.symbols.values_mut() {
                    state.requested_at = now;
                    state.retry_at = now + self.cfg.sync_timeout;
                    state.retry_backoff = self.cfg.sync_timeout;
                }
            }
            ConnMsg::Disconnected => {
                // Every book is stale until the reconnect's snapshots arrive.
                self.connected_since = None;
                for state in self.symbols.values_mut() {
                    state.book = None;
                }
            }
            ConnMsg::Event {
                received_at,
                received_mono,
                event,
            } => {
                self.as_of = self.as_of.max(received_at);
                self.on_event(received_at, received_mono, event);
            }
        }
    }

    fn on_event(&mut self, received_at: DateTime<Utc>, mono: Instant, event: FeedEvent) {
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
                state.last_event = Some(mono);
                if let Some(expected) = state.verify.take() {
                    if expected != book {
                        self.counters.recovered += 1;
                        tracing::warn!(
                            shard = self.id,
                            symbol,
                            "quiet-check resubscribe found a different book; updates were missed"
                        );
                    }
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
                state.last_event = Some(mono);
                let Some(sb) = state.book.as_mut() else {
                    return;
                };
                match sb.book.apply(side, price, change) {
                    Ok(quantity) => {
                        let exchange_ts = exchange_ts.unwrap_or(received_at);
                        sb.exchange_ts = exchange_ts;
                        state.retry_backoff = self.cfg.sync_timeout;
                        if let Some(last) = state.last_update {
                            let gap = mono.saturating_duration_since(last).as_secs_f64();
                            state.ewma_gap =
                                Some(state.ewma_gap.map_or(gap, |e| 0.9 * e + 0.1 * gap));
                        }
                        state.last_update = Some(mono);
                        state.quiet_backoff = self.cfg.min_quiet;
                        self.emit_update(&symbol, received_at, exchange_ts, side, price, quantity);
                    }
                    Err(e) => {
                        tracing::warn!(shard = self.id, symbol, "book diverged: {e}");
                        self.resubscribe(&symbol, false, "book diverged");
                    }
                }
            }
            FeedEvent::Desync { symbols, reason } => {
                if symbols.len() > 1 {
                    tracing::info!(shard = self.id, "{} symbols desynced: {reason}", symbols.len());
                }
                let now = Instant::now();
                for symbol in symbols {
                    let Some(state) = self.symbols.get_mut(&symbol) else {
                        continue;
                    };
                    state.book = None;
                    state.requested_at = now;
                    state.retry_at = state.retry_at.max(now + self.cfg.sync_timeout);
                }
            }
        }
    }

    fn health_check(&mut self) {
        if self.connected_since.is_none() {
            return;
        }
        let now = Instant::now();
        let mut actions: Vec<(String, bool, String)> = Vec::new();

        for (symbol, state) in &mut self.symbols {
            if state.book.is_none() && now >= state.retry_at {
                actions.push((symbol.clone(), false, format!(
                    "not synced {:?} after subscribing",
                    now - state.requested_at
                )));
            }

            // Quiet relative to the symbol's own history: resubscribe and
            // verify the fresh snapshot.
            let silence = now - state.last_event.unwrap_or(state.requested_at);
            let threshold = state
                .ewma_gap
                .map(|gap| Duration::from_secs_f64(gap * self.cfg.quiet_factor))
                .unwrap_or_default()
                .max(self.cfg.min_quiet);
            if silence > threshold && now >= state.quiet_check_at {
                if state.book.is_some() {
                    actions.push((symbol.clone(), true, format!("quiet for {silence:?}")));
                }
                state.quiet_check_at = now + state.quiet_backoff;
                state.quiet_backoff = (state.quiet_backoff * 2).min(self.cfg.max_quiet_backoff);
            }
        }
        // Cap resubscribes per tick so a burst (e.g. after a reconnect) can't
        // flood the exchange with commands; the rest are still due and go out
        // on the next ticks.
        let mut budget = MAX_RESUBSCRIBES_PER_TICK;
        let mut deferred = 0;
        for (symbol, verify, reason) in actions {
            if budget == 0 {
                deferred += 1;
                continue;
            }
            budget -= 1;
            self.resubscribe(&symbol, verify, &reason);
        }
        if deferred > 0 {
            tracing::debug!(shard = self.id, deferred, "resubscribes deferred to later ticks");
        }

        self.maybe_recycle(now);
    }

    /// Reconnect the socket periodically, to shed any state that degrades
    /// over a long-lived connection on either side.
    fn maybe_recycle(&mut self, now: Instant) {
        let Some(since) = self.connected_since else {
            return;
        };
        if now - since >= self.recycle_after {
            tracing::info!(shard = self.id, "recycling socket");
            self.recycle_after = jittered(self.cfg.recycle_interval);
            let _ = self.tx.send(ConnCommand::Reconnect);
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
            "shard {}: {} symbols, synced {}, updates {}, snapshots {}, resubscribes {}, missed-data recoveries {}",
            self.id,
            self.symbols.len(),
            synced.len(),
            c.updates,
            c.snapshots,
            c.resubscribes,
            c.recovered,
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
            let cfg = Arc::new(Config::default());
            let (out_tx, out) = mpsc::unbounded_channel();
            let (tx, commands) = mpsc::unbounded_channel();
            let mut shard = Shard::new(0, cfg, out_tx, tx, vec![SYM.to_owned()]);
            shard.on_conn_msg(ConnMsg::Connected);
            Self {
                shard,
                out,
                commands,
            }
        }

        fn event_at(&mut self, received_at: DateTime<Utc>, event: FeedEvent) {
            self.shard.on_conn_msg(ConnMsg::Event {
                received_at,
                received_mono: Instant::now(),
                event,
            });
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
    fn unsynced_symbol_is_retried_with_backoff() {
        let mut h = Harness::new();
        h.commands();
        h.shard.symbols.get_mut(SYM).unwrap().retry_at = Instant::now();
        h.shard.health_check();
        assert_eq!(h.commands().len(), 1);
        // The next retry is not due yet.
        h.shard.health_check();
        assert!(h.commands().is_empty());
        assert!(h.shard.symbols[SYM].retry_backoff > h.shard.cfg.sync_timeout);
    }

    #[test]
    fn resubscribes_are_rate_limited() {
        let mut h = Harness::new();
        let symbols: Vec<String> = (0..50).map(|i| format!("S{i}")).collect();
        h.shard.add(symbols);
        h.commands();
        let now = Instant::now();
        for state in h.shard.symbols.values_mut() {
            state.retry_at = now;
        }
        let resubscribes = |cmds: Vec<ConnCommand>| {
            cmds.iter().filter(|c| matches!(c, ConnCommand::Resubscribe(_))).count()
        };
        h.shard.health_check();
        assert_eq!(resubscribes(h.commands()), MAX_RESUBSCRIBES_PER_TICK);
        // The rest go out on later ticks.
        h.shard.health_check();
        assert_eq!(resubscribes(h.commands()), MAX_RESUBSCRIBES_PER_TICK);
    }

    #[test]
    fn quiet_symbol_is_resubscribed_and_its_snapshot_verified() {
        let mut h = Harness::new();
        h.snapshot(&[(400_000, 10)], &[]);
        h.commands();
        let state = h.shard.symbols.get_mut(SYM).unwrap();
        let long_ago = Instant::now() - h.shard.cfg.min_quiet * 2;
        state.last_event = Some(long_ago);
        state.quiet_check_at = long_ago;
        state.retry_at = Instant::now() + Duration::from_secs(60);
        h.shard.health_check();
        assert!(matches!(
            h.commands().as_slice(),
            [ConnCommand::Resubscribe(s)] if s == SYM
        ));
        // The fresh snapshot differs from what we held: updates were missed.
        h.snapshot(&[(400_000, 12)], &[]);
        assert_eq!(h.shard.counters.recovered, 1);
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
