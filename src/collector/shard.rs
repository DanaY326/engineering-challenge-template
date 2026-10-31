//! A shard: a set of symbols served by `cfg.replicas` redundant websockets.
//!
//! Every replica keeps its own book per symbol. Exactly one synced replica per
//! symbol is the *primary*, and only its updates are written out. Whenever the
//! primary changes we write a snapshot first, since the replicas' books may
//! differ (Extension 1).
//!
//! The shard also monitors subscriptions (Extension 2):
//! - a replica that never syncs a symbol is resubscribed, with backoff;
//! - a replica that lags another replica on a symbol is resubscribed;
//! - a symbol that is quiet compared to its own history is resubscribed on
//!   one replica, and the fresh snapshot is checked against our book;
//! - whole sockets are recycled periodically, one at a time, while the other
//!   replicas are healthy.

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
use super::connection::{ConnCommand, Connection, ReplicaMsg, ReplicaMsgKind};
use super::exchange::{Exchange, FeedEvent};
use super::output::OutMsg;
use crate::Side;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;

pub enum ShardCommand {
    Add(Vec<String>),
    Remove(Vec<String>),
}

struct ReplicaBook {
    book: Book,
    exchange_ts: DateTime<Utc>,
}

/// One replica's view of one symbol.
struct ReplicaSymbol {
    book: Option<ReplicaBook>,
    last_event: Option<Instant>,
    /// Last level update (not snapshot): proof the subscription is live.
    last_level: Option<Instant>,
    /// When the current book was synced from a snapshot.
    synced_at: Option<Instant>,

    /// When we last (re)subscribed; the sync deadline is measured from here.
    requested_at: Instant,
    /// No monitoring-triggered resubscribe before this.
    retry_at: Instant,
    retry_backoff: Duration,
    /// After a quiet-check resubscribe: the book to compare the fresh
    /// snapshot against.
    verify: Option<Book>,
}

struct SymbolState {
    replicas: Vec<ReplicaSymbol>,
    primary: Option<usize>,
    /// EWMA of the time between primary updates, in seconds.
    ewma_gap: Option<f64>,
    last_primary_update: Option<Instant>,
    quiet_check_at: Instant,
    quiet_backoff: Duration,
    next_quiet_replica: usize,
    /// The latest timestamp written for this symbol; output timestamps are
    /// kept monotonic per symbol.
    last_emitted: Option<DateTime<Utc>>,
    /// Updates (by match key) not yet received equally by every replica:
    /// `(key, first received, times each replica received it)`. Counts, not
    /// a bitmask, because the same key can legitimately occur twice. Healthy
    /// replicas catch up within milliseconds; an entry where a synced replica
    /// is still short after `stale_lag` means it is missing data.
    unmatched: Vec<(u64, Instant, Vec<u16>)>,
}

impl SymbolState {
    /// Replica `r` received the update identified by `key`.
    fn record_update(&mut self, key: u64, r: usize, at: Instant) {
        if self.replicas.len() < 2 {
            return;
        }
        let i = match self.unmatched.iter().position(|(k, _, _)| *k == key) {
            Some(i) => i,
            None => {
                self.unmatched.push((key, at, vec![0; self.replicas.len()]));
                self.unmatched.len() - 1
            }
        };
        let counts = &mut self.unmatched[i].2;
        counts[r] = counts[r].saturating_add(1);
        if counts.iter().all(|&c| c == counts[0]) {
            self.unmatched.swap_remove(i);
        }
    }

    /// How long ago another replica received an update that synced replica
    /// `r` still hasn't, if that is longer than `stale_lag`. Updates from
    /// before `r`'s latest snapshot are covered by that snapshot.
    fn missed_update(&self, r: usize, stale_lag: Duration, now: Instant) -> Option<Duration> {
        let synced_at = self.replicas[r].synced_at?;
        self.unmatched
            .iter()
            .filter(|(_, at, counts)| {
                *at > synced_at && counts[r] < counts.iter().copied().max().unwrap_or(0)
            })
            .map(|(_, at, _)| now - *at)
            .filter(|&age| age > stale_lag)
            .max()
    }

    /// Forget updates old enough to have been judged already.
    fn expire_unmatched(&mut self, stale_lag: Duration, now: Instant) {
        self.unmatched.retain(|(_, at, _)| now - *at <= stale_lag * 2);
    }

    fn new(cfg: &Config, now: Instant) -> Self {
        Self {
            replicas: (0..cfg.replicas)
                .map(|_| ReplicaSymbol {
                    book: None,
                    last_event: None,
                    requested_at: now,
                    retry_at: now + cfg.sync_timeout,
                    retry_backoff: cfg.sync_timeout,
                    verify: None,
                    last_level: None,
                    synced_at: None,
                })
                .collect(),
            primary: None,
            ewma_gap: None,
            last_primary_update: None,
            quiet_check_at: now + cfg.min_quiet,
            quiet_backoff: cfg.min_quiet,
            next_quiet_replica: 0,
            last_emitted: None,
            unmatched: Vec::new(),
        }
    }
}

struct Replica {
    tx: UnboundedSender<ConnCommand>,
    connected_since: Option<Instant>,
    recycle_after: Duration,
}

/// Most monitoring-triggered resubscribes per socket per health check (1 s).
const MAX_RESUBSCRIBES_PER_TICK: usize = 20;

#[derive(Default)]
struct Counters {
    updates: u64,
    snapshots: u64,
    switches: u64,
    resubscribes: u64,
    recovered: u64,
}

pub struct Shard {
    id: usize,
    cfg: Arc<Config>,
    out: UnboundedSender<OutMsg>,
    replicas: Vec<Replica>,
    symbols: HashMap<String, SymbolState>,
    counters: Counters,
    last_recycle: Instant,
    /// Everything received up to this time has been applied: the latest
    /// receive time processed, or "now" when no events are queued. Snapshots
    /// we generate (failover, minute) are stamped with it, since stamping
    /// them with the wall clock would put them after queued events that were
    /// received earlier.
    as_of: DateTime<Utc>,
}

/// Spawn a shard and its replica connections.
pub fn spawn<E: Exchange>(
    id: usize,
    exchange: Arc<E>,
    symbols: Vec<String>,
    cfg: Arc<Config>,
    out: UnboundedSender<OutMsg>,
    cancel: CancellationToken,
) -> (UnboundedSender<ShardCommand>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let mut replica_txs = Vec::new();
    let mut connections = Vec::new();
    for replica in 0..cfg.replicas {
        let (tx, commands) = mpsc::unbounded_channel();
        let connection = Connection {
            exchange: exchange.clone(),
            cfg: cfg.clone(),
            shard: id,
            replica,
            symbols: symbols.iter().cloned().collect::<BTreeSet<_>>(),
            commands,
            events: event_tx.clone(),
            cancel: cancel.clone(),
        };
        connections.push(tokio::spawn(connection.run()));
        replica_txs.push(tx);
    }
    drop(event_tx);

    let shard = Shard::new(id, cfg, out, replica_txs, symbols);
    let handle = tokio::spawn(async move {
        shard.run(cmd_rx, event_rx, cancel).await;
        // Wait for every websocket to close before the output is closed.
        for connection in connections {
            let _ = connection.await;
        }
    });
    (cmd_tx, handle)
}

/// Spread recycles out so replicas never line up.
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
        replica_txs: Vec<UnboundedSender<ConnCommand>>,
        symbols: Vec<String>,
    ) -> Self {
        let now = Instant::now();
        Shard {
            id,
            symbols: symbols
                .into_iter()
                .map(|s| (s, SymbolState::new(&cfg, now)))
                .collect(),
            replicas: replica_txs
                .into_iter()
                .map(|tx| Replica {
                    tx,
                    connected_since: None,
                    recycle_after: jittered(cfg.recycle_interval),
                })
                .collect(),
            cfg,
            out,
            counters: Counters::default(),
            last_recycle: now,
            as_of: Utc::now(),
        }
    }

    async fn run(
        mut self,
        mut commands: UnboundedReceiver<ShardCommand>,
        mut events: UnboundedReceiver<ReplicaMsg>,
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
                Some(msg) = events.recv() => self.on_replica_msg(msg),
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
        for replica in &self.replicas {
            let _ = replica.tx.send(ConnCommand::Subscribe(fresh.clone()));
        }
    }

    fn remove(&mut self, symbols: Vec<String>) {
        for symbol in &symbols {
            self.symbols.remove(symbol);
        }
        for replica in &self.replicas {
            let _ = replica.tx.send(ConnCommand::Unsubscribe(symbols.clone()));
        }
    }

    /// With no events queued, everything received so far has been applied.
    fn catch_up(&mut self, events: &UnboundedReceiver<ReplicaMsg>) {
        if events.is_empty() {
            self.as_of = self.as_of.max(Utc::now());
        }
    }

    /// Clamp `ts` so this symbol's output timestamps never go backwards. Only
    /// moves a timestamp when events from different sockets race through the
    /// shard's queue, by at most that race.
    fn monotonic(&mut self, symbol: &str, ts: DateTime<Utc>) -> DateTime<Utc> {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return ts;
        };
        let ts = state.last_emitted.map_or(ts, |last| last.max(ts));
        state.last_emitted = Some(ts);
        ts
    }

    fn emit_snapshot(&mut self, symbol: &str, replica: usize, received_at: DateTime<Utc>) {
        if self.symbols[symbol].replicas[replica].book.is_none() {
            return;
        }
        let received_at = self.monotonic(symbol, received_at);
        let rb = self.symbols[symbol].replicas[replica].book.as_ref().unwrap();
        let snapshot = rb.book.to_snapshot(symbol, received_at, rb.exchange_ts);
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

    /// Make the freshest synced replica the primary, writing its snapshot.
    /// Called whenever the current primary can no longer be trusted.
    fn failover(&mut self, symbol: &str, reason: &str) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let old = state.primary;
        let candidate = (0..state.replicas.len())
            .filter(|&r| Some(r) != old && state.replicas[r].book.is_some())
            .max_by_key(|&r| state.replicas[r].last_event);
        state.primary = candidate;
        match candidate {
            Some(new) => {
                self.counters.switches += 1;
                tracing::debug!(shard = self.id, symbol, ?old, new, "switching source: {reason}");
                self.emit_snapshot(symbol, new, self.as_of);
            }
            None => tracing::debug!(shard = self.id, symbol, ?old, "no synced source: {reason}"),
        }
    }

    /// Invalidate `replica`'s book and ask it to resubscribe.
    fn resubscribe(&mut self, symbol: &str, replica: usize, verify: bool, reason: &str) {
        let now = Instant::now();
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let rs = &mut state.replicas[replica];
        let old = rs.book.take();
        rs.verify = old.filter(|_| verify).map(|rb| rb.book);
        rs.requested_at = now;
        rs.retry_at = now + rs.retry_backoff;
        rs.retry_backoff = (rs.retry_backoff * 2).min(self.cfg.max_retry_backoff);
        let was_primary = state.primary == Some(replica);
        self.counters.resubscribes += 1;
        tracing::debug!(shard = self.id, symbol, replica, "resubscribing: {reason}");
        let _ = self.replicas[replica]
            .tx
            .send(ConnCommand::Resubscribe(symbol.to_owned()));
        if was_primary {
            self.failover(symbol, reason);
        }
    }

    fn on_replica_msg(&mut self, msg: ReplicaMsg) {
        let r = msg.replica;
        let now = Instant::now();
        match msg.kind {
            ReplicaMsgKind::Connected => {
                self.replicas[r].connected_since = Some(now);
                for state in self.symbols.values_mut() {
                    let rs = &mut state.replicas[r];
                    rs.requested_at = now;
                    rs.retry_at = now + self.cfg.sync_timeout;
                    rs.retry_backoff = self.cfg.sync_timeout;
                }
            }
            ReplicaMsgKind::Disconnected => {
                self.replicas[r].connected_since = None;
                let mut orphaned = Vec::new();
                for (symbol, state) in &mut self.symbols {
                    state.replicas[r].book = None;
                    if state.primary == Some(r) {
                        orphaned.push(symbol.clone());
                    }
                }
                for symbol in orphaned {
                    self.failover(&symbol, "primary disconnected");
                }
            }
            ReplicaMsgKind::Event {
                received_at,
                received_mono,
                event,
            } => {
                self.as_of = self.as_of.max(received_at);
                self.on_event(r, received_at, received_mono, event);
            }
        }
    }

    fn on_event(&mut self, r: usize, received_at: DateTime<Utc>, mono: Instant, event: FeedEvent) {
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
                let rs = &mut state.replicas[r];
                rs.last_event = Some(mono);
                rs.synced_at = Some(mono);
                if let Some(expected) = rs.verify.take() {
                    if expected != book {
                        self.counters.recovered += 1;
                        tracing::warn!(
                            shard = self.id,
                            symbol,
                            replica = r,
                            "quiet-check resubscribe found a different book; updates were missed"
                        );
                    }
                }
                let previous = rs.book.replace(ReplicaBook {
                    book,
                    exchange_ts: exchange_ts.unwrap_or(received_at),
                });
                match state.primary {
                    Some(p) if p == r => {
                        // A snapshot on the live source: write the difference
                        // as level updates so the output stays incremental.
                        let rb = state.replicas[r].book.as_ref().unwrap();
                        let exchange_ts = rb.exchange_ts;
                        let changes = match &previous {
                            Some(prev) => prev.book.diff(&rb.book),
                            None => Vec::new(),
                        };
                        if previous.is_none() {
                            self.emit_snapshot(&symbol, r, received_at);
                        }
                        for (side, price, qty) in changes {
                            self.emit_update(&symbol, received_at, exchange_ts, side, price, qty);
                        }
                    }
                    None => {
                        state.primary = Some(r);
                        self.emit_snapshot(&symbol, r, received_at);
                    }
                    Some(_) => {}
                }
            }
            FeedEvent::Level {
                symbol,
                exchange_ts,
                side,
                price,
                change,
                match_key,
            } => {
                let Some(state) = self.symbols.get_mut(&symbol) else {
                    return;
                };
                if let Some(key) = match_key {
                    state.record_update(key, r, mono);
                }
                let rs = &mut state.replicas[r];
                rs.last_event = Some(mono);
                rs.last_level = Some(mono);
                let Some(rb) = rs.book.as_mut() else {
                    return;
                };
                match rb.book.apply(side, price, change) {
                    Ok(quantity) => {
                        let exchange_ts = exchange_ts.unwrap_or(received_at);
                        rb.exchange_ts = exchange_ts;
                        rs.retry_backoff = self.cfg.sync_timeout;
                        if state.primary == Some(r) {
                            if let Some(last) = state.last_primary_update {
                                let gap = mono.saturating_duration_since(last).as_secs_f64();
                                state.ewma_gap =
                                    Some(state.ewma_gap.map_or(gap, |e| 0.9 * e + 0.1 * gap));
                            }
                            state.last_primary_update = Some(mono);
                            state.quiet_backoff = self.cfg.min_quiet;
                            self.emit_update(&symbol, received_at, exchange_ts, side, price, quantity);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(shard = self.id, symbol, replica = r, "book diverged: {e}");
                        self.resubscribe(&symbol, r, false, "book diverged");
                    }
                }
            }
            FeedEvent::Seen { symbol, match_key } => {
                if let Some(state) = self.symbols.get_mut(&symbol) {
                    state.replicas[r].last_event = Some(mono);
                    if let Some(key) = match_key {
                        state.record_update(key, r, mono);
                    }
                }
            }
            FeedEvent::Desync { symbols, reason } => {
                if symbols.len() > 1 {
                    tracing::info!(shard = self.id, replica = r, "{} symbols desynced: {reason}", symbols.len());
                }
                for symbol in symbols {
                    let Some(state) = self.symbols.get_mut(&symbol) else {
                        continue;
                    };
                    let now = Instant::now();
                    let rs = &mut state.replicas[r];
                    rs.book = None;
                    rs.requested_at = now;
                    rs.retry_at = rs.retry_at.max(now + self.cfg.sync_timeout);
                    if state.primary == Some(r) {
                        self.failover(&symbol, &reason);
                    }
                }
            }
        }
    }

    fn health_check(&mut self) {
        let now = Instant::now();
        let connected: Vec<bool> = self
            .replicas
            .iter()
            .map(|r| r.connected_since.is_some())
            .collect();
        let mut actions: Vec<(String, usize, bool, String)> = Vec::new();

        for (symbol, state) in &mut self.symbols {
            let newest = state.replicas.iter().filter_map(|rs| rs.last_event).max();
            for (r, rs) in state.replicas.iter().enumerate() {
                if !connected[r] || now < rs.retry_at {
                    continue;
                }
                if rs.book.is_none() {
                    actions.push((symbol.clone(), r, false, format!(
                        "not synced {:?} after subscribing",
                        now - rs.requested_at
                    )));
                } else if let Some(missed) = state.missed_update(r, self.cfg.stale_lag, now) {
                    actions.push((symbol.clone(), r, false, format!(
                        "missed an update another replica received {missed:.0?} ago"
                    )));
                }
            }
            state.expire_unmatched(self.cfg.stale_lag, now);

            // Quiet relative to the symbol's own history: resubscribe one
            // synced replica and verify its fresh snapshot.
            let silence = now - newest.unwrap_or(state.replicas[0].requested_at);
            let threshold = state
                .ewma_gap
                .map(|gap| Duration::from_secs_f64(gap * self.cfg.quiet_factor))
                .unwrap_or_default()
                .max(self.cfg.min_quiet);
            if silence > threshold && now >= state.quiet_check_at {
                // Prefer a non-primary replica so the output source is not
                // disturbed; fall back to the primary if it is the only one.
                let n = state.replicas.len();
                let candidates: Vec<usize> = (0..n)
                    .map(|i| (state.next_quiet_replica + i) % n)
                    .filter(|&r| connected[r] && state.replicas[r].book.is_some())
                    .collect();
                let pick = candidates
                    .iter()
                    .copied()
                    .find(|&r| Some(r) != state.primary)
                    .or(candidates.first().copied());
                if let Some(r) = pick {
                    state.next_quiet_replica = (r + 1) % n;
                    actions.push((symbol.clone(), r, true, format!("quiet for {silence:?}")));
                }
                state.quiet_check_at = now + state.quiet_backoff;
                state.quiet_backoff = (state.quiet_backoff * 2).min(self.cfg.max_quiet_backoff);
            }
        }
        // Cap resubscribes per socket per tick so a burst (e.g. after a
        // reconnect) can't flood the exchange with commands; the rest are
        // still due and go out on the next ticks.
        let mut budget = vec![MAX_RESUBSCRIBES_PER_TICK; self.replicas.len()];
        let mut deferred = 0;
        for (symbol, r, verify, reason) in actions {
            if budget[r] == 0 {
                deferred += 1;
                continue;
            }
            budget[r] -= 1;
            self.resubscribe(&symbol, r, verify, &reason);
        }
        if deferred > 0 {
            tracing::debug!(shard = self.id, deferred, "resubscribes deferred to later ticks");
        }

        self.maybe_recycle(now, &connected);
    }

    /// Recycle at most one socket at a time, and only while every other
    /// replica is connected and mostly synced, so we never lose coverage.
    fn maybe_recycle(&mut self, now: Instant, connected: &[bool]) {
        if self.replicas.len() < 2 || now - self.last_recycle < Duration::from_secs(60) {
            return;
        }
        let total = self.symbols.len().max(1);
        let synced: Vec<usize> = (0..self.replicas.len())
            .map(|r| {
                self.symbols
                    .values()
                    .filter(|s| s.replicas[r].book.is_some())
                    .count()
            })
            .collect();
        for r in 0..self.replicas.len() {
            let Some(since) = self.replicas[r].connected_since else {
                continue;
            };
            if now - since < self.replicas[r].recycle_after {
                continue;
            }
            let others_healthy = (0..self.replicas.len())
                .filter(|&o| o != r)
                .all(|o| connected[o] && synced[o] * 10 >= total * 9);
            if others_healthy {
                tracing::info!(shard = self.id, replica = r, "recycling socket");
                self.replicas[r].recycle_after = jittered(self.cfg.recycle_interval);
                let _ = self.replicas[r].tx.send(ConnCommand::Reconnect);
                self.last_recycle = now;
                return;
            }
        }
    }

    fn minute_tick(&mut self) {
        let received_at = self.as_of;
        let mut no_source = 0;
        let symbols: Vec<(String, Option<usize>)> = self
            .symbols
            .iter()
            .map(|(s, state)| (s.clone(), state.primary))
            .collect();
        for (symbol, primary) in symbols {
            match primary {
                Some(p) => self.emit_snapshot(&symbol, p, received_at),
                None => no_source += 1,
            }
        }

        let synced: Vec<usize> = (0..self.replicas.len())
            .map(|r| {
                self.symbols
                    .values()
                    .filter(|s| s.replicas[r].book.is_some())
                    .count()
            })
            .collect();
        let c = &self.counters;
        tracing::info!(
            "shard {}: {} symbols, synced per replica {:?}, no source {}, updates {}, snapshots {}, switches {}, resubscribes {}, missed-data recoveries {}",
            self.id,
            self.symbols.len(),
            synced,
            no_source,
            c.updates,
            c.snapshots,
            c.switches,
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
        commands: Vec<UnboundedReceiver<ConnCommand>>,
    }

    impl Harness {
        fn new(replicas: usize) -> Self {
            let cfg = Arc::new(Config {
                replicas,
                ..Config::default()
            });
            let (out_tx, out) = mpsc::unbounded_channel();
            let (txs, commands): (Vec<_>, Vec<_>) =
                (0..replicas).map(|_| mpsc::unbounded_channel()).unzip();
            let mut shard = Shard::new(0, cfg, out_tx, txs, vec![SYM.to_owned()]);
            for r in 0..replicas {
                shard.on_replica_msg(ReplicaMsg {
                    replica: r,
                    kind: ReplicaMsgKind::Connected,
                });
            }
            Self {
                shard,
                out,
                commands,
            }
        }

        fn event(&mut self, replica: usize, event: FeedEvent) {
            self.shard.on_replica_msg(ReplicaMsg {
                replica,
                kind: ReplicaMsgKind::Event {
                    received_at: Utc::now(),
                    received_mono: Instant::now(),
                    event,
                },
            });
        }

        fn snapshot(&mut self, replica: usize, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
            self.event(
                replica,
                FeedEvent::Snapshot {
                    symbol: SYM.to_owned(),
                    exchange_ts: None,
                    bids: bids.to_vec(),
                    asks: asks.to_vec(),
                },
            );
        }

        fn delta(&mut self, replica: usize, side: Side, price: i64, delta: i64) {
            self.event(
                replica,
                FeedEvent::Level {
                    symbol: SYM.to_owned(),
                    exchange_ts: None,
                    side,
                    price,
                    change: LevelChange::Delta(delta),
                    match_key: None,
                },
            );
        }

        fn output(&mut self) -> Vec<OutMsg> {
            std::iter::from_fn(|| self.out.try_recv().ok()).collect()
        }

        fn commands(&mut self, replica: usize) -> Vec<ConnCommand> {
            std::iter::from_fn(|| self.commands[replica].try_recv().ok()).collect()
        }

        fn primary(&self) -> Option<usize> {
            self.shard.symbols[SYM].primary
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
    fn first_synced_replica_becomes_primary_and_only_it_is_written() {
        let mut h = Harness::new(2);
        h.snapshot(1, &[(400_000, 10)], &[]);
        assert_eq!(h.primary(), Some(1));
        let out = h.output();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(400_000, 10)]);

        // The standby syncs silently.
        h.snapshot(0, &[(400_000, 10)], &[]);
        assert!(h.output().is_empty());

        // Only the primary's updates are written, as absolute quantities.
        h.delta(0, Side::Buy, 400_000, 5);
        assert!(h.output().is_empty());
        h.delta(1, Side::Buy, 400_000, 5);
        let out = h.output();
        assert_eq!(update(&out[0]), (Side::Buy, 400_000, 15));
    }

    #[test]
    fn disconnect_switches_source_with_snapshot_of_new_book() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        // Replica 1 saw an update replica 0 missed.
        h.delta(1, Side::Buy, 500_000, 3);
        h.output();

        h.shard.on_replica_msg(ReplicaMsg {
            replica: 0,
            kind: ReplicaMsgKind::Disconnected,
        });
        assert_eq!(h.primary(), Some(1));
        let out = h.output();
        assert_eq!(out.len(), 1);
        // Bids are best first.
        assert_eq!(snapshot_bids(&out[0]), vec![(500_000, 3), (400_000, 10)]);
    }

    #[test]
    fn desync_without_alternative_goes_dark_until_next_snapshot() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.output();
        h.event(
            0,
            FeedEvent::Desync {
                symbols: vec![SYM.to_owned()],
                reason: "seq gap".to_owned(),
            },
        );
        assert_eq!(h.primary(), None);
        // Updates on a desynced book are dropped, not written.
        h.delta(0, Side::Buy, 400_000, 1);
        assert!(h.output().is_empty());

        h.snapshot(0, &[(400_000, 7)], &[]);
        assert_eq!(h.primary(), Some(0));
        assert_eq!(snapshot_bids(&h.output()[0]), vec![(400_000, 7)]);
    }

    #[test]
    fn diverged_book_resubscribes_and_fails_over() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        h.output();

        // A delta that would take a level negative proves divergence.
        h.delta(0, Side::Buy, 400_000, -11);
        assert!(matches!(
            h.commands(0).as_slice(),
            [ConnCommand::Resubscribe(s)] if s == SYM
        ));
        assert_eq!(h.primary(), Some(1));
        let out = h.output();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(400_000, 10)]);
    }

    #[test]
    fn snapshot_on_live_source_is_written_as_level_diff() {
        let mut h = Harness::new(1);
        h.snapshot(0, &[(400_000, 10), (390_000, 5)], &[(600_000, 2)]);
        h.output();
        h.snapshot(0, &[(400_000, 12)], &[(600_000, 2)]);
        let mut updates: Vec<_> = h.output().iter().map(update).collect();
        updates.sort_by_key(|&(side, price, _)| (side as u8, price));
        assert_eq!(updates, vec![(Side::Buy, 390_000, 0), (Side::Buy, 400_000, 12)]);
    }

    #[test]
    fn lagging_primary_is_resubscribed_and_replaced() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        h.output();
        h.commands(0);

        // Replica 1 has been receiving updates; replica 0 went quiet.
        let now = Instant::now();
        let state = h.shard.symbols.get_mut(SYM).unwrap();
        for rs in &mut state.replicas {
            rs.synced_at = Some(now - Duration::from_secs(60));
        }
        state.unmatched.push((42, now - Duration::from_secs(11), vec![0, 1]));
        for rs in &mut state.replicas {
            rs.retry_at = now;
        }
        h.shard.health_check();

        assert!(matches!(
            h.commands(0).as_slice(),
            [ConnCommand::Resubscribe(s)] if s == SYM
        ));
        assert!(h.commands(1).is_empty());
        assert_eq!(h.primary(), Some(1));
        assert_eq!(h.output().len(), 1);
    }

    #[test]
    fn fresh_snapshot_on_one_replica_does_not_make_the_other_stale() {
        // A quiet symbol: its last level update was long ago on both.
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        let now = Instant::now();
        let state = h.shard.symbols.get_mut(SYM).unwrap();
        for rs in &mut state.replicas {
            rs.synced_at = Some(now - Duration::from_secs(600));
            rs.last_level = Some(now - Duration::from_secs(300));
            rs.retry_at = now;
        }
        // Replica 1 resubscribes and gets a fresh snapshot.
        h.snapshot(1, &[(400_000, 10)], &[]);
        h.shard.health_check();
        assert!(h.commands(0).is_empty());
        assert!(h.commands(1).is_empty());

        // Replica 1 gets a new update; replica 0's copy is a few ms behind.
        // Neither is stale, even though replica 0's last update is old.
        h.delta(1, Side::Buy, 400_000, 1);
        h.shard.health_check();
        h.delta(0, Side::Buy, 400_000, 1);
        h.shard.health_check();
        assert!(h.commands(0).is_empty());
        assert!(h.commands(1).is_empty());
    }

    #[test]
    fn replica_missing_an_update_is_resubscribed_but_second_arrival_is_not() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        h.commands(0);
        h.commands(1);
        let keyed = |key| FeedEvent::Level {
            symbol: SYM.to_owned(),
            exchange_ts: None,
            side: Side::Buy,
            price: 400_000,
            change: LevelChange::Delta(1),
            match_key: Some(key),
        };
        // Update 1 reaches replica 0 first, then replica 1: fully matched.
        h.event(0, keyed(1));
        h.event(1, keyed(1));
        // The same key twice on both replicas is matched too.
        h.event(0, keyed(3));
        h.event(1, keyed(3));
        h.event(0, keyed(3));
        h.event(1, keyed(3));
        // Update 2 only ever reaches replica 1.
        h.event(1, keyed(2));
        let state = h.shard.symbols.get_mut(SYM).unwrap();
        assert_eq!(state.unmatched.len(), 1);
        let now = Instant::now();
        for rs in &mut state.replicas {
            rs.retry_at = now;
        }
        // Not stale yet.
        h.shard.health_check();
        assert!(h.commands(0).is_empty() && h.commands(1).is_empty());
        // After stale_lag, only replica 0 (which missed update 2) is resubscribed.
        let state = h.shard.symbols.get_mut(SYM).unwrap();
        for rs in &mut state.replicas {
            rs.synced_at = Some(now - Duration::from_secs(60));
        }
        state.unmatched[0].1 = now - Duration::from_secs(11);
        h.shard.health_check();
        assert!(matches!(h.commands(0).as_slice(), [ConnCommand::Resubscribe(s)] if s == SYM));
        assert!(h.commands(1).is_empty());
    }

    #[test]
    fn resubscribes_are_rate_limited_per_socket() {
        let mut h = Harness::new(2);
        let symbols: Vec<String> = (0..50).map(|i| format!("S{i}")).collect();
        h.shard.add(symbols.clone());
        h.commands(0);
        let now = Instant::now();
        for state in h.shard.symbols.values_mut() {
            state.replicas[0].retry_at = now;
        }
        h.shard.health_check();
        let resubscribes = |cmds: Vec<ConnCommand>| {
            cmds.iter().filter(|c| matches!(c, ConnCommand::Resubscribe(_))).count()
        };
        assert_eq!(resubscribes(h.commands(0)), MAX_RESUBSCRIBES_PER_TICK);
        // The rest go out on later ticks.
        h.shard.health_check();
        assert_eq!(resubscribes(h.commands(0)), MAX_RESUBSCRIBES_PER_TICK);
    }

    #[test]
    fn unsynced_replica_is_retried_with_backoff() {
        let mut h = Harness::new(2);
        h.snapshot(0, &[(400_000, 10)], &[]);
        let now = Instant::now();
        let rs = &mut h.shard.symbols.get_mut(SYM).unwrap().replicas[1];
        rs.retry_at = now;
        h.shard.health_check();
        assert_eq!(h.commands(1).len(), 1);
        // The next retry is not due yet.
        h.shard.health_check();
        assert!(h.commands(1).is_empty());
        let rs = &h.shard.symbols[SYM].replicas[1];
        assert!(rs.retry_backoff > h.shard.cfg.sync_timeout);
    }

    #[test]
    fn output_timestamps_never_go_backwards() {
        let mut h = Harness::new(2);
        let t0 = Utc::now();
        let at = |ms| t0 + chrono::Duration::milliseconds(ms);
        let level = |price| FeedEvent::Level {
            symbol: SYM.to_owned(),
            exchange_ts: None,
            side: Side::Buy,
            price,
            change: LevelChange::Delta(1),
            match_key: None,
        };
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.snapshot(1, &[(400_000, 10)], &[]);
        h.output();
        let send = |h: &mut Harness, replica, ms, event| {
            h.shard.on_replica_msg(ReplicaMsg {
                replica,
                kind: ReplicaMsgKind::Event {
                    received_at: at(ms),
                    received_mono: Instant::now(),
                    event,
                },
            });
        };
        // The primary's update at +10ms is written, then it fails; replica
        // 1's queued update was received earlier (+5ms) but is processed later.
        send(&mut h, 0, 10, level(400_000));
        send(&mut h, 1, 5, level(400_000));
        h.shard.on_replica_msg(ReplicaMsg {
            replica: 0,
            kind: ReplicaMsgKind::Disconnected,
        });
        send(&mut h, 1, 12, level(400_000));
        h.shard.minute_tick();
        let stamps: Vec<DateTime<Utc>> = h
            .output()
            .iter()
            .map(|m| match m {
                OutMsg::Snapshot(s) => s.received_at,
                OutMsg::Update(u) => u.backtest_timestamp,
            })
            .collect();
        assert_eq!(stamps.len(), 4);
        assert!(stamps.windows(2).all(|w| w[0] <= w[1]), "{stamps:?}");
        // The failover snapshot is not stamped later than what was received.
        assert!(stamps[1] <= at(12));
    }

    #[test]
    fn minute_tick_snapshots_every_sourced_symbol() {
        let mut h = Harness::new(2);
        h.shard.add(vec!["B".to_owned()]);
        h.snapshot(0, &[(400_000, 10)], &[]);
        h.output();
        h.shard.minute_tick();
        // "A" has a source, "B" never synced.
        let out = h.output();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(400_000, 10)]);
    }
}
