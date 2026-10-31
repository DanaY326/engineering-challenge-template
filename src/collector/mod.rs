//! An exchange-agnostic orderbook collector.
//!
//! [`run`] periodically lists the exchange's live symbols and spreads them
//! across shards (see [`shard`]), each served by several redundant websockets
//! (see [`connection`]). Shards write snapshots and level updates to the
//! output channel consumed by [`output::spawn_writer`].

pub mod book;
pub mod connection;
pub mod exchange;
pub mod fixed;
pub mod output;
pub mod shard;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use exchange::Exchange;
use output::OutMsg;
use shard::ShardCommand;

#[derive(Debug, Clone)]
pub struct Config {
    /// Redundant websockets per shard.
    pub replicas: usize,
    pub symbols_per_shard: usize,
    pub discovery_interval: Duration,
    /// Only collect this many symbols (for testing).
    pub max_symbols: Option<usize>,
    /// Reconnect each socket roughly this often (±25% jitter).
    pub recycle_interval: Duration,
    /// Resubscribe a symbol that has not synced this long after subscribing.
    pub sync_timeout: Duration,
    pub max_retry_backoff: Duration,
    /// Resubscribe a replica that is this far behind another on a symbol.
    pub stale_lag: Duration,
    /// A symbol is "quiet" after max(min_quiet, quiet_factor × its usual gap).
    pub min_quiet: Duration,
    pub quiet_factor: f64,
    pub max_quiet_backoff: Duration,
    pub heartbeat_interval: Duration,
    /// Reconnect a socket that has received nothing (not even a pong) for this long.
    pub read_timeout: Duration,
    /// Total websockets we may open. Defaults to 95% of the exchange's limit
    /// (headroom for reconnects); once reached, new symbols are added to the
    /// least-loaded shards instead of opening new ones.
    pub max_connections: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            replicas: 2,
            symbols_per_shard: 250,
            discovery_interval: Duration::from_secs(60),
            max_symbols: None,
            recycle_interval: Duration::from_secs(30 * 60),
            sync_timeout: Duration::from_secs(20),
            max_retry_backoff: Duration::from_secs(10 * 60),
            stale_lag: Duration::from_secs(10),
            min_quiet: Duration::from_secs(10 * 60),
            quiet_factor: 30.0,
            max_quiet_backoff: Duration::from_secs(4 * 60 * 60),
            heartbeat_interval: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            max_connections: None,
        }
    }
}

struct ShardHandle {
    tx: UnboundedSender<ShardCommand>,
    join: JoinHandle<()>,
    symbols: HashSet<String>,
}

struct Collector<E: Exchange> {
    exchange: Arc<E>,
    cfg: Arc<Config>,
    out: UnboundedSender<OutMsg>,
    cancel: CancellationToken,
    shards: Vec<ShardHandle>,
    assignment: HashMap<String, usize>,
    max_shards: Option<usize>,
    warned_full: bool,
}

/// Run until `cancel` fires, then wait for every shard (and socket) to close.
pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    cfg: Config,
    out: UnboundedSender<OutMsg>,
    cancel: CancellationToken,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("voluntas-orderbook-collector")
        .build()
        .expect("building HTTP client");
    let budget = cfg
        .max_connections
        .or(exchange.max_connections().map(|limit| limit * 95 / 100));
    let max_shards = budget.map(|budget| (budget / cfg.replicas).max(1));
    if let Some(max_shards) = max_shards {
        tracing::info!(
            "{}: at most {max_shards} shards × {} sockets",
            exchange.name(),
            cfg.replicas
        );
    }
    let mut collector = Collector {
        exchange,
        cfg: Arc::new(cfg),
        out,
        cancel: cancel.clone(),
        shards: Vec::new(),
        assignment: HashMap::new(),
        max_shards,
        warned_full: false,
    };

    'discovery: loop {
        // Start collecting each page's new symbols as soon as it arrives (a
        // full listing can take minutes); only remove symbols once the
        // listing is complete.
        let (page_tx, mut page_rx) = mpsc::unbounded_channel();
        let exchange = collector.exchange.clone();
        let listing = exchange.list_symbols(&http, collector.cfg.max_symbols, page_tx);
        tokio::pin!(listing);
        let mut added = 0;
        let listing = loop {
            tokio::select! {
                _ = cancel.cancelled() => break 'discovery,
                Some(page) = page_rx.recv() => added += collector.add(page),
                listing = &mut listing => break listing,
            }
        };
        while let Ok(page) = page_rx.try_recv() {
            added += collector.add(page);
        }
        match listing {
            Ok(symbols) => {
                let (more, removed) = collector.reconcile(symbols);
                collector.log_changes(added + more, removed);
            }
            Err(e) => {
                collector.log_changes(added, 0);
                tracing::warn!("listing {} symbols failed: {e:#}", collector.exchange.name());
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(collector.cfg.discovery_interval) => {}
        }
    }

    tracing::info!("shutting down {} shards", collector.shards.len());
    for shard in collector.shards {
        let _ = shard.join.await;
    }
}

impl<E: Exchange> Collector<E> {
    /// Apply a complete listing: remove unlisted symbols and add new ones.
    /// Returns `(added, removed)`.
    fn reconcile(&mut self, listed: Vec<String>) -> (usize, usize) {
        if listed.is_empty() && !self.assignment.is_empty() {
            tracing::warn!("exchange listed no symbols; ignoring rather than unsubscribing everything");
            return (0, 0);
        }
        let listed: HashSet<String> = listed.into_iter().collect();

        let mut removed: HashMap<usize, Vec<String>> = HashMap::new();
        self.assignment.retain(|symbol, shard| {
            let keep = listed.contains(symbol);
            if !keep {
                removed.entry(*shard).or_default().push(symbol.clone());
            }
            keep
        });
        let removed_count: usize = removed.values().map(Vec::len).sum();
        for (shard, symbols) in removed {
            for symbol in &symbols {
                self.shards[shard].symbols.remove(symbol);
            }
            let _ = self.shards[shard].tx.send(ShardCommand::Remove(symbols));
        }
        (self.add(listed.into_iter().collect()), removed_count)
    }

    /// Assign symbols we don't collect yet to shards. Returns how many were
    /// added.
    fn add(&mut self, symbols: Vec<String>) -> usize {
        let mut added: Vec<String> = symbols
            .into_iter()
            .filter(|s| !self.assignment.contains_key(s))
            .collect();
        added.sort();
        added.dedup();
        if let Some(max) = self.cfg.max_symbols {
            added.truncate(max.saturating_sub(self.assignment.len()));
        }
        let added_count = added.len();
        let mut added = added.into_iter().peekable();

        // Fill holes in existing shards first, then open new shards.
        for (id, shard) in self.shards.iter_mut().enumerate() {
            let room = self.cfg.symbols_per_shard.saturating_sub(shard.symbols.len());
            let batch: Vec<String> = added.by_ref().take(room).collect();
            if batch.is_empty() {
                continue;
            }
            for symbol in &batch {
                shard.symbols.insert(symbol.clone());
                self.assignment.insert(symbol.clone(), id);
            }
            let _ = shard.tx.send(ShardCommand::Add(batch));
        }
        while added.peek().is_some() {
            if self.max_shards.is_some_and(|max| self.shards.len() >= max) {
                self.add_to_least_loaded(added.by_ref().collect());
                break;
            }
            let batch: Vec<String> = added.by_ref().take(self.cfg.symbols_per_shard).collect();
            let id = self.shards.len();
            for symbol in &batch {
                self.assignment.insert(symbol.clone(), id);
            }
            let (tx, join) = shard::spawn(
                id,
                self.exchange.clone(),
                batch.clone(),
                self.cfg.clone(),
                self.out.clone(),
                self.cancel.clone(),
            );
            self.shards.push(ShardHandle {
                tx,
                join,
                symbols: batch.into_iter().collect(),
            });
        }
        added_count
    }

    /// The connection budget is used up: grow the least-loaded shards beyond
    /// `symbols_per_shard` rather than opening sockets the exchange would
    /// refuse (and that would steal slots from healthy shards on reconnect).
    fn add_to_least_loaded(&mut self, symbols: Vec<String>) {
        if !self.warned_full {
            self.warned_full = true;
            tracing::warn!(
                "{}: connection budget reached ({} shards); adding symbols to existing shards beyond {} per shard",
                self.exchange.name(),
                self.shards.len(),
                self.cfg.symbols_per_shard,
            );
        }
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in symbols {
            let (id, shard) = self
                .shards
                .iter_mut()
                .enumerate()
                .min_by_key(|(_, shard)| shard.symbols.len())
                .expect("max_shards is at least 1");
            shard.symbols.insert(symbol.clone());
            self.assignment.insert(symbol.clone(), id);
            batches.entry(id).or_default().push(symbol);
        }
        for (id, batch) in batches {
            let _ = self.shards[id].tx.send(ShardCommand::Add(batch));
        }
    }

    fn log_changes(&self, added: usize, removed: usize) {
        if added > 0 || removed > 0 {
            tracing::info!(
                "{}: +{added} -{removed} symbols; now {} symbols on {} shards × {} sockets",
                self.exchange.name(),
                self.assignment.len(),
                self.shards.len(),
                self.cfg.replicas,
            );
        }
    }
}
