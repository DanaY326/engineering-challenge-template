//! One websocket connection (a "replica") serving a shard's symbols.
//!
//! The task owns the socket and the exchange [`Session`]. It reconnects with
//! backoff, sends heartbeats, tears the socket down if it goes silent, and
//! forwards every decoded event to the shard, stamped with the time the frame
//! was read off the socket.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::Config;
use super::exchange::{Exchange, FeedEvent, Session};

pub enum ConnCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Resubscribe(String),
    /// Tear down and reconnect (periodic recycling, Extension 2).
    Reconnect,
}

pub struct ReplicaMsg {
    pub replica: usize,
    pub kind: ReplicaMsgKind,
}

pub enum ReplicaMsgKind {
    Connected,
    Disconnected,
    Event {
        received_at: DateTime<Utc>,
        received_mono: Instant,
        event: FeedEvent,
    },
}

const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A connection that lived this long resets the reconnect backoff.
const HEALTHY_UPTIME: Duration = Duration::from_secs(60);

enum Exit {
    Shutdown,
    Reconnect(String),
}

pub struct Connection<E: Exchange> {
    pub exchange: Arc<E>,
    pub cfg: Arc<Config>,
    pub shard: usize,
    pub replica: usize,
    pub symbols: BTreeSet<String>,
    pub commands: UnboundedReceiver<ConnCommand>,
    pub events: UnboundedSender<ReplicaMsg>,
    pub cancel: CancellationToken,
}

impl<E: Exchange> Connection<E> {
    fn send(&self, kind: ReplicaMsgKind) {
        // The shard only goes away on shutdown, at which point we stop too.
        let _ = self.events.send(ReplicaMsg {
            replica: self.replica,
            kind,
        });
    }

    /// Apply a command while disconnected; the next connection subscribes to
    /// the resulting symbol set.
    fn apply_offline(&mut self, command: ConnCommand) {
        match command {
            ConnCommand::Subscribe(symbols) => self.symbols.extend(symbols),
            ConnCommand::Unsubscribe(symbols) => {
                for symbol in &symbols {
                    self.symbols.remove(symbol);
                }
            }
            ConnCommand::Resubscribe(_) | ConnCommand::Reconnect => {}
        }
    }

    pub async fn run(mut self) {
        let mut backoff = MIN_BACKOFF;
        loop {
            let started = Instant::now();
            let exit = self.connect_and_stream().await;
            self.send(ReplicaMsgKind::Disconnected);
            let reason = match exit {
                Exit::Shutdown => return,
                Exit::Reconnect(reason) => reason,
            };
            if started.elapsed() > HEALTHY_UPTIME {
                backoff = MIN_BACKOFF;
            }
            tracing::info!(
                shard = self.shard,
                replica = self.replica,
                "{} disconnected ({reason}); reconnecting in {backoff:?}",
                self.exchange.name()
            );
            let wake = tokio::time::sleep(backoff);
            tokio::pin!(wake);
            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    _ = &mut wake => break,
                    Some(command) = self.commands.recv() => self.apply_offline(command),
                }
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn connect_and_stream(&mut self) -> Exit {
        let request = match self.exchange.connect_request() {
            Ok(request) => request,
            Err(e) => return Exit::Reconnect(format!("building request: {e:#}")),
        };
        // Disable Nagle: we care about latency, not packet count.
        let connect = tokio_tungstenite::connect_async_with_config(request, None, true);
        let ws = tokio::select! {
            _ = self.cancel.cancelled() => return Exit::Shutdown,
            result = connect => match result {
                Ok((ws, _)) => ws,
                Err(e) => return Exit::Reconnect(format!("connect failed: {e}")),
            },
        };
        let (mut sink, mut stream) = ws.split();
        let mut session = self.exchange.new_session();
        let mut events = Vec::new();
        let mut out = Vec::new();

        let symbols: Vec<String> = self.symbols.iter().cloned().collect();
        session.subscribe(&symbols, &mut out);
        self.send(ReplicaMsgKind::Connected);
        tracing::debug!(
            shard = self.shard,
            replica = self.replica,
            "connected, subscribing to {} symbols",
            symbols.len()
        );

        let mut heartbeat = tokio::time::interval(self.cfg.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_rx = Instant::now();

        let exit = loop {
            if let Err(e) = send_all(&mut sink, &mut out).await {
                break Exit::Reconnect(format!("send failed: {e}"));
            }
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break Exit::Shutdown,
                frame = stream.next() => {
                    let received_mono = Instant::now();
                    let received_at = Utc::now();
                    last_rx = received_mono;
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = session.decode(&text, &mut events, &mut out) {
                                break Exit::Reconnect(format!("decode error: {e:#}; frame: {}", truncate(&text)));
                            }
                            for event in events.drain(..) {
                                self.send(ReplicaMsgKind::Event { received_at, received_mono, event });
                            }
                        }
                        Some(Ok(Message::Close(frame))) => break Exit::Reconnect(format!("server closed: {frame:?}")),
                        Some(Ok(_)) => {}
                        Some(Err(e)) => break Exit::Reconnect(format!("read error: {e}")),
                        None => break Exit::Reconnect("stream ended".to_owned()),
                    }
                }
                command = self.commands.recv() => match command {
                    None => break Exit::Shutdown,
                    Some(ConnCommand::Subscribe(symbols)) => {
                        let fresh: Vec<String> = symbols.into_iter().filter(|s| self.symbols.insert(s.clone())).collect();
                        session.subscribe(&fresh, &mut out);
                    }
                    Some(ConnCommand::Unsubscribe(symbols)) => {
                        let gone: Vec<String> = symbols.into_iter().filter(|s| self.symbols.remove(s)).collect();
                        session.unsubscribe(&gone, &mut out);
                    }
                    Some(ConnCommand::Resubscribe(symbol)) => {
                        if self.symbols.contains(&symbol) {
                            session.resubscribe(&symbol, &mut events, &mut out);
                            let now = (Utc::now(), Instant::now());
                            for event in events.drain(..) {
                                self.send(ReplicaMsgKind::Event { received_at: now.0, received_mono: now.1, event });
                            }
                        }
                    }
                    Some(ConnCommand::Reconnect) => break Exit::Reconnect("periodic recycle".to_owned()),
                },
                _ = heartbeat.tick() => {
                    if last_rx.elapsed() > self.cfg.read_timeout {
                        break Exit::Reconnect(format!("no data for {:?}", last_rx.elapsed()));
                    }
                    if sink.send(Message::Ping(Vec::new())).await.is_err() {
                        break Exit::Reconnect("ping failed".to_owned());
                    }
                    if let Some(text) = self.exchange.app_heartbeat() {
                        out.push(text.to_owned());
                    }
                }
            }
        };
        // Best-effort close handshake so the exchange frees the session.
        let _ = tokio::time::timeout(Duration::from_secs(2), sink.close()).await;
        exit
    }
}

async fn send_all<S>(sink: &mut S, out: &mut Vec<String>) -> Result<(), S::Error>
where
    S: futures_util::Sink<Message> + Unpin,
{
    if out.is_empty() {
        return Ok(());
    }
    for frame in out.drain(..) {
        sink.feed(Message::Text(frame)).await?;
    }
    sink.flush().await
}

fn truncate(s: &str) -> &str {
    match s.char_indices().nth(300) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}
