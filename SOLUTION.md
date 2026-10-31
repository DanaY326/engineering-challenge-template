# Orderbook collector: design notes

## Running

```sh
cargo build --release

# Kalshi (needs an API key; RSA or Ed25519 PEM). Put KALSHI_API_KEY_ID and
# KALSHI_PRIVATE_KEY_PATH in .env (see .env.example); it is loaded on startup.
cp .env.example .env   # then fill it in
./target/release/engineering-challenge > kalshi.sbe

# Read the output back with the template's AlignedSbeReader and sanity-check it
./target/release/inspect kalshi.sbe --print 20
```

SBE goes to stdout by default (`-o -`); logs go to stderr (`RUST_LOG=debug` for
per-symbol failover/resubscribe detail). `--max-symbols N` limits collection
for testing; `--help` lists the rest.

On SIGINT every websocket sends a close frame, each shard waits for its
sockets, and then the writer drains the channel, flushes the final chunk, and
closes the file. A second SIGINT exits immediately.

## Output semantics

- Prices and quantities use the schema's fixed point (1_000_000 = $1 or 1
  contract). Decimal strings are parsed exactly, never via `f64`.
- YES bids are bids, and NO bids at `p` are asks at `1 - p`. The symbol is
  the Kalshi market ticker.
- `PriceLevelUpdate.quantity` is the level's new absolute quantity (0 =
  removed). `backtest_timestamp` is when the frame was read off the socket.
  `exchange_timestamp` is Kalshi's timestamp (`ts_ms`, or `sending_ts_ms`
  for snapshots). If Kalshi gives none, the receive time is used.
- Per symbol, output timestamps never go backwards. Snapshots we generate
  (on failover and every minute) are stamped with the latest receive time
  the shard has processed, not the wall clock. Otherwise they could land
  after queued updates that were received earlier.
- A `Snapshot` is written:
  - when a symbol first gets a synced source;
  - whenever its source websocket changes;
  - for every symbol at each wall-clock minute boundary.

  Between snapshots, applying the updates in order reproduces the book.
  `inspect` checks this.
- Output uses the template's `AlignedSbeWriter` with 64 MiB chunks. The
  chunk size must match the reader's, so data reaches stdout in 64 MiB pieces.

## Architecture

```
discovery loop ──(add/remove symbols)──► shard 0..N ──► writer thread ──► AlignedSbeWriter
                                          │  ▲
                              commands    ▼  │  normalized events + receive time
                                    replica sockets 0..R
                                          │
                                 Kalshi session (encode/decode)
```

| Module | Role |
|---|---|
| `collector/exchange/` | The only exchange-specific code. `kalshi.rs`: auth, market discovery, subscribe/resubscribe, decoding into `Snapshot` / `Level` / `Desync` / `Seen`, sequence checks. |
| `collector/connection.rs` | One websocket. It handles connect and reconnect with backoff, sends pings, and reconnects after 30 s of silence. It timestamps each frame as soon as it is read. |
| `collector/shard.rs` | Runs ≤ `symbols_per_shard` symbols on `replicas` sockets, keeping one book per (symbol, replica). It writes updates and minute snapshots, and handles failover, monitoring and recycling. |
| `collector/mod.rs` | The discovery loop. New symbols start collecting page by page as the listing streams in. Removals are applied only once the listing is complete, so a partial listing never unsubscribes live markets. |
| `collector/output.rs` | The writer thread, so file I/O never stalls the network tasks. An output error, such as a full disk, shuts the collector down instead of silently dropping data. |
| `bin/inspect.rs` | Reads an output file with the template's reader and checks it: no update before its symbol's first snapshot, no per-symbol timestamp going backwards, no crossed snapshot. |

**Load balancing.** Symbols are packed into shards, each with its own
sockets, so a busy or broken shard doesn't affect the others. Kalshi has
about 169k open non-MVE markets (measured 2026-10-30), most of them
illiquid, so the default is 1000 markets per shard. Change it with
`--symbols-per-shard`.

**Connection budget.** Kalshi refuses the 201st concurrent websocket on an
account with HTTP 429. This limit is undocumented; I measured it. Sockets
beyond the limit keep retrying, and each time a healthy socket drops, one
of them takes the freed slot, so the healthy shard can't reconnect. The
collector therefore takes 95% of that limit (190 sockets) as its
budget, adjustable with `--max-connections`. Once the budget is used, new
symbols go to the least-loaded existing shard instead of opening sockets
that would be refused.

## Completeness

- **Kalshi sequence numbers.** Each `subscribe` covers ≤100 tickers and gets
  its own `sid` with a gapless `seq`. Acks for `update_subscription` also
  consume a seq. On a gap, that one subscription is dropped and resubscribed
  from scratch, and its tickers are marked `Desync` on that socket only.
- **Kalshi snapshot clock.** A snapshot includes every change with `clock`
  ≤ its own. Deltas at or below it that arrive after the snapshot are
  skipped, or they would be applied twice. Only the first deltas after a
  snapshot are compared, so a clock reset during exchange maintenance can't
  make us drop live updates.
- **Book integrity.** A delta that would make a level negative proves the
  book has diverged. That symbol is resubscribed on that socket.

## Extension 1: redundant websockets

Every shard runs `--replicas` (default 2) independent sockets, each with the
same subscriptions and its own copy of every book. Per symbol, one synced
replica is the **primary**, and only its updates are written. The primary is
replaced, with a snapshot written from the new replica's book, when:

- its socket disconnects, stalls (no frames for 30 s), or is recycled;
- it reports a `Desync` (seq gap, resubscribe) or its book diverges;
- it **misses an update** another replica received. Every update carries a
  match key `(ticker, clock, side, price, delta)`, using Kalshi's
  exchange-wide `clock`. Per symbol, the shard counts how many times each replica has
  seen each key. Healthy replicas catch up within milliseconds. If a synced
  replica is still short of a key 10 s (`stale_lag`) after another replica
  got it, it has lost data, so it is resubscribed and replaced. Updates from
  before the replica's latest snapshot don't count, since the snapshot
  covers them. Comparing *update identities* rather than "time of last
  message" matters; see **Running at full scale**.

When no replica is synced, nothing is written for that symbol. The next
replica to sync writes a snapshot, so a consumer sees the gap as a snapshot
boundary instead of silently missing deltas.

## Extension 2: subscription monitoring

Every second, the shard checks each (symbol, replica):

- **Never synced.** No snapshot 20 s after subscribing → resubscribe that
  symbol on that socket. Retries back off 20 s → 10 min. Single-ticker
  retries also isolate a bad ticker that made a whole subscribe batch fail.
- **Missed updates.** A replica missing an update another replica got is
  resubscribed (see Extension 1).
- **Quiet compared with its own history.** Each symbol keeps an EWMA of the
  time between updates. Silence longer than max(10 min, 30 × that gap)
  triggers a resubscribe on one *non-primary* replica. The fresh snapshot
  is compared with the book we held. A mismatch is logged as a
  "missed-data recovery" and counted. Repeat checks back off up to 4 h, so
  dead markets cost almost nothing.
- **Rate limit.** At most 20 monitoring-triggered resubscribes per socket
  per second; the rest wait for the next tick. A burst, such as everything
  after a reconnect, can't flood the exchange with commands.
- **Periodic socket recycling.** Each socket reconnects about every 30 min
  (±25% jitter). A socket recycles only when every other replica in the
  shard is connected and ≥90% synced, and at most one per shard per minute.
  So recycling never leaves a symbol without a source.

Each shard logs one summary line per minute: synced counts per replica,
symbols with no source, updates, snapshots, switches, resubscribes, and
recoveries.

## Running at full scale (Kalshi, 2026-10-30)

The first full-universe runs exposed three bugs that small runs never hit:

1. **Connection cap.** 200 concurrent sockets per account (see
   **Connection budget**).
2. **A resubscribe feedback loop.** "Behind another replica" first compared
   the time of each replica's last message, and snapshots counted. A
   resubscribe on replica A gives A a fresh snapshot, which made a healthy
   but quiet replica B look stale, so B was resubscribed, which made A look
   stale... Resubscribes grew to 188k/min. Kalshi then started resetting
   connections, and the books diverged during the churn.
3. **A timing race**, still in the second attempt, which compared the time of
   the last *update*. On a quiet market, whichever replica received an
   update second looked "behind" until its next update, minutes later.
   Comparing times can't tell "the same update, received second" from "a
   new update". Matching the updates themselves by key can, and that is
   the fix.

| full-universe run | markets covered | synced per socket | no source | resubscribes/min | conn. resets | book divergences |
|---|---|---|---|---|---|---|
| first (no budget) | 106k of 169k | ~91% | 522 | 4 → 34k | 9 | 18 |
| budget, time-based "behind" | 169k | ~93% | 138–2,439 | 9k → 188k | 220 | 36 |
| budget, key matching (bitmask) | 169k | 99.8% | 0 | 2–6k | 0 | 0 |
| budget, key matching (counts) | **169k** | **100%** (after start-up) | **0** | **0** (after start-up) | 1 | **0** |

In the final run, the only churn was the start-up minute, while about 170k
initial subscriptions were in flight (about 2.6k resubscribes). From minute
3 on, every market was synced on both sockets, with 0 resubscribes and 0
source switches at about 1M updates/min. The run wrote 5.35M messages
(945k snapshots, 4.4M updates) in 5 minutes. `inspect` found it clean: 0
orphan updates, 0 timestamps going backwards, 0 crossed books. The bitmask
version still counted a key that legitimately occurs twice (same clock,
side, price, size) as "missed"; per-replica counts fixed that.

## Verification

- `cargo test --release` runs 20 unit tests:
  - fixed-point parsing; book apply;
  - Kalshi decode, seq-gap recovery, the snapshot-clock filter, the
    resubscribe handshake and Ed25519 signing;
  - the shard's state machine, driven without sockets: only the primary is
    written; failover writes a snapshot of the new source's book; a
    desynced symbol goes dark until its next snapshot; a diverged book is
    resubscribed; a replica that misses an update is resubscribed, but one that merely
    receives it second is not; unsynced replicas are retried with backoff;
    resubscribes are rate-limited; timestamps stay monotonic; minute
    snapshots.
- Live Kalshi runs (Ed25519 key), read back with `inspect`: 300 markets on
  2 shards × 2 sockets were all clean, with 389 two-sided books and none
  crossed (confirming the NO→ask inversion) and no delta driving a level
  negative (confirming the delta semantics). Full-universe runs: see
  **Running at full scale**. Minute snapshots land at :00, and SIGINT
  closes the sockets, flushes and exits 0.

## Known limitations and next steps

- New markets are found by polling REST. A full Kalshi listing takes about
  75 s because of rate limits, so a new market is picked up within about
  2 min. Kalshi's `market_lifecycle_v2` channel would cut that to
  milliseconds. No resting orders are lost either way, because the first
  snapshot holds the full book; only the timing of changes made before we
  subscribed is lost. Pages are applied as they arrive, so collection
  starts within a second of launch.
- Output is a single file. Hourly rotation, with a snapshot at the start of
  each file, would make files self-contained.
- The writer is fed by an unbounded channel. A stalled disk grows memory
  instead of back-pressuring the sockets. That choice is deliberate, since
  back-pressure would distort receive timestamps, but it should be alerted on.
- Kalshi's limits are undocumented. The 200-socket cap was measured on one
  account and may differ by tier. At about 1,780 markets per socket nothing
  failed in 5 minutes; longer runs should watch for `Subscription buffer
  overflow` (error 25), which the collector handles by reconnecting the
  socket.
- The 200-socket cap forces a trade-off. Full coverage of 169k markets
  leaves room for only 2 replicas per shard. Restricting to liquid markets
  (or using several accounts) would allow 3+.
