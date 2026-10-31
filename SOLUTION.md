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
per-symbol resubscribe detail). `--max-symbols N` limits collection
for testing; `--help` lists the rest.

On SIGINT every websocket sends a close frame, each shard waits for its
socket, and then the writer drains the channel, flushes the final chunk, and
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
- Per symbol, output timestamps never go backwards. Minute snapshots are
  stamped with the latest receive time the shard has processed, not the
  wall clock. Otherwise they could land after queued updates that were
  received earlier.
- A `Snapshot` is written:
  - whenever a symbol (re)syncs: on subscribe, and after a reconnect or a
    resync;
  - for every synced symbol at each wall-clock minute boundary.

  Between snapshots, applying the updates in order reproduces the book.
  `inspect` checks this. While a symbol is not synced (e.g. its socket is
  reconnecting), nothing is written for it; the next snapshot marks the gap.
- Output uses the template's `AlignedSbeWriter` with 64 MiB chunks. The
  chunk size must match the reader's, so data reaches stdout in 64 MiB pieces.

## Architecture

```
discovery loop ──(add/remove symbols)──► shard 0..N ──► writer thread ──► AlignedSbeWriter
                                          │  ▲
                              commands    ▼  │  normalized events + receive time
                                    one socket per shard
                                          │
                                 Kalshi session (encode/decode)
```

| Module | Role |
|---|---|
| `collector/exchange/` | The only exchange-specific code. `kalshi.rs`: auth, market discovery, subscribe/resubscribe, decoding into `Snapshot` / `Level` / `Desync`, sequence checks. |
| `collector/connection.rs` | One websocket. It handles connect and reconnect with backoff, sends pings, and reconnects after 30 s of silence. It timestamps each frame as soon as it is read. |
| `collector/shard.rs` | Runs ≤ `symbols_per_shard` symbols on one socket, keeping one book per symbol. It writes updates and minute snapshots, and resubscribes a symbol whose book diverges. |
| `collector/mod.rs` | The discovery loop. New symbols start collecting page by page as the listing streams in. Removals are applied only once the listing is complete, so a partial listing never unsubscribes live markets. |
| `collector/output.rs` | The writer thread, so file I/O never stalls the network tasks. An output error, such as a full disk, shuts the collector down instead of silently dropping data. |
| `bin/inspect.rs` | Reads an output file with the template's reader and checks it: no update before its symbol's first snapshot, no per-symbol timestamp going backwards, no crossed snapshot. |

**Load balancing.** Symbols are packed into shards, each with its own
socket, so a busy or broken shard doesn't affect the others. Kalshi has
about 169k open non-MVE markets (measured 2026-10-30), most of them
illiquid, so the default is 1000 markets per shard. Change it with
`--symbols-per-shard`.

**Connection budget.** Kalshi refuses the 201st concurrent websocket on an
account with HTTP 429. This limit is undocumented; I measured it. Sockets
beyond the limit keep retrying, and each time a healthy socket drops, one
of them takes the freed slot, so the healthy shard can't reconnect. The
collector therefore takes 95% of that limit (190 sockets, i.e. 190 shards)
as its budget, adjustable with `--max-connections`. Once the budget is used,
new symbols go to the least-loaded existing shard instead of opening sockets
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

## Verification

- `cargo test --release` runs 14 unit tests:
  - fixed-point parsing; book apply;
  - Kalshi decode, seq-gap recovery, the snapshot-clock filter, the
    resubscribe handshake and Ed25519 signing;
  - the shard's state machine, driven without sockets: snapshots and
    updates are written as absolute quantities; a desynced symbol goes dark
    until its next snapshot; a diverged book is resubscribed; timestamps
    stay monotonic; minute snapshots.
- Live Kalshi run of this stage (200 markets on one socket, 80 s): every
  market synced, minute snapshots at :00, SIGINT closed the socket, flushed
  and exited 0, and `inspect` found the output clean (0 orphan updates, 0
  timestamps going backwards, 0 crossed books).

## Known limitations and next steps

- One socket per shard: when it drops, its markets go dark until it
  reconnects (the gap is marked by fresh snapshots). Redundant sockets would
  remove these gaps.
- Subscriptions are not monitored: a subscription the exchange rejects, or
  one that silently stops delivering, is only fixed when the socket
  reconnects.
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
  account and may differ by tier.
