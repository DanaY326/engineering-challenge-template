# Latency experiments (Extension 4)

Correctness comes first, but we already run redundant sockets per shard, so
it costs little to measure which socket receives each update first and to
route output through the faster one.

## Instrumentation built into the collector

- Every frame is stamped with wall-clock and monotonic time as soon as it is
  read off the socket. Decoding happens after that.
- Each `Level` event carries a `match_key` that identifies the same exchange
  update on every connection:
  - Kalshi: `(ticker, clock, side, price, delta)`. `clock` is Kalshi's
    exchange-wide sequence, which is the same on every connection.
  - Polymarket: `(asset_id, hash, side, price, size)`.
- Each shard records, per replica:
  - **first %**: how often that replica received an update before every
    other replica;
  - **lag**: mean and max time behind the first receiver, when it was not
    first;
  - **transit**: mean of receive time minus exchange timestamp.

  Every shard logs one line per minute with these numbers.
- With `prefer_fastest` on (the default), each symbol moves to the shard's
  most-often-first replica at the minute boundary. The move costs nothing
  extra, because the minute snapshot written right after it re-baselines the
  book anyway.

## Results: Polymarket (measured 2026-10-28, from a home connection in Toronto)

Setup: 250 tokens, **6 redundant sockets** to
`wss://ws-subscriptions-clob.polymarket.com/ws/market`, all opened at the
same moment, run for 5 minutes (`--replicas 6 --no-prefer-fastest`).

| minute | r0 | r1 | r2 | r3 | r4 | r5 |
|---|---|---|---|---|---|---|
| 1 | 0% | 18% | **81%** | 0% | 0% | 0% |
| 2 | 1% | 28% | **72%** | 0% | 0% | 0% |
| 3 | 1% | 17% | **80%** | 1% | 1% | 1% |
| 4 | 1% | 13% | **82%** | 1% | 1% | 3% |
| 5 | 1% | 18% | **78%** | 1% | 0% | 2% |

*Share of matched updates each socket received first.*

When they were not first, the four slow sockets trailed by **8–14 ms on
average**, with spikes up to **290 ms**. The fastest socket trailed by
2.6–7.7 ms on the occasions it lost.

An earlier run with 2 sockets per shard on two shards showed the same
pattern: one socket was first 75–91% of the time. Once, the usually-faster
socket trailed by **103 ms on average** for a whole minute.

### Takeaways

1. **Connection speed is sticky.** Which socket was fastest stayed stable for
   the whole lifetime of the connections, so "open many connections and keep
   the fastest" works on Polymarket. All six sockets resolved to the same
   Cloudflare anycast edge (YYZ). The difference must therefore come from
   behind the edge, from whichever origin server or edge-to-origin path a
   connection is pinned to. Changing the TCP source port mostly varies the
   same thing: a new 5-tuple lands on a different backend.
2. **A practical strategy:**
   - Open more sockets than needed (e.g. 4–6 per shard).
   - Measure first-% over a few minutes.
   - Keep the best 2 as primary and hot standby, and recycle the rest to
     draw new backends.
   - Repeat whenever a socket is recycled.

   The collector implements this. `prefer_fastest` routes each symbol
   through the most-often-first replica. A replica that is first for less
   than a quarter of its fair share for 3 consecutive minutes is recycled
   early, to draw a new backend connection, via `recycle_slow_after`
   (default 3). That only happens while the shard's other replicas are
   healthy, and at most once per shard per minute.
3. **Redundancy also hides tail latency.** Even the best socket sometimes
   loses by 50–200 ms. Merging the fastest arrival across replicas per
   update, which Kalshi's `clock` makes possible, would recover those cases.
   Polymarket has no sequence numbers, so there it is harder to do safely.
4. **Clocks matter more than sockets for one-way latency.** Measured
   "transit" was about **−55 ms**: Polymarket's timestamps were ahead of this
   machine's clock. Absolute exchange-to-us latency cannot be measured
   without a disciplined clock (chrony/PTP, ideally in the exchange's
   region). Relative comparisons between sockets on the same host are
   unaffected.

## Kalshi

### First websocket measurement (2026-10-30, Toronto)

300 markets, 2 sockets per shard, 2 shards, about 95 s, matched on
Kalshi's `clock`:

| shard | r0 first | r1 first | mean lag when second | max lag |
|---|---|---|---|---|
| 0 | 40% | 60% | 0.60 / 0.82 ms | 10.9 ms |
| 1 | 60% | 40% | 0.48 / 0.57 ms | 2.0 ms |

Unlike Polymarket, the two Kalshi sockets were close to interchangeable.
Neither dominated, and the loser trailed by well under a millisecond on
average. That suggests a single fan-out tier behind the ELB rather than
backends of varying speed. This is one short sample, so the 6-socket and
ELB-pinning experiments below are still worth running. Measured "transit"
was about −110 ms: Kalshi's timestamps run ahead of this Mac's clock, the
same clock-skew caveat as above.

### Network path

- **The new host skips the CDN.** `external-api-ws.kalshi.com` (Kalshi's
  current recommended host, and our default) resolves straight to an **AWS
  ELB in `us-east-2` (Ohio)**. It has at least 5 node IPs, and `server:
  awselb/2.0`. The legacy `api.elections.kalshi.com` resolves to CloudFront
  (`65.8.x.x`). Using the new host removes a CDN hop.
- **TCP connect times varied between ELB nodes from Toronto**, from 36 ms to
  111 ms (3 samples). Different nodes may sit in different availability
  zones. Pinning connections to specific node IPs, while keeping SNI and the
  Host header set to the hostname, is a cheap experiment.
- **Host in us-east-2.** The single biggest win is presumably running the
  collector in `us-east-2`, ideally in the same AZ as the ELB nodes that
  deliver data fastest.

### Next Kalshi experiments

The collector already logs everything needed:

```sh
KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=... \
  ./target/release/engineering-challenge --exchange kalshi \
  --max-symbols 500 --replicas 6 --no-prefer-fastest -o /dev/null
```

1. **Is the fastest socket sticky on Kalshi too?** Repeat the 6-socket
   experiment above. Kalshi's `clock` gives an exact match key, so the
   comparison is precise.
2. **ELB node pinning.** Run one replica per resolved ELB IP, then compare
   first-% by IP and AZ.
3. **Connecting right after maintenance.** Kalshi has scheduled maintenance
   windows. Connect at reopening, then compare first-% against connections
   opened an hour later. If early connections land on less-loaded backends,
   they should win more often.
4. **Subscription size per socket.** Compare lag for a socket carrying 50
   active markets against one carrying 1,000. This tests whether Kalshi
   serializes per-connection fan-out, which tells us how to size shards for
   latency instead of only for limits.
5. **`sending_ts_ms − ts_ms`** shows the exchange's internal queueing. It is
   clock-skew free, because both are Kalshi timestamps.

## Other levers (not yet tried)

- `TCP_NODELAY` is already on for every websocket. Next steps are kernel
  busy-polling, pinning the network threads, and avoiding allocation on the
  hot path (the JSON decode currently allocates per message).
- A separate hot standby that stays connected but isn't subscribed, ready to
  replace a socket that drops. That cuts reconnect time (TCP + TLS +
  websocket upgrade measured ~45–80 ms to Polymarket, ~75–190 ms to Kalshi
  from here) out of the failover path.
