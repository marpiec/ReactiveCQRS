# Working notes: event delivery (second risk assessment)

- **Area:** event delivery (prefix R2-DLV)
- **Scope:** `core/.../eventbus/{EventsBusActor, FlowControl, InFlight}`, `core/.../delivery/{OutboxPollerActor, RedeliveryPolicy, Republish}`,
  `util/ReadWatch`, the outbox reads and deletes in `PostgresEventStoreState`, `PublishEventsAck` in `AggregateCommandBusActor` /
  `Owners`, the owner's `publish`/`republish`/`delivered`, `AggregateListenerActor`, and the projection's answers as far as the
  bus depends on them. Focus: the fixes since v1.0.0 (F186, F188, F191, F212, F221, F222, F228, F255, F258, F260, F264, F268).
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT)
- Nothing was run; traces from the code. Numbers are estimates unless a source is named.

## Findings

### R2-DLV-1 — Lost messages keep a subscriber's window shut for good when its traffic is on aggregates held behind (F221's escape does not cover them)
- **Medium · Unlikely (cluster only) · Confirmed (code)** for the mechanism; the message loss is **Strongly suspected** (documented Pekko behaviour).
- **Where:** `FlowControl.scala:45-49` (`admits`: the "nothing in flight" escape requires `!s.behind.contains(aggregateId)`),
  `:97-98` (`pulls` only while `s.events <= window / 2 && s.bytes <= windowBytes / 2`, and `events`/`bytes` include `presumed`),
  `:72-78` (presumed released only by an answer to a later-sent message), `:124-131` (`expire` moves sent to `presumed`);
  `ProjectionActor.scala:364-375` (a `Status.Failure(MessageTooLargeException)` delivery is not handled, so never answered).
- **Scenario:** a singleton projection S on node B takes a state type whose live traffic is a bounded hot set (a few hundred
  aggregates each updated at least once every 10 minutes). Node A's bus sends to S across nodes. A GC pause or a slow link on B
  fills Artery's outbound queue (3 072 per association, `outbound-message-queue-size`, Pekko reference.conf: "messages will be
  dropped if the queue becomes full"); a few thousand deliveries, or with F222 a few hundred state-carrying ones (> 32 MB), are
  dropped. Another source: deliveries whose state is over the frame, which arrive as `MessageTooLargeException` and are never
  answered.
- **Mechanism:**
  1. The dropped messages stay in `sent` for the deadline, then move to `presumed`; they keep counting in `events`/`bytes`.
  2. While they are over half the window, `pulls` returns nothing and live publications are held (share = 2), so every hot
     aggregate becomes and stays behind (`heldAt` is refreshed at each hold, so `expire` never drops it).
  3. `presumed` is released only by an answer to a message A sent after them. The only send still possible is the escape
     (`sent.isEmpty && pulling.isEmpty`), and it excludes aggregates behind. Poller re-publications of the held aggregates are
     held too. Gap fills help only if S sees a gap and the owner sits on A (another node's bus sends the fill otherwise).
  4. With no publication of a non-behind aggregate, nothing is ever sent or answered: a stable stall.
- **Impact:** S gets nothing more of those aggregates from A's bus until A or S restarts (or `RemoveSubscriber`); its read model
  goes stale; the outbox grows and the poller logs "Held for a full window"/"Undelivered" every 30 minutes. No data loss. With a
  varied traffic mix the stall ends at the first publication of an idle aggregate, so it is a 10-minute stall per loss instead.
  Before F221 the lost messages simply expired and pulls resumed.
- **Validation:** `FlowControlSpec`: window 10, deadline 1; `sent` 6 single-event messages (never answered); `hold` aggregate H;
  `expire(now + deadline)`; then repeat `hold(H)`, `expire`, `pulls`, `admits(H)` for many deadlines: `pulls` stays empty and
  `admits(H)` false. In `EventsBusActorSpec`: a probe subscriber that drops its first 6 messages and acks everything else; publish
  only to one aggregate; assert it is never delivered again.
- **Suggested fix:** give pulls the same escape as live publications: with `sent` and `pulling` empty, pull the oldest behind
  aggregate (one part) regardless of `presumed`; or drop `presumed` entries older than two deadlines when nothing was sent since.
- **Regression test:** the spec above, asserting a pull of H within one deadline after `sent` empties.
- **Continues:** F221 (and the first assessment's lesser item on Artery drops).

### R2-DLV-2 — Kept aggregates are re-published at 200 a second per node, outside the poller's budget: a burst of restores on every node when a late singleton answers
- **Medium · Possible · Confirmed (code)** for the rate; **Strongly suspected** for the load and heap effect (estimates).
- **Where:** `EventsBusActor.scala:195-196` (`MaxToRepublish` 100 000, `RepublishedPerTick` 1 000), `:284-292` (`republishKept`),
  `:295-301`, `:409` (every 5-s tick); `Owners.scala:53-61, 106-116` (an owner per request; idle owners stopped only after 2 minutes,
  200 per sweep); `AggregateOwner.scala:287-288` (each `RepublishOutbox`: restore, `oldestVersionToPublish`, `squashedVersion`,
  `readPart` of up to 1 000 events, a full publication).
- **Scenario:** a release adds a projection or saga singleton (F187), or a singleton starts slower than `registrationTimeout`.
  Every node's bus starts unanswered, keeps up to 100 000 aggregates, and keeps every row. When the singleton finally answers (the
  oldest node replaced last), all nodes at once send `RepublishOutbox` for 1 000 aggregates per 5-s tick: 200/s per node, about
  8 minutes for 100 000. The poller, whose budget of 1 000 per 30 s (33/s cluster-wide) exists so "a large backlog does not start and
  restore every owner at once" (`RedeliveryPolicy.scala` doc), re-publishes the same aggregates meanwhile.
- **Mechanism:** nothing bounds the bus's re-publications by the poller's budget or across nodes; each request on a passivated owner
  is a full restore plus 3-4 queries and a re-publication to every subscriber; owners pile up (at 200/s for at least 2 minutes before
  they can be swept: about 24 000+ per type and node, estimate). Aggregates whose rows another node already delivered are restored
  for nothing (`oldestVersionToPublish` returns none).
- **Impact:** at rollout end, on a 6-node cluster about 1 200 restores/s and several thousand queries/s on top of live load
  (estimate), pool and blocking-dispatcher saturation, command latency and timeouts, heap growth of the owners' states (24 000 × 10 KB
  ≈ 240 MB per type and node, estimate); duplicate deliveries to every subscriber.
- **Validation:** a bus spec with a never-answering subscriber and `registrationTimeout` 1 s; publish 10 000 aggregates; answer;
  count `RepublishOutbox` per tick at the publisher probe (1 000). Load: E2 with 3 nodes, a new singleton held back 10 minutes under
  load; measure restores/s and pool waits when it answers.
- **Suggested fix:** pace the bus at the poller's rate (for example 100 per tick), or hand the kept set to the poller (reset their
  give-ups so the next polls take them, oldest first, within its budget); skip aggregates without rows with one batched
  `SELECT DISTINCT aggregate_id FROM events_to_publish WHERE aggregate_id = ANY(?)` before asking owners.
- **Regression test:** the spec above, asserting at most the configured pace per tick.
- **Continues:** F188 / F187 (D87).

### R2-DLV-3 — A slow outbox read is abandoned and started again, so a large outbox is never polled and its scans pile up
- **Medium · Unlikely · Confirmed (code)**; read times are estimates.
- **Where:** `ReadWatch.scala:17-23` (a read past `readTimeout` is given up and a new one starts), `OutboxPollerActor.scala:68-80, 82-89`
  (the result of a given-up read is discarded; F260 adds a second full aggregation when the first returns `maxTracked`);
  `PostgresEventStoreState.scala:882-891` (`GROUP BY` over every outbox row, joined to `aggregates`); the book's
  `socketTimeout=600` (`docs/book/18-production.md:25, 43`) against `readTimeout` 5 minutes.
- **Scenario:** a subscriber unanswered or stopped for a weekend (D87, D20b): the outbox holds hundreds of millions of rows. The two
  aggregations take more than 5 minutes (estimate: a few hundred million rows at 1-5 M rows/s).
- **Mechanism:** `ReadWatch` cannot tell a slow read from a hung one. Every poll after 5 minutes abandons the running read (its
  result is ignored when it comes) and starts another; with the book's settings each runs until it ends or the 10-minute socket
  timeout, so two full scans overlap; without `socketTimeout`, one more piles up every 5 minutes, each holding a connection and a
  blocking thread.
- **Impact:** no poll completes: no re-publication, no give-up reports, frozen outbox gauges (only the "did not end" error and
  `reactivecqrs.outbox.read.abandoned` show it), exactly when the backlog is largest; extra DB load from overlapping scans.
- **Validation:** `OutboxPollerActorSpec` with `readTimeout` 1 s and a store stub whose read takes 1.5 s: no `Republish` is ever sent;
  count concurrent reads in the stub (2 or more).
- **Suggested fix:** run the read with `SET LOCAL statement_timeout` = `readTimeout` so the database ends it; never start a second read
  while one runs (give up only after the socket timeout); log the read's duration and grow the timeout from the last successful read;
  make the outbox query cheaper (an index-only aggregate, or one query for both ends).
- **Regression test:** the stub above, asserting the slow read's result is used and reads never overlap.
- **Continues:** F186, F260.

### R2-DLV-4 — The bus keeps an entry for every send for `stallAfter`, also for deliveries answered at once
- **Low · Likely (at high rates) · Confirmed (code)**; sizes are estimates.
- **Where:** `InFlight.scala:15, 36` (every new watched delivery enqueues `(key, lastSent)`), `:45-52` (entries leave only once older
  than `stallAfter`); `EventsBusActor.scala:172, 642`.
- **Mechanism:** the queue of last sends is not cleaned when a delivery completes, so it holds rate × `stallAfter` entries (Tuple2,
  boxed Long, `EventKey` with its ids: about 80-100 bytes). At 11 226 events/s (load-M5, measured rate) and the default 1 minute:
  about 670 000 entries, about 60 MB, each surviving to the old generation. A host that sets `stallAfter` to an hour keeps about
  40 M entries (about 3.5 GB). Before F212 a completed delivery left nothing behind.
- **Impact:** heap and old-generation churn proportional to throughput, not to lag; with a long `stallAfter`, out of memory.
- **Validation:** `InFlightSpec`: put and remove 1 M deliveries within one "minute"; assert the queue size (1 M today).
- **Suggested fix:** keep last sends in a `LinkedHashMap[K, Long]` (re-put moves to the end, `remove` deletes), so completed
  deliveries leave at once; or enqueue only deliveries still waiting at the next tick.
- **Regression test:** the spec above, asserting the queue is empty after the removals.
- **Continues:** F212.

### R2-DLV-5 — Measuring the state of every publication for a state listener on another node runs on the bus's only thread
- **Low · Possible · Strongly suspected** (cost estimated).
- **Where:** `EventsBusActor.scala:446-448, 453, 497` (`stateBytes` → `Measured.state`), `:520-525` (`messageSize.of(root)` writes the
  whole state through mpjsons to a null stream); `ClusterSerializer.scala` `sizeOf`.
- **Mechanism:** F222 counts the state for each remote state subscriber, measured once per publication, on the bus thread; Artery then
  serializes it again. Serialization costs about 1.6 µs/KB (estimate from `docs/perf/jsonnames-M6.md`: 0.37-0.40 µs per 104-239-byte
  event). A 50 KB state: about 80 µs per publication; at 10 000 publications/s, about 0.8 s of bus CPU per second.
- **Impact:** in a cluster with sharded owners and singleton state projections (most publications go to another node), large states at
  high rates saturate the node's single bus: every subscriber's deliveries and acks queue behind it.
- **Validation:** bench: two nodes, an `AggregateListener` singleton on node 2, 50 KB states, commands on node 1; profile the bus
  thread with and without the remote listener.
- **Suggested fix:** estimate the state's size per aggregate type from a sample (every N-th publication, or the owner's last snapshot
  size), or measure only when the subscriber's window is near full.
- **Regression test:** a bus spec counting `MessageSize.of` calls: at most one per N publications of a type.
- **Continues:** F222.

## Lesser items

- `EventsBusActor.scala:475-478`: an adopted delivery keeps `incomplete` from its one-subscriber creation (made while a subscriber was
  unanswered), so the full publication after D87's answer completes it without an ack; the rows wait for another poller round
  (30 minutes when given up). F264.
- `EventsBusActor.scala:169`: `Delivery.without` uses `filterNot` on the target sets: O(S) per answer, O(S²) per event with S
  subscriptions of a type; `waitsFor` in `wanted` (`:441`) is O(S) per subscription and event. A `- target` would do. Cliff only with
  hundreds of subscriptions per type.
- `EventsBusActor.scala:623-635`: every expired target becomes an `Undelivered` (with `Instant.now`) before the queue is cut to 100:
  millions of objects in one tick after a mass expiry. Take the last 100 first.
- `EventsBusActor.scala:453` vs `:497`: admission counts the state once per subscription, while `partsFor` sends it with each part; a
  multi-part remote `AggregatesWithEventsDelivery` is admitted below what it is then charged.
- `EventsBusActor.scala:195, 280-281`: beyond 100 000 kept aggregates, the rest wait for the poller, which by then has given them up
  (30-minute retries, at most half its budget): a long unanswered period drains at about 1 000 aggregates per 30 s (estimate: 1 M in
  about 8 h).
- `FlowControl.scala:72-76`: an answer to a message whose key was sent again releases `presumed` up to the later send's time, though
  the later copy may still be queued; small over-release.
- `OutboxPollerActor.scala:82-89`: F260's second aggregation doubles the poll's database time exactly when the outbox is large (see
  R2-DLV-3).
- `EventsBusActor.scala:356-364`: `asking` is still not cleaned on `Terminated`; an unanswered subscriber that stopped is asked every
  tick forever (dead letters). Known from the first assessment; unchanged.

## Fixes checked and found sound

- **F188 / F187 / F255 (D87):** no publication is stashed; kept aggregates bounded at 100 000; timeout start; unanswered deliveries
  incomplete; `RemoveSubscriber` and answers clear it; readiness released on start. Sound, apart from the pace (R2-DLV-2).
- **F191:** the bus logs `MessageFailed` once per subscription and aggregate (bounded set of 10 000).
- **F212:** `InFlight` keeps first-sent order (adoption re-inserts at the end with a new `firstSent`; nanoTime is monotonic), the held
  count is balanced in `put`/`remove`, stale queue entries are skipped. Correct; memory caveat R2-DLV-4.
- **F221:** accounting balanced (`sent`/`presumed`/`take`), presumed kept in send order, a stalled subscriber gets one message per
  deadline. Gap: R2-DLV-1.
- **F222:** state counted when admitted and when sent, per message, remote only. Cost: R2-DLV-5.
- **F228:** `DeliveryStatus.undelivered` and `OutboxStatus.gaveUp` capped at 100 with totals.
- **F258:** instance names carry the projection; the bus's gauge and status keys no longer collide.
- **F260:** newest `maxTracked / 2` read and de-duplicated; within its documented limit.
- **F264:** a full publication adopts publisher-less deliveries for every live subscription (sent or held), with the publisher and a
  new deadline; no premature ack found. Lesser item on `incomplete` above.
- **F268:** give-up needs the last re-publication 4 minutes old; equivalent to before for normal aggregates, fixes the late-first case.
- **F186 (poller part):** a hung read no longer stops the poller for ever; a slow one does (R2-DLV-3).
