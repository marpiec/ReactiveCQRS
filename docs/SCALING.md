# Scaling and high load

How the library behaves when the volume or the load is high: where it throttles, what it holds in
memory, how it fails or stalls, and what to do about it. The scenarios follow the questions asked
at M5: 10 000 command requests at once, aggregates of 50 000 events, a store of 100 million events,
writes that outrun the projections, and commands and events of 5 MB (section 7). Section 6 covers
the messages between the nodes of a cluster.

**How to read the numbers.**
- **Measured** numbers come from the bench reports [docs/perf/load-M5.md](perf/load-M5.md)
  (commit cd23537), [docs/perf/large-M5.md](perf/large-M5.md) (commit ee1b140) and
  [docs/perf/baseline-M4.md](perf/baseline-M4.md). Each is a single run on one
  machine: Windows 10, 12 CPU threads, OpenJDK 17, a 2 GB heap, PostgreSQL 17.4 on the same machine,
  and a JDBC pool of 40 connections. They are good for comparing runs. They are not capacity figures
  for production hardware.
- **Estimated** numbers are computed from measured ones by the arithmetic shown next to them. They
  were not measured.
- **Defaults** come from the library's code (the file is named) and from Pekko 1.7.1's
  `reference.conf` files (`pekko-remote`, `pekko-cluster-sharding`, `pekko-cluster-tools`).

## 1. Where it throttles

The library has no global admission control. Work that cannot run yet waits in the places in the
table below, and most of them are unbounded. Only three limits answer at once instead of letting
work wait: the serial queue, the command timeout and the per-aggregate limits. **A host that can
receive bursts must bound the commands it lets in** (section 2).

| Resource | Default | When it is exhausted |
| --- | --- | --- |
| PostgreSQL commits | one transaction per command (measured: 1.0) | The commit rate bounds the command rate. Measured: 11 226 commands/s on existing aggregates and 4 269/s on new ones (load-M5), with no subscribers. |
| Blocking dispatcher (`reactivecqrs.blocking-dispatcher`) | 32 threads (`reference.conf`) | Owners and every JDBC call run on it. Work waits in its queue, which is unbounded. A long restore holds a thread for its whole length. |
| JDBC pool (set up by the host) | ScalikeJDBC: 8 connections, a 5 s wait | A thread that waits more than 5 s for a connection fails its work. For a command, the caller gets `EventHandlingError`. |
| Owner mailbox | unbounded (Pekko) | The commands of one busy aggregate wait in it. Each one's 60 s timeout starts when the owner takes it. |
| Serial queue (`CommandProcessing.Serial`) | 1 000 per aggregate | Further commands are answered `CommandRejected` at once (measured, section 2). |
| Command timeout (`AggregateContext.commandTimeout`) | 60 s | The caller gets `CommandHandlingError`, and the command is never persisted afterwards (R5). |
| Id pools (`IdPool`) | 10 000 ids per pool, fetched ahead | A `FirstCommand` waits for a late pool. A failing generator answers `CommandHandlingError` (F75). |
| Events per aggregate (`PostgresEventStoreState.aggregateVersionLimit`) | 10 000 | Every command on an aggregate at the limit fails with `EventHandlingError` (section 3). |
| Event size (`eventSizeLimit`, per class `eventSizeLimitPerClassName`) | 100 000 characters of JSON | The command fails with `EventHandlingError`. Larger events cost memory and disk in proportion (section 7). |
| Event bus (`EventsBusActor`) | one actor per node | It keeps one entry per event in flight until every subscription answers, with no flow control (F93), and ends a delivery at a 10-minute deadline. |
| Projection | mailbox unbounded; up to 100 aggregates per transaction | Measured: 2 732 events/s while commands run (baseline-M4). The lag lives in memory (section 5). |
| Outbox poller (`OutboxPollerOptions`) | a poll every 30 s, at most 1 000 aggregates re-published per poll, gives up after 10 min | A large backlog drains at 1 000 aggregates per poll. Given-up rows wait for a restart (section 4). |
| Re-publication parts (`OwnerSettings.maxRepublishEvents`) | 1 000 events per message | Counted in events; the bus splits them again for a subscriber on another node (F121, section 6). |
| Subscription version cache | 10 000 aggregates (LRU) | Misses read the database. |
| Rebuild (`BuilderSettings`) | pages of 1 000, chunks of 1 000 events, 2 chunks in flight, 10 000 replay states | Measured: 6 769 events/s (baseline-M4). Evicted states are read again from snapshots (section 4). |
| Owners in memory | local: an owner idle for 2 min beyond the 200 most recent, or idle for 12 h, is stopped by a sweep every 5 min (`Owners.scala`). Sharded: Pekko passivates after 120 s idle. | Not a limit: every aggregate touched in the last minutes has an owner and its state in memory (section 8). |
| Messages between nodes (Artery) | `maximum-frame-size` 256 KiB | Pekko drops a larger message unseen. The library splits deliveries, refuses large commands and fails other large messages at once (section 6). |
| Shard region buffer | 100 000 messages per region | It holds messages while a shard is located or handed over. Beyond that, Pekko drops them. |
| Singleton proxy buffer | 1 000 messages | It holds messages while the singleton moves. Beyond that, Pekko drops them. |
| Hand-over (`handoff-timeout`, shutdown phase) | 90 s (the library's `reference.conf`; Pekko's own default is 60 s and 10 s) | The shard's commands wait in the region's buffer until the new owner runs. |

## 2. 10 000 command requests at once

Measured (load-M5, "10000 commands at once"). Every command was sent at once, with no client
waiting for an answer, and there were no subscribers:

| Target | Outcome | Last answer after | Throughput | p50 / p99 latency | Heap before / peak |
| --- | --- | --- | --- | --- | --- |
| 10 000 new aggregates (`FirstCommand`) | all succeeded | 2 342 ms | 4 269/s | 1 797 / 2 204 ms | 14 / 163 MB |
| 10 000 existing aggregates, one command each | all succeeded | 891 ms | 11 226/s | 470 / 867 ms | 34 / 118 MB |
| one aggregate, concurrent mode (F99) | all succeeded | 3 815 ms | 2 621/s | 2 136 / 3 768 ms | 35 / 106 MB |
| one aggregate, serial mode | 1 003 succeeded, 8 997 `CommandRejected` | 594 ms | — | 92 / 552 ms | 17 / 52 MB |

**How it throttles.** On many aggregates, every command is accepted. The queue forms in the
blocking dispatcher's queue and in front of the JDBC pool, so latency is roughly the number of
commands in flight divided by the commit rate (Little's law). The last of 10 000 commands was
answered after 0.9 s on existing aggregates and 2.3 s on new ones. A new aggregate costs more
because it creates an owner and inserts the aggregate's row. On one aggregate in concurrent mode, F99
runs the contended commands one at a time, so none fails on a conflict. Each one waits behind all
earlier ones instead. Serial mode keeps at most 1 000 waiting and rejects the rest at once.

**Memory.** About 15 KB per in-flight command that creates an aggregate, and 8 KB per command on an
existing aggregate (estimated: (163 − 14) MB / 10 000 and (118 − 34) MB / 10 000). The peaks include
garbage not yet collected, so these are upper bounds.

**How it fails.** Estimated, by scaling the measured rates and memory linearly:
- **100 000 at once on new aggregates** would need about 1.5 GB of heap at the peak, and the last
  answer would come after about 23 s.
- **1 000 000 at once** would need about 15 GB, more than a typical heap: the node runs out of
  memory first. Even with enough memory, the commands still queued about 60 s after the owner took
  them would get `CommandHandlingError`. At 4 269/s that is everything past roughly the first
  256 000 (60 s × 4 269/s).
- **On one aggregate in concurrent mode**, the timeout is reached at about 157 000 waiting commands
  (60 s × 2 621/s).
- **A caller whose own ask timeout is shorter than 60 s** gets `AskTimeoutException`. Its command
  may still be applied later.
- **A database that stalls for more than 5 s** fails the commands waiting for a connection with
  `EventHandlingError`.

**In a cluster** (sharded owners), each command crosses to its owner's node and its answer comes
back. Both must fit the frame (section 6). While a shard is located for the first time, or handed
over, its commands wait in the region's buffer of 100 000 messages. During a rolling restart they
wait up to the 90 s hand-over. Callers with shorter timeouts see `AskTimeoutException` even though
their commands are applied.

**Recommendations.**
- **Bound the commands in flight at the edge** of the host, before they reach the command bus. Aim
  for about the commit rate times the latency you accept. On the measured machine that is about
  10 000/s × 0.5 s = 5 000 commands in flight (estimated). Answer the excess with "busy" (HTTP
  429/503) instead of queueing it.
- **Give retried commands an idempotency key** (`IdempotentCommand`). Retry only after a timeout or
  a rejection, through any node. The key makes "applied once" hold even when the first attempt went
  through late (as `RollingUpgradeSpec` does).
- **Make the client's timeout longer than the command timeout** (60 s), or shorten the context's
  `commandTimeout`, so the library's answer arrives before the client gives up.
- **Size the JDBC pool at least as large as the blocking dispatcher** (32 threads). The bench uses
  40. PostgreSQL's `max_connections` must cover every node's pool.
- **Use serial mode for aggregates that many clients write at once.** A full queue is answered at
  once, which clients can back off from. Better still, design such hot spots away (split the
  aggregate).

## 3. Aggregates with 50 000 events

Measured (load-M5, "An aggregate with 50000 events"; events carry a 200-character note; the bench
lifts the per-aggregate limit):

| Measure | Value |
| --- | --- |
| restore by a new owner, every event | 392 ms; heap 17 → 52 MB |
| restore from its snapshot (D30) | 12 ms |
| a command after the restore | 3 ms |
| every event in one answer (`GetEventsForAggregate`) | 264 ms; heap peak 168 MB |
| that answer, serialized for another node | 23 573 KiB |
| a re-publication part of 1 000 events, serialized | 470 KiB |
| one event's delivery, serialized | 561 bytes |
| events table with its indexes, per event | 382 bytes |

baseline-M4 adds restores of 100 000 events (356 ms, heap peak 102 MB) and 500 000 events (2 088 ms,
146 MB). From a snapshot, each took 1 to 5 ms.

**The default limit stops such an aggregate at 10 000 events.** `aggregateVersionLimit` (10 000 by
default) refuses a command whose events would go past it. The caller gets `EventHandlingError`, and
the log names `TooManyEventsException`. (Before M5 one more event got through, F125.) To allow
longer histories, construct the `PostgresEventStoreState` with a larger limit. The limit applies
to every type of that store.

**Where length costs:**
- **Restore.** Without a snapshot, an owner replays every event on its first message. That happens
  after the owner was stopped for being idle, after a hand-over, and after a restart. The replay
  holds one of the 32 blocking threads and a JDBC connection for its length, and the owner takes
  no other message meanwhile. A node can restore at most about 80 such aggregates per second, and
  every other owner's database work on the node waits behind them. With snapshots, about 2 700 per
  second. Both are estimated: 32 threads / 0.392 s and 32 / 0.012 s, if 32 restores run in parallel
  at the measured speed.
- **Commands.** Length does not slow them (3 ms). The owner keeps the state, not the events.
- **`GetEventsForAggregate`** returns the whole history in one message: 168 MB of heap at the peak
  for 50 000 events. It is read on the asking node, from the store, so it never crosses nodes
  (since M5, F121).
- **Re-publication** (after an outage, or for a projection's gap) goes in parts of 1 000 events, one
  round trip each: 50 parts for the whole history. A part is 470 KiB, so the bus splits it again
  for a subscriber on another node (since M5, F121).
- **States.** Subscribers that take the state receive the whole state with every publication, so
  the state's size, not the history's length, counts there (section 6).

**Recommendations.**
- **Keep aggregates short by design.** Split by a natural boundary (a period, a document version).
  Restores, re-publications and rebuild states all grow with history length.
- **Turn on snapshots** (`SnapshotPolicy`, every 200 events and on passivation by default) for every
  type whose aggregates grow past a few thousand events.
- **Squash long histories** (D47), on request or automatically (`AutomaticSquash`). The squash event
  replaces events 1..N, so restores and re-publications start there. Reads below the squash point
  are refused.
- **Raise `aggregateVersionLimit` deliberately.** It guards against runaway aggregates.
- **Do not ask for the whole history of a long aggregate.** `GetEventsForAggregateForVersion` reads
  up to a version, on the asking node.

## 4. A store of 100 million events

Estimated from the measured numbers. None of this was run at this scale.

| Concern | Estimate | Arithmetic and assumptions |
| --- | --- | --- |
| Events table with indexes | about 38 GB | 382 bytes per event (measured; most events have a 200-character note) × 100 000 000. Scale it by your events' JSON size. |
| Writing them | about 2.5 h at full load without subscribers; about 5.8 h with two projections | 100 000 000 / 11 226 per second (load-M5); / 4 780 per second (baseline-M4) |
| One rebuild of an events-based projection | about 4.1 h | 100 000 000 / 6 769 per second (baseline-M4, where every state fit the replay cache) |
| Restoring one aggregate | unchanged | It reads by `aggregate_id`. B-tree depth grows with the logarithm of the table size. |
| Subscriptions rows | one per (projection, kind, aggregate): 30 000 000 for 10 million aggregates and 3 projections | one row per subscription per aggregate |
| Draining an outbox backlog | about 8.3 h for 1 million aggregates | 1 000 000 / 1 000 per poll × 30 s |

**What grows with the store:**
- **Rebuilds.** They take hours: the builder reads every event of the projection's types in commit
  order. The active build keeps serving while the new one builds (M4), so a rebuild costs time and
  database load, not downtime. Two problems appear only at this scale:
  - A state-listening projection replayed in commit order keeps at most 10 000 aggregate states. An
    evicted state is read again from its snapshot and the events after it, so when far more
    aggregates interleave, the build slows down by one restore per miss (not measured). Projections
    that need only the latest state build from final states instead (D62): one restore per
    aggregate.
  - The commit-order read joins every event to its aggregate to filter by type. A projection whose
    types hold a small share of the store still walks most of the store (F124).
- **The outbox.** `events_to_publish` takes an insert and a delete per event. Autovacuum must keep
  up with it, or the table and its index bloat. The outbox poller groups the *whole* outbox every
  30 s and keeps one entry per waiting aggregate in memory. That is cheap while the outbox is near
  empty (normal running) and grows with a backlog after an outage (F122). An aggregate the poller
  gave up on after 10 minutes waits for the poller's next start, even when its subscriber is back
  (F123). Subscribers' gap requests still fill aggregates that receive new events.
- **Hot tables.** `aggregates` (one update per command) and `subscriptions` (one update per event
  per projection) churn the same way.

**Recommendations.**
- **Plan rebuild windows from the rate.** About 6 800 events/s per build on the measured machine.
  Run two builds at once at most (the default).
- **Prefer final-state builds for state-only projections**, and snapshots for long aggregates.
- **Tune autovacuum** for `events_to_publish`, `subscriptions` and `aggregates`: a low scale
  factor, so it runs often on these hot tables.
- **Watch the outbox.** After a long outage of a projection, rebuilding the projection can be
  faster than draining the outbox at 1 000 aggregates per poll. Otherwise raise
  `RedeliveryPolicy.Options.maxRepublishPerPoll` while it drains.
- **Budget disk from your own events.** Size times count, plus about 190 bytes per event of row
  overhead, other columns and indexes. Estimated: the measured 382 bytes less the bench events'
  average JSON of about 190 bytes.

## 5. Writes faster than the projections

Measured (baseline-M4):
- Commands reached 4 780/s with two projections, while a projection applied 2 732 events/s.
- In a 60 s soak of 475 403 commands, the projections caught up 201 s after the load ended. The heap
  held 177 MB after GC right after the load, against 15 MB before.
- 1 000 000 events at 11 348 commands/s were all projected 330 s after the load started.

**How it behaves.** The lag lives in memory. The bus keeps one delivery per event in flight, and
each projection's mailbox holds the events themselves. There is no flow control yet (F93, planned
in M5). The soak's numbers give a few hundred bytes per event behind (estimated: 162 MB for several
hundred thousand events), so 10 million events behind would take several GB.

**How it fails.**
- Without flow control, memory grows until the lag ends or the heap runs out.
- At the 10-minute deadline, the bus ends a delivery as undelivered and leaves its outbox row. The
  projection still applies the event from its mailbox, but its acknowledgement then arrives too late
  to delete the row.
- The outbox poller re-publishes such rows at 3 and 6 minutes, gives up at 10, and logs
  "Undelivered" until its next start (F123).

**Recommendations.**
- **Watch the lag:** `GetDeliveryStatus.waiting` on each node's bus and `GetProjectionStatus` on
  each projection.
- **Keep projections cheap per event.** They apply up to 100 aggregates per transaction; listeners
  that call slow external services belong in sagas or in projections of their own.
- **Leave headroom between the command rate and the slowest projection's rate.**
- **For a projection far behind, rebuild it** rather than letting it catch up through the bus.

## 6. Messages between nodes

On a single node, messages are not serialized and have no size limit. In a cluster, every message
that crosses nodes is serialized (D45) and must fit Artery's frame: `maximum-frame-size` is 256 KiB
by default, including a header. **Pekko drops a larger message and logs an error; its sender is not
told.** Seen in `ClusterMessageSizeSpec` before F121's fix: "Discarding oversized payload … max
allowed size 262144 bytes", after which an ask timed out and a delivery stayed undelivered.

The library's limit (`MessageSize.limit`) is the frame less 8 KiB for Artery's header: 248 KiB by
default.

| Message | Crosses nodes when | Size | Since M5 (F121) |
| --- | --- | --- | --- |
| A command | the owner runs on another node (sharded) | the application's command | Over the limit, it is refused at once with `FailureResponse`, wherever its owner runs. |
| A command's answer, `GetAggregate` and `GetAggregateMinVersion` answers | the owner runs on another node | the response; the whole state | Over the limit, the asker fails at once with `MessageTooLargeException`. |
| `GetEventsForAggregate` answer | never | the whole history: 23 573 KiB for 50 000 events (measured) | Read on the asking node, from the store. |
| A delivery to a subscriber | the subscriber (a projection's singleton, a saga) runs on another node | the publication's events, plus the state for `AggregateWithType` and `AggregateWithTypeAndEvents` kinds: 561 bytes for one event (measured) | Several events go in parts of at most half the limit. A single event or a state over the limit fails at the subscriber, and the sender logs it. |
| A re-publication part | as a delivery | up to 1 000 events: 470 KiB with 200-character notes (measured) | Split as a delivery. |
| A projection's query and its answer | the projection's singleton runs on another node | the application's query result | Over the limit, the asker fails at once with `MessageTooLargeException`. |

Every message over the limit is logged on the sending node with its class and size, at most once
per class every 10 seconds.

**Recommendations.**
- **Keep commands, responses, query answers and aggregate states that cross nodes well under
  248 KiB.** A state that subscribers take with every event is sent with every event: 1 KB states
  at 10 000 events/s are 10 MB/s per subscriber (estimated).
- **Read a large state on the asking node** with `GetAggregateAtInstant(id, now)`. It is restored
  from the store there, instead of the owner sending it from another node.
- **If larger messages are unavoidable**, raise `pekko.remote.artery.advanced.maximum-frame-size`.
  Pekko keeps up to `buffer-pool-size` (128) buffers of that size in direct memory: a 1 MiB frame
  can take up to 128 MiB (estimated). Alternatively, route large messages to dedicated paths with
  `large-message-destinations` (2 MiB frames, 32 buffers by default).
- **Size the network for deliveries.** About 561 bytes per event per remote subscriber for small
  events (measured): 10 000 events/s to two remote projections is about 11 MB/s (estimated).

## 7. Commands and events of several megabytes

Measured ([docs/perf/large-M5.md](perf/large-M5.md), commit ee1b140). Each command carries a
document of 5 000 000 random letters and digits (4.8 MB), stored in one event. The bench's event
store allows such events for that class; the library's default limit is 100 000 characters per
event.

| Measure | Value |
| --- | --- |
| 20 commands one after another, on one aggregate | p50 90 ms, max 164 ms; 31.8 MB/s of documents; heap 15 → 105 MB |
| 20 commands at once, each on its own aggregate | last answer after 890 ms; heap 117 → 358 MB |
| events table with its indexes and TOAST, per event | 5.0 MB (random text: PostgreSQL's compression gains nothing) |
| restore of an aggregate of 20 such events | 517 ms; heap 117 → 323 MB |
| every event in one answer (`GetEventsForAggregate`) | 377 ms; heap peak 324 MB |
| one event's delivery, serialized for another node | 4.8 MB, against a limit of 248 KiB |

**By default such an event is refused.** `eventSizeLimit` is 100 000 characters of JSON per event.
Over it, the command fails with `EventHandlingError` and the log names `EventTooLargeException`. A
host that needs larger events raises `eventSizeLimitPerClassName` for those classes only.

**Where size costs** (estimated from the measurements unless marked):
- **Memory per command in flight:** about 12 MB for a 5 MB document, for the command, the event's
  JSON and the copy sent to PostgreSQL (estimated: (358 − 117) MB / 20; the peak includes garbage
  not yet collected). 100 such commands at once take about 1.2 GB; 10 000 fit no heap. Bounding
  commands by count (section 2) is not enough here: bound the bytes in flight.
- **Writing:** 31.8 MB/s with one caller on the measured machine. Each event goes to the table and
  to the WAL in full, so disk bandwidth bounds it: about 10 MB written per 5 MB event (estimated).
- **Restore:** the store reads 1 000 rows per fetch (`fetchSize`), so all of an aggregate's large
  events are read at once. 20 events of 5 MB took 206 MB of heap (measured), about 10 MB per event;
  1 000 such events would need about 10 GB (estimated) and fail.
- **Batches counted in events:** a re-publication part (1 000 events), a rebuild page and chunk
  (1 000 events each), a projection's transaction (up to 100 aggregates) and the bus's window
  hold whatever their events weigh. A part of 1 000 events of 5 MB is about 5 GB (estimated, F126).
- **`GetEventsForAggregate`:** the whole history in one answer: 324 MB at the peak for 20 events
  (measured).
- **Between nodes:** one such event is 4.8 MB serialized, far over the limit. Since F121, a
  subscriber on another node receives a `MessageTooLargeException` instead and the sending node
  logs it; the projection stays behind for that aggregate until its gap is recorded as a failure
  (D20a, after `gapGiveUpAfter`, 10 minutes). For sharded types, a command over the limit is refused.
- **States and snapshots:** a state that holds the document goes with every publication to state
  subscribers, and into every snapshot.

**Recommendations.**
- **Keep documents out of events and commands (the claim-check pattern).** Store the file first,
  in an object store or a table of its own. Then send a command that names it (an id, a hash, a
  size), and keep only the reference in the event and the state. Events stay small, so restores,
  deliveries, rebuilds and messages between nodes stay cheap, and the history does not carry every
  version of every file.
- **If documents must be events**, keep each class's limit as low as it can be, and also:
  - turn on snapshots and keep the document out of the state, so a restore does not read the old
    documents;
  - lower the event store's `fetchSize` (for example to 10), or set the PostgreSQL driver's
    `adaptiveFetch=true` with a `maxResultBuffer`, so reads take a few rows at a time;
  - lower the builder's `pageSize` and `chunkEvents` (`ProjectionsSettings.builder`);
  - bound the bytes of commands in flight at the edge (for example, uploads at once × the largest
    document ≤ a quarter of the heap);
  - in a cluster, raise `pekko.remote.artery.advanced.maximum-frame-size` above the largest event.
    Pekko keeps up to 128 buffers of that size in direct memory, so 8 MiB frames can take up to
    1 GiB (estimated). Alternatively, route the projections' paths to `large-message-destinations`
    and raise `maximum-large-frame-size` (32 buffers).
- **Size disk and WAL for the documents:** 5 MB per event in the table (measured), and about as
  much again in the WAL (estimated).

## 8. Memory per node

| Holder | Size | Bound |
| --- | --- | --- |
| Owners | the state plus the actor, per aggregate touched recently | local: idle owners stopped by the 5-minute sweep; sharded: after 120 s idle |
| Commands in flight | 8 to 15 KB each, upper bound (section 2); about 12 MB for one carrying a 5 MB document (section 7) | none in the library: bound them at the edge, by bytes for large ones |
| Bus deliveries and projection mailboxes | a few hundred bytes per event behind, plus the events (section 5) | none until F93's flow control |
| `GetEventsForAggregate` answers | the whole history: 168 MB for 50 000 events (measured) | none |
| A restore | transient: 52 MB for 50 000 events, 146 MB for 500 000 (measured peaks) | one per owner restoring, at most 32 at once |
| A rebuild | 150 MB peak for 112 211 events (measured, baseline-M4) | pages, 2 chunks in flight, 10 000 replay states, each the size of a state |
| Subscription version cache | 10 000 aggregates' versions | LRU |
| Outbox poller | one entry per aggregate waiting in the outbox | none (F122) |
| Artery buffers (direct memory) | up to 128 × the frame size, and 32 × the large frame size | Pekko's buffer pools |

Measured for orientation: 15 to 35 MB of heap after GC at rest, 177 MB right after the 60 s soak,
and 168 MB at the peak of a 50 000-event read. A heap of a few GB leaves room for bursts and lag;
the owners' states set the rest (estimated).

## 9. Signals

- **`GetDeliveryStatus`** (each node's bus): deliveries waiting, the latest undelivered ones,
  and stopped subscribers.
- **`GetOutboxStatus`** (outbox poller): aggregates waiting in the outbox, and those given up.
- **`GetProjectionStatus`** (each projection): failures, aggregates being retried, and gaps.
  `GetBuildStatus` reports a build.
- **Readiness (D71):** a node is not ready while its bus waits for its subscribers.
- **Log lines:**
  - "Undelivered: …" (poller)
  - "Event bus waits for the subscriptions of: …"
  - stall lines and summaries of the bus
  - "Error persisting events, errorId …" (`EventHandlingError`)
  - "Error handling command …" (`CommandHandlingError`)
  - "… is too large to be sent between nodes" (the cluster serializer, F121), and Pekko's
    "Discarding oversized payload" for messages it did not write
  - "Buffer is full" from shard regions and singleton proxies
- **PostgreSQL:**
  - connections in use (`pg_stat_activity`)
  - autovacuum on the hot tables (`pg_stat_user_tables`)
  - table and index sizes

## 10. Issues found by this analysis

Recorded in [KNOWN-ISSUES](KNOWN-ISSUES.md):

| Issue | Planned in |
| --- | --- |
| F121: messages between nodes could exceed Artery's frame and were dropped silently | fixed in M5 |
| F122: the outbox poll groups the whole outbox every 30 s and keeps every waiting aggregate in memory | M5, with flow control |
| F123: given-up aggregates wait for the poller's next start, even when their subscriber is back | M5, with flow control |
| F124: a commit-order build walks the events of every type to find its own | M6 |
| F125: the per-aggregate limit let one event too many through | fixed in M5 |
| F126: batches counted in events (fetches, re-publication parts, rebuild pages and chunks, the bus's window) hold whatever their events weigh | M5 |
