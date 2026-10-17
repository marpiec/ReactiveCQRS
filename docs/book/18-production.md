# Chapter 18. Running It in Production

Your application works on your laptop and in the test cluster. Production is a different animal:
real traffic, real outages, real upgrades. This chapter gives you a start-up checklist, shows you how
to watch the system while it runs (metrics, logs and status questions), turns the library's load
measurements into practical advice, and ends with the list of things to do when you move to 1.0.

---

## The start-up checklist

Pilots run through a checklist before every take-off. It is short, it is boring, and it is the reason
planes do not forget their landing gear. Here is the one for a node of your application, in the order
the steps run. Chapters [2](02-setting-up.md) to [4](04-first-command.md) showed the wiring in
detail; this is the production view of it.

### 1. Set up the connection pool, first

The library uses ScalikeJDBC's default connection pool, and it must exist before any library code
runs:

```scala
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

ConnectionPool.singleton("jdbc:postgresql://db.internal:5432/shop", "shop", sys.env("SHOP_DB_PASSWORD"),
  ConnectionPoolSettings(initialSize = 8, maxSize = 40, connectionTimeoutMillis = 5000L))
```

Make the pool **at least as large as the blocking dispatcher** (32 threads by default, step 4): every
owner and every database call of the library runs on that dispatcher, and each may hold a connection.
The library's benchmarks use 40. And remember that PostgreSQL's `max_connections` must cover the pools
of *all* your nodes.

> **Warning:** ScalikeJDBC's own default is a pool of 8 connections with a 5-second wait. Under load,
> work that waits longer than that for a connection fails, and the caller of a command gets an
> `EventHandlingError`.

### 2. Initialize the schema, on every start

Each PostgreSQL component has an `initSchema()` that creates whatever is missing and leaves the rest
alone, so you call them on every start:

```scala
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
val registry = new PostgresProjectionRegistry().initSchema()
val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema() // this one returns Unit, so it gets a line of its own
```

The id sequences are created by the uid generator (`UidGeneratorActor.postgres(system)`), and each
document store creates its own table.

When several nodes start at the same moment, they do not trip over each other: every schema
initialization runs under a PostgreSQL advisory lock, one process at a time. A node waits up to five
minutes for the lock; after that its start fails with "The schema lock was not free for 5 minutes:
another process initializes the schema".

> **Warning:** do not skip `PostgresCommandResponseState.initSchema()`. It creates the unique index
> that idempotent commands rely on; without it, every idempotent command fails.

The heavy jobs (big indexes, backfills) are *not* part of `initSchema()`. They are separate calls you
run in the background (step 7), so a start never waits for them.

### 3. Set the installation number

Aggregate ids are unique across installations: installation number *n* makes its ids from
*n* × 10^12 onwards. Set the same number on every node of an installation:

```hocon
reactivecqrs.installation-number = 3
```

The default, 0, is the legacy range below 10^12, where every id made before 1.0 lives. The uid
generator made by `UidGeneratorActor.postgres(system)` moves the aggregate-id sequence forward into
the range (never back), and refuses to start when the sequence is already past the range: a wrong
number, or two installations sharing one database. If you choose ids yourself (`chosenAggregateId`,
[chapter 5](05-command-flavors.md)), take them from `ReserveAggregateId` or below 10^12; the command
bus refuses an id in another installation's range.

### 4. Size the dispatchers

The library brings two thread pools of its own, in its `reference.conf`:

| Dispatcher | What runs on it | Default |
|---|---|---|
| `reactivecqrs.blocking-dispatcher` | aggregate owners and all the library's database work | 32 threads |
| `reactivecqrs.handler-dispatcher` | your command handlers | 32 threads |

Override them in your `application.conf`:

```hocon
reactivecqrs {
  blocking-dispatcher.thread-pool-executor.fixed-pool-size = 40   # near the JDBC pool
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 64    # handlers that wait for I/O
}
```

Size the blocking dispatcher near your JDBC pool. Give the handler dispatcher more threads when your
handlers wait (for another service, for a file), and about as many as you have cores when they
compute.

Projections do database work too, so create them on the blocking dispatcher:
`Props(...).withDispatcher(BlockingDispatcher.id)` (`io.reactivecqrs.core.util.BlockingDispatcher`).
A projection left on the default dispatcher logs a warning when it starts.

### 5. Start one outbox poller and one squash scheduler

Both must run **once per system**: on a single node, create them once; in a cluster, start them with
`Singletons.start` ([chapter 17](17-cluster.md)):

```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.delivery.OutboxPollerActor
import org.apache.pekko.actor.Props

// eventStore and eventBus as wired before; cartContext has an automatic squash rule, carts is its command bus
Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))
Singletons.start(system, "squashScheduler",
  Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(cartContext, carts)))))
```

- **The outbox poller** alone re-delivers events that some subscriber has not confirmed. It polls
  first after 10 seconds, then every 30 seconds; it asks for a re-publication 3 and 6 minutes after
  an event waits, reports it "undelivered" at 10 minutes, and then tries again every 30 minutes.
  Events left over from before a restart go out at the first poll.
- **The squash scheduler** squashes the aggregates of contexts that have an automatic squash rule
  (`SquashPolicy.automatic`, [chapter 14](14-squash.md)); `SquashTarget.of` refuses a context
  without one. It first runs after a minute, then every hour, at most 100 aggregates per type per
  run, one at a time.

### 6. Register every subscriber

Send each node's event bus the list of everything that listens:
`eventBus ! RegisterSubscribers(Seq(cartItems, ...))`. The bus asks each subscriber what it wants and
publishes nothing until all of them have answered; meanwhile the node is not ready (`Readiness`,
[chapter 17](17-cluster.md)), and the bus logs "Event bus waits for the subscriptions of: ..." with
the names of the late ones.

Send one list. A second `RegisterSubscribers` before the bus has started adds to the first. One sent
after the start registers its subscribers too, but they miss what was published before they answered,
and the bus warns: "Event bus registered ... after its start". A new projection that must see the
whole history is started with `Projections.start` ([chapter 10](10-rebuilding-projections.md)), which
builds it from the stored events. A subscriber that answers the bus's question with a failure (a
managed projection whose registry is unavailable) is named in a warning and asked again at every
tick.

How long is "meanwhile"? Measured on 17/10/2026 (`docs/perf/baseline-M5.md`, one machine): a new
system with two projections answered its first command after 668 ms and its projections saw the first
event after 767 ms.

### 7. After an upgrade: the online jobs

Some jobs are too heavy to run while a node starts. You run them once, in the background, after the
version that needs them is up. Each is safe to stop and run again.

```scala
eventStore.buildIndexesConcurrently()          // indexes built without blocking writes
val spaces = eventStore.backfillSpaces()       // SpaceBackfill(filled, left)
val keys = eventStore.backfillEventKeys()      // EventKeysBackfill(written, missing)
```

- `buildIndexesConcurrently()` builds the indexes that a large table cannot get in a start-up
  transaction (`events (aggregate_id, version)`, `aggregates (space_id, id)` and, since 1.0's last
  steps, the aggregates' own rows by type), with PostgreSQL's `CREATE INDEX CONCURRENTLY`. It does
  nothing when they exist. Until the last one exists, a projection build on a large installation
  reads a page of aggregates in about 100 ms instead of 1 ms.
- `backfillSpaces(spaceOf)` gives aggregates stored without an organization the space of their first
  event; for old first events that do not know theirs, you pass a function that does
  ([chapter 16](16-organizations.md)). Run it before you rebuild a projection for one organization.
- `backfillEventKeys()` gives every older event its key (its aggregate's type and space), so a
  projection build reads only the events it needs. Run it once *every* node runs 1.0, then
  `buildIndexesConcurrently()` again. From then on, do not go back to an earlier version: earlier
  versions write events without keys.

### 8. Before an upgrade: the stored-data check

Before you move a database to a new version, check that the new version can read everything in it.
`StoredDataCheck` reads every stored JSON value (events, archived events, sagas, idempotent responses
and the projection tables you name) and only reads. Run it from your application, which has all your
classes on its classpath, against a **copy** of production:

```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames,
  Seq(StoredDataCheck.ProjectionTable[ClusterCartItems]("cluster_carts_v1")))
println(report.text)
```

A projection table is named as its document store is, without the `projection_` prefix (for a
managed projection, with its build's suffix, such as `_v1`). The report lists, per table and class,
the rows read and the unreadable ones with examples, the aggregates still without a space, and the
events by event type version (in 1.0 they must all be 0). `report.unreadable` must be zero.

Before you delete an old event class from your code, `StoredDataCheck.usersOf(className)` tells you
which aggregates still have events of it, in the events and in the squash archive.

## Watching it run

### Metrics

The library measures what it does and hands the numbers to you. You implement a small trait with
three methods and plug it in, before or after the actors start:

```scala
import io.reactivecqrs.core.metrics.{Metrics, ReactiveMetrics}

class ForwardingMetrics extends Metrics {
  override def count(name: String, tags: Map[String, String], amount: Long): Unit =
    () // add `amount` to your metrics system's counter `name`, with `tags`
  override def time(name: String, tags: Map[String, String], nanos: Long): Unit =
    () // record `nanos` in your timer `name`
  override def gauge(name: String, tags: Map[String, String], value: Long): Unit =
    () // set your gauge `name` to `value`
}

ReactiveMetrics(system).use(new ForwardingMetrics)
```

The library calls these on its own threads, often: your implementation must be quick, thread-safe,
and must never throw. Until you call `use`, nothing is measured. Tags are few and never ids, so they
are safe for any metrics system. What you get:

| Name | Kind | Tags | What it tells you |
|---|---|---|---|
| `reactivecqrs.command` | time | `type`, `command`, `outcome` | how long each command took, and how it ended (the answer's class, or `Timeout`) |
| `reactivecqrs.events.stored` | count | `type` | events stored |
| `reactivecqrs.events.stored.bytes` | count | `type` | their stored sizes |
| `reactivecqrs.restore` | time | `type`, `from` | an owner read its aggregate, from a `snapshot` or from `events` |
| `reactivecqrs.bus.waiting` | gauge | | a bus's deliveries in flight |
| `reactivecqrs.bus.held` | gauge | | deliveries held back for subscribers that are behind |
| `reactivecqrs.bus.behind` | gauge | `subscriber` | aggregates held for each subscriber; 0 when it keeps up |
| `reactivecqrs.projection.applied` | count | `projection` | versions a projection applied |
| `reactivecqrs.projection.failed` | count | `projection` | aggregates a projection gave up on ([chapter 9](09-projection-failures.md)) |
| `reactivecqrs.outbox.waiting` | gauge | | aggregates waiting in the outbox at each poll |
| `reactivecqrs.outbox.given.up` | gauge | | aggregates the poller reported undelivered |
| `reactivecqrs.outbox.republished` | count | | re-publications the poller asked for |
| `reactivecqrs.message.too.large` | count | `class` | messages too large to cross nodes ([chapter 17](17-cluster.md)) |

> **Tip:** three alerts cover most trouble: `reactivecqrs.outbox.given.up` above zero,
> `reactivecqrs.bus.behind` growing for one subscriber, and `reactivecqrs.command` with
> `outcome=Timeout` or `CommandHandlingError`.

> **Under the hood:** the full list, with when each measure is taken, is in
> [ARCHITECTURE.md, section 14](../ARCHITECTURE.md). [Chapter 19](19-testing.md) shows
> `RecordingMetrics`, which keeps the measures in memory for tests.

### Status questions

Some actors answer a status question you can ask at any time (from an admin page, a health endpoint,
a support tool):

| Ask | Whom | Answer |
|---|---|---|
| `GetDeliveryStatus` | each node's event bus | `DeliveryStatus`: deliveries in flight, the latest that ended undelivered, stopped subscribers, deliveries held and the aggregates held per subscriber |
| `GetOutboxStatus` | the outbox poller | `OutboxStatus`: aggregates waiting in the outbox, and those given up |
| `GetProjectionStatus` | each projection | `ProjectionStatus`: failed aggregates, retries and gaps ([chapter 9](09-projection-failures.md)) |
| `GetBuilds` | each managed projection | `Builds`: the active build and the one in progress ([chapter 10](10-rebuilding-projections.md)) |
| `GetBuildStatus` | each managed projection | `BuildStatus`: the running build's phase and how far it got |
| `GetSquashStatus` | the squash scheduler | `SquashStatus`: squashed, refused, still waiting |

In a cluster, ask the poller, the scheduler and the projections through their proxies, from any
node; ask each node's bus separately.

### Logs worth an alert

| Log line starts with | From | What it means |
|---|---|---|
| "Undelivered: ..." | outbox poller | an aggregate's events waited 10 minutes for some subscriber; they are tried again every 30 minutes |
| "Undelivered after ..." or "Held for a full window after ..." | event bus | a delivery reached its 10-minute deadline; its outbox row stays |
| "Subscriber ... stopped" | event bus | a subscriber stopped; its events stay in the outbox until the next start |
| "Event bus waits for the subscriptions of: ..." | event bus | at start: a registered subscriber has not answered; the node is not ready |
| "Event bus asked ... for its subscriptions and got a failure" | event bus | a subscriber could not answer; it is asked again at every tick, and the line gives the cause |
| "Event bus registered ... after its start" | event bus | a `RegisterSubscribers` came after the start: those subscribers missed what was published before |
| "Delivery waits for ..." and "... deliveries wait longer than ..." | event bus | a subscriber is slow to answer |
| "Subscribers with events held beyond their window ..." | event bus | a projection is behind; its events wait in the store |
| "Error handling command ..., errorId: [...]" | aggregate owner | a caller got `CommandHandlingError` with that error id |
| "Error persisting events, errorId: [...]" | aggregate owner | a caller got `EventHandlingError`; the line names the cause (too many events, an event too large, a database error) |
| "A ... of ... bytes is too large to be sent between nodes" | cluster serializer | a message could not cross nodes ([chapter 17](17-cluster.md)) |
| "Buffer is full" | Pekko shard regions and singleton proxies | messages were dropped while a shard or a singleton moved |

The error id in the two owner lines is the one the caller received: a support request that quotes it
leads straight to the stack trace.

### PostgreSQL

Watch, as for any busy PostgreSQL: connections in use (`pg_stat_activity`), autovacuum on the hot
tables (`pg_stat_user_tables`), and table and index sizes. The hot tables are `events_to_publish`
(the outbox: an insert and a delete per event), `subscriptions` (an update per event per projection)
and `aggregates` (an update per command). Give them a low autovacuum scale factor, so it runs often.

## Under heavy load

The library has no global brake. Work that cannot run yet waits in queues, most of them unbounded,
and only three limits answer at once instead of letting work wait: the serial queue, the command
timeout and the per-aggregate limits. This section tells you how the library behaves when things get
big, and what to do about it.

**How to read the numbers.** *Measured* numbers come from the reports in `docs/perf/`, each a single
run on 17/10/2026 on one machine: Windows 10, 12 CPU threads, OpenJDK 17 with a 2 GB heap, PostgreSQL
17.4 on the same machine, and a JDBC pool of 40 connections. They are good for comparing, not capacity
figures for your hardware. *Estimated* numbers are arithmetic on measured ones, and were not measured.

### A crowd of commands at once

Measured (`load-M5`): 10 000 commands sent at once, no subscribers.

| Target | Outcome | Last answer after | Rate |
|---|---|---|---|
| 10 000 new carts (first commands) | all succeeded | 2 342 ms | 4 269 commands/s |
| 10 000 existing carts, one command each | all succeeded | 891 ms | 11 226 commands/s |
| one cart, concurrent processing | all succeeded | 3 815 ms | 2 621 commands/s |
| one cart, serial processing | 1 003 succeeded, 8 997 `CommandRejected` at once | 594 ms | |

On many aggregates every command is accepted; they queue for threads and connections, so the last
answer comes after roughly "commands in flight ÷ commit rate". Estimated from the same run: about
15 KB of heap per command in flight that creates an aggregate and 8 KB per command on an existing one
(upper bounds), so 100 000 new carts at once would need about 1.5 GB of heap and answer the last
after about 23 seconds; a million would not fit a typical heap.

What to do:

1. **Bound the commands in flight at the edge** of your application, before they reach the command
   bus: about your commit rate times the latency you accept. On the measured machine,
   10 000/s × 0.5 s = 5 000 commands in flight (estimated). Answer the rest "busy" (HTTP 429 or 503).
2. **Give retried commands an idempotency key** ([chapter 5](05-command-flavors.md)), so a retry
   after a timeout is applied once even when the first attempt went through late.
3. **Make the client's timeout longer than the command timeout** (60 seconds by default), or shorten
   the context's `commandTimeout`, so the library's answer arrives before the client gives up.
4. **Use serial processing for aggregates that many clients write at once**: a full queue is
   answered at once, and clients can back off. Better still, split such a hot aggregate.

### A mountain of events

How a store of 100 million events would look, estimated from the measured per-event numbers (none of
this was run at that scale):

| | Estimate | From |
|---|---|---|
| events table with its indexes | about 38 GB | 382 bytes per event, measured with 200-character notes; scale by your events' JSON |
| writing them at full load, no subscribers | about 2.5 hours | 11 226 events/s (`load-M5`) |
| one rebuild of an events-based projection | about 4.1 hours | 6 769 events/s (`baseline-M4`) |
| draining an outbox backlog of 1 million aggregates | about 8.3 hours | 1 000 aggregates per 30-second poll |

When writes outrun a projection, the bus sends it at most a **window** of 10 000 events and 64 MB not
yet answered; the rest waits in the store and is pulled for the projection as it catches up. Measured
(`flow-M5` against `baseline-M4`): in a 60-second soak with 16 callers and two projections, the system
committed 671 768 commands and the projections caught up 89 seconds after the load (before the window
existed: 475 403 commands, 201 seconds). In a cluster the window is per node's bus, so with three
nodes a projection's mailbox can hold up to three windows: size `DeliveryOptions.window` and
`windowBytes` (`new EventsBusActor(DeliveryOptions(...))`) for the number of nodes.

What to do:

- **Watch the lag:** `bus.held` and `bus.behind`, `GetDeliveryStatus` and `GetProjectionStatus`.
- **Keep projections cheap per event.** A listener that calls a slow outside service belongs in a saga
  or a projection of its own. Leave headroom between your command rate and your slowest projection.
- **Rebuild a projection that is far behind** ([chapter 10](10-rebuilding-projections.md)) instead
  of letting it catch up through the outbox at 1 000 aggregates per poll; or raise the poller's pace
  while it drains:
  `OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000))`.
- **Plan rebuild windows from the rate:** about 6 800 events/s per build on the measured machine.
- **Budget disk from your own events:** their JSON size plus about 190 bytes per event (estimated).

### Aggregates with tens of thousands of events

By default an aggregate stops at **10 000 events**: a command that would go past the limit fails with
`EventHandlingError`, and the log names `TooManyEventsException`. The limit guards against runaway
aggregates; raise it deliberately, for the whole store:
`new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`.

Measured (`load-M5`, with the limit lifted): for an aggregate of 50 000 events, a new owner's restore
took 392 ms, a restore from its snapshot 12 ms, and a command after the restore 3 ms; the whole
history in one answer (`GetEventsForAggregate`) took 264 ms and 168 MB of heap at its peak. In
`baseline-M4`, 500 000 events took 2 088 ms to restore, and 1 ms from a snapshot.

Length does not slow commands down (the owner keeps the state, not the events), but it slows every
restore: after a passivation, a hand-over, a restart. Estimated: a node can restore at most about 80
aggregates of 50 000 events per second without snapshots, and about 2 700 with them.

What to do:

- **Keep aggregates short by design:** split by a natural boundary, such as a period or a document
  version.
- **Turn on snapshots** ([chapter 13](13-snapshots.md)) for every type whose aggregates grow past a
  few thousand events.
- **Squash long histories** ([chapter 14](14-squash.md)), on request or automatically.
- **Do not ask for the whole history of a long aggregate:** `GetEventsForAggregateForVersion` reads
  up to a version.

### Commands and events of several megabytes

By default an event may take **100 000 characters of JSON**; a larger one fails the command with
`EventHandlingError`, and the log names `EventTooLargeException`. You can raise the limit for chosen
classes only, with `eventSizeLimitPerClassName` (a map from class name to characters).

Measured (`large-M5`), each command carrying a document of 5 000 000 random characters (4.8 MB) in one
event:

| | Measured |
|---|---|
| 20 commands one after another, one aggregate | median 90 ms, slowest 164 ms; 31.8 MB/s of documents |
| 20 commands at once, each on its own aggregate | last answer after 890 ms; heap 117 → 358 MB |
| disk per event (table, indexes, TOAST) | 5.0 MB |
| restore of an aggregate of 20 such events | 517 ms; heap 117 → 323 MB |
| one event's delivery serialized for another node | 4.8 MB, against a limit of 248 KiB |

Estimated: about 12 MB of heap per such command in flight, so 100 at once take about 1.2 GB.

What to do:

- **Keep documents out of events and commands** (the claim-check pattern): store the file first, in
  an object store or a table of its own, then send a command that names it (an id, a hash, a size).
  Events stay small, and so do restores, deliveries, rebuilds and messages between nodes.
- **If documents must be events,** keep each class's limit as low as it can be, and:
  - turn on snapshots and keep the document out of the state;
  - lower the event store's `fetchSize` (1 000 rows by default; for example 10), or set the
    PostgreSQL driver's `adaptiveFetch=true` with a `maxResultBuffer`, so reads take a few rows at a
    time;
  - lower the builder's `pageSize` and `chunkEvents` (`ProjectionsSettings(..., builder =
    BuilderSettings(...))`);
  - bound the *bytes* of commands in flight at the edge, not just their count (for example, uploads
    at once × the largest document ≤ a quarter of the heap);
  - in a cluster, raise the frame size above the largest event ([chapter 17](17-cluster.md)).

```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
  fetchSize = 10,
  eventSizeLimitPerClassName = Map(classOf[ReceiptAttached].getName -> 6000000) // an event of your own
).initSchema()
```

### Command handlers that run for minutes

Command handlers run on the handler dispatcher, 32 threads by default. Measured (`heavy-M5`), with
handlers of 5 seconds standing in for handlers of minutes:

| Case | Outcome |
|---|---|
| 36 at once, each holding its thread | all succeeded, the last after 10.0 s (32 at a time); meanwhile the event bus answered in 8 ms, but a light command on another aggregate waited 5.4 s |
| 36 at once, the work on a pool of the application's own (`AsyncCommandResult`) | the last after 5.0 s; the bus answered in 0 ms, a light command in 3 ms |
| one, on an aggregate that light commands changed meanwhile (concurrent processing) | succeeded after 10.2 s: it conflicted once and ran again; 25 light commands waited up to 4.8 s |
| one longer than its timeout | `CommandHandlingError` at the timeout, and nothing stored |

So a 5-minute handler with the default 60-second `commandTimeout` always ends in
`CommandHandlingError`, and its events are never stored.

What to do:

- **Move long work out of the command.** A command records the request (one event); a saga or a
  worker does the work on its own threads, retried and resumable; a second command records the
  result. The aggregate stays available, and no timeout, hand-over or conflict touches the long work.
- **If a command must do it:**
  - return `AsyncCommandResult` with the work on a bounded thread pool of your own;
  - raise that context's `commandTimeout` above the longest run;
  - use serial processing for that type, so the work never runs again after a conflict;
  - give clients an idempotency key and a long enough timeout, or answer "accepted" and report the
    result later through a projection;
  - for a `Sharded` type, raise the hand-over timeouts and the process manager's grace period above
    the command timeout ([chapter 17](17-cluster.md)).

> **Under the hood:** every number above, with its arithmetic and the reasoning behind each
> recommendation, is in [SCALING.md](../SCALING.md); the raw reports are in `docs/perf/`.

## When you upgrade to 1.0

If you are coming from 0.12, the move to 1.0 is a **one-off, planned stop**: a 0.12 node cannot join a
1.0 cluster. Rolling upgrades ([chapter 17](17-cluster.md)) start from 1.0 on. A step-by-step
migration runbook, rehearsed on a copy of production, is coming with 1.0. Until then, here are the
steps the library's milestones asked of a host, in the order you meet them.

**Before the stop, on a copy of production:**

1. Run `StoredDataCheck` (step 8 of the checklist): every row must read, and every event must be at
   event type version 0.
2. Look for code that compares `Option` fields of events with `null`: with 1.0's JSON library an
   absent `Option` field reads as `None`.
3. Give every command a saga sends an idempotency key, its `SagaStep` ([chapter 12](12-sagas.md)),
   so a saga resumed after a crash never applies a step twice.

**In your code:**

4. Event versioning is gone: when an event's shape must change, add a new class (such as
   `ItemAddedV2`) and keep the old one ([chapter 6](06-events.md)).
5. Every first event names its organization's space, `SpaceId(0)` for the platform; a new aggregate
   whose first event gives `SpaceId.unknown` is refused ([chapter 16](16-organizations.md)).
6. An event class that is generic in its aggregate root takes a `ClassTag` (`[R: ClassTag]`); a type
   key you build by hand is `AggregateType.of[R]`.
7. Projections of aggregates that use undo must handle undo events: a rebuild delivers them.
8. Classes that cross nodes follow the four rules of [chapter 17](17-cluster.md) (fields added
   later are `Option`s).

**In your wiring:**

9. Call `initSchema()` on every PostgreSQL component at every start (step 2), the command response
   state and the subscriptions state included; the latter creates the table of failed subscriptions.
10. Send each event bus `RegisterSubscribers` with every projection and listener, and run one
    `OutboxPollerActor` per system.
11. Create projections on `BlockingDispatcher.id` and size the dispatchers (step 4). The id sequences
    step by 10 000 from the first start; the library sets that itself.
12. Start each read model through `Projections.start` ([chapter 10](10-rebuilding-projections.md)):
    use the `ActorRef` it returns for queries and `RegisterSubscribers`, name its tables with
    `build.documentStore(...)`, give each build caches of its own, never commit or roll back the
    session a listener is given, and retire removed projections with `Projections.retire(name)`.
    Projections with side effects stay unmanaged.
13. Use the cluster settings on every node, a single one too; start the poller, the squash scheduler
    and the saga runners with `Singletons.start`; register the projections' proxies on every bus
    ([chapter 17](17-cluster.md)).
14. Set `reactivecqrs.installation-number` (step 3), make the uid generator with
    `UidGeneratorActor.postgres(system)`, and check that no code of yours reads an id as an `Int`.
15. Set `pekko.cluster.app-version` per release, and add `ReadinessCheck` to your health checks.
16. Plug in your metrics (`ReactiveMetrics(system).use(...)`), make the JDBC pool at least 32
    connections, bound the commands in flight, and tune autovacuum for the hot tables.

**After the new version runs, in the background:**

17. `buildIndexesConcurrently()`.
18. `backfillSpaces(...)`, before you rebuild any projection for one organization.
19. Once every node runs 1.0: `backfillEventKeys()`, then `buildIndexesConcurrently()` again.

**New things you may opt into, one context at a time, whenever you like:** snapshots
([chapter 13](13-snapshots.md)), serial command processing ([chapter 5](05-command-flavors.md)),
squash and its audit archive ([chapter 14](14-squash.md)), a metadata envelope on events
(`WithMetadata`, [chapter 6](06-events.md)), rows of your own written with the events
(`WithOutboxWriter`), and `Sharded` placement ([chapter 17](17-cluster.md)).

> **Under the hood:** these steps are the host rows H1 to H11 of
> [KNOWN-ISSUES.md, section 4](../KNOWN-ISSUES.md), and the stop-and-migrate plan is decision D81.

## What you learned

- A node starts in a fixed order: the connection pool, the schema (safe on every start, one node at a
  time), the installation number, sized dispatchers, one poller and one scheduler per system, and the
  bus's subscribers. Heavy jobs run separately, in the background.
- `StoredDataCheck` tells you before an upgrade whether the new version reads everything stored.
- `ReactiveMetrics(system).use(...)` hands you the library's measures; status questions and a short
  list of log lines tell you when something waits or fails.
- Under load: bound commands at the edge, keep projections cheap, keep aggregates short (snapshots
  and squash help), keep documents out of events, and move long work out of commands.
- Every number in this chapter says where it came from: measured once on one machine, or estimated.
- Moving from 0.12 to 1.0 is a planned stop with a short list of code, wiring and data steps.

Next: [chapter 19](19-testing.md) shows how to test all of this, from a single event handler to a
cluster in one JVM.
