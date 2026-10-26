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

ConnectionPool.singleton("jdbc:postgresql://db.internal:5432/shop?tcpKeepAlive=true&socketTimeout=600&logServerErrorDetail=false",
  "shop", sys.env("SHOP_DB_PASSWORD"),
  ConnectionPoolSettings(initialSize = 8, maxSize = 40, connectionTimeoutMillis = 5000L, validationQuery = "SELECT 1"))
```

Make the pool **at least as large as the blocking dispatcher** (32 threads by default, step 4): every
owner and every database call of the library runs on that dispatcher, and each may hold a connection.
The library's benchmarks use 40. And remember that PostgreSQL's `max_connections` must cover the pools
of *all* your nodes.

> **Warning:** ScalikeJDBC's own default is a pool of 8 connections with a 5-second wait. Under load,
> work that waits longer than that for a connection fails, and the caller of a command gets an
> `EventHandlingError`. A node whose pool is smaller than its blocking dispatcher logs a warning
> when it starts.

The rest of that code guards against a database that stops answering: after a failover, a connection
can wait for an answer that never comes, holding a thread of the library for ever.

- `socketTimeout=600`: a read that gets no answer for 10 minutes fails. Choose it above the longest
  statement your nodes run, and run the long jobs of step 7 (index builds, backfills) from a process
  without it.
- `tcpKeepAlive=true`: the operating system notices a connection whose other end is gone.
- `validationQuery`: a connection is checked before it is handed out, so after a database restart the
  dead ones are replaced instead of failing a piece of work each. It costs a round trip per borrow.
- `logServerErrorDetail=false`: the driver's errors do not quote rows (see "Personal data and the logs"
  below).

On the database, `ALTER ROLE shop SET idle_in_transaction_session_timeout = '10min';` ends the sessions
a hung or dead process left open in a transaction. And the library's own singletons do not wait for
ever either: the outbox poller and the squash scheduler give up a read that runs longer than five
minutes (`readTimeout` in their options), log an error, and read again.

### 2. Initialize the schema, on every start

Each PostgreSQL component has an `initSchema()` that creates whatever is missing and leaves the rest
alone, so you call them on every start:

<!-- snippet: cart/ProductionSpec.scala#tables -->
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

A start also stays out of the way of long transactions, such as a backup or an organization's export.
`initSchema()` reads the catalog first and touches a table only when something is missing from it, so
an ordinary start takes no table lock at all. When it does have to change a table (the first start of a
new version, say) and a long transaction holds that table, it waits at most two seconds at a time for
the lock, about a minute in all. Then the start fails with "The start could not finish ...: it waited
... for the table's lock, which a long transaction holds". Look for that transaction in
`pg_stat_activity`, and start the node again once it has ended. Meanwhile every other node keeps
working.

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

<!-- snippet: cart/ProductionSpec.scala#singletons -->
```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.delivery.OutboxPollerActor
import org.apache.pekko.actor.Props

// eventStore and eventBus as wired before; cartContext has an automatic squash rule, carts is its command bus
val outboxPoller = Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))
val squashScheduler = Singletons.start(system, "squashScheduler",
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

A subscriber that never answers does not keep the node down. After `registrationTimeout` (2 minutes)
the bus starts without it and logs "Event bus starts without the subscriptions of: ..."; one that
stops before it answers is left out at once ("... stopped before it answered: it never subscribed").
Until every registered subscriber has answered, no event leaves the outbox; once they all did, the
owners publish what they kept again. One that never comes back keeps the outbox growing: remove it
with `RemoveSubscriber`, or restart the node.

How long is "meanwhile"? Measured on 17/10/2026 (`docs/perf/baseline-M5.md`, one machine): a new
system with two projections answered its first command after 668 ms and its projections saw the first
event after 767 ms.

### 7. After an upgrade: the online jobs

Some jobs are too heavy to run while a node starts: big indexes and backfills over every aggregate. They
run once, in the background, after the version that needs them is up. The simplest way is to let the
application run them itself, and have full builds of projections wait for them:

```scala
import io.reactivecqrs.core.jobs.{OnlineJobs, OnlineJobsSettings}
import io.reactivecqrs.core.rebuild.BuildHold

val onlineJobs = OnlineJobs.start(system, OnlineJobsSettings(eventStore, spaceOf = spaceOfOldAggregates))
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry,
  contexts, holdBuilds = Some(BuildHold.untilStoreReady(eventStore))))
```

`OnlineJobs.start` (on every node; in a cluster it is a singleton) runs two jobs, one after the other:

1. **`indexes`**: the event store's large indexes and each document table's index by organization;
2. **`spaces`**: the organization of aggregates stored before they had one, with your `spaceOf`
   ([chapter 16](16-organizations.md)).

Each is paced as below, and each is saved in the table `online_jobs` when it ends: a node that stops
in the middle leaves it to the next runner, which skips what is done. Only one runner works at a time (it holds a PostgreSQL advisory lock), and its
index builds run on a connection of their own whose timeout is `longStatementTimeout` (6 hours), not
your pool's `socketTimeout`, which would cut a long `CREATE INDEX CONCURRENTLY` short. Ask it how it
goes:

```scala
import io.reactivecqrs.core.jobs.OnlineJobs.{GetOnlineJobsStatus, RetryOnlineJobs}

onlineJobs ? GetOnlineJobsStatus  // OnlineJobsStatus(jobs, buildHoldReleased, readiness)
onlineJobs ? RetryOnlineJobs      // runs the failed jobs again
eventStore.releaseBuildHold()     // the override: held builds start, ready or not
```

Each job's `state` is `OnlineJobState.Waiting`, `Running`, `Done` or `Failed`, with its counts (document
tables indexed, spaces filled and left). With the hold, full builds start once the indexes are there
([chapter 10](10-rebuilding-projections.md)). A failed job
keeps them waiting, and `Builds.heldBy` names the failure; fix the cause and send `RetryOnlineJobs`
(a failed job also runs again at the next start), or, if the builds should run anyway, call
`eventStore.releaseBuildHold()`, which releases them on every node, for good. Metrics `reactivecqrs.online.job` (0 waiting, 1 running, 2 done, 3 failed) and
`reactivecqrs.online.job.progress`, by `job`, follow it, and each change of a job is one log line.

You can also run the jobs yourself, from a maintenance task:

<!-- snippet: cart/ProductionSpec.scala#maintenance -->
```scala
eventStore.buildIndexesConcurrently()          // indexes built without blocking writes
val spaces = eventStore.backfillSpaces()       // SpaceBackfill(filled, left)
```

- `buildIndexesConcurrently()` builds the indexes that a large table cannot get in a start-up
  transaction (`events (aggregate_id, version)`, `aggregates (space_id, id)` and, since 1.0's last
  steps, the aggregates' own rows by type), with PostgreSQL's `CREATE INDEX CONCURRENTLY`. It does
  nothing when they exist. Until the last one exists, a projection build on a large installation
  reads a page of aggregates in about 100 ms instead of 1 ms. Since 1.0.1 the index on
  `events (aggregate_id, version)` is unique, so no aggregate can get two events of one version: run
  it again after the upgrade to 1.0.1. If some aggregate already has such a pair, it keeps the plain
  index and logs an error, and `StoredDataCheck` names the pairs (`repeatedVersions`).
- `backfillSpaces(spaceOf)` gives aggregates stored without an organization the space of their first
  event; for old first events that do not know theirs, you pass a function that does
  ([chapter 16](16-organizations.md)). Run it after the first start of the new version (on a 0.12
  schema it does nothing), and before you rebuild a projection for one organization.
### 8. Before an upgrade: the stored-data check

Before you move a database to a new version, check that the new version can read everything in it.
`StoredDataCheck` reads every stored JSON value (events, archived events, sagas, idempotent responses
and the projection tables you name) and only reads. Run it from your application, which has all your
classes on its classpath, against a **copy** of production:

<!-- snippet: cart/ProductionSpec.scala#check -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames,
  Seq(StoredDataCheck.ProjectionTable[CartSummary]("carts_list")))
println(report.text)
```

A projection table is named as its document store is, without the `projection_` prefix (for a
managed projection, with its build's suffix, such as `_v1`). The report lists, per table and class,
the rows read and the unreadable ones with examples, the aggregates still without a space, the
events by event type version (in 1.0 they must all be 0), and the tables it did not find
(`report.missingTables`): a name you mistyped, or on a database from before 1.0, `events_archive`.
`report.unreadable` must be zero. Since 1.0.1 it also names the versions stored twice
(`report.repeatedVersions`, which keep the unique index of step 7 from being built) and, when you
pass `contexts = Seq(...)` with every aggregate context of the application, the aggregate types none
of them has (`report.typesWithoutContext`): their command buses would refuse to restore those
aggregates (`reactivecqrs.aggregate-type-check`, appendix E). Both must be empty. Since 1.0.1-RC2 it
also lists the orphan events, whose aggregate has no row of its own (`report.orphans`: how many, of
how many aggregates, and the first hundred aggregates with their event counts, classes, first and
last event ids and times). Nothing reads them; whether they go is your call.

Before you delete an old event class from your code, `StoredDataCheck.usersOf(className)` tells you
which aggregates still have events of it, in the events and in the squash archive.

## Watching it run

### Metrics

The library measures what it does and hands the numbers to you. You implement a small trait with
three methods,

<!-- snippet: cart/ProductionSpec.scala#forwardingMetrics -->
```scala
import io.reactivecqrs.core.metrics.Metrics

class ForwardingMetrics extends Metrics {
  override def count(name: String, tags: Map[String, String], amount: Long): Unit =
    () // add `amount` to your metrics system's counter `name`, with `tags`
  override def time(name: String, tags: Map[String, String], nanos: Long): Unit =
    () // record `nanos` in your timer `name`
  override def gauge(name: String, tags: Map[String, String], value: Long): Unit =
    () // set your gauge `name` to `value`
}
```

and plug it in, before or after the actors start:

<!-- snippet: cart/ProductionSpec.scala#useMetrics -->
```scala
import io.reactivecqrs.core.metrics.ReactiveMetrics

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
| `reactivecqrs.outbox.read.abandoned` | count | | reads of the outbox given up after `readTimeout`: a connection no longer answers |
| `reactivecqrs.message.too.large` | count | `class` | messages too large to cross nodes ([chapter 17](17-cluster.md)) |
| `reactivecqrs.restore.failures` | gauge | | aggregates that cannot be restored, recorded (at each outbox poll) |
| `reactivecqrs.online.job` | gauge | `job` | each online job's state: 0 waiting, 1 running, 2 done, 3 failed |
| `reactivecqrs.online.job.progress` | gauge | `job` | how far it got: document tables indexed, spaces filled |
| `reactivecqrs.builds.held` | gauge | | full builds waiting for a `BuildHold` ([chapter 10](10-rebuilding-projections.md)); 0 once it is over |

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
| `GetBuildStatus` | each managed projection | `BuildStatus`: the running build's phase and how far it got, as its builder last reported (`reportedAt`), answered at once; `heldBy` while a hold keeps it waiting |
| `GetSquashStatus` | the squash scheduler | `SquashStatus`: squashed, refused, still waiting |
| `GetOnlineJobsStatus` | the online jobs' runner | `OnlineJobsStatus`: each job's state and counts, whether the build hold was released, and what the store lacks for builds |

In a cluster, ask the poller, the scheduler and the projections through their proxies, from any
node; ask each node's bus separately.

### Logs worth an alert

| Log line starts with | From | What it means |
|---|---|---|
| "Undelivered: ..." | outbox poller | an aggregate's events waited 10 minutes for some subscriber; they are tried again every 30 minutes |
| "Undelivered after ..." or "Held for a full window after ..." | event bus | a delivery reached its 10-minute deadline; its outbox row stays |
| "Subscriber ... stopped" (an error) | event bus | a subscriber stopped and nothing replaces it; its events stay in the outbox until the next start. The same line at INFO is normal: the node shuts down, or a projection's replacement is on its way |
| "Event bus waits for the subscriptions of: ..." | event bus | at start: a registered subscriber has not answered; the node is not ready |
| "Event bus starts without the subscriptions of: ..." | event bus | a registered subscriber did not answer in `registrationTimeout`; the bus runs, but no event leaves the outbox until it answers |
| "Subscriber ... stopped before it answered" | event bus | a registered subscriber stopped at start; no event leaves the outbox until it is removed or the node restarts |
| "Event bus asked ... for its subscriptions and got a failure" | event bus | a subscriber could not answer; it is asked again at every tick, and the line gives the cause |
| "Aggregate ... cannot be restored; it is recorded" or "Building ...: ... cannot be restored; it is recorded" (warnings) | owner, builder | an aggregate your code cannot replay; once per aggregate. See "When stored data is wrong" |
| "Online job ... failed: ..." | the online jobs' runner | a job failed; builds held for it keep waiting until `RetryOnlineJobs`, the next start, or `releaseBuildHold()` |
| "Full builds of projections are held: ..." (a warning) | `Projections` | a `BuildHold` keeps builds waiting, for the reason given; "... are released" follows once it is over |
| "Event bus registered ... after its start" | event bus | a `RegisterSubscribers` came after the start: those subscribers missed what was published before |
| "Delivery waits for ..." and "... deliveries wait longer than ..." | event bus | a subscriber is slow to answer |
| "Subscribers with events held beyond their window ..." | event bus | a projection is behind; its events wait in the store |
| "Error handling command ..., errorId: [...]" | aggregate owner | a caller got `CommandHandlingError` with that error id |
| "Error persisting events, errorId: [...]" | aggregate owner | a caller got `EventHandlingError`; the line names the cause (too many events, an event too large, a database error) |
| "A ... of ... bytes is too large to be sent between nodes" | cluster serializer | a message could not cross nodes ([chapter 17](17-cluster.md)) |
| "Messages between nodes would go through ..." | readiness | the library's serializer is not bound: the node stays not ready until `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }` is in its configuration ([chapter 17](17-cluster.md)) |
| "Buffer is full" | Pekko shard regions and singleton proxies | messages were dropped while a shard or a singleton moved |

The error id in the two owner lines is the one the caller received: a support request that quotes it
leads straight to the stack trace.

### Personal data and the logs

Logs are kept for long and read by many, and no erasure reaches them ([chapter 15](15-rewriting-history.md)).
So the library keeps the values it handles out of its logs, and out of what it stores and answers about
failures:

- Its database statements run with ScalikeJDBC's SQL logging off. A statement that fails is not logged
  with its parameters, which would be whole events, documents and saga orders.
- A failure is named without the values it failed on: a `MatchError` becomes "a value of class
  ...", a PostgreSQL error its SQLState and constraint, an mpjsons error its type and position. Other
  exceptions keep their message, cut to 1 000 characters.

Your part:

- Keep personal data out of your own exceptions' messages: the library logs them, and stores them in
  `subscription_failures` when a listener fails.
- Add `logServerErrorDetail=false` to the JDBC URL, so that the driver's exceptions do not quote rows
  either.
- For the sessions your code opens, set `GlobalSettings.loggingSQLErrors = false` once at start, or open
  them with `import io.reactivecqrs.core.util.SqlLogging.withoutValues` in scope.

To diagnose a failure with its values, start a node with `-Dreactivecqrs.log-values=true`. ScalikeJDBC
then logs its statements as configured, and every failure is logged in full on the logger
`io.reactivecqrs.detail`. Turn it off again: those lines carry personal data.

### Clocks and time zones

Run every JVM on UTC (`-Duser.timezone=UTC`) and keep the nodes' clocks in sync (NTP). Events take
their time from the writing node's clock, and the outbox poller compares it with its own: a node
whose clock is minutes off makes early or late re-deliveries and give-ups, and reads "at an instant"
see its events at the wrong time. Some times (archives, snapshots, schema steps) are written in the
session's zone, which pgjdbc takes from the JVM.

### PostgreSQL

Watch, as for any busy PostgreSQL: connections in use (`pg_stat_activity`), autovacuum on the hot
tables (`pg_stat_user_tables`), and table and index sizes. The hot tables are `events_to_publish`
(the outbox: an insert and a delete per event), `subscriptions` (an update per event per projection)
and `aggregates` (an update per command). Give them a low autovacuum scale factor, so it runs often.

## When stored data is wrong

Years of history collect a few surprises: an event your handler no longer accepts, a row an old
script deleted by halves. The library never repairs such data behind your back; it finds it, tells
you, and keeps the rest running. This section is the operator's side of it;
[chapter 10](10-rebuilding-projections.md) shows what a build does meanwhile, and
[chapter 21](21-rebuilds-and-repairs.md) maps every kind of such data to its check and its repairs.

### Aggregates that cannot be restored

An aggregate cannot be restored when one of its events cannot be read, or when your event handler
throws on it (a `MatchError` for an event the code forgot, a check that old data fails). Whoever meets
it records it, in the table `restore_failures`: the aggregate's owner (for a command, a query or a
re-publication), a build of a projection (in any of its phases), or `RestoreCheck` below. Each record
holds the aggregate, its type and organization, the version and event id it failed at, the
exception's class and message (without values, as every log line), where it happened, when first
and last, how many times, and whether an operator skipped it in a build. One warning goes to the
log per aggregate, not per retry, and the gauge `reactivecqrs.restore.failures` counts them at each
outbox poll. Read them:

```scala
import io.reactivecqrs.core.eventstore.RestoreFailureFilter

eventStore.restoreFailures()                                              // all of them, the oldest first
eventStore.restoreFailures(RestoreFailureFilter(projection = Some("carts"))) // those of one projection's builds
```

A record goes when the aggregate restores again there, when it is deleted for good or purged, or when
its build is dropped. An owner that cannot restore its aggregate answers a projection asking for its
events with `CannotRestore(aggregateId, reason)`, so the projection's failure says why at once,
instead of "versions from N are missing" ten minutes later.

Before an upgrade, find them all at once, with your new code, on a copy of production:

```scala
import io.reactivecqrs.core.tools.RestoreCheck

val report = RestoreCheck.run(eventStore, Seq(cartContext))  // restores every cart with this code
println(report.text)
```

It restores every aggregate of the contexts you pass, records those that fail (and clears the records
of those that pass), and lists the first hundred ids (`report.failed`; the records say why). It reads a page of aggregates at a time; give it a
`pause` to run it on a live database.

What then? Three ways out, the first one preferred:

1. **Fix the code.** A handler that throws on old data is a bug of the handler: make it accept the
   event, deploy, and send `RetryAllFailedSubscriptions` to the projections that failed (through
   `ToBuildInProgress` for a build in progress). The aggregate restores again, the records go, and a
   blocked build switches at its next look.
2. **Skip the aggregate in one build**, when its documents can wait: `SkipAggregate(aggregateId)`
   ([chapter 10](10-rebuilding-projections.md)). The data stays; the build simply leaves the
   aggregate out, and the record says "skipped".
3. **Remove the aggregate**, when it should not exist at all. That is the next section.

### Removing an aggregate by hand

The library has no call that deletes an aggregate it cannot restore: a permanent delete is an event,
and an aggregate that cannot be restored takes no command. You remove it in one transaction, with
these statements, which name every table of the library that holds rows of one aggregate (here
`4711`; put your id in every line):

```sql
BEGIN;
-- Stop (ROLLBACK) if this lists anything: copies of the aggregate read its events.
SELECT id FROM aggregates WHERE base_id = 4711 AND id <> 4711;
DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = 4711);
DELETE FROM events WHERE aggregate_id = 4711;
DELETE FROM events_archive WHERE aggregate_id = 4711;
DELETE FROM events_to_publish WHERE aggregate_id = 4711;
DELETE FROM snapshots WHERE aggregate_id = 4711;
DELETE FROM aggregate_lineage WHERE aggregate_id = 4711 OR copied_from = 4711;
DELETE FROM history_rewrites WHERE aggregate_id = 4711;
DELETE FROM subscriptions WHERE aggregate_id = 4711;
DELETE FROM subscription_failures WHERE aggregate_id = 4711;
DELETE FROM commands_responses WHERE aggregate_id = 4711;
DELETE FROM restore_failures WHERE aggregate_id = 4711;
DELETE FROM aggregates WHERE id = 4711;                    -- its own row, and its rows of the bases it copied
COMMIT;
```

Run it while nothing writes to the aggregate (nothing can: its owner cannot restore it). The first
query must list nothing: an aggregate that others were duplicated from carries part of their
history, and removing it would break them. Afterwards, send `RetryAllFailedSubscriptions` to every
projection that failed on it (and through `ToBuildInProgress` to a build in progress): a retry of an
aggregate that no longer exists clears its failure, as for one deleted for good, and a build that
waited only for it switches at its next look, without a restart.

What it leaves: the documents your projections derived from the aggregate before it failed. Clear
them as a permanent delete would, or with a targeted rebuild of the aggregate
(`RebuildAggregates(OneAggregate(id))`, whose `clearForRebuild` removes them, and which then finds
nothing to deliver). The same statements remove orphan events (an aggregate whose row is already
gone): the `aggregates` lines then delete nothing.

> **Warning:** this is the only place where you delete events yourself. Take a backup first, check the
> aggregate id twice, and keep the output of `StoredDataCheck` and `restoreFailures` from before.

### Versions missing from the store

Each aggregate's events are numbered 1, 2, 3 and so on, with no holes: one owner writes them, in
order. A hole can only come from outside, such as a row deleted by hand years ago. A restore does not
notice (it applies the events it finds), but a build does: it cannot deliver version 27 to a read
model that never got 26. Since 1.0.2 the build fails the aggregate there, once, with
"versions from 26 to 26 are missing from the store", records it (`restoreFailures`, error class
`missing versions`) and goes on with the others; before, it asked for the missing versions again and
again, without a word. To move past the hole and apply what follows it, skip the missing versions:

```scala
cartNames ? ToBuildInProgress(SkipFailedVersion(cartId, pastMissing = true))
```

`SkipAggregate` would leave out the whole aggregate instead. Find such aggregates before an upgrade:
`RestoreCheck` lists them (`report.gaps`: each aggregate's first missing version and how many), and so
does this read-only query, which reads the whole `events` table (run it on a copy, or when the
database is quiet):

```sql
SELECT a.id, a.type_id, a.base_version - coalesce(a.squashed_version, 1) + 1 - count(DISTINCT e.version) AS missing
  FROM aggregates a
  JOIN events e ON e.aggregate_id = a.id AND e.version BETWEEN coalesce(a.squashed_version, 1) AND a.base_version
 WHERE a.id = a.base_id
 GROUP BY a.id, a.type_id, a.base_version, a.squashed_version
HAVING count(DISTINCT e.version) < a.base_version - coalesce(a.squashed_version, 1) + 1;
```

A squashed aggregate counts from its squash point; a duplicate counts its own events only.
`RestoreFailureFilter(source = Some(RestoreFailureSource.Check))` lists only what the check recorded.

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
| one rebuild of an events-based projection | about 4.8 hours | 5 749 events/s (`d96-after`) |
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
- **Plan rebuild windows from the rate:** about 5 800 events/s per build on the measured machine.
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

<!-- snippet: cart/ProductionSpec.scala#largeEvents -->
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
1.0 cluster. Rolling upgrades ([chapter 17](17-cluster.md)) start from 1.0 on. The step-by-step
runbook, to rehearse on a copy of production first, is [UPGRADE-0.12-TO-1.0.md](../UPGRADE-0.12-TO-1.0.md).
Here are the changes it asks of a host, in the order you meet them.

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
13. Use the cluster settings on every node, a single one too, with the library's serializer bound in
    them; start the poller, the squash scheduler
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
