# Working notes: cross-cutting

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (SYS-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Code read in full**
- `aggregate/AggregateOwner`, `AggregateRestorer`, `OutboxWriter`, `SquashScheduler`
- `commandhandler/AggregateCommandBusActor`, `Owners`, `Idempotency`, `CommandResponseState`
- `eventstore/PostgresEventStoreState`, `OutboxDeletes`
- `delivery/OutboxPollerActor`, `RedeliveryPolicy`, `Republish`
- `eventbus/EventsBusActor`, `FlowControl`
- `projection/ProjectionActor`
- `rebuild/Projections` (the supervisor)
- `saga/SagaRunner`, `SagaDefinition`, `PostgresSagaState`, `SagaState`
- `uid/*`, `types/TypesNamesState`
- `util/*` (`AfterCommit`, `SchemaLock`, `Backoff`, `ActorLogging`, `BlockingDispatcher`)
- `cluster/Singletons`, `Members`, `Readiness`, `MessageSize`
- `metrics/Metrics`, `core/src/main/resources/reference.conf`

**Code read in part**
- `SubscriptionsState` (lines 200–523), `SubscribableProjectionActor` (60–229)
- `PostgresDocumentStore` (1–200, 455–540)
- `SpaceExport`, `SpacePurge`, `StoredDataCheck`, `EphemeralEventStoreState`
- the DDL of every schema initializer
- `ProjectionBuilder` (its `transient` check only), and the error messages of `ClusterSerializer`

**Dependency sources** (ScalikeJDBC 4.3.5 sources jar from the local Coursier cache): `GlobalSettings`, `StatementExecutor`, `ParameterBinder`, `LoggingSQLAndTimeSettings`, `Commons2ConnectionPool`, `ConnectionPoolSettings`.

**Docs**
- `SCALING.md` and `KNOWN-ISSUES.md`, whole.
- The archive entries for F35, F63, F80–F92, F117, F131, F138, F143–F145.
- `ARCHITECTURE.md` §0–§5.3 and §14.
- Book chapters 02, 04, 05, 12, 15, 17 and 18 (the relevant parts).
- The suite log `examples/target/full-suite-normal.log` (run of 18/10/2026), used as real evidence.

**Not examined in depth**
- `ClusterSerializer` internals, `ProjectionBuilder` and `ProjectionRegistry`, `ReplayStates`.
- The bodies of `HistoryRewriter` and `SpaceImport`, `MemoryEventStoreState`, `SequenceGate`, `PurgedIds`, `SubscriptionsState` lines 1–199.

**Not verified by running anything** (no sbt, no database): the PostgreSQL, PgJDBC and Pekko behaviours I rely on. They come from their documented behaviour and are marked "Strongly suspected" where they decide the outcome.

## 2. Findings

### SYS-1 — Personal data from events, documents and saga orders reaches logs and `subscription_failures`, which erasure cannot reach
- **Severity:** High · **Likelihood:** Likely · **Confidence:** Confirmed (code and the repo's own suite log)
- **Location:**
  - ScalikeJDBC 4.3.5: `GlobalSettings.loggingSQLErrors = true` (`GlobalSettings.scala:22`). `StatementExecutor.scala:441-465` (LoggingSQLIfFailed) logs the SQL with its parameters inlined. Strings are cut at `maxColumnSize = Some(100)`; any other value is printed whole through `toString` (`StatementExecutor.scala:40-110`), so a `java.sql.Array` of events is printed in full.
  - `PostgresEventStoreState.scala:105-113`: the events are bound as `array("text", serialized)`.
  - `ProjectionActor.scala:762-764`, `774-783`, `587`; `EventsBusActor.scala:475-476`.
  - `SubscriptionsState.scala:275-283`: `error` is stored, up to 4 000 characters.
  - `SagaRunner.scala:145`, `204`, `207`: `order = ${saga.order}`, and any unknown message.
  - `ActorLogging.scala:25`: `s"Error while handling $o ..."`. `AggregateCommandBusActor.scala:235`.
- **Preconditions:**
  - The defaults of ScalikeJDBC. The library, the book and the examples never change them.
  - PgJDBC's default `logServerErrorDetail=true` (seen in the log below).
  - Events, documents and saga orders hold personal data.
- **Triggering scenario:** any SQL failure while persisting or projecting. Several are routine:
  - two nodes' owners of a `Local` type writing one aggregate ("Concurrent aggregate modification exception");
  - a race on a chosen id (F120, `aggregates_pk`);
  - undo or duplication refusals;
  - a DB error in the middle of a statement;
  - a projection's unique index violated (for example, on an e-mail field);
  - a saga revert failing.
- **Failure mechanism:** taken from the repo's own suite log (test data):
  - `full-suite-normal.log:3178`: `ERROR scalikejdbc.StatementExecutor ... [SQL Execution Failed] SELECT add_events(1, {"0"}, 500000000035, ..., {"{\"name\":\"Groceries\"}"}, null::jsonb) (Reason: ... "aggregates_pk"`, with the whole event array.
  - Lines 2517-2529: the document and `Detail: Key ((document #>> '{name}'...))=(Groceries) already exists` appear, in this order:
    - in ScalikeJDBC's ERROR line;
    - in the projection's WARN ("failed on ... retrying: PSQLException ... Detail");
    - in its ERROR at give-up;
    - in the bus's WARN "Subscriber ... failed on aggregate ...: <error>" (line 2619).
  - The bus's WARN is repeated for every later delivery of that aggregate, because each one is answered `MessageFailed(..., failed(key).error)` (`ProjectionActor.scala:587`).
  - The same text is stored in `subscription_failures.error`.
- **Expected impact:**
  - Personal data ends up in central log stores (with their retention and wide access) and in a table that `rewriteEverywhere` and `HistoryRewriter` (D76) do not touch.
  - The book's erasure warning (`15-rewriting-history.md:313-317`) says logs are out of reach. It does not say that the library itself writes event and document content there.
  - Hard to notice: the lines look like ordinary error diagnostics.
- **Validation:** a spec with a capturing log appender:
  - write events and documents carrying a marker value (test data only);
  - force a chosen-id race and a unique-index violation;
  - assert the marker is absent from every log line and from `subscription_failures.error`.
  - The assertion fails today.
- **Recommended fix:**
  - Run the library's statements with an implicit ScalikeJDBC `SettingsProvider` that turns `loggingSQLErrors` off. Log the library's own line instead: SQL template, SQLState and constraint name, no parameters.
  - Document `logServerErrorDetail=false` for the JDBC URL, or strip `Detail:` before logging or storing.
  - Store only the exception class, SQLState and constraint in `subscription_failures.error`, and log a `MessageFailed` once per aggregate.
  - Remove `order = ...`, message `toString` and `$o` from logs and exception texts.
  - Have the complete rewrite clear the aggregate's failure text.
  - Trade-off: less detail when debugging (the errorId still links the caller to the log).
- **Regression test:** the capture spec above.

### SYS-2 — No JDBC statement or socket timeouts: one hung connection pins an owner, a blocking thread, and the outbox poller or squash scheduler forever
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Strongly suspected (it depends on PgJDBC's defaults and on the network failure mode; the latches are confirmed in code)
- **Location:**
  - `OutboxPollerActor.scala:55, 62, 64, 71`: `case Poll => if (!reading) read()`, and `reading` is cleared only by `Read(result)`.
  - `SquashScheduler.scala:66, 79-82, 100`: the same latch.
  - `AggregateOwner.scala:339`: the command timeout is a message to self. `:495-505` persists synchronously; `:600-610` handles the timeout only when it is dequeued.
  - `Owners.scala:48, 56-57`: `keptForStopping` grows for an owner that never stops.
  - `reference.conf:9-17`: a fixed pool of 32 threads.
  - The repository contains no `queryTimeout`, `statement_timeout`, `socketTimeout` or `tcpKeepAlive`.
  - The book's pool settings (`02-setting-up.md:110-114`, `18-production.md:23-26`) set no validation query, so `testOnBorrow` stays off (`Commons2ConnectionPool`).
- **Preconditions:**
  - PgJDBC defaults (PgJDBC connection-parameter documentation): `socketTimeout=0`, meaning reads never time out, and `tcpKeepAlive=false`.
  - A DB failover or network fault that leaves connections half-open (no RST).
- **Triggering scenario:** a managed-PostgreSQL failover while the poller's `readWaitingInOutbox` and about 30 owners or projections have statements in flight.
- **Failure mechanism:**
  - Each in-flight read waits for bytes that never come.
  - The poller never clears `reading`, so re-delivery stops cluster-wide (it is a singleton). It stops silently, and its gauges freeze at their last values.
  - An owner's `CommandTimedOut` cannot be processed while its thread is blocked.
  - Each hung call pins one of the 32 blocking threads. Once the threads serving owners, projections, supervisors and JDBC futures are pinned, the node stops.
  - After the database returns, nothing frees them; only a restart does.
- **Expected impact:**
  - Callers see ask timeouts, not `CommandHandlingError`.
  - Delivery depends on live publications only until the node restarts.
  - `SCALING.md:92` ("A database that stalls for more than 5 s fails the commands waiting for a connection") is true only for threads waiting for a connection, not for statements already sent.
- **Validation:** put toxiproxy between the JVM and PostgreSQL, blackhole it during a poll and a persist, then restore it after 1 minute. Assert that the poller polls again and that the command is answered within `commandTimeout` + ε. Both fail today.
- **Recommended fix:**
  - Document and set, in the book and the examples:
    - `socketTimeout` above the longest legitimate statement;
    - `tcpKeepAlive=true`;
    - a `validationQuery`;
    - per-role `statement_timeout` and `idle_in_transaction_session_timeout`.
  - Add a watchdog to `reading`: keep a token and a start time; after N intervals, log an error, clear the flag and ignore the stale result.
  - Put a query timeout on the poller's and the scheduler's reads.
  - Answer timeouts from outside the owner (SYS-6).
  - Trade-off: timeouts must be per operation, as restores, exports and builder pages can be long.
- **Regression test:** an `EventStoreState` whose `readWaitingInOutbox` blocks forever. Assert that a later `Poll` starts a new read and logs an error.

### SYS-3 — Startup DDL takes ACCESS EXCLUSIVE and SHARE locks on hot tables at every node start, without `lock_timeout`: behind any long transaction, the whole cluster's writes queue
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Strongly suspected (PostgreSQL lock behaviour, not run here)
- **Location:**
  - `PostgresEventStoreSchemaInitializer.scala:12-41` runs at every start:
    - `:58-61` `ALTER TABLE events DROP COLUMN IF EXISTS command_id`;
    - `:64-85` `CREATE INDEX IF NOT EXISTS` on `events` and `aggregates`;
    - `:132-135`, `:156-169` `ALTER TABLE aggregates/events ADD COLUMN IF NOT EXISTS`.
  - `CommandResponseState.scala:92-99, 115-122`; `PostgresSagaSchemaInitializer.scala:18-19`.
  - `PostgresDocumentStore.scala:39-49, 82-89`, which runs in every projection constructor.
  - `SchemaLock.scala:20, 25-45`: a polling loop for up to 5 minutes, holding a thread and a connection.
  - Long transactions:
    - `SpaceExport.scala:125-141`: one transaction streams a whole space's events and writes each line to the caller's `Writer`;
    - `StoredDataCheck.usersOf` `:147-158` reads all of `events`;
    - restores of long aggregates (`PostgresEventStoreState.scala:167-207`).
- **Preconditions:** a node or projection instance starts while a transaction holds a lock on `events` or `aggregates`, or while an anti-wraparound autovacuum runs on them. That autovacuum is not cancelled by lock waiters (PostgreSQL documentation, "Routine Vacuuming").
- **Triggering scenario:** an organization export of 20 minutes (D40) while Kubernetes restarts one pod.
- **Failure mechanism:**
  - `ALTER TABLE ... IF [NOT] EXISTS` takes ACCESS EXCLUSIVE before it checks the column.
  - The request queues behind the export, and PostgreSQL makes later conflicting requests wait behind a queued one (PostgreSQL documentation, "Explicit Locking").
  - So every `add_events` and every restore on every node waits, with no timeout (SYS-2), pinning threads and connections.
  - The starting node holds `SchemaLock`. Other starting nodes, and every projection constructor, wait up to 5 minutes, each holding a blocking thread and a connection, then fail.
- **Expected impact:**
  - A cluster-wide write stall for the rest of the long transaction.
  - Nodes failing to start (crash loops).
  - Projections that fail construction (SYS-12 for unmanaged ones).
  - Visible only in `pg_locks` and `pg_stat_activity`.
- **Validation:**
  - Session A: `BEGIN; SELECT count(*) FROM events;`, kept open.
  - Thread B: `initSchema()`.
  - Thread C: time one persist.
  - Today C waits until A ends.
- **Recommended fix:**
  - Check the catalog first (`information_schema` and `pg_indexes`, which take no table locks) and run DDL only when something is missing.
  - When DDL must run, use `SET LOCAL lock_timeout` in a short transaction and retry. A `SET LOCAL` is scoped to the transaction, so it does not stay on the pooled connection, which was SchemaLock's concern.
  - Page `SpaceExport` by `events.id` in short transactions, and do not write to the caller's `Writer` inside one.
  - Trade-off: a start may need retries.
- **Regression test:** the validation above, asserting that C completes in under 1 s.

### SYS-4 — Unbounded stashes while a registered subscriber cannot answer: one node's sharded aggregates stop being delivered anywhere, and its heap grows until OOM
- **Severity:** High · **Likelihood:** Unlikely · **Confidence:** Confirmed (code) for the stash; Strongly suspected for the cluster consequence (shard allocation)
- **Location:**
  - `EventsBusActor.scala:202-233`: while registering, `case _ => stash()` (`:232`). `:251-258`: the bus starts only once every subscriber has answered.
  - `Projections.scala:278`: the supervisor's answer to `GetSubscriptionRequests` waits until its instances are known. `:280` stashes. `:606-624`: an instance that stops sends the supervisor back to starting, every 10 s, without end.
  - `Owners.scala:145-166`: the shard region starts in the command bus constructor, whatever `Readiness` says.
  - `AggregateOwner.scala:855-858`: owners publish only to their own node's bus.
- **Preconditions:**
  - A managed projection whose instance cannot be built for long: a new unique index rejected by existing documents, a bug, or SchemaLock timeouts (SYS-3).
  - A node starts meanwhile.
  - Sharded types.
- **Triggering scenario:** after a deploy of such a projection, pod B restarts or the cluster scales up.
- **Failure mechanism:**
  - B's bus waits in `registering` forever.
  - B's region still receives shards, and its owners persist and publish into the stash. Pekko's default stash capacity is unbounded.
  - The poller's re-publications go to the same owners, so they land in the stash again.
  - The supervisor stashes every query as well.
- **Expected impact:**
  - Events of the aggregates B owns reach no projection, saga or listener anywhere.
  - B's heap grows with every event and state until OOM; after the restart the same thing happens again.
  - B is "not ready", which keeps HTTP traffic away and hides that it still owns shards.
  - The only sign: B's WARN "Event bus waits for the subscriptions of: ..." every 5 s.
- **Validation:** a `TestCluster` of 2 with a managed projection whose `create` throws. Restart node B, send 10 000 commands to a sharded type, and measure B's heap and what the projections received.
- **Recommended fix:**
  - Never stash `PublishEvents` while registering: the outbox has them, so drop them or keep a small bound.
  - After a registration timeout, start and treat the missing subscriber as stopped, so its rows stay in the outbox; log an error.
  - Let the supervisor answer with its last known listeners (from the registry) or with `Status.Failure`, and fail queries instead of stashing them.
- **Regression test:** a bus with a subscriber that never answers. Publish N events, assert that memory stays bounded and that the outbox rows remain.

### SYS-5 — A commit whose outcome is unknown is answered `EventHandlingError`, which the book defines as "Nothing was stored"
- **Severity:** Medium · **Likelihood:** Possible (rare per command, systematic in each failover) · **Confidence:** Confirmed (code)
- **Location:**
  - `AggregateOwner.scala:494-505`: `Try { eventStore.localTx {...} }`. `:537-541`: `case Failure(e) => EventHandlingError`.
  - `AfterCommit.scala:24-37`: ScalikeJDBC's `DB.localTx` commits at the end of the block.
  - `docs/book/05-command-flavors.md:441` ("... or the database failed. Nothing was stored.").
- **Preconditions:** the connection breaks during COMMIT, after the server committed.
- **Triggering scenario:** a failover while about 30 commands are committing.
- **Failure mechanism:** `commit()` throws (SQLState 08xxx). The owner answers `EventHandlingError`, keeps its old version and does not publish. The events are stored nonetheless.
- **Expected impact:**
  - A client that follows the book retries:
    - a non-idempotent command is applied twice;
    - a `FirstCommand` without a chosen id creates a second aggregate.
  - The stored events wait about 3 minutes for the poller.
  - The owner's next command meets a SQL conflict and reloads its state.
- **Validation:** a `DataSource` wrapper whose `commit()` commits and then throws `SQLException("08006")`. Today the caller gets `EventHandlingError` while the rows exist.
- **Recommended fix:**
  - On a failure of `commit()` or a connection-class SQLState, check on a fresh connection whether the transaction landed: the stored version equals version + n. That is conclusive for sharded owners and for nodes that are alone; otherwise compare the events stored at those versions.
  - Answer success and publish if it landed.
  - Answer a distinct "outcome unknown" response if the check fails too, and correct the book.
- **Regression test:** the validation above, asserting success (or "outcome unknown") and a following command without conflict.

### SYS-6 — The command timeout does not bound the answer time, so a slow database turns client retries into duplicates
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `AggregateOwner.scala:339`: the timer.
  - `:400-417`: database reads per attempt on the owner's thread (`storedResponse`, `caughtUp`).
  - `:445-451`: `HandlerFinished` persists a command that is still pending, whatever the wall time.
  - `:481-543`: a synchronous persist.
  - `:600-610`: `timedOut` runs only when dequeued.
  - `SCALING.md:109` (the advice on client timeouts).
- **Preconditions:** a slow database (statements of about 1–2 s, lock waits, I/O saturation) and an aggregate with queued commands.
- **Triggering scenario:** 30 commands queued on one aggregate, at about 8 s of database work each (estimated).
- **Failure mechanism:**
  - `CommandTimedOut` waits in the mailbox behind other commands' database work.
  - A `HandlerFinished` queued before it is persisted and answered with success after 60 s.
  - Timeout errors arrive late too.
- **Expected impact:**
  - Answers after minutes (estimated: 30 × 8 s ≈ 4 min).
  - Clients that follow SCALING ("longer than 60 s") still give up and retry; without idempotency keys that means duplicates.
  - R5 holds in message order, not in time.
- **Validation:** an `EventStoreState` decorator that sleeps 2 s per call, with `commandTimeout` 5 s and 10 commands on one aggregate. Measure the answer times.
- **Recommended fix:**
  - Before opening the persist transaction, check `nanoTime - arrived > commandTimeout` and answer the timeout instead of persisting.
  - Let the command bus (or a small deadline actor) answer at the deadline; the owner then skips the persist by the same check.
  - Correct SCALING: client timeout > `commandTimeout` + the longest transaction, and retry only with idempotency keys.
- **Regression test:** as the validation, asserting each answer within `commandTimeout` + one transaction and no persist after its deadline.

### SYS-7 — The live projection path turns transient database or pool errors into durable recorded failures
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code) for the escalation; Strongly suspected for the trigger's timing
- **Location:**
  - `ProjectionActor.scala:150`: 9 attempts, about 2 minutes.
  - `:755-766`: every failure counts as an attempt.
  - `:773-792`: once a failure could not be stored, the next one gives up at once (`failedAttempts = maxAttempts - 1`).
  - `:586-587`: later deliveries are answered `MessageFailed`.
  - `:992-1003`: failures are retried only at start, at GoLive, or on request.
  - For contrast, the builder's `transient` check (F117), `ProjectionBuilder.scala:282-285`.
  - `PostgresEventStoreState.scala:492, 627`: a failed subscription blocks the aggregate's delete and squash.
- **Preconditions:** either of:
  - the pool is exhausted for more than about 2 minutes. It is shared with host threads, handler threads that read, parallel projection pools, `OutboxDeletes` and `SchemaLock`, and the docs size it like the blocking dispatcher;
  - a database outage of 2 minutes or more, after which the pool hands out stale connections (no `validationQuery`).
- **Triggering scenario:**
  - At recovery, the batch fails on a stale connection and gives up at once.
  - `recordFailure` then borrows a fresh connection and succeeds.
- **Failure mechanism:** there is no transient classification on the live path.
- **Expected impact:**
  - "Failed subscriptions" appear for listeners that are fine.
  - Their later events are answered as failed, so their outbox rows are deleted.
  - The read model stays stale for those aggregates until an operator retries or the projection restarts.
  - Squash and permanent delete of those aggregates are blocked.
  - It looks like a listener bug.
- **Validation:** a `SubscriptionsState` decorator whose `localTx` throws SQLState 08006 for 3 minutes while `recordFailure` works. Today a failure is recorded.
- **Recommended fix:**
  - Reuse `transient` in `afterBatch`: transient errors do not use up attempts (capped back-off, logged once per interval).
  - Optionally retry recorded failures automatically, rate-limited.
  - Document `validationQuery`.
- **Regression test:** the validation above, asserting that no failure is recorded and the batch is applied after recovery.

### SYS-8 — Managed projections share subscriber names, so the lag signals collide or disappear
- **Severity:** Medium · **Likelihood:** Likely (two or more managed projections) · **Confidence:** Confirmed (code)
- **Location:**
  - `Projections.scala:226, 251-254, 325`: instances are named `build-<n>-<k>` with a counter per supervisor, so every projection's first instance of build 1 is `build-1-1`.
  - `EventsBusActor.scala:348`: the gauge tag is `subscriber.path.name`.
  - `:621`: `DeliveryStatus.behind` is a `Map` keyed by `path.name`.
  - `:475, :563-565, :584-585, :593-599`: log lines; `Undelivered.subscriber`.
- **Preconditions:** none beyond the normal use of `Projections.start`.
- **Triggering scenario:** projection A is behind and projection B is healthy.
- **Failure mechanism:** the gauge is written twice per tick with the same tags, and the last write wins. One of the two `behind` entries is lost.
- **Expected impact:** the lag signals that SCALING §5 and §10 tell operators to watch are ambiguous or wrong. A's lag can be hidden by B's 0, and "Undelivered ... to build-1-1" names no projection.
- **Validation:** two managed projections, one with a slow listener. Assert `DeliveryStatus.behind.size == 2`; it fails today.
- **Recommended fix:** name instances `<projection>-build-<n>-<k>`, or label subscribers by the registered subscriber they came from (`answeredWith`). Trade-off: metric series are renamed.
- **Regression test:** the validation above.

### SYS-9 — A transient database error in `caughtUp()` (Local types on several nodes) leaves queries unanswered and serial queues stalled
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:** `AggregateOwner.scala`:
  - `:317-318`: `eventStore.storedVersion(...)` outside any `Try`;
  - callers: `:272` (`GetState`), `:815` (min-version query), `:834-841` (min-version expiry), `:407` (`attempt`), `:277-282`;
  - `:300-307`: `guarded` → `become(unrestored)`;
  - `:355`: a serial queue waits while a started command is pending.
- **Preconditions:** a `Local` type on a node that is not alone (F131's path), and a pool timeout or connection reset during the key lookup.
- **Triggering scenario:** the key lookup fails while a `GetState`, a min-version query (or its deadline), or a serial command is handled.
- **Failure mechanism:**
  - The exception skips `respondTo !`.
  - At a min-version deadline, the expired queries are neither answered nor given a new timer.
  - A started command stays pending until its timeout.
- **Expected impact:**
  - `GetState` and min-version callers see only their own ask timeout; F81's "answered at maxMillis" is broken.
  - Min-version queries can wait until the owner stops. With 200 owners or fewer that is after 12 h idle.
  - Serial aggregates stall for up to `commandTimeout`.
- **Validation:** `storedVersion` throws once, with `Members` not alone. Send `GetState`, a min-version query with a deadline, and a serial command, and assert all are answered.
- **Recommended fix:** map a failing lookup to `Some(AggregateRestoreFailedException)` inside `caughtUp()`, and always answer the expired queries.
- **Regression test:** the validation above.

### SYS-10 — Each bus tick scans and copies the whole deliveries map, which makes a lagging projection slow the node's only bus
- **Severity:** Medium · **Likelihood:** Possible (a long lag at a high rate) · **Confidence:** Hypothesis (the size of the effect is not measured)
- **Location:**
  - `EventsBusActor.scala:330-339` (Tick), `:345`, `:559` (`deliveries.filter(...).toSeq`), `:581` (`deliveries.toSeq.filter(...)` every 5 s), `:591`, `:542`, `:606`.
  - `SCALING.md:282`: up to about 6 million held keys (estimated there).
- **Preconditions:** a subscriber behind its window for minutes at a high commit rate.
- **Triggering scenario:** the documented F93 regime, with millions of held deliveries.
- **Failure mechanism:**
  - Every tick allocates a copy as large as the map and iterates it two or three times, on the single actor that routes every publication of the node.
  - With millions of entries this would take hundreds of MB per tick and seconds (estimated).
  - The bus falls behind, and its unbounded mailbox fills with `PublishEvents` carrying events and states, outside flow control.
- **Expected impact:** one lagging projection degrades delivery to all subscribers, and heap grows beyond SCALING's estimate for keys alone.
- **Validation:** a bench with 1–6 M held deliveries, measuring the time and allocation of each tick.
- **Recommended fix:** keep deliveries in structures ordered by deadline (a timing wheel), keep the held count incrementally, and never copy the whole map.
- **Regression test:** a micro-bench bounding the tick time at 5 M entries.

### SYS-11 — A type-name cache miss inside an open transaction borrows a second connection under one JVM-wide lock
- **Severity:** Low · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `TypesNamesState.scala:73-108`: `synchronized { loadAll(); DB.localTx/readOnly }`.
  - Callers inside an open transaction:
    - `PostgresEventStoreState.scala:105, 110`: the persist transaction;
    - `:193`: a restore's streaming transaction;
    - `SubscriptionsState.scala:502-504`: a listener transaction;
    - `CommandResponseState.scala:128-131`.
- **Preconditions:** a pool as large as the blocking dispatcher (32), and one of:
  - a start under load, before the cache is loaded;
  - a deploy that adds classes;
  - another node writes a class this node has not seen yet.
- **Triggering scenario:** 32 threads, each in a transaction, ask for a type id that is not cached.
- **Failure mechanism:** the thread inside the lock waits `connectionTimeoutMillis` for a second connection while the others hold theirs and queue on the lock.
- **Expected impact:** a stall of several seconds across the node and one failed command or restore per round. With a hung connection (SYS-2) the lock is held indefinitely.
- **Validation:** a pool of size 2, and two transactions asking for a new name.
- **Recommended fix:** load the cache in `initSchema()`, resolve ids before transactions open, and do not hold the lock across database I/O.
- **Regression test:** the validation above, asserting no wait.

### SYS-12 — A subscriber that dies before answering is dropped silently: the node reports ready, and the log says its events wait in the outbox when they do not
- **Severity:** Medium · **Likelihood:** Unlikely · **Confidence:** Confirmed (code)
- **Location:** `EventsBusActor.scala`:
  - `:220-223`: `Terminated` during registration, then start;
  - `:286-294`;
  - `:516`: ERROR "its events stay in the outbox until the next start";
  - `:254`: `Readiness` marked done.
- **Preconditions:**
  - An unmanaged projection (those with side effects stay unmanaged, per M4) fails in its constructor: a database hiccup in its document store's `init()`, SchemaLock's 5-minute limit (SYS-3), or an index that cannot be built.
  - Pekko stops actors that fail in their constructor.
- **Triggering scenario:** a node start during a database hiccup or a schema-lock wait.
- **Failure mechanism:** the subscriber never subscribed, so nothing waits for it and its rows are deleted once the others acknowledge.
- **Expected impact:**
  - The side effects of every event until the next restart are lost (e-mails, external calls).
  - After a restart, only aggregates that get a later event are repaired, through their gap.
  - The single ERROR line is misleading.
- **Validation:** register an actor that throws in its constructor. Assert that today the node is ready and its events are lost.
- **Recommended fix:** keep the node not ready (or the rows kept) and log that the subscriber never subscribed. Recommend starting unmanaged subscribers under a back-off supervisor.
- **Regression test:** the validation above, inverted.

## 3. Lesser findings
- **Metrics after the commit:** `AggregateOwner.scala:507-509` calls metrics after the commit and before the answer. A throwing metrics backend gets a committed command answered `CommandHandlingError` at its timeout and leaves it unpublished. The contract says "must not throw", but there is no guard.
- **Saga resume herd:** `SagaRunner.scala:95-111` starts every stored saga at once when a runner starts (a singleton move). Step timeouts can then revert sagas whose commands would have succeeded. Hypothesis.
- **`OutboxDeletes.flushNow` (`OutboxDeletes.scala:47-66`):** it deletes at most one batch of 5 000 rows. Under load, the "acknowledged rows go first" assumption of squash and delete fails, giving spurious `SquashRefused` or "not deleted yet".
- **Command bus timers:** `AggregateCommandBusActor.scala:171, 176` schedules timers that are never cancelled; the bus has no `postStop`.
- **Expired-responses clean-up (`AggregateCommandBusActor.scala:174-185`):** the DELETE runs per command bus and per node, that is types × nodes every hour, not N times as D53 says. It is one unbounded DELETE when a backlog expires.
- **`failed_since` time zone:** `SubscriptionsState.scala:281, 289` binds and reads it in the JVM's zone, unlike event times (UTC, F65). Nodes in different zones shift it.
- **Wall clock in projections:** delayed queries and subscription time-to-live use the wall clock (`ProjectionActor.scala:1093-1108, 1182-1192`; `SubscribableProjectionActor.scala:181-191`). An NTP step backwards delays parked answers by the size of the step.
- **Clock skew in `RedeliveryPolicy` (`:66-69, 75-77`):** it compares `event_time` written with other nodes' clocks against the poller's clock. A skew of 10 minutes or more gives immediate "Undelivered" errors.
- **Pool sizing advice:** the book (`02:114`, `18:23-37`) and `SCALING.md:111` count only the blocking dispatcher. Also borrowing connections: `OutboxDeletes`, SchemaLock's second connection, parallel pools (`ProjectionActor.scala:279-281`), handler threads that read, and host threads. Without a validation query, every idle connection fails one unit of work after a database restart.
- **`SubscribableProjectionActor` push cost:** O(N) per update over all subscriptions of a type, and O(N²) on a mass expiry (`:144-163, 186-228`).
- **`SpaceExport` and vacuum:** its single read transaction (`SpaceExport.scala:125-141`) holds back vacuum on hot tables (outbox, subscriptions, aggregates) for the whole export.
- **Historical reads:** `historicalRead` (`AggregateCommandBusActor.scala:394-404`) has no concurrency bound on the shared 32-thread blocking dispatcher.
- **A stopped subscriber that is not removed:** it makes every re-publication of its types futile (`incomplete`, `EventsBusActor.scala:392-393, 497-503`). The poller still restores up to 1 000 owners per poll for them, and its full `GROUP BY` (`PostgresEventStoreState.scala:826-835`) grows with the backlog.
- **Saga runner restart:** if the host's `spaceOf` throws, it restarts and silently drops the orders waiting for ids (`SagaRunner.scala:146-149, 219-223`).
- **`creation_time` time zone:** `aggregates.creation_time` and `history_rewrites.at` are written with `current_timestamp` in the session's zone, while `SpaceExport.scala:116` reads `creation_time` as UTC. Exports are shifted on non-UTC JVMs.

## 4. Known, re-checked
- **F35 (accepted):** the write retries wait 51.1 s in all (from `SagaDefinition.scala:50-51`), plus each attempt's own wait for a connection. An outage of about 1–2.5 minutes (estimated) therefore strands every saga that wrote during it until the runner restarts. There is no reload timer and no admin "resume" message, and in-flight callers get no answer.
- **F92:** the fix covers the bus, the poller and projection retries. During a database outage the flood moves elsewhere:
  - `AggregateOwner.scala:238`: an ERROR with stack trace per aggregate every 5 s while messages arrive;
  - `:539`: one per command;
  - `ProjectionActor.scala:788`: one per aggregate every 30 s;
  - ScalikeJDBC: one per failed statement, with values (SYS-1).
- **F81:** the fix is incomplete under database errors (SYS-9).
- **F131:** the book's "It costs a little" (`17-cluster.md:212`) understates the cost. With round-robin routing, each command on an aggregate written from several nodes restores it whole, which is a full replay without snapshots.
- **SCALING.md:92 and :109, book `05-command-flavors.md:441`:** these statements are inaccurate (SYS-2, SYS-6, SYS-5).

## 5. Checked and safe
- **Id generator failures** are answered and restart nothing (`AggregateCommandBusActor.scala:374-391`, F75).
- **The event bus** does no JDBC, is guarded in its running state, and its deadlines use `nanoTime`.
- **A failed owner restore** leads to failedRestore, passivation after 5 s, and a fresh restore; the owner recovers after an outage.
- **A projection failure that cannot be stored** is not answered and is retried every 30 s (F85).
- **A supervisor that cannot start** retries every 10 s; its instances are stopped, never restarted.
- **Concurrent schema initialization** is serialized by `SchemaLock` (F63); only the lock queue issue of SYS-3 remains.
- **Futures** never touch actor state, and `sender()` is captured before them (command bus, poller, scheduler, sagas, uid generator).
- **The command bus** has no realistic throw site in `receive`, and failed owners are stopped (AR7).
- **Owner deadlines, bus deadlines, flow control and owner reaping** use monotonic time.
- **Event times** are bound as UTC (F65).
- **Counters that can wrap** (`drainTokens`, `queueIds`, `spawnedCount`, `children`) are compared only by equality; the `Backoff` shift is capped at 30.
- **Failed outbox deletes** only cause duplicate re-publications, which the sequence gate drops.
- **Caches** change only after commit (F16, F18). Persist failures roll back whole (F68).
- **Shutdown:** late timers go to dead letters, sharded owners drain their commands (D72), and `OutboxDeletes` runs on a daemon thread.
- **Documented bounds hold:** the serial queue (1 000), the flow-control window, `maxTracked`, replay states, the subscription LRU, the purged-ids window, and the undelivered list (1 000).

## 6. Open questions
- What are Neula's JDBC and pool settings (`socketTimeout`, `tcpKeepAlive`, `logServerErrorDetail`, `validationQuery`) and its PostgreSQL role timeouts? Does its HA failover send RST, or go silent?
- Are `scalikejdbc.*` ERROR logs shipped centrally, with what retention and access? Do unique document indexes hold personal values (for example, e-mails)?
- How many managed projections are there, and do dashboards use `reactivecqrs.bus.behind` or `DeliveryStatus.behind`?
- Which types stay `Local` in a multi-node cluster, and is HTTP routing sticky per aggregate?
- How many sagas are pending at a runner move, and what timeouts do their steps use?
- Are `SpaceExport`, `usersOf` or `spaceRows` run on production? How often does anti-wraparound vacuum run on `events` and `aggregates`?
- What does a bus tick cost at 1–6 M deliveries (to be measured)?
- What does a slow-database fault test show (deferred at review 9)? Does the poller's 3- and 6-minute re-publication of deliveries that are merely slow amplify the load (the decision of review 4)?
- Do Neula's clients retry after `EventHandlingError` or an ask timeout without idempotency keys (H3 is still open)?
