# Working notes: cross-cutting and system level (second assessment)

- **Area:** cross-cutting and system level (prefix R2-SYS)
- **Scope:** start-up and shutdown order and host wiring (E2, the book's checklist), PostgreSQL slow, down, failing over
  or read-only, dispatchers against the pool, `Readiness`, metrics, personal data in logs, the test kit, the book's
  upgrade and operations instructions, mixed versions during the 1.0.0 → 1.0.1 rolling upgrade, compound failures.
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT)
- **Method:** read only, nothing run. Traces from the code at HEAD and at `v1.0.0`; PostgreSQL, PgJDBC and Pekko
  behaviour from their documentation (marked "Strongly suspected" where it decides the outcome). Numbers are
  estimated unless a source is named.

Read: `util/{Ddl,SchemaLock,Errors,SqlLogging,ActorLogging,BlockingDispatcher,ReadWatch}`, `cluster/{Readiness,
Singletons,Members}`, the ClusterSerializer diff since 1.0.0, `metrics/Metrics`, `EventsBusActor` (whole),
`OutboxPollerActor`, `RedeliveryPolicy`, `OutboxDeletes`, `PostgresEventStoreSchemaInitializer` (HEAD and v1.0.0),
`PostgresSagaSchemaInitializer`, `CommandResponseState` (schema), `PostgresDocumentStore` (init, indexes),
`AggregateOwner` (restore, guard, persist), `AggregateCommandBusActor` (receive), `Projections` (naming, dispatcher),
`SagaRunner` (step retries), `SagaDefinition`, `SagaActor`, `StateShape`, `UidGeneratorActor`, `PostgresUidGenerator`,
`testkit/*`, E2 (`ClusterSetup`, `e2*.conf`, `deploy/e2/*`), book chapters 16 (upgrade steps), 17 (wiring, rolling
upgrades, deployment), 18 (checklist, upgrade list), 8 and 12 (the relevant parts), CHANGELOG 1.0.1, the archive
entries of F185–F191, F196, F205, F233, F253–F258, F269, F272, and the first assessment's §10, §11, §14 and notes.

---

### R2-SYS-1 — A start that fails after the actor system exists leaves a live, never-ready member in the cluster
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code) for the stuck process and bus;
  Strongly suspected for the singleton and coordinator consequence (Pekko's documented singleton rule).
- **Where:**
  - `examples/.../e2/ClusterSetup.scala:112-117`: the pool, the actor system, Pekko Management and Cluster
    Bootstrap start before `startNode`; nothing catches a failure of `startNode` (`:42-70`).
  - Failures that `startNode` can throw: `initSchema()` (database away, `Ddl` after 30 attempts,
    `SchemaLock` after 5 minutes), `UidGeneratorActor.postgres` (F204's refusals),
    `PostgresProjectionRegistry().initSchema()` (`ProjectionRegistry.scala:90, 117`: 1.0.1's new
    `projections.pending_target` column) — the last one *after* the uid generator, the event bus and the sharded
    command bus exist.
  - Pekko's default `daemonic = off`: the actor system's threads keep the JVM alive when `main` throws.
  - `e2.conf` configures readiness checks only; Pekko Management has no liveness checks by default, so `/alive`
    (`kubernetes.yaml` livenessProbe) answers 200 for ever.
  - `EventsBusActor.scala:252`: the registration timeout (F188) runs only once `RegisterSubscribers` arrived; without
    it the bus stays in `registering` (`:264`: a warning every 5 s).
  - `Owners.scala:151-166`: the shard region starts in the command bus's constructor, so the coordinator allocates and
    rebalances shards to it whatever `Readiness` says.
- **Scenario:** a full start while the database is not reachable yet (after a site outage, during a failover or
  maintenance), or the first 1.0.1 start while `pg_dump` holds `projections` (the new column's `ALTER` gives up after
  90 s), or a wrong installation number.
- **Mechanism:**
  - `main` throws; the JVM stays up as a cluster member (Bootstrap joined), not ready, alive. Kubernetes and Docker
    restart only exited or not-alive containers, so nothing restarts it.
  - Failed before any actor: the member runs no `ClusterSingletonManager` and no shard region. Pekko runs a singleton
    (and each type's shard coordinator, itself a singleton) only on the oldest member that started its manager; when
    the stuck member is or becomes the oldest (all nodes stuck at a full start; or the others restart later), the
    outbox poller, squash scheduler, saga runners, managed projections and every sharded type's coordinator run nowhere
    in the cluster.
  - Failed after the command bus: its region takes shards; their owners accept and persist commands, publish to a bus
    that never starts; their outbox rows wait, and the poller's re-publications land on the same bus. Events of ~1/N of
    the aggregates reach no projection or saga until someone kills the pod.
- **Impact:** an outage that outlives its cause and needs a manual restart; or silently stale read models and sagas
  that never fire for a share of aggregates while commands succeed. The book (18:86-89) says the start "fails" and to
  "start the node again", which the E2 pattern never does.
- **Validation:** E2 node with `DB_URL` pointing at a closed port: the process stays, `/alive` 200, `/ready` 503,
  member Up. Second case: make `PostgresProjectionRegistry.initSchema()` throw; send commands to a sharded type from
  another node; their events never reach the carts list.
- **Suggested fix:** in E2 and the book, wrap the start: on any failure run `CoordinatedShutdown(system).run(...)`
  (with `exit-jvm = on`) or `System.exit(1)`; initialize the schema *before* creating the actor system and joining;
  start Bootstrap only after `startNode`. In the library, a liveness check that fails while `Readiness.Starting` lasts
  longer than a bound (or `Readiness.failed(reason)` that a host calls), and give the bus a registration deadline from
  its creation, not from `RegisterSubscribers`.
- **Regression test:** an E2-style start whose `initSchema` throws ends the process (exit code ≠ 0); a bus never sent
  `RegisterSubscribers` reports a liveness failure after the bound.
- **Continues:** F233 (readiness), F188 (registration timeout), CLSG-6 of the first notes.

### R2-SYS-2 — Schema-lock waiters each hold a pooled connection: with a pool as large as the blocking dispatcher, many projection starts make the lock holder fail, one after another
- **Severity** High · **Likelihood** Possible (Likely with pool = 32 and 32 or more document stores) · **Confidence**
  Confirmed (code) for the connection accounting; Strongly suspected for how many fail (a race).
- **Where:**
  - `SchemaLock.scala:25-46`: `ConnectionPool.borrow()` first, then `pg_try_advisory_lock` every 50 ms for up to
    5 minutes, holding that connection and the thread.
  - `PostgresDocumentStore.scala:72-75`: `init()` under `SchemaLock`, then `DB.autoCommit` (a second connection).
  - `Projections.scala:353`: instances (and their stores) are made on the blocking dispatcher; the book (18:135-136)
    tells hosts to do the same for unmanaged ones.
  - Pool sizes the project ships: `maxSize = 32` in book ch. 2 (`02-setting-up.md:114`), ch. 4, E1/E2
    (`BasicSetup.scala:35`), the test kit (`TestDatabase.scala:41`); H10 says "at least 32"; ch. 18 "at least as
    large as the blocking dispatcher". `PoolCheck` warns only when connections < threads.
- **Scenario:** a node (or the new oldest node after a singleton move) starts 32 or more projections; Neula has about
  60 (first assessment's estimate).
- **Mechanism:** 32 constructors on 32 threads each borrow a connection and wait for the advisory lock; the holder's
  `DB.autoCommit` finds the pool empty, waits `connectionTimeoutMillis` (3–5 s), throws; its constructor fails. The
  freed thread starts the next queued constructor, which takes the freed connection before the next lock holder polls
  (50 ms), so the cascade repeats until fewer stores wait than the pool holds: about (stores − 31) failures, one every
  3–5 s (estimated). Meanwhile no blocking thread is free for owners on that node.
- **Impact:** managed instances stop and restart after 10 s (`restartAfter`); unmanaged projections (side effects)
  are stopped for good, the bus keeps every event's outbox row for them (F255), and the poller re-publishes the growing
  backlog until the next restart. Commands on the node stall during the start.
- **Validation:** pool `maxSize = 4`, blocking dispatcher 4 threads, 8 actors on it each making a
  `PostgresDocumentStore`: some fail with "Cannot get a connection … Timeout waiting for idle object".
- **Suggested fix:** take a JVM-wide lock before borrowing (one waiter per JVM), or poll the advisory lock on a
  connection borrowed and returned per try, or run the holder's work on the lock's own connection; make `PoolCheck`
  require threads + 2 and say why.
- **Regression test:** the validation, asserting every store starts.
- **Continues:** F272, F63 (and DOCS-4 of the first notes, which covered the wait, not the pool).

### R2-SYS-3 — F185's retry loop turns one blocked schema change into 30 stalls of 2 s on the table, cluster-wide, at every restart
- **Severity** Medium (High once a release adds a column to `events` or `aggregates`) · **Likelihood** Possible ·
  **Confidence** Strongly suspected (PostgreSQL's lock queue: a waiting ACCESS EXCLUSIVE request blocks later
  requests of any mode; documented under "Explicit Locking").
- **Where:** `Ddl.scala:23-25, 51-73` (`lock_timeout` 2 s, 30 attempts, 1 s apart); 1.0.1's new columns:
  `PostgresEventStoreSchemaInitializer.scala:173` (`snapshots.shape`), `PostgresSagaSchemaInitializer.scala:21-25`,
  `ProjectionRegistry.scala:117`; book `18-production.md:84-89`.
- **Scenario:** the first 1.0.1 start while `pg_dump` (ACCESS SHARE on every table for hours) or a long export runs.
- **Mechanism:** each attempt queues `ALTER TABLE … ADD COLUMN` for 2 s; every statement on the table, from every node,
  queues behind it; then 1 s free; 90 s in all, then the start fails, under `SchemaLock`, so other nodes' schema work
  waits too. A restarting pod (CrashLoopBackOff) repeats it for as long as the backup runs.
- **Impact:** `snapshots`: restores of snapshot contexts and the owners' snapshot writes (on the owner thread) stall up to
  2 s, about 60 s in each 90 s window; `sagas`: every saga write. The book says "about a minute in all" and "Meanwhile
  every other node keeps working"; neither holds.
- **Validation:** session A `BEGIN; SELECT 1 FROM snapshots LIMIT 1;` kept open; drop the column `shape`; start
  `initSchema()`; time `readSnapshot` calls from a third session: 2-s waits every 3 s for 90 s.
- **Suggested fix:** before queuing, look in `pg_locks` for a granted conflicting lock and wait without queuing; a
  shorter `lock_timeout` (100–200 ms) with jittered, growing pauses; release `SchemaLock` between attempts; correct
  the book.
- **Regression test:** with a conflicting reader open, statements on the table never wait longer than the short
  timeout during a start.
- **Continues:** F185.

### R2-SYS-4 — A document index built at start has no bound but the driver's: with the book's `socketTimeout=600`, a build over 10 minutes never completes and is retried at every instance start
- **Severity** Medium · **Likelihood** Unlikely (an index changed on a large unmanaged table, or on a managed one
  without a new version) · **Confidence** Strongly suspected (PgJDBC closes the connection on the timeout; the server
  then rolls the open transaction back).
- **Where:** `PostgresDocumentStore.scala:72-95` (`syncIndices` under `SchemaLock`), `:161-164` (plain
  `CREATE [UNIQUE] INDEX` through `Ddl.change`, i.e. inside a transaction); book `18-production.md:43-45`
  (`socketTimeout=600`), `08-projections.md:148-151` (the advice for large tables applies to managed projections only;
  side-effect projections stay unmanaged).
- **Mechanism:** the build holds SHARE on the table (its writes stop) and `SchemaLock` (every other schema step and
  store start in the cluster waits, then fails after 5 minutes: DOCS-4). At 600 s the client gives up; the server
  finishes, finds no client, rolls back. The instance fails; a managed one restarts in 10 s and starts the build again.
- **Impact:** a projection that never starts, its table's writes blocked in 10-minute bursts, and failed starts of
  other projections and nodes for the duration.
- **Validation:** a 20 M-document table, a new `MultipleTextIndex`, `socketTimeout=60`: the instance never starts,
  `pg_indexes` never shows the index.
- **Suggested fix:** build start-time indexes with `CREATE INDEX CONCURRENTLY` outside the schema lock (drop an invalid
  leftover first, as `buildSpaceIndexesConcurrently` does), on a connection without `socketTimeout`, or refuse a missing
  index on a large table at start with a message naming a background job.
- **Regression test:** an index added to a large table is built without blocking writes and survives a short socket
  timeout.
- **Continues:** F186 (its socket timeout), F185.

### R2-SYS-5 — During the 1.0.0 → 1.0.1 rollout, a 1.0.0 node's snapshot update keeps the shape a 1.0.1 node wrote, so F196 comes back
- **Severity** High · **Likelihood** Unlikely (needs a state class changed in the same release) · **Confidence**
  Confirmed (code).
- **Where:** `v1.0.0:PostgresEventStoreState.scala:236-241` (`ON CONFLICT … DO UPDATE` sets version, context and schema
  versions, state, created; not `shape`); HEAD `PostgresEventStoreState.scala:249-260`, `AggregateRestorer.scala:92-94`
  (a snapshot is used when `s.shape == shape`); book `13-snapshots.md:166-168` ("Snapshots written by 1.0.0 have no
  shape").
- **Scenario:** the release that brings 1.0.1 also adds a field to a snapshotted state class (F196's case); a `Local`
  aggregate is snapshotted by a 1.0.1 node, then by a 1.0.0 node (round-robin routing; or a shard moving back).
- **Mechanism:** the row holds the old class's JSON with the new class's shape tag; a 1.0.1 restore takes it, and the new
  field reads as zero, false or null, until a 1.0.1 node writes the next snapshot.
- **Impact:** F196's silent wrong state, deciding commands and possibly written into events, for the aggregates touched
  by both versions.
- **Validation:** write a snapshot with shape S2 (1.0.1), then run 1.0.0's upsert SQL with an old-shape state; restore
  with the new class: it is used.
- **Suggested fix:** for this upgrade, the book says: do not change snapshotted state classes in the release that leaves
  1.0.0, or raise `schemaVersion` with them (1.0.0 writes its old value, so the snapshot is not used); or have 1.0.1 store
  shape and state together (e.g. the shape inside the `state` JSON, or a `state2` column) so an older writer cannot pair
  them.
- **Regression test:** the validation, asserting the snapshot is not used.
- **Continues:** F196.

### R2-SYS-6 — Step retries are on by default for every saga, `SagaActor`s included: with H3's unkeyed commands, the upgrade to 1.0.1 turns a timed-out step into duplicate commands
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code); duplication depends on the host's
  commands (H3: Neula's ~29 sagas send none with an idempotency key).
- **Where:** `SagaDefinition.scala:48, 62-63` (`defaultStepRetries`: 5 attempts); `SagaActor.scala:12` (inherits it);
  `SagaRunner.scala:247-257, 266`; CHANGELOG 1.0.1 ("A step whose future fails runs again with its `SagaStep` key");
  book `18-production.md:578-579` and KNOWN-ISSUES H3.
- **Scenario:** a slow database or a GC pause; the step's ask times out (asks are usually shorter than the 60-s
  `commandTimeout`), the command is applied late; or a step sends two commands and fails on the second's answer.
- **Mechanism:** in 1.0.0 the step reverted; now it runs again up to four more times with the same `SagaStep`, which
  deduplicates only commands that are `IdempotentCommand`s carrying it.
- **Impact:** commands applied twice or more (a second cart, a second charge), introduced by a library upgrade with no
  code change; nothing in the 1.0.1 notes warns that unkeyed sagas change behaviour.
- **Validation:** a `SagaActor` whose step sends a plain `FirstCommand` with an ask timeout shorter than a delayed
  persist: two aggregates are created.
- **Suggested fix:** default `stepRetries` to no retry for `SagaActor` (and for definitions that do not opt in), or
  retry only when the step's commands are idempotent; flag it in the CHANGELOG and the book's upgrade list.
- **Regression test:** the validation, expecting one aggregate (no retry) unless the definition opts in.
- **Continues:** F205, H3.

### R2-SYS-7 — At the end of a rollout that adds a singleton, every node's bus re-publishes up to 100 000 kept aggregates at 200 a second, six times the poller's budget, while the poller re-publishes the same rows
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code) for the rates; Hypothesis for the
  database load (not measured).
- **Where:** `EventsBusActor.scala:195-196` (`MaxToRepublish` 100 000, `RepublishedPerTick` 1 000 per 5-s tick),
  `:284-292`, `:464` (every publication is kept while a subscriber is unanswered); `RedeliveryPolicy.scala:20-28`
  (1 000 per 30-s poll, chosen so a backlog "does not start and restore every owner at once"); book 17 ("A projection
  or saga added in the new version").
- **Scenario:** a release adds a projection (F187's case); each new node's bus starts without it and keeps every
  aggregate published for the rest of the rollout; when the last old node leaves, the singleton answers every bus in the
  same tick.
- **Mechanism:** N buses × 200 `RepublishOutbox`/s: each starts (restores) a passivated owner, reads its outbox, and
  sends its events again to every subscriber, which check their subscription version (the cache holds 10 000
  aggregates); the poller's 3- and 6-minute attempts on the same rows add more.
- **Impact:** a restore and duplicate-delivery surge of several minutes (100 000 / 200 ≈ 8 min per node, estimated)
  on the node that just took every singleton; command latency and timeouts there.
- **Validation:** bench: 3 nodes, 100 000 aggregates kept per bus, measure restores/s and command latency at release.
- **Suggested fix:** re-publish kept aggregates at the poller's budget (or hand them to the poller), and skip those the
  poller already re-published.
- **Regression test:** at most `maxRepublishPerPoll` per `pollInterval` across bus and poller.
- **Continues:** F188 (D87).

### R2-SYS-8 — The `bus.behind` gauge gets a new series for every managed instance, so series grow without bound and a stopped instance's last lag stays visible
- **Severity** Low · **Likelihood** Likely · **Confidence** Confirmed (code) for the names; Strongly suspected for the
  stale value (push gauges are kept by tag in common registries).
- **Where:** `Projections.scala:273-276` (`<name>-<prefix>-<build>-<children>`, a counter per supervisor);
  `EventsBusActor.scala:415-420`; `Metrics.scala:5-8` and ARCHITECTURE §14 ("tags … never ids").
- **Mechanism:** every replaced instance (a singleton move at each rolling restart, a restart after a failure, each new
  build) is a new tag value; the old one is no longer sampled and keeps its last value in a host registry, with no way
  to remove it through `Metrics`.
- **Impact:** a lag alert that never clears after a move of a lagging projection; cardinality growing with every deploy.
- **Suggested fix:** tag by projection name and build (`<name>-build-<n>`), never the instance counter; sample 0 once for
  a subscriber that leaves.
- **Regression test:** two instance replacements leave one series per projection and build.
- **Continues:** F258.

### R2-SYS-9 — `Errors.transient` misclassifies two failover-time errors: a statement timeout or cancel is retried for ever without a record, and a read-only node records failures for healthy listeners
- **Severity** Medium · **Likelihood** Possible · **Confidence** Strongly suspected (depends on the role's settings and
  the failover method).
- **Where:** `Errors.scala:47-57` (classes 08, 40, 53, 57, or no SQLState); `ProjectionActor.scala:781-790` (transient:
  retried every 30 s, never counted); `AggregateOwner.scala:580-587`.
- **Mechanism:**
  - 57014 (`statement_timeout`, or an operator's `pg_cancel_backend`) is class 57: a batch that always exceeds a role's
    statement timeout (up to 100 aggregates per transaction, D61) is retried every 30 s for ever; no failure record,
    `GetProjectionStatus` shows nothing, its deliveries end undelivered.
  - 25006 (`read_only_sql_transaction`: a pool that reaches the demoted primary through stale DNS after a failover,
    common with JVM DNS caching; `validationQuery = SELECT 1` passes there) is counted as the listener's failure: a
    window longer than the ~2-minute retry budget leaves failure records for healthy listeners, F254's case.
- **Suggested fix:** treat 57014 as transient only a bounded number of times (then a failure naming the timeout); treat
  25006 (and 55P03 from a role `lock_timeout`) as transient; document `maxLifetime`/DNS TTL for failovers.
- **Regression test:** a listener failing with 57014 is recorded after N tries; one failing with 25006 for 5 minutes
  leaves no record.
- **Continues:** F254.

---

## Lesser items
- `AggregateCommandBusActor.scala:252`: an unknown message is answered `Status.Failure` with `m.toString` in the text;
  `ClusterSerializer` keeps an `IllegalArgumentException`'s message (`Errors.message`), so a command's values reach the
  caller's node and logs (F191).
- `AggregateCommandBusActor.scala:177`, `Projections.scala:250`: `OneForOneStrategy()` logs a child's failure through
  Pekko, unredacted; e.g. an instance whose store's unique index fails, with PgJDBC's default `logServerErrorDetail`.
- `ActorLogging.scala:19`: `logReceive` wraps `LoggingReceive`; with `pekko.actor.debug.receive = on` whole commands are
  logged at DEBUG.
- E2's pool (`BasicSetup.scala:33-35`) has no `validationQuery` and `maxSize` = the dispatcher's 32: the deployment
  example does not follow chapter 18 step 1, and meets R2-SYS-2.
- Book `18-production.md:46`: `tcpKeepAlive=true` notices a dead peer only after the OS keepalive time (Linux: 2 h by
  default); the real bound is `socketTimeout`.
- Book `18-production.md:31-34`: a pool wait failure is now answered `CommandHandlingError` (a SQLException without a
  state is "transient", `Errors.scala:53`), not `EventHandlingError`.
- Book `18-production.md:86`: "about a minute in all"; `Ddl` waits up to 30 × (2 s + 1 s) = 90 s per step.
- `Readiness.scala:39`: a member that never creates an `EventsBusActor` (a query-only or saga-only role) is never ready;
  the book does not say every node needs one.
- No 1.0.0 → 1.0.1 upgrade list in the book: ch. 18 step 7 and its upgrade list omit `buildSpaceIndexesConcurrently`
  (only ch. 16:281 and the CHANGELOG), the saga-retry change (R2-SYS-6), R2-SYS-5, and "no erasure while 1.0.0 nodes
  run" (they call `add_events` without the generation, `PostgresEventStoreSchemaInitializer.scala:274-286`, F195).
- `ReadWatch` (5 min, `OutboxPollerActor.scala:43`) gives up before the book's `socketTimeout` (10 min): two reads can
  run at once, each holding a thread and a connection.
- `StateShape.scala:30-44`: a field typed by a trait or abstract class (a sealed ADT) adds no subclass fields to the
  shape, so a field added to such a case class keeps old snapshots in use (F196).
- `OutboxDeletes.scala:47-50`: `flushNow` on an owner thread waits for a background flush, which a hung delete holds up
  to `socketTimeout`.
- `Ddl.has` (`to_regclass`) counts an INVALID index as present: on a small store an interrupted
  `buildIndexesConcurrently` leaves the unique index invalid and the plain one dropped
  (`PostgresEventStoreSchemaInitializer.scala:22, 190-196`).

## Fixes checked and found sound
- F185: an unchanged schema takes no table lock at start (`Ddl` reads the catalog first); `CREATE OR REPLACE FUNCTION`
  takes none either. (Its retry cost: R2-SYS-3.)
- F186: `ReadWatch` numbers reads and ignores late results; the poller and scheduler read again.
- F187, F188, F255: publications are not stashed; the bus starts after `registrationTimeout` and keeps outbox rows for
  unanswered subscribers (gaps: R2-SYS-1, R2-SYS-7).
- F190: library sessions run with `SqlLogging.withoutValues`.
- F191: `RedactingLog` and `Errors`; the cluster serializer sends `Errors.message` (gaps: lesser items).
- F233: not ready until the first bus, and from the shutdown's first phase.
- F253: connection-class failures around the commit answer `CommandHandlingError` and restore.
- F269: for 1.0.0 → 1.0.1, a 1.0.0 node that restarts replaces only the 9-argument `add_events` (the wrapper only 1.0.0
  calls) and bodies identical to 1.0.1's; the 10-argument form stays.
- F272: `PoolCheck` warns once when the pool is smaller than the dispatcher (its threshold: R2-SYS-2).
- F171, F210: `JsonPath.literal` writes ordinary paths as 1.0.0 did, so no existing index is rebuilt at the first 1.0.1
  start (and keys with spaces stop being rebuilt every start).
- F229: a command past its timeout is neither read for nor persisted.
