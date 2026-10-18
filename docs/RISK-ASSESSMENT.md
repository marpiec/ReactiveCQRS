# Risk assessment of ReactiveCQRS 1.0.0

How, when and why the library as released could fail: bugs, data corruption, lost personal-data erasures, hangs,
unbounded growth, performance cliffs and security problems, under realistic, extreme and hostile conditions. Made on
18/10/2026 at the owner's request, from the code of `v1.0.0` (line numbers are those of `master` at `bdabef3`, whose
code is the release's).

**How it was made.**

- Nothing was run. By request, no test was written or run: every finding comes from reading the code, the docs and the
  sources of the dependencies (ScalikeJDBC 4.3.5, Pekko 1.7.1, mpjsons 0.7.0).
- Ten areas were read in parallel, each against the 184 issues already known (KNOWN-ISSUES.md and its archive). The
  findings were merged, and the serious ones were checked again in the code. Only what is new is reported here.
  Closed issues found incomplete get a new number too.
- Each area's working notes, with the lesser items not triaged here, are linked in §14.

**Confidence levels.**

- **Confirmed (code):** the path was traced end to end, and the failure follows from the code as written.
- **Strongly suspected:** it also depends on a dependency's documented behaviour, on timing, or on configuration.
- **Hypothesis:** plausible, but needs evidence.

In KNOWN-ISSUES' own terms, where "confirmed" means reproduced by a test, all of these are "likely" (Confirmed (code))
or "suspected" (the other two): nothing here has been reproduced yet.

**Numbers.** They are estimates unless they name their source: `docs/perf` and SCALING.md hold the project's
measurements.

**Where the findings live.** Each finding has an F-number (F185–F274) and is recorded, open, in
[KNOWN-ISSUES.md](KNOWN-ISSUES.md) §3.

## 1. Executive summary

**The core is sound.** 1.0.0's main path holds up under adversarial reading:

- **Locking:** no lost update (the procedures check versions under the aggregate's row lock).
- **Delivery:** at-least-once delivery with deduplication by version, and order per aggregate.
- **Values in SQL:** values are bound; injection is possible only through document-store paths.
- **Actors:** no actor state touched from futures, and no late `sender()`.
- **Flow control:** its accounting is balanced.
- **Purge:** it reaches every library table.
- **Arithmetic:** overflows are guarded.

The risks lie around that core, in 90 findings:

| Severity | Count | Of which likely or possible |
|---|---|---|
| Critical | 0 | — |
| High | 27 | 19 |
| Medium | 50 | 38 |
| Low | 13 | 9 |

**Why no Critical finding.** None loses committed events in normal operation, and none can be exploited from outside
without a mistake in the host's code. The High findings can still cause incidents, silent corruption or compliance
failures. They cluster in nine themes:

1. **Personal data leaks out of erasure's reach** (F190, F191, likely). Failed SQL statements are logged with their
   values. Exception texts carrying events, states and orders reach logs, error replies and `subscription_failures`.
   None of these is reached by the complete rewrite, which GDPR Art. 17 erasure relies on (Regulation (EU) 2016/679,
   https://eur-lex.europa.eu/eli/reg/2016/679/oj).
2. **An erasure can report "complete" while the value remains** (F192–F195, F244, F245). Causes:
   - a rerun after a refused rebuild;
   - squashed copies with no lineage;
   - attached copies never rebuilt;
   - a command running across the rewrite;
   - aggregates at the version limit;
   - unreadable events.
3. **Starts and rolling upgrades can stall the cluster** (F185, F187–F189). Every node start takes an exclusive lock on
   `events` with no lock timeout. A release that adds a projection or a saga cannot finish a readiness-gated rollout. A
   projection whose new store setup fails keeps its node's event bus waiting forever, stashing every publication.
4. **A hung or slow database stops nodes silently** (F186, F229, F253, F254). No statement, socket or lock timeout
   exists:
   - a hung read stops all re-delivery in the cluster;
   - the command timeout cannot fire while its owner waits on JDBC;
   - a commit whose outcome is unknown is answered "nothing was stored";
   - transient errors become recorded projection failures.
5. **Moving an organization can corrupt it or cross tenants** (F198–F200). The export is not one snapshot, so a write
   during it gives two events at one version after the import. The import checks neither completeness nor references.
   The purge does not guard the platform space.
6. **Sagas can contradict what they answered** (F205–F207, F232). A step that timed out is reverted while its command
   still lands. A saga answered "failed" can run forward at the next start. A retried order starts a second saga. One
   saga can run twice in one runner.
7. **Silent state errors** (F196, F197, F201):
   - a snapshot of a state class that gained a field reads that field as zero, false or null;
   - an aggregate id of another type is restored and written by the wrong context;
   - a stale failure record can make an operator's skip acknowledge a real event without applying it.
8. **Drafts can lose edits** (F208, F209). After a materialization times out, the next one returns the older stored
   copy and drops the draft. Ephemeral id tokens repeat across starts.
9. **Document-store queries** (F210, F211). JSON paths and sort fields are written into SQL unescaped. The convenience
   finders read a whole read model, without a limit.

**What to do first** (§13, all without a data migration):

- stop logging SQL values and sanitize failure texts;
- check the catalog before start-up DDL, with a lock timeout;
- document and set JDBC timeouts;
- close the erasure gaps;
- let the bus start without a subscriber that cannot answer;
- export in one snapshot and validate imports.

## 2. Library architecture and critical assumptions

**The map.** CLAUDE.md maps the code and ARCHITECTURE.md describes it. In short:

1. A command goes to its type's command bus.
2. The bus routes it to the aggregate's owner (an actor per aggregate, `Local` per node or `Sharded` across the
   cluster).
3. The owner runs the domain's handler in a future, then persists the events through stored procedures. Each
   procedure checks the version under the aggregate's row lock and writes an outbox row in the same transaction.
4. After the commit, the owner publishes to its node's event bus.
5. The bus delivers to the subscribers (projections, sagas, listeners) under flow control. Acknowledged rows are
   deleted from the outbox; a singleton poller re-publishes what waits.
6. Projections apply events in transactions with their subscription versions.
7. Managed projections are built from the store and switched in one registry update.

Organizations (spaces) can be exported, imported and purged; histories can be squashed, undone, duplicated and
rewritten (the complete rewrite is erasure). Every node runs the schema initializers at start, under one advisory
lock.

**What the design takes for granted, and where it fails:**

| # | Assumption | Relied on by | Holds? |
|---|---|---|---|
| A1 | One writer per aggregate: the procedures' version check under the row lock decides | owners, `add_events` | Holds for versions (I1). Fails for history changes that keep the version (F195), ids of another type (F197) and imported histories (F198) |
| A2 | The outbox is the queue; subscribers deduplicate by version | bus, sequence gate | Holds, except where a subscription is moved past the aggregate (F201) or deliveries are acknowledged unapplied (F202) |
| A3 | Every statement returns, and in time | every JDBC call | Not guaranteed: no statement, socket or lock timeouts (F186), and start-up DDL waits on locks (F185) |
| A4 | A stored shape never changes silently | snapshots, events (D29) | Snapshots rely on a manual `schemaVersion` bump (F196) |
| A5 | The host sends each id to its own type's bus | command buses, owners | Not checked anywhere (F197) |
| A6 | Error texts are safe to log and to store | logs, `subscription_failures`, replies | Not true (F190, F191) |
| A7 | Every registered subscriber answers its subscription request | bus start-up | Can fail forever (F187–F189) |
| A8 | Start-up schema steps are cheap | `initSchema` at every start | They take exclusive table locks (F185) and replace the procedures (F269) |
| A9 | Node clocks agree | re-delivery, instant reads, the squash policy | Assumed, not checked (F246, F270) |
| A10 | An organization is quiet while it is exported, purged or imported | `SpaceExport`, `SpacePurge`, `SpaceImport` | Documented, not enforced (F198, F200) |
| A11 | The pool has a connection for every concurrent JDBC user, and no code holds one while asking for another | pool sizing advice | A nested borrow exists (F230); the default configuration is mismatched (F272) |
| A12 | A command is answered within `commandTimeout` | owners, the book's client advice | Only in message order, not in time (F229, F186) |
| A13 | An erasure reaches every copy of a value | `HistoryRewriter` | Several gaps (F191–F195, F244, F245) |

Invariants under threat (KNOWN-ISSUES §2):

- **I1** (one writer, versions by one): F195, F197, F198.
- **I3** (outbox rows go only once answered): F202.
- **I4** (contiguous versions): F201.
- **I6** (restores equal replays): F196.
- **I9** (older data stays readable): F269.

## 3. Critical and high-severity findings

No finding is rated Critical. The 27 High findings follow, by theme.

### 3.1 Starts, upgrades and a database that does not answer

#### F185 — Every node start takes exclusive locks on the event tables, with no lock timeout
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code) for the statements; the lock queue is
  PostgreSQL's documented behaviour.
- **Where:**
  - `PostgresEventStoreSchemaInitializer.scala:12-41` runs at every start.
  - ACCESS EXCLUSIVE: `ALTER TABLE events DROP COLUMN IF EXISTS command_id` (58-62), and `ADD COLUMN IF NOT EXISTS` on
    `aggregates` and `events` (132-136, 156-169).
  - SHARE: `CREATE INDEX IF NOT EXISTS` (64-86, 112-116, 206-207).
  - The same pattern: `CommandResponseState.scala:93-121`, `PostgresSagaSchemaInitializer.scala:18-20`,
    `SubscriptionsState.scala:250-272`, and `PostgresDocumentStore.scala:43-66` (in every store's constructor).
  - `SchemaLock.scala:20, 39-46`: one advisory lock for all of it, no `lock_timeout`, a 5-minute wait.
- **Preconditions:** a node or projection instance starts while another session holds a lock on one of these tables
  for long. Examples:
  - `pg_dump` (ACCESS SHARE for the whole dump);
  - an organization export, `StoredDataCheck`, `backfillEventKeys`, or a long report;
  - for the SHARE steps, also a long writer, or an anti-wraparound autovacuum, which does not yield.
- **Scenario:** a pod restarts during a nightly `pg_dump` of an hour.
- **Mechanism:**
  1. PostgreSQL takes the table lock before it checks `IF [NOT] EXISTS`, so the ALTER waits although it has nothing
     to do.
  2. While it waits, every later request on `events` queues behind it: each `add_events`, restore and page read, from
     every node.
  3. The starting node holds `SchemaLock`. Nodes and projection instances that start meanwhile wait up to 5 minutes,
     each holding a thread and a connection, then fail.
- **Impact:**
  - Every write and restore in the cluster stops for the rest of the long transaction.
  - Nodes crash-loop at start.
  - It looks like a database outage; `pg_stat_activity` shows the queue behind the ALTER.
- **Validation:**
  1. Session A: `BEGIN; SELECT count(*) FROM events;`, left open.
  2. Session B: `initSchema()` on a complete schema.
  3. Session C: one `add_events`. It waits until A ends.
- **Fix:**
  - Read the catalog first (`information_schema.columns`, `to_regclass`, `pg_indexes` take no table locks), and run
    DDL only for what is missing.
  - Run the rest in short transactions with `SET LOCAL lock_timeout`, retried with back-off.
  - Record one-off steps in `schema_steps`.
  - Trade-off: exact catalog checks to keep up; a start may retry.
- **Regression test:** the validation completes C within 1 s.
- **Sources:**
  - https://www.postgresql.org/docs/current/explicit-locking.html
  - https://www.postgresql.org/docs/current/sql-altertable.html

#### F186 — No statement, socket or lock timeouts: a hung connection pins owners and threads, and silently stops re-delivery
- **Severity** High · **Likelihood** Possible · **Confidence** Strongly suspected. The latches and the missing timeouts
  are confirmed in code; how a connection hangs depends on the network and the driver.
- **Where:**
  - `OutboxPollerActor.scala:55-73`: `case Poll => if (!reading) read()`; `reading` clears only when a read returns.
  - `SquashScheduler.scala:66, 79-82`: the same latch.
  - `AggregateOwner.scala:339`: the command timeout is a message to the owner itself, whose JDBC is synchronous
    (`persist` 494-505, `restore` 227, `caughtUp` 318).
  - `reference.conf`: 32 blocking threads.
  - Nowhere in the code or docs: `queryTimeout`, `statement_timeout`, `lock_timeout`, `socketTimeout`,
    `tcpKeepAlive`. The book's pool settings have no validation query.
- **Preconditions:**
  - pgjdbc's defaults: `socketTimeout=0`, so reads never time out, and `tcpKeepAlive=false`.
  - A failover or a network fault that leaves connections half-open, or a statement waiting on a lock (F185, F215).
- **Scenario:** a managed-PostgreSQL failover while the poller reads the outbox and 30 owners or projections have
  statements in flight.
- **Mechanism:**
  1. Each in-flight read waits for bytes that never come.
  2. The poller's `reading` never clears, so re-delivery stops cluster-wide, silently (the poller is a singleton). Its
     gauges freeze at their last values.
  3. An owner's `CommandTimedOut` waits behind its blocked thread.
  4. Each hung call pins one of the 32 blocking threads. Once all are pinned, the node stops: owners, projections,
     supervisors and futures. Nothing frees them when the database returns.
- **Impact:**
  - Callers hit their own ask timeouts and retry; without idempotency keys, that duplicates commands.
  - Lost deliveries are never re-published until a restart.
  - Readiness does not notice.
  - SCALING.md's "fails the commands waiting for a connection" holds only for threads still waiting for a connection.
- **Validation:** put toxiproxy between the JVM and PostgreSQL; black-hole it during a poll and a persist, and restore
  it after a minute. The poller never polls again, and the command is never answered.
- **Fix:**
  - Document and set, in the book and the examples:
    - `socketTimeout` above the longest legitimate statement;
    - `tcpKeepAlive=true` and a validation query;
    - role-level `statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout`.
  - Give the poller's and the scheduler's latches a watchdog: a start time and a generation. After N intervals, log an
    ERROR, read again, and ignore the stale result.
  - Put query timeouts on their reads.
  - Trade-off: timeouts must allow for long operations (restores, exports, build pages).
- **Regression test:** an `EventStoreState` whose `readWaitingInOutbox` blocks forever; a later poll logs an error
  and reads again.
- **Source:** pgjdbc connection parameters, https://jdbc.postgresql.org/documentation/use/#connection-parameters

#### F187 — A release that adds a singleton cannot finish a readiness-gated rolling upgrade
- **Severity** High · **Likelihood** Likely: any release that adds a managed projection registered on the buses, or a
  saga runner · **Confidence** Confirmed (code). Pekko runs a singleton on the oldest member only.
- **Where:**
  - `Singletons.scala:17-21`.
  - Pekko's `ClusterSingletonManager` stays `Younger` while the oldest member exists, whether or not that member runs
    a manager of the name. `ClusterSingletonProxy` looks only at the oldest member.
  - `EventsBusActor.scala:191, 232, 252`: the bus waits for every registered subscriber, stashes meanwhile, and keeps
    the node not ready.
  - E2's `kubernetes.yaml:66-69`: pods are replaced one at a time, each once the one before is ready.
- **Preconditions:**
  - Release N+1 starts a singleton name that N lacks. Examples: a `Projections.start` proxy registered with
    `RegisterSubscribers` on every bus, as the book shows; a `SagaRunner.start`; probably a new `Sharded` type too,
    whose coordinator is a singleton.
  - The oldest node is replaced last (D66), and the rollout waits for readiness.
- **Scenario:** three nodes run N. N+1 adds projection P, and the StatefulSet replaces shop-2 first.
- **Mechanism:**
  1. shop-2's manager of P stays Younger, because shop-0, the oldest and still on N, has no manager of P.
  2. shop-2's proxy never finds P. shop-2's bus asks P every 5 s, never gets an answer, and stays in `registering`.
     It stashes every publication (F188) and stays not ready.
  3. Kubernetes waits for shop-2 before it touches shop-1. So shop-0, whose replacement would let P start, is never
     replaced.
- **Impact:**
  - The rollout hangs until an operator forces it.
  - Meanwhile, read models are stale for every aggregate owned on shop-2, while commands succeed, and shop-2's heap
    grows.
  - Without a readiness gate, there is the same lag for the whole rollout.
  - For a new saga runner, its orders sit in the proxies' buffers (1 000, then dropped at DEBUG), and callers time
    out.
- **Validation:** a variant of `RollingUpgradeSpec` in which the new nodes register an extra projection. They never
  become ready.
- **Fix:**
  - The bus does not wait for a registered proxy that cannot answer while older versions run
    (`Members.olderVersionsRun`). It treats it as a late registration: asked every tick, added when it answers, not
    counted for readiness. Managed projections catch up from the store.
  - Mirror F132 (`retire` waits) for `start`.
  - Document that a new singleton starts only once the oldest node runs the release.
  - Trade-off: an unmanaged late subscriber misses earlier live events (F162 already warns).
- **Regression test:** the variant becomes ready and delivers while the old oldest node runs.
- **Source:** https://pekko.apache.org/docs/pekko/current/cluster-singleton.html

#### F188 — The event bus waits forever for a subscriber that cannot answer, and stashes every publication meanwhile
- **Severity** High · **Likelihood** Possible; F187 and F189 make it likely · **Confidence** Confirmed (code). That
  shards are placed onto the waiting node is Strongly suspected.
- **Where:**
  - `EventsBusActor.scala:202-233`: `case _ => stash()`, and Pekko's default stash is unbounded.
  - `EventsBusActor.scala:251-258`: the bus starts only once every registered subscriber has answered.
  - `AggregateOwner.scala:855-858`: owners publish to their node's bus whatever its state.
  - `Owners.scala:145-166`: the shard region starts whatever `Readiness` says.
  - `Projections.scala:268-280`: a supervisor in `starting` stashes too.
- **Preconditions:** a registered subscriber that never answers. Examples:
  - F187's proxy, or F189's supervisor;
  - a projection that cannot start (F164 keeps the bus waiting by design);
  - a `SchemaLock` timeout (F185).
- **Scenario:** after a deploy whose projection cannot be built, a node restarts.
- **Mechanism:**
  1. The bus stays in `registering`.
  2. The node still owns shards: D66 places new ones on the newest node.
  3. Its owners persist and publish into the stash, and the poller's re-publications land there again.
- **Impact:**
  - Events of the aggregates owned on that node reach no projection, saga or listener anywhere.
  - The heap grows with every event and state until OOM; after the restart, the same happens again.
  - The only sign is "Event bus waits for the subscriptions of: …" every 5 s.
  - No data loss: the outbox keeps the rows.
- **Validation:** register a probe that never answers, and publish 100 000 events of 10 KB. The heap grows by their
  size.
- **Fix:**
  - Never stash `PublishEvents` while registering. Drop them (the outbox is the queue), keep only their
    `(type, aggregate)` in a bounded set, and have those re-published once started.
  - After a registration timeout, start, and treat the missing subscriber as stopped: its rows stay in the outbox. Log
    an ERROR and count a metric.
  - Let a supervisor answer from its registry, or with `Status.Failure`.
  - Trade-off: the first deliveries after the start come from re-publication.
- **Regression test:** a waiting bus receives 1 000 publications, keeps only their ids, and has their owners
  re-publish once it starts.

#### F189 — The active build's instance runs the new code's store setup on the old tables; when that fails, the projection and its node's bus stay down
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code). That the index creation fails on the
  old data is PostgreSQL behaviour.
- **Where:**
  - `Projections.scala:304-307`: `active = registered.active.map(instanceOf)`.
  - `Projections.scala:325`: `instanceOf` constructs the instance with the current code's `create`.
  - `PostgresDocumentStore.scala:41-66`: `init()` runs in the constructor. It drops undeclared indexes, creates
    missing ones without `CONCURRENTLY`, and rebuilds changed ones (F171).
  - `Projections.scala:230, 606-624`: an active instance that stops sends the supervisor back to `starting`, every
    10 s.
  - `Projections.scala:268-280`: `starting` stashes everything.
- **Preconditions:** a new projection version cannot construct on the old build's tables. Typically it declares an
  index those documents violate: a UNIQUE index, or a typed `::bigint` index on a field stored as text.
- **Scenario:** V5 → V6 adds `UniqueTextIndex` on `email`, the kind of change that motivates a new version.
- **Mechanism:**
  1. At start, the active V5 instance is built with V6's code on `projection_x_v5`.
  2. `CREATE UNIQUE INDEX` fails, so the constructor throws.
  3. The supervisor stops everything and starts again after 10 s, forever.
  4. `Listeners` never arrives, so the V6 builder never starts. The bus is never answered (F188), and queries are
     stashed without bound.
  5. Even when the index can be built, a non-concurrent build on a large active table holds `SchemaLock` for minutes,
     and other initializers fail after 5 minutes.
- **Impact:**
  - On a single node, or on any node restarted later, the bus never starts.
  - In a running cluster, the projection has no instance: its queries hang and its outbox rows grow.
  - Recovery needs a code change. F171's advice ("a new version builds new tables") does not avoid it.
- **Validation:** V1 holds documents with a duplicate `email`; start V2 with a unique index on it. `GetBuilds` never
  answers, and the bus stays in `registering`.
- **Fix:**
  - A build whose version is not the code's must not synchronize indexes: give the store a `syncIndexes = false` mode,
    or build indexes `CONCURRENTLY` outside constructors.
  - When the active instance cannot be made, the supervisor takes `Listeners` from the build instance, answers the bus
    with what exists, and lets the build proceed.
  - Bound the `starting` stash and answer queries with a failure; back off, and show the error in the status.
  - Trade-off: the old build serves without the new indexes until the switch.
- **Regression test:** the validation scenario builds and switches V2, starts the bus, and answers queries within
  `restartAfter`.

### 3.2 Personal data and erasure

#### F190 — Failed SQL statements are logged with their values: events, documents and saga orders reach the logs
- **Severity** High · **Likelihood** Likely · **Confidence** Confirmed (code, and the project's own suite log).
- **Where:**
  - ScalikeJDBC 4.3.5: `GlobalSettings.loggingSQLErrors = true` by default (`GlobalSettings.scala:22`).
  - `StatementExecutor.scala:441-465` logs a failed statement at ERROR with its parameters inlined. Strings are cut at
    100 characters; other values are printed whole, such as the `java.sql.Array` of serialized events that
    `PostgresEventStoreState.scala:105-113` binds.
  - The library, the book and the examples never change this default.
- **Preconditions:** none beyond the defaults. pgjdbc's `logServerErrorDetail=true` also adds the row's values to
  constraint errors.
- **Scenario:** any failed statement. Examples:
  - two nodes writing one `Local` aggregate;
  - a chosen-id race (F120);
  - a refused undo or duplication;
  - a unique document index violated;
  - a saga write failing during a failover.
- **Mechanism:** `examples/target/full-suite-normal.log` of 18/10/2026 (test data) has 39 such lines. Among them:
  - an `add_events` call with its whole array of serialized events;
  - an `INSERT INTO sagas` with the saga's order.
- **Impact:**
  - Personal data in central log stores, with their retention and wide access.
  - No erasure (D76) or purge reaches them (GDPR Art. 17).
  - It looks like ordinary diagnostics.
- **Validation:** a log-capturing spec:
  1. Write events, documents and a saga order carrying a marker string.
  2. Force a chosen-id race and a unique-index violation.
  3. Assert the marker is in no log line. This fails today.
- **Fix:**
  - Run the library's statements with an implicit `SettingsProvider` that turns `loggingSQLErrors` off.
  - Log the library's own line instead: the statement template, the SQLState and the constraint name, with no values.
  - Document `logServerErrorDetail=false` for the JDBC URL.
  - Trade-off: less detail when debugging; the error id still links a reply to the log.
- **Regression test:** the capture spec above.
- **Sources:**
  - https://scalikejdbc.org/documentation/configuration.html
  - https://jdbc.postgresql.org/documentation/use/#connection-parameters

#### F191 — Exception texts carry event, state, order and document values into logs, replies, `subscription_failures` and the bus's warnings
- **Severity** High · **Likelihood** Likely · **Confidence** Confirmed (code). That `MatchError`'s message is the
  value's `toString` is standard Scala; pgjdbc's error detail is its documented default.
- **Where:**
  - **Projections:**
    - `ProjectionActor.scala:759` builds the text: `e.getClass.getSimpleName + ": " + e.getMessage`.
    - Lines 762-764 log it at WARN; 774-785 at ERROR, kept untruncated in memory and in `MessageFailed`.
    - Line 587 answers every later delivery of the aggregate with it; 965 is the builder's unreadable-event text.
    - `SubscriptionsState.scala:275-284` stores up to 4 000 characters of it.
    - `EventsBusActor.scala:475-476` logs one WARN per `MessageFailed`, unthrottled.
  - **Owners:**
    - `AggregateOwner.scala:237-239, 488-489, 626-628, 789-791`.
    - `Exceptions.scala:15-16`: `AggregateRestoreFailedException` carries `cause.getMessage` to callers, also across
      nodes.
    - `AggregateRestorer.scala:103-105`, `AggregateCommandBusActor.scala:437`.
  - **Sagas:** `SagaRunner.scala:145, 193, 204, 207` log `order = ${saga.order}` and any unexpected message.
  - **Elsewhere:** `ActorLogging.scala:25`.
  - **mpjsons 0.7.0:** read errors quote 80 characters of the JSON (`MPJsons.scala:13-35`). Those texts reach
    `SpaceImport.scala:237-249`, `HistoryRewriter.scala:51-52` and `StoredDataCheck.scala:166`.
- **Preconditions:** personal data in events, states, orders or documents.
- **Scenario:**
  - A new event class ships before a listener has a case for it. The listener throws `MatchError`, whose message is
    the event's `toString`, with every field.
  - A squash event's `MatchError` carries the whole state.
  - A revert that fails logs the whole saga order.
- **Mechanism:**
  - The text goes to a WARN, then an ERROR with stack, then a stored row, then one bus WARN per later event of the
    aggregate.
  - Re-publications at 3, 6 and every 30 minutes repeat it forever.
  - A listener that fails a whole type at 1 000 events/s writes about 1 000 WARN lines per second (estimate).
- **Impact:** personal data in:
  - logs;
  - admin-tool status answers;
  - error replies, which a REST layer may show to another user;
  - `subscription_failures`, a table the complete rewrite does not touch (ARCHITECTURE §4.9's "not reached" list omits
    it).
  
  Log floods follow as well.
- **Validation:** a listener throws `MatchError` on an event carrying a marker string. Assert the marker is in neither
  the logs, the stored failure, nor the status. Then rewrite the aggregate and assert the stored failure no longer
  holds it.
- **Fix:**
  - Store, send and log only the exception's class, an error id and a bounded, sanitized message; for a `MatchError`,
    only the value's class.
  - Log the full exception once, on a restricted logger, behind a debug option.
  - Log a `MessageFailed` once per (subscriber, aggregate, kind).
  - For sagas, log the name, id, step and phase, never the order.
  - Have the complete rewrite clear the aggregate's failure texts.
  - Trade-off: less detail in the stored record.
- **Regression test:** the validation above, and a bounded `MessageFailed.error`.

#### F192 — An erasure that could not rebuild a projection reports "complete" when run again; a build in progress switches in with the erased values
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `HistoryRewriter.scala:25, 49`: `rebuild(walked.rewritten.map(_.id), …)`.
  - `HistoryRewriter.scala:67-69`: an aggregate the rewrite did not change is `unchanged`.
  - `HistoryRewriter.scala:78-93`: the fold over an empty list marks every projection `rebuilt`.
  - `Projections.scala:512-514`: `RebuildAggregates` is refused while a build or another targeted rebuild runs.
  - `ProjectionBuilder.scala:216, 314`: the bulk pass reads by event id, and the reconciliation compares versions
    only.
- **Preconditions:** an erasure through `HistoryRewriter` that names a managed projection, while one of these holds:
  - (a) the projection's build runs. Builds follow version changes and take hours on a large store.
  - (b) another erasure's targeted rebuild runs.
  - (c) the 5-minute ask times out.
  - (d) the node stops between the rewrite and the rebuild.
- **Scenario:** two erasures at once, or one during a build.
- **Mechanism:**
  1. Run 1 commits the rewrite; the projection refuses the rebuild, and the report lists it as failed.
  2. Run 2 is the documented remedy, but it changes nothing. The aggregate is `unchanged`, the rebuild set is empty,
     and every projection is reported `rebuilt`: `complete = true`.
  3. The rewrite updated rows in place, with the same ids and versions. So a build that already passed the
     aggregate's event ids never reads it again, and it becomes active at the switch with the old values.
  4. The refusal says the build "replaces the active one whole", which is false for rows it has already read.
- **Impact:** personal data stays in read models after a report that says complete (GDPR). Only `history_rewrites`
  records the change.
- **Validation:** a projection that refuses the first `RebuildAggregates`. Run 1 reports it failed; run 2 reports
  complete, and the projection got no second request.
- **Fix:**
  - Make the rebuild duty durable (per audit row and projection), or rebuild every visited aggregate on each run.
  - Let the supervisor queue targeted rebuilds behind a running one, and apply them to a build in progress before its
    switch.
  - Never count a projection as rebuilt when nothing was asked of it.
  - Trade-off: more targeted work.
- **Regression test:** the validation rebuilds on run 2; two concurrent erasures on two aggregates both rebuild.

#### F193 — Squashing a duplicate drops its link to its base, so a later erasure of the base never reaches it
- **Severity** High · **Likelihood** Possible; Likely for types that both duplicate and squash · **Confidence**
  Confirmed (code, both stores).
- **Where:**
  - `PostgresEventStoreState.squash` 758-785. Line 782, `DELETE FROM aggregates WHERE id = $id AND base_id <> $id`,
    writes no `aggregate_lineage` row, unlike `detachDuplicate` (750-754).
  - `MemoryEventStoreState.squash` 244-261 behaves the same.
  - `copiesOf` (715-719) and `HistoryRewriter.walk` find copies only through chain rows or lineage.
  - `squashCandidates` picks duplicates like any aggregate.
- **Preconditions:** duplication and squash used on one type, and a base that holds personal data.
- **Scenario:**
  1. Copy D is made of template B at version 10.
  2. The scheduler squashes D to version 40. Its squash event is D's state, which holds B's values.
  3. Later, `HistoryRewriter.rewrite(B, …)` runs.
- **Mechanism:** D50 says squashing a duplicate detaches it, and D76 says every detach records lineage. This one
  records none, so `copiesOf(B)` misses D. D is never rewritten, and the report says complete.
- **Impact:**
  - The erased values stay in D's squash event, snapshots and read models.
  - Nothing links D to B any more.
  - Copies squashed before a fix have lost the link for good.
- **Validation:** copy, squash the copy, and assert that `copiesOf(base)` contains it. This fails in both stores.
- **Fix:**
  - Insert the lineage of the squashed aggregate's own chain before deleting it, with `detachDuplicate`'s statement,
    in both stores.
  - For copies squashed already, a manual audit in the runbook.
  - Trade-off: none.
- **Regression test:** the validation, plus an erasure of the base that changes the copy's squash event.

#### F194 — After its base is erased, an attached duplicate is reported unchanged and never rebuilt; a lost history notice is never repaired
- **Severity** High · **Likelihood** Likely where copies and state-receiving projections exist · **Confidence**
  Confirmed (code).
- **Where:**
  - `AggregateOwner.scala:780-787`: only a rewrite that changed something notifies the duplicates and restores.
  - `AggregateOwner.scala:796-799`: a failed notice is only logged.
  - `HistoryRewriter.scala:49, 67-69`: only rewritten aggregates are rebuilt.
  - `Projections.scala:533`: `OneAggregate(id)` covers that id only.
  - `AggregateOwner.scala:318`: sharded and lone owners never compare with the store.
- **Preconditions:** an erased base B with attached copies D that read B's history, and projections that receive D's
  state.
- **Scenario:** `rewrite(B)` changes B and rebuilds B.
- **Mechanism:**
  1. D's own events are unchanged, so D answers `HistoryRewritten(D, 0, 0)` and is put in `unchanged`, never rebuilt.
     D's documents show B's old values until D's next event.
  2. If the duplicates lookup fails, or the node dies between the commit and the notice, D's sharded owner keeps
     serving the old state: in reads, and in new events that copy it.
  3. A rerun changes nothing: both aggregates answer 0, which skips both the notice and the restore.
- **Impact:** personal data in read models, and possibly in a live owner and in new events, after an erasure reported
  complete.
- **Validation:**
  - Erase B with an attached copy D: D's document still has the value.
  - Fail `duplicatesReading` once and rerun: D's owner still serves the value.
- **Fix:**
  - Rebuild every attached duplicate that reads a rewritten aggregate.
  - Always notify the duplicates on a rewrite.
  - A sharded owner restores when the stored history generation differs from its own: one lookup per rewrite.
  - Trade-off: more rebuild work for templates with many copies.
- **Regression test:** both validation cases are cleared.

#### F195 — A history change that keeps the version is not seen at persist: a command running across an erasure writes the erased values back
- **Severity** High · **Likelihood** Possible: a narrow window, wider with asynchronous handlers and with `Local`
  owners on other nodes · **Confidence** Confirmed (code).
- **Where:**
  - `AggregateOwner.scala:281-282`: a rewrite runs without waiting for running handlers (compare `materialize` at
    740-741).
  - `AggregateOwner.scala:419-443`: handlers run in futures on the state they started from; rewrite commands read
    their events at 428.
  - `AggregateOwner.scala:467`: `persistOrAnswer` compares only `baseVersion != version`.
  - `AggregateOwner.scala:770-787`: the rewrite keeps the version.
  - `PostgresEventStoreSchemaInitializer.scala:293`: `add_events` compares the version, not `history_generation`.
  - `overwriteEvents` (138-149) updates by version and class.
- **Preconditions:** any of these:
  - A command in flight on the aggregate whose event copies an erased field from the state.
  - A rewrite-only command in flight. Neula's anonymization uses `RewriteHistoryConcurrentCommand` (F73).
  - A `Local` owner on another node whose catch-up ran before the rewrite committed.
  - The same with a `SquashMayChangeState` squash, or with commands already queued in a duplicate's owner when its
    base changes (F145's gap).
- **Scenario:** an asynchronous handler (1–5 s) builds `InvoiceSent(email = state.email)` while `RewriteEverywhere`
  arrives.
- **Mechanism:**
  1. The rewrite commits: the generation goes up by one, the version stays. The owner restores the erased state.
  2. The handler finishes with the same base version, so it persists, and event V+1 carries the old value.
  3. A rewrite command in flight is worse: it overwrites the erased event with its copy, built from the old one.
- **Impact:** erased data is back in the store and the read models after a report that said complete. Undetectable
  without a scan.
- **Validation:** a latch-blocked handler copies a state field. Send the command, then the rewrite, then release the
  latch: event V+1 holds the old value. Repeat with a rewrite command.
- **Fix:**
  - Keep the history generation in each pending command, and treat a change like a changed version (rerun or
    conflict).
  - Or defer rewrites and state-changing squashes while handlers run, bounded by `commandTimeout`.
  - Across nodes, compare the generation at commit: an `add_events` overload, or `SELECT … FOR UPDATE` of the
    aggregate's row in the persist transaction.
  - Trade-off: more conflicts, only on aggregates being rewritten.
- **Regression test:** the validation in both modes, plus a two-node case with the rewrite committed between the other
  owner's catch-up and its persist.

### 3.3 Integrity of stored data

#### F196 — A snapshot of a state class that gained a field is restored with zero, false or null in that field, silently
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code, and mpjsons 0.7.0's source).
- **Where:**
  - `AggregateRestorer.scala:63-78`: a snapshot is used when its context version and `schemaVersion` match.
  - `PostgresEventStoreState.scala:225-229`: `mpjsons.deserialize` reads the stored JSON into the current class.
  - mpjsons' `BeanDeserializer`: "absent keys get zero / false / null / None".
  - `AggregateContext.scala:131-134`: "Change `schemaVersion` when the state class changes shape". This is a manual
    step.
- **Preconditions:** a context with snapshots (recommended for long histories, H8), and a release that adds a field to
  the root, or to a class it holds, without raising `schemaVersion`.
- **Scenario:** `Limits(max)` gains `warnAt: Int = 80`. After the deploy, every restore from a snapshot gives
  `warnAt = 0`, because Scala default values are not used.
- **Mechanism:**
  - A removed or renamed field fails the read, and the restore falls back to a full replay (safe).
  - An added field passes silently.
  - Handlers then decide on the wrong state and store the decisions as events. The owner publishes the wrong state to
    state listeners and writes it as the next snapshot.
- **Impact:**
  - Wrong decisions stored as permanent events: I6 is broken between the snapshot path and a replay.
  - Null pointer errors later.
  - Specs without snapshots pass, so it is hard to detect.
- **Validation:** restore a snapshot JSON without the new field into the new class, and compare with a full replay.
  They differ.
- **Fix:**
  - Store a shape fingerprint with each snapshot: a hash, by Java reflection, of the field names and types of the root
    and the classes it reaches. Treat a mismatch as unusable.
  - Optionally, compare a sampled snapshot restore with a replay after each start.
  - Trade-off: one replay per aggregate after a harmless shape change.
- **Regression test:** a snapshot with an old fingerprint is not used, and the state equals a replay.

#### F197 — An aggregate id of another type is restored by the wrong context, which then writes into it
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code) that no layer checks the type. That the
  restore succeeds through a snapshot is Strongly suspected (mpjsons skips unknown keys).
- **Where:**
  - `AggregateCommandBusActor.scala:197-203, 212`: any id is routed.
  - `PostgresEventStoreState.scala:160-200`: restores select by aggregate id, with no type.
  - `PostgresEventStoreState.scala:225-228`: `readSnapshot` reads the JSON as the asking type's class; the row records
    no type.
  - `PostgresEventStoreSchemaInitializer.scala:281`: `UPDATE aggregates … WHERE id = … AND base_id = …`, with no
    `type_id`.
  - `AggregateOwner.scala:425-426, 647`.
- **Preconditions:**
  - Type B's aggregate id X reaches type A's bus: an unvalidated id from a client, or a host or saga bug.
  - A restores X. Either from a snapshot (both types take snapshots with equal context `version` and `schemaVersion`,
    the defaults, and X has no event after its snapshot, as is usual after a passivation), or through total event
    handlers.
- **Scenario:** an A `ConcurrentCommand` with `aggregateId = X`, which needs no version.
- **Mechanism:**
  1. A's owner restores X from B's snapshot; the missing fields read as null or zero.
  2. A's handler runs, and `add_events` appends A's events to X.
  3. At passivation, A writes its state as X's snapshot.
- **Impact:**
  - B's X no longer restores, or restores to a state of nulls and keeps taking commands.
  - A's projections receive B's events as type A and record failures. These then block X's squash and permanent
    delete.
  - `GetAggregate(X)` through A's bus reveals B's fields that share a name with A's.
  - Nothing fails when the bad events are written.
- **Validation:** two snapshot contexts of `version` 1. Create and passivate B's X. Then `GetAggregate(X)` on A's bus
  succeeds, and an A concurrent command is stored.
- **Fix:**
  - The restore reads `type_id` with `history_generation` (same row) and refuses a mismatch, naming both types.
  - `add_events` and `add_undo_event` add `AND type_id = …`, with a distinct error.
  - Snapshots record their state class and refuse a mismatch.
  - Trade-off: existing rows' type ids must match `AggregateType.of[R]` (the D75 renames). Run `StoredDataCheck`
    first, or only log mismatches for a while.
- **Regression test:** a query, a command and a squash sent to the wrong bus are refused, and X is unchanged.

#### F198 — An organization export is not one snapshot: a write during it gives two events at one version after the import
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `SpaceExport.scala:37-46, 58-60`: chains, types, detach events, aggregates and events are read in separate
    transactions under READ COMMITTED (`writeAggregates` 109-122, `writeEvents` 125-141, `writeArchived` 143).
  - `SpaceExport.scala:62-65`: the outbox count is read afterwards.
  - `SpaceImport.scala:101-113, 176-189`: versions are taken as given.
  - `PostgresEventStoreSchemaInitializer.scala:173-177`: `events (aggregate_id, version)` is not unique.
  - `PostgresEventStoreState.scala:188`: restores read `version <= base_version`.
  - ARCHITECTURE, SCALING and the M6 design all say the export is one read transaction.
- **Preconditions:** an organization written to during its export. The docs ask for a quiet organization, but nothing
  enforces it, and the squash scheduler, sagas and host timers still run.
- **Scenario:**
  1. The aggregates pass records X at version N.
  2. A command commits N+1.
  3. The events pass writes events 1..N+1.
  4. After the import, X has `base_version = N` and events 1..N+1.
- **Mechanism:**
  - The restore reads up to N. The next command expects N, the procedure finds N, and it inserts a second N+1.
  - A squash between the passes imports `squashed_version = NULL` over a squashed history. The guards of D49 and D50
    then compare with NULL and refuse nothing (F96 and F102 come back).
  - An undo between the passes imports noop marks that the aggregate row does not reach yet.
- **Impact:** silent corruption in the target:
  - restores apply both events N+1, in undefined order;
  - the sequence gate sees N+1 twice;
  - later `overwriteEvents`, `squash` and `detachDuplicate` fail on `updated != 1`;
  - read models diverge for good.
- **Validation:**
  1. Export with a `Writer` that blocks at the first aggregate line until a command on that aggregate commits.
  2. Import, then send a command to the aggregate.
  3. `SELECT version, count(*) FROM events WHERE aggregate_id = X GROUP BY 1 HAVING count(*) > 1` returns a row.
- **Fix:**
  - Read every pass in one `REPEATABLE READ READ ONLY` transaction, building the detach events from the same snapshot
    (through the session, or `pg_export_snapshot()`).
  - In the import's first pass, refuse an aggregate whose events are not exactly its range (F199).
  - Build a unique index on `events (aggregate_id, version)` concurrently, after checking that production has no
    duplicates.
  - Trade-off: the snapshot holds back vacuum for the export's length; the events cursor already does.
- **Regression test:** the validation gives a consistent file or a refusal, and a direct duplicate insert fails.

#### F199 — The organization import cannot tell a complete file from a truncated or inconsistent one, and trusts its references and class names
- **Severity** High · **Likelihood** Unlikely: it needs a damaged, inconsistent or crafted file, and the threat model
  includes files from another installation · **Confidence** Confirmed (code).
- **Where:**
  - `SpaceFormat.scala:42-44`: the format has no trailer.
  - `SpaceImport.scala:96-115`: the first pass checks the format, the classes, the sizes, and that each event's
    aggregate is in the file. It never checks versions, chain bases or lineage.
  - `SpaceImport.scala:186-189, 208-210`: the file's versions and `mapped(row.base)` are written as given. A base that
    is neither in the file nor colliding stays as it is.
  - `SpaceImport.scala:90`: `Class.forName(name)` initializes the class.
  - `SpaceImport.scala:161`: mpjsons builds any class, through a public no-arg constructor or
    `Unsafe.allocateInstance`.
  - `SpaceImport.scala:114`: archived lines are checked for their class only.
  - `SpaceImport.scala:110-111` against 165-166: sizes are measured on the source JSON, but written after ids are
    remapped, which makes them grow.
- **Preconditions:** a file cut by a failed export (it ends on a whole line), a cut transfer, a file from F198, or a
  crafted one.
- **Scenarios:**
  - **A (truncated):** an aggregate line says N, but its events end at M < N.
  - **B (references):** an aggregate line has a chain row whose base V is another organization's aggregate in the
    target.
- **Mechanism:**
  - **A:**
    - The owner restores M. Every command expects M against the stored N, so each gets a concurrency error, retried
      and failed, forever.
    - A cut in the archive section drops audit events silently.
    - The report says complete.
  - **B:**
    - X's restore joins `events.aggregate_id = aggregates.base_id`, so X shows V's state.
    - The space rebuild publishes it into the importing organization.
    - A later squash or delete of V detaches X, copying V's data into X for good.
  - **Both:**
    - Initializers of named classes run in the first pass.
    - Root classes with no command bus make the space unpurgeable.
    - Every class name takes a SMALLINT type id (F247).
    - Archived events of a missing aggregate, and events that grow past a limit when ids are remapped, fail the
      second pass after everything else was written.
- **Impact:**
  - A: aggregates that cannot be written until repaired in SQL; lost audit events, while the source may already be
    purged.
  - B: one organization reads another's data.
  - Both: hours lost, and a purge before a retry that fails the same way.
- **Validation:**
  - Cut the last lines of an export and import it: the report says complete, then every command fails.
  - Import a crafted single-aggregate file with a chain row to another organization's aggregate: `GetAggregate(X)`
    equals V's state.
- **Fix:**
  - A trailer line with counts (and a checksum); refuse a file without it.
  - In the first pass, per aggregate, require:
    - each version from `squashedVersion.getOrElse(1)` to `version`, exactly once;
    - archived versions ≤ the version;
    - chain and lineage bases inside the file, with chain orders 1..n;
    - sizes checked on the remapped JSON.
  - Load classes with `Class.forName(name, false, loader)`. Require event classes to extend `Event`, and root classes
    to belong to contexts given to the import.
  - Trade-off: format version 2, and the import takes the contexts, as the export does.
- **Regression test:** a truncated events section, a truncated archive, an extra version and a foreign chain base are
  each refused before any write, with no class initialized.

#### F200 — The organization purge does not guard the platform space (0) or the legacy space (-1)
- **Severity** High · **Likelihood** Unlikely: an operator or admin-form error · **Confidence** Confirmed (code).
- **Where:**
  - `SpacePurge.scala:57-72` (no check) and 112-126.
  - `PostgresEventStoreState.scala:543-547`.
  - Rows written before spaces existed have space -1 (`PostgresEventStoreSchemaInitializer.scala:134`,
    `SpaceId.scala:5-7`).
- **Preconditions:** `purge(SpaceId(0))` or `purge(SpaceId(-1))`. For example, from an admin form whose empty value
  becomes 0, or from `SpaceId.unknown`.
- **Mechanism:**
  - `purge(0)` first detaches every other organization's copy of a platform aggregate, rewriting their first events.
    Then it deletes the platform's aggregates.
  - `purge(-1)` deletes every aggregate stored before `backfillSpaces`.
  - Batches commit one by one.
- **Impact:** data loss across the whole installation, irreversible.
- **Validation:** `purge(SpaceId(0))` on a test schema with platform aggregates deletes them.
- **Fix:** refuse 0 and negative spaces unless an explicit option allows them, and/or take the expected aggregate count
  as a confirmation. Trade-off: none.
- **Regression test:** `purge(0)` and `purge(-1)` are refused, with no row changed.

#### F201 — A stale failure record becomes a false "versions missing" failure; skipping it moves the subscription past the aggregate, and the next event is acknowledged unapplied
- **Severity** High · **Likelihood** Unlikely per aggregate; Possible during mass retries · **Confidence** Confirmed
  (code).
- **Where:** `ProjectionActor.scala`:
  - 743-749: a retried failure is cleared in its own autocommit after the batch. If that fails, it is only logged:
    "retried again at the next start".
  - 992-1003 `loadFailures`, and 1024-1029 `retry`.
  - 682: with nothing pending, a retry asks from `applied + 1`.
  - 669-670: a delivery at or below the applied version is acknowledged.
  - 1043-1044: `skipFailedVersions` advances to the failure's version.
  - 420-424: `ServeUnchanged` forgets failures, and a later `ConnectedTo` does not reload them.
  - Also `AggregateOwner.scala:868-878`: there is nothing to re-publish beyond the aggregate's version.
- **Preconditions:** a `subscription_failures` row at or below the stored subscription version. It arises when:
  - the clear after a successful retry failed;
  - `loadFailures` failed at start (on a single node it is not asked again);
  - `ServeUnchanged` dropped failures;
  - `recordFailure` committed but its answer was lost.
- **Scenario:** after a fix, `RetryAllFailedSubscriptions` applies v7–v9 of cart 42, but the clear hits a pool
  timeout. The cart stays idle.
- **Mechanism:**
  1. Until a restart the row is invisible: the status, retry and skip act on in-memory failures only. Yet it blocks the
     cart's squash and permanent delete.
  2. After a restart, the retry asks from v10. The owner has nothing beyond v9.
  3. 10 minutes later the projection records "versions from 10 are missing". From then on, every event of the cart is
     answered `MessageFailed`.
  4. An operator who skips it moves the subscription to v10, while the cart is at v9.
  5. The next command writes v10, which is acknowledged without being applied.
- **Impact:**
  - An event lost for good for event listeners (until the next event, for state listeners), with nothing recorded.
  - Meanwhile the aggregate's events are refused, and its deletes (GDPR flows) and squashes are blocked.
  - The stored error points at the wrong cause.
- **Validation:** with a subscriptions state whose `clearFailure` throws once:
  1. Fail at v3, then retry.
  2. Restart with a short give-up.
  3. Skip, then publish v4. The listener never sees v4.
- **Fix:**
  - When `applied >= failure.version` in `retry` or `loadFailures`, clear the row at once and have the owner
    re-publish.
  - Keep `retryOf` until the clear succeeds.
  - Never let a skip advance past the aggregate's stored version.
  - Let the owner answer a range past its version with "nothing after N".
  - Retry `loadFailures` on a timer, and keep failures across `ServeUnchanged`.
  - Trade-off: one read per retry, one more message type.
- **Regression test:** the validation, plus failures kept across `ServeUnchanged`.

#### F202 — With `ServeStale`, an abandoned build leaves the active build missing every event committed during the build
- **Severity** High · **Likelihood** Unlikely: an opt-in option plus a rollback during a build · **Confidence**
  Confirmed (code).
- **Where:**
  - `Projections.scala:351-353`: `takesDeliveries`. Under `ServeStale`, only the build's instance is subscribed.
  - `Projections.scala:575-579`, `291-296` and `304`: abandon and drop, then the active instance serves live.
  - `ProjectionActor.scala:583-585`: while building, deliveries are acknowledged without being applied.
- **Preconditions:** `whileBuilding = ServeStale`, and a build in progress is abandoned: a rollback, or
  `RebuildProjection` and then a rollback.
- **Scenario:** deploy V+1 of a `ServeStale` projection. Its build runs for hours; then roll back to V.
- **Mechanism:**
  1. During the build, only the build's instance takes deliveries, and it acknowledges them unapplied, so the outbox
     rows go.
  2. At the rollback, the build is dropped and the active V build serves live again, with nothing reconciling it.
- **Impact:**
  - Every aggregate changed during the build stays stale until its next event, whose gap makes the owner re-publish.
  - Aggregates without later events stay stale indefinitely.
  - No failure, gap or status entry shows it.
- **Validation:** start a V+1 build that cannot finish, commit events, and restart with V. The active documents lack
  those events, and the status is clean.
- **Fix:**
  - When an active build that took no deliveries serves live again, run a reconciliation-only builder against it
    (positions with the bulk pass done and target `All`).
  - Or record from which event id it stopped taking deliveries.
  - Trade-off: a full aggregate walk after a rollback.
- **Regression test:** as the validation, with every document current after the restart.

#### F203 — Build table and sequence names are not checked against PostgreSQL's 63-byte limit: two builds can share one table, and retiring one drops the live table
- **Severity** High · **Likelihood** Unlikely · **Confidence** Confirmed (code); the truncation is PostgreSQL's
  documented behaviour.
- **Where:**
  - `ProjectionBuild.scala:27-41`: `checked(base) + record.suffix`, with no length check. The document store does
    check its own names (`PostgresDocumentStore.scala:34-38`).
  - `ProjectionRegistry.scala:73`: the suffix grows with the build number.
  - `ProjectionRegistry.scala:188-196`: retiring a build runs `DROP TABLE IF EXISTS <name>`.
- **Preconditions:** a `build.table` or `build.sequence` base of about 56 characters or more, and builds that
  accumulate (repairs, context-version rebuilds).
- **Scenario:** a 60-character base at version 3. `<base>_v3` (63 characters, active) and `<base>_v3_b2` (66, cut to
  `<base>_v3`) are one table.
- **Mechanism:**
  1. The new build's `CREATE TABLE IF NOT EXISTS` reuses the active table and replays history into it.
  2. At the switch, dropping the old build runs `DROP TABLE` on the same cut name, and removes the table the new
     active build uses.
  3. A document-store name that crosses 63 at a later build makes the store refuse in its constructor, and the build
     restarts every 10 s forever.
- **Impact:** a corrupted live read model during the build. After the switch, its state table is gone, and every
  listener fails with "relation does not exist".
- **Validation:** a 60-character `build.table` name, then `RebuildProjection`. `pg_class` shows one table; after the
  switch, none.
- **Fix:**
  - Refuse names over 63 bytes when the instance is created; the active build keeps serving.
  - Shorten the suffix (the registry holds the version), or hash long bases.
  - In `drop`, refuse an object whose first 63 bytes match a live build's name.
  - Trade-off: none.
- **Regression test:** as the validation: refused before any write, the active table intact.
- **Source:** https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

#### F204 — The id generator trusts its configuration: a too-high installation number or a lowered sequence step hands out wrong or duplicate ids
- **Severity** High · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:**
  - `PostgresUidGenerator.scala:27`: `poolSize` is read once, at start.
  - `PostgresUidGenerator.scala:34-43`: a pool is `[NEXTVAL, NEXTVAL + poolSize)`.
  - `PostgresUidGenerator.scala:46-54`: `moveInto` runs `if (next < r.from) setval(…, r.from)`, which moves the
    shared sequence up into the node's range.
  - `UidGeneratorSchemaInitializer.scala:26-29`: `ALTER … INCREMENT BY 10000` at every start.
- **Preconditions:**
  - **A:** a node starts against the installation's database with a higher `installation-number` (a copied
    configuration).
  - **B:** the sequence step is lowered: by a DBA, by a rollback from a later version with larger pools, or by a later
    version with smaller pools running next to 1.0 nodes.
- **Mechanism:**
  - **A:**
    1. The wrong node moves the sequence into its range.
    2. Every correct node's next pool then lies past its range's end. Every creation without a chosen id, and every
       `ReserveAggregateId`, fails, and each retry still consumes 10 000 ids.
    3. A correct node that restarts refuses to start, with a message that blames its (correct) number.
  - **B:** a node with a cached size of 10 000 hands out `[v, v + 10 000)` while another node's `NEXTVAL` returns
    `v + 5 000`, inside that pool. F23's "no id twice" holds only when the step grows.
- **Impact:**
  - **A:** creation stops installation-wide until a DBA runs `setval`, and the wrong node's aggregates carry another
    installation's ids.
  - **B:** duplicate ids:
    - aggregate ids: conflicts;
    - command ids: the owner's pending map collides, so one caller gets another's answer and the other gets none;
    - saga ids: step keys collide, so a step is answered with another saga's response and not applied.
- **Validation:**
  - **A:** generator(0) draws a pool; generator(3) is constructed; generator(0) then fails.
  - **B:** two generators of 10 000, then `ALTER … INCREMENT BY 5000`; alternate draws overlap.
- **Fix:**
  - Record the installation number in the database at the first move, and refuse a node whose number differs. Move
    the sequence only once.
  - Never lower the step at start (`GREATEST(current, 10000)`). Read the step with each draw, and use the smaller of
    it and the cached size.
  - Trade-off: renumbering becomes an explicit operation, and each pool costs one catalog read.
- **Regression test:** a wrong node fails at start, with the sequence untouched; pools stay disjoint after a lowered
  step.

### 3.4 Sagas and drafts

#### F205 — A saga step whose command timed out is reverted, but the command can still be applied later, and nothing undoes it
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code). The late application is the library's
  documented behaviour (SCALING.md:90-99).
- **Where:**
  - `SagaRunner.scala:192-195`: a failed step answers and stores REVERTING.
  - `SagaRunner.scala:74`: `Timeout(60.seconds)` for `SagaActor` sagas. The book's sagas also use 60 s
    (`12-sagas.md:239, 455, 495`).
  - `AggregateOwner.scala:328-339`: the command's own timeout counts from its arrival at the owner.
  - `CheckoutSaga.scala:53-58`: every other answer is a failure.
- **Preconditions:** a step's ask timeout no longer than the command timeout plus the command's delay before it
  reaches its owner. Delays come from:
  - a shard hand-over (up to 90 s);
  - a passivated `Local` owner;
  - a `FirstCommand` waiting for ids;
  - a command that finishes near 60 s.
- **Scenario:** in a rolling restart, ChargePayment waits 70 s in the region while its shard is handed over. The
  saga's ask fails at 60 s.
- **Mechanism:**
  1. The runner answers failure, stores REVERTING, and reverts the earlier steps: it releases the items.
  2. At 70 s the charge reaches its owner and is applied, since the command's own 60 s count from its arrival.
  3. Reverts only undo earlier steps, and their keys differ.
  4. The book's pattern also turns retryable answers into a revert: `CommandRejected` (D67's write switch during a
     rollout), `CommandHandlingError`, and `EventHandlingError` (a transient error, or a commit whose acknowledgement
     was lost, F252).
- **Impact:** a silent inconsistency. The customer is charged, the items are released, and the caller is told the
  checkout failed. No error is logged.
- **Validation:** a command bus that delays the command 2 s, with a step timeout of 1 s. The command is applied, and
  the revert ran as well.
- **Fix:**
  - On a step failure, run the same step again (same `SagaStep` key) with back-off, a bounded number of times, before
    answering and reverting. A late application then returns its stored response.
  - The book maps ask timeouts, `CommandRejected`, `CommandHandlingError` and `EventHandlingError` to "retry".
  - Set step timeouts above the command timeout plus a hand-over: over 150 s.
  - Trade-off: failure answers come later.
- **Regression test:** the validation continues forward instead of reverting.

#### F206 — A saga answered "failed" can run forward to success at the runner's next start, days later
- **Severity** High · **Likelihood** Possible: the database unavailable for about 51 s or more while sagas run, as in
  a failover · **Confidence** Confirmed (code).
- **Where:**
  - `SagaRunner.scala:189-195`: the answer is sent before the REVERTING write.
  - `SagaRunner.scala:246-252`: when write retries run out, a CreateSaga is refused. Anything else is logged ("it
    resumes from its last stored step after a restart") and the saga is dropped from memory.
  - `SagaRunner.scala:116-118`: sagas are loaded only in `preStart`.
  - `AggregateOwner.scala:448, 465-473, 500`: only stored successes are recorded under the idempotency key.
  - `PostgresSagaState.scala:23-30`: INSERT without `ON CONFLICT`.
  - Write retries give up after 51.1 s (computed from `Backoff(100 ms, 30 s, 10)`).
- **Scenario:**
  1. During a 70 s failover, ChargePayment gets `EventHandlingError`, the step fails, and the caller is told
     "refused".
  2. The REVERTING write fails 10 times, and the saga is dropped from memory. Its row is still at step 2.
  3. The user retries, and a new checkout succeeds.
  4. Weeks later the runner restarts and runs step 2 again, with key `id|2`. No response was stored for the failure,
     so the charge is applied a second time.
- **Variants:**
  - A CreateSaga whose commit acknowledgement was lost is retried into the unique index. Its caller hears "the saga
    could not be stored", but the row runs at the next start.
  - At a hand-over, the old runner's INSERT can commit after the new runner has loaded. That saga waits until the next
    start, and its caller never gets an answer.
  - A resume after the 30-day response retention re-applies a step.
- **Impact:** a duplicated business operation, out of context, contradicting what the user was told. One ERROR line is
  the only trace: there is no saga status query or metric.
- **Validation:** a saga state that fails the REVERTING write 10 times, and a step that refuses once and then succeeds.
  Restart the runner: the saga completes forward.
- **Fix:**
  - Never drop a saga while its runner lives: retry at the 30 s cap, with an ERROR and a metric.
  - Store REVERTING before answering a failure.
  - Create with `ON CONFLICT (name, saga_id) DO NOTHING`, an existing row counting as stored.
  - Drain in-flight writes before the runner stops.
  - Add `GetSagaStatus` and saga metrics.
  - Trade-off: failure answers wait for the database.
- **Regression test:** the scenario ends in REVERTING, and a CreateSaga retry that hits the duplicate key counts as
  stored.

#### F207 — Saga orders cannot be made idempotent, so a caller that retries during a singleton move starts the saga twice
- **Severity** High, for sagas that move money or stock · **Likelihood** Possible · **Confidence** Confirmed (code);
  the duplicate needs a client that retries.
- **Where:**
  - `SagaRunner.scala:131-134, 148-152`: every order gets a new saga id.
  - `Saga.scala:26-32`: `SagaOrder` has no key.
  - Pekko's singleton proxy buffers up to 1 000 messages and delivers them later.
  - SCALING.md:531 measured 48–49 s from a kill to the singletons running elsewhere.
- **Scenario:**
  1. The oldest node is killed. For about 49 s, orders wait in each node's proxy.
  2. A client times out at 30 s and retries through another node.
  3. When the runner starts, both proxies flush.
- **Mechanism:** two saga ids make different `SagaStep` keys, so both orders reserve and charge.
- **Impact:** the process runs twice, and both runs answer success. Nothing detects it.
- **Validation:** with the runner's node down, send the same order through two nodes. Once the runner starts, two sagas
  run.
- **Fix:**
  - An optional order key (`IdempotentSagaOrder.orderKey`), stored with a unique `(name, order_key)`.
  - A repeat joins the running saga, or gets its stored final answer, for the response retention.
  - Trade-off: a column, an index, and a clean-up.
- **Regression test:** a keyed order sent twice gives one saga and two identical answers.

#### F208 — A materialization that timed out lets the draft change again; the next one returns the older stored copy and drops the draft with its newer edits
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `AggregateOwner.scala:747`: `Timeout(commandTimeout)`.
  - `AggregateOwner.scala:754, 760`: a timeout clears `materializing` and answers `MaterializeRefused`.
  - `AggregateOwner.scala:330`: commands are refused only while materializing.
  - `AggregateOwner.scala:402-404`: a stored response is answered before anything runs.
  - `AggregateContext.scala:90-92`: the key is the ephemeral id only.
- **Preconditions:** the persistent side stores `StoreFirstEvent` more than `commandTimeout` (60 s) after it was
  sent. Causes:
  - a shard hand-over during a rolling restart (up to 90 s, SCALING §1–2);
  - a late id pool;
  - a saturated blocking dispatcher;
  - a slow database.
- **Scenario:**
  1. Materialize the draft at version 5. No answer comes within 60 s.
  2. The draft takes commands again, and the user edits it to version 7.
  3. The first `StoreFirstEvent` commits later, with the state of version 5.
  4. The user materializes again.
- **Mechanism:** the second `StoreFirstEvent` finds the key's stored response for the first copy, or loses the race on
  the key. The owner then drops the draft and answers `Materialized(id, P1)`.
- **Impact:** the edits made after the first attempt are silently lost. The book promises that "what is stored is
  exactly what the caller saw" (`20-ephemeral-aggregates.md:76-77`).
- **Validation:**
  1. A persistent-bus probe holds the first `StoreFirstEvent` past the timeout, then forwards it.
  2. Edit the draft, and materialize again.
  3. The stored aggregate lacks the edit, and the draft is gone.
- **Fix:**
  - After an outcome that is not known, keep the draft frozen (commands refused), and re-send the same idempotent
    request until a definite answer comes.
  - Or key by (id, version), and refuse without dropping the draft when the stored response is for another version.
  - Trade-off: a draft stays read-only while the persistent side is unreachable.
- **Regression test:** the validation, plus both `StoreFirstEvent`s in flight at once.

#### F209 — Ephemeral id tokens repeat across starts, so materialization keys collide within the response retention
- **Severity** High: a pointer to another draft's aggregate, possibly another organization's · **Likelihood** Unlikely
  · **Confidence** Confirmed (code); the probability is estimated.
- **Where:**
  - `AggregateCommandBusActor.scala:147`: `1 + Random.nextInt(2^29 - 1)` per start.
  - `AggregateCommandBusActor.scala:170`: ids follow the token from its block's top, so the first draft of every start
    gets the same id.
  - `AggregateContext.scala:90-92`: the key is `"materialize|" + id`, with no type and no start.
  - Responses are kept 30 days.
- **Mechanism:**
  - Two starts (any node, any type) that draw one token within the retention hand out the same ids.
  - Materializing id X in the later start returns the earlier start's stored `Materialized(X, P_old)`. The draft is
    dropped, and the caller is pointed at an aggregate of another draft.
  - Two nodes running at once with one token also accept each other's misrouted draft commands.
- **Probability (estimated):** the birthday bound S² / (2 · 2^29) for S starts within 30 days:
  - 1 000 starts: about 0.09 %;
  - 10 000 starts: about 9 %;
  - a host calling `Random.setSeed` makes a collision certain.
- **Impact:** a draft silently lost, and a pointer to someone else's data.
- **Validation:** force one token on two ephemeral buses, then create and materialize a draft on each. The second is
  answered with the first's aggregate.
- **Fix:** a 128-bit random value per start (a UUID or `SecureRandom`), with it and the type in `MaterializationKey`.
  Trade-off: none, as drafts never survive a restart.
- **Regression test:** the validation.

### 3.5 Document store

#### F210 — Document-store JSON paths and sort fields are written into SQL unescaped
- **Severity** High · **Likelihood** Unlikely. Neula's code builds paths from constants: a grep of its projections on
  18/10/2026 found one path made from a class name, and none from request data · **Confidence** Confirmed (code).
- **Where:** `PostgresDocumentStore.scala`:
  - 221-238 (sort), 240-259 (parts), 338-358 (`whereClause`), 124-153 (index paths).
  - Fragments such as `"document #>> '{" + path.mkString(",") + "}'"` go into `SQL(...)`.
  - Values are bound (F158, F159). Paths and sort fields are neither bound nor checked; compare the table-name check
    at 26, and `ProjectionBuild.checked`.
- **Preconditions:** a path element or sort field containing `}`, `'`, `,` or `\`. That happens with:
  - a JSON key holding such characters: map keys from user data, or custom attribute names;
  - a host that builds paths or sorts from request data ("sort by the column the UI picked").
- **Scenario:**
  - A path element `x}' = '' OR '1'='1` closes the fragment and appends SQL.
  - A key `a,b` becomes the two-element path `a → b`.
- **Mechanism:** only values go through `?`; paths are concatenated.
- **Impact:** SQL injection against every organization's data, if a path ever carries user input; otherwise, silently
  wrong results.
- **Validation:** `findDocumentByPaths(ExpectedSingleTextValue(Seq("name}' OR version>'0"), "x"))` runs the injected
  predicate.
- **Fix:** bind the path as a `?::text[]`, as `findDocumentByObjectInArray` has done since F158, or allow only
  `[A-Za-z0-9_]` in path elements and sort fields. Trade-off: an allowlist forbids JSON keys with punctuation.
- **Regression test:** a path element with `'`, `,` or `}` is bound or refused, and a key with a comma matches.

#### F211 — Document-store finders without a limit read the whole read model into memory
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `DocumentStore.scala:102`: `DocumentStoreQuery.basic` sets no limit.
  - `DocumentStore.scala:183-192, 213-219`: `findDocumentByPath`, `findDocumentByPaths` and
    `findDocumentsByPathWithOneOfTheValues` use it.
  - `PostgresDocumentStore.scala:233-234`: a `LIMIT` only when `limit >= 0`.
  - `PostgresDocumentStore.scala:261-270`, `418-426` (`findAll`), `437-468` (`getDocuments`).
- **Preconditions:** a large read model: millions of rows in a model across organizations, or one big tenant.
- **Scenario:** a query handler calls `findDocumentByPath(Seq("status"), "active")`, the most natural call, on such a
  table.
- **Mechanism:** the whole result is fetched with `.list()`, and each document is deserialized (and cached, when the
  store caches). Meanwhile the projection actor answers and applies nothing.
- **Impact:** a long GC pause, or an OutOfMemoryError that takes the node down (a rebalance, in a cluster). A query
  that was cheap while the model was small becomes fatal as a tenant grows. The book warns only about `findAll`.
- **Validation:** two million rows, and `findDocumentByPath` with a bounded heap.
- **Fix:**
  - Give the convenience finders a store-level default cap. Beyond it, fail, or truncate with a log line and a metric.
  - Keep an explicit unbounded call for small models.
  - Document it in SCALING.
  - Trade-off: a cap must be loud, or it hides rows.
- **Regression test:** with N+1 rows and a cap of N, an error or a truncation signal; `findAll` on a small model still
  returns everything.

## 4. Performance and scalability risks

#### F212 — Every bus tick scans and copies all deliveries, so a long lag slows the node's only bus
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code) for the O(n) work; the size is
  estimated.
- **Where:**
  - `EventsBusActor.scala:330-338`: the tick.
  - `EventsBusActor.scala:345, 559, 575, 581, 591, 606`: counts, filters and `toSeq` copies of `deliveries`.
  - The same copies in `subscriberStopped`, `removeSubscriber` and `completeFor` (519, 537, 542).
  - `FlowControl.scala:130`.
- **Preconditions:** a subscriber behind its window for minutes at a high commit rate.
  - SCALING §5 estimates up to about 6 million held keys.
  - The poller adds up to 1 000 aggregates × 1 000 events per poll (`AggregateOwner.scala:868-876`).
- **Mechanism:**
  1. Each 5-second tick makes 3–4 passes and 1–2 full copies, a tuple and a list cell per entry: about 290 MB at
     6 million entries (estimate).
  2. That is roughly 0.5–1 s of the bus's single thread per tick (estimate).
  3. Publications and acks of every subscriber on the node wait behind it. That lengthens the lag: a feedback loop.
- **Impact:**
  - Delivery to every subscriber on the node slows, during the backlogs flow control was meant to absorb.
  - Long GC pauses hit the owners too.
  - Only a profile shows it.
- **Validation:**
  - Window 1, and a probe that never answers.
  - Send 1, 2 and 4 million single-event publications.
  - Time a status request right after a tick, and record allocations with JFR.
- **Fix:**
  - Keep deliveries in a `LinkedHashMap` in first-sent order, so expiry pops from the head.
  - A queue by last send for the stall lines.
  - Incremental counters for the gauges, and a per-subscriber index for stops and removals.
  - Iterators or `filterInPlace` instead of copies.
  - Trade-off: more invariants to keep in step.
- **Regression test:** the tick's time is bounded independently of the number of entries (a micro-benchmark at
  5 million).

#### F213 — A command expecting a version above the owner's forces a full restore on every attempt
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `AggregateOwner.scala:406-407`: `if (expected.exists(_ > version)) restore() else caughtUp()`.
- **Preconditions:** the host passes a client's version as `expectedVersion` (optimistic locking).
- **Scenario:** a client sends `expectedVersion = Int.MaxValue` 50 times per second to an aggregate of 10 000 events.
- **Mechanism:**
  - Each attempt replays the whole history, then answers a conflict. A replay of 50 000 events took 392 ms (measured,
    SCALING §3); 10 000 events take about 80 ms (estimate).
  - A failed restore also puts the owner in its failed state for 5 s.
- **Impact:** a cost the client controls. 32 such aggregates saturate the node's blocking threads and connections.
- **Validation:** N such commands give N restores (`reactivecqrs.restore`).
- **Fix:** compare the stored version first, with one key lookup. Restore only when the store really is ahead, and
  then incrementally (F214); otherwise answer the conflict at once.
- **Regression test:** N such commands give 0 restores and N conflicts.

#### F214 — A `Local` owner on several nodes replays its whole aggregate before each read or command that follows another node's write
- **Severity** Medium · **Likelihood** Possible; Likely for `Local` types without snapshots used through several nodes
  · **Confidence** Confirmed (code).
- **Where:** `AggregateOwner.scala:317-324`. `caughtUp` calls `restore()`, a full replay
  (`AggregateRestorer.scala:50-58`), although the owner already holds the state at its version.
- **Scenario:** node A writes aggregate X every second while node B serves X's reads.
- **Mechanism:** H10 and SCALING count F131's cost as "a key lookup each". The replay that follows another node's
  write is not counted.
- **Impact:** every read on B is a full replay, about 80 ms for 10 000 events (estimate), and B's owner of X
  saturates.
- **Validation:** two nodes alternating 100 times give 100 replays.
- **Fix:**
  - Catch up incrementally when the history generation is unchanged and no squash point lies above the owner's
    version: apply only the events after its version, as the snapshot path does (lines 45-48). Otherwise restore.
  - Document snapshots or `Sharded` for such types.
- **Regression test:** at most one replay on the reading node, and the states equal a replay (a property test with undo
  and squash on the other node).

#### F215 — Operations on a base with many copies replay the base once per copy and hold every copy's row lock throughout
- **Severity** Medium · **Likelihood** Possible · **Confidence** Strongly suspected. The locking is confirmed in code;
  the durations are estimated.
- **Where:**
  - `AggregateOwner.squashHistory` 711 → `detachEvents` → `AggregateRestorer.scala:102, 113-119`: the base's history
    is replayed once per copy.
  - `PostgresEventStoreState.scala:470-472`: `lockChain` locks `FOR UPDATE` every row of every aggregate reading the
    base, including each copy's own row, which that copy's `add_events` updates.
  - The lock is held through:
    - `rewriteEverywhere` (674), with the host's `alter` and `OutboxWriter.rewrite` in the same transaction;
    - `deletingPermanently` (496), `squash` (767) and `detachDuplicates` (522).
  - `SquashScheduler.scala:92-94`: moves on after its answer timeout while the squash goes on.
- **Scenario:** erasing, squashing or deleting a template with 10 000 copies (SCALING §4's example) while 32 commands
  arrive for copies on one node.
- **Mechanism (estimates):**
  1. 2 000–10 000 replays and about 60 000 statements in one transaction: tens of seconds to minutes.
  2. Each copy's command waits on its row, pinning a blocking thread and a connection.
  3. At 32 waiting commands, the node's blocking dispatcher stops: owners of every type, projections, the poller.
  4. The pool's other borrowers fail after 5 s.
- **Impact:** a node-wide stall (across nodes, for sharded copies), and command timeouts the owners cannot answer
  (F186).
- **Validation:** a base with 2 000 copies. Rewrite a base of 20 000 events while 64 commands hit 64 copies, and
  compare their latency with the transaction's length.
- **Fix:**
  - Compute the base's state once per distinct base version.
  - Lock the base's own rows `FOR UPDATE` and the readers `FOR KEY SHARE`, which stops duplications but not commands.
    Raise the readers' generation in one short final statement.
  - Detach copies in short batches before the squash or delete; each copy keeps its state (D85).
  - Give persist transactions a `lock_timeout`, and have the scheduler wait for a running squash.
  - Trade-off: batched detaches are not atomic with the squash, which is harmless, as each copy keeps its state.
- **Regression test:** copies' commands finish within a bound while a long rewrite of their base runs, and base
  replays are at most the number of distinct base versions.

#### F216 — Push subscriptions do not scale: every update runs every filter of its type, expiry and cancel are linear, and nothing is bounded
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code); the times are estimated.
- **Where:** `SubscribableProjectionActor.scala`:
  - 81-83: one `List` per type.
  - 148-174: `deliverUpdate` runs every subscription's filter of that type for each update.
  - 185-207 (expiry) and 209-228 (cancel): `length` and `filterNot` over the whole list, per subscription.
  - 100-126: no cap, and listeners are not watched.
  - 83, 150-152: the updates cache is bounded by time only.
- **Scenario:** 50 000 open pages (chapter 11's pattern), 1 000 updates per second. A web node with 15 000 sessions
  dies, and 10 minutes later its subscriptions expire.
- **Mechanism (estimates):**
  - 5·10^7 filter calls per second on one thread.
  - The expiry costs about 1.5·10^9 list steps: seconds to tens of seconds, with the projection blocked.
  - A graceful stop of that node sends 15 000 cancels, each linear.
  - A client looping on subscribe grows the map without limit for 10 minutes.
  - Dead listeners get pushes until they expire.
- **Impact:** projection lag and unanswered queries, for every tenant, during web-tier restarts or client bugs.
- **Validation:** an actor benchmark: expire 15 000 of 50 000 subscriptions in one tick, and measure the lag with
  20 000 subscriptions at 1 000 updates per second.
- **Fix:**
  - A per-type `LinkedHashSet` or map.
  - An optional routing key in subscribe and send.
  - Watch listeners.
  - Caps per projection and per listener.
  - A cache bounded by count and bytes.
- **Regression test:** the benchmark within a time budget, and a terminated listener's subscription removed.

#### F217 — A large targeted rebuild, a purge's last step, or a retired build's drop each runs as one long operation on the projection's thread
- **Severity** Medium · **Likelihood** Possible: each import ends in `RebuildAggregates(OneSpace)` (D40) ·
  **Confidence** Confirmed (code); the sizes are estimated.
- **Where:**
  - `ProjectionActor.scala:430-439`: `aggregatesOf()` on the actor's thread, then one transaction for
    `clearForRebuild` and `resetVersions`, then `toSet`.
  - `ProjectionActor.scala:447-455`: `PurgeSpace` clears in one transaction.
  - `Projections.scala:524-527`: every target id is read into a `Vector`.
  - `Projections.scala:631-638`: the retired build's drop.
  - `SubscriptionsState.scala:443-455`: one DELETE of all the retired build's subscription rows.
  - `SubscriptionsState.scala:463-474`: every delete in one transaction, with the cache evicted under the monitor that
    all projections share.
  - `PostgresDocumentStore.scala:73-80, 556-564`: no index on `space_id`, so `clearSpace` scans the whole table.
- **Scenario:** `RebuildAggregates(OneType)` over 10 million aggregates, or the switch of a projection with 10–20
  million subscription rows.
- **Mechanism (estimates):**
  - About 10 000 page reads: up to 17 minutes without F173's index.
  - About 450 MB of ids.
  - One transaction deleting millions of rows, plus a full scan per document table.
  - The drop's DELETE runs on the supervisor's thread: after the switch, at start (the F116 sweep) and in `retire`,
    under the global `SchemaLock`.
- **Impact (estimates):**
  - For minutes up to an hour, the projection answers and applies nothing, for every tenant.
  - An OOM risk on 1 GB heaps (E2's size).
  - The long transaction holds back vacuum across the database.
- **Validation:** time a `OneType` reset over 1 million aggregates (handling time, heap, transaction length); run
  `EXPLAIN` on `clearSpace`; time a switch with 10 million rows.
- **Fix:**
  - Page the reset and the clear in short transactions, driven by self-messages; the pending target is already
    re-runnable. The builder follows the pages.
  - Delete a retired build's rows in batches, off the supervisor's thread.
  - Add an index on `space_id`, built concurrently.
  - Trade-off: the target is partly cleared for a while, as it already is while rebuilt.
- **Regression test:** query latency stays under 1 s and heap stays bounded during a large reset and a large drop.

#### F218 — The `Local` owners' sweep passivates in unpaced bursts, and each stop writes a snapshot on the shared blocking dispatcher
- **Severity** Medium · **Likelihood** Possible: `Local` is the default placement, and `onPassivation = true` the
  default · **Confidence** Strongly suspected. The mechanism is confirmed; the size is estimated.
- **Where:**
  - `Owners.scala:104-116`: every 5 minutes, every owner idle for over 2 minutes beyond the 200 most recent is
    stopped, in one loop.
  - `AggregateOwner.scala:676-679`: a stopping owner writes a snapshot when its version is above its snapshot's. After
    a restore from events that is 0, so owners that were only read write one too.
- **Scenario:** 20 000 aggregates of one type touched within 5 minutes on a node.
- **Mechanism:**
  1. One sweep sends 20 000 stops, which queue 20 000 snapshot upserts on the 32-thread, first-in-first-out
     dispatcher.
  2. Every command of every type waits behind them: about 0.6–1.9 s, at 1–3 ms each (estimate).
  3. Id-pool refills queue behind them too.
- **Impact:**
  - A latency spike every 5 minutes.
  - Client timeouts at large counts.
  - Snapshot churn when `keepAliveLimit` is small.
- **Validation:** a bench with a `Local` snapshot type and 20 000 owners. Measure the p99 of another type's commands
  for 10 s after a sweep.
- **Fix:**
  - Stop at most K owners per tick.
  - Skip passivation snapshots below a number of events since the last one, or write them on a separate small pool.
  - A larger default `keepAliveLimit`.
- **Regression test:** a sweep stops at most K owners at once, and the bench's p99 stays within its bound.

#### F219 — `AfterCommit` keeps JVM-wide maps behind one monitor, on every projection's transaction path
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code); the contention is not measured.
- **Where:**
  - `AfterCommit.scala:19-21`: two static `IdentityHashMap`s.
  - `AfterCommit.scala:29, 39, 49, 59, 73`: every access is synchronized on one monitor.
  - Every projection transaction uses it (`SubscriptionsState.localTx:434`, `PostgresEventStoreState.localTx:859`),
    as does every document-store cache write (`cacheAfterCommit:493-504`).
- **Mechanism:**
  - Every commit and every cached read, across all projections, serializes on one monitor. The sections are short,
    but global.
  - The maps are shared by every actor system in the JVM, keyed by `DBSession` identity. That is correct only while
    ScalikeJDBC makes a new session per transaction.
  - The deferral holds only for transactions opened through `AfterCommit.localTx`. A bare `DB.localTx` (in
    `ProjectionRegistry`, `SpacePurge`, `SpaceImport`) applies cache writes at once.
- **Impact:** a hidden throughput ceiling with many parallel projections, and state coupled between actor systems in
  one JVM (tests, or a host embedding two).
- **Validation:**
  - Profile many projections on the 32-thread dispatcher, looking for contention on `AfterCommit$`.
  - Run two actor systems on one pool; their deferred actions must not mix.
- **Fix:** keep the deferred actions on the session (ScalikeJDBC's attributes), or in a thread-local scoped to the
  transaction; at least, shard the monitor.
- **Regression test:** two concurrent transactions each see only their own deferred actions, and a rollback drops them.

#### F220 — `backfillEventKeys` on a large store: a row lock and WAL per old event, a later full re-freeze, long scans that hold back vacuum, and runs that abort on concurrent deletes
- **Severity** Medium · **Likelihood** Likely: H11 asks every upgraded installation to run it once · **Confidence**
  Strongly suspected (PostgreSQL internals; not measured).
- **Where:** `PostgresEventStoreSchemaInitializer.scala`:
  - 204-205: `event_keys … REFERENCES events (id) ON DELETE CASCADE`.
  - 291-314: batches of 10 000, unpaced; whole-table anti-joins at 293 and 307-309; no retry.
- **Mechanism:**
  - Each insert's foreign-key check takes `FOR KEY SHARE` on its old event row. That marks the tuple, writes WAL and
    clears the page's all-frozen bit, so the next aggressive vacuum reads the whole table again.
  - The two anti-joins run for minutes and hold back vacuum on `events_to_publish`, `aggregates` and
    `subscriptions`.
  - An event deleted meanwhile by a squash or a purge fails the check, and the exception ends the run.
- **Impact (estimated):**
  - WAL in the order of the table's size, and replica lag.
  - Bloat on the hot tables.
  - Runs to restart by hand.
  - A later full freeze.
- **Validation:** on a production copy, compare `pg_current_wal_lsn()` and `pg_visibility_map_summary('events')`
  before and after; run a squash concurrently and watch the run abort.
- **Fix:**
  - Pace the batches, and retry a failed batch.
  - Drop the whole-table scans: the walk finds rows without keys by itself.
  - Measure it in the D81 rehearsal.
- **Regression test:** a backfill with concurrent deletes in its range completes.

## 5. Resource exhaustion and memory management risks

#### F221 — A subscriber stalled past the deadline is sent another window every deadline, so its mailbox is not bounded by the window
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `FlowControl.scala:116-123`: `expire` drops records older than the deadline, which frees their room.
  - `FlowControl.scala:41-45`: `admits`.
  - `EventsBusActor.scala:335, 375`.
- **Preconditions:** either:
  - a subscriber that stops answering without stopping, such as a listener blocked on a lock or an external call
    without a timeout;
  - or one slower than window / deadline, which is 10 000 events in 10 minutes (about 17 events per second), with a
    backlog.
- **Mechanism:**
  1. Ten minutes after each send, `expire` takes the record off.
  2. The messages are still in the mailbox, but `admits` sees room and sends a new window: live events, the poller's
     full re-publications (duplicates of what is already queued), and pulls.
  3. Nothing ties the reopening to an answer from the subscriber.
- **Impact:**
  - The mailbox grows by up to one window (64 MB of event bytes) per deadline per bus, about 384 MB/h (estimate),
    until OOM.
  - This contradicts SCALING §5 and §9's "a mailbox holds at most its window".
  - No data loss.
- **Validation:** window 10, deadline 2 s, and a probe that never answers. Publish a new aggregate's event every
  100 ms for 10 s. The probe receives about 50 messages, against a bound of 10.
- **Fix:**
  - Keep the expired counts as "presumed queued" until the subscriber answers anything, stops or is removed.
  - A subscriber that answered nothing within a deadline is stalled: hold everything for it (keys only), and pull once
    it answers.
- **Regression test:** at most `window` messages stay unanswered until the probe answers.

#### F222 — The byte window ignores the aggregate states that state subscriptions carry, and on another node each message holds its own copy
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `EventsBusActor.scala:363`: sizes are the events' stored sizes.
  - `EventsBusActor.scala:375, 414`: the window's accounting.
  - `EventsBusActor.scala:448, 470-471`: states go in `AggregateWithType` and `AggregateWithTypeAndEvents`.
  - `ClusterSerializer.scala:241-245`: the state is written as JSON in each message.
- **Preconditions:**
  - A cluster, with a projection singleton whose state listeners run on another node than the owners.
  - States of tens of KB.
  - The projection lagging.
- **Mechanism:** flow control counts events and their bytes. Each state-carrying message is serialized on its own, so
  on the receiver each holds a private copy of the state until its batch is applied.
- **Impact (estimates):**
  - 10 000 messages of 50 KB is about 500 MB per remote bus; with 248 KiB states, about 2.4 GB.
  - The singleton's node runs out of memory exactly when flow control should protect it.
  - In one JVM states are shared, so only a cluster shows it.
- **Validation:** a `TestCluster` of two nodes, a blocking state listener and 100 KB states. Compare the receiver's
  heap with `flow.status` bytes.
- **Fix:**
  - Count the state in `admits` and `sent` for the state kinds: measured for remote subscribers (`Measured.state`
    exists), estimated per type locally.
  - At least, document that `windowBytes` excludes states.
  - Trade-off: one mpjsons write per measured state.
- **Regression test:** with a 1 MB state and 4 MB of window bytes, at most 4 messages are outstanding.

#### F223 — The reconciliation and gap fills hold every missing event of a page of up to 1 000 aggregates in memory at once
- **Severity** Medium · **Likelihood** Possible · **Confidence** Strongly suspected; it depends on volumes.
- **Where:**
  - `ProjectionBuilder.scala:219-223, 310-319, 334-348, 364-371`: every chunk of a page is queued in `ready`.
  - `Projections.scala:343-347`: a build instance is subscribed before it has a slot.
  - `ProjectionBuilder.scala:122-124`.
- **Preconditions:** a build restarted after its bulk pass, and waiting for a slot. With `buildsAtOnce = 2`, after a
  deploy that rebuilds many projections, that can be hours, while its instance acknowledges deliveries unapplied.
- **Mechanism:** each reconciliation page reads all missing versions of up to 1 000 aggregates (up to 10 000 versions
  each), and their restored states, into `ready`. Byte limits bound only individual chunks.
- **Impact:** a heap spike. An OOM exits the JVM, and at restart the same build resumes with a larger backlog: a crash
  loop.
- **Validation:** a reconciliation over 1 000 aggregates, each 5 000 versions behind. Measure peak heap and
  `ready.size`.
- **Fix:** a cursor (aggregate, kind, next version) that produces items lazily, at most `chunksInFlight + 1` chunks
  ahead. The same for gap fills.
- **Regression test:** peak heap stays bounded whatever the backlog.

#### F224 — An ephemeral bus keeps every idempotent response in memory for 30 days, whatever happens to its drafts
- **Severity** Medium · **Likelihood** Possible, when draft commands carry keys · **Confidence** Confirmed (code).
- **Where:**
  - `AggregateCommandBusActor.scala:82`: `new MemoryCommandResponseState()`, with 30 days.
  - `AggregateCommandBusActor.scala:299-304`: dropping a draft leaves its responses.
  - `CommandResponseState.scala:33-53`: an immutable map, and an hourly `partition` of all of it under the lock.
- **Mechanism:** each keyed draft command adds an entry, kept for 30 days though it is useless once the draft is gone.
  `maxAggregates` does not bound these entries.
- **Impact (estimated at 250 bytes per entry):**
  - 1 keyed command per second is about 650 MB after 30 days; 10 per second, about 6.5 GB.
  - The hourly pass holds the lock that every keyed draft command needs.
- **Validation:** send N keyed draft commands, drop the drafts, and check the state's size after the idle time.
- **Fix:** keep responses only for the bus's idle time, or remove a draft's entries when it is dropped. Use a mutable
  map with an expiry queue.
- **Regression test:** entries go with their draft.

#### F225 — The organization export builds every cross-space detach event before writing, and binds their ids in one IN list
- **Severity** Medium · **Likelihood** Possible: organizations whose aggregates are copies of platform templates ·
  **Confidence** Confirmed (code); the sizes are estimated.
- **Where:** `SpaceExport.scala:41-46`: `WHERE a.id IN ($outside)`, then a restore and a squash event per copy, all
  held until the end.
- **Mechanism:**
  - N copies cost N restores and N full-state events in memory before the first line is written.
  - The IN list binds N parameters. pgjdbc refuses a statement with more than 65 535: the protocol counts parameters
    in 16 bits. This is documented driver behaviour, not tested here.
- **Impact:**
  - Above 65 535 such copies, the export is impossible.
  - Below that, the heap holds about N × the state size: 50 000 × 20 KB is about 1 GB (estimate), with a long phase
    before any output.
- **Validation:** a space with 70 000 copies of a base in another space. The export throws at the type query.
- **Fix:** a join instead of the IN list, and build each detach event when its version-1 row is reached.
- **Regression test:** 70 000 copies export with a bounded heap.

#### F226 — The organization import's write batches are bounded by count only
- **Severity** Medium · **Likelihood** Possible: classes with raised size limits (the bench uses 6 000 000
  characters) · **Confidence** Confirmed (code).
- **Where:**
  - `SpaceImport.scala:53`: `eventsPerTransaction = 10000`.
  - `SpaceImport.scala:216-221, 230`.
  - The M6 design says "one event in memory at a time"; F126 bounded the bus's parts by bytes.
- **Mechanism:**
  1. Up to 10 000 serialized events, and their batch parameters, are held before a flush. At 100 000 characters each,
     that is about 1–2 GB (estimate).
  2. An OutOfMemoryError is fatal, so the `Try` at line 201 does not catch it.
  3. Earlier batches stay committed, and no report is written.
- **Impact:** a failed import with no record of where it stopped.
- **Validation:** import 1 000 events of 1 000 000 characters with a 64 MB heap.
- **Fix:** flush on bytes too, for example 16 MB as F126 does.
- **Regression test:** the validation scenario completes.

#### F227 — An oversized cluster message is serialized in full before the marker replaces it
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `ClusterSerializer.scala:78-81, 93-99`: the message is written into a `ByteArrayOutputStream` and copied
  before it is compared with the limit; the JSON is first built as a String (line 245).
- **Scenario:** a 20 MB state is asked from another node, repeatedly.
- **Mechanism:** each ask holds about five times the message size at once (string, bytes, buffer, copy; estimate), on
  Artery's outbound lane, which delays every other message to that node.
- **Impact:** GC pressure or OOM, and latency spikes, only to send a 50-byte marker.
- **Validation:** measure the allocation of `toBinary` for a 50 MB value (ThreadMXBean).
- **Fix:** write through a counting stream that stops at the limit plus one, then write the marker.
- **Regression test:** allocation stays below twice the limit, and the marker is returned.

#### F228 — Status answers grow without bound, and fail between nodes during incidents
- **Severity** Low · **Likelihood** Possible, in an incident · **Confidence** Confirmed (code); the per-entry size is
  estimated.
- **Where:**
  - `OutboxPollerActor.scala:66`: up to 100 000 given-up aggregates.
  - `EventsBusActor.scala:181-182, 620`: up to 1 000 undelivered entries.
  - `ProjectionActor.scala:487, 1059-1061`: every failure, with its full error.
  - `MessageSize.scala:16-21`: the limit, 248 KiB.
- **Mechanism:** about 800 given-up aggregates, or about 250 failures of 1 KB, exceed the frame between nodes
  (estimates), and the asker gets `MessageTooLargeException`.
- **Impact:** admin tools fail when many aggregates are undelivered or failed, which is exactly in an incident. The
  gauges and logs remain.
- **Validation:** a `TestCluster` with the poller on node 1 and 2 000 given-up aggregates, asked through node 2's
  proxy.
- **Fix:** answer counts plus the first N items, with truncated errors, and add paged queries.
- **Regression test:** a successful answer with the totals.

## 6. Concurrency, deadlocks, race conditions and hangs

Closely related, and in §3: F186 (hangs on statements that never return) and F195 (a rewrite racing a running
handler).

#### F229 — The command timeout does not bound the answer time, and commands are persisted after their deadline
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `AggregateOwner.scala`:
  - 339: the timer.
  - 400-417: database reads per attempt, on the owner's thread.
  - 445-451: `HandlerFinished` persists a pending command whatever the time.
  - 481-543: the persist is synchronous.
  - 600-610: `timedOut` runs only when it is dequeued.
  - The client advice is at SCALING.md:109.
- **Preconditions:** a slow database (statements of 1–2 s, lock waits, I/O saturation), and commands queued on one
  aggregate.
- **Scenario:** 30 commands queued on one aggregate, at about 8 s of database work each (estimate).
- **Mechanism:**
  - `CommandTimedOut` waits in the mailbox behind other commands' database work.
  - A `HandlerFinished` queued before it is persisted, and answered with success after 60 s.
  - Timeout errors arrive late too.
- **Impact:**
  - Answers come after minutes: 30 × 8 s is about 4 minutes (estimate).
  - Clients that follow SCALING ("longer than 60 s") give up and retry; without idempotency keys, commands are
    applied twice.
- **Validation:** an event store decorator that sleeps 2 s per call, `commandTimeout` 5 s, and 10 commands on one
  aggregate. Record the answer times and the late persists.
- **Fix:**
  - Before opening the persist transaction, answer the timeout instead of persisting if the deadline has passed.
  - Let the command bus, or a small deadline actor, answer at the deadline; the owner skips the persist by the same
    check.
  - Correct SCALING's client advice: a client timeout above `commandTimeout` plus the longest transaction, and retries
    only with idempotency keys.
- **Regression test:** each answer comes within `commandTimeout` plus one transaction, and nothing is persisted after
  its deadline.

#### F230 — Registering a type name takes a second connection under a JVM-wide lock, from inside open transactions
- **Severity** Medium · **Likelihood** Possible · **Confidence** Strongly suspected for the stall; Confirmed (code) for
  the nesting.
- **Where:**
  - `TypesNamesState.scala:73-108`: `synchronized { loadAll(); DB.localTx/readOnly … }`.
  - Callers inside an open transaction:
    - `PostgresEventStoreState.scala:105, 110` (persist), 193 (a restore's streaming transaction), 746 and 773
      (squash, detach);
    - `SubscriptionsState.scala:502-504` (listener transactions);
    - `CommandResponseState.scala:128-133, 146`.
- **Preconditions:** a pool about as large as the blocking dispatcher (the book's advice), and one of:
  - a start under load, before the cache is loaded;
  - a deploy that adds classes;
  - a class that another node wrote first.
- **Scenario:** 32 threads, each in a transaction, ask for a type id not yet cached.
- **Mechanism:**
  1. Each thread holds its connection while it queues on the monitor.
  2. The thread inside needs one more connection. With the pool exhausted, it waits 5 s and fails
     (`EventHandlingError`), and the next thread repeats it.
  3. With a hung connection (F186), the lock is held indefinitely.
- **Impact:** node-wide stalls of seconds after deploys, and one failed command or restore per round.
- **Validation:** a pool of 2, and two transactions asking for a new name.
- **Fix:**
  - Load the cache in `initSchema()`.
  - Resolve ids before transactions open, or on the caller's session.
  - Never hold the lock across database I/O.
- **Regression test:** the validation, with no wait.

#### F231 — A failed version lookup in `caughtUp()` strands commands and queries, and drops the owner's state
- **Severity** Medium · **Likelihood** Possible: `Local` types on a node that is not alone · **Confidence** Confirmed
  (code).
- **Where:** `AggregateOwner.scala`:
  - 317-318: `eventStore.storedVersion` outside any `Try`.
  - Callers: 272 (`GetState`), 407 (`attempt`), 815 and 834-841 (min-version queries and their deadline), 277-282
    (squash, rewrite).
  - 300-307: `guarded` → `become(unrestored)`.
  - 355: a serial queue waits while a started command is pending.
- **Scenario:** a pool timeout, or a reset connection, during the key lookup.
- **Mechanism:**
  - The exception skips the answer.
  - The started command stays pending until its timeout; in serial mode the queue waits up to 60 s behind it.
  - At a min-version deadline, the expired queries are neither answered nor re-armed.
  - The next message triggers a full restore, which adds load during the overload.
- **Impact:**
  - Callers hit their own ask timeouts, which breaks F81's "answered at maxMillis".
  - Serial aggregates stall for up to `commandTimeout`.
  - Restore storms amplify the database's trouble.
- **Validation:** `storedVersion` throws once, with `Members` not alone. A `GetState`, a min-version query with a
  deadline and a serial command must each be answered within 1 s.
- **Fix:**
  - Catch the lookup's failure in `caughtUp`, and answer that one request: `CommandHandlingError`, or a failure for
    queries.
  - Keep the state, and always answer expired queries.
- **Regression test:** the validation.

#### F232 — One saga can run twice at once in one runner, and saga rows have no compare-and-set to stop the stale copy
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `SagaRunner.scala:36`: `LoadPendingSagas` is public.
  - `SagaRunner.scala:96-114, 128-134, 140`: orders are created while the first load is pending or being retried, and
    the load resumes every row of the name, including sagas already running.
  - `PostgresSagaState.scala:34`: `UPDATE … WHERE name = ? AND saga_id = ?`, with no step or phase check.
- **Triggers:**
  - The first load fails or is slow (the 5 s borrow wait while the new oldest node takes over every singleton), while
    buffered orders are created.
  - An operator sends `LoadPendingSagas` to revive sagas stranded by F206; its scaladoc invites that.
  - The old runner's writes land after a hand-over.
  - A split brain.
- **Mechanism:**
  - Two copies run one step with one key. Each writes without a check, and each answers the caller.
  - Refusals are not stored (F206), so the copies can diverge: one stores REVERTING and releases the items, while the
    other writes the next step over it and charges.
  - Progress can move backwards.
- **Impact:** double answers, duplicated effects, and a saga that is both reverted and completed. While H3 is open for
  Neula, every duplicate step applies its command twice.
- **Validation:**
  - A saga state whose first `loadAllSagas` fails, with an order sent before the retry: the step runs twice.
  - `LoadPendingSagas` sent during a slow step: the same.
- **Fix:**
  - Keep the set of running saga ids, and ignore loads of them.
  - Stash orders until the first load succeeds.
  - Make `LoadPendingSagas` private.
  - Write with compare-and-set (`… AND step = ? AND phase = ?`); a false result stops that copy.
- **Regression test:** both validations run each step once.

#### F233 — Readiness says "ready" before the node has started, and stays ready while it shuts down
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `Readiness.scala:17, 24`: `waiting` starts empty, and `ready = waiting.isEmpty`.
  - `EventsBusActor.scala:191`: the first `waitFor` happens when the bus is constructed.
  - E2's `startProcess` runs management, then bootstrap, then `startNode`, whose `initSchema()` waits up to 5 minutes
    on the schema lock before any bus exists.
  - `kubernetes.yaml:118-123`: no `initialDelaySeconds`; period 5 s; failure threshold 3.
- **Mechanism:**
  - Pods still waiting on the schema lock answer 200 on `/ready`.
  - One success makes a pod Ready, and three failures (15 s) are needed to undo it, so a rollout moves on early.
  - Nothing marks a leaving node not ready during its 90 s coordinated shutdown.
- **Impact:** the rollout outpaces the nodes, and traffic reaches nodes whose command buses do not exist yet, or are
  stopping.
- **Validation:** on a fresh system with no bus, `Readiness(system).ready` is true.
- **Fix:**
  - `waitFor("start")` when the extension is created, cleared by a `Readiness(system).started()` that the host calls
    once its actors exist.
  - A CoordinatedShutdown task in `before-service-unbind` that makes the node not ready.
  - Document `initialDelaySeconds`.
  - Trade-off: a host that never calls `started()` is never ready. That is fail-safe, and needs a release note.
- **Regression test:** not ready until `started()`, and not ready again once the shutdown begins.

#### F234 — Asynchronous delayed queries ignore their deadline while updates keep coming, and a search that throws drops every parked query
- **Severity** Medium · **Likelihood** Possible: under sustained load, or during a catch-up · **Confidence** Confirmed
  (code).
- **Where:** `ProjectionActor.scala`:
  - 495-496, 1116-1120, 1162-1178: `parkOrSearchAgain` searches again whenever updates were applied since the last
    search, and `searchAgain` never compares `until` with the time. Only parked queries reach the deadline.
  - 1082-1088, and 365 with 875-878: a query flushes the group, which raises `updatesApplied`.
  - 1090-1112: `replayQueries` empties the parked queries before its loop, which has no `try`.
- **Scenario:** a read-your-writes query (chapter 8, `maxMillis = 2000`) for a version that never comes, made while the
  projection works through a backlog. Or a pool timeout during a replay.
- **Mechanism:**
  - Under steady deliveries, the query is searched again on every pass and never parks.
  - A search that throws synchronously aborts the replay; that query and every one after it are never answered.
- **Impact:** callers hit their ask timeouts instead of the documented answer at the deadline, and repeated searches
  add load in the busiest periods.
- **Validation:**
  - A delivery for another aggregate every 2 ms, and a query for an unknown version with 200 ms. Expected: `None`
    within about 300 ms.
  - Two parked queries whose search throws on its second call, then a replay. Expected: both answered.
- **Fix:**
  - Answer at the deadline in `parkOrSearchAgain`.
  - Search again at most once, then park.
  - Wrap each search in a `try`.
- **Regression test:** both validations.

#### F235 — A switch that the registry reports as not done leaves the supervisor stuck until a restart
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:**
  - `Projections.scala:463-486`: on an exception, a check later; on `false`, only a log line ("no longer the build in
    progress … it does not switch").
  - Refused while the supervisor believes a build runs: 399-401 (`PurgeCheck`), 512-514 (targeted rebuilds), 561-563
    (`rebuild`).
- **Scenario:** the switch's `UPDATE` commits in autocommit, but the client sees an exception (a network blip). The
  retry then returns `false`.
- **Mechanism:** the registry says the new build is active, while the supervisor keeps the old one active and the new
  one "caught up", indefinitely. Nothing reads the registry again.
- **Impact:** until a restart:
  - queries go to the old build;
  - purges, targeted rebuilds (erasure) and `RebuildProjection` are refused;
  - `GetBuilds` contradicts the registry.
- **Validation:** a registry double whose `switch` commits and then throws.
- **Fix:** on `false` or an exception, read `registry.projection(name)` again. If the build is active, complete the
  switch in memory; otherwise, start again.
- **Regression test:** with the double above, the new build is served within `restartAfter`.

#### F236 — On an ephemeral bus, a first command served after the ids ran out blocks every later command for good
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `AggregateCommandBusActor.scala`:
  - 242-247, 258-259: ids are checked on arrival only.
  - 336-337, 347: `serveWaiting` stops at the head of the queue.
  - 368: an ephemeral bus never asks for an aggregate pool, so nothing ever ends the wait.
- **Scenario:** k ids are left in the 2^24 block, the command-id pool is briefly empty, and more than k first commands
  arrive.
- **Mechanism:** first command k+1 sits at the head of the waiting queue forever, and every later command queues
  behind it.
- **Impact:** every command on every draft of that bus goes unanswered until the node restarts, while the queue grows.
- **Validation:** a small block (a test hook) and a delayed command pool. Send k+1 first commands, then a command to an
  existing draft.
- **Fix:** answer such a first command with `CommandRejected`, and drop it from the queue.
- **Regression test:** the validation.

#### F237 — The snapshot generation guard is not atomic: a snapshot can be written over a history that changed meanwhile
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Strongly suspected (PostgreSQL's visibility rules);
  F119's test is sequential.
- **Where:**
  - `PostgresEventStoreState.scala:236-241`: the generation is checked by a non-locking `WHERE EXISTS` inside
    `INSERT … ON CONFLICT`, and `snapshots` keeps no generation.
  - `AggregateOwner.scala:679`: the passivation snapshot.
- **Scenario:** `Local` owners on two nodes, or a copy's owner while its base is rewritten. A passivation snapshot's
  statement starts before another writer's undo or rewrite commits.
- **Mechanism:** the snapshot either waits on the row the writer deleted and then inserts, or commits unseen by the
  writer's DELETE. Restores then start from the stale state.
- **Impact:** an undone event, or an erased value, comes back through the snapshot. F119 is reopened.
- **Validation:** a two-connection test that interleaves the snapshot insert with an undo's transaction.
- **Fix:** take `FOR SHARE` on the aggregate's own row in the guard; or store the generation with the snapshot and
  check it on read.
- **Regression test:** after the interleaving, no stale snapshot is used.

#### F238 — A node's subscription version cache goes stale for subscribers whose writer runs elsewhere
- **Severity** Medium · **Likelihood** Unlikely: it needs per-node subscribers that share the node's state with
  singletons · **Confidence** Confirmed (code).
- **Where:**
  - `SubscriptionsState.scala:341-344, 480`: a node's `PostgresSubscriptionsState` caches every subscriber's versions
    of an aggregate, loaded by any projection on that node.
  - `ProjectionActor.scala:669-670`.
- **Mechanism:**
  - Entries stay frozen for subscribers written on another node.
  - After a singleton moves to such a node, stale-low entries cause spurious gaps and failed groups. Listeners without
    a database session repeat their side effects.
  - Stale-high entries (after a purge and a re-import with the same ids) acknowledge deliveries without applying them.
- **Impact:** wrong or skipped updates. F48's "one writer keeps caches valid" does not hold across nodes.
- **Validation:** a singleton projection moves to a node whose per-node subscriber loaded its versions earlier.
- **Fix:** cache a subscriber's versions only on the node that writes them, or check a cached version against the
  database before acknowledging a delivery as a duplicate.
- **Regression test:** after the move, no spurious gap, and nothing acknowledged unapplied.

#### F239 — Ask paths are reused after a node restarts at the same address, so a late answer can reach another caller
- **Severity** Low · **Likelihood** Unlikely · **Confidence** Confirmed (code). Pekko's counter for temporary paths
  restarts at 0.
- **Where:** Pekko `LocalActorRefProvider.tempPath`: temporary paths carry no UID, and their counter starts at 0.
  F137 routes answers by the caller's path.
- **Scenario:** a caller's node restarts with the same host name (Compose, on-premise), and a late answer from before
  the restart arrives.
- **Mechanism:** the answer is addressed to a temporary path that a new, unrelated ask now holds.
- **Impact:** one user's result is delivered to another's request; in the worst case, across tenants. F137 is
  reopened.
- **Validation:** a two-node test that restarts the asker at the same address, with a delayed answer in flight.
- **Fix:** carry a per-start random token (or the system's UID) in reply addresses, and drop answers that do not match.
- **Regression test:** the validation drops the late answer.

## 7. Security vulnerabilities and abuse scenarios

**High findings in §3.** Two High findings are security problems:

- **F210:** SQL is built from document-store paths.
- **F199:** the import trusts a file's references and class names, so a crafted file lets one organization read
  another's data.

The main privacy risk is personal data in logs and failure records (F190, F191).

**What held.** Every value is bound, and every SQL identifier the library builds is checked against
`[a-zA-Z0-9_]`, except the document paths (F210). Metrics tags carry no ids. Error ids are random.

#### F240 — The cluster serializer's reader trusts its payload: it loads any class named in the bytes and allocates unchecked lengths
- **Severity** Medium · **Likelihood** Unlikely. This is outside the stated threat model, which trusts the cluster
  network; but E2's Artery port has neither TLS nor a NetworkPolicy · **Confidence** Confirmed (code).
- **Where:**
  - `ClusterSerializer.scala:345-354`: the product, module and JSON shapes load any class named in the bytes. Nothing
    checks for library classes on read, and reading `MODULE$` runs object initializers.
  - `ClusterSerializer.scala:401-405`: `str()` allocates `readInt()` bytes, unchecked.
- **Scenario:** anyone who can reach a node's Artery port sends a crafted frame.
- **Mechanism:**
  - Arbitrary classes are initialized on the receiver.
  - A corrupt or crafted length forces about 2 GB, an OutOfMemoryError follows, and with
    `jvm-exit-on-fatal-error` the JVM exits.
- **Impact:** a remote node crash, and class-initialization side effects, by anyone on the cluster network.
- **Validation:** feed `fromBinary` a frame naming an unrelated class, and one with a huge length.
- **Fix:**
  - On read, accept only library classes and classes the host registered.
  - Bound lengths by the frame size.
  - Enable Artery TLS and a NetworkPolicy in E2 and in the book.
- **Regression test:** both frames are refused, without the allocation.
- **Source:** https://pekko.apache.org/docs/pekko/current/remoting-artery.html

#### F241 — Any sender can cancel, renew or take over a push subscription by its id
- **Severity** Low · **Likelihood** Possible, if clients choose ids · **Confidence** Confirmed (code).
- **Where:** `SubscribableProjectionActor.scala:67-70, 106-109, 178-183`.
- **Mechanism:** subscription ids are not tied to their listener. A client that chooses or learns one can cancel
  another user's updates, or receive them.
- **Impact:** lost or stolen push updates. This is a host concern, but the library offers no guard.
- **Fix:** tie each subscription to its listener's `ActorRef` and refuse cancels and renewals from others; generate the
  ids in the library.
- **Validation and regression test:** a second listener's cancel of another's id is refused.

#### F242 — `StoreFirstEvent` is accepted by any persistent bus, and stores any first event without the command handlers
- **Severity** Low · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `AggregateContext.scala:96-97`; the persistent bus takes it for materialization.
- **Mechanism:** a host component that can send to a command bus can create an aggregate with any first event,
  bypassing the domain's checks.
- **Impact:** defence in depth only, as whoever can send messages to a bus is trusted anyway.
- **Fix:** accept `StoreFirstEvent` only from an ephemeral bus's materialization: a token both share, or a separate
  entry point.
- **Regression test:** a direct `StoreFirstEvent` from elsewhere is refused.

#### F243 — A conflict answer reveals another organization's aggregate and its version
- **Severity** Low · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `AggregateConcurrentModificationError.was`.
  - `IdRange.scala:24-28`: a chosen legacy id ahead of the sequence is allowed, and collides later.
- **Mechanism:** a command with a chosen id that exists in another space is refused with that aggregate's version.
  With F197, a command to another type's id reveals its version too.
- **Impact:** a small information leak across organizations: an id's existence and version.
- **Fix:** answer a conflict on an aggregate of another space or type with a generic refusal.
- **Regression test:** the answer carries no version for a foreign aggregate.

## 8. Input validation, boundary conditions and numerical issues

#### F244 — The version limit is checked on events that add no version: long aggregates can be neither erased, squashed nor permanently deleted
- **Severity** Medium (GDPR Art. 17) · **Likelihood** Possible; it depends on the stored data and the configured limit
  · **Confidence** Confirmed (code).
- **Where:**
  - `PostgresEventStoreState.scala:86-95`: `serializedChecked` throws `TooManyEventsException` when
    `version >= aggregateVersionLimit`.
  - It is called by `overwriteEvents` (144, with `e.version - 1`), `rewriteEverywhere` (701, with `c.version - 1`) and
    `squash` (773, with `n - 1`).
  - `AggregateOwner.scala:724-726, 789-791`: the refusals.
  - A `PermanentDeleteEvent` takes a version, so it is refused at the limit too. The `TooManyEventsException` doc
    (`Exceptions.scala:44-46`) mentions only growth.
- **Preconditions:** an aggregate at or above the store's limit (10 000 by default). That happens when:
  - 0.12.47 checked the version before the event (F125), so aggregates that hit its limit sit at 10 001;
  - histories older than the limit (09/04/2025) may be longer: F22 speaks of up to 500 000 events in Neula;
  - a host lowers the limit, or a tool builds a store with the default.
- **Scenario:**
  - erasing a name held by the last event of an aggregate at version 10 001;
  - `AutomaticSquash` on an aggregate of 50 000 events;
  - a permanent delete of such an aggregate.
- **Mechanism:**
  - The guard against growth throws for an event that only replaces a row.
  - The rewrite is all-or-nothing.
  - The squash event is built, with a restore, before the store throws, and the scheduler asks again every run.
- **Impact:**
  - Such aggregates cannot be erased, squashed (the documented remedy for long histories) or deleted.
  - The error blames "too many versions".
  - Each scheduler run wastes a restore.
  - The memory store has no such check, so tests cannot show it.
- **Validation:** write 5 events with limit 10. Then, with limit 3, `overwriteEvents(v5)`, a rewrite of v5 and
  `squash(5)` each throw.
- **Fix:** check sizes everywhere, but the version limit only where versions are added (`persistPlain`,
  `persistOne`); let a permanent delete pass at the limit.
- **Regression test:** the three calls succeed, and so does a permanent delete at the limit; a new event is still
  refused.

#### F245 — An unreadable stored event makes its aggregate impossible to erase
- **Severity** Medium (GDPR) · **Likelihood** Possible: F52 and F53 make unreadable rows plausible at the 1.0 migration
  · **Confidence** Confirmed (code).
- **Where:**
  - `PostgresEventStoreState.scala:678-680`: `rewriteEverywhere` deserializes every row first, so one failure aborts
    it.
  - `PostgresEventStoreState.scala:343-344`: `readCommittedPage` turns a read failure into its message.
  - `AggregateOwner.scala:789-791`.
- **Scenario:** an aggregate whose personal data must be erased holds an event that mpjsons 0.7 cannot read: an old
  JDK form, a changed field type, a hand repair.
- **Mechanism:** the rewrite fails before it changes anything, and its reason carries mpjsons' window of the JSON
  (F191).
- **Impact:** the aggregate can be erased only with manual SQL, and its failure rows block deletes and squashes (D20a,
  D51).
- **Validation:** corrupt one event, then send `RewriteEverywhere`: the answer is `RewriteRefused`.
- **Fix:** give erasure a path for unreadable rows, reported as such: a change on the raw JSON, or a tombstone event
  class that replaces the row.
- **Regression test:** an aggregate with an unreadable event is erased, and the report names the tombstone.

#### F246 — The squash scheduler assumes event times grow with event ids; an import, or a node with a wrong clock, makes it squash history the policy keeps
- **Severity** Medium; the state and the `keep` classes survive · **Likelihood** Possible: it needs `keepNewerThan`
  and an import or a clock error · **Confidence** Confirmed (code).
- **Where:**
  - `PostgresEventStoreState.scala:810-822`: `firstEventNotOlderThan` binary-searches ids on `event_time < at`.
  - `PostgresEventStoreState.scala:793-798`: each aggregate is squashed up to its newest event with
    `e.id < boundary`.
  - `SpaceImport.scala:186-189, 218`: imported events get new ids but keep their original times.
  - The comment at 807-809 ("imports count as new … never squashed early") does not hold for a binary search over a
    predicate that is not monotonic.
- **Scenario (illustrative estimate):**
  - 100 million events, `keepNewerThan` 30 days, and the true boundary near id 90 million.
  - Ten days ago an import wrote ids 93–98 million, with times from 2019.
  - The probe at 93.75 million lands inside the import and reads as older, so the search ends near 98 million.
- **Mechanism:** native events between 30 and 10 days old count as older than 30 days, and their aggregates are
  squashed up to them. This repeats every run, until the import is older than `keepNewerThan`.
- **Impact:**
  - Recent events are deleted; only the `keep` classes are archived.
  - Reads and undos in that range fail with `AggregateSquashedException`.
  - No signal. The memory store uses exact times, so tests cannot show it.
- **Validation:** ids 1–899 recent, 900–1 400 from 2019, 1 401–2 000 current. `firstEventNotOlderThan` returns 1 401
  instead of about 800.
- **Fix:** per candidate, compute `upTo = min(version) - 1` over its events with `event_time >= at`, using an index on
  `events (aggregate_id, event_time)` built concurrently. Trade-off: one more index on `events`.
- **Regression test:** the validation data in `PostgresEventStoreStateSpec`.

#### F247 — The SMALLINT type-id space is consumed by every build, and by every node's first sight of a name
- **Severity** Medium, when exhausted · **Likelihood** Unlikely; Possible with scheduled rebuilds · **Confidence**
  Confirmed (code).
- **Where:**
  - `ProjectionRegistry.scala:76, 153`: a new subscriber name per build.
  - `TypesNamesState.scala:59-62`: `id SMALLINT`.
  - `TypesNamesState.scala:82-93`: `INSERT … nextval … ON CONFLICT DO NOTHING`. Since F78's fix removed the SELECT,
    `nextval` is drawn before the conflict is detected, so every node that first meets a name another node created
    burns an id.
  - Imports add every class name of their file (F199).
- **Scenario:** a nightly `RebuildProjection` of 60 projections uses about 22 000 ids a year. The limit is 32 767 names,
  shared with event and aggregate classes.
- **Mechanism:** past 32 767, the INSERT fails with "smallint out of range".
- **Impact:**
  - No new event class can be stored, so its commands fail.
  - A new build's first chunk fails, and the builder resends it every 5 s, forever.
- **Validation:** count `types_names` rows and `types_names_seq` across N builds and node starts.
- **Fix:**
  - Reuse two subscriber identities per projection, or delete a retired build's name.
  - SELECT before the INSERT.
  - Report the sequence's use (in StoredDataCheck, or as a gauge).
  - Widen the column in a later contract step.
- **Regression test:** ids stay bounded across 100 builds, and a new name takes one sequence step.

#### F248 — An organization import keeps ids that the target's own generator will hand out later
- **Severity** Medium · **Likelihood** Unlikely: two databases with one installation number, such as both on the
  legacy 0 by default, or a staging copy of production · **Confidence** Confirmed (code).
- **Where:**
  - `SpaceImport.scala:92-95, 124-136`: only ids already present are remapped, and the header's installation is
    ignored.
  - `SpaceExport.scala:30`: `installation = 0` by default.
  - The uid sequence is never moved past the imported ids.
- **Mechanism:**
  1. A later pool contains an imported id.
  2. The new owner meets that id's row: a conflict, or a failed restore if the row is of another type.
  3. The first command fails, and creations keep failing while the sequence passes the imported range.
- **Impact:** intermittent creation failures for months, with misleading concurrency errors on aggregates that are new.
- **Validation:** with the target's sequence at S, import id S+5, then draw ids up to S+5: the first command fails.
- **Fix:** give the import the target's `IdRange`, and remap ids inside it at or above the sequence; or move the
  sequence past them under `SchemaLock`, never back.
- **Regression test:** the id is remapped, or never handed out.

#### F249 — With large-message destinations configured, the command size check uses the large frame for every destination
- **Severity** Medium · **Likelihood** Possible: SCALING §6–7 suggests that setting · **Confidence** Strongly
  suspected. Artery drops oversized ordinary messages, as recorded in F121.
- **Where:**
  - `MessageSize.scala:16-21`: `max(frame, largeFrame) - 8 KiB`.
  - `AggregateCommandBusActor.scala:326-334`.
  - `ClusterSerializer.scala:72, 80`.
- **Mechanism:** a sharded command between about 248 KiB and 2 MiB passes the check and is written whole. Sent to an
  owner path that is not a large destination, it goes over the ordinary stream, and Artery drops it: F121's symptom,
  no answer, and every retry dropped.
- **Impact:** the command can never run while its owner is on another node. It looks like a hang.
- **Validation:** a `TestCluster` with large destinations set for another path. A 400 KiB command to a remote owner
  times out, and Artery logs "Discarding oversized payload".
- **Fix:** check commands and the marker against `maximum-frame-size`, and allow the large frame only for listed paths.
- **Regression test:** the validation is refused at once, with a size error.

#### F250 — Fatal errors in handlers or restores are not contained, so a poison aggregate can crash-loop nodes
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Strongly suspected; it relies on Pekko's default
  `jvm-exit-on-fatal-error = on`.
- **Where:**
  - `AggregateOwner.scala:560-561`: `nextRoot` catches `Exception`.
  - `AggregateOwner.scala:302-303, 227`: `NonFatal`, through `Try`.
  - Scala's `Future` rethrows fatal errors (`runHandler`, 429-442).
  - mpjsons' `maxDepth` is 0 (unlimited), and no library instance sets it (`ClusterSerializer.scala:34`).
- **Scenario:** a recursive handler over a deep tree, or an event with deeply nested JSON, throws
  `StackOverflowError`.
- **Mechanism:** the JVM exits. If the event is stored, every automatic restore kills the node that runs it: the
  poller at 3, 6 and 30 minutes, gap requests, the squash scheduler, rebuilds.
- **Impact:** nodes crash-loop until the aggregate is repaired by hand.
- **Validation:** a recursion 10^6 deep in a handler. The system must stay up, and the command be answered.
- **Fix:**
  - Map `StackOverflowError`, and only that error, to `CommandHandlingError` or a failed restore.
  - Set `maxDepth` on the library's mpjsons instances, and recommend it for the host's.
- **Regression test:** the validation.
- **Source:** Pekko's configuration reference, `pekko.jvm-exit-on-fatal-error`,
  https://pekko.apache.org/docs/pekko/current/general/configuration-reference.html

#### F251 — Waiting min-version queries are unbounded in number and duration, and each publish scans them all
- **Severity** Low · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `AggregateOwner.scala`:
  - 821-822: `maxMillis` is never capped, so a query can wait up to 24.8 days.
  - 843-847: O(n) per insert.
  - 826-831: O(n) per publish.
- **Scenario:** clients wait, with long `maxMillis`, for versions that never come (after a failure).
- **Impact:** 10^5 such queries cost about 5·10^9 operations to build (estimate), and every command scans them.
- **Fix:** cap the queries per owner, and cap `maxMillis`; index the queries by deadline and by version.
- **Validation and regression test:** with 10^4 unreachable queries, the publish cost stays bounded and the cap holds.

#### F252 — Values the library does not check before it uses them
- **Severity** Low · **Likelihood** Possible · **Confidence** Confirmed (code).
- **What:**
  - **Long idempotency keys:** a key over 256 characters (`VARCHAR(256)`, `CommandResponseState.scala:81`) fails at
    the INSERT after the handler ran, as an `EventHandlingError` naming the event class. A key built from personal
    data stays 30 days in a table erasure does not reach.
  - **Long caller paths:** a caller path longer than `respond_to VARCHAR(256)` (`PostgresSagaSchemaInitializer.scala:28`)
    is refused only after 51 s of retries.
  - **Negative versions:** a negative `AggregateVersion` throws `ArrayIndexOutOfBoundsException`
    (`AggregateVersion.scala:11-17`).
  - **Unknown saga phases:** `SagaPhase.byName` sits outside the `Try` (`PostgresSagaState.scala:53-54`). A row with an
    unknown phase fails every load of its name, retried forever. F151 is reopened.
  - **Root class names:** a root class whose simple name has a letter outside ASCII makes the owner's actor name
    invalid, and the bus restarts at every new owner (`Owners.scala:83`).
  - **Store parameters:** the store's constructor parameters are not validated. `fetchSize = 0` disables streaming, and
    a non-positive limit refuses every event (`PostgresEventStoreState.scala:31-34`).
- **Fix:** check each at the boundary, with a clear error:
  - keys and paths, before the handler runs;
  - phases, inside the `Try`;
  - actor names, encoded;
  - parameters, required to be positive.
- **Regression test:** one case per item, each refused early with a named error.

## 9. State consistency, error handling and recovery

#### F253 — A commit whose outcome is unknown is answered "nothing was stored"
- **Severity** Medium · **Likelihood** Possible: rare for one command, systematic in each failover · **Confidence**
  Confirmed (code).
- **Where:**
  - `AggregateOwner.scala:494-505`: `Try { eventStore.localTx {…} }`.
  - `AggregateOwner.scala:537-541`: `case Failure(e) => EventHandlingError`. The owner keeps its version and
    publishes nothing.
  - `AfterCommit.scala:24-37`: the commit happens at the block's end.
  - The book, `docs/book/05-command-flavors.md:441`: "… or the database failed. Nothing was stored."
- **Preconditions:** the connection breaks during COMMIT, after the server committed (SQLState 08xxx).
- **Scenario:** a failover while about 30 commands commit.
- **Mechanism:**
  1. `commit()` throws, and the owner answers `EventHandlingError` without reloading. The events are stored
     nonetheless.
  2. A sharded or lone owner never compares with the store (line 318), so `GetAggregate` keeps answering the old
     version.
  3. The poller's `RepublishOutbox` arrives with `from > version`, so nothing is sent (868-870). "Undelivered" is
     logged at 10 minutes, then every 30.
  4. On a lone node with 200 owners or fewer, those retries keep the owner alive, so projections miss the events until
     the aggregate's next command.
- **Impact:**
  - A client that follows the book retries: a non-idempotent command is applied twice, and a `FirstCommand` without a
    chosen id creates a second aggregate.
  - Projection updates are delayed, or missing.
- **Validation:** a `DataSource` wrapper whose `commit()` commits and then throws `SQLException("08006")`. The caller
  gets `EventHandlingError`, while the rows exist.
- **Fix:**
  - When `commit()` fails, or with a connection-class SQLState, check on a fresh connection whether the transaction
    landed. The stored version equals version + n: conclusive for sharded and lone owners; otherwise compare the events
    at those versions.
  - If it landed, answer success and publish; if the check fails too, answer a distinct "outcome unknown".
  - In `republish`, restore once when `from > version`.
  - Correct the book.
- **Regression test:** the validation answers success (or "outcome unknown"), and the next command meets no conflict.

#### F254 — Transient database errors become durable recorded failures in live projections
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code) for the escalation; Strongly
  suspected for the trigger's timing.
- **Where:**
  - `ProjectionActor.scala:150`: 9 attempts, about 2 minutes.
  - `ProjectionActor.scala:755-766`: every failure counts as an attempt.
  - `ProjectionActor.scala:773-792`: once a failure could not be stored, the next one gives up at once.
  - `ProjectionActor.scala:586-587`: later deliveries are answered `MessageFailed`.
  - `ProjectionActor.scala:992-1003`: failures are retried only at start, at GoLive, or on request.
  - Compare the builder's `transient` check (F117, `ProjectionBuilder.scala:282-285`).
  - `PostgresEventStoreState.scala:492, 627`: a failed subscription blocks deletes and squashes.
- **Preconditions:** either:
  - the pool is exhausted for more than about 2 minutes. It is shared with host threads, handlers, parallel projection
    pools, `OutboxDeletes` and `SchemaLock`;
  - or a database outage of 2 minutes or more, after which the pool hands out stale connections, as there is no
    validation query.
- **Mechanism:** at recovery, the batch fails on a stale connection and gives up at once. `recordFailure` then borrows
  a fresh connection, and succeeds.
- **Impact:**
  - "Failed subscriptions" appear for listeners that are fine.
  - Their later events are answered as failed, so their outbox rows go, and the read model stays stale for those
    aggregates until an operator retries or the projection restarts.
  - Their squashes and deletes are blocked.
  - It looks like a listener bug.
- **Validation:** a subscriptions state whose `localTx` throws SQLState 08006 for 3 minutes, while `recordFailure`
  works. Today a failure is recorded.
- **Fix:**
  - Reuse the builder's `transient` check in `afterBatch`: transient errors use no attempt, back off with a cap, and
    are logged once per interval.
  - Optionally, retry recorded failures automatically, rate-limited.
  - Document a validation query.
- **Regression test:** the validation records nothing, and applies the batch after recovery.

#### F255 — A subscriber that stops before answering is dropped silently, and the node reports ready
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `EventsBusActor.scala`:
  - 220-223: on `Terminated` during registration, the bus starts.
  - 286-294.
  - 516: an ERROR saying "its events stay in the outbox until the next start".
  - 254: readiness is marked done.
- **Preconditions:** an unmanaged projection fails in its constructor (projections with side effects stay unmanaged,
  per M4). Causes:
  - a database hiccup in its store's `init()`;
  - `SchemaLock`'s 5 minutes (F185);
  - an index that cannot be built (F189).
  
  Pekko stops actors that fail in their constructor.
- **Mechanism:** the subscriber never subscribed, so nothing waits for it, and its rows are deleted once the others
  acknowledge.
- **Impact:**
  - The side effects of every event until the next restart are lost: e-mails, external calls.
  - After a restart, only aggregates that get a later event are repaired, through their gap.
  - The single ERROR line is misleading.
- **Validation:** register an actor that throws in its constructor. The node is ready, and its events are lost.
- **Fix:** keep the node not ready (or the rows kept), and log that the subscriber never subscribed. Recommend starting
  unmanaged subscribers under a back-off supervisor.
- **Regression test:** the validation, inverted.

#### F256 — Ephemeral buses call the context's outbox writer with no session, on an in-memory store that never rolls back
- **Severity** Medium; High where the writer has side effects · **Likelihood** Possible: a `WithOutboxWriter` context
  that also runs ephemerally · **Confidence** Confirmed (code). The exact exception `NoSession` raises is Strongly
  suspected.
- **Where:**
  - `AggregateCommandBusActor.scala:81-84, 150-151`: the ephemeral bus builds its owners' settings from the same
    context.
  - `AggregateOwner.scala:121-124`: the writer comes from any context that mixes in `WithOutboxWriter`; see also
    494-505 and 537-540.
  - `MemoryEventStoreState.scala:455-457`: `localTx` is just `block(NoSession)`.
  - `MemoryEventStoreState.scala:45-47`: a version mismatch is an `IllegalStateException`, which never triggers a
    reload.
  - The M6 design says "no outbox writer" (`M6-organizations.md:489, 532`).
- **Mechanism:** the persist applies the events in memory, then `write(…)(NoSession)` runs.
  - **A writer that uses the session fails.** The caller gets `EventHandlingError`, but the events stay in the draft.
    The owner's version does not move, so every later command fails with "Incorrect version" until the owner is
    passivated; then the "failed" changes appear.
  - **A writer that ignores the session** writes host rows (jobs, timers, messages) for drafts with negative ids.
  - **The same missing rollback** lets a keyed copy that loses the key race keep its events: orphan drafts that count
    toward `maxAggregates`.
- **Impact:** drafts that cannot be used, answers that misstate what happened, or real side effects for drafts.
- **Validation:**
  1. Run `ephemeral(new CartsWithJobs, …)`, the context of OutboxHookSpec.
  2. Create a draft, and add an item twice: each gets `EventHandlingError`.
  3. Passivate, then read: the first item is there.
- **Fix:**
  - Ephemeral owners get no outbox writer.
  - The in-memory `localTx` restores the aggregate's entries on an exception; or the idempotency key is recorded
    before persisting.
- **Regression test:** the validation with both writer styles, and a race of keyed creations that leaves one draft.

#### F257 — An ephemeral bus that restarts loses its drafts, then refuses every new draft until the node restarts
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `AggregateCommandBusActor.scala`:
  - 81-84: the store is made inside the Props creator.
  - 147: a new token; 133: an empty id pool.
  - 169-172: the id block is received only in `preStart`, and 187-189: `postRestart` skips `preStart`.
  - 258-259: the refusal "ids are spent; a restart brings new ones".
- **Scenario:** any exception in the bus's receive, under default supervision. For example, a host `Metrics` that
  throws in `dropEphemeral` (303), or an invalid actor name (F252).
- **Impact:**
  - Every draft is answered `EphemeralAggregateLost`.
  - Every new draft is rejected, with a message that blames a restart, when a restart caused it.
- **Validation:** a metrics stub that throws once on `reactivecqrs.ephemeral.dropped`, then a new draft.
- **Fix:** never let the bus throw (catch inside receive, as F70 does), and redo the ephemeral setup in `postRestart`.
- **Regression test:** the validation.

#### F258 — Managed projection instances share actor names, so lag signals and logs collide
- **Severity** Medium · **Likelihood** Likely, with two or more managed projections · **Confidence** Confirmed (code).
- **Where:**
  - `Projections.scala:251-254, 325`: instances are named `build-<n>-<k>`, with a counter per supervisor. So every
    adopted projection's active instance is `build-0-1`, and every new projection's first instance `build-1-1`.
  - `EventsBusActor.scala:347-349`: the gauge's tag is `subscriber.path.name`.
  - `EventsBusActor.scala:620-621`: `DeliveryStatus.behind` is keyed by the name.
  - Log lines at 475, 516, 563-565, 584-585, 593-599.
- **Scenario:** projection A is behind, and projection B is healthy.
- **Mechanism:**
  - The gauge is written twice per tick with the same tags, and the last write wins.
  - One `behind` entry is lost.
  - "Undelivered … to build-0-1" names no projection.
- **Impact:** the lag signals that SCALING §5 and §10 tell operators to watch are wrong or ambiguous: A's lag can be
  hidden by B's 0. Incident logs cannot be attributed.
- **Validation:** two managed projections, one of them lagging. `DeliveryStatus.behind` has one entry.
- **Fix:** name instances `<projection>-build-<n>-<k>`, or label subscribers by the registered subscriber they came
  from. Trade-off: metric series are renamed.
- **Regression test:** two distinct entries and series.

#### F259 — After a targeted reset, a live event of a target aggregate re-runs side effects for its whole history
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `ProjectionActor.scala`:
  - 722: a gap after the reset makes the owner re-publish from version 1.
  - 527, 807: those batches are not `fromBuilder`, so `rebuilding` is false, and pushes and other side effects run.
- **Scenario:** an erasure or an import resets aggregates, and one of them gets a new event before the targeted builder
  reaches it.
- **Mechanism:** the projection sees a gap from version 1, asks for it, and applies the history as if it were live
  events.
- **Impact:** pushes and other effects for old events, and one full re-publication per active aggregate. F114 is
  reopened.
- **Validation:** reset an aggregate, then commit an event to it before the builder runs. Every version gets a push.
- **Fix:** mark batches below a target's reset point as rebuilding, or hold live deliveries of reset aggregates until
  the targeted builder has passed them.
- **Regression test:** no push for versions below the event that arrived.

#### F260 — A stopped subscriber that is not removed makes the poller's work futile, and starves other aggregates' re-delivery
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `EventsBusActor.scala:392-393, 497-503`: deliveries to a stopped subscriber stay incomplete.
  - `RedeliveryPolicy`, with `maxTracked`: the oldest 100 000 waiting aggregates.
  - `PostgresEventStoreState.scala:826-835`: the grouping query joins `aggregates`, with no timeout.
- **Scenario:** a subscriber stopped for days and never removed (no `RemoveSubscriber`), whose aggregates fill the
  oldest 100 000 tracked.
- **Mechanism:**
  - Every re-publication of its types stays incomplete.
  - The poller restores up to 1 000 owners per poll for nothing.
  - Deliveries lost for any other aggregate are never read again, as they lie beyond the tracked set.
  - The grouping query grows with the backlog.
- **Impact:**
  - Lost deliveries of healthy subscribers are not re-delivered until a restart or a removal.
  - Wasted restores, and a slower database.
  - F122 and F141 are reopened.
- **Validation:** a stopped subscriber with 100 001 waiting aggregates, and one lost delivery to another subscriber.
  The lost delivery is never re-published.
- **Fix:**
  - Track waiting aggregates per subscriber, and skip those that wait only for stopped subscribers; or page past them.
  - Alert on a stopped subscriber with rows waiting.
- **Regression test:** the lost delivery is re-published within one poll interval.

#### F261 — Deleting an aggregate's idempotent responses removes their deduplication too, so a retried keyed creation creates it again
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** F167's clean-up of a deleted aggregate's `commands_responses` rows.
- **Scenario:** an aggregate created by a keyed command is permanently deleted. Then a client retry, or a saga step run
  again (F206), sends the same key.
- **Mechanism:** no stored response is found, so the command runs as new.
- **Impact:** a deleted aggregate comes back, with a new id, possibly after an erasure request.
- **Validation:** create with key K, delete permanently, and send K again: a new aggregate is created.
- **Fix:** keep a tombstone per key (the key, with its info cleared) for the retention period.
- **Regression test:** the retry gets the stored, cleared response.

#### F262 — A listener's `afterCommit` called in the first part of a two-part listener runs before the commit, and again when the work is remade
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `ProjectionActor.scala`:
  - 846-849: in the `(id, events) => { …; session => … }` shape of F109, `afterCommit` in the first part runs at once.
  - 929: it runs again when a failed group remakes the work.
- **Mechanism:** F161 defers only what runs inside the session part.
- **Impact:** pushes, or other after-commit work, for changes that are rolled back; duplicates when work is remade.
  F161 is reopened.
- **Validation:** a listener that calls `afterCommit` in its first part, with a failing group. The action runs twice,
  once before any commit.
- **Fix:** make `afterCommit` outside a transaction fail loudly, or queue it for the transaction that the listener's
  second part opens.
- **Regression test:** the validation runs the action once, after the commit.

#### F263 — An oversized request is replaced by a marker that never reaches the asker, who waits out its full timeout
- **Severity** Low · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - F121's marker.
  - `SagaRunner.scala:145`: logs "Received incorrect message".
  - `Projections.scala:439`: the supervisor forwards an oversized query to its instance, which leaves it unhandled.
- **Impact:** askers wait their whole timeout. SCALING §6's "the asker fails at once" holds for answers only. F121 is
  reopened.
- **Fix:** receivers answer the marker with the size failure, to its sender.
- **Validation and regression test:** an oversized query fails at once.

#### F264 — Deliveries created by a re-publication to one subscriber never acknowledge the owner, and a later full publication does not adopt them
- **Severity** Low · **Likelihood** Possible; systematic after a lag past the deadline · **Confidence** Confirmed
  (code).
- **Where:**
  - `EventsBusActor.scala:392`: `if (onlyTo.isEmpty) Some(publisher) else None`.
  - `EventsBusActor.scala:368-369, 395-401, 498`.
  - `AggregateOwner.scala:883-887`.
- **Mechanism:**
  1. A delivery ended at the deadline is re-published to one subscriber (a pull, or its gap request), without a
     publisher.
  2. A full publication within the next minutes finds that delivery, and goes only to that subscriber.
  3. The subscriber's ack completes it without a `PublishEventsAck`, so the owner's part-by-part continuation stops.
- **Impact:** rows stay after every subscriber has the events: more poller rounds, spurious "Undelivered", duplicate
  traffic.
- **Validation:** subscribers S and T, a short deadline, and the sequence in DLV-9 of the delivery notes. No ack
  reaches the publisher.
- **Fix:** a full publication adopts a delivery that has no publisher: it sets the publisher, and adds every live
  subscription not yet answered.
- **Regression test:** one `PublishEventsAck` reaches the publisher.

#### F265 — A rewrite without a new event that the store refuses strands its caller until the timeout, and its cause is never logged
- **Severity** Low · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - `AggregateOwner.scala:538`: `EventHandlingError(events.head.getClass…)` with `events` empty, a case F146 allows.
  - `overwriteEvents` 147: D17's refusal, or F154's size check.
- **Mechanism:**
  - `events.head` throws, and `guarded` logs that instead of the cause.
  - The owner becomes unrestored.
  - The command is answered at its timeout, not at once, although D17 promises an immediate `EventHandlingError`.
- **Fix:** take the name from the events or from the rewritten events, and log the cause first.
- **Regression test:** the error comes within 1 s, and the log names the refusal.

#### F266 — `clearSpace` and `clearAllData` evict the document cache before the commit, not after
- **Severity** Low · **Likelihood** Unlikely · **Confidence** Confirmed (code).
- **Where:** `PostgresDocumentStore.scala:556-564, 604-609, 669-675`. Unlike `removeDocument`, they do not go
  through `cacheAfterCommit`.
- **Mechanism:** a concurrent read between the eviction and the commit puts a soon-deleted document back into the
  cache.
- **Impact:** a purged document is served from the cache until another write evicts it.
- **Fix:** evict through `cacheAfterCommit`: now, and again after the commit.
- **Regression test:** the purge-under-concurrent-read case in `CachesAfterRollbackSpec`.

#### F267 — The push updates cache replays one type's updates through another type's filter
- **Severity** Low · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `SubscribableProjectionActor.scala`:
  - 46: cache entries have no type.
  - 150-151.
  - 113-118: an unchecked cast.
  - 123-125: one catch around the whole subscribe.
- **Mechanism:** a typed filter throws `ClassCastException` on the first entry of the other type. The replay stops,
  but the subscription stays registered, so the client never learns that it missed updates.
- **Fix:** keep the type in each entry, and replay only entries of the subscription's type, each in its own `Try`.
- **Regression test:** push B, then A, and subscribe for A with a typed filter. A is replayed.

#### F268 — Aggregates first re-published late are reported "Undelivered" 30 s later
- **Severity** Low · **Likelihood** Likely, during backlog drains · **Confidence** Confirmed (code).
- **Where:** `RedeliveryPolicy.scala`:
  - 63-64: a new aggregate is timed from its oldest row's time.
  - 52-55: a postponed one is not remembered.
  - 79-83: once both attempts are counted, it is given up at the next poll.
- **Preconditions:** a row first seen late. Causes:
  - postponement by the per-poll budget of 1 000;
  - being beyond `maxTracked`;
  - a writing node whose clock is behind;
  - a long command (`event_time` is the command's start, D10).
- **Mechanism:** one re-publication counts both attempts. 30 s later the aggregate is given up if its rows are still
  there, which during a backlog they usually are.
- **Impact:**
  - A stream of "Undelivered" errors, and a rising given-up gauge.
  - Those aggregates are then retried only every 30 minutes, with half the budget (F141): a slower drain, and
    misleading alerts.
- **Fix:** give up only when the last attempt is at least `giveUpAfter - attemptsAfter.last` old; or remember
  postponed aggregates.
- **Regression test:** a new aggregate 11 minutes old, re-published at poll 1, is not given up until 4 minutes later.

## 10. Dependency, environment and compatibility risks

#### F269 — Any node's start replaces the stored procedures with its own version's bodies
- **Severity** Medium · **Likelihood** Possible: at the first procedure change after 1.0, or at a rollback ·
  **Confidence** Confirmed (code).
- **Where:**
  - `PostgresEventStoreSchemaInitializer.scala:35-39, 237-407`: `dropOldFunctions`, and an unconditional
    `CREATE OR REPLACE` of the four procedures.
  - `SchemaLock` only serializes, and no version is stored.
  - H11's "do not roll back below M6" is the only guard.
- **Scenarios:**
  - 1.1 changes `add_events`. Mid-rollout, a 1.0 pod crash-restarts and installs 1.0's body for every node, until the
    next 1.1 start. M6's event keys would be lost this way (F124).
  - 1.1 renames a signature, and its `dropOldFunctions` drops the one that 1.0 pods still call.
- **Mechanism:** whichever node starts last decides the procedures' bodies. Nothing compares versions.
- **Impact:** silent gaps in data the new code relies on, or failing commands during a rollout.
- **Validation:** install the current bodies, run an initializer with an older body, then call `add_events`. No
  `event_keys` row is written.
- **Fix:**
  - Store a procedure version in `schema_steps`, and replace a procedure only with a newer one.
  - A changed signature gets a new name; old names are dropped in a later contract release (D69).
  - When the database is newer than the code, skip DDL, with a log line.
- **Regression test:** an older initializer leaves `pg_get_functiondef('add_events'::regproc)` unchanged.

#### F270 — Clocks and time zones: node clocks drive re-delivery, give-ups, instant reads and the squash policy, and several timestamps are written in the session's zone
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:**
  - **Event times** come from the writing node's clock at the command's start (`PostgresEventStoreState.scala:827`,
    `AggregateOwner.scala:484`):
    - `RedeliveryPolicy.scala:61-69, 75-77` compares them with the poller's clock;
    - instant reads take `max(version)` with `event_time <= instant` (`processEvents`, 173);
    - the squash policy relies on them (F246).
  - **Written in the session's zone, read as UTC:** `creation_time`, `events_archive.archived`, `snapshots.created`,
    `aggregate_lineage.detached`, `history_rewrites.at` and `schema_steps.completed` are written with
    `current_timestamp` in the session's zone, which pgjdbc sets to the JVM's. `readArchive` (661-662) and
    `SpaceExport` (117, 151) read them as UTC.
  - **Bound and read in the JVM's zone:** `subscription_failures.failed_since` (`SubscriptionsState.scala:269, 281,
    289`).
  - **Wall clock:** projection deadlines and subscriptions' time to live (`ProjectionActor.scala:1093-1108,
    1182-1192`; `SubscribableProjectionActor.scala:181-191`).
- **Scenario:** a node whose clock is 10 minutes behind; an NTP step backwards; a JVM not on UTC.
- **Mechanism:**
  - A node whose clock is behind gets duplicate re-publications and early give-ups; one ahead gets late first
    attempts.
  - After a hand-over, instant reads include later-stamped events.
  - An NTP step backwards delays parked answers by its size.
  - On a JVM not on UTC, values shift, and are ambiguous in the autumn DST hour.
- **Impact:** spurious "Undelivered" alerts, wrong instant reads, and shifted audit times in exports and archives. Neula
  runs on UTC, which hides the zone part.
- **Fix:**
  - Bind every timestamp as UTC, as F65 did for `event_time`.
  - Take `event_time` from the database's clock at commit; or document the clock-skew limit and alert on it.
  - Use monotonic time for deadlines.
- **Validation and regression test:** with a node on a skewed clock and a JVM on another zone, re-delivery, instant
  reads and exported times are unchanged.

#### F271 — New application message classes are dropped by old nodes during a rolling upgrade
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** ARCHITECTURE §13.6 warns about commands only. A new `SagaOrder`, query or listener message class sent to
  the old oldest node fails to deserialize there, and is dropped.
- **Scenario:** a release adds a saga order class, while the saga runner (a singleton) still runs on the old oldest
  node.
- **Impact:** orders and queries are lost during the rollout, with only a serializer error on the receiver. Callers time
  out.
- **Fix:** extend §13.6's rule, and the D67 write switch, to every message class a singleton receives, and refuse to
  send such messages while older versions run.
- **Validation and regression test:** a `RollingUpgradeSpec` variant that sends a new order class is refused at the
  sender, with a clear error.

#### F272 — Mismatched defaults: a 32-thread blocking dispatcher against an 8-connection default pool, no validation query, and driver defaults that hang or log values
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code); the driver's defaults are documented.
- **Where:**
  - `reference.conf:10-17`: 32 threads.
  - ScalikeJDBC's `ConnectionPoolSettings` default `maxSize` is 8.
  - The book (`02-setting-up.md:114`, `18-production.md:23-37`) and SCALING.md:111 size the pool by the blocking
    dispatcher only. Other borrowers are left out: `OutboxDeletes`, `SchemaLock`'s second connection, parallel
    projection pools (`ProjectionActor.scala:279-281`), handlers that read, and host threads.
  - No validation query, so after a database restart every idle connection fails one unit of work.
  - pgjdbc's `socketTimeout=0`, `tcpKeepAlive=false` and `logServerErrorDetail=true` (F186, F190).
- **Impact:**
  - With the defaults, 32 threads contend for 8 connections: 5 s waits and failed work.
  - After a database restart, a burst of failures, which F254 turns into recorded failures.
- **Fix:**
  - One "production settings" section in the book, with every pool and driver setting the library needs, and why.
  - A warning at start when the pool is smaller than the dispatcher.
- **Validation and regression test:** a start with an 8-connection pool logs the warning.

## 11. Compound failure scenarios and cascading failures

Most serious incidents come from findings meeting each other. Each chain below names its links, and the cheapest
place to break it.

**C1 — A database failover, without timeouts.**

The chain:

1. Half-open connections (F186): the poller's latch sticks, and re-delivery stops cluster-wide.
2. Owners are pinned and `CommandTimedOut` cannot run. Once all 32 blocking threads are pinned, the node stalls, and
   readiness does not notice (F233).
3. Clients time out and retry (F229), and commits whose acknowledgement was lost are answered "nothing stored" (F253).
   Retried non-idempotent commands are applied twice.
4. Saga steps are reverted while their commands still land (F205), and sagas answered "failed" are resumed later
   (F206).
5. Projections' stale connections become recorded failures (F254). Their later events are answered as failed, outbox
   rows are deleted, and read models stay stale.
6. The failed statements and failures are logged with personal data (F190, F191).
7. After recovery, retries whose clear fails leave stale failure rows (F201).

Break it with driver and role timeouts (F186, F272): the cheapest and most effective fix. Then transient
classification (F254), the outcome check (F253) and saga step retries (F205).

**C2 — A rolling restart during a backup or an export.**

The chain:

1. `pg_dump` or `SpaceExport` holds ACCESS SHARE on `events`.
2. A restarting node's start-up ALTER waits behind it (F185), and every statement on `events` queues behind the ALTER,
   cluster-wide.
3. Owners are pinned, with no lock timeout (F186).
4. The starting node holds `SchemaLock`. Other starting nodes fail after 5 minutes and crash-loop; each restart runs
   the DDL again. Readiness may have said "ready" already (F233).
5. If an older-version pod restarts meanwhile, it replaces the procedures (F269).

Break it with the catalog check and `lock_timeout` (F185).

**C3 — A release that adds a projection, rolled out behind readiness gates.**

The chain:

1. The new singleton cannot start while the old oldest node runs (F187).
2. The new node's bus waits, and stashes every publication (F188). The events of its sharded aggregates reach no
   subscriber, and its heap grows.
3. If the release also adds a unique index that the old documents violate, the active instance cannot be made (F189),
   so even the old version stops serving.
4. The node runs out of memory and restarts, and the same happens again. The rollout hangs.

Break it with the bus's registration timeout and no stash (F188); proxies treated as late registrations while older
versions run (F187); and no index sync for builds of another version (F189).

**C4 — A slow projection under sustained load.**

The chain:

1. A heavy listener falls behind. Windows reopen at every deadline (F221), and states are not counted (F222), so the
   singleton node's mailbox grows.
2. The bus's tick scans millions of deliveries (F212), which slows every delivery and holds more keys.
3. GC pauses on the node's owners raise command latency, and clients retry (F229).
4. The singleton's node runs out of memory, and the singleton moves.
5. The new node's bus starts with the backlog, and the poller re-publishes 1 000 aggregates in full per poll:
   duplicates to every subscriber.

Break it with stalled-subscriber detection (F221), state bytes in the window (F222), and incremental tick structures
(F212).

**C5 — An erasure request (GDPR Art. 17) on a busy installation.**

The host erases a customer, and `HistoryRewriter` runs. Along the way:

- copies squashed earlier are not found (F193);
- attached copies are reported unchanged and not rebuilt (F194);
- a command in flight writes a value back (F195);
- a build in progress, or a concurrent erasure, refuses the rebuild, and the rerun reports complete (F192);
- an aggregate at the version limit (F244), or with an unreadable event (F245), cannot be rewritten;
- failure texts and SQL logs keep the values (F190, F191);
- snapshots can bring values back, through the non-atomic guard (F237).

The operator then holds a report that says "complete" while the value remains in up to eight places. Break it with
each fix in §3.2, then an end-to-end erasure audit test that searches every table and the captured logs for a marker
(§12).

**C6 — An outage, a retry, and an operator's skip.**

The chain:

1. A short outage records failures for healthy listeners (F254).
2. After the fix, a retry succeeds, but its clear fails in a second blip (F201's precondition).
3. At the next start, a false "missing" failure is recorded, and the operator skips it.
4. The next real event is acknowledged without being applied: the read model silently diverges, for good.

Break it with transient classification (F254); stale rows cleared at once; and skips bounded by the stored version
(F201).

**C7 — An id from the wrong place.**

The chain:

1. A client sends an id of another type (F197).
2. The owner restores it from a snapshot of the other type, with the missing fields read as zero: the same weakness as
   F196.
3. The command stores events of type A on B's aggregate.
4. A's projections record failures, which block squash and delete. B's aggregate restores to nulls.
5. On a conflict, its version is revealed to the client (F243).

Break it with the type check on restore and in `add_events` (F197).

**C8 — A template with many copies.**

The chain:

1. Erasing, squashing or deleting a template with 10 000 copies (F215) holds every copy's row lock for minutes.
2. Commands on copies block, pinning blocking threads. At 32, the node stalls (F186), and command timeouts cannot be
   answered (F229). Clients retry.
3. Meanwhile, the erasure misses squashed copies (F193) and leaves attached ones unrebuilt (F194).

Break it with batched detaches and `FOR KEY SHARE` locks (F215), and a `lock_timeout` on persists.

**C9 — Moving an organization.**

The chain:

1. An export of a live organization (F198) gives a file whose versions disagree.
2. The import does not check them (F199), so the target gets duplicate versions. The space rebuild applies them, and
   the read model diverges.
3. Ids that collide with the target's future pools (F248) cause creation failures months later.
4. A purge before a retry, if mistyped as space 0 or -1, deletes data installation-wide (F200).

Break it with one snapshot (F198), first-pass validation (F199), id ranges (F248), and the purge guard (F200).

**C10 — A database restart with the default pool settings.**

The chain:

1. A pool of 8 for 32 threads, with no validation query (F272). After the restart, every idle connection fails once.
2. Projections record failures (F254).
3. Owners fail their restores and stay failed for 5 s.
4. Type-name registration waits for a second connection under the JVM-wide lock (F230).
5. Command errors and log bursts with values follow (F190).

Break it with the production settings (F272), transient classification (F254), and type names resolved outside
transactions (F230).

**Feedback loops to monitor.** The bus's tick against held keys (C4); re-publication of 1 000 aggregates per poll
during a backlog (C4); and restore storms after failed lookups (F231) and after another node's writes (F214).

## 12. Missing tests and recommended testing strategies

No test was written or run for this assessment, as asked. The regression test named in each finding is the plan.

**What exists today:**

- Suites: core 325, examples 107 and testdomain 194 scenarios on PostgreSQL, also as a cluster of one.
- E2's fault tests: kills, stops and partitions under load (measured 18/10/2026).
- The bench.

**What they leave out:**

1. **Database fault injection.** A proxy between the JVM and PostgreSQL (toxiproxy, or a small TCP proxy) that black-
   holes, delays and resets connections during polls, persists and commits; and lock holders during starts. Covers
   F185, F186, F229, F253 and F254. The slow-database fault test deferred at review 9 belongs here.
2. **Rolling upgrades** that add or remove singletons, sharded types and message classes, and that change procedures:
   variants of `RollingUpgradeSpec` (F187, F269, F271).
3. **An end-to-end erasure audit.** One scenario per erasure path, each followed by a search for a marker value in
   every table, every read model and the captured logs (F190–F195, F237, F244, F245). The paths:
   - copies, attached and squashed;
   - a build in progress, and two erasures at once;
   - a command in flight;
   - an aggregate at the version limit, and one with an unreadable event.
4. **Log capture.** An appender in the test kit that fails a spec when a marker value (test data) appears in any log
   line (F190, F191, F252).
5. **Export and import under concurrency and damage:** a writer during the export; truncated, duplicated and crafted
   files (F198, F199, F248).
6. **Property-based restores.** Random histories with undo, squash, duplication, rewrite and snapshots, asserting that
   a snapshot restore, a replay and an incremental catch-up agree (F196, F214, F237).
7. **Sagas under faults:** delayed commands, failing state writes, duplicate loads (F205–F207, F232).
8. **Long runs and large inputs:**
   - a soak with a stalled subscriber, measuring mailbox size;
   - the bus's tick at millions of deliveries;
   - push subscriptions at tens of thousands;
   - reconciliations with large backlogs;
   - imports with large events.
   
   Covers F212, F216, F221–F223 and F226.
9. **Test doubles:** one contract spec, run against both the memory and the PostgreSQL stores (F273, F274).
10. **Clocks and zones:** runs on a skewed clock and on a JVM not on UTC (F270).

**Test infrastructure** (lesser items from the data-lifecycle notes):

- `TestDatabase` drops `test_run_` schemas older than 6 h by each JVM's own clock. So a run longer than 6 h, or a
  machine whose clock is 6 h fast, drops schemas still in use; nothing checks whether a schema is in use (an advisory
  lock per run would).
- In fixed-schema mode, `TestIds` restarts in each JVM.

#### F273 — The memory document store ignores sort, offset and limit
- **Severity** Medium · **Likelihood** Likely: the book encourages level-2 tests · **Confidence** Confirmed (code).
- **Where:**
  - `MemoryDocumentStore.scala:28-47`: `findDocument` only filters.
  - `MemoryDocumentStore.scala:99-101`: `findDocumentByObjectInArray` ignores its limit.
- **Mechanism:** a paged or ordered query returns every match, in hash order, in tests; in production it returns the
  top N, in order.
- **Impact:** paging and ordering bugs pass level-2 tests, and F211's unbounded fetch is never flagged. F14's list of
  gaps omits this one.
- **Fix:** apply `sortBy`, `offset` and `limit` in the memory store, or list the gap among F14's documented
  divergences.
- **Regression test:** `MemoryDocumentStoreSpec` with sorted, limited queries, against the same expectations as
  `PostgresDocumentStoreSpec`.

#### F274 — The memory event store diverges from PostgreSQL exactly where several of these findings live
- **Severity** Medium · **Likelihood** Likely · **Confidence** Confirmed (code).
- **Where:** `MemoryEventStoreState.scala`:
  - `writeSnapshot` is atomic (165-169), which hides F237.
  - `squashCandidates` uses exact times (459-470), which hides F246.
  - Rewrites and squashes skip the size and limit checks (244-261, 353-381, 473-489), which hides F244.
  - `duplicatesReading(below)` returns direct copies only, where PostgreSQL returns every depth (229-242, against
    `PostgresEventStoreState.scala:474-477`).
  - It accepts a `DuplicationEvent` on an existing aggregate, and a non-first event that creates one; PostgreSQL
    refuses both.
  - A version mismatch is an `IllegalStateException`, so the owner's conflict path never runs in memory (45-58).
- **Impact:** specs on the memory store pass where PostgreSQL fails or behaves otherwise, so several findings here are
  invisible to them.
- **Fix:** align the memory store on each point, or run the affected specs on PostgreSQL only and list the gaps.
- **Regression test:** one shared contract spec, run against both stores.

## 13. Concrete remediation plan, ordered by priority

Each fix follows the project's way of working: failure modes first, a red test first, and the book updated. Sizes are
estimates of effort (S, M, L), not measurements.

**Step 1: before Neula moves production to 1.0.** Small changes, with no data migration.

| # | Findings | What | Size |
|---|---|---|---|
| 1 | F190, F191, F252 (keys) | Library statements without value logging; sanitized failure texts; a failure logged once; `logServerErrorDetail=false` in the book | S–M |
| 2 | F185 | Catalog checks and `lock_timeout` in every start-up initializer | M |
| 3 | F186, F272 | Driver and role timeouts in the book and E2; watchdogs on the poller's and scheduler's latches; a warning at start for a small pool | S–M |
| 4 | F188, F187, F189, F255 | A registration timeout for the bus; no stash of publications; proxies as late registrations while older versions run; no index sync for builds of another version | M |
| 5 | F200 | Refuse purges of space 0 and of negative spaces | S |
| 6 | F193 | Lineage on squash, in both stores, plus an audit step in the runbook | S |

**Step 2: erasure and the correctness of stored data.**

| # | Findings | What | Size |
|---|---|---|---|
| 7 | F192, F194, F195, F237, F244, F245 | The erasure fixes, with the audit test of §12 | M–L |
| 8 | F198, F199, F225, F226, F248 | Export in one snapshot; import validation (format 2); id ranges; batches by bytes | M–L |
| 9 | F197, F196 | Type checks on restore and in the procedures; snapshot fingerprints | M |
| 10 | F201, F254, F259, F261 | Projection failure handling; tombstones for response keys | M |

**Step 3: sagas and drafts.**

| # | Findings | What | Size |
|---|---|---|---|
| 11 | F205, F206, F207, F232 | Step retries; sagas never dropped; idempotent orders; compare-and-set writes; status and metrics | L |
| 12 | F208, F209, F236, F256, F257 | Materialization outcomes; per-start tokens; ephemeral buses without an outbox writer, and robust | M |

**Step 4: performance, memory and operations.**

| # | Findings | What | Size |
|---|---|---|---|
| 13 | F210, F211, F273 | Document-store paths bound or checked; default caps; the memory double | M |
| 14 | F212, F221, F222, F223, F228, F258, F260, F264, F268 | Delivery: tick structures, stalled subscribers, state bytes, paged status, names, poller fairness | M–L |
| 15 | F213–F220, F229–F231, F234, F235 | Owners, rebuilds and projections | M–L |
| 16 | F203, F204, F233, F240, F247, F249, F250, F269, F270, F271 | Limits, configuration and upgrades | M |
| 17 | §12 | The tests of §12, alongside each step | — |

## 14. Residual risks and areas requiring further investigation

**Facts needed from Neula.** These decide several likelihoods:

- the JDBC URL and pool settings, the role timeouts, and how its database fails over (F186, F272);
- the backup method, and long transactions during restarts (F185);
- which contexts take snapshots, and whether a release check raises `schemaVersion` (F196);
- whether hosts check ids' types (F197);
- the use of duplication and squash, and the widest fan-out of copies (F193–F195, F215);
- exports of live organizations, and imports combined with `keepNewerThan` (F198, F246);
- saga step timeouts and how answers are mapped, and H3 (F205–F207);
- ephemeral types, keyed draft commands and outbox writers (F208, F209, F224, F256);
- the use of `ServeStale` (F202);
- the length of build names, and how often projections are rebuilt (F203, F247);
- Artery TLS, and a NetworkPolicy (F240);
- log retention and access, and whether error replies reach end users (F190, F191).

**Measurements needed:**

- the bus's tick at 1–6 million deliveries (F212);
- push subscriptions at scale (F216);
- `backfillEventKeys` on a production copy: WAL and duration (F220);
- the slow-database fault test (F186, F229);
- the passivation sweep's latency (F218);
- `AfterCommit`'s contention (F219).

**Not examined in depth:**

- Pekko's internals: Artery's queues under bursts, and shard allocation during rollouts (only in part);
- mpjsons' thread safety;
- Neula's code, beyond the grep of document paths;
- the specs themselves.

Findings that rest on documented dependency behaviour (PostgreSQL's locks, pgjdbc's defaults, Pekko's singletons) are
confirmed by that documentation, not by a run. Line numbers drift as the code changes.

**The working notes.** Each area's notes keep its full traces, and further lesser items, with files and lines, that
have no F-number here:

- [aggregate owner](risk-assessment/owner.md)
- [command bus and ids](risk-assessment/command-bus.md)
- [event store](risk-assessment/event-store.md)
- [delivery](risk-assessment/delivery.md)
- [projections](risk-assessment/projections.md)
- [document store and configuration](risk-assessment/document-store.md)
- [rebuilds](risk-assessment/rebuilds.md)
- [cluster and sagas](risk-assessment/cluster-and-sagas.md)
- [data lifecycle](risk-assessment/data-lifecycle.md)
- [cross-cutting](risk-assessment/cross-cutting.md)

Their ids map to the F-numbers of this report:

| Notes | F-numbers |
|---|---|
| OWN | 1 F196 · 2 F193 · 3 F194 · 4 F195 · 5 F186, F229 · 6 F231 · 7 F191 · 8 F213 · 9 F214 · 10 F215 · 11 F253 · 12 F208, F209 · 13 F250 · 14 F265 · 15 F251 |
| BUS | 1 F197 · 2 F208 · 3 F256 · 4 F204 · 5 F209 · 6 F204 · 7 F218 · 8 F224 · 9 F249 · 10 F236 · 11 F257 |
| STORE | 1 F193 · 2 F198 · 3 F185 · 4 F244 · 5 F245, F191 · 6 F246 · 7 F215 · 8 F269 · 9 F220 |
| DLV | 1 F221 · 2 F222 · 3 F188 · 4 F191 · 5 F212 · 6 F258 · 7 F186 · 8 F228 · 9 F264 · 10 F268 |
| PROJ | 1 F201 · 2 F191 · 3 F234 · 4 F216 · 5 F217 · 6 F228 · 7 F267 |
| DOCS | 1 F211 · 2 F210 · 3 F273 · 4 F185 · 5 F219 · 6 F266 |
| RBLD | 1 F189 · 2 F192 · 3 F202 · 4 F203 · 5 F217 · 6 F223 · 7 F235 · 8 F247 |
| CLSG | 1 F187 · 2 F205 · 3 F206 · 4 F207 · 5 F232 · 6 F233 · 7 F191 · 8 F185 · 9 F227 |
| LIFE | 1 F192 · 2 F195 · 3 F198 · 4, 5, 10 F199 · 6 F200 · 7 F230, F247 · 8 F225 · 9 F226 · 11 F191 · 12 F248 |
| SYS | 1 F190 · 2 F186 · 3 F185 · 4 F188 · 5 F253 · 6 F229 · 7 F254 · 8 F258 · 9 F231 · 10 F212 · 11 F230 · 12 F255 |

**Closed issues found incomplete,** and where they continue:

- F8 → F192, F195; F14 → F273, F274; F23 → F204; F35, F136 → F206.
- F48 → F238; F65 → F270; F78 → F230, F247; F81 → F231.
- F92 → F191; F93 → F221; F110, F171 → F189; F114 → F259.
- F119 → F237; F121 → F249, F263; F122, F141 → F260; F126 → F222, F226.
- F131 → F214; F137 → F239; F145 → F195; F151 → F252.
- F161 → F262; F167 → F261.
- Decisions: D39 → F204; D76 → F193; D77 → F208, F209.
