# ReactiveCQRS — decisions and known issues

The single backlog for the 1.0 line: the decisions that steer the work, and every known
problem with its planned fix. `docs/ARCHITECTURE.md` describes how the library works today
(0.12.47); `docs/IMPLEMENTATION-PLAN.md` orders the work into milestones.

| | |
|---|---|
| Status | Agreed, 16/10/2026 (design session with the library owner) |
| Baseline | `master` @ `4b5060f`, version 0.12.47 |
| Only consumer | Neula (`neulaserver`), so API compatibility is not required; stored data compatibility is |
| Inputs | Static reads of this repo, of `neulaserver` and of mpjsons 0.7.0; Neula's change requests R1–R21 (`C:\Tmp\ReactiveCQRS Change Requests.md`) |

**How to read the issues.** Severity and confidence follow the disciplined-coding guidelines:

- **High**: data loss or corruption, a security hole, or wrong results in a main flow.
  **Medium**: a bug under specific conditions, missing error handling, or a performance
  problem at realistic scale. **Low**: a maintainability issue likely to cause bugs later.
- **confirmed**: reproduced or shown by a test. **likely**: clear from reading the code.
  **suspected**: depends on something not yet checked (usually production data or Neula code).

Nothing below has been reproduced yet: every issue starts as *likely* or *suspected* and
becomes *confirmed* when its reproducing test fails (milestone M1 onward). Line numbers are
from `4b5060f`. Each issue is fixed in the milestone where its code is reworked anyway (D84).

Contents: [1. Decisions](#1-decisions) · [2. Invariants](#2-invariants) ·
[3. Issues in the library](#3-issues-in-the-library) · [4. Issues on the Neula side](#4-issues-on-the-neula-side) ·
[5. Answers to Neula's questions](#5-answers-to-neulas-questions) · [6. Index by milestone](#6-index-by-milestone) ·
[7. Architecture reviews](#7-architecture-reviews)

---

## 1. Decisions

Decision **Dn** is the answer to question **Qn** of the 16/10/2026 design session; gaps are
questions that were confirmations, not decisions. Each decision is written as target
behaviour.

### 1.1 Process and documentation

| ID | Decision |
|---|---|
| D1 | This file is the only fix backlog. `ARCHITECTURE.md` describes the as-is library; its §11 is a summary with a link here. |
| D2 | Every issue entry has: ID, severity, confidence, status, fix milestone, owning module, symptom, root cause with `file:line`, related items, reproducing test (at the owning module's interface), fix with recommendation, invariants touched. |
| D5 | CLAUDE.md §7 keeps only a pointer to this file and the editing cautions; problems are not tracked there. |
| D6 | Every fix comes with a test at the interface of the module it changes. In-process logic is tested with in-memory adapters; SQL procedures and PostgreSQL adapters with PostgreSQL tests. Step 0 of M1 makes the test suite compile and fixes the in-memory test doubles. |
| D25 | Decisions are recorded in this section; later work follows them instead of re-deciding. |
| D84 | Severity and confidence use the scale above (supersedes the colour scale of D13). Issues are fixed during the 1.0 build, at the milestone where the code they live in is reworked. |

### 1.2 Scope and releases

| ID | Decision |
|---|---|
| D27 | Requirements = the owner's five changes (organization separation and migration, cluster, performance, squash into a snapshot event, removal of event versioning) + Neula's R1–R21. R1 (retry, then mark failed, with status and repair) replaces D9. API compatibility is not required. Opt-in per aggregate context only where a feature changes stored data or runtime behaviour (snapshots, serial mode, sharding, squash), so Neula can migrate one aggregate type at a time. |
| D64 | One development line straight to **1.0**; no intermediate stable releases. Supersedes D23 and D26. |
| D79 | Progress is marked by git tags `v1.0.0-M1`, `-M2`, …; the owner publishes to the Neula Nexus after the work is done. No API promises between milestones; dev databases may need resetting between them. Neula production stays on 0.12.47 until 1.0. |
| D80 | No 0.12.x back-ports. F21 and F1 stay in production until 1.0 as accepted risks. |
| D81 | 0.12.47 → 1.0 is a one-off planned stop-and-migrate (0.12 nodes cannot join a 1.0 cluster). Rehearsed on a production copy with measured duration; outbox, saga and subscription rows carried over; heavy jobs (space backfill, mpjsons dry run) done online beforehand; changed projections shadow-rebuilt after start. Rolling upgrades apply from 1.0 on. |
| D82 | Work is ordered in milestones M1–M6 (see the implementation plan, which adds M7 for documentation and release). Neula stage-1 needs (R1–R4, R7–R10, R12, R13) are complete at M3. |
| D74 | Library baseline: Scala 2.13 (latest), Pekko 1.7.x, ScalikeJDBC 4.3.x, PostgreSQL JDBC 42.7.x (latest), mpjsons 0.7.0, Java 21 bytecode; Pekko Management (cluster bootstrap, Kubernetes lease, health checks) for the cluster. |
| D75 | Keep a path to Scala 3: no new runtime reflection; aggregate and listener type keys move from `TypeTag`/`typeOf` to `ClassTag` or class names; `Event` stops carrying a `TypeTag`. |
| D78 | mpjsons 0.7 is adopted only after a data gate: (1) golden test, every stored-format class serialized and round-tripped by 0.6.50 and 0.7.0 and compared; (2) dry run reading every stored JSON row of a production copy; (3) Neula fixes `== null` checks on `Option` event fields. |

### 1.3 Stored data

| ID | Decision |
|---|---|
| D28 | Data rules for every change: (1) no upgrade rewrites existing `events` rows; only two explicit operations may change stored events: the GDPR history rewrite and squash; (2) schema changes are additive, new columns nullable with defaults computed on read, large indexes built `CONCURRENTLY`, initializers idempotent; (3) in-flight data survives an upgrade: outbox rows, saga rows, subscription offsets, `types_names` ids, uid sequences, document-store tables; (4) every upgrade is tested against a copy of production data: every aggregate type restores, every projection rebuilds, every saga resumes. |
| D69 | Schema changes are expand-then-contract across versions: a version only adds and works with the previous version's code; removals happen in a later version; heavy migrations (backfills, large indexes) run as separate online jobs, not in startup initialization. |
| D24 | The unused `event_bus` table stays until a later contract step. The `commands_responses` index is fixed inside `initSchema()` (drop the old one if present, create a unique index on `key`); the host must call that `initSchema()`. |
| D29 | Event versioning is removed: no `eventsVersions`, `EV`, version maps. `event_type_version` is always written as 0 and the column stays. Events evolve as new `V<n>` classes with the old class `@deprecated`. Before release, run `SELECT event_type_version, count(*) FROM events GROUP BY 1` on production; the reader fails with a clear message on a non-zero version. A type-alias registry stays an optional backlog item. |
| D10 | One timestamp per event: the one chosen in the command executor, truncated to microseconds, is passed into SQL as `event_time`. Column type unchanged. |
| D38 | The organization is the `SpaceId`; every aggregate gets it from its `FirstEvent`. Space 0 is the platform. Ids below 1001 are constants shared by all installations and never remapped. Legacy `aggregates` rows with `space_id = -1` get a one-off backfill (aggregates table only) through a per-context space function or a Neula script. |
| D39 | `AggregateId` stays one `Long`. New ids are unique across installations: `installationNumber × 10^12 + sequence`, kept below 2^53 for the TypeScript UI. Existing ids stay as a legacy range; negative ids stay reserved for ephemeral aggregates. Requires F12 fixed and a Neula check that no id is read as `Int`. The installation number may be issued with the licence. |

### 1.4 Delivery of events to subscribers

| ID | Decision |
|---|---|
| D3 | Delivery is one module with a small interface. Publisher side: "versions a..b of aggregate X are committed". Subscriber side: deliver, the subscriber acks a version range or reports a failure. Contract: at-least-once, ordered per aggregate, the subscriber deduplicates by version. |
| D7 | The bus-side cursor (`EventBusState`, `event_bus`, `publishedEvents`, `eventsPropagatedNotPersisted`) is deleted; the bus routes and tracks acks only. |
| D8 | One owner of retries: a single timer sends the aggregate's owner a re-publish message (creating the owner if needed); the owner re-reads its own `events_to_publish` rows and publishes them with its current state. The in-memory `eventsToPublish` and `pendingPublish` lists go away. Refined in the M2 design: the poller counts the re-delivery attempts (3 and 6 minutes) and the deadline per aggregate; the owner re-publishes from its oldest outbox version to its current version. |
| D20a | A failing subscriber retries the aggregate itself with back-off (about 2 minutes), then records the failure in its subscription row (failed version, error, since) and replies `Failed`. The publisher then stops re-delivering that event to it; the outbox row is deleted once every subscriber has acked or failed. Other aggregates keep flowing; the failure shows in status and metrics. Recovery: `RetryFailedSubscription`, and automatically when the projection starts (rate-limited); the subscriber then reads the missing events from the event store. An operator may also skip one version explicitly. Re-deliveries for an aggregate under internal retry are ignored. Refined in M2: failures are kept in a table of their own, `subscription_failures` (subscriber, subscription type, aggregate, failed version, error, since), instead of new columns of `subscriptions`; same content, apart from the version rows that the cache and the replay dump manage. |
| D20b | Silence (no ack, no failure) gets limited re-delivery: at about 3 and 6 minutes, deadline 10 minutes (configurable; the first re-delivery comes after the subscriber's own retry budget). At the deadline: log and alert "undelivered to X", mark it in delivery status. The deadline counts from the first attempt in the running process; at startup every outbox row, including ones past their deadline, gets a fresh set of attempts. Stalled deliveries are logged: one line when an event first stalls, plus a periodic summary. Bus bookkeeping for an event ends on ack, failure or deadline (includes former D19); the bus watches its subscribers and logs an error if one stops. |
| D20c | A detected gap is filled from the event store: a subscriber at v5 that receives v7 gets v6 again. No unbounded buffering. Refined in the M2 design (`docs/design/M2-delivery.md`): for every listener kind the subscriber asks the owner, which re-publishes the range from the store to that subscriber only, so subscribers need no event-store access. |
| D4 | One pure, in-process **sequence gate** decides for every subscriber message: ack only, apply versions a..b, or fetch the gap. The three listener kinds are thin adapters over it. |
| D15 | The host passes the bus the list of subscriber actors it created. On start the bus asks each one for its subscription requests (empty list allowed), holds publications without blocking (`Stash` + `pipeTo`) until all answered, logs missing ones by name while waiting. Subscriptions are fixed after start. `EventBusSubscriptionsManager`, push subscription from `preStart` and `minimumExpectedSubscriptions` are removed. |
| D9 | Superseded by D20a and R1 (D27). |
| D19 | Merged into D20b. |

### 1.5 Commands, aggregates and their features

| ID | Decision |
|---|---|
| D11 | A failed restore fails fast: restore moves from the constructor to the first message; after a failure the repository actor itself answers every query and command with an explicit error for a short time (`failedRestoreRetryAfter`) and then stops; the command bus watches its repository actors and forgets stopped ones, so the next request restores again. |
| D12 | Idempotency keys are globally unique: a unique index on `commands_responses(key)`; a colliding command's transaction rolls back (including its events) and the caller gets the stored response. Usable by any command kind, including `FirstCommand` with a caller-chosen id (R7); stored responses have a retention period (30 days by default). Only commands that changed state store a response: a copy of a command that failed or produced no events runs again. |
| D37 | Contexts that stay in concurrent mode: on a version conflict the owner reloads state from the store, retries with jittered back-off, and gives up after about 10 attempts with `AggregateConcurrentModificationError`. |
| D31 | Serial command mode (R8), opt-in per context: the aggregate's owner queues commands and runs one at a time; handlers always see the latest state; `ConcurrentCommand` re-runs disappear; queue limit and overflow policy configurable. In the cluster the shard entity uses the same mechanism. |
| D56 (R5) | Async command timeout configurable per context (default 60 s); on timeout the caller gets `CommandHandlingError(timeout)`. |
| D30 | Performance snapshots do not consume versions: table `snapshots(aggregate_id, version, schema_version, state, created)`, written after persist by policy (every N events, on passivation). Restore = latest compatible snapshot + later events. Undo, history rewrite, a change of the context `version` or a schema change make older snapshots unusable. Snapshots may store raw root JSON because they are disposable. |
| D47 | Squash replaces versions 1..N with one event stored at version N; the aggregate version is unchanged; restore = squash event + later events; one transaction, executed by the aggregate's owner; opt-in per context. |
| D48 | The squash event is a context-defined domain event class (a `FirstEvent` built from the state), handled by `eventHandlers` and projections, evolved with `V<n>` rules. The framework never stores raw root JSON as permanent history. |
| D49 | Undo cannot cross the squash point: `add_undo_event` rejects it with an explicit error. Noop markers at or below N are deleted by the squash. |
| D50 | Before squashing a base aggregate below a version that duplicates still read, each such duplicate is detached: its version-1 `DuplicationEvent` is replaced by a squash event holding its state at version 1, and its base-chain rows are removed. Squashing a duplicate detaches it the same way. |
| D51 | Squash runs only with no `events_to_publish` rows and no failed subscriptions for the aggregate; live projections are not notified; rebuilds see the squash event first; performance snapshots at or below N are deleted; historical reads below N return an explicit "squashed" error (BusinessEntity contexts do not opt in until R11's archive exists); disk space is reclaimed by (auto)VACUUM, shrinking files needs `pg_repack`/`VACUUM FULL` (operations note). |
| D52 | Squash runs on explicit request (`SquashAggregate(id, upTo)`) or by an optional per-context policy run by the squash scheduler singleton. R11's selective squash with an audit archive is an extension on top. |
| D32 | Metadata envelope (R13): a new nullable JSONB `metadata` column on `events`; old rows get a default envelope built from `user_id` on read; commands supply metadata through a trait; `EventInfo` carries it; aggregate event handlers keep their signature. |
| D33 | Outbox hook (R12): a typed writer per context, `(aggregateId, version, events, session) => Unit`, called inside the persist transaction; a failure rolls back the events; idempotent replays never call it twice. |
| D17 | History rewrite is safe by contract: a rewritten event must keep its class; the in-memory aggregate is rebuilt from the store after a rewrite; rewrite + compensating event stays the documented way to notify projections. |
| D76 | A complete history rewrite (R15) also covers squash events, performance snapshots, outbox-hook rows on request, duplicates (automatically while attached, through recorded lineage when detached), triggers a targeted rebuild of affected projections for the aggregate and its duplicates, and writes an audit entry (who, when, which classes and fields, no values). |
| D77 | Ephemeral aggregates (R16): the same context running only in memory on one node, never written, not published, negative ids; `Materialize` persists one squash-style first event under a new positive id returned to the caller; loss on node failure is reported. |
| D16 | The document-store cache is never ahead of the database: cache writes made inside a listener transaction take effect only after commit and are discarded on rollback. |
| D36 | F21 is the first fix after step 0, bundled with D16 and stale-entry eviction before retries. |

### 1.6 Projections and rebuild

| ID | Decision |
|---|---|
| D57 | Shadow rebuild: a new projection version builds into its own tables (`projection_<name>_v<N>`) under its own subscription identity (class + version), reading the event store; the old version keeps serving; when caught up it takes over, old tables are dropped; indexes are built before the switch; a failed rebuild leaves the old version untouched. Replaces clear-then-replay. |
| D58 | Rebuild reads the store and delivers only to the projection being rebuilt (no bus, no other subscribers). |
| D59 | Replay follows commit order (`events.id`) across types, grouping consecutive events of one aggregate; replay states live in a bounded cache restored from snapshot + tail when evicted; a duplicate's first event reuses the base's in-memory state when the base is exactly at `base_version`. |
| D60 | Rebuild delivers exactly the live event sequence, including undone events and undo events, with state as it was at each version. |
| D61 | Rebuild applies many aggregates per transaction with offsets saved in the same transaction; it is resumable. The in-memory subscription mode and `dump()` are removed (supersedes D14). |
| D62 | Projections whose listeners are all `AggregateListener` are rebuilt with one message per aggregate carrying the final state (snapshot + tail), `created = true`. |
| D63 | A projection version bump rebuilds that projection only; an aggregate context version bump invalidates its snapshots and rebuilds only projections that receive that type's state; explicit targeted rebuild for one aggregate, one type or one organization (R14). |
| D70 | A registry row per projection name holds the active version; queries go to the active version; switch-over is one database update; the old version stops after the rollout. |
| D56 (R14) | Projection status (lag, failed aggregates, position); targeted rebuild; many aggregates per transaction; slices later (after 1.0). |
| D56 (R19, R20, R21) | A small `Metrics` interface with a no-op default; tracing context carried in metadata (R13). In-memory adapters for every new feature and an event-schema reference test utility (R20). Durable scheduled commands (R21) are not built. |
| D14 | Superseded by D61. |

### 1.7 Cluster and upgrades

| ID | Decision |
|---|---|
| D42 | Cloud runs a multi-node Pekko cluster on one shared database; on-premise is a cluster of one that behaves like today. No separate databases per organization inside one cluster. |
| D43 | Cluster Sharding places aggregates by aggregate-id hash, opt-in per context; reaping becomes passivation; fairness between organizations is handled by limits, not placement. |
| D44 | Delivery stays outbox + push from the aggregate's owner (D8, D20). Pulling one global ordered log is a later performance option. |
| D45 | Commands, responses, events and states travel between nodes through one Pekko serializer built on mpjsons with the class name as manifest; Java serialization is off. |
| D68 | That serializer reads messages tolerantly (mpjsons `ignoreNonExistingFields`, defaults for missing fields); stored events keep strict reading; an incompatible message change gets a new class name. |
| D46 | Split Brain Resolver with a lease (Kubernetes lease on GKE); the SQL version check stays the last line of defence. Cluster Singletons: outbox poller, saga runner, replay orchestration, squash scheduler. |
| D53 | Each projection runs once in the cluster as a Cluster Singleton; owners push to it; its caches stay valid as the only writer; slices for hot projections come later. |
| D54 | The owner sends aggregate state with events only to subscriptions that need it, serialized by D45; gap fetches ask the owner for state through the sharded bus. |
| D55 | `GetAggregate`, `GetAggregateMinVersion` and projection queries keep their messages and are routed transparently to the shard owner or the projection singleton. |
| D65 | Goal: zero-downtime rolling upgrades from 1.0 on (cloud cluster; on-premise temporarily runs two nodes, or does a fast restart). |
| D66 | During a rollout work moves only from old to new nodes (`pekko.cluster.app-version` + default shard allocation, which avoids old nodes); singletons move once (oldest node last). Confirm on the chosen Pekko version with a spike. |
| D67 | A new event class (or `V<n>`, saga order class, squash event class) ships first with its handlers; it is written only after every node runs that version (per-feature write switch, or the next version). |
| D71 | A node is ready only after its subscriber list is registered, shards and proxies are up and the schema check passed; blocking startup waits (bus init timer and 180 s `Await`, 30 s `EnsureEventsPublished` delay, index builds) are removed or asynchronous. |
| D72 | Handover passivates an aggregate after its current command; outbox rows stay for the new owner; serial queues drain or reject with a retryable error; sagas resume from their stored step (requires idempotent saga commands, H3). |
| D73 | Every version's upgrade test includes a rolling-upgrade test (old and new in one cluster under load): no lost or doubled command, projections catch up, no deserialization errors, projection switch-over works. |

### 1.8 Organizations

| ID | Decision |
|---|---|
| D40 | Moving an organization = export one space (aggregates, events; cross-organization duplication bases materialized as squash events) + import with fresh ids only where legacy ids collide; typed id fields (`AggregateId` and Neula's wrappers) remapped by the codec, untyped references by a per-context rewriter; projections rebuilt for that space. |
| D41 | Physical separation of an organization means a separate installation (moved with D40), not partitioned tables. |
| D56 (R17) | Purge one organization across all framework tables and document stores; rebuild per organization; per-organization rate limits after 1.0. |

### 1.9 Performance and sagas

| ID | Decision |
|---|---|
| D34 | Low-risk performance fixes: blocking dispatcher for JDBC; id pools of 10 000; streaming restore (autocommit off + `fetchSize`) and index `events(aggregate_id, version)` replacing `events(aggregate_id)`; event handlers run once; batched outbox deletes; set-based `add_events`; type ids computed once; mpjsons serializers cached per class; no global lock in the `AggregateId` pool; bounded subscription cache; keyed delayed queries; lazy log strings. |
| D35 | All commits stay synchronous. Projection throughput comes from batching many aggregates per transaction instead. |
| D21 | Sagas: the `throw` inside `onComplete` (saga silently lost) is High; missing `sagas` index Medium; ignored row counts Medium; Neula's saga-issued commands become idempotent (H3). |
| D22 | Every still-valid CLAUDE.md §7 item is carried into this file with an updated severity; items resolved by a decision are marked so; stale items are dropped. |

---

## 2. Invariants

Changes must keep these true (from `ARCHITECTURE.md` §10, extended by the decisions).

| ID | Invariant |
|---|---|
| I1 | One writer per aggregate; versions grow by exactly one per event; the SQL procedures stay the final optimistic lock. |
| I2 | Events, aggregate rows, outbox rows, idempotent responses and outbox-hook rows are written in one transaction; publication happens after commit. |
| I3 | An outbox row is deleted only when every subscriber has acked or recorded a failure. |
| I4 | Subscribers deduplicate by version and advance only on contiguous versions. |
| I5 | Listener writes and the subscription offset update share one transaction; caches change only after commit. |
| I6 | Event handlers are pure: restore, replay, snapshot restore and live application produce the same state. |
| I7 | Class names of stored types (aggregates, events, saga orders, responses, projections) are stable identifiers; a rename is a data migration. |
| I8 | Aggregate roots are immutable and shared by reference. |
| I9 | Stored data written by an older version stays readable (D28, D69). |

---

## 3. Issues in the library

Status is **open** for every issue unless noted. "Fix in" is the milestone from the
implementation plan.

### Delivery and subscriptions

#### F1 — In-memory bus cursor never advances for aggregates that existed before the process started
- **Medium**, likely · **Fixed in M2** (16/10/2026, bus rewrite: no cursor, D7; `EventsBusActorSpec` "Bookkeeping") · Owning module: Delivery (`EventsBusActor`, `EventBusState`)
- **Symptom:** memory grows with uptime; per-ack CPU grows; the bus-side duplicate filter does nothing for older aggregates.
- **Root cause:** `MemoryEventBusState` starts every aggregate at 0 (`EventBusState.scala:22-27`); the first finished version of an older aggregate is never cursor+1, so every acked version is appended to `eventsPropagatedNotPersisted` and re-sorted (`EventsBusActor.scala:435-448`). `publishedEvents` (`:95`) and the state map (`EventBusState.scala:20`) are never evicted either.
- **Related:** F4, F11, F47; D7. **Accepted production risk until 1.0 (D80).**
- **Test:** publish v11 for an aggregate unknown to the bus, ack it, repeat; assert bus bookkeeping returns to empty.
- **Fix:** delete the cursor and its maps (D7).
- **Invariants:** I3, I4.

#### F2 — Unacknowledged events are not re-sent while their repository actor is alive
- **High**, likely · **Fixed in M2** (16/10/2026, outbox poller, D8; `OutboxPollerSpec`) · Delivery, `AggregateRepositoryActor`, `AggregateCommandBusActor`
- **Symptom:** a projection that once failed on an event (transient error) stays behind for that aggregate; later events are buffered as gaps until the actor is reaped (12 h idle, or never for a busy aggregate).
- **Root cause:** the re-send list is loaded only at restore (`AggregateRepositoryActor.scala:77-99`); `pendingPublish` is never re-sent; `EnsureEventsPublished` skips existing actors (`AggregateCommandBusActor.scala:132-153`); listener exceptions are only logged (`ProjectionActor.scala:476-477, 623-626, 709-710`).
- **Related:** F45, J3, R3; D8, D20a–c.
- **Test:** drop the first delivery to a subscriber while the aggregate stays active; assert delivery within the re-delivery interval.
- **Fix:** D8 (single retry owner reading the outbox), D20a–c.
- **Invariants:** I3.

#### F3 — In-memory subscription mode loses progress unless `dump()` is called
- **Low**, likely (Neula uses it only in its rebuild process) · Fix in **M4** · `SubscriptionsState`
- **Symptom:** a live node built with `keepInMemory = true` stalls projections after a restart.
- **Root cause:** progress is written only by `dump()` (`SubscriptionsState.scala:246-255, 303-350`), called only by the replayer (`EventsReplayerActor.scala:179-182, 198`).
- **Fix:** remove the mode (D61).

#### F4 — Already-published events are dropped without an ack; the in-flight counter leaks
- **Medium**, confirmed · **Fixed in M2** (16/10/2026, bus rewrite: no cursor, D7) · Delivery
- **Reproduced by:** `EventsReplaySpec` (PostgreSQL bus state) when the schema already holds ShoppingCart events, written by earlier runs or, with per-run schemas, by the specs that run before it: the replay never receives `ConsumerFinished` and times out after 50 s. It passes on a schema without earlier events.
- **Regression test:** since M2 `EventsReplaySpec` replays notes of its own aggregate type after a live projection applied them and their outbox rows were deleted. Replaying the other specs' carts, as before, stalls on their undone events instead (F58).
- **Symptom:** outbox rows of re-sent events are never deleted and re-sent every 180 s; back-pressure capacity shrinks; a replay against an advanced cursor never finishes.
- **Root cause:** events at or below the cursor are filtered (`EventsBusActor.scala:294`) but counted in `receivedInProgressMessages` (`:358`) and never finished; no `PublishEventsAck` is sent (`:365-368`).
- **Related:** F1; D7.
- **Fix:** removed together with the cursor (D7); keep counters balanced in the new bus.

#### F5 — Event listeners receive already-processed events on a partial overlap
- **High**, likely · **Fixed in M2** (16/10/2026: every listener kind goes through the sequence gate and gets only the versions after its applied one; `ProjectionDeliverySpec`) · `ProjectionActor` (sequence gate)
- **Symptom:** non-idempotent event listeners apply events twice (wrong read models); a duplicate `insertDocument` throws, the batch is never acked and the aggregate stalls.
- **Root cause:** `processEventsUpdate` passes `e.events` instead of the new events (`ProjectionActor.scala:437, 445, 461, 467`), unlike the aggregate-with-events path (`:608, 615`). Overlaps happen through re-sends.
- **Related:** F1, F2; D4.
- **Test:** sequence gate with offset 3 receives 2..5; assert the listener sees 4..5 only.
- **Fix:** D4 (one sequence gate for all listener kinds).
- **Invariants:** I4.

#### F6 — Clearing a projection's subscriptions leaves the caches behind
- **Medium**, likely · **Fixed in M2** (16/10/2026: clearing removes the subscriber's cached versions and failures too; `PostgresSubscriptionsStateSpec`, red first), obsolete after **M4** · `SubscriptionsState`
- **Symptom:** rebuilding a live projection skips aggregates still cached; in-memory mode updates rows that no longer exist.
- **Root cause:** `clearSubscriptionsInfo` deletes rows only (`SubscriptionsState.scala:291-294`); caches at `:131-132`.
- **Fix:** clear caches with the rows; shadow rebuild (D57) removes the clear step.

#### F11 — Bus cursor can skip an unfinished version
- **Low**, likely (needs custom classifiers; Neula has none) · **Fixed in M2** (16/10/2026, removed with the cursor, D7) · Delivery
- **Root cause:** one ack finishing a non-contiguous set moves the cursor to its last version (`EventsBusActor.scala:433-441`).

#### F15 — Per-aggregate async queues in parallel mode are never pruned
- **Low**, likely · **Fixed in M2** (16/10/2026: one queue per aggregate and listener kind, removed when idle; in parallel mode only one batch per aggregate runs at a time) · `ProjectionActor`
- **Root cause:** `states` grows per aggregate (`ProjectionActor.scala:120, 566`); used by Neula's two parallel listeners.
- **Fix:** remove a queue when it drains (sequence gate rework).

#### F18 — Subscription cache advanced before the transaction commits
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `SubscriptionsState`
- **Fix:** `PostgresSubscriptionsState.localTx` is an `AfterCommit.localTx`; the cache update is registered with `AfterCommit` and runs only after the commit. Test: `CachesAfterRollbackSpec` (subscription scenarios; the rollback one failed before the fix).
- **Symptom:** if the commit itself fails, later deliveries count as already processed and are skipped (Neula J2).
- **Root cause:** in DB mode the cache update runs inside the transaction block (`SubscriptionsState.scala:256-271`), before commit.
- **Test:** listener transaction whose commit fails; assert the same events are delivered again (Neula R2 acceptance).
- **Fix:** advance the cache after commit.
- **Invariants:** I5.

#### F26 — Outbox write amplification; one DELETE per ack, fire-and-forget
- **Medium**, likely · **Fixed in M2** (16/10/2026: the PostgreSQL store deletes delivered rows in batches on one thread, at most 20 ms late, and logs a failed batch with its cause; the old error log dropped the exception) · Delivery, `PostgresEventStoreState`
- **Root cause:** every event is inserted into and deleted from `events_to_publish`; the delete runs in an unsupervised `Future` per ack (`AggregateRepositoryActor.scala:377-382`).
- **Fix:** batched delete (`event_id = ANY(?)`) from one writer with failure handling; consider `(aggregate_id, version)` as the key. A published-version cursor instead of the table is a post-1.0 option (D44).

#### F28 — Every processed update re-runs all pending delayed queries
- **Low**, likely · **Partly fixed in M2** (16/10/2026: one replay per burst of applied updates. Keying the queries by aggregate needs the query to name its aggregate, an API change of little value; left as is) · `ProjectionActor`
- **Root cause:** `ReplayQueries` after each processed update (`ProjectionActor.scala:492-494, 641-643, 725-727, 771-793`); `scheduleNearest` scans the whole list.
- **Fix:** keyed delayed queries; coalesce replays (D34).

#### F29 — Subscription cache never evicted in live (database) mode
- **Medium**, likely · **Fixed in M2** (16/10/2026: an LRU cache of at most `maxCachedAggregates`, 10 000 by default; the in-memory replay mode keeps everything until `dump()`) · `SubscriptionsState`
- **Symptom:** memory grows with every aggregate a projection ever touched (about 0.5–1 KB each, estimate).
- **Root cause:** `perAggregate` is evicted only by `dump()`, which live mode never calls (`SubscriptionsState.scala:131-132, 196-223`).
- **Fix:** bounded cache; the database is the source of truth (D34).

#### F66 — The periodic check for unpublished events never ran a query
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresEventStoreState.readAggregatesWithEventsToPublish`
- **Symptom:** after the first check 30 s after start, events whose publication was lost were never found again by `EnsureEventsPublished` (every 60 s); only a repository actor restart re-sent them. Worsens F2.
- **Root cause:** `.bind(...).stripMargin.foreach` was chained after an `if/else`, so it applied only to the `else` branch; with `oldOnly = true` the method built a query and returned without running it.
- **Fix:** build the query, then bind and run it. Test: `EventStoreStateSpec` "… ids beyond 32 bits" (uses `oldOnly = true`).

#### F37 — Outbox scan query per aggregate type every minute
- **Low**, likely · **Fixed in M2** (16/10/2026: one outbox poller query for all types every 30 s) · `AggregateCommandBusActor`, `PostgresEventStoreState`
- **Root cause:** each command bus runs its own scan (`PostgresEventStoreState.scala:352-374`), 51 per minute in Neula.
- **Fix:** one outbox poller for all types (D8; a singleton in the cluster, D46).

#### F41 — Bus initialization blocks and depends on a hand-maintained subscription count
- **Medium**, likely · **Fixed in M2** (16/10/2026, D15: `RegisterSubscribers`, no blocking wait, no count) · `EventsBusActor`, `EventBusSubscriptionsManager`
- **Symptom:** a wrong count stalls the bus at startup (180 s cycles that drop the triggering message) or starts it without the last projections.
- **Root cause:** `Await` of up to 180 s in `initSubscriptions` (`EventsBusActor.scala:187-205`); Neula hard-codes 106 (H2).
- **Fix:** D15.

#### F42 — A late subscription crashes the subscriptions manager and wipes its state
- **Medium**, likely · **Fixed in M2** (16/10/2026, manager removed by D15) · `EventBusSubscriptionsManager`
- **Root cause:** `throw` at `EventBusSubscriptionsManager.scala:69`; the default restart clears `subscriptionsRequests`.

#### F45 — Unbounded projection delay buffers
- **Medium**, likely · **Fixed in M2** (16/10/2026: a gap is asked from the owner, again every 10 s while it lasts; only the deliveries in flight wait; `ProjectionDeliverySpec`) · `ProjectionActor`
- **Root cause:** `delayed*Update` maps (`ProjectionActor.scala:105-107`) hold messages until a gap fills, which may never happen (F2).
- **Fix:** gap fetch from the store (D20c).

#### F47 — `PostgresEventBusState` batch update ignores row counts
- **Low** (unused by Neula), likely · **Fixed in M2** (16/10/2026, deleted, D7) · `EventBusState.scala:127-140`

#### F77 — A failed compare-and-set leaves the subscription cache stale
- **Medium**, likely · **Fixed in M2** (16/10/2026: a failed database write evicts the aggregate's cached versions; `PostgresSubscriptionsStateSpec`, red first) · `PostgresSubscriptionsState.newEventIdInDB`
- **Symptom:** when the stored subscription version moved without this instance's cache (another node, a clear in the database, a repair), every later update of that aggregate for that projection fails the same way: the projection is stuck on it until the process restarts.
- **Root cause:** the failed `UPDATE … WHERE aggregate_version = ?` throws, the transaction rolls back, and the cached version is kept; the next attempt starts from it again.
- **Found:** plan review at the M1 close (review 3, §7).
- **Fix:** evict the cached entry when the database check fails, so the next attempt reads the stored version.

#### F79 — The event bus's 120-second debug timer changes actor state from another thread
- **Medium**, likely · **Fixed in M2** (16/10/2026, bus rewrite: the timer sends the bus a `Tick` message) · `EventsBusActor` constructor (`scheduler.schedule(120 s, …, new Runnable …)`)
- **Symptom:** the timer iterates `messagesSent` while the actor changes it, updates `lastLogged` and `orderedMessagePostponedTimestamp`, and calls `orderMoreMessagesToConsume()`: data races on the bus's mutable state, possibly `ConcurrentModificationException` or a corrupted map.
- **Root cause:** the timer runs a `Runnable` on a dispatcher thread instead of sending the actor a message.
- **Found:** plan review at the M1 close.
- **Fix:** the rewritten bus sends itself a tick message; statistics are read in `receive` only.

#### F80 — A failing projection query restarts the projection actor
- **Medium**, likely · **Fixed in M2** (16/10/2026: a failing message or query is answered with `Status.Failure`, never rethrown; the parallel pool is shut down on stop) · `ProjectionActor.delayIfNotAvailable` and the "empty list of events" throws
- **Symptom:** a query whose search throws is answered with the failure and then thrown again, which restarts the actor: its delay buffers and waiting queries are dropped, and with parallel processing a new thread pool is created while the old one is never shut down.
- **Root cause:** exceptions leave `receive`; no supervision policy (AR7); the thread pool is created per actor instance.
- **Found:** plan review at the M1 close.
- **Fix:** answer the failure without rethrowing; one shared, closed-on-stop pool or the blocking dispatcher (F27).

Found at the M2 close (review 4: the delivery soak of the bench, a read-only audit, our own reading). All
but F93 are fixed in M2; the tests named were red first, or red under a mutation that restores the old behaviour.

#### F83 — A long re-publication went out in one message, and a busy projection asked for it again every 10 s
- **High**, confirmed (the soak ran out of a 2 GB heap) · **Fixed in M2** · `AggregateRepositoryActor.republish`, `ProjectionActor.requestMissingVersions`
- **Symptom:** a projection that starts behind an aggregate with 30 000 events asks for the whole history; the owner sends it in one message, and while the projection works through its mailbox the 10-second re-request sends it again and again, until the heap runs out.
- **Fix:** re-publications go in parts of at most 1 000 events (`maxRepublishEvents`): the owner sends the next outbox part when the previous one is acknowledged, a projection asks for the next part of a gap itself. Gap requests repeat after 10 s, doubling up to 5 minutes. Tests: `RepublishSpec` (parts), `ProjectionDeliverySpec` (back-off).

#### F84 — The bus tracked a subscriber, not each of its subscriptions
- **High** if a projection has two listener kinds for one aggregate type, confirmed · **Fixed in M2** · `EventsBusActor`
- **Symptom:** one kind's acknowledgement ended the delivery for the other kind too: the owner deleted the outbox row while the other kind still retried, and a permanent delete could run before the slower kind applied it.
- **Fix:** a delivery waits for each (subscriber, kind); `MessageAck` and `MessageFailed` carry the `DeliveryKind`. Test: `EventsBusActorSpec` "waits for each kind a subscriber takes".

#### F85 — A failure that could not be stored was still answered as failed
- **High**, confirmed · **Fixed in M2** · `ProjectionActor.giveUp`
- **Symptom:** during a database outage longer than the retries, `recordFailure` failed too, but the deliveries were answered `MessageFailed`: the outbox rows were deleted and, after a restart, neither the rows nor the failure existed; the aggregate stayed stale until its next event.
- **Fix:** nothing is answered until the failure is stored; the batch is tried again every 30 s meanwhile. Test: `ProjectionDeliverySpec` "a failure that cannot be stored is not answered".

#### F86 — Deliveries covered by a batch were acknowledged before the batch was applied
- **Medium**, confirmed · **Fixed in M2** · `ProjectionActor.drainFrom`
- **Symptom:** a delivery whose versions all lay in the batch being assembled was acknowledged at once; if the batch then failed and the process stopped, its outbox row was gone (a gap fill has no rows of its own).
- **Fix:** such deliveries join the batch and are acknowledged when it is applied; only deliveries at or below the stored version are acknowledged at once. Test: `ProjectionDeliverySpec` "a delivery covered by a batch that then fails".

#### F87 — A stopped subscriber silently missed later events
- **Medium**, confirmed · **Fixed in M2** · `EventsBusActor.subscriberStopped`
- **Symptom:** the bus dropped a stopped subscriber's subscriptions, so later events were acknowledged without it and their rows deleted; after a restart it never received them for idle aggregates.
- **Fix:** a stopped subscriber keeps its subscriptions, marked stopped: deliveries it would take end without an owner ack, so the rows stay until the next start; `DeliveryStatus.stopped` lists it. Test: `EventsBusActorSpec` "a subscriber that stops ... later events stay in the outbox for it".

#### F88 — Retrying a failure discarded it before the re-published versions arrived
- **Medium**, confirmed · **Fixed in M2** · `ProjectionActor.retry`
- **Symptom:** the failure record was cleared and one untracked request sent; when that request was lost (no command bus registered yet at start, an owner in failed restore) the aggregate stayed stale with no record.
- **Fix:** a retry is a tracked request, repeated like a gap until a batch reaches the failed version; then the record is cleared. `RetryAllFailedSubscriptions` retries every failure, 20 per second. A skip still asks once for the versions after it: there may be none. Test: `ProjectionDeliverySpec` "RetryFailedSubscription asks for the failed version again, until it comes".

#### F89 — In parallel mode a gap check could start a second batch while one was running
- **Low**, likely · **Fixed in M2** · `ProjectionActor` (`Drain`, `Applied`)
- **Fix:** a scheduled drain during a running batch is left to the batch's end, and a batch's result is tied to its queue. Test: `ProjectionDeliverySpec` "in parallel mode a gap check while a batch runs".

#### F90 — A gap that could not be filled was asked for forever
- **Low**, confirmed · **Fixed in M2** · `ProjectionActor.requestMissingVersions`
- **Symptom:** a deleted aggregate, a missing publisher or a failing owner left the projection asking every 10 s, its queue never idle, the owner never reaped.
- **Fix:** after `gapGiveUpAfter` (10 minutes) the gap is recorded as a failure at its first version, visible and repairable. Test: `ProjectionDeliverySpec` "a gap that is never filled".

#### F91 — Delivered rows stayed in the outbox when their owner had been reaped
- **Medium**, confirmed (measured) · **Fixed in M2** (re-measured in `baseline-M2`) · `AggregateRepositoryActor`, `AggregateCommandBusActor`, `PostgresEventStoreState` (`OutboxDeletes`)
- **Symptom:** after 1 000 000 events at about 12 000 commands/s, 555 666 delivered rows (546 704 in a second run) were still in the outbox after the projections caught up; only the poller's re-publications, 1 000 aggregates per 30 s, would clear them.
- **Root cause:** the bus acknowledged the owners, and the command bus reaps idle owners (beyond 200, idle more than 2 minutes, every 5 minutes; re-publications count as activity and trigger it). Acknowledgements that arrived after the reaping, because the projections lagged, went to stopped actors. First taken for slow deletes.
- **Fix:** owners publish on behalf of their command bus (`publicationAcks`), which lives as long as the system and deletes the rows itself; a live owner gets the acknowledgement only to continue re-publishing its outbox. Deletes go as two arrays (`unnest`), up to 5 000 rows per statement. Test: `OutboxPollerSpec` "Deleting delivered rows happens when the delivery is acknowledged after the aggregate's owner has stopped" (red first).

#### F92 — Logs flooded under overload
- **Medium**, confirmed (measured) · **Fixed in M2** · `EventsBusActor.checkDeadlines`, `OutboxPollerActor`, `ProjectionActor`
- **Symptom:** the soak wrote about 960 000 "delivery waits" lines (one per delivery) and one "undelivered" line per aggregate; a failing listener warned at every retry.
- **Fix:** at most 10 lines per tick or poll, with a count and the periodic summary for the rest; a failing batch warns once.

#### F93 — No back-pressure on the live path
- **Medium**, confirmed (measured) · Fix in **M4** (projection throughput, D61) and a flow-control design after it · `EventsBusActor`, `ProjectionActor`
- **Symptom:** when commands come faster than a projection applies them, its mailbox and the bus's deliveries grow with the lag. Measured: 1 000 000 events at about 12 000 commands/s against projections at about 1 700 events/s ended 10 minutes behind; it fit in a 2 GB heap (peak not measured).
- **Fix idea:** a window per subscription in the bus; past it, events are left in the outbox and fetched from the store when the subscriber catches up (`RepublishRange`), so memory holds keys, not payloads.

#### F95 — A subscribable projection's timer outlived it, and its cache of recent updates was trimmed only by new updates
- **Low**, likely (found at the M2 simplification pass) · **Fixed in M2** · `SubscribableProjectionActor`
- **Symptom:** the one-minute `ClearIdleSubscriptions` timer was never cancelled, so a stopped projection (a rebuild's, or one stopped and created again within a system) left a timer sending to dead letters for the life of the actor system. The cache of recent updates (`updatesCacheTTL`) was trimmed only when the next update arrived, so after a burst it kept the burst until then.
- **Fix:** the timer is cancelled when the projection stops, and each tick also trims the cache to its TTL. No test: neither is visible in the actor's answers.

### Commands and aggregates

#### F7 — Event timestamps differ between live delivery and the store
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`, `PostgresEventStoreState`
- **Symptom:** state and projections that use event timestamps (Neula: process flows, reports, functions) differ after restore, re-send or replay; the difference is the persist queue wait.
- **Root cause:** the live path uses the executor's `Instant` (`CommandExecutorActor.scala:198`); SQL stores a later `Instant.now` (`PostgresEventStoreState.scala:72, 85, 99, 112, 172`).
- **Test:** `EventStoreStateSpec` "Events keep the timestamp they were persisted with" (single and batched writes; failed on PostgreSQL before the fix). An end-to-end check through the actors (Neula R6) is still to be written with the projection work in M2.
- **Fix:** D10. The executor takes one timestamp per command, truncated to microseconds (PostgreSQL's precision); the store writes that timestamp instead of its own `Instant.now`. Follow-up: F65.
- **Invariants:** I6.

#### F8 — History rewrite is half-applied
- **Medium**, likely · Fix in **M3** (contract, D17) and **M6** (complete rewrite, D76) · `PostgresEventStoreState`, `AggregateRepositoryActor`
- **Symptom:** a rewrite to a different class corrupts history; the live root keeps old values; projections are not told; duplicates and snapshots keep old values (GDPR).
- **Root cause:** `overwriteEvents` updates only the JSON (`PostgresEventStoreState.scala:180-186`); no rebuild after rewrite (`AggregateRepositoryActor.scala:128-134`).
- **Fix:** D17, then D76.

#### F9 — Idempotent command responses: broken index, ambiguous lookup, race
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandResponseState`, `Idempotency`
- **Status:** index part fixed first (D24): `initSchema()` failed on a fresh database with `column "aggregate_id" does not exist`, which blocked `ReactiveTestDomainSpec` and `EventsReplaySpec`; it now drops the old index if present and creates a unique index on `key`. Collision handling, the race and retention fixed next (D12, R7).
- **Symptom:** `initSchema()` fails on a fresh database, or the lookup scans the table; duplicate keys make `responseByKey` throw; two concurrent identical commands can both execute.
- **Root cause:** unique index on `(aggregate_id, key)` but no `aggregate_id` column (`CommandResponseState.scala:36-53`); `.single()` on a non-unique key (`:60-65`); check-then-act (`CommandHandlerActor.scala:95-106`).
- **Test:** `IdempotentCommandsSpec` (sequential, concurrent first command, concurrent `Command`, concurrent `ConcurrentCommand`, after a restart, caller-chosen id) and `CommandResponseStateSpec` (both adapters). Before the fix every concurrent case got no reply at all: the key collision was thrown out of the persist transaction and restarted the repository actor (AR3).
- **Fix:** D12, D24, through the new `Idempotency` module (AR2). The response insert is `ON CONFLICT (key) DO NOTHING`; a taken key throws `IdempotencyKeyTakenException`, which rolls the persist back, and the executor answers with the stored response; a version conflict of an idempotent command is answered the same way when a copy already stored its response. Stored responses older than the retention period (default 30 days, a `PostgresCommandResponseState` parameter) are removed hourly by each command bus (index on `handling_timestamp`). R7's caller-chosen id: `FirstCommand.chosenAggregateId`, with ids from the new `ReserveAggregateId` bus message.
- **Operations:** the insert needs the unique index, so `initSchema()` must run (H5): without it every idempotent command fails loudly.

#### F10 — A failed restore leaves a dead actor reference cached
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor`, `AggregateRepositoryActor`
- **Symptom:** commands for that aggregate time out (60 s) until reaping.
- **Root cause:** restore in the constructor (`AggregateRepositoryActor.scala:101`) → `ActorInitializationException` → default Stop; refs stay in maps (`AggregateCommandBusActor.scala:84-85, 198-207`); no `watch`.
- **Test:** `FailedRestoreSpec` (before the fix, both the command and the query got no reply and the 10 s ask timed out).
- **Fix:** D11. The repository actor restores on its first message. After a failure it answers every query with `Failure(AggregateRestoreFailedException)` (the command executor turns that into `CommandHandlingError`) and refuses persists with `EventHandlingError`; after `failedRestoreRetryAfter` (default 5 s, an `AggregateCommandBusActor.apply` parameter) it stops. The command bus watches its repository actors and on `Terminated` forgets the actor and its command handler, so the next request tries the restore again. The "remember the failure briefly" part of D11 is the failed actor itself staying alive for that window. A request routed in the moment between the stop and the `Terminated` message still waits for the command timeout.

#### F12 — Aggregate ids read as `Int`
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresEventStoreState`
- **Fix:** read as `Long`. Test: `EventStoreStateSpec` "… ids beyond 32 bits" (with `rs.int` the ids came back wrapped to other, sometimes negative, values).
- **Root cause:** `aggregateHandler(AggregateId(rs.int(1)))` (`PostgresEventStoreState.scala:371`) overflows past 2^31.

#### F13 — Command executor lingers after a `CommandFailure`
- **Low**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`
- **Test:** `CommandOutcomesSpec` (the executor was still alive after the failure reply).
- **Fix:** stop after replying.

#### F19 — Stale aggregate after an external write; concurrent commands spin until timeout
- **Medium**, confirmed (High in a cluster) · **Fixed in M1** (16/10/2026) · `AggregateRepositoryActor`, `CommandExecutorActor`
- **Fix:** the repository actor reloads from the store when asked for a version newer than its own, and after an SQL concurrent-modification error. Test: `ConcurrentModificationSpec` (an external write followed by a concurrent command, or by a command expecting the new version, now succeeds; both failed before the fix).
- **Symptom:** after another node, a rewrite or an SQL repair writes the aggregate, every `ConcurrentCommand` conflicts until the 60 s executor timeout (Neula J6).
- **Root cause:** the SQL conflict is answered with the in-memory version, never reloaded (`AggregateRepositoryActor.scala:300-307`); the executor re-asks the same stale actor without limit (`CommandExecutorActor.scala:78-85`).
- **Test:** external write, then a command on the same aggregate succeeds (Neula R4).
- **Fix:** reload on conflict; D37.

#### F20 — Async handler past 60 s gets no reply
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`, `AggregateContext`
- **Fix:** `AggregateContext.commandTimeout` (default 60 s); when it passes, the caller gets `CommandHandlingError` and the executor stops, in every state. The command may still complete afterwards (the warning log says so); idempotency keys (R7) protect a retry. Test: `CommandOutcomesSpec` (no reply before the fix).
- **Fix:** D56 (R5): configurable timeout and an explicit error reply.

#### F24 — Concurrent-command retry storm
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`
- **Fix:** D37: re-runs after a conflict wait with jittered exponential back-off (10–500 ms) and stop after 10 attempts with `AggregateConcurrentModificationError`. Test: `ConcurrentModificationSpec` "gives up …" (timed out after 10 s before the fix).
- **Root cause:** unlimited retries without back-off (`CommandExecutorActor.scala:78-85`); handler CPU about N²/2 under a burst of N (estimate).
- **Fix:** D37; serial mode (D31) for contexts that opt in.

#### F25 — Event handlers run twice per persisted event
- **Low**, likely · Fix in **M3** · `AggregateRepositoryActor.scala:150-172, 133`
- **Fix:** check the version before the trial, reuse the trial result (D34).

#### F44 — No snapshots: every restore replays the full history
- **Medium**, likely · Fix in **M3** · `AggregateRepositoryActor`, event store
- **Fix:** D30.

#### F46 — Command-bus child cache reap sorts the whole map
- **Low**, likely · Fix in **M5** (replaced by passivation, D43) · `AggregateCommandBusActor.scala:168-184`

#### F70 — An unknown message restarts the command bus and drops every command in flight
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor.receive` (catch-all)
- **Symptom:** a message the bus does not understand (for example a command class that does not extend a command type) gets no reply, and every command of that aggregate type running at that moment never gets one either.
- **Root cause:** the catch-all threw `IllegalArgumentException`; the default supervision restarts the bus, and the default `preRestart` stops all its children, executors included (AR7).
- **Found:** architecture review 2 (§7).
- **Test:** `CommandOutcomesSpec` "A message the command bus does not understand" (no reply, and the in-flight command's executor was stopped, before the fix).
- **Fix:** the bus logs a warning and answers `Status.Failure(IllegalArgumentException)`.

#### F73 — A concurrent history rewrite re-run after a conflict rewrites nothing
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor.onConflict`; present since 0.12
- **Symptom:** a `RewriteHistoryConcurrentCommand` (Neula's anonymization path) that meets a version conflict is re-run with no events: the retry stores its new event but rewrites none of the history, and the caller gets success.
- **Root cause:** the retry asked the repository for the current state only (`GetAggregateRootCurrentVersion`), while the first attempt asks for the state with the events to rewrite.
- **Found:** documentation audit at the M1 review (§7, review 3).
- **Test:** `ConcurrentModificationSpec` "re-run after a conflict, it still rewrites the history" (the creation event kept its old name before the fix).
- **Fix:** a retry asks for the same data as the first attempt.

#### F74 — Each events query for an earlier version leaves an actor behind
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateRepositoryActor.receiveReturnEvents`; present since 0.12
- **Symptom:** every `GetEventsForAggregateForVersion` starts a temporary repository actor that never stops and is never reaped: memory grows with every call.
- **Root cause:** the other single-read answers stop the actor; this one did not.
- **Found:** documentation audit and plan review at the M1 review.
- **Test:** `CommandOutcomesSpec` "Queries for an earlier version leave no temporary actor behind" (three actors were left before the fix).
- **Fix:** one `stopIfSingleRead()` after every single-read answer.

#### F75 — A failing id generator restarts the command bus or the saga actor
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor` routing, `SagaActor`
- **Symptom:** when the uid generator fails, the command that needed an id gets no reply, and the bus restarts, which stops its children and every command in flight for that aggregate type. The saga actor restarts too and, because a restart ran `preStart` again, loaded its pending sagas a second time while the running ones went on: steps ran twice.
- **Root cause:** the blocking id wait (F23) throws inside `receive`; since F40 it does so at once instead of after 60 s. No supervision policy (AR7), and `SagaActor` did not override `postRestart`.
- **Found:** documentation audit and plan review at the M1 review.
- **Test:** `CommandOutcomesSpec` "A command bus whose id generator fails" (no reply, in-flight command lost, before the fix; also `ReserveAggregateId`), `SagaRecoverySpec` "A saga order when no saga id can be had" (no reply and a second load before the fix).
- **Fix:** the bus answers a command it cannot route with `CommandHandlingError` (and `ReserveAggregateId` with `Status.Failure`); the saga actor refuses the order with `SagaFailureResponse`; `SagaActor.postRestart` no longer reloads (not separately tested: no remaining path makes it throw). Other throws inside the bus's `receive` (the outbox scan query) remain for AR7.

#### F72 — Every reload starts another re-send timer chain
- **Low**, likely · **Fixed in M2** (16/10/2026, removed with the in-memory re-send, D8) · `AggregateRepositoryActor.assureRestoredState`, `resendEventsToPublish`
- **Symptom:** while a subscriber does not ack, an aggregate that was reloaded N times re-sends its waiting events N times every 180 s. Subscribers drop the duplicates by version (I4), so the cost is load and log noise.
- **Root cause:** each restore schedules `ResendPersistedMessages`, and each such message schedules the next one while events wait. Since M1 restores also happen on reloads (F19), after undo and duplication events, and after failed persists.
- **Found:** M1 code review (review 3, §7).

#### F67 — A command for an aggregate that does not exist gets no reply until the timeout
- **Medium**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `CommandExecutorActor.scala` `handleFollowingCommand`, `handleRewriteHistoryCommand`
- **Symptom:** a `Command` or `RewriteHistoryCommand` whose expected version does not match an aggregate at version 0 (it does not exist, or the id is wrong) is answered only by the command timeout (`CommandHandlingError` after 60 s) instead of `AggregateConcurrentModificationError`.
- **Root cause:** for version 0 the repository returns the aggregate whatever version was asked for; the executor then sends itself `AggregateConcurrentModificationError` while in the state that waits for the aggregate, which drops it as unhandled (AR4).
- **Found:** architecture review 1 (§7).
- **Test:** `CommandOutcomesSpec` "… a command for an aggregate that does not exist" (no reply within 10 s before the fix).
- **Fix:** the conflict is answered where it is detected, through the same conflict handling as persist conflicts.

#### F68 — A persist that fails part-way commits what it already wrote
- **High**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `AggregateRepositoryActor.persist`
- **Symptom:** a command is reported as failed (`EventHandlingError`) while part of it is stored: the first of several events when a later one fails its size or count check, or a history overwrite when the new event is rejected. The in-memory state does not include the stored part, and projections receive it later through the outbox.
- **Root cause:** `persistEvents` returns failures as a `Failure` value; the transaction block returned that value normally, so `DB.localTx` committed. SQL errors were rolled back by PostgreSQL itself (the transaction is aborted), but checks that throw in Scala after an earlier statement were not. Reachable only on the one-by-one path (a single event, or several with an undo or duplication event) and for rewrites; the batched path checks every event before writing.
- **Found:** architecture review 1, while making every persist outcome a reply (AR3).
- **Test:** `PersistTransactionSpec` (two events stored instead of none before the fix).
- **Fix:** every failure is thrown inside the transaction, which rolls back whole.

#### F69 — A reload after a version conflict applies the previous command's events twice
- **High**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `AggregateRepositoryActor`; introduced on 16/10/2026 by the F19 fix (not in 0.12.47)
- **Symptom:** after two commands race on one aggregate, the actor is one command ahead of the store (version 3 with two items where the store has version 2 and one item); later commands are judged against that wrong state, and the wrong state is sent to state-receiving listeners.
- **Root cause:** after a successful persist the repository sent itself `EventsPersisted` and applied the events only when that message came round. A `PersistEvents` already queued ahead of it passed the in-memory version check, failed in SQL, and the F19 reload read the store, which already held the events; `EventsPersisted` then applied them again.
- **Found:** architecture review 1, while tracing the concurrent idempotent path.
- **Test:** `ConcurrentModificationSpec` "after a version conflict the actor's state still matches the store" (version 3 instead of 2 before the fix).
- **Fix:** the events are applied right after the commit, before the next message; `EventsPersisted` is gone.

#### F81 — An expired min-version query is dropped without a reply
- **Low**, likely · Fix in **M3** (aggregate owner, AR1) · `AggregateRepositoryActor.receiveReturnAggregateRootMinVersion`, `replayDelayedQueries`
- **Symptom:** a `GetAggregateMinVersion` whose version is not reached within its time gets no answer; the caller only sees its own ask time out and cannot tell "not yet" from "lost".
- **Root cause:** expired queries are filtered out of `delayedQueries` without answering them, and only when another query or event arrives.
- **Found:** plan review at the M1 close.
- **Fix:** a timer per deadline answers the current version (or a "not reached" reply) when it expires.

### Event store and SQL

#### F22 — Restore loads the whole event stream into memory before handling it
- **Medium**, likely · Fix in **M3** · `PostgresEventStoreState`
- **Root cause:** `DB.readOnly` keeps autocommit on and no `fetchSize` is set (`PostgresEventStoreState.scala:202`), so the driver buffers every row (up to 500 000 in Neula).
- **Fix:** streaming read (D34); snapshots (D30).

#### F31 — `add_events` loops `add_event`; `add_undo_event` scans and sorts the whole history
- **Low**, likely · Fix in **M3** · `PostgresEventStoreSchemaInitializer.scala:178-194, 217-220`
- **Fix:** set-based SQL (D34).

#### F38 — Restore query order has no matching index; one index is redundant
- **Low**, likely · Fix in **M3** · `PostgresEventStoreSchemaInitializer.scala:49-59`
- **Fix:** index `events(aggregate_id, version)` built `CONCURRENTLY`, drop `events(aggregate_id)` in a later contract step (D34, D69).

#### F65 — Event times are stored without a time zone
- **Low**, likely · Fix in **M3** · `PostgresEventStoreState`, schema
- **Symptom:** on a JVM whose default zone is not UTC, event times can shift by an hour around a daylight-saving change, because `event_time` is `TIMESTAMP` (without time zone) and is converted with the JVM default zone on write and read. Neula forces UTC (`Startup.scala:16`), so it is not affected today.
- **Fix:** bind and read `event_time` with an explicit UTC calendar; the column type stays (D28). Promised as a separate entry by D10.

#### F39 — Instant reads filter in Scala
- **Low**, likely (1 call site in Neula) · Fix in **M3** · `PostgresEventStoreState.scala:233`
- **Fix:** push the `event_time` predicate into SQL.

#### F49 — Legacy `aggregates` rows have `space_id = -1`
- **Medium**, suspected (count unknown) · Pre-check in **M1** (`StoredDataCheck` reports it), backfill in **M4** (before the per-organization rebuild needs it) · schema
- **Root cause:** the column was added with `DEFAULT -1` (`PostgresEventStoreSchemaInitializer.scala:117-121`); six deprecated Neula events use `SpaceId.unknown`.
- **Check:** `SELECT count(*) FROM aggregates WHERE space_id = -1` on production.
- **Fix:** D38.

#### F76 — Permanently deleting an aggregate that duplicates read from breaks the duplicates
- **High** if Neula deletes such aggregates, suspected · Fix in **M3** (with the duplicate detach of D50) · `PostgresEventStoreState.deletePublishedEventsToPublish` (`PermanentDeleteEvent` branch)
- **Symptom:** after a `PermanentDeleteEvent` on an aggregate that has duplicates, those duplicates restore without the history they share with it, so their state is silently wrong; their chain rows in `aggregates` point at a base that no longer exists.
- **Root cause:** the delete removes the base's events and rows only; duplicates read the base's events through their chain rows (D50 solves the same problem for squash).
- **Found:** plan review at the M1 close.
- **Check:** Neula, on a production copy: `SELECT count(*) FROM aggregates a WHERE a.base_id <> a.id AND NOT EXISTS (SELECT 1 FROM aggregates b WHERE b.id = a.base_id AND b.base_id = b.id)` (duplicates whose base is gone).
- **Fix:** detach every duplicate before deleting its base, as D50 does before a squash. With D20a the delete also needs a precondition like D51 (no failed subscriptions), because it removes the `subscriptions` rows that would hold the failure. The precondition is in since M2 (16/10/2026): the delete waits while the aggregate has a row in `subscription_failures` (`PermanentDeleteSpec`).

### Performance and runtime

#### F23 — Id pools of 100 and a blocking wait on refill
- **Medium**, likely · Fix in **M3** · `UidGeneratorSchemaInitializer.scala:18-28`, `AggregateCommandBusActor.scala:315-348`, `SagaActor.scala:136-152`
- **Fix:** pools of 10 000 (`ALTER SEQUENCE … INCREMENT BY`), asynchronous prefetch at a low-water mark (D34).
- **Note (16/10/2026):** since F40 a failing id store fails the `Await` at once instead of after 60 s; F75 makes the bus and the saga actor answer that failure instead of restarting. The `Await` itself (the bus blocks while a pool is fetched) goes with the asynchronous prefetch.

#### F27 — Blocking JDBC runs on the default dispatcher
- **Medium**, likely · Fix in **M3** · persist, restore, projections, futures
- **Fix:** a dedicated blocking dispatcher (D34).

#### F30 — Reflection per event on hot paths
- **Low**, likely · Fix in **M3** · `PostgresEventStoreState.scala:69, 82, 96, 109, 127`; `AggregateRepositoryActor.scala:312`
- **Fix:** precompute type ids; cache mpjsons serializers per class (D34).

#### F32 — Global synchronized LRU pool for `AggregateId`
- **Low**, likely · Fix in **M3** · `api/.../id/AggregateId.scala:8-27`
- **Fix:** drop the pooling (D34).

#### F33 — Small hot-path waste
- **Low**, likely · Fix in **M2** for `pendingPublish.distinct` (`AggregateRepositoryActor.scala:95, 136`, removed with D8: **fixed** 16/10/2026) and the ERROR-level debug line (`ProjectionActor.scala:645`, rewritten with the sequence gate: **fixed** 16/10/2026); in **M3** for the eager log strings

#### F40 — `println` in production paths
- **Low**, confirmed · **Fixed in M1** (16/10/2026) · `UidGeneratorActor.scala:39, 50, 61` (failures also leave the requester unanswered), `SagaActor.scala:47`, `EventsBusActor.scala:182-185`
- **Test:** `UidGeneratorActorSpec` (the ask timed out before the fix).
- **Fix:** logger instead of `println`; the uid generator answers a failure with `Status.Failure`, so the requester fails at once. The replay tools keep their console output (operator progress), reworked in M4.

#### F48 — In-memory caches have no cross-node invalidation
- **Medium** in a cluster, likely · Fix in **M5** · `PostgresDocumentStore`, `PostgresSubscriptionsState`
- **Fix:** one writer per projection (D53) keeps its caches valid.

#### F78 — The type-name cache is changed from many threads without a lock; two nodes can race on a new name
- **Medium**, likely · Fix in **M3** (with F30), the cross-node part in **M5** (with D67) · `PostgresTypesNamesState.typeIdByClassName`, `classNameById`, `insertName`
- **Symptom:** the first use of a new class from two threads at once updates a plain `HashMap` concurrently (the lock covers only the database read); two nodes storing a new class at the same moment both `INSERT` the name and one persist fails on the unique index.
- **Root cause:** `cache.getOrElseUpdate` runs outside the monitor; the insert has no conflict handling.
- **Found:** plan review at the M1 close.
- **Fix:** a concurrent map, and `INSERT … ON CONFLICT (name) DO NOTHING` followed by a read.

### Document store

#### F16 — Document cache can be ahead of the database after a rollback
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresDocumentStore`
- **Fix:** every cache write goes through `cacheAfterCommit`: inside an `AfterCommit.localTx` (which every projection update is) the entry is dropped at once and stored after the commit; an older version never replaces a newer one. Outside such a transaction caching is immediate, as before. Test: `CachesAfterRollbackSpec` (document scenarios; both rollback ones failed before the fix).
- **Symptom:** after a rolled-back listener transaction, reads serve documents the database does not have; later updates conflict.
- **Root cause:** cache writes inside the listener transaction (`insertDocument`, `updateDocument` in `PostgresDocumentStore.scala`; transaction at `ProjectionActor.scala:466-469`).
- **Fix:** D16.
- **Invariants:** I5.

#### F21 — Document store loses updates on a version conflict
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresDocumentStore`
- **Fix:** one versioned write (`writeNextVersion`) used by all four update paths: it reports a conflict, evicts the stale cache entry so the retry reads fresh data, and every path retries at most 10 times. `overwriteDocument` skips the JSON write only when a version check proves the cached copy is current; before, a stale cached copy equal to the new document left the database unchanged.
- **Tests:** `PostgresDocumentStoreConflictSpec` (failed before the fix: lost update `ac` instead of `bc`, "Too many tries" on a stale cache entry, `StackOverflowError` in the auto-id store). The overwrite case did not run before the fix (the suite aborted); its failure on the old code is inferred from reading.
- **Related:** cache writes before commit were fixed separately (F16).
- **Symptom:** a read-model change that meets a version conflict is silently dropped and the cache holds a document the database does not have; the auto-id store can loop forever. Probably happening in Neula today (about 60 cached stores, parallel listeners). **Accepted production risk until 1.0 (D80).**
- **Root cause:** in `updateDocumentRecur` the changed-document branch ends with `val updated = …` (`PostgresDocumentStore.scala:543-546`), so the block returns `Unit` and `updated == 0` is never true; retries re-read the same stale cache entry; the auto-id variant recurses without a limit (`:665-666`).
- **Test:** force a version conflict on a changed document; assert a retry against fresh data, or an error after the limit, never a silent success.
- **Fix:** return the row count, evict the cache entry before retrying, limit retries in both stores; D16 for commit-safe caching.

#### F43 — Document-store migration is not one transaction
- **Low**, likely · Fix in **M4** (where the document-store tables and indexes are reworked) · `PostgresDocumentStore.scala:30-33`
- **Fix:** run the migration steps in one transaction, following D69 for large tables.

### Sagas

#### F17 — Saga uses `ActorContext` off its thread
- **Low**, likely · **Fixed in M1** (16/10/2026, with F35) · `SagaActor.scala:96, 99`
- **Fix:** step results come back as messages; replies and decisions run on the actor's thread.

#### F34 — `sagas` table has no primary key or index
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresSagaSchemaInitializer.scala:10-23`
- **Test:** `PostgresSagaStateSpec` "holds one row per saga name and id" (a duplicate row was accepted before the fix).
- **Fix:** unique index `sagas_name_saga_id_idx (name, saga_id)` built `CONCURRENTLY`; an invalid index left by an interrupted build is dropped and rebuilt; duplicate rows stop `initSchema()` with an explicit message (Neula: check for duplicates on a production copy before upgrading, `SELECT name, saga_id FROM sagas GROUP BY 1, 2 HAVING count(*) > 1`).

#### F35 — Saga failures thrown inside `onComplete` lose the saga
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `SagaActor.scala:70, 80, 108, 132`
- **Symptom:** a failed persist leaves the saga unfinished, with no retry and only an unsupervised exception.
- **Test:** `SagaRecoverySpec`: writes that fail a few times (before the fix the caller never got an answer), an order that cannot be stored (no answer before), a saga that resumes after a restart without repeating its last step's effects.
- **Fix:** every write result comes back as a message; failed writes are retried with back-off (100 ms doubling to 30 s, at most `maxPersistAttempts` = 10, both overridable), then the saga is left at its last stored step and resumes after a restart. An order that cannot be stored is answered with `SagaFailureResponse`. Pending sagas are read completely before any starts, and a failed load is retried.

#### F36 — Saga update and delete ignore row counts
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresSagaState.scala:23-33`
- **Test:** `PostgresSagaStateSpec` (both returned `Unit` before the fix).
- **Fix:** `updateSaga` and `deleteSaga` return whether the row was there; the actor stops a saga whose row vanished, with an error log.

### Serialization (mpjsons 0.7)

#### F51 — mpjsons 0.7 may serialize the `TypeTag` held by every event
- **Closed, not a problem** (16/10/2026): mpjsons 0.7.0 skips the synthetic evidence field. `StoredFormatsSpec` shows event JSON without it, references recorded with 0.6.50 still read back, and the full suite passes on 0.7.0. Removing the `TypeTag` (D75) is still wanted for Scala 3, but no longer urgent.
- Original concern: `api/.../Event.scala:7`
- **Root cause:** 0.7 writes inherited fields; `Event[A: TypeTag]` stores its context bound as a field; 0.7 skips only fields reflection marks synthetic (mpjsons `ReflectionUtilNoCache.scala:50`). If not skipped, every event write fails.
- **Fix:** D75 removes the `TypeTag` from `Event`; the golden test proves the output.

#### F52 — JDK value types stored in the old private-field form cannot be read by 0.7
- **High** if present, suspected · Check in **M1** (dry run, D78)
- **Root cause:** mpjsons `docs/upgrading.md:30`; Neula's 2022 release may have run on JDK 8.
- **Fix:** register converters reading the old form, if the dry run finds such rows.
- **Tool (16/10/2026):** `io.reactivecqrs.core.tools.StoredDataCheck.run(mpjsons, typesNamesState, contexts, projectionTables)` reads every stored JSON value (events, sagas, idempotent responses, the projection tables given) with the mpjsons on the classpath and reports rows and unreadable rows per table and class, with the first failures; it also reports the F49 count and the D29 distribution. Neula runs it from its application against a production copy. Test: `StoredDataCheckSpec`.

#### F53 — An absent `Option` field reads as `None` instead of `null`
- **Medium**, suspected · Check in **M1** · mpjsons `docs/upgrading.md:31`; see H4.

#### F64 — Command responses with custom info cannot be serialized for idempotent commands
- **Medium**, confirmed · **Fixed in M1** (16/10/2026, with R7, D12) · `CommandResponseState.scala:58-61`
- **Symptom:** an idempotent command whose handler returns response info (`CustomSuccessResponse`) fails the whole persist transaction, so its events are not written. Neula has no idempotent commands yet; R7 and H3 will add them.
- **Root cause:** responses are serialized by class name (`mpjsons.serialize(response, response.getClass.getName)`); for `CustomSuccessResponse[INFO]` mpjsons cannot resolve `INFO` ("key not found: type INFO"), with both 0.6.50 and 0.7.0.
- **Test:** found while building `StoredFormatsSpec`; `IdempotentCommandsSpec` "a response with custom info …" (no reply before the fix) and `CommandResponseStateSpec`.
- **Fix:** the id and version are stored as a `SuccessResponse`, the info in new nullable columns `info` and `info_type_id` with its own class (additive schema change, D28). The info must be of a class without type parameters, such as a case class; otherwise storing fails with an explicit message. Existing rows are read as before (no row with custom info could have been written).

### Schema initialization

#### F63 — Concurrent schema initialization fails
- **Medium**, confirmed · Fix in **M5** (migration runner, D69) · `PostgresEventStoreSchemaInitializer`
- **Symptom:** two processes initializing the schema at once (test suites in parallel today; cluster nodes starting together later) fail with a duplicate key on `pg_proc_proname_args_nsp_index` for `add_event`.
- **Root cause:** `CREATE OR REPLACE FUNCTION` and the other initialization statements run without any lock.
- **Fix:** run migrations once, under a PostgreSQL advisory lock, outside node startup (D69). Tests now run one suite at a time.

#### F71 — Catalog lookups ignore the schema
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresUidGenerator.readSequenceStep`, `PostgresDocumentStore.readExistingIndices`, `PostgresEventStoreState.countAllEvents`, `PostgresSagaSchemaInitializer` (the F34 index check, new code of the same day)
- **Symptom:** in a database where another schema holds objects of the same names (a second installation, a copy, earlier test runs), the uid generator fails to start (`TooManyRowsException` from `pg_sequences`), a projection's indices or the saga key index are not created because the other schema's are seen, and the event count reads `public.events` whatever the current schema is.
- **Root cause:** catalog queries filtered by object name only, and one hard-coded `public`.
- **Found:** per-run test schemas (below): every testdomain spec failed at start.
- **Test:** `SchemaAwareCatalogSpec` (all three scenarios failed before the fix), and the whole suite now runs in a fresh schema per run.
- **Fix:** `schemaname = current_schema()`, `to_regclass(…)` and `'events'::regclass`, so the library works in whatever schema the connection's `search_path` selects. Also `countAllEvents` treats the `-1` estimate of a never-analyzed table (PostgreSQL 14+) like 0 and counts exactly.

### Rebuild

#### F54 — Rebuild needs downtime and clears read models first
- **Medium**, likely · Fix in **M4** · `EventsReplayOrchestrator.scala:79-81`; Neula runs it as a separate run mode
- **Fix:** D57, D70.

#### F55 — The rebuild clear is not atomic
- **Medium**, likely · Fix in **M4** · `SubscriptionsState.scala:291-294`; Neula clears documents in a separate auto-commit (e.g. `KanbansProjection.scala:30`)
- **Fix:** removed by D57.

#### F56 — Replayed events reach every subscriber of the type
- **Medium**, likely · Fix in **M4** · `EventsBusActor.scala:300-326`
- **Fix:** D58.

#### F57 — Rebuild order differs from live order
- **Medium**, suspected (depends on cross-type Neula projections) · Fix in **M4** · `EventsReplayerActor.scala:118-168`
- **Fix:** D59.

#### F58 — Rebuild hides undo, and the hidden versions stall the rebuild
- **High**, confirmed (raised from Medium in M2, 16/10/2026) · Fix in **M4** · `PostgresEventStoreState.scala:284`
- **Symptom:** a rebuild leaves out undone events and undo events. A projection that rebuilds from cleared state then sees a gap at those versions (a cart with an undo at version 5: 1–3, then 6), keeps the later event waiting for versions that never come, and never acknowledges it. So the replay does not end: it goes on only when the bus deadline (D20b, 10 minutes) ends the stalled deliveries, which leaves those aggregates behind in the rebuilt projection, or the replayer's own 10-minute timeout exits the JVM first (F82).
- **Found:** the full test run in M2. The old `EventsReplaySpec` replayed the carts of the other specs, one of them with an undo, and both projections kept version 6 waiting for 4 and 5. (They had also failed on version 1, because the spec reused the other specs' read-model tables; that was a test fault.)
- **Fix:** D60; see H6.

#### F59 — Rebuild skips events still in the outbox
- **Low**, likely · Fix in **M4** · `EventsReplayerActor.scala:155-168`
- **Fix:** rebuild reads the store directly (D58); live delivery dedups.

#### F60 — Rebuild progress is not durable
- **Medium**, likely · Fix in **M4** · `EventsReplayerActor.scala:179-182`
- **Fix:** D61.

#### F61 — Rebuild triggers are too coarse
- **Low**, likely · Fix in **M4** · `EventsReplayOrchestrator.scala:51-63`
- **Fix:** D63.

#### F62 — Rebuild throughput limits
- **Medium**, likely · Fix in **M4**
- **Root cause:** one transaction per aggregate per projection; a 100-event window in Neula (H1); every duplicate re-reads its base's history (`ReplayAggregateRepositoryActor.scala:70-77`); state-only projections replay every event; one replay actor per aggregate lingers 30 s; full state with every replayed message.
- **Fix:** D58, D59, D61, D62.

#### F82 — The replayer stops the whole JVM when a replay stalls
- **Medium**, likely · Fix in **M4** (replaced by the shadow rebuild) · `EventsReplayerActor` (`System.exit(-1)`)
- **Symptom:** a replay that times out ends the host application's process; a single unreadable stored event makes the replayer throw, so the replay never answers (found with per-run test schemas, where specs damaged events and left them).
- **Found:** plan review at the M1 close.
- **Fix:** report the failure to the requester and stop the replay only.

#### F94 — Failure records of a removed projection block permanent deletes
- **Low**, likely · Fix in **M4** (the projection registry, D57, removes a retired projection's rows) · `PostgresEventStoreState.deleteAggregatePermanently`
- **Symptom:** `subscription_failures` rows of a projection that was removed or renamed are never retried or cleared, and a permanent delete waits while the aggregate has any.
- **Found:** M2 close (review 4).
- **Until then:** an operator deletes the rows of the retired projection by its `subscriber_type_id`.

### Code structure

#### F14 — In-memory test doubles are wrong or incomplete
- **Low**, confirmed · **Fixed in M1, step 0** (16/10/2026) · in-memory adapters
- **Found and fixed:**
  - `MemorySubscriptionsState`: `clearSubscriptionsInfo` removed the *other* subscribers' entries; a stale compare-and-set failed silently; an update without a prior read was dropped.
  - `MemoryEventStoreState`: a command with several events got versions 1, 3, 5; the outbox read compared versions with the aggregate id, so it was always empty; deleting an already-deleted event threw; the re-publish scan ignored the aggregate type; duplicated aggregates did not include their base's history; `countEventsForAggregateTypes` and `overwriteEvents` were `???`.
  - `MemoryDocumentStore`: six query conditions were `???`, two were missing (match error), `ILIKE` was an exact match, batch inserts were `???`.
- **Tests:** `MemorySubscriptionsStateSpec`, `MemoryEventStoreStateTestDoubleSpec`, `MemoryDocumentStoreSpec` (query conditions); `MemoryEventStoreStateSpec` (duplication) now passes.
- **Still not mirrored** (documented gaps of the test double): instant reads, `PermanentDeleteEvent` clean-up, spaces in the document store.

#### F50 — Type keys disagree for nested classes; runtime reflection blocks Scala 3
- **Low**, likely · Fix in **M3** (with F30 and D29, which touch the same type keys) · `AggregateContext.scala:54` vs `ProjectionActor.scala:180-196`; also the outbox scan (`AggregateCommandBusActor`: `runtimeClass.getName`) vs persist (`typeSymbol.fullName`), so a nested aggregate class never gets its waiting events re-sent
- **Fix:** D75.

---

## 4. Issues on the Neula side

The library cannot fix these; they are listed so the 1.0 migration does not miss them.

| ID | Severity, confidence | Issue | When |
|---|---|---|---|
| H1 | Low, likely | `eventBusBufferSize = 100` (`neula.conf:348`) throttles rebuilds; obsolete once rebuild bypasses the bus (D58). | M4 |
| H2 | Medium, likely | Hard-coded expected subscription count 106 (`Context.scala:461-474`); replaced by the subscriber list (D15). | M2 |
| H3 | Medium, likely | About 29 sagas, but no command implements `IdempotentCommand`: a saga resumed after a crash can run its current command twice. Make saga-issued commands idempotent with `SagaStep`. | after M1 (needs D12) |
| H4 | Medium, suspected | `== null` checks on `Option` fields of events behave differently under mpjsons 0.7 (F53). | M1 gate |
| H5 | Medium, likely | `PostgresCommandResponseState` is built without `initSchema()` (`Context.scala:483`), so D24's index fix would not run; since the F9 fix every idempotent command then fails (the response insert needs the unique index). | M1 |
| H6 | Medium, suspected | Event listeners of ScreenRelease and ProcessRelease (the two aggregates with undo) must handle undo, since rebuild will deliver it (D60). | M4 |
| H7 | Medium, likely | The host wiring changes with M2: create one `OutboxPollerActor`; send the bus `RegisterSubscribers` with every projection and listener (with H2); drop `EventBusSubscriptionsManagerApi` and `PostgresEventBusState`; call `PostgresSubscriptionsState.initSchema()`, which creates `subscription_failures`. Show `GetProjectionStatus` failures and offer `RetryFailedSubscription` and `SkipFailedVersion` in the admin tools (R1, R14). `SubscriptionsState`'s six per-kind version methods became `lastVersion` and `newVersion` with a `SubscriptionType`; `ProcessFlowDetailsProjectionSpec` calls the old ones. | M2 (Neula) |

---

## 5. Answers to Neula's questions

From §7 of Neula's change requests.

| # | Question | Answer |
|---|---|---|
| 1 | Where does the squash snapshot event live? | Squash events are in the `events` table at version N (D47, D48). Performance snapshots are in a separate `snapshots` table and never consume versions (D30). Projections see squash events like other events; rebuilds of state-only projections start from snapshots (D62). |
| 2 | Can audit events stay below a squash point? | Not in 1.0's basic squash; R11's selective squash keeps them in a readable archive table (D52). BusinessEntity contexts should not opt in before that. |
| 3 | Is the organization the existing `SpaceId`, required on every aggregate? | Yes (D38), with a backfill for legacy rows. |
| 4 | Which delivery mechanism in the cluster? | Outbox + push from the aggregate's owner, with bounded re-delivery and gap fetch from the store (D44, D20a–c). |
| 5 | Does anything change for rows with `event_type_version = 0`? | No (D29). Non-zero rows make the reader fail with a clear message; a production check runs before release. |
| 6 | Are serial mode and cluster ownership one mechanism? | Yes (D31). |
| 7 | How do state-receiving listeners get state in the cluster? | The owner sends it with the events (serialized, D54); gap fetches ask the owner. |

---

## 6. Index by milestone

| Milestone | Issues |
|---|---|
| M1 | F7 ✔, F9 ✔, F10 ✔, F12 ✔, F13 ✔, F14 ✔, F16 ✔, F17 ✔, F18 ✔, F19 ✔, F20 ✔, F21 ✔, F24 ✔, F34 ✔, F35 ✔, F36 ✔, F40 ✔, F64 ✔, F66 ✔, F67 ✔, F68 ✔, F69 ✔, F70 ✔, F71 ✔, F73 ✔, F74 ✔, F75 ✔; AR2 ✔, AR3 ✔; checks F49, F51 ✔ (not a problem), F52, F53, F76 (Neula, on a production copy, with `StoredDataCheck` and the F76 query); Neula H3, H4, H5 |
| M2 | AR6 (M2 part ✔), AR7 (event bus and projections ✔); F1 ✔, F2 ✔, F4 ✔, F5 ✔, F6 ✔, F11 ✔, F15 ✔, F26 ✔, F28 (coalesced ✔), F29 ✔, F33 (part ✔), F37 ✔, F41 ✔, F42 ✔, F45 ✔, F47 ✔, F72 ✔, F77 ✔, F79 ✔, F80 ✔, F83 ✔, F84 ✔, F85 ✔, F86 ✔, F87 ✔, F88 ✔, F89 ✔, F90 ✔, F91 ✔, F92 ✔, F95 ✔; Neula H2, H7 |
| M3 | AR1, AR4, AR7 (owner and command bus); AR8; F8 (contract), F22, F23, F25, F27, F30, F31, F32, F33 (rest), F38, F39, F44, F50, F65, F76, F78, F81 |
| M4 | F3, F43, F49 (backfill), F54, F55, F56, F57, F58, F59, F60, F61, F62, F82, F93, F94; Neula H1, H6 |
| M5 | AR5; F46, F48, F63, F78 (between nodes) |
| M6 | F8 (complete) |

---

## 7. Architecture reviews

Regular reviews of the code touched by the work, at the level of modules and their
interfaces rather than single bugs. Each finding gets a proposal and the milestone where it
is best done; small ones are folded into the current work item.

### Review 1, 16/10/2026: the command pipeline (after F10, before F9)

Scope: `AggregateCommandBusActor`, `CommandHandlerActor`, `CommandExecutorActor`,
`AggregateRepositoryActor`, `CommandResponseState`.

#### AR1 — Two per-aggregate actors where every decision assumes one owner
- **Finding:** per aggregate the bus keeps a `CommandHandlerActor` and an
  `AggregateRepositoryActor` in two maps that must change together (F10 had to forget both
  at once). The command handler is shallow: five near-identical branches that create an
  executor and send the repository one query, plus the idempotency pre-check. D8, D31, D37,
  D43, D47 and D72 all speak of "the aggregate's owner" as one entity; the serial queue (D31)
  and the shard entity (D43) need exactly one.
- **Deletion test:** deleting `CommandHandlerActor` moves about 20 lines into the repository
  actor and removes one map, one actor per aggregate and one message hop per command.
- **Proposal:** one per-aggregate owner (the repository actor, renamed `AggregateOwner`)
  that owns restore and lifecycle, idempotency, the command queue (concurrent or serial),
  persistence and publication. The bus keeps one map. Executors stay as per-command runners
  of possibly asynchronous handlers, as children of the owner.
- **When:** M3, first work item, before serial mode and snapshots; M5 sharding then turns
  the owner into the shard entity.

#### AR2 — Idempotency is spread over four modules
- **Finding:** the key is extracted from the command in three actors (handler pre-check,
  executor, repository) and stored through `CommandResponseState`; D12's collision handling
  would add two more places. Low locality: a change to the key rules touches every actor of
  the pipeline.
- **Proposal:** one `Idempotency` module with a small interface: the stored response for a
  command, recording a response inside the persist transaction, recognising a taken key, and
  retention. Callers never see keys.
- **When:** now, with F9 (M1). **Done** 16/10/2026: `core/commandhandler/Idempotency.scala`.

#### AR3 — The persist transaction's failure modes are not part of its interface
- **Finding:** `persist` handles a `Failure` returned by `persistEvents`, but anything thrown
  inside the transaction (storing the idempotent response, a history overwrite, and in M3 the
  outbox hook of D33) escapes `receive`, restarts the actor and leaves the executor without a
  reply until the command timeout.
- **Proposal:** every outcome of the persist transaction becomes exactly one reply to the
  executor (persisted, version conflict, idempotency key taken, error); no exception leaves
  the transaction block.
- **When:** now, with F9, because D12 adds an outcome and D33 will add code inside the
  transaction. **Done** 16/10/2026; doing it exposed F68.

#### AR4 — The executor's protocol is untyped, so a reply in the wrong state is lost
- **Finding:** replies to the executor are a mix of `Try`, case classes and exceptions sent
  to itself; every state ends in a catch-all `unhandled`. A reply arriving in a state that
  does not expect it is dropped silently and the caller waits for the timeout (F67).
- **Proposal:** a sealed reply protocol between owner and executor, handled the same way in
  every state. With AR1 the executor's protocol shrinks to "run the handler, return the
  result".
- **When:** M3 with AR1; F67 is fixed now.

### Review 2, 16/10/2026: sagas, id allocation and failure isolation (after F35)

Scope: `SagaActor`, `PostgresSagaState`, `UidGeneratorActor`, and how failures travel from
these modules to the command bus.

#### AR5 — Saga authors subclass the runner
- **Finding:** a saga is written by extending `SagaActor` and supplying `uidGenerator`,
  `state` and `name`; user code and the framework's write-and-retry state machine share one
  class and one actor context. That sharing caused F17 (user-visible `context` used from
  callbacks) and makes the runner's interface the whole actor API. Aggregates already show
  the deeper shape: the author supplies an `AggregateContext` (data and functions), the
  framework owns the actors.
- **Proposal:** a `SagaDefinition` (name, `handleOrder`, `handleRevert`) run by a
  framework-owned saga runner; the caller to answer is kept as a serialized `ActorRef`
  instead of a path string, which also works across nodes (D45).
- **When:** M5, with the saga runner singleton (D46).

#### AR6 — Retry and back-off rules are repeated with different shapes
- **Finding:** the executor retries concurrent commands (10 attempts, 10–500 ms with
  jitter), the saga runner retries writes (10 attempts, 100 ms–30 s), the document store
  retries updates (10 immediate attempts), and M2 adds bounded re-delivery (D20b). Each has
  its own arithmetic and its own limit, so tuning them or reporting on them means visiting
  every one.
- **Proposal:** one `Backoff` module in `core/util` (delay for an attempt, jitter, limit),
  configured per use.
- **When:** M2, when the delivery retries are built; the saga retries move onto it then, the executor retry in M3 with AR1 and AR4 (which rewrite it). The document-store retry stays immediate: it runs inside the listener transaction, so waiting would hold its locks (refined in review 3).

#### AR7 — A per-request failure can restart a long-lived actor
- **Finding:** there is no `supervisorStrategy` anywhere, so Pekko's default applies: a
  throw restarts the actor, and the default `preRestart` stops all its children. The
  command bus threw on an unknown message (F70, fixed now) and still throws when id
  allocation fails (F23 note); a restart of the bus drops every in-flight command of that
  aggregate type without a reply.
- **Proposal:** long-lived actors (command bus, aggregate owner, event bus, saga runner)
  never throw for a failure that belongs to one request; they answer it. Each parent states
  explicitly what happens to a failed child (executors stop, owners stop and are forgotten
  as in F10).
- **When:** M2 for the event bus and the projection actors (rewritten there; F79, F80), M3 for the aggregate owner and the command bus (with AR1). Done in M1: F70 and F75 (unknown messages, id failures). Still open in the bus: a failing outbox-scan query throws inside `receive` (refined in review 3).

### Review 3, 16/10/2026: closing M1 (architecture description, plan and code)

Scope: `docs/ARCHITECTURE.md`, the remaining milestones of `docs/IMPLEMENTATION-PLAN.md` and
this document, checked against the code at the end of M1 (`cc8925c`). Two read-only audits
(the description against the code, the plan's premises against the code) and our own
reading; every finding below was checked in the code before it was recorded.

**Outcome.** The decisions hold: none is reversed. M1 changed no code that a later work item
depends on in a way that invalidates it. The plan needs ordering changes, notes on how later
items must build on what M1 added, and new issues. The architecture description had drifted
and is updated with this review.

**Fixed now, found by the audits:** F73 (a concurrent rewrite re-run after a conflict
rewrote nothing), F74 (a leaked actor per historical events query), F75 (id-generator
failures restarted the command bus and the saga actor).

**New issues:** F76 (PermanentDelete of a duplicate's base, High if it happens; Neula check
added), F77 (stale subscription cache after a failed compare-and-set), F78 (type-name cache
without a lock), F79 (bus timer off the actor thread), F80 (projection queries restart the
projection actor), F81 (expired min-version queries unanswered), F82 (replayer exits the
JVM). F72 came from our own reading.

**Moved:** F43 to M4, F50 to M3, the F49 backfill to M4, part of F33 to M2. AR6 and AR7
refined (above).

**Plan refinements** (recorded in the implementation plan):
- M1: the F4 reproduction (`EventsReplaySpec`) is marked `pendingUntilFixed`, so the suite is
  green and the test turns red when M2 fixes F4, as a reminder to make it a plain test.
- M2: the single retry owner (D8) separates "reload the state" from "re-send the outbox"
  (F72) and keeps the PermanentDelete trigger, which today rides on the acked events; gap
  fetches for event listeners keep noop rows (D60); the controllable clock also drives the
  timers M1 added (command timeout, retries, failed-restore window, retention).
- M3: the aggregate owner (AR1) absorbs the idempotency pre-check and the failed-restore
  answers, and drains executors on passivation (stopping a command handler stops its
  executors today); snapshots hook into `assureRestoredState` (all restore paths), fall back
  to a full replay when a snapshot cannot be read, and are invalidated inside the
  transaction of an undo or rewrite; the outbox hook runs inside an `AfterCommit` transaction
  and after `idempotency.record`; the blocking dispatcher (F27) must not bring back F69
  (persist synchronously or stash); M3's schema changes (procedures for D32, F31, D49, the
  snapshots table, F65) are one schema step.
- M4: batching across aggregates needs a transaction-local overlay of the subscription and
  document caches, because M1 defers cache updates to the commit (D16).
- M5: the migration runner and F63 come before the multi-node sharding tests; cluster
  serialization needs AR4's sealed protocol first (replies are `Try` today); a duplicate
  `FirstCommand` with a caller-chosen id during a handover must map the `aggregates` key
  violation to a conflict; `chosenAggregateId` is validated against the id ranges (D39); the
  retention clean-up becomes a singleton.
- M6: purge and complete rewrite need `commands_responses` rows tied to an aggregate (add
  the column before Neula stores responses, H3) and saga orders tied to a space; ephemeral
  aggregates (D77) cannot be built on the in-memory test double; the import round trip can
  use two schemas of one database (F71).
- M7: the upgrade runbook carries M1's prerequisites (`initSchema()` for command responses,
  H5; no duplicate saga rows, F34; `StoredDataCheck` on a production copy).

#### AR8 — Subscription classifiers do not mix with the sequence gate

- **Finding:** a subscription whose classifier leaves out some events of an aggregate sees
  gaps at those versions; asking the owner does not help, because the re-publication passes
  the same classifier, so the gap ends as a recorded failure after 10 minutes (F90).
  `ProjectionActor` subscribes with accept-all classifiers and Neula uses none, so nothing is
  affected today.
- **Proposal:** remove classifiers, or allow them only for subscribers that do not run the
  gate (their acknowledgements are tracked per accepted event already).
- **When:** M3, with the owner's protocol work (AR4); low priority.

### Review 4, 16/10/2026: closing M2 (architecture description, plan and code)

Scope: the delivery module as built (bus, outbox poller, re-delivery policy, sequence gate,
projections, subscription state, the owner's publication), `docs/ARCHITECTURE.md` §5–§7 and
the remaining milestones. Inputs: the bench's delivery soak at full size (1 000 000 events,
twice), a read-only audit of the M2 code, and our own reading; every finding was checked in
the code, and every fix has a test that was red first or red under a mutation.

**Outcome.** The delivery decisions hold (D3, D4, D7, D8, D15, D20a–c); none is reversed.
Refinements: D20a's failure record has a table of its own; re-publications go in parts of
1 000 events; the bus tracks each (subscriber, kind); a stopped subscriber keeps its
subscriptions so its events stay in the outbox; a retry is repeated until the failed version
is applied; a gap open for 10 minutes becomes a recorded failure. One problem stays open: the
live path has no back-pressure (F93).

**Fixed now:** F83–F92; F95 (found by the simplification pass at the close).

**New open issues:** F93 (no back-pressure on the live path, measured), F94 (failure records
of a removed projection block permanent deletes); AR8 (classifiers and the gate).

**Observations:**
- The pure parts held: the sequence gate and the re-delivery policy needed no change. Five of
  the seven audit findings were in the projection's queue handling, the deepest part of the
  module; it now has 17 scenarios, five of them checked by mutation.
- The bus keeps one entry per event in flight. Per-aggregate tracking (subscribers acknowledge
  in order, so "applied up to v" is enough) would make it one entry per aggregate and let the
  command bus delete "up to v"; it belongs with the flow-control work of F93, not before.
- Acknowledgements went to the owner, whose lifetime the command bus decides (F91). They now
  go to the command bus, which outlives every owner; the owner gets them passed on only to
  continue a paged re-publication.
- The audit proposed skipping the poller's re-publication while a delivery waits. It was tried
  and dropped: it saves little load, and a delivery lost while waiting (a subscriber restarted
  mid-message) would then wait for the deadline and the next start.
- The outbox operations live on `EventStoreState`: the event procedures write the rows, so
  that is where the seam is.

**Plan refinements** (recorded in the implementation plan):
- M3: the owner (AR1) keeps the delivery duties of M2 unchanged, with their tests; a
  re-publication from below a squash point starts at the squash event; F27's blocking
  dispatcher also covers the projections, the outbox poller and the replayer; AR8.
- M4: F93 comes with projection throughput (batching across aggregates, D61) and is measured
  with the M2 delivery soak; rebuilds must not depend on asking live owners for gaps (D60
  delivers the full sequence); the bench's other scenarios move to live subscriptions when the
  in-memory mode goes (D61); F94 with the projection registry.
- M5: re-publish requests reach the owner's shard through the command bus; the outbox
  poller's fresh attempts after a singleton handover are bounded by its per-poll limit; the
  metrics interface reads `GetDeliveryStatus`, `GetOutboxStatus` and `GetProjectionStatus`
  (R1's metric part).
- M7: the runbook carries M2's wiring changes (H7); the operator guide explains failures
  (status, retry, retry all, skip), "undelivered" and stopped subscribers.

**Looking ahead** (added at the user's request: reviews also weigh the changes the plan and
Neula's requests will bring, and shape the code so they are easy):
- *Outbox cleanup no longer depends on the owner's lifetime* (F91): owners publish on behalf
  of an actor passed in (`publicationAcks`, the command bus today), which deletes the rows and
  carries out permanent deletes. M3's passivation and M5's sharding can stop or move owners
  freely; in M5 a node's command bus deletes rows without waking a sharded owner, and only
  passes the acknowledgement on for re-publication paging.
- *M3 owner (AR1)*: the owner's delivery duties are three small ones (publish after commit,
  re-publish from the store in parts, continue on an acknowledgement); they move into
  `AggregateOwner` as they are, with their tests.
- *M4 rebuild*: the projection takes deliveries from any sender and answers that sender, so a
  rebuild can feed one projection directly (D58) through the same gate, failure records and
  status. Harder: batches across aggregates (D61) need `applyBatch`'s transaction to span
  several queues, plus the cache overlay noted at review 3; the per-queue structure allows it
  (the listener call and the version write are already separate steps).
- *M5 cluster*: the bus answers per (subscriber, kind) and acknowledges the actor named in the
  publication, both independent of where actors run; re-publish requests reach owners through
  the command bus. The delivery messages carry `Seq[EventInfo[Any]]` and `Option[Any]` state,
  so AR4's serializable protocol must cover them too.
- *F93 flow control*: the bus's per-subscription tracking gives the window's unit, and the
  targeted re-publication (`RepublishRange`) is the fetch-from-store path a window needs.
- *R19 metrics*: `GetDeliveryStatus`, `GetOutboxStatus` and `GetProjectionStatus` already hold
  the numbers; a metrics interface reads them rather than threading counters through the code.
- *Watch*: classifiers (AR8) are the one feature that conflicts with the gate; the bus's
  bookkeeping is per event in flight, which flow control would turn into per aggregate.
