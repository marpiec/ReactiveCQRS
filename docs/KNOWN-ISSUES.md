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
[5. Answers to Neula's questions](#5-answers-to-neulas-questions) · [6. Index by milestone](#6-index-by-milestone)

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
| D8 | One owner of retries: a single timer sends the aggregate's owner a re-publish message (creating the owner if needed); the owner re-reads its own `events_to_publish` rows and publishes them with its current state. The in-memory `eventsToPublish` and `pendingPublish` lists go away. |
| D20a | A failing subscriber retries the aggregate itself with back-off (about 2 minutes), then records the failure in its subscription row (failed version, error, since) and replies `Failed`. The publisher then stops re-delivering that event to it; the outbox row is deleted once every subscriber has acked or failed. Other aggregates keep flowing; the failure shows in status and metrics. Recovery: `RetryFailedSubscription`, and automatically when the projection starts (rate-limited); the subscriber then reads the missing events from the event store. An operator may also skip one version explicitly. Re-deliveries for an aggregate under internal retry are ignored. |
| D20b | Silence (no ack, no failure) gets limited re-delivery: at about 3 and 6 minutes, deadline 10 minutes (configurable; the first re-delivery comes after the subscriber's own retry budget). At the deadline: log and alert "undelivered to X", mark it in delivery status. The deadline counts from the first attempt in the running process; at startup every outbox row, including ones past their deadline, gets a fresh set of attempts. Stalled deliveries are logged: one line when an event first stalls, plus a periodic summary. Bus bookkeeping for an event ends on ack, failure or deadline (includes former D19); the bus watches its subscribers and logs an error if one stops. |
| D20c | A detected gap is filled from the event store: a subscriber at v5 that receives v7 reads v6 itself. Event listeners read events only; state-receiving listeners ask the aggregate's owner for the state at the needed version. No unbounded buffering. |
| D4 | One pure, in-process **sequence gate** decides for every subscriber message: ack only, apply versions a..b, or fetch the gap. The three listener kinds are thin adapters over it. |
| D15 | The host passes the bus the list of subscriber actors it created. On start the bus asks each one for its subscription requests (empty list allowed), holds publications without blocking (`Stash` + `pipeTo`) until all answered, logs missing ones by name while waiting. Subscriptions are fixed after start. `EventBusSubscriptionsManager`, push subscription from `preStart` and `minimumExpectedSubscriptions` are removed. |
| D9 | Superseded by D20a and R1 (D27). |
| D19 | Merged into D20b. |

### 1.5 Commands, aggregates and their features

| ID | Decision |
|---|---|
| D11 | A failed restore fails fast: the command bus watches its repository actors, removes dead refs, remembers the failure briefly and answers commands and queries for that aggregate with an explicit error. Restore moves from the constructor to the first message. |
| D12 | Idempotency keys are globally unique: a unique index on `commands_responses(key)`; a colliding command's transaction rolls back (including its events) and the caller gets the stored response. Usable by any command kind, including `FirstCommand` with a caller-chosen id (R7); stored responses have a retention period. |
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
- **Medium**, likely · Fix in **M2** · Owning module: Delivery (`EventsBusActor`, `EventBusState`)
- **Symptom:** memory grows with uptime; per-ack CPU grows; the bus-side duplicate filter does nothing for older aggregates.
- **Root cause:** `MemoryEventBusState` starts every aggregate at 0 (`EventBusState.scala:22-27`); the first finished version of an older aggregate is never cursor+1, so every acked version is appended to `eventsPropagatedNotPersisted` and re-sorted (`EventsBusActor.scala:435-448`). `publishedEvents` (`:95`) and the state map (`EventBusState.scala:20`) are never evicted either.
- **Related:** F4, F11, F47; D7. **Accepted production risk until 1.0 (D80).**
- **Test:** publish v11 for an aggregate unknown to the bus, ack it, repeat; assert bus bookkeeping returns to empty.
- **Fix:** delete the cursor and its maps (D7).
- **Invariants:** I3, I4.

#### F2 — Unacknowledged events are not re-sent while their repository actor is alive
- **High**, likely · Fix in **M2** · Delivery, `AggregateRepositoryActor`, `AggregateCommandBusActor`
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
- **Medium**, confirmed · Fix in **M2** · Delivery
- **Reproduced by:** `EventsReplaySpec` (PostgreSQL bus state) when the database already holds ShoppingCart events from earlier runs: the replay never receives `ConsumerFinished` and times out after 50 s. It passes on a database without earlier events.
- **Symptom:** outbox rows of re-sent events are never deleted and re-sent every 180 s; back-pressure capacity shrinks; a replay against an advanced cursor never finishes.
- **Root cause:** events at or below the cursor are filtered (`EventsBusActor.scala:294`) but counted in `receivedInProgressMessages` (`:358`) and never finished; no `PublishEventsAck` is sent (`:365-368`).
- **Related:** F1; D7.
- **Fix:** removed together with the cursor (D7); keep counters balanced in the new bus.

#### F5 — Event listeners receive already-processed events on a partial overlap
- **High**, likely · Fix in **M2** · `ProjectionActor` (sequence gate)
- **Symptom:** non-idempotent event listeners apply events twice (wrong read models); a duplicate `insertDocument` throws, the batch is never acked and the aggregate stalls.
- **Root cause:** `processEventsUpdate` passes `e.events` instead of the new events (`ProjectionActor.scala:437, 445, 461, 467`), unlike the aggregate-with-events path (`:608, 615`). Overlaps happen through re-sends.
- **Related:** F1, F2; D4.
- **Test:** sequence gate with offset 3 receives 2..5; assert the listener sees 4..5 only.
- **Fix:** D4 (one sequence gate for all listener kinds).
- **Invariants:** I4.

#### F6 — Clearing a projection's subscriptions leaves the caches behind
- **Medium**, likely · Fix in **M2** (cache handling), obsolete after **M4** · `SubscriptionsState`
- **Symptom:** rebuilding a live projection skips aggregates still cached; in-memory mode updates rows that no longer exist.
- **Root cause:** `clearSubscriptionsInfo` deletes rows only (`SubscriptionsState.scala:291-294`); caches at `:131-132`.
- **Fix:** clear caches with the rows; shadow rebuild (D57) removes the clear step.

#### F11 — Bus cursor can skip an unfinished version
- **Low**, likely (needs custom classifiers; Neula has none) · Fix in **M2** (removed with the cursor, D7) · Delivery
- **Root cause:** one ack finishing a non-contiguous set moves the cursor to its last version (`EventsBusActor.scala:433-441`).

#### F15 — Per-aggregate async queues in parallel mode are never pruned
- **Low**, likely · Fix in **M2** · `ProjectionActor`
- **Root cause:** `states` grows per aggregate (`ProjectionActor.scala:120, 566`); used by Neula's two parallel listeners.
- **Fix:** remove a queue when it drains (sequence gate rework).

#### F18 — Subscription cache advanced before the transaction commits
- **Medium**, likely · Fix in **M1** · `SubscriptionsState`
- **Symptom:** if the commit itself fails, later deliveries count as already processed and are skipped (Neula J2).
- **Root cause:** in DB mode the cache update runs inside the transaction block (`SubscriptionsState.scala:256-271`), before commit.
- **Test:** listener transaction whose commit fails; assert the same events are delivered again (Neula R2 acceptance).
- **Fix:** advance the cache after commit.
- **Invariants:** I5.

#### F26 — Outbox write amplification; one DELETE per ack, fire-and-forget
- **Medium**, likely · Fix in **M2** · Delivery, `PostgresEventStoreState`
- **Root cause:** every event is inserted into and deleted from `events_to_publish`; the delete runs in an unsupervised `Future` per ack (`AggregateRepositoryActor.scala:377-382`).
- **Fix:** batched delete (`event_id = ANY(?)`) from one writer with failure handling; consider `(aggregate_id, version)` as the key. A published-version cursor instead of the table is a post-1.0 option (D44).

#### F28 — Every processed update re-runs all pending delayed queries
- **Low**, likely · Fix in **M2** · `ProjectionActor`
- **Root cause:** `ReplayQueries` after each processed update (`ProjectionActor.scala:492-494, 641-643, 725-727, 771-793`); `scheduleNearest` scans the whole list.
- **Fix:** keyed delayed queries; coalesce replays (D34).

#### F29 — Subscription cache never evicted in live (database) mode
- **Medium**, likely · Fix in **M2** · `SubscriptionsState`
- **Symptom:** memory grows with every aggregate a projection ever touched (about 0.5–1 KB each, estimate).
- **Root cause:** `perAggregate` is evicted only by `dump()`, which live mode never calls (`SubscriptionsState.scala:131-132, 196-223`).
- **Fix:** bounded cache; the database is the source of truth (D34).

#### F37 — Outbox scan query per aggregate type every minute
- **Low**, likely · Fix in **M2** · `AggregateCommandBusActor`, `PostgresEventStoreState`
- **Root cause:** each command bus runs its own scan (`PostgresEventStoreState.scala:352-374`), 51 per minute in Neula.
- **Fix:** one outbox poller for all types (D8; a singleton in the cluster, D46).

#### F41 — Bus initialization blocks and depends on a hand-maintained subscription count
- **Medium**, likely · Fix in **M2** · `EventsBusActor`, `EventBusSubscriptionsManager`
- **Symptom:** a wrong count stalls the bus at startup (180 s cycles that drop the triggering message) or starts it without the last projections.
- **Root cause:** `Await` of up to 180 s in `initSubscriptions` (`EventsBusActor.scala:187-205`); Neula hard-codes 106 (H2).
- **Fix:** D15.

#### F42 — A late subscription crashes the subscriptions manager and wipes its state
- **Medium**, likely · Fix in **M2** (manager removed by D15) · `EventBusSubscriptionsManager`
- **Root cause:** `throw` at `EventBusSubscriptionsManager.scala:69`; the default restart clears `subscriptionsRequests`.

#### F45 — Unbounded projection delay buffers
- **Medium**, likely · Fix in **M2** · `ProjectionActor`
- **Root cause:** `delayed*Update` maps (`ProjectionActor.scala:105-107`) hold messages until a gap fills, which may never happen (F2).
- **Fix:** gap fetch from the store (D20c).

#### F47 — `PostgresEventBusState` batch update ignores row counts
- **Low** (unused by Neula), likely · Fix in **M2** (deleted, D7) · `EventBusState.scala:127-140`

### Commands and aggregates

#### F7 — Event timestamps differ between live delivery and the store
- **High**, likely · Fix in **M1** · `CommandExecutorActor`, `PostgresEventStoreState`
- **Symptom:** state and projections that use event timestamps (Neula: process flows, reports, functions) differ after restore, re-send or replay; the difference is the persist queue wait.
- **Root cause:** the live path uses the executor's `Instant` (`CommandExecutorActor.scala:198`); SQL stores a later `Instant.now` (`PostgresEventStoreState.scala:72, 85, 99, 112, 172`).
- **Test:** persist with a controlled clock; assert restored `EventInfo.timestamp` equals the live one (Neula R6).
- **Fix:** D10.
- **Invariants:** I6.

#### F8 — History rewrite is half-applied
- **Medium**, likely · Fix in **M3** (contract, D17) and **M6** (complete rewrite, D76) · `PostgresEventStoreState`, `AggregateRepositoryActor`
- **Symptom:** a rewrite to a different class corrupts history; the live root keeps old values; projections are not told; duplicates and snapshots keep old values (GDPR).
- **Root cause:** `overwriteEvents` updates only the JSON (`PostgresEventStoreState.scala:180-186`); no rebuild after rewrite (`AggregateRepositoryActor.scala:128-134`).
- **Fix:** D17, then D76.

#### F9 — Idempotent command responses: broken index, ambiguous lookup, race
- **Medium**, confirmed · Fix in **M1** · `CommandResponseState`
- **Status:** index part fixed 16/10/2026 (D24): `initSchema()` failed on a fresh database with `column "aggregate_id" does not exist`, which blocked `ReactiveTestDomainSpec` and `EventsReplaySpec`; it now drops the old index if present and creates a unique index on `key`. Still open: collision handling and the check-then-act race (D12), response retention (R7), F64.
- **Symptom:** `initSchema()` fails on a fresh database, or the lookup scans the table; duplicate keys make `responseByKey` throw; two concurrent identical commands can both execute.
- **Root cause:** unique index on `(aggregate_id, key)` but no `aggregate_id` column (`CommandResponseState.scala:36-53`); `.single()` on a non-unique key (`:60-65`); check-then-act (`CommandHandlerActor.scala:95-106`).
- **Test:** the same idempotent command twice, concurrently and after a restart: one set of events, the same response (Neula R7).
- **Fix:** D12, D24.

#### F10 — A failed restore leaves a dead actor reference cached
- **Medium**, likely · Fix in **M1** · `AggregateCommandBusActor`, `AggregateRepositoryActor`
- **Symptom:** commands for that aggregate time out (60 s) until reaping.
- **Root cause:** restore in the constructor (`AggregateRepositoryActor.scala:101`) → `ActorInitializationException` → default Stop; refs stay in maps (`AggregateCommandBusActor.scala:84-85, 198-207`); no `watch`.
- **Fix:** D11.

#### F12 — Aggregate ids read as `Int`
- **Medium**, likely · Fix in **M1** (before D39) · `PostgresEventStoreState`
- **Root cause:** `aggregateHandler(AggregateId(rs.int(1)))` (`PostgresEventStoreState.scala:371`) overflows past 2^31.

#### F13 — Command executor lingers after a `CommandFailure`
- **Low**, likely · Fix in **M1** · `CommandExecutorActor.scala:217-218`
- **Fix:** stop after replying.

#### F19 — Stale aggregate after an external write; concurrent commands spin until timeout
- **Medium**, likely (High in a cluster) · Fix in **M1** · `AggregateRepositoryActor`, `CommandExecutorActor`
- **Symptom:** after another node, a rewrite or an SQL repair writes the aggregate, every `ConcurrentCommand` conflicts until the 60 s executor timeout (Neula J6).
- **Root cause:** the SQL conflict is answered with the in-memory version, never reloaded (`AggregateRepositoryActor.scala:300-307`); the executor re-asks the same stale actor without limit (`CommandExecutorActor.scala:78-85`).
- **Test:** external write, then a command on the same aggregate succeeds (Neula R4).
- **Fix:** reload on conflict; D37.

#### F20 — Async handler past 60 s gets no reply
- **Medium**, likely · Fix in **M1** · `CommandExecutorActor.scala:38`
- **Fix:** D56 (R5): configurable timeout and an explicit error reply.

#### F24 — Concurrent-command retry storm
- **Medium**, likely · Fix in **M1** · `CommandExecutorActor`
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

#### F39 — Instant reads filter in Scala
- **Low**, likely (1 call site in Neula) · Fix in **M3** · `PostgresEventStoreState.scala:233`
- **Fix:** push the `event_time` predicate into SQL.

#### F49 — Legacy `aggregates` rows have `space_id = -1`
- **Medium**, suspected (count unknown) · Pre-check in **M1**, backfill in **M6** · schema
- **Root cause:** the column was added with `DEFAULT -1` (`PostgresEventStoreSchemaInitializer.scala:117-121`); six deprecated Neula events use `SpaceId.unknown`.
- **Check:** `SELECT count(*) FROM aggregates WHERE space_id = -1` on production.
- **Fix:** D38.

### Performance and runtime

#### F23 — Id pools of 100 and a blocking wait on refill
- **Medium**, likely · Fix in **M3** · `UidGeneratorSchemaInitializer.scala:18-28`, `AggregateCommandBusActor.scala:315-348`, `SagaActor.scala:136-152`
- **Fix:** pools of 10 000 (`ALTER SEQUENCE … INCREMENT BY`), asynchronous prefetch at a low-water mark (D34).

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
- **Low**, likely · Fix in **M3** · `pendingPublish.distinct` (`AggregateRepositoryActor.scala:95, 136`), eager log strings and an ERROR-level debug line (`ProjectionActor.scala:645`)

#### F40 — `println` in production paths
- **Low**, likely · Fix in **M1** · `UidGeneratorActor.scala:39, 50, 61` (failures also leave the requester unanswered), `SagaActor.scala:47`, `EventsBusActor.scala:182-185`

#### F48 — In-memory caches have no cross-node invalidation
- **Medium** in a cluster, likely · Fix in **M5** · `PostgresDocumentStore`, `PostgresSubscriptionsState`
- **Fix:** one writer per projection (D53) keeps its caches valid.

### Document store

#### F16 — Document cache can be ahead of the database after a rollback
- **High**, likely · Fix in **M1** · `PostgresDocumentStore`, `ProjectionActor`
- **Symptom:** after a rolled-back listener transaction, reads serve documents the database does not have; later updates conflict.
- **Root cause:** cache writes inside the listener transaction (`insertDocument`, `updateDocument` in `PostgresDocumentStore.scala`; transaction at `ProjectionActor.scala:466-469`).
- **Fix:** D16.
- **Invariants:** I5.

#### F21 — Document store loses updates on a version conflict
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresDocumentStore`
- **Fix:** one versioned write (`writeNextVersion`) used by all four update paths: it reports a conflict, evicts the stale cache entry so the retry reads fresh data, and every path retries at most 10 times. `overwriteDocument` skips the JSON write only when a version check proves the cached copy is current; before, a stale cached copy equal to the new document left the database unchanged.
- **Tests:** `PostgresDocumentStoreConflictSpec` (failed before the fix: lost update `ac` instead of `bc`, "Too many tries" on a stale cache entry, `StackOverflowError` in the auto-id store). The overwrite case did not run before the fix (the suite aborted); its failure on the old code is inferred from reading.
- **Not yet fixed:** cache writes still happen before commit (F16).
- **Symptom:** a read-model change that meets a version conflict is silently dropped and the cache holds a document the database does not have; the auto-id store can loop forever. Probably happening in Neula today (about 60 cached stores, parallel listeners). **Accepted production risk until 1.0 (D80).**
- **Root cause:** in `updateDocumentRecur` the changed-document branch ends with `val updated = …` (`PostgresDocumentStore.scala:543-546`), so the block returns `Unit` and `updated == 0` is never true; retries re-read the same stale cache entry; the auto-id variant recurses without a limit (`:665-666`).
- **Test:** force a version conflict on a changed document; assert a retry against fresh data, or an error after the limit, never a silent success.
- **Fix:** return the row count, evict the cache entry before retrying, limit retries in both stores; D16 for commit-safe caching.

#### F43 — Document-store migration is not one transaction
- **Low**, likely · Fix in **M3** · `PostgresDocumentStore.scala:30-33`
- **Fix:** run the migration steps in one transaction, following D69 for large tables.

### Sagas

#### F17 — Saga uses `ActorContext` off its thread
- **Low**, likely · Fix in **M1** · `SagaActor.scala:96, 99`

#### F34 — `sagas` table has no primary key or index
- **Medium**, likely · Fix in **M1** · `PostgresSagaSchemaInitializer.scala:10-23`
- **Fix:** unique index `(name, saga_id)` built `CONCURRENTLY`.

#### F35 — Saga failures thrown inside `onComplete` lose the saga
- **High**, likely · Fix in **M1** · `SagaActor.scala:70, 80, 108, 132`
- **Symptom:** a failed persist leaves the saga unfinished, with no retry and only an unsupervised exception.
- **Fix:** handle failures in the actor (message to self), retry with back-off, log.

#### F36 — Saga update and delete ignore row counts
- **Medium**, likely · Fix in **M1** · `PostgresSagaState.scala:23-33`

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

#### F53 — An absent `Option` field reads as `None` instead of `null`
- **Medium**, suspected · Check in **M1** · mpjsons `docs/upgrading.md:31`; see H4.

#### F64 — Command responses with custom info cannot be serialized for idempotent commands
- **Medium**, confirmed · Fix in **M1** (with R7, D12) · `CommandResponseState.scala:58-61`
- **Symptom:** an idempotent command whose handler returns response info (`CustomSuccessResponse`) fails the whole persist transaction, so its events are not written. Neula has no idempotent commands yet; R7 and H3 will add them.
- **Root cause:** responses are serialized by class name (`mpjsons.serialize(response, response.getClass.getName)`); for `CustomSuccessResponse[INFO]` mpjsons cannot resolve `INFO` ("key not found: type INFO"), with both 0.6.50 and 0.7.0.
- **Test:** found while building `StoredFormatsSpec`; a dedicated failing test comes with the fix.
- **Fix:** store the info value with its own class name next to the response.

### Schema initialization

#### F63 — Concurrent schema initialization fails
- **Medium**, confirmed · Fix in **M5** (migration runner, D69) · `PostgresEventStoreSchemaInitializer`
- **Symptom:** two processes initializing the schema at once (test suites in parallel today; cluster nodes starting together later) fail with a duplicate key on `pg_proc_proname_args_nsp_index` for `add_event`.
- **Root cause:** `CREATE OR REPLACE FUNCTION` and the other initialization statements run without any lock.
- **Fix:** run migrations once, under a PostgreSQL advisory lock, outside node startup (D69). Tests now run one suite at a time.

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

#### F58 — Rebuild hides undo
- **Medium**, likely · Fix in **M4** · `PostgresEventStoreState.scala:284`
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
- **Low**, likely · Fix in **M5** (groundwork; earlier for `Event` if F51 needs it) · `AggregateContext.scala:54` vs `ProjectionActor.scala:180-196`
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
| H5 | Low, likely | `PostgresCommandResponseState` is built without `initSchema()` (`Context.scala:483`), so D24's index fix would not run. | M1 |
| H6 | Medium, suspected | Event listeners of ScreenRelease and ProcessRelease (the two aggregates with undo) must handle undo, since rebuild will deliver it (D60). | M4 |

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
| M1 | F7, F9, F10, F12, F13, F14 ✔, F16, F17, F18, F19, F20, F21 ✔, F24, F34, F35, F36, F40, F64; checks F49, F51 ✔ (not a problem), F52, F53; Neula H3, H4, H5 |
| M2 | F1, F2, F4, F5, F6, F11, F15, F26, F28, F29, F37, F41, F42, F45, F47; Neula H2 |
| M3 | F8 (contract), F22, F23, F25, F27, F30, F31, F32, F33, F38, F39, F43, F44 |
| M4 | F3, F54, F55, F56, F57, F58, F59, F60, F61, F62; Neula H1, H6 |
| M5 | F46, F48, F50, F63 |
| M6 | F8 (complete), F49 (backfill) |
