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
| D75 | Keep a path to Scala 3: no new runtime reflection; aggregate and listener type keys move from `TypeTag`/`typeOf` to `ClassTag` or class names; `Event` stops carrying a `TypeTag`. *(M3: type keys and events, F50; M5: `StoredDataCheck`. Left on Scala 2 `TypeTag`, with mpjsons, which reads by it and by reflection: the document store's typed reads (`findDocumentPartByPaths` and the like, `PostgresDocumentStore`) and `SubscribableProjectionActor`'s update keys, where a `ClassTag` would erase type arguments and mix two subscriptions; both move when mpjsons is replaced, a step of its own after 1.0.)* |
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
| D39 | `AggregateId` stays one `Long`. New ids are unique across installations: `installationNumber × 10^12 + sequence`, kept below 2^53 for the TypeScript UI. Existing ids stay as a legacy range; negative ids stay reserved for ephemeral aggregates. Requires F12 fixed and a Neula check that no id is read as `Int`. The installation number may be issued with the licence. **Done in M5:** `IdRange` (`reactivecqrs.installation-number`, 0 by default: the legacy range below 10^12); `UidGeneratorActor.postgres(system)` makes aggregate ids in the range, moving the sequence forward when it is behind and refusing to start past the range or to hand out a pool past its end or 2^53; the command bus refuses a chosen id that is not positive, not below 2^53, or in another installation's range. |

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
| D51 | Squash runs only with no `events_to_publish` rows and no failed subscriptions for the aggregate; live projections are not notified; rebuilds see the squash event first; performance snapshots at or below N are deleted; historical reads below N return an explicit "squashed" error (BusinessEntity contexts do not opt in until R11's archive exists); disk space is reclaimed by (auto)VACUUM, shrinking files needs `pg_repack`/`VACUUM FULL` (operations note). **Refined in M3 (16/10/2026):** "no rows" means none at or below N (rows above N are not touched); a duplicate the squash detaches must have no outbox row at its version 1 and no failed subscription (the detach changes that event); an undo after N that undid events at or below N blocks it too; the preconditions are checked again inside the transaction, under the chain lock (F96). The squash event keeps event N's id and time. |
| D52 | Squash runs on explicit request (`SquashAggregate(id, upTo)`) or by an optional per-context policy run by the squash scheduler singleton. R11's selective squash with an audit archive is an extension on top. **Done in M6 (17/10/2026):** `SquashPolicy.keep` names the audit classes a squash moves into `events_archive` in its transaction, with their undo marks (the event at the squash point and a detached duplicate's first event too); `GetArchivedEvents(id)` reads them, nothing else does; `AutomaticSquash.keepNewerThan` keeps the recent history (one id boundary per run, not a walk through each aggregate's newer events); `StoredDataCheck.usersOf` says whether a class can go; a permanent delete takes the archive. |
| D85 | (16/10/2026, at the user's request) Every squash event is checked before it is stored: the event handlers apply it to the initial state as a restore will (by the user it is stored by, at the time of the event it replaces), and the result must equal (`==`) the state it replaces; otherwise the squash is refused, and so is a permanent delete that would detach a duplicate with it (F76). A squash event class that mixes in `SquashMayChangeState` may differ on purpose: it is not compared (its handler must still apply it), it takes every snapshot of its aggregate and of the duplicates reading it, and the owners of the aggregate and of all its duplicates read their states again (F101); projections are not told (D51). |
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
| D57 | Shadow rebuild: a new projection version builds into its own tables (`projection_<name>_v<N>`) under its own subscription identity (class + version), reading the event store; the old version keeps serving; when caught up it takes over, old tables are dropped; indexes are built before the switch; a failed rebuild leaves the old version untouched. Replaces clear-then-replay. **Refined in M4 (17/10/2026):** builds, not versions, own tables (`_v<N>`, then `_v<N>_b<build>` for a later build of the same version, as a context version bump or a repair rebuilds it) and subscriptions (`projection:<name><suffix>`, independent of the class); the switch waits while the new build has failed subscriptions; on one node, the active build keeps applying live events with the current code while a version is built (`ServeLive`, the default) or serves unchanged (`ServeStale`). |
| D58 | Rebuild reads the store and delivers only to the projection being rebuilt (no bus, no other subscribers). **Refined in M4:** a projection with side effects is not managed, so it is never rebuilt; a managed projection's listeners see `rebuilding` until its build serves. |
| D59 | Replay follows commit order (`events.id`) across types, grouping consecutive events of one aggregate; replay states live in a bounded cache restored from snapshot + tail when evicted; a duplicate's first event reuses the base's in-memory state when the base is exactly at `base_version`. |
| D60 | Rebuild delivers exactly the live event sequence, including undone events and undo events, with state as it was at each version. |
| D61 | Rebuild applies many aggregates per transaction with offsets saved in the same transaction; it is resumable. The in-memory subscription mode and `dump()` are removed (supersedes D14). **Refined in M4:** the offsets that decide, each aggregate's subscription version, commit with the chunk; the bulk pass's read position is saved after each applied chunk, so a resume repeats at most the chunks in flight, which those versions make harmless. Live deliveries that arrive together are applied the same way, up to 100 aggregates per transaction, their versions read at once; a group that fails is applied again one aggregate at a time. |
| D62 | Projections whose listeners are all `AggregateListener` are rebuilt with one message per aggregate carrying the final state (snapshot + tail), `created = true`. |
| D63 | A projection version bump rebuilds that projection only; an aggregate context version bump invalidates its snapshots and rebuilds only projections that receive that type's state; explicit targeted rebuild for one aggregate, one type or one organization (R14). |
| D70 | A registry row per projection name holds the active version; queries go to the active version; switch-over is one database update; the old version stops after the rollout. **Refined in M4:** the row holds the active build and the build in progress; a projection's supervisor, whose `ActorRef` the host uses for queries, routes them to the active build (or a new projection's first build, which serves while built); a projection 0.12 built is adopted as build 0 (D81). |
| D56 (R14) | Projection status (lag, failed aggregates, position); targeted rebuild; many aggregates per transaction; slices later (after 1.0). |
| D56 (R19, R20, R21) | A small `Metrics` interface with a no-op default; tracing context carried in metadata (R13). In-memory adapters for every new feature and an event-schema reference test utility (R20). Durable scheduled commands (R21) are not built. *(R19 done in M5, 17/10/2026: `Metrics`, `ReactiveMetrics`, `RecordingMetrics`; the measures in ARCHITECTURE §14.)* |
| D14 | Superseded by D61. |

### 1.7 Cluster and upgrades

| ID | Decision |
|---|---|
| D42 | Cloud runs a multi-node Pekko cluster on one shared database; on-premise is a cluster of one that behaves like today. No separate databases per organization inside one cluster. |
| D43 | Cluster Sharding places aggregates by aggregate-id hash, opt-in per context; reaping becomes passivation; fairness between organizations is handled by limits, not placement. **Done in M5:** `AggregateContext.placement = Placement.Sharded(numberOfShards)`; each node's command bus starts the type's shard region (`owner-<type>`) and sends everything to its owners through it (`ToOwner`); the shard passivates idle owners (F46); acknowledgements reach an owner only on its own node. `Local` stays the default. |
| D44 | Delivery stays outbox + push from the aggregate's owner (D8, D20). Pulling one global ordered log is a later performance option. **Done in M5:** a bus per node; an owner publishes to its node's bus; a subscriber that runs once is registered on every bus through its proxy, and the registration names the ref asked (`GetSubscriptionRequests(registered)`), so the supervisor behind a proxy answers for it; a bus asks a registered subscriber again when the subscribers it answered with stop, and its new answer replaces them. |
| D45 | Commands, responses, events and states travel between nodes through one Pekko serializer built on mpjsons with the class name as manifest; Java serialization is off. **Done in M5:** `ClusterSerializer`, bound to `java.io.Serializable`, writes the library's messages field by field (an `ActorRef` as its serialized path, a polymorphic value as its runtime class with its JSON), the application's values as the mpjsons JSON of their runtime class, and the library's exceptions with their fields (bound explicitly, so a `Status.Failure` keeps them too); any other exception arrives as a `RemoteFailure`. Messages defined inside an actor class or carrying functions stay on their node. |
| D68 | That serializer reads messages tolerantly (mpjsons `ignoreNonExistingFields`, defaults for missing fields); stored events keep strict reading; an incompatible message change gets a new class name. **Done in M5:** a library message is matched by field name (a field the reader lacks is skipped, a missing one takes its Scala default, else `None`, zero or null), so a library upgrade can add a field with a default; a changed field type needs a new class. The application's classes are read by mpjsons, where a missing field reads as zero, null or `None`, never its Scala default: a field added to one must be an `Option` (F135). |
| D46 | Split Brain Resolver with a lease (Kubernetes lease on GKE); the SQL version check stays the last line of defence. Cluster Singletons: outbox poller, saga runner, replay orchestration, squash scheduler. **Done in M5:** `Singletons.start` (a manager on every node, a proxy returned); the projection supervisors run as singletons through `Projections.start`; the host starts the outbox poller, the squash scheduler and each saga runner with it. A saga stores its caller with the caller's node address, so an answer never resolves to an actor of the same path on another node. |
| D53 | Each projection runs once in the cluster as a Cluster Singleton; owners push to it; its caches stay valid as the only writer; slices for hot projections come later. **Refined in M5:** the response clean-up stays with the command buses: an idempotent `DELETE`, hourly at random minutes, which N nodes run N times at no real cost; a singleton would add wiring for nothing. |
| D54 | The owner sends aggregate state with events only to subscriptions that need it, serialized by D45; gap fetches ask the owner for state through the sharded bus. |
| D55 | `GetAggregate`, `GetAggregateMinVersion` and projection queries keep their messages and are routed transparently to the shard owner or the projection singleton. **Done in M5:** owner queries go through the region (`ToOwner`), projection queries through the supervisor's proxy, with the caller as sender. |
| D65 | Goal: zero-downtime rolling upgrades from 1.0 on (cloud cluster; on-premise temporarily runs two nodes, or does a fast restart). |
| D66 | During a rollout work moves only from old to new nodes (`pekko.cluster.app-version` + default shard allocation, which avoids old nodes); singletons move once (oldest node last). Confirm on the chosen Pekko version with a spike. |
| D67 | A new event class (or `V<n>`, saga order class, squash event class) ships first with its handlers; it is written only after every node runs that version (per-feature write switch, or the next version). **Done in M5:** `AggregateContext.writtenFrom` (a class and the app-version from which every node reads it); each node keeps the lowest app-version among the members (`Members`, from the members' events) in its command buses' contexts; an owner refuses to persist such an event earlier (`CommandRejected`), and a squash such a squash event (`SquashRefused`); handlers ask `everyNodeRuns(version)`. All members at Pekko's 0.0.0 (no app-versions) or no cluster: everything is written. Saga order classes are not guarded: the runner, a singleton on the oldest node, writes them itself. |
| D71 | A node is ready only after its subscriber list is registered, shards and proxies are up and the schema check passed; blocking startup waits (bus init timer and 180 s `Await`, 30 s `EnsureEventsPublished` delay, index builds) are removed or asynchronous. **Done in M5:** `Readiness(system)` waits for each event bus until its subscribers answered (through the singletons' proxies, the singletons too); `ReadinessCheck` is a Pekko Management readiness check; the blocking startup waits were gone by M3. |
| D72 | Handover passivates an aggregate after its current command; outbox rows stay for the new owner; serial queues drain or reject with a retryable error; sagas resume from their stored step (requires idempotent saga commands, H3). **Done in M5 for owners:** the region's hand-off message is `StopOwner`, so an owner finishes its commands (its serial queue drains) and writes its passivation snapshot before it stops; a stop the bus asks for goes through the shard (`PassivateOwner`), which keeps what arrives meanwhile; `handoff-timeout` and the shutdown phase of the regions are 90 s in core's `reference.conf`, above the default command timeout. Sagas: step 5. |
| D73 | Every version's upgrade test includes a rolling-upgrade test (old and new in one cluster under load): no lost or doubled command, projections catch up, no deserialization errors, projection switch-over works. **Done in M5 (the test):** `RollingUpgradeSpec`: two nodes of 1.0.0 and two of 2.0.0, which adds an event class, under load from every node; the old nodes leave, the oldest last; no command lost or applied twice (clients retry through another node with an idempotency key), the projection has every item, the new class is written only once the old nodes are gone. Deserialization errors cannot show in one JVM, where every node has every class. |

### 1.8 Organizations

| ID | Decision |
|---|---|
| D40 | Moving an organization = export one space (aggregates, events; cross-organization duplication bases materialized as squash events) + import with fresh ids only where legacy ids collide; typed id fields (`AggregateId` and Neula's wrappers) remapped by the codec, untyped references by a per-context rewriter; projections rebuilt for that space. **Done in M6 (17/10/2026):** `SpaceExport` (JSON lines, streamed; copies of other organizations' aggregates detached with a checked squash event) and `SpaceImport` (two passes: a check that writes nothing, then the rows in commit order with new event ids; `AggregateId`, `UserId` and the space mapped by post-deserialization transforms of an `MPJsons` of its own; `ImportRewriter` per type); ARCHITECTURE §4.8. |
| D41 | Physical separation of an organization means a separate installation (moved with D40), not partitioned tables. |
| D56 (R17) | Purge one organization across all framework tables and document stores; rebuild per organization; per-organization rate limits after 1.0. **Done in M6 (17/10/2026):** `SpacePurge` (ARCHITECTURE §4.7): plan and refusals before anything changes, detaches of other organizations' copies through their command buses, batches of at most 1 000 aggregates and 100 000 events with the host's `PurgeHook`s in their transactions, owners stopped, projections told (`PurgedAggregates`, a drop window for deliveries on their way) and cleared (`PurgeSpace`), sagas tagged with the space; `commands_responses.aggregate_id` and `sagas.space_id` written from now on. |

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
- **Low**, likely (Neula uses it only in its rebuild process) · **Fixed in M4** (17/10/2026: the mode and `dump()` are removed; the database is the only source of truth, and one transaction may move a subscription several times, D61; `PostgresSubscriptionsStateSpec`, red under a mutation that drops the transaction's own versions) · `SubscriptionsState`
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
- **Medium**, confirmed (measured) · **Throughput part fixed in M4** (17/10/2026: deliveries that arrive together are applied up to 100 aggregates per transaction, their versions read in one query per listener kind (D61 on the live path); measured on one machine, 200 000 events of 20 000 counters with failing listeners in the first 30 s: every event projected after 62 s instead of 96 s, about 3 200 instead of 2 100 events/s; 3 200 commands on 16 counters: 2 600 to 2 900 instead of 1 500 events/s, 3.4 instead of 7.0 statements per command; `ProjectionGroupsSpec`). **Flow control fixed in M5** (17/10/2026: a window per subscriber of 10 000 events and 64 MB of their stored sizes not yet answered; what does not fit is held, its aggregate behind for that subscriber, and pulled from the owners once half the window is free; held deliveries complete when the subscriber answers the pulled part, so the outbox is the queue and its rows go as soon as the slow subscriber has them; `FlowControlSpec`, `EventsBusActorSpec` and `DeliveryEndToEndSpec`, caught under mutations that admit everything or never pull) · `EventsBusActor`, `FlowControl`, `ProjectionActor`
- **Symptom:** when commands come faster than a projection applies them, its mailbox and the bus's deliveries grow with the lag. Measured: 1 000 000 events at about 12 000 commands/s against projections at about 1 700 events/s ended 10 minutes behind; it fit in a 2 GB heap (peak not measured).
- **Fix idea:** a window per subscription in the bus; past it, events are left in the outbox and fetched from the store when the subscriber catches up (`RepublishRange`), so memory holds keys, not payloads.

#### F95 — A subscribable projection's timer outlived it, and its cache of recent updates was trimmed only by new updates
- **Low**, likely (found at the M2 simplification pass) · **Fixed in M2** · `SubscribableProjectionActor`
- **Symptom:** the one-minute `ClearIdleSubscriptions` timer was never cancelled, so a stopped projection (a rebuild's, or one stopped and created again within a system) left a timer sending to dead letters for the life of the actor system. The cache of recent updates (`updatesCacheTTL`) was trimmed only when the next update arrived, so after a burst it kept the burst until then.
- **Fix:** the timer is cancelled when the projection stops, and each tick also trims the cache to its TTL. No test: neither is visible in the actor's answers.

#### F113 — A failure at a squash start could not be skipped
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: `SkipFailedVersion` moves the subscription from the version it has to the failed one; `ProjectionDeliverySpec`, red under the old skip) · `ProjectionActor.skipFailedVersions`
- **Symptom:** a skip moved the subscription from the version before the failed one, but a failure at a squash event the subscription started at (F98) is recorded there while the subscription is at zero, so the compare-and-set failed and the failure stayed, keeping a build from switching and the aggregate's squash and permanent delete waiting.
- **Found:** M4 close (review 6), audit.

#### F115 — A cache fill could miss a commit made while it read
- **Medium**, confirmed (a narrow race) · **Fixed in M4** (17/10/2026: a read that fills the cache is not cached when a commit changed the aggregate's versions while it ran; `PostgresSubscriptionsStateSpec`, red when changes are not tracked) · `PostgresSubscriptionsState`
- **Symptom:** the versions of an aggregate not cached were read outside the monitor and cached after; a commit in between (another projection's new version, a targeted rebuild's reset) changed only cached aggregates, so the read cached an old version: behind the database (a spurious gap, a group applied again one batch at a time), or, after a reset, ahead of it, so the rebuild skipped the aggregate.
- **Found:** M4 close (review 6), audit.

#### F121 — Messages between nodes can exceed Artery's frame and are dropped
- **High**, confirmed (measured sizes; the drop seen in `ClusterMessageSizeSpec`) · **Fixed in M5** (17/10/2026) · `EventsBusActor`, `AggregateCommandBusActor`, `ClusterSerializer`, `MessageSize`
- **Symptom:** in a cluster, Pekko drops a message larger than `pekko.remote.artery.advanced.maximum-frame-size` (256 KiB by default) and logs an error; its sender is not told. A re-publication part of 1 000 events with 200-character notes is 470 KiB serialized (docs/perf/load-M5.md), so a projection on another node never receives it: its gap request asks for the same part again, and the aggregate stays behind in that projection for good. A command with many events does the same on its first publication. A `GetEventsForAggregate` answer from an owner on another node (23 573 KiB for 50 000 events) never arrives, and the asker times out. The application's states, commands and answers above the frame are dropped the same way.
- **Root cause:** parts are bounded by events, not bytes; the frame did not matter before M5, when every message stayed in one JVM.
- **Fix:** `MessageSize` gives the limit, the frame (or the large frame when `large-message-destinations` is set) less 8 KiB, and measures values with the serializer Pekko uses for them. The bus sends a subscriber on another node several events in parts of at most half the limit, each event measured once per publication. `GetEventsForAggregate` is answered on the asking node from the store, like the historical reads; the owner no longer answers it. A command of a sharded type over the limit is refused with `FailureResponse` wherever its owner runs. The cluster serializer writes any message over the limit as a marker that the receiving node reads as `Status.Failure(MessageTooLargeException)`, so an asker fails at once, and logs its class and size (at most once per class every 10 s). Hosts that need larger single messages raise the frame (docs/SCALING.md §6).
- **Test:** `ClusterMessageSizeSpec` (two nodes: a subscriber and a projection on the other node receive 1 000 events of about 450 bytes; every event of an aggregate whose owner runs on the other node; a state over the limit fails at once for an asker on the other node; a command over the limit refused through either node; all five red before the fix), `ClusterSerializerSpec` (the marker and the failure's round trip).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F122 — The outbox poll groups the whole outbox and keeps every waiting aggregate in memory
- **Medium**, likely (estimated, not measured) · **Fixed in M5** (17/10/2026, in part) · `PostgresEventStoreState.readWaitingInOutbox`, `OutboxPollerActor`, `RedeliveryPolicy`
- **Symptom:** every 30 s the poller groups all of `events_to_publish` by aggregate and keeps one entry per waiting aggregate, and re-publishes at most 1 000 aggregates per poll. After a long outage of a projection (millions of rows), each poll scans every row, the poller's memory grows with the aggregates, and draining 1 000 000 aggregates takes about 8.3 h (estimated: 1 000 000 / 1 000 × 30 s).
- **Fix:** the poller reads at most `maxTracked` (100 000) waiting aggregates, those waiting longest first, so its memory is bounded; the grouping itself still reads every row (cheap while the outbox is near empty; documented in SCALING §4). Test: `PostgresEventStoreStateSpec` (the oldest first, at most the limit; red when the limit is dropped).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F123 — Given-up aggregates wait for the poller's next start, even when their subscriber is back
- **Medium**, confirmed (by reading `RedeliveryPolicy`) · **Fixed in M5** (17/10/2026) · `RedeliveryPolicy`
- **Symptom:** after its attempts at 3 and 6 minutes, the poller gives up on an aggregate at 10 minutes and does not re-publish it again while it runs (D20b: the rows stay for the next start). A subscriber that stopped answering for longer (an overload, F93; a partition; a long pause) and recovers without a restart keeps missing the events of aggregates that get no new events; those that do are filled by gap requests. In a cluster the poller is a singleton, which starts again only when the oldest node leaves.
- **Fix:** after the report, a given-up aggregate is re-published again every `retryGivenUpEvery` (30 minutes), within the per-poll limit. Test: `RedeliveryPolicySpec` (re-published at 41 and 71 minutes after giving up at 11; red before).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F128 — A re-publication could send a subscription versions with a hole in one message
- **High**, confirmed (by reading `SequenceGate`; reproduced in `EventsBusActorSpec`) · **Fixed in M5** (17/10/2026) · `EventsBusActor.publish`
- **Symptom:** a full re-publication (the outbox's) sends each subscription only the versions whose deliveries wait for it, or have none. A subscription added after an event that still waits for the others (a new build's, D57; a singleton's new instance, M5) was sent, for versions 1 to 3 where version 2 waited for another subscriber only, one message of versions 1 and 3. The sequence gate takes a message as a range: it applied 1 and 3 and recorded 3, so version 2 was never applied in that read model, and nothing reported it.
- **Fix:** each subscription's events are split into runs of consecutive versions before the parts (F121); a run after a hole is a gap the subscriber asks for (D20c). Test: `EventsBusActorSpec` "sends each subscription consecutive versions only" (red before).
- **Found:** M5 close, audit of delivery.

#### F129 — A singleton's proxy was asked once for its new subscribers
- **Medium**, likely (the ask can be lost; not seen) · **Fixed in M5** (17/10/2026) · `EventsBusActor`
- **Symptom:** when a projection's instance stops (its singleton moves), the bus asks the registered proxy again for its subscriptions, once. The proxy drops what its buffer cannot hold while the singleton moves, and the ask can reach the supervisor that is stopping. Lost, the bus never subscribed the new instance: that node's events of its types stayed in the outbox, and its gap requests to owners on that node were re-published to no subscriber, until the node restarted.
- **Fix:** a registered subscriber asked again is asked every tick until it answers, as at the start. Test: `EventsBusActorSpec` "the proxy is asked again every tick until it answers" (red before).
- **Found:** M5 close, audit of the cluster.

#### F130 — Flow control let live traffic starve what it held, and its accounting drifted
- **Medium**, confirmed (by reading; each part reproduced in `FlowControlSpec` or `EventsBusActorSpec`) · **Fixed in M5** (17/10/2026) · `FlowControl`, `EventsBusActor`
- **Symptom:** (1) A subscriber with aggregates behind kept taking live publications of others up to its whole window, while pulls start only at half of it: under steady load the held aggregates waited until the deadline and then for the poller, minutes later. (2) Live publications could take the room that pulls had asked for, so the subscriber held more than its window once the pulled parts came. (3) An aggregate held again was dropped a deadline after its first hold, while what was held later still waited, so its next live event made the subscriber ask for a gap. (4) Events held again (re-published from the outbox while held) counted their bytes again, and a pull counted every byte held as coming in its part, so pulls asked for more room than they brought and fewer ran at once. (5) A held delivery that its pull sent was logged as stalled at once, its wait counted from when it was held.
- **Fix:** while aggregates are behind, a live publication takes at most half the window, so pulls always find the other half; what pulls asked for counts as in flight; an aggregate stays behind a deadline after its latest hold; only versions new to a held range add their share of the bytes, and a part and what it leaves each count their share; a delivery stalls from when it was last sent. Tests: `FlowControlSpec` (five scenarios), `EventsBusActorSpec` "a held delivery stalls from when its pull sends it"; each red under a mutation that restores the old rule.
- **Found:** M5 close, audit of delivery.

#### F133 — Push subscriptions vanished without notice when a projection's singleton moved
- **Medium**, confirmed · **Fixed in M5** (17/10/2026) · `SubscribableProjectionActor.renewSubscription`
- **Symptom:** push subscriptions live in the instance's memory. When the projection's singleton moves (a rolling upgrade, a node that leaves), the new instance does not know them, and a `RenewSubscription` for an id it does not know was ignored: listeners kept renewing and never received another update, and were never told.
- **Fix:** a renewal of an unknown subscription (expired, or held by an instance that stopped) is answered with `ProjectionSubscriptionsCancelled(List(id))`, so the listener subscribes again. Test: `SubscribableSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F141 — Retries of given-up aggregates could take a poll's whole budget
- **Low**, confirmed (by reading; reproduced in `RedeliveryPolicySpec`) · **Fixed in M5** (17/10/2026) · `RedeliveryPolicy.decide`
- **Symptom:** a poll re-publishes at most 1 000 aggregates, the longest waiting first, and since F123 given-up aggregates are retried every 30 minutes. Behind a subscriber stopped for long (its rows never go), 60 000 given-up aggregates or more took every poll's budget, so newer rows' attempts at 3 and 6 minutes waited without end and were never reported as undelivered. Each retry also re-publishes to every subscriber, which acknowledge duplicates, and restores an owner.
- **Fix:** while other aggregates are due, retries take at most half the budget; each kind takes what the other leaves. Test: `RedeliveryPolicySpec` (red before).
- **Found:** M5 close, audit of delivery.

#### F161 — Subscribable projections push before the listener's transaction commits, and one failing filter stops an update
- **Medium**, confirmed (by reading) · **Fix in M7** · `SubscribableProjectionActor.sendUpdate`
- **Symptom:** `sendUpdate` sends at once, so a batch that rolls back has pushed a change never stored (and pushes it again when retried); one `try` wraps the loop over subscriptions, so a filter that throws keeps the update from the rest; a reused subscription id gives the newest listener every update twice and the older none.
- **Found:** writing the book (M6, 17/10/2026).

#### F162 — The event bus ignores a late `RegisterSubscribers`, and waits for a subscriber that answered with a failure
- **Low**, confirmed (by reading) · **Fix in M7** · `EventsBusActor`
- **Symptom:** a `RegisterSubscribers` sent after start-up is unhandled, without a log line; a `Status.Failure` from a subscriber during start-up is stashed, so the bus keeps waiting for its subscriptions.
- **Found:** writing the book (M6, 17/10/2026).

#### F163 — Delayed queries with a `Future` can miss the update they wait for
- **Low**, confirmed (by reading) · **Fix in M7** · `ProjectionActor` (`delayIfNotAvailableAsync`, `...AsyncCustom`)
- **Symptom:** such a query is parked only once its `Future` answered; an update applied meanwhile asks for no replay, and at the deadline the answer is `None` without a last search. The custom variant answers `None` at its deadline, where it answers a `T` on success.
- **Found:** writing the book (M6, 17/10/2026).

#### F164 — A second listener of one kind for one type silently replaces the first
- **Low**, confirmed (by reading) · **Fix in M7** (refused at start) · `ProjectionActor` (the listener maps)
- **Found:** writing the book (M6, 17/10/2026).

#### F169 — `OutboxStatus.waiting` can count fewer aggregates than wait
- **Low**, confirmed (by reading) · **Fix in M7** · `OutboxPollerActor`, `RedeliveryPolicy`
- **Symptom:** it is the poller's tracked aggregates; new ones postponed past `maxRepublishPerPoll` are not tracked yet.
- **Found:** writing the book (M6, 17/10/2026).

#### F166 — A permanent delete held by a failed subscription runs only at the next re-publication
- **Low**, confirmed (by reading) · **Fix in M7** · `AggregateCommandBusActor.deletePermanently`
- **Symptom:** once the failure is retried or skipped, the delete happens when the poller next re-publishes its event: every 30 minutes after the 10-minute report.
- **Found:** writing the book (M6, 17/10/2026).

### Commands and aggregates

#### F7 — Event timestamps differ between live delivery and the store
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`, `PostgresEventStoreState`
- **Symptom:** state and projections that use event timestamps (Neula: process flows, reports, functions) differ after restore, re-send or replay; the difference is the persist queue wait.
- **Root cause:** the live path uses the executor's `Instant` (`CommandExecutorActor.scala:198`); SQL stores a later `Instant.now` (`PostgresEventStoreState.scala:72, 85, 99, 112, 172`).
- **Test:** `EventStoreStateSpec` "Events keep the timestamp they were persisted with" (single and batched writes; failed on PostgreSQL before the fix). An end-to-end check through the actors (Neula R6) is still to be written with the projection work in M2.
- **Fix:** D10. The executor takes one timestamp per command, truncated to microseconds (PostgreSQL's precision); the store writes that timestamp instead of its own `Instant.now`. Follow-up: F65.
- **Invariants:** I6.

#### F8 — History rewrite is half-applied
- **Medium**, confirmed · **Contract part fixed in M3** (16/10/2026), complete rewrite in **M6** (D76) · `PostgresEventStoreState.overwriteEvents`, `AggregateOwner`
- **Symptom:** a rewrite to a different class corrupts history; the live root keeps old values; projections are not told; duplicates and snapshots keep old values (GDPR).
- **Root cause:** `overwriteEvents` updates only the JSON (`PostgresEventStoreState.scala:180-186`); no rebuild after rewrite (`AggregateRepositoryActor.scala:128-134`).
- **Fix (M3, D17):** the store refuses a rewritten event of another class than the stored one (`RewriteChangesClassException`; the command gets `EventHandlingError` and nothing is stored); the owner reads its state again after a rewrite; the snapshots of the aggregate and of its duplicates are deleted. Still open for M6 (D76): projections are told only through a compensating event, and the live owners of duplicates keep their state until they are reloaded. Tests: `HistoryRewriteSpec`, `EventStoreStateSpec` (both stores).

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
- **Low**, confirmed · **Fixed in M3** (16/10/2026) · `AggregateOwner.persist`, `nextRoot`
- **Fix:** the version is checked before the handlers run on a copy of the state; after the commit that copy becomes the owner's state (D34). Test: `AggregateOwnerSpec` "run once per persisted event" (red under a mutation that applies the events again after the commit).

#### F44 — No snapshots: every restore replays the full history
- **Medium**, likely · **Fixed in M3** (16/10/2026) · `AggregateRestorer`, `AggregateOwner`, event store
- **Fix:** D30, opt-in per context (`SnapshotPolicy`). Tests: `SnapshotSpec` (a property test over random histories with undo and duplication, red under two mutations; R10 with 100 000 events; the fallbacks; invalidation), `EventStoreStateSpec` for both stores.

#### F46 — Command-bus child cache reap sorts the whole map
- **Low**, likely · **Fixed in M5 for sharded types** (17/10/2026; replaced by passivation, D43) · `LocalOwners.sweep`
- **As fixed:** the owners of a `Sharded` type are passivated by their shards (`pekko.cluster.sharding.passivation`); the bus keeps no children for them and runs no sweep. A `Local` type keeps the sweep, for ephemeral aggregates (M6) and gradual moves. Test: `ShardedOwnersSpec` (an idle owner is passivated by its shard and comes back with its state).

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
- **Low**, confirmed · **Fixed in M3** (16/10/2026, with the owner) · `AggregateOwner` (min-version queries)
- **Symptom:** a `GetAggregateMinVersion` whose version is not reached within its time gets no answer; the caller only sees its own ask time out and cannot tell "not yet" from "lost".
- **Root cause:** expired queries are filtered out of `delayedQueries` without answering them, and only when another query or event arrives.
- **Found:** plan review at the M1 close.
- **Fix:** a timer for the nearest deadline answers expired queries with `Failure(AggregateInIncorrectVersionException)`; a stopping owner answers its waiting queries the same way. Test: `AggregateOwnerSpec` "is answered with an error when its time is up" (red with the timer removed).

#### F119 — A stopping owner could write a snapshot of a history that changed under it
- **Medium**, likely · **Fixed in M5** (17/10/2026) · `AggregateOwner.writeSnapshot`, `EventStoreState.writeSnapshot`
- **Symptom:** after a rewrite of a base, or a squash or detach that may change the state, the owner of a duplicate that was stopping wrote its state of before as its passivation snapshot, after the change had deleted the snapshots; later restores started from that state. So could another node's owner of a `Local` aggregate after an undo. With sharded owners (D43) the window grows: a stale stop that reaches a passivating owner is kept by its shard for the next owner.
- **Root cause:** a snapshot write was checked only against the aggregate's version, which these changes leave as it was.
- **Fix:** `aggregates.history_generation`, raised in the transaction of every change that deletes snapshots because their state may be wrong (an undo, a rewrite of the aggregate or of its base, a squash or a detach that may change the state); the restore reads it before the history, and a snapshot is written only while the generation is the one its state was read at. A state-preserving squash raises nothing.
- **Test:** `EventStoreStateSpec` "A snapshot of a state read before an undo or a rewrite is not written, also a duplicate's" (both stores; failed without the check in either store, and with a rewrite or an undo that does not raise the generation).
- **Found:** M5 design of sharded owners.

#### F120 — A first command racing another owner on a chosen id was answered with an error
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `AggregateOwner.movedOnWithoutThisOwner`
- **Symptom:** two owners of one aggregate at once (nodes of a `Local` type, or a hand-over, D43) creating it under a chosen id: the one that lost got `EventHandlingError`, also for an idempotent command whose copy had won.
- **Root cause:** both found the aggregate missing and inserted it; the loser's insert broke the primary key of `aggregates`, which the owner did not take for a concurrent modification.
- **Fix:** a unique-key violation on `aggregates` (its only unique key; matched by table, as a database made by an older version may name the key otherwise) counts as a concurrent modification: the owner reads the aggregate again, and the caller gets the stored response of an idempotent command, or the conflict.
- **Test:** `ChosenIdRaceSpec` (both owners pass the existence check before either inserts, as a table lock holds them until both wait; `EventHandlingError` before the fix).
- **Found:** plan review at the M1 close (refinement of M5 item 3).

#### F131 — Owners of a `Local` type on several nodes answered from states the others had moved past
- **High** for clusters of several nodes, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M5** (17/10/2026) · `AggregateOwner.caughtUp`, `Members`, `EventStoreState.storedVersion`
- **Symptom:** `Local` is the default placement, and every node that serves a type has an owner of an aggregate it touched. A running owner read the store again only for a command that expected a newer version (I1 assumed one writer). So after node B wrote version 6, node A's owner answered `GetAggregate` with version 5 for as long as it ran (with 200 owners or fewer, it is never stopped as idle); `GetAggregateMinVersion` for version 6 through A waited and failed; the poller's `RepublishOutbox` and a projection's gap request through A re-published nothing past version 5, and the gap became a recorded failure after its retries, which blocks the aggregate's squash and permanent delete.
- **Fix:** an owner of a `Local` type on a node that is not alone (`Members.alone`: not a cluster, or its only member) reads the aggregate's stored version and history generation (one key lookup) before it answers the state, a min-version query not reached (and again at the query's deadline), a re-publication and a squash, and restores the aggregate when they differ from its own. Commands were left to the store's version check (F19), which misses changes that keep the version: they catch up too since F145. A sharded owner is the only one and never asks; changing a type's placement or `numberOfShards` makes two owners per aggregate while old and new nodes run together, so it needs a full stop (H10).
- **Test:** `LocalOwnersSpec` (two nodes: a read, a re-publication, a min-version query waiting and one answered at once; all four red without the check), `MembersSpec` (whether the node is alone).
- **Found:** M5 close, audit of the cluster.

#### F138 — The write switch allowed every new event class before the node knew its cluster
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `Members` (was `AppVersions`)
- **Symptom:** the lowest app-version started as none, and an empty member list gave none too, which means "write every class". A new node that took a command for a `Local` type before it had joined (the readiness check usually keeps traffic away) could write a class that older members cannot read (D67).
- **Fix:** in a cluster the lowest version is Pekko's 0.0.0 until the node knows a member, and again if it knows none. Test: `MembersSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F134 — A permanent delete detached duplicates with a squash event that older nodes cannot read
- **Low**, confirmed (reproduced in `PermanentDeleteSpec`) · **Fixed in M5** (17/10/2026) · `AggregateCommandBusActor.deletePermanently`
- **Symptom:** a squash checks its detach events against the write switch (D67); a permanent delete, which detaches the duplicates with the same squash events (F76), did not. A squash event class added in a version was written into the duplicates during the rollout, and owners on nodes of the older version could no longer restore them.
- **Fix:** the detach events are checked like a squash's; while a node cannot read their class, the delete waits as for a failed subscription (its event stays in the outbox and is tried again when delivered again). Test: `PermanentDeleteSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F143 — A permanent delete detached duplicates whose first event subscribers did not have yet
- **Low**, confirmed (by reading; reproduced in `EventStoreStateSpec`) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState`, `MemoryEventStoreState` (`detachBlocker`)
- **Symptom:** a squash waits while a duplicate it detaches still has its first event in the outbox or a failed subscription (D51): subscribers must have the original first event before the detach replaces it. A permanent delete detached the same way without those two checks, so a subscriber could get the detach's squash event as the duplicate's first event instead of the duplication event.
- **Fix:** one check for both, which the delete answers as "not deleted yet" (its event stays in the outbox and is tried again when delivered again). The outbox rows this node has acknowledged are deleted before either checks (they were deleted in batches up to 20 ms later), so a delete right after a duplicate's first event was acknowledged is not refused for it. Test: `EventStoreStateSpec` (both stores; red before); `PermanentDeleteSpec` caught the batching race.
- **Found:** M6 survey.

#### F144 — Owners of contexts without snapshots never matched the stored history generation after an undo
- **Medium**, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M6** (17/10/2026; introduced by F131 the same day) · `AggregateRestorer.restore`
- **Symptom:** the restorer read the history generation only for contexts with snapshots, so an owner of any other context held generation 0, while an undo, a rewrite or a squash that may change the state raises the stored one. Since F131 a `Local` owner on a node that is not alone compares both before it answers, so after such a change it never matched again and restored the whole aggregate before every read (three reads, three restores).
- **Fix:** the restorer reads the generation for every context (one key lookup per restore). Test: `LocalOwnersSpec` (one restore for three reads after an undo on the other node; three under the old rule).
- **Found:** M6 survey.

#### F145 — Commands on another node's `Local` owner ran on a state changed without a new version
- **Medium**, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M6** (17/10/2026) · `AggregateOwner.attempt`
- **Symptom:** F131 made `Local` owners catch up before reads only. A change that keeps an aggregate's version (a rewrite of its base, a squash that may change its state, a rewrite without a new event) left another node's owner with its state of before, and a command there ran on it and committed: a duplicate whose base was renamed took a command on the old name.
- **Fix:** such an owner catches up before a command too (one key lookup per command, on nodes that are not alone). Test: `LocalOwnersSpec` (red before).
- **Found:** M6 survey.

#### F146 — A history rewrite without a new event was answered with success and not stored
- **Medium**, confirmed (by reading) · **Fix in M6** (with the complete rewrite, D76) · `AggregateOwner.persistOrAnswer`
- **Symptom:** a command that gives no events writes nothing (R9), and the owner applied that rule to rewrites too: a `RewriteCommandSuccess` with rewritten events and no new event was answered `SuccessResponse` and dropped. D17 recommends a compensating event, but nothing requires one, and a rewrite for R15 has none to add.
- **Found:** M6 survey.

#### F147 — A duplication at a version its base has not reached was stored, and the copy then read the base's later events
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `EventStoreStateSpec` "F147", both stores) · `add_duplication_event`
- **Symptom:** `add_duplication_event` checks that the base exists and that the version is not below its squash point, but not that the base has reached it. A copy made "at version 10" of a base at version 5 reads the base's versions 6 to 10 as they are stored later: the copy's state changes without an event of its own, and its subscribers are never told.
- **Fix:** the procedure refuses a version above the base's current one, under the lock it already takes.
- **Found:** writing the book (M6, 17/10/2026).

#### F148 — Event handlers must handle undo events, although the owner reads the state again after an undo
- **Low**, confirmed (by reading; the test domain's TODO) · **Fix in M7** · `AggregateOwner.nextRoot`
- **Symptom:** the owner applies every new event to a copy of the state before the commit, undo events included, so a context without a case for its undo event fails the command with `EventHandlingError`; restores skip undo events, and the state after an undo is read from the store.
- **Found:** writing the book (M6, 17/10/2026).

#### F149 — A refused first event without a space answered with the decision's number
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `AggregateOwner.persistOrAnswer`
- **Symptom:** the `FailureResponse` text, which users see, named the decision "(D38)".
- **Found:** writing the book (M6, 17/10/2026).

#### F154 — Squash and detach events were stored without the event size check
- **Low**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresEventStoreStateSpec` "F154") · `PostgresEventStoreState.squash`, `detachDuplicate`, `overwriteEvents`
- **Symptom:** the three wrote their events with `mpjsons.serialize` rather than `serializedChecked`, so a large state gave a squash event above `eventSizeLimit` (and its per-class limits), and a rewrite could grow an event past it.
- **Fix:** all three are checked; the transaction then fails and nothing changes (a squash is answered `SquashRefused`).
- **Found:** writing the book (M6, 17/10/2026).

#### F155 — An automatic squash checks its event as `UserId(0)`, so a state that records its last user never passes
- **Medium**, confirmed (by reading) · **Decide at review 8** (M6 close) · `AggregateRestorer.squashEvent`, `SquashScheduler`
- **Symptom:** D85's check applies the squash event as the request's user, as a restore will; the scheduler asks as `UserId(0)`. A context whose event handlers copy the user into the state ("last changed by") gets every automatic squash refused, and a manual one passes only when the right person asks. Options: the squash event takes the user of the event it replaces (keeping the squasher in its metadata), or the check compares states without such fields (the context says which).
- **Found:** writing the book (M6, 17/10/2026).

#### F157 — Dead API: `OnConcurrentModification` and `SimulateEvent`
- **Low**, confirmed · **Fix in M7** (API clean-up) · `api/.../command/OnConcurrentModification.scala`, `AggregateContext.scala`
- **Symptom:** `OnConcurrentModification` (`Retry`/`Fail`) is used nowhere: the kind of command alone decides what a clash does. `SimulateEvent` is declared, and the command bus answers it as an unknown message. `NoEventsForAggregateException` is never thrown (it has only a serializer binding).
- **Found:** writing the book (M6, 17/10/2026).

#### F165 — `io.reactivecqrs.api.Nothing` hides `scala.Nothing` after `import io.reactivecqrs.api._`
- **Low**, confirmed · **Fix in M7** (API clean-up, a rename) · `api/.../CustomCommandResponse.scala`
- **Found:** writing the book (M6, 17/10/2026).

### Event store and SQL

#### F22 — Restore loads the whole event stream into memory before handling it
- **Medium**, likely · **Fixed in M3** (16/10/2026; heap measured in `baseline-M3`) · `PostgresEventStoreState.readAndProcessEvents`
- **Root cause:** `DB.readOnly` keeps autocommit on and no `fetchSize` is set (`PostgresEventStoreState.scala:202`), so the driver buffers every row (up to 500 000 in Neula).
- **Fix:** restores read in a transaction (autocommit off) with a fetch size, so the driver streams the rows (D34); snapshots (D30) shorten the history read.

#### F31 — `add_events` loops `add_event`; `add_undo_event` scans and sorts the whole history
- **Low**, likely · **Fixed in M3** (16/10/2026; statements measured in `baseline-M3`) · `PostgresEventStoreSchemaInitializer` (`add_events`, `add_undo_event`)
- **Fix:** `add_events` updates the aggregate's row once for the whole command and inserts the events in a loop inside the function; plain and first events always go through it, one call per command. `add_undo_event` walks back the index on `(aggregate_id, version)` with `LIMIT` instead of sorting the history (D34).

#### F38 — Restore query order has no matching index; one index is redundant
- **Low**, likely · **Fixed in M3** (16/10/2026), the drop later · `PostgresEventStoreState.buildIndexesConcurrently`
- **Fix:** index `events(aggregate_id, version)` built `CONCURRENTLY` by `buildIndexesConcurrently()`, which the host runs once in the background after the upgrade (H8); an index an interrupted build left invalid is built again. Drop `events(aggregate_id)` in a later contract step (D34, D69). Test: `PostgresEventStoreStateSpec`.

#### F65 — Event times are stored without a time zone
- **Low**, confirmed · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState` (`utc`, `utcTime`)
- **Symptom:** on a JVM whose default zone is not UTC, event times can shift by an hour around a daylight-saving change, because `event_time` is `TIMESTAMP` (without time zone) and is converted with the JVM default zone on write and read. Neula forces UTC (`Startup.scala:16`), so it is not affected today.
- **Fix:** `event_time` is bound and read with an explicit UTC calendar; the column type stays (D28). Neula runs in UTC, so its stored values read back unchanged. Promised as a separate entry by D10. Test: `PostgresEventStoreStateSpec` "event times are stored as UTC" (red under a mutation binding with the JVM's zone, on a machine in Central European time).

#### F39 — Instant reads filter in Scala
- **Low**, confirmed (1 call site in Neula) · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState.readAndProcessEvents`
- **Also wrong:** a read at an instant used today's undo flags, so an event undone after that instant showed as undone.
- **Fix:** the version at the instant is found in SQL, then the read runs as for that version, undo flags included. Test: `PostgresEventStoreStateSpec` "a read at an instant".

#### F49 — Legacy `aggregates` rows have `space_id = -1`
- **Medium**, suspected (count unknown) · Pre-check in **M1** (`StoredDataCheck` reports it), **backfill built in M4** (17/10/2026: `PostgresEventStoreState.backfillSpaces(spaceOf)` gives each such aggregate the space of its first event, or the host's function's for a first event that does not know it, online in batches; `ProjectionsSpec`. Neula runs it once, H9) · schema
- **Root cause:** the column was added with `DEFAULT -1` (`PostgresEventStoreSchemaInitializer.scala:117-121`); six deprecated Neula events use `SpaceId.unknown`.
- **Check:** `SELECT count(*) FROM aggregates WHERE space_id = -1` on production.
- **Fix:** D38.

#### F76 — Permanently deleting an aggregate that duplicates read from breaks the duplicates
- **High** if Neula deletes such aggregates, suspected · **Fixed in M3** (16/10/2026, with the duplicate detach of D50) · `PostgresEventStoreState.deletePublishedEventsToPublish` (`PermanentDeleteEvent` branch)
- **Symptom:** after a `PermanentDeleteEvent` on an aggregate that has duplicates, those duplicates restore without the history they share with it, so their state is silently wrong; their chain rows in `aggregates` point at a base that no longer exists.
- **Root cause:** the delete removes the base's events and rows only; duplicates read the base's events through their chain rows (D50 solves the same problem for squash).
- **Found:** plan review at the M1 close.
- **Check:** Neula, on a production copy: `SELECT count(*) FROM aggregates a WHERE a.base_id <> a.id AND NOT EXISTS (SELECT 1 FROM aggregates b WHERE b.id = a.base_id AND b.base_id = b.id)` (duplicates whose base is gone).
- **Fix:** detach every duplicate before deleting its base, as D50 does before a squash. With D20a the delete also needs a precondition like D51 (no failed subscriptions), because it removes the `subscriptions` rows that would hold the failure. The precondition is in since M2 (16/10/2026): the delete waits while the aggregate has a row in `subscription_failures` (`PermanentDeleteSpec`). Since M3 the command bus restores each duplicate at version 1 and the delete, in its transaction, makes that state the duplicate's first event (the type's squash event) and removes its chain rows, under the chain lock of F96; a duplicate made meanwhile postpones the delete. A type without a squash event cannot detach: its aggregate with duplicates is not deleted, with an error log, and the event stays in the outbox (H8).
- **Tests:** `PermanentDeleteSpec` (the duplicate keeps its state; without a squash event the delete waits; failed against the old delete), `EventStoreStateSpec` (both stores).

#### F96 — A duplication could read history a squash removes
- **High** once squash is used, found in the M3 squash design review (16/10/2026) · **Fixed in M3** · `add_duplication_event`, `PostgresEventStoreState.squash`/`deleteAggregatePermanently`
- **Symptom:** a duplication of an aggregate at a version below its squash point would read events that no longer exist, so the duplicate's state is silently wrong; a duplication committed between the owner listing the duplicates to detach and the squash's commit would not be detached; the same for a permanent delete (F76).
- **Fix:** `add_duplication_event` locks the base's chain rows `FOR KEY SHARE` in key order and then refuses a missing base or a version below its `squashed_version`; a squash or permanent delete locks the aggregate's chain and its duplicates' rows to it `FOR UPDATE` in the same order, then checks its preconditions again and compares the duplicates with those it was given (else it changes nothing, "changed meanwhile"). Whichever runs first, the other sees its result. The in-memory store mirrors the checks.
- **Tests:** `PostgresEventStoreStateSpec` (a duplication during a squash waits and is refused; a squash waiting for a duplication sees it and changes nothing), `EventStoreStateSpec`, `SquashSpec`; both failed against the code without the locks and checks.

F100–F107 were found by the read-only audit at the M3 close (review 5, 16/10/2026) and fixed then; each test below failed against the code before the fix.

#### F100 — Owners wrote snapshots of states the store no longer had
- **High**, confirmed · **Fixed in M3** · `AggregateOwner.stopNow`, `AggregateCommandBusActor.stopOwner`, `writeSnapshot`
- **Symptom:** an owner stopped after its aggregate was deleted for good wrote a snapshot of it, so the erased state stayed in `snapshots`; owners of duplicates whose history changed under them (a detach with a state-changing squash event, a rewrite of their base) wrote their old states back as snapshots, which later restores read.
- **Fix:** the command bus stops such owners with `StopOwnerStale`, which writes no snapshot (also when the owner was stopping already); after a rewrite or a state-changing squash the owner names the duplicates whose histories changed (`HistoryChanged`); a snapshot is written only while the aggregate is stored at that version.
- **Tests:** `PermanentDeleteSpec` (no snapshot after a delete), `SquashSpec`, `HistoryRewriteSpec` (a duplicate's live owner shows its base's rewritten history).

#### F101 — A squash that changed the state on purpose left the duplicates still reading the aggregate stale
- **Medium**, confirmed · **Fixed in M3** · `PostgresEventStoreState.squash`, `AggregateOwner.squashHistory` (and the in-memory store)
- **Symptom:** after a `SquashMayChangeState` squash, duplicates that read the aggregate above the squash point read the new squash event, so their states changed, but their snapshots and running owners kept the old ones.
- **Fix:** such a squash deletes the snapshots of every aggregate still reading the squashed one and has the owners of all its duplicates stopped. Test: `SquashSpec`.

#### F102 — A read at an instant before the squash point answered "did not exist"
- **Medium**, confirmed · **Fixed in M3** · `PostgresEventStoreState.processEvents`
- **Symptom:** no event is left before the squash point, so an instant before the squash event's time resolved to version 0 and the read answered an empty aggregate; also, a squash committed between the read's check and its query left the read incomplete.
- **Fix:** the squash point is checked after the read, in its transaction, and an instant before the squash event's time fails with `AggregateSquashedException` (now also naming the instant). Test: `PostgresEventStoreStateSpec`.

#### F103 — A history rewrite of a duplicate reached its base's events
- **Medium**, confirmed (from before 1.0) · **Fixed in M3** · `AggregateRestorer.eventsOfClasses`
- **Symptom:** the rewrite handler got the base's events of the classes asked for, with the base's versions, and the store applied them to the duplicate's own rows: the rewrite failed on the class check, or, with the same class, replaced the duplicate's own event.
- **Fix:** only the aggregate's own events are given to the handler. Test: `HistoryRewriteSpec`.

#### F104 — A squash or delete could deadlock with a duplication of a duplicate
- **Low** (PostgreSQL detected it and aborted one side), confirmed · **Fixed in M3** · `PostgresEventStoreState.lockChain`
- **Symptom:** the squash locked only the rows that point to the aggregate, then deleted the detached duplicate's other chain rows, which a duplication of that duplicate could hold while it waited for a row the squash held.
- **Fix:** one ordered statement locks every chain row of the aggregate and of each aggregate reading it. Test: `PostgresEventStoreStateSpec` (the two run side by side; deadlocked before).

#### F105 — Detaching a duplicate whose first event was undone changed its state
- **Low**, confirmed · **Fixed in M3** · `squashBlocker`, `deleteAggregatePermanently` (both stores)
- **Symptom:** the detach replaced the undone first event with a squash event that was no longer undone, so the duplicate got back what its undo had taken; the in-memory store kept it undone, so the stores differed.
- **Fix:** a squash is refused, and a permanent delete waits, while a duplicate to detach has its first event undone. Test: `EventStoreStateSpec`.

#### F106 — The in-memory store checked a command's undos against the history before the command
- **Low**, confirmed · **Fixed in M3** · `MemoryEventStoreState.persistEvents`
- **Symptom:** after a squash, an undo of an event stored by the same command was refused, and a second undo that reached the squash event was accepted; PostgreSQL does the opposite, as its procedures store the events one after another.
- **Fix:** each event is checked against the events before it, the command's included, before anything changes. Test: `EventStoreStateSpec`.

#### F107 — A failed pool fetched ahead failed commands that had ids at hand
- **Low**, confirmed · **Fixed in M3** · `AggregateCommandBusActor.idsNotReceived`
- **Symptom:** when the next pool of one kind of ids did not come, every waiting command was answered with an error, also while ids of that kind were still at hand (they waited for the other kind).
- **Fix:** a failed pool fails only the commands that need its kind of ids while none is left. Test: `CommandOutcomesSpec`.

#### F125 — The per-aggregate limit lets one event too many through
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `PostgresEventStoreState.serializedChecked`
- **Symptom:** `aggregateVersionLimit = 10 000` let an aggregate reach version 10 001: the check compared the version before the event with the limit, not the event's own.
- **Fix:** an event is refused when the version before it has reached the limit. Test: `PostgresEventStoreStateSpec` (events stored together, one by one and as an undo; red before the fix).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F142 — An event's size was its characters when written and its bytes when read
- **Low**, confirmed (reproduced in `PostgresEventStoreStateSpec`) · **Fixed in M5** (17/10/2026) · `PostgresEventStoreState`
- **Symptom:** the store reported a written event's size as the length of its JSON string (UTF-16 characters), and a read event's as PostgreSQL's `octet_length` (UTF-8 bytes). For text outside ASCII, such as Polish, they differ (161 bytes against 111 characters for 50 ASCII and 50 Polish letters), so the bus's byte window and the stored-bytes metric counted the same event differently on the live path and in re-publications.
- **Fix:** a written event's size is its UTF-8 bytes, counted without encoding the string. The per-class size limits stay in characters. Test: `PostgresEventStoreStateSpec` (sizes of Polish text, red before; the count against `getBytes`).
- **Found:** M5 close, audit of delivery.

#### F168 — `EventTooLargeException` spoke of bytes for a limit in characters
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `api/.../Exceptions.scala`
- **Symptom:** the check compares the JSON's length in characters (`PostgresEventStoreState.serializedChecked`); the message says bytes.
- **Found:** writing the book (M6, 17/10/2026).

#### F167 — A permanent delete left the aggregate's stored command responses to expire
- **Low**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresEventStoreStateSpec` "F167") · `PostgresEventStoreState.deletingPermanently`
- **Symptom:** since M6 a stored response names its aggregate (R17), and a purge deletes the responses of the aggregates it removes; a permanent delete still left them for the retention (30 days by default), with the aggregate's data in the response's info.
- **Found:** writing the book (M6, 17/10/2026).

### Performance and runtime

#### F172 — A builder's page of events read every event of the store once the tables were analyzed
- **High** (performance at scale), confirmed (measured) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState.readCommittedPage`, `pageOf`, `backfillEventKeys`
- **Symptom:** in the short JSON keys bench (IMPLEMENTATION-PLAN §7.1), a managed rebuild over 1 000 000 events ran at about 4 400 events/s, then at about 2 500 once PostgreSQL's autoanalyze had run on the tables. `EXPLAIN ANALYZE` of one page over 2 000 000 events of 2 000 aggregates: 786 ms, 643 ms of them reading all 2 000 000 events through `events (aggregate_id)`. Every page did so, so a rebuild's reading grew with the store's size times its pages.
- **Root cause:** the page joined each event to its aggregate's own row with `a.id = e.aggregate_id AND a.base_id = e.aggregate_id`. The planner takes the implied `id = base_id` at its default selectivity of 0.5 % (it holds for nearly every row), so it expected 10 aggregates and drove the join from `aggregates`, reading all their events, instead of looking up the page's 1 000 events and their rows.
- **Fix:** each event's own row is looked up by its key in a `LATERAL` subquery, whose `LIMIT 1` keeps the planner from flattening it into such a join: 5 ms for the same page. The same for the page of named aggregates, the walk before the event keys exist (43 ms → 0.6 ms on the same data) and the event-key backfill (F124).
- **Verified:** `EXPLAIN ANALYZE` on the bench's data before and after; the specs pass unchanged (the plans changed, not the rows); the rebuild of `docs/perf/jsonnames-M6.md` ran with the fix.
- **Found:** the short JSON keys bench (M6, 17/10/2026).

#### F173 — A page of a type's aggregates costs about 80 ms at a million aggregates of the type
- **Low** (performance at scale), measured · **Fix in M7** · `PostgresEventStoreState.aggregatesOf`
- **Symptom:** the builder's final-state pass (D62), its reconciliation and the targeted rebuild of a type walk the type's aggregates 1 000 at a time. With 1 000 000 aggregates of one type, a page in the middle took 70 to 100 ms (`EXPLAIN ANALYZE`, PostgreSQL 17.4): misjudging `id = base_id` at 0.5 % again, the planner scans `aggregates (base_id)` in parallel, and leaves an added index on `(type_id, id)` unused. About 80 s for the whole walk (estimated from the page).
- **Fix idea:** a partial index `ON aggregates (type_id, id) WHERE id = base_id` (built concurrently on a large table, D69), which gives the planner both the order and the right count.
- **Found:** checking the queries like F172's (M6, 17/10/2026).

#### F127 — Command handlers ran on Pekko's default dispatcher
- **Medium**, confirmed (measured) · **Fixed in M5** (17/10/2026) · `AggregateOwner` (`handlersContext`)
- **Symptom:** a command handler that holds its thread for long (computing, or waiting for I/O) held one of the default dispatcher's threads, which the command buses, the event bus, projections and sagas run on too. Measured (docs/perf/heavy-before-F127.md): 16 handlers of 5 s on 12 threads held a light command on another aggregate for 4.9 s; handlers of minutes would stall the node's delivery for minutes.
- **Fix:** handlers run on `reactivecqrs.handler-dispatcher` (32 threads). Measured after (docs/perf/heavy-M5.md): with 36 such handlers the event bus answered in 8 ms; commands still wait while every handler thread is held, so heavy work belongs on the application's own threads (`AsyncCommandResult`) or out of the command (docs/SCALING.md §8). Test: `HandlerDispatcherSpec` (red under the default dispatcher).
- **Found:** analysis of command handlers that take minutes at M5 (docs/SCALING.md §8).

#### F126 — Batches counted in events hold whatever their events weigh
- **Medium**, confirmed (measured with 5 MB events; the larger batches estimated) · **Fixed in M5** (17/10/2026) · `AggregateOwner.republish`, `ProjectionBuilder`, `PostgresEventStoreState` (`fetchSize`), `EventsBusActor`
- **Symptom:** an event store that allows large events (`eventSizeLimitPerClassName`) keeps every batch counted in events: a re-publication part of 1 000 events, a rebuild page and chunk of 1 000 events, a fetch of 1 000 rows, and the window F93 plans in events. With events of 5 MB a part is about 5 GB, more than any heap. Measured (docs/perf/large-M5.md): a restore of 20 such events took 206 MB of heap, about 10 MB per event.
- **Fix:** the store reports each event's stored size when it writes it (`persist`) and reads it (`readPart`, `readCommittedPage`), cutting a range by the sizes in SQL (`octet_length`, without detoasting); re-publication parts are at most 1 000 events and 16 MB, rebuild pages 64 MB and chunks 16 MB (a page its bytes cut short is not the last), the reconciliation reads an aggregate's missing range in parts, and the bus's window counts bytes (F93). Restores still fetch 1 000 rows at a time: hosts with large events lower `fetchSize` or set the driver's `adaptiveFetch` (SCALING §7). Tests: `PostgresEventStoreStateSpec`, `ProjectionBuilderSpec` (a page cut short did not end the bulk pass: red under the old `readAll`), `EventsBusActorSpec`.
- **Found:** analysis of commands and events of 5 MB at M5 (docs/SCALING.md §7).

#### F23 — Id pools of 100 and a blocking wait on refill
- **Medium**, likely · **Fixed in M3** (16/10/2026) · `UidGeneratorSchemaInitializer.scala:18-28`, `AggregateCommandBusActor.scala:315-348`, `SagaActor.scala:136-152`
- **Fix:** pools of 10 000 (`ALTER SEQUENCE … INCREMENT BY`), asynchronous prefetch at a low-water mark (D34).
- **Note (16/10/2026):** since F40 a failing id store fails the `Await` at once instead of after 60 s; F75 makes the bus and the saga actor answer that failure instead of restarting. The `Await` itself (the bus blocks while a pool is fetched) goes with the asynchronous prefetch.
- **As fixed:** the sequences step by 10 000 (an older one is changed at start; the next pool starts past every pool handed out, so no id is given twice, also with a node still on the old size). The command bus and the saga actor keep their ids in an `IdPool`, which asks for the next pool when a fifth is left; a command or order that arrives while none is at hand waits, with the ones after it, and the actor keeps answering other messages; a pool that does not come fails the waiting ones (F75) and is asked for again with the next. Tests: `IdPoolSpec`; `CommandOutcomesSpec` (a query is answered while a command waits for a slow pool; failed against the old bus), the F75 scenarios.

#### F99 — Contended concurrent commands wait a back-off even for conflicts their owner sees itself
- **Low** (serial mode is the remedy, R8), confirmed (measured at the M3 close, 16/10/2026) · **Fixed in M5** (17/10/2026) · `AggregateOwner.ownConflict`, `runContended`
- **Symptom:** 16 callers sending `ConcurrentCommand`s at once to one aggregate: 738 commands/s, p99 380 ms (`baseline-M3.md`), against 1 166 commands/s and p99 197 ms before the owner (`baseline-M2.md`). The same load in serial mode runs at 2 501 commands/s, p99 11 ms.
- **Root cause:** every conflict waits D37's jittered back-off (10 ms doubling) before the command runs again, also when the owner itself saw it (another command of the same owner committed first), where no other writer is involved and waiting spreads nothing.
- **Fix idea:** a command that conflicts with a commit of its own owner runs again as soon as that commit is done, one at a time (serial after a first conflict); conflicts found by the store (another writer) keep the back-off. Measure with the bench's contended scenario; the attempt limit must not fail commands that only waited their turn.
- **As fixed:** a concurrent command whose handler ran on a version that a commit of its own owner has since passed waits its turn in a queue and runs again on the newer state, one at a time, oldest first; while any wait, new concurrent commands wait behind them, so a contended owner runs serially until its queue is empty. Waiting is not an attempt. Conflicts found by the store keep D37's back-off, and a command back from it waits its turn too. Test: `ContendedCommandsSpec` (40 commands at once to one aggregate: all succeed, at most 80 handler runs; 233 with the back-off). `SerialCommandsSpec`'s comparison now finds no conflict in concurrent mode under 1 000 commands at once. Measured (single runs, this machine, 16 callers, 800 commands): 887 commands/s, p99 36.4 ms, max 48 ms, against M4's 1 133/s, p99 339 ms, max 613 ms; serial mode in the same run 1 646/s, p99 16.9 ms (M4: 2 384/s), so that run was about 30% slower throughout.

#### F27 — Blocking JDBC runs on the default dispatcher
- **Medium**, likely · **Fixed in M3** (16/10/2026) · persist, restore, projections, futures
- **Fix:** a dedicated blocking dispatcher (D34).
- **Note (16/10/2026):** the command bus carries out permanent deletes on its own thread, and since F76 it first restores each duplicate of the deleted aggregate there; rare, but it holds up every command of the type meanwhile. Step 10 moves that work off the bus.
- **As fixed:** `reactivecqrs.blocking-dispatcher` in core's `reference.conf` (fixed pool of 32, throughput 1). Owners run on it, so persisting stays synchronous on their thread (F69 holds), and their command handlers run on the default dispatcher. The command bus (historical reads, the permanent delete with its restores, expired responses), the uid generator, the saga state, the outbox poller and the squash scheduler run their JDBC in futures on it and take the result back as a message. Projections are made by the host, which should use `Props(...).withDispatcher(BlockingDispatcher.id)`; one on the default dispatcher logs a warning at start (H8). The replayer is left to M4. Test: `CommandOutcomesSpec` (where an owner's event handlers and command handlers run; failed without the change).

#### F30 — Reflection per event on hot paths
- **Low**, likely · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState.scala:69, 82, 96, 109, 127`; `AggregateRepositoryActor.scala:312`
- **Fix:** precompute type ids; cache mpjsons serializers per class (D34).
- **As fixed:** events carry a `ClassTag` of their root instead of a `TypeTag` (D75), so the persist path takes the aggregate type key from the class once per command, without runtime reflection; mpjsons 0.7 already caches its codecs and type lookups per class in thread-safe maps; type ids come from the cache of F78.

#### F32 — Global synchronized LRU pool for `AggregateId`
- **Low**, likely · **Fixed in M3** (16/10/2026) · `api/.../id/AggregateId.scala:8-27`
- **Fix:** drop the pooling (D34).
- **As fixed:** `AggregateId(n)` makes a new instance; ids compare by value. The public `LRUCache` it used is deprecated.

#### F33 — Small hot-path waste
- **Low**, likely · Fix in **M2** for `pendingPublish.distinct` (`AggregateRepositoryActor.scala:95, 136`, removed with D8: **fixed** 16/10/2026) and the ERROR-level debug line (`ProjectionActor.scala:645`, rewritten with the sequence gate: **fixed** 16/10/2026); in **M3** for the eager log strings (**fixed** 16/10/2026: the optional events logger builds its lines only when debug is on)

#### F40 — `println` in production paths
- **Low**, confirmed · **Fixed in M1** (16/10/2026) · `UidGeneratorActor.scala:39, 50, 61` (failures also leave the requester unanswered), `SagaActor.scala:47`, `EventsBusActor.scala:182-185`
- **Test:** `UidGeneratorActorSpec` (the ask timed out before the fix).
- **Fix:** logger instead of `println`; the uid generator answers a failure with `Status.Failure`, so the requester fails at once. The replay tools keep their console output (operator progress), reworked in M4.

#### F48 — In-memory caches have no cross-node invalidation
- **Medium** in a cluster, likely · **Fixed in M5** (17/10/2026) · `PostgresDocumentStore`, `PostgresSubscriptionsState`
- **Fix:** one writer per projection (D53) keeps its caches valid.
- **As fixed:** a managed projection's supervisor, with its instances and builder, runs once in the cluster (`Projections.start` on a cluster makes it a singleton), so its document caches and subscription versions have one writer. Two writers at once, in a partition before the resolver downs a side, are decided by the subscription versions' compare-and-set and the document versions, as before. A subscriber registered on one node's bus only sees the events of that node's owners: whatever must see every event runs once. Test: `ClusterDeliverySpec` (one instance across three nodes, also after its node left; three without the singleton).

#### F78 — The type-name cache is changed from many threads without a lock; two nodes can race on a new name
- **Medium**, likely · **Fixed in M3** (16/10/2026, both parts) · `PostgresTypesNamesState.typeIdByClassName`, `classNameById`, `insertName`
- **Symptom:** the first use of a new class from two threads at once updates a plain `HashMap` concurrently (the lock covers only the database read); two nodes storing a new class at the same moment both `INSERT` the name and one persist fails on the unique index.
- **Root cause:** `cache.getOrElseUpdate` runs outside the monitor; the insert has no conflict handling.
- **Found:** plan review at the M1 close.
- **Fix:** a concurrent map, and `INSERT … ON CONFLICT (name) DO NOTHING` followed by a read.
- **Test:** `PostgresTypesNamesStateSpec` (four instances and forty threads ask for one new name: one id; failed before).

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
- **Low**, likely · **Fixed in M4** (17/10/2026: the table and its column changes run in one transaction; the indexes stay apart, as before) · `PostgresDocumentStore.scala:30-33`
- **Fix:** run the migration steps in one transaction, following D69 for large tables.

#### F118 — The document store refused valid table names
- **Low**, confirmed · **Fixed in M4** (17/10/2026: only the names the store makes count, the table and each index; `PostgresDocumentStoreSpec`, red before the fix) · `PostgresDocumentStore`
- **Symptom:** the guard against names PostgreSQL cuts at 63 characters always counted an index name, so a table name of 47 to 52 characters (58 to 63 with `projection_`) was refused for a store without indexes; with a build's suffix more names passed the limit, and the refusal stopped the build's instance (F110).
- **Found:** M4 close (review 6), audit.

#### F158 — `findDocumentByObjectInArray` wrote its value into the SQL text
- **High**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresDocumentStoreSpec` "F158") · `PostgresDocumentStore.findDocumentByObjectInArray`
- **Symptom:** the value is put between quotes in the query without escaping, so a value with a quote breaks the query, and one made to do so changes it (SQL injection) when a host passes a user's input.
- **Fix:** the JSON is bound as a parameter, written by a JSON encoder.
- **Found:** writing the book (M6, 17/10/2026).

#### F159 — Some document queries made invalid SQL
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresDocumentStoreSpec` "F159") · `PostgresDocumentStore.createQuery`, `createPartsQuery`
- **Symptom:** `findDocument` with no condition gave `WHERE` with nothing after it (the memory store returns every document); the parts queries' int and long sorts closed the quote in the wrong place, always added `LIMIT`, so `DocumentStoreQuery.basic` (limit -1) was refused, and read string parts as text, which the JSON reader refuses.
- **Found:** writing the book (M6, 17/10/2026).

#### F156 — The memory document store ignores spaces
- **Low**, confirmed · **Fix in M7** (the test kit, E11) · `MemoryDocumentStore`
- **Symptom:** `clearSpace` does nothing, so a test of clearing an organization against it passes without testing anything.
- **Found:** writing the book (M6, 17/10/2026).

#### F160 — Two finders read outside the listener's transaction
- **Low**, confirmed (by reading) · **Fix in M7** · `DocumentStore.findDocumentByPath(ExpectedValue)`, `findDocumentByPaths`
- **Symptom:** they take no session, so inside a listener they read what is committed, not what the listener's batch wrote.
- **Found:** writing the book (M6, 17/10/2026).

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
- **Fix:** every write result comes back as a message; failed writes are retried with back-off (100 ms doubling to 30 s, at most 10 attempts: `SagaActor.persistRetries`, overridable), then the saga is left at its last stored step and resumes after a restart. An order that cannot be stored is answered with `SagaFailureResponse`. Pending sagas are read completely before any starts, and a failed load is retried.

#### F36 — Saga update and delete ignore row counts
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresSagaState.scala:23-33`
- **Test:** `PostgresSagaStateSpec` (both returned `Unit` before the fix).
- **Fix:** `updateSaga` and `deleteSaga` return whether the row was there; the actor stops a saga whose row vanished, with an error log.

#### F136 — A saga can answer its caller twice after its runner moves
- **Low**, likely · **Accepted in M5** (17/10/2026; documented) · `SagaActor.handleStepResult`
- **Symptom:** a saga answers its caller before the write that ends it (or moves it to reverting) is stored. If the runner's node stops in between, the runner on the next node loads the saga at the same step, runs it again (its commands are idempotent by `SagaStep`, H3) and answers again: a live caller gets the answer twice.
- **Decision:** answers stay at least once. Answering after the write would lose the answer when the node stops between the commit and the answer, and the caller would order the saga again, which runs it all again under a new id. An `ask` takes the first answer (the second goes to dead letters); a caller that is a long-lived actor tells the answers apart by the saga's id, or takes any answer after the first as a repeat. Documented in ARCHITECTURE §9 and H10.
- **Found:** M5 close, audit of the cluster.

#### F137 — A saga stored the caller of an ask made on the runner's node without the node's address
- **Low**, likely (confirmed in `SagaCallerPathSpec`) · **Fixed in M5** (17/10/2026) · `SagaActor.callerPath`
- **Symptom:** Pekko writes an `ask`'s temporary actor with its node's address only while it serializes a message. Stored outside one, its path had no address (`pekko://cluster/temp/$a`), so after the runner moved, the answer went to the actor of that path on the new node: dead letters, or an unrelated ask of the same temporary name. Against D46.
- **Fix:** the path is written within the node's transport information, which gives it the address. Test: `SagaCallerPathSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F151 — One unreadable stored saga order kept every saga of its name from resuming
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresSagaStateSpec` "F151") · `PostgresSagaState.loadAllSagas`
- **Symptom:** the load reads every row of a saga name before any starts, and one order that no longer reads fails the whole load, which is retried without end: no saga of that name resumes.
- **Fix:** an unreadable row is logged and left out (`StoredDataCheck` reports it); the others resume.
- **Found:** writing the book (M6, 17/10/2026).

### Serialization (mpjsons 0.7)

#### F51 — mpjsons 0.7 may serialize the `TypeTag` held by every event
- **Closed, not a problem** (16/10/2026): mpjsons 0.7.0 skips the synthetic evidence field. `StoredFormatsSpec` shows event JSON without it, references recorded with 0.6.50 still read back, and the full suite passes on 0.7.0. Removing the `TypeTag` (D75) is still wanted for Scala 3, but no longer urgent.
- Original concern: `api/.../Event.scala:7`
- **Root cause:** 0.7 writes inherited fields; `Event[A: TypeTag]` stores its context bound as a field; 0.7 skips only fields reflection marks synthetic (mpjsons `ReflectionUtilNoCache.scala:50`). If not skipped, every event write fails.
- **Fix:** D75 removes the `TypeTag` from `Event`; the golden test proves the output.
- **Done (M3, 16/10/2026):** `Event` carries a `ClassTag` instead (D75); its evidence field is synthetic like the old one, and `StoredFormatsSpec` (the committed references and "an event carries no reflection data") passes unchanged.

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

#### F135 — The upgrade notes let an added field of an application class rely on its default
- **Medium**, likely (by mpjsons's documented behaviour) · **Fixed in M5** (17/10/2026; documentation) · H10, D68
- **Symptom:** H10 and the design said a field added later to a command, state or answer "must be an `Option` or have a default". The cluster serializer applies Scala defaults to the library's messages only; the application's classes are read by mpjsons with `ignoreNonExistingFields`, where a missing field reads as zero, null or `None`. A command of version 2 with `tags: List[String] = Nil`, sent by a node of version 1, reached its owner with `tags == null`.
- **Fix:** H10, D68 and the design say that a field added to an application class must be an `Option` (or a number or flag whose zero is right); a default is not applied.
- **Found:** M5 close, audit of the cluster.

### Schema initialization

#### F63 — Concurrent schema initialization fails
- **Medium**, confirmed · **Fixed in M5** (17/10/2026: every schema initialization, the components' `initSchema()` and the document stores' tables, runs under a PostgreSQL advisory lock held on a connection of its own, so processes that start together initialize one at a time; `SchemaLockSpec`, red before the fix with a duplicate `pg_type` key) · `PostgresEventStoreSchemaInitializer` and every `initSchema`
- **Symptom:** two processes initializing the schema at once (test suites in parallel today; cluster nodes starting together later) fail with a duplicate key on `pg_proc_proname_args_nsp_index` for `add_event`.
- **Root cause:** `CREATE OR REPLACE FUNCTION` and the other initialization statements run without any lock.
- **Fix:** run migrations once, under a PostgreSQL advisory lock, outside node startup (D69). Tests now run one suite at a time.

#### F71 — Catalog lookups ignore the schema
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresUidGenerator.readSequenceStep`, `PostgresDocumentStore.readExistingIndices`, `PostgresEventStoreState.countAllEvents` (removed with the replayer in M4), `PostgresSagaSchemaInitializer` (the F34 index check, new code of the same day)
- **Symptom:** in a database where another schema holds objects of the same names (a second installation, a copy, earlier test runs), the uid generator fails to start (`TooManyRowsException` from `pg_sequences`), a projection's indices or the saga key index are not created because the other schema's are seen, and the event count reads `public.events` whatever the current schema is.
- **Root cause:** catalog queries filtered by object name only, and one hard-coded `public`.
- **Found:** per-run test schemas (below): every testdomain spec failed at start.
- **Test:** `SchemaAwareCatalogSpec` (all three scenarios failed before the fix), and the whole suite now runs in a fresh schema per run.
- **Fix:** `schemaname = current_schema()`, `to_regclass(…)` and `'events'::regclass`, so the library works in whatever schema the connection's `search_path` selects. Also `countAllEvents` treats the `-1` estimate of a never-analyzed table (PostgreSQL 14+) like 0 and counts exactly.

### Rebuild

#### F54 — Rebuild needs downtime and clears read models first
- **Medium**, likely · **Fixed in M4** (17/10/2026: managed projections build a new version in tables of their own while the active version serves, and switch in one update; `ProjectionsSpec`) · `EventsReplayOrchestrator.scala:79-81`; Neula runs it as a separate run mode
- **Fix:** D57, D70.

#### F55 — The rebuild clear is not atomic
- **Medium**, likely · **Fixed in M4** (17/10/2026: a build starts in new tables, so nothing is cleared) · `SubscriptionsState.scala:291-294`; Neula clears documents in a separate auto-commit (e.g. `KanbansProjection.scala:30`)
- **Fix:** removed by D57.

#### F56 — Replayed events reach every subscriber of the type
- **Medium**, likely · **Fixed in M4** (17/10/2026: the builder delivers to the projection it builds only; `ProjectionBuilderSpec`) · `EventsBusActor.scala:300-326`
- **Fix:** D58.

#### F57 — Rebuild order differs from live order
- **Medium**, suspected (depends on cross-type Neula projections) · **Fixed in M4** (17/10/2026: the bulk pass reads every type of the projection in commit order; `ProjectionBuilderSpec`) · `EventsReplayerActor.scala:118-168`
- **Fix:** D59.

#### F98 — A subscriber that never saw an aggregate's squash point cannot start at the squash event
- **Medium** once squash is used, confirmed by reading (found at the M3 close, 16/10/2026) · **Fixed in M4** (17/10/2026: a re-publication from the squash point says so, `startsAtSquash`, through the bus to the delivery messages, and the gate starts a subscription below it there; a state listener is told the aggregate is created. Tests: `SequenceGateSpec`, `ProjectionDeliverySpec` (red under a mutation that ignores the flag), `EventsBusActorSpec`, `RepublishSpec`. The rebuild's part, reading each aggregate from its squash event, comes with the builder) · `SequenceGate.decide`, `ProjectionActor.drainFrom`, the delivery messages
- **Symptom:** a projection added after an aggregate was squashed (or one whose progress was reset) asks for the aggregate's events from version 1; the owner re-publishes from the squash event at N (D51), and the gate sees a gap before N, asks again, and gives up after `gapGiveUpAfter` (10 minutes) with a recorded failure. Live subscribers are not affected: D51 lets a squash run only after every subscriber applied the versions up to N.
- **Fix idea:** a delivery that starts with the squash event says so (the owner knows its squash point), and the gate applies such a delivery as the start of the aggregate's sequence when the subscriber is below it; M4's rebuild reads from the store and starts each aggregate at its squash event (D51).
- **Until then (H8):** add a projection before squashing the aggregate types it reads, or expect its failures on squashed aggregates.

#### F58 — Rebuild hides undo, and the hidden versions stall the rebuild
- **High**, confirmed (raised from Medium in M2, 16/10/2026) · **Fixed in M4** (17/10/2026: the builder delivers every event as live delivery sent it, undone ones and undos too, with each state as it was; `ProjectionBuilderSpec`, red under a mutation that ignores the undo in the replay states) · `PostgresEventStoreState.scala:284`
- **Symptom:** a rebuild leaves out undone events and undo events. A projection that rebuilds from cleared state then sees a gap at those versions (a cart with an undo at version 5: 1–3, then 6), keeps the later event waiting for versions that never come, and never acknowledges it. So the replay does not end: it goes on only when the bus deadline (D20b, 10 minutes) ends the stalled deliveries, which leaves those aggregates behind in the rebuilt projection, or the replayer's own 10-minute timeout exits the JVM first (F82).
- **Found:** the full test run in M2. The old `EventsReplaySpec` replayed the carts of the other specs, one of them with an undo, and both projections kept version 6 waiting for 4 and 5. (They had also failed on version 1, because the spec reused the other specs' read-model tables; that was a test fault.)
- **Fix:** D60; see H6.

#### F59 — Rebuild skips events still in the outbox
- **Low**, likely · **Fixed in M4** (17/10/2026: the builder reads the store, outbox or not; the gate drops what live delivery brings again) · `EventsReplayerActor.scala:155-168`
- **Fix:** rebuild reads the store directly (D58); live delivery dedups.

#### F60 — Rebuild progress is not durable
- **Medium**, likely · **Fixed in M4** (17/10/2026: the subscription versions commit with each chunk, and the bulk position is saved after it; a stopped build continues; `ProjectionBuilderSpec`, `ProjectionsSpec`) · `EventsReplayerActor.scala:179-182`
- **Fix:** D61.

#### F61 — Rebuild triggers are too coarse
- **Low**, likely · **Fixed in M4** (17/10/2026: a projection is built when its version changes, when a context whose states it receives changes version, or on request, and only that projection; one aggregate, one type or one organization is rebuilt in place with `RebuildAggregates`; `ProjectionsSpec`, red under mutations that skip the version reset or widen the target) · `EventsReplayOrchestrator.scala:51-63`
- **Fix:** D63.

#### F62 — Rebuild throughput limits
- **Medium**, likely · **Fixed in M4** (17/10/2026: a build reads the store directly, with no bus and no window, and applies chunks of 1 000 events in one transaction each, in commit order, through bounded replay states restored from snapshots, reusing a duplicate's base state; state-only projections take each aggregate's final state; `ProjectionBuilderSpec`; the bench's rebuild scenario measures it, docs/perf/baseline-M4.md. Live deliveries are applied in groups too, F93)
- **Root cause:** one transaction per aggregate per projection; a 100-event window in Neula (H1); every duplicate re-reads its base's history (`ReplayAggregateRepositoryActor.scala:70-77`); state-only projections replay every event; one replay actor per aggregate lingers 30 s; full state with every replayed message.
- **Fix:** D58, D59, D61, D62.

#### F82 — The replayer stops the whole JVM when a replay stalls
- **Medium**, likely · **Fixed in M4** (17/10/2026: the replayer is removed. A build stops nothing but itself: a stored event it cannot read fails only that aggregate's subscription, visibly (D20a), and the build goes on; a store it cannot read is read again after `retryAfter`; `ProjectionBuilderSpec`) · `EventsReplayerActor` (`System.exit(-1)`), removed
- **Symptom:** a replay that times out ends the host application's process; a single unreadable stored event makes the replayer throw, so the replay never answers (found with per-run test schemas, where specs damaged events and left them).
- **Found:** plan review at the M1 close.
- **Fix:** report the failure to the requester and stop the replay only.

#### F94 — Failure records of a removed projection block permanent deletes
- **Low**, likely · **Fixed in M4** (17/10/2026: a retired build's subscription versions and failures are dropped with its tables, and `Projections.retire(name)` drops every build of a projection removed from the code; `ProjectionsSpec`) · `PostgresEventStoreState.deleteAggregatePermanently`
- **Symptom:** `subscription_failures` rows of a projection that was removed or renamed are never retried or cleared, and a permanent delete waits while the aggregate has any.
- **Found:** M2 close (review 4).
- **Until then:** an operator deletes the rows of the retired projection by its `subscriber_type_id`.

#### F108 — A projection supervisor that failed on a message lost its bus
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: the supervisor never restarts (AR7): a message it fails on is logged and answered with `Status.Failure`; a switch that fails in the registry is tried again after `restartAfter`; a context check or a targeted rebuild the registry could not record waits for the next start; `ProjectionsSpec`, red before the fix) · `ProjectionSupervisor`
- **Symptom:** a registry or store error while the supervisor handled a message (the switch, a rebuild request, a targeted rebuild, the context check at start) restarted it. The restarted supervisor made new instances but never subscribed them, as the bus asks a subscriber for its subscriptions once, so the projection took no event until the process restarted while its queries were answered from tables that no longer moved. A targeted rebuild whose end could not be recorded also kept refusing every later one.
- **Found:** M4 close (review 6), by reading.

#### F109 — A listener that failed while making its work stopped its aggregate in a live group
- **Medium**, confirmed (new code of M4, `875b1ba`) · **Fixed in M4** (17/10/2026: making the work is part of the batch, so its failure is retried and recorded like any listener failure; `ProjectionGroupsSpec`, red before the fix) · `ProjectionActor.addToGroup`
- **Symptom:** a listener written as `(id, events) => { ...; session => ... }` that threw before returning its work, with live groups on, threw out of the group's drain: that aggregate's queue stayed at work forever and the other queues due in the same drain were dropped until another delivery came for them.
- **Found:** M4 close (review 6), by reading.

#### F110 — An instance that stopped by itself took the active build down and let events leave the outbox unseen
- **High**, confirmed · **Fixed in M4** (17/10/2026: a build whose instance stops starts again alone after a wait, the active build serving on; the instances that stop stay in the bus as stopped subscribers, which keeps their events in the outbox, until their replacements are subscribed, and then leave it without acknowledging (`RemoveSubscriber(keepOutbox = true)`); build slots are held or awaited once per supervisor, and every child has a name of its own; `ProjectionsSpec`, `BuildSlotsSpec`, red before the fix) · `ProjectionSupervisor`, `BuildSlots`, `EventsBusActor`
- **Symptom:** when any instance of a managed projection stopped by itself (a new build whose stores could not be made, a fatal error), the supervisor removed every instance from the bus, which acknowledged the deliveries they had not applied, stopped the active one too and started over after `restartAfter`, again and again while the cause lasted. Events published meanwhile left the outbox without the projection (I3), the active build stopped serving (D57), and at a start a build that kept failing kept the supervisor from answering the bus, so nothing was published anywhere. A second slot request granted a second builder, and a child created under a name still in use kept the supervisor from starting.
- **Found:** M4 close (review 6), by both audits.

#### F111 — An aggregate deleted for good during a build stayed in the new build
- **High**, confirmed · **Fixed in M4** (17/10/2026: while built, a delivery that removes its aggregate, a permanent delete event or no state for a state listener, goes through the gate like a live one, missing versions coming from the builder, instead of being acknowledged unapplied; `ProjectionsSpec`, red before the fix) · `ProjectionActor.receiveDelivery`
- **Symptom:** the build in progress acknowledged the delete without applying it, as the builder reads the store; the delete then removed the aggregate's events and row, so neither the bulk pass nor the reconciliation could deliver it, and a document the bulk pass had made stayed in the new build.
- **Found:** M4 close (review 6), audit.

#### F112 — A targeted rebuild of a type or an organization lost aggregates made while it began
- **High** under load, confirmed · **Fixed in M4** (17/10/2026: the projection lists the target's aggregates when it handles the reset, after every delivery taken before; an organization is not rebuilt while aggregates of its types have no space; the deliveries waiting for reset aggregates are acknowledged, as the builder gives their events again; `ProjectionsSpec`, `ProjectionDeliverySpec`, red before the fix) · `ProjectionSupervisor.rebuildTarget`, `ProjectionActor`
- **Symptom:** the supervisor listed the aggregates before the reset reached the projection. One made and applied in between lost its document to `clearForRebuild` but kept its version, so the builder found it applied and never gave it back; so did an aggregate still without a space (F49). The queues of reset aggregates were dropped with their deliveries unanswered, which waited for the poller.
- **Found:** M4 close (review 6), both audits.

#### F114 — A targeted rebuild ran listeners' side effects for history
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: `rebuilding` is also true while a listener applies what a builder gives, a chunk or an item handed to a queue; `ProjectionDeliverySpec`, red under the old flag) · `ProjectionActor`
- **Symptom:** a targeted rebuild feeds the live instance, whose `rebuilding` was false, so push updates and other guarded side effects fired again for each historical state of the target.
- **Note:** a `noDBSession` listener in a chunk that fails runs again when the chunk is applied one item at a time: at least once, as documented.
- **Found:** M4 close (review 6), audit.

#### F116 — A build retired or abandoned just before a stop was never dropped
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: a supervisor drops at its start every build of its projection that is neither active nor in progress; a retired build loses its subscription versions and failures before its registry row, so a stop in between leaves it to that sweep; `Projections.retire` drops every build the registry has; `ProjectionsSpec`, red without the sweep) · `ProjectionSupervisor.drop`, `ProjectionRegistry`
- **Symptom:** a retired build was dropped only from memory, so a stop before the drop left its tables, its versions and its failures, which block permanent deletes and squashes (F94 again); a later build of the same version could reuse its suffix and subscriber, and inherit its versions.
- **Found:** M4 close (review 6), audit.

#### F117 — A store that failed for a moment failed a build's subscriptions
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: a read error of the connection, resource or shutdown kind is read again, as a page is, or the builder stops and its supervisor starts it again from the saved position; only what cannot be read at all fails its subscription; `ProjectionBuilderSpec`, red before the fix) · `ProjectionBuilder`
- **Symptom:** any error while reading an aggregate by version or restoring its state, a lost connection too, was recorded as an unreadable event at the first version asked, and after `GoLive` nothing retried it, so the build waited for an operator before it could switch.
- **Found:** M4 close (review 6), audit.

#### F124 — A commit-order build walks the events of every type to find its own
- **Medium**, likely (not measured) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState.readCommittedPage`, `event_keys`
- **Symptom:** the bulk pass reads `events` in id order joined with `aggregates` to keep its types (and, for a scoped build, its aggregates or space). A projection whose types hold a small share of a large store, or a rebuild of one organization (D56, R17), walks the whole store once to find its events: hours at 100 million events, however few are its own (estimated).
- **Fix:** `event_keys (id → events.id ON DELETE CASCADE, type_id, space_id)`, indexed on `(type_id, id)` and `(space_id, type_id, id)`: the three procedures write each event's key, the cascade deletes it with the event, the space backfill (F49) updates it; `backfillEventKeys()` gives the events from before theirs online (D69) and records the step in `schema_steps` once every event has one (an installation without events records it at once). From then the page reads one index scan per type, in a space by its space; named aggregates are read through `events (aggregate_id, id)` from the start. Columns on `events` were rejected: their backfill would rewrite every event row (D28). Measured: the extra insert per event cost nothing measurable (two single runs each, within their spread, `docs/design/M6-organizations.md`). Tests: `PostgresEventStoreStateSpec` (keys from every procedure and gone with their events; the backfill and the switch of the read path, with the same pages; the space backfill), each red under its mutation; the builder and equivalence specs read through the keys in their fresh schemas.
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F139 — A range read by version stopped at a squash far above its start
- **Medium**, low likelihood, confirmed (reproduced in `ProjectionBuilderSpec`) · **Fixed in M5** (17/10/2026) · `ProjectionBuilder.itemsFrom`
- **Symptom:** the reconciliation and gap fills read an aggregate's missing range in parts from the projection's version, and stopped at the first empty part. Since F126 a part is a window of versions; when the aggregate had been squashed far above that version (more than a part's events, for example by the squash scheduler during a long build), the first window held nothing, as the versions below the squash event are gone, and the aggregate got no item: the build switched in with it stale until its next event, or a gap fill ended as a recorded failure.
- **Fix:** a range starts at the squash point when it is below it, and an empty part looks at the squash point again, so a squash made after the aggregate was listed is found too. Test: `ProjectionBuilderSpec` "a range from far below the squash point starts at the squash event" (both builds red before).
- **Found:** M5 close, audit of delivery.

#### F140 — The builder's chunks could hold a whole page, and a long range restored its state once per part
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `ProjectionBuilder.committed`, `itemsFrom`, `chunked`
- **Symptom:** in the bulk pass, consecutive events of one aggregate in a page became one item, and a chunk ended only after an item took it past `chunkEvents` or `chunkBytes`: a chunk could hold up to a page (1 000 events, 64 MB), against the 16 MB that ARCHITECTURE §8.1 promised. A range read by version (the reconciliation, gap fills) restored the aggregate's state for each part, for a listener of events with state: ten restores, each a replay, for 10 000 events.
- **Fix:** a run ends where it would pass a chunk's events or bytes, and a range is read in parts of a chunk's events; a chunk ends before an item that would not fit, so it holds at most `chunkEvents` and `chunkBytes` (one event larger than that goes alone). A range restores its state once, at its end, and every part carries it, as an owner's re-published parts do. Test: `ProjectionBuilderSpec` "a long run of one aggregate is cut to fit chunks" (chunk sizes, and one restore; each red under its own mutation).
- **Found:** M5 close, audit of delivery.

#### F132 — `Projections.retire` dropped a projection that nodes of the older version still ran
- **Medium**, confirmed (reproduced in `RollingUpgradeSpec`) · **Fixed in M5** (17/10/2026) · `Projections.retire`, `Members.olderVersionsRun`
- **Symptom:** every node calls `retire` for a projection removed from its code as it starts, and it dropped the tables, subscription rows and registry entry at once. In a rolling upgrade the first node of the new version dropped the projection while its singleton still served on the oldest node, of the old version: queries failed, and its listeners wrote new failure rows after the drop, which kept permanent deletes waiting until a later restart retired it again. Against expand-then-contract (D69) and zero-downtime upgrades (D65).
- **Fix:** while a member runs an older app-version than the node (or the node knows no member yet), the drop waits, looked at again every 10 s; it happens once every member runs the node's version or a later one. Test: `RollingUpgradeSpec` (the removed projection still serves while the old nodes run, and is retired once they left; red when the drop does not wait).
- **Found:** M5 close, audit of the cluster.

#### F152 — A host cannot watch a build's progress
- **Low**, confirmed (by reading) · **Fix in M7** (operations) · `ProjectionSupervisor`
- **Symptom:** `GetBuildStatus` is the builder's, and the supervisor passes unknown messages to the serving instance, which ignores it; `GetBuilds` answers the `BuildRecord` captured when the instance was made, whose position is never refreshed. SCALING §10 lists `GetBuildStatus` as a signal.
- **Found:** writing the book (M6, 17/10/2026).

#### F153 — `ServeStale` holds only for builds begun when the supervisor starts
- **Low**, confirmed (by reading) · **Fix in M7** · `ProjectionSupervisor`
- **Symptom:** for a `RebuildProjection` at run time the active instance is already subscribed and keeps taking live events; after a restart during that build it serves stale.
- **Found:** writing the book (M6, 17/10/2026).

### Code structure

#### F14 — In-memory test doubles are wrong or incomplete
- **Low**, confirmed · **Fixed in M1, step 0** (16/10/2026) · in-memory adapters
- **Found and fixed:**
  - `MemorySubscriptionsState`: `clearSubscriptionsInfo` removed the *other* subscribers' entries; a stale compare-and-set failed silently; an update without a prior read was dropped.
  - `MemoryEventStoreState`: a command with several events got versions 1, 3, 5; the outbox read compared versions with the aggregate id, so it was always empty; deleting an already-deleted event threw; the re-publish scan ignored the aggregate type; duplicated aggregates did not include their base's history; `countEventsForAggregateTypes` and `overwriteEvents` were `???`.
  - `MemoryDocumentStore`: six query conditions were `???`, two were missing (match error), `ILIKE` was an exact match, batch inserts were `???`.
- **Tests:** `MemorySubscriptionsStateSpec`, `MemoryEventStoreStateTestDoubleSpec`, `MemoryDocumentStoreSpec` (query conditions); `MemoryEventStoreStateSpec` (duplication) now passes.
- **Still not mirrored** (documented gaps of the test double): instant reads, `PermanentDeleteEvent` clean-up, spaces in the document store.

#### F97 — The in-memory store undid an undo instead of the event before it
- **Low**, confirmed (found with the squash tests, 16/10/2026) · **Fixed in M3** · `MemoryEventStoreState.withNoopFlags`
- **Symptom:** an undo right after another undo counted the first undo as the event it undoes, so in memory it brought the first undo's events back, while PostgreSQL's `add_undo_event` undoes the newest event not undone yet. Tests on the in-memory store saw another state than production.
- **Fix:** the flags are computed as the procedure does: each undo marks itself and the newest `eventsCount` events not marked yet. The store's versions also come from the last event, not the count of events, since a squash removes events.
- **Test:** `EventStoreStateSpec` "An undo right after another…" (both stores; failed on the in-memory store before).

#### F50 — Type keys disagree for nested classes; runtime reflection blocks Scala 3
- **Low**, likely · **Fixed in M3** (16/10/2026, with F30) · `AggregateContext.scala:54` vs `ProjectionActor.scala:180-196`; also the outbox scan (`AggregateCommandBusActor`: `runtimeClass.getName`) vs persist (`typeSymbol.fullName`), so a nested aggregate class never gets its waiting events re-sent
- **Fix:** D75.
- **As fixed:** one key, `AggregateType.of[R]`, for the context, the command bus, the owner, the stores, projection listeners, the replayer and the squash scheduler: Scala's full name of the class, which is what the store has always kept (`typeSymbol.fullName`), taken from the class (`getName` with `$` read as `.`). No stored name changes. Events and listeners take a `ClassTag` instead of a `TypeTag`. Test: `EventStoreStateSpec` (a nested root class, both stores). Left: `SubscribableProjectionActor` keys updates by `TypeTag` (a listener-side key, not stored).

---

#### F150 — `StoredDataCheck.usersOf` said "None" for a class nobody uses
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `StoredDataCheck.usersOf` (its comment)
- **Symptom:** the method always answers `ClassUsers`; a class can go when both its counts are zero.
- **Found:** writing the book (M6, 17/10/2026).

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
| H8 | Low, likely | M3 host steps: run `PostgresEventStoreState.buildIndexesConcurrently()` once after the upgrade, in the background (F38); opt in to snapshots (`AggregateContext.snapshots`) for the contexts with long histories (R10); commands give their events a metadata envelope by mixing in `WithMetadata` (R13); a context writes its own rows with its events by mixing in `WithOutboxWriter` (R12); serial mode is `commandProcessing = Serial(...)` (R8), and `CommandRejected` is a new response; `PassivateAggregate(id)` is available for operations. Squash: a context opts in with a squash event (`squash = Some(SquashPolicy(state => SquashEvent(...)))`, a `FirstEvent` its event handlers and projections handle); every squash checks that its event gives the state back exactly (D85), and an event class that leaves data out on purpose mixes in `SquashMayChangeState`; `SquashAggregate(id, upTo)` squashes on request, and a `SquashScheduler` started with `SquashTarget.of(context, commandBus)` squashes by `SquashPolicy.automatic`; contexts with audit duties wait for R11 (M6). A type whose aggregates are duplicated and permanently deleted needs a squash event, or those deletes wait (F76). Dispatcher (F27): create projections with `Props(...).withDispatcher(BlockingDispatcher.id)` and size `reactivecqrs.blocking-dispatcher` near the JDBC pool; the id sequences step by 10 000 from the first start (F23). Subscriptions take no classifier any more (AR8). Until M4, add a projection before squashing the aggregate types it reads (F98). Run the golden event logs with and without snapshots and compare the handlers' results (the plan's M3 check). Type keys (D75): events and projection listeners take a `ClassTag` of the root, so an event class generic in its root needs `[R: ClassTag]` where it had `[R: TypeTag]`; `Event.aggregateRootType` is now `aggregateRootClass`; a key built by hand should be `AggregateType.of[R]` (for a nested root class `Outer.Inner`, as stored); `api.id.LRUCache` is deprecated. | M3 (Neula) |
| H9 | Medium, likely | M4 host steps: create one `Projections` and start each read model through it (`projections.start[P](name, version, actorName) { build => ... }`), passing the returned supervisor where the projection's `ActorRef` went (query APIs, the REST layer, `RegisterSubscribers`); name every store's table with `build.documentStore(table)` and the state classes' tables and sequences with `build.table`/`build.sequence`, and give each build's stores caches of their own, as a cache shared by two builds mixes their documents (state shared between two projections needs one owner, the other asks it); replace the rebuild-mode constructor flags with `rebuilding` (side effects) and `onActivated()` (state kept in memory while built must not outlive a chunk: a build commits chunk by chunk and resumes); keep the reactors (side effects) unmanaged; drop the offline rebuild run mode, the empty index lists of rebuild mode and the upgrade gate that relied on it. M4 removed the 0.12 replayer and what only it used: `EventsReplayOrchestrator`, `VersionsState` (its `components_versions` table stays, read once to adopt the projections 0.12 built), the command buses' `eventsReplayMode` argument, `DeliveryOptions.maxReplayBuffer` (so `eventBusBufferSize`, H1, goes), the projections' `version`, `onClearProjectionData` and `GetSubscribedAggregates`, and the `replayed` argument of the delivery messages, which tests that build them by hand drop. Live deliveries that arrive together are applied up to 100 aggregates per transaction (`ProjectionActorOptions.maxBatchesPerTransaction`, D61): a listener must never commit or roll back the session it is given. Retire removed projections with `Projections.retire(name)`. Pass the aggregate contexts whose states projections receive. Implement `clearForRebuild` in the projections that need targeted rebuilds (`RebuildAggregates`); before rebuilding an organization, run `PostgresEventStoreState.backfillSpaces` once, with a function for the six deprecated first events that use `SpaceId.unknown` (F49). | M4 (Neula) |
| H10 | Medium, likely | M5 host steps (growing with M5): set `pekko.actor.provider = cluster` with the split brain resolver (`keep-majority`, or `lease-majority` with the Kubernetes lease) on every node, also on-premise (a cluster of one, D42); Java serialization stays off and every message between nodes goes through the library's serializer: commands, events, states, saga orders and responses and query messages and answers must be classes mpjsons writes and reads by their class (no fields typed `Any`, a trait, `ActorRef` or `Throwable`), and a field added to one later must be an `Option`: a missing field reads as `None`, zero or null, never its Scala default (D68, F135). Contexts opt in to one owner per aggregate with `placement = Placement.Sharded(n)`; then make `terminationGracePeriodSeconds` exceed the hand-over timeouts (90 s in core's `reference.conf`, above the default command timeout; raise both with longer command timeouts). Start the outbox poller, the squash scheduler and each saga runner with `Singletons.start(system, name, props)` on every node, and send saga orders to the returned proxy; `Projections.start` returns a proxy on a cluster: register it on every node's bus. `ProjectionsSettings` no longer takes the event bus. A subscriber that answers the bus itself replies `SubscriptionRequests(requests, registered)` to `GetSubscriptionRequests(registered)`. Ids (D39): set `reactivecqrs.installation-number` on every node of an installation, make the uid generator with `UidGeneratorActor.postgres(system)`, and choose aggregate ids only below 10^12 (system and legacy ids) or from `ReserveAggregateId`; check that no id is read as `Int`. A subscriber registered on one node only sees that node's owners' events. Rolling upgrades (D65–D73): set `pekko.cluster.app-version` to each release's version on every node; a context that adds an event class lists it in `writtenFrom` with the first version that reads it, and its handlers write the older event until `everyNodeRuns(version)`; add `io.reactivecqrs.core.cluster.ReadinessCheck` to `pekko.management.health-checks.readiness-checks`. Messages between nodes (F121): keep commands, their answers, states and query answers under 248 KiB, or raise `pekko.remote.artery.advanced.maximum-frame-size` (direct memory: up to 128 buffers of it); keep documents out of events and commands (docs/SCALING.md §7). Load (docs/SCALING.md): bound the commands in flight at the edge (and by bytes for large ones), give the JDBC pool at least 32 connections, tune autovacuum for `events_to_publish`, `subscriptions` and `aggregates`; with events of megabytes, lower `PostgresEventStoreState.fetchSize` or set the driver's `adaptiveFetch` with a `maxResultBuffer`. Metrics (R19): send them on with `ReactiveMetrics(system).use(...)` (ARCHITECTURE §14). Command handlers run on `reactivecqrs.handler-dispatcher` (32 threads, F127): size it for handlers that wait; give heavy work its own threads (`AsyncCommandResult`) or move it out of the command, and for handlers of minutes raise `commandTimeout` and, for sharded types, the hand-over timeouts above them (docs/SCALING.md §8). In a cluster of several nodes, reads of a `Local` type cost a key lookup each (F131): make types read often `Sharded`; change a type's placement or `numberOfShards` only with a full stop, never in a rolling upgrade. Size the bus's window per node: a projection's mailbox can hold one window from each node (SCALING §5). A listener of push subscriptions that gets `ProjectionSubscriptionsCancelled` for a renewal subscribes again (F133). Saga answers are at least once: a caller that is a long-lived actor takes a second answer for the same saga as a repeat (F136). | M5 (Neula) |
| H11 | Medium, likely | M6 host steps (growing with M6): every first event gives the organization's space (`SpaceId(0)` for the platform); one without is refused with `FailureResponse` (D38). Once every node runs M6, run `PostgresEventStoreState.backfillEventKeys()` once in the background, and `buildIndexesConcurrently()` again (it now also builds `aggregates (space_id)`): until the keys are complete, builds of a few types or of one organization walk the whole store (F124); do not roll back below M6 afterwards, as earlier versions write events without keys. Purge (R17): `PostgresCommandResponseState.initSchema()` adds `commands_responses.aggregate_id` and builds its index concurrently (responses stored before have none and expire with the retention), `PostgresSagaState.initSchema()` adds `sagas.space_id`; sagas whose orders belong to an organization override `spaceOf(order)`; `buildIndexesConcurrently()` now builds `aggregates (space_id, id)` and drops the first M6 steps' `aggregates (space_id)`; a purge is `new SpacePurge(eventStore, commandBuses, projections, hooks).purge(space)`, run from one node while the organization is quiet, with this node's command bus of every type the organization has, every projection that holds its documents (each must implement `clearForRebuild` for `OneSpace`; do not rebuild them meanwhile), and a `PurgeHook` for each table of the host's own (an outbox writer's rows); check its `PurgeReport.complete`, run it again if not, and verify with `StoredDataCheck.spaceRows(space)`. Moving an organization (D40): `new SpaceExport(eventStore, contexts).write(space, writer)` from a quiet organization (`contexts`: those of its types; a copy of another organization's aggregate needs its type's squash event), then `new SpaceImport(eventStore, uidGenerator, rewriters, newMPJsons)(system).read(file, into)` into an empty space of the other installation (give `newMPJsons` an instance set up as yours, with your converters; an `ImportRewriter` for each type that holds ids as plain numbers or text), then `RebuildAggregates(OneSpace(into))` on each projection; if the report is not `complete`, purge `into` and import again. Contexts with audit duties name their audit event classes in `SquashPolicy.keep` before they opt in to squash (R11): those events move to `events_archive` and are read with `GetArchivedEvents(id)`; `AutomaticSquash.keepNewerThan` keeps the recent history; `StoredDataCheck.usersOf(className)` lists who still uses a class before it is removed from the code. | M6 (Neula) |

---

## 5. Answers to Neula's questions

From §7 of Neula's change requests.

| # | Question | Answer |
|---|---|---|
| 1 | Where does the squash snapshot event live? | Squash events are in the `events` table at version N (D47, D48). Performance snapshots are in a separate `snapshots` table and never consume versions (D30). Projections see squash events like other events; rebuilds of state-only projections start from snapshots (D62). |
| 2 | Can audit events stay below a squash point? | Since M6, yes: the classes a squash policy keeps move to a readable archive table instead of going (R11, D52). BusinessEntity contexts name their audit classes there before they opt in. |
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
| M3 | AR1 ✔, AR4 ✔, AR6 (executor retry ✔), AR7 (owner and command bus ✔); AR8 ✔; F8 (contract ✔), F22 ✔, F23 ✔, F25 ✔, F27 ✔, F30 ✔, F31 ✔, F32 ✔, F33 (rest ✔), F38 ✔, F39 ✔, F44 ✔, F50 ✔, F65 ✔, F76 ✔, F78 ✔, F81 ✔, F96 ✔, F97 ✔, F100–F107 ✔; D85 ✔; Neula H8 |
| M4 | F3 ✔, F43 ✔, F49 (backfill ✔), F54 ✔, F55 ✔, F56 ✔, F57 ✔, F58 ✔, F59 ✔, F60 ✔, F61 ✔, F62 ✔, F82 ✔, F93 (throughput ✔; flow control open), F94 ✔, F98 ✔, F108–F118 ✔; Neula H1, H6, H9 |
| M5 | F46 ✔ (sharded types), F48 ✔, F63 ✔, F93 ✔ (flow control), F99 ✔, F119 ✔, F120 ✔, F121 ✔, F122 ✔, F123 ✔, F125 ✔, F126 ✔, F127 ✔, F128 ✔, F129 ✔, F130 ✔, F131 ✔, F132 ✔, F133 ✔, F134 ✔, F135 ✔, F136 (accepted), F137 ✔, F138 ✔, F139 ✔, F140 ✔, F141 ✔, F142 ✔ (F78 between nodes ✔ in M3) |
| M6 | F8 (complete), F124 ✔, F143 ✔, F144 ✔, F145 ✔, F146, F147 ✔, F149 ✔, F150 ✔, F151 ✔, F154 ✔, F155 (decide at review 8), F158 ✔, F159 ✔, F167 ✔, F168 ✔, F172 ✔ |
| M7 | AR5 (moved from M5 at review 7); F148, F152, F153, F156, F157, F160, F161, F162, F163, F164, F165, F166, F169 (found writing the book), F173 |

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
- **Done** 16/10/2026 (M3): `AggregateOwner` (`core/aggregate/`). Refined: the per-command executor actors went too; the owner runs a handler as a `Future` with a timer per command, and stops by passivation (the command bus keeps the messages that arrive meanwhile).
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
- **When:** M3 with AR1; F67 is fixed now. **Done** 16/10/2026 by construction: with the executors gone, the owner's messages to itself about a command (`HandlerFinished`, `CommandTimedOut`, `RunAgain`) are private and handled in every state.

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
- **When:** M7, with the migration guide for Neula's sagas (moved from M5 at review 7: the runner already runs as a singleton and answers across nodes, F137).

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
- **When:** M2 for the event bus and the projection actors (rewritten there; F79, F80), M3 for the aggregate owner and the command bus (with AR1). Done in M1: F70 and F75 (unknown messages, id failures). The outbox scan left the command bus in M2. **Done for the owner and the command bus** 16/10/2026 (M3): the owner answers per-command failures and, on an unexpected one, reads its state again; the command bus stops a failed owner instead of restarting it.

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
- **Decided and done (M3, 16/10/2026):** classifiers are removed. Every subscription takes every event of its aggregate type; `SubscribeForEvents`, `SubscribeForAggregates` and `SubscribeForAggregatesWithEvents` take no classifier, and `SubscriptionClassifier` and `EventType` are gone. A subscriber that wants a part of the events filters in its listener.

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

### Review 5, 16/10/2026: closing M3 (architecture description, plan and code)

Scope: the aggregate owner and its features as built (AR1; snapshots D30, serial mode R8, the
metadata envelope D32, the outbox hook D33, the rewrite contract D17, squash D47–D52 with its
check D85 and scheduler), the type keys (D75), the id pools (F23), the blocking dispatcher
(F27), `docs/ARCHITECTURE.md` §2–§4 and §10, and the remaining milestones. Inputs: the bench
(`baseline-M3.md` against `baseline-M2.md`, plus a restore-only run), a read-only audit of the
M3 code by a separate agent, and our own reading; every finding was checked in the code, and
every fix has a test that failed against the code before it.

**Outcome.** The M3 decisions hold (D17, D29–D35, D47–D52, D75); D85 was added at the user's
request: a squash event must give back exactly the state it replaces, unless its class says
it may differ (`SquashMayChangeState`). Refinements: D51's preconditions also cover the
duplicates a squash detaches (their first event neither undone nor waiting, no failed
subscription) and run again under the chain lock; the chain lock covers every row a detach
changes. Two problems stay open: F98 (a subscriber that never saw a squash point cannot start
at the squash event, M4) and F99 (contended concurrent commands wait a back-off, M5; serial
mode is the remedy).

**Fixed now:** F100–F107, found by the audit; earlier in the milestone F96 and F97, found with
the squash work.

**New open issues:** F98, F99.

**Measured** (`baseline-M3.md`; single runs on one machine, against `baseline-M2.md`):
- Commands on many aggregates without subscribers: 11 151 commands/s (M2: 9 738), p99 3.9 ms
  (7.7 ms), one SQL statement per command; with two projections 6 112 commands/s (6 024).
- Serial mode (R8): 12 793 commands/s on many aggregates, not below concurrent mode; on one
  contended aggregate 2 501 commands/s, p99 11 ms, no conflict. Concurrent mode on that
  aggregate: 738 commands/s, p99 380 ms (M2: 1 166, p99 197 ms), see F99.
- Restore (R10): from a snapshot 2–7 ms at every size up to 500 000 events. A full replay of
  500 000 events takes 1.95 s with a peak heap of 142 MB in a restore-only run (M2: 2.16 s,
  450 MB), 3.3 s in the full run after the earlier scenarios had filled the database; a fetch
  size of 10 000 instead of 1 000 changed nothing.
- Writes straight through the store: 33 814 events/s (M2: 21 924; the set-based `add_events`,
  F31).
- Soaks: the idle heap is back at 20 MB after the memory soak; the delivery soak of 1 000 000
  events ends with no delivery waiting, an empty outbox and no failure, 21 MB after each of 5
  restarts, every event projected 663 s after the load started (M2: 603 s).
- Startup 787/822 ms in the full run (M2: 525/560 ms); a short run of the same code measured
  511/546 ms, so this is the run's noise, to be watched in the M4 baseline.

**Observations:**
- The audit's findings sit at the seams M3 added: what the store changes under a running owner
  (a permanent delete, a detach, a rewrite of a base) and what readers of old history see (an
  instant before the squash point, a duplicate's rewrite reaching its base). The rule now
  (`ARCHITECTURE.md` §10, invariants 10 and 11): a change of stored history that a running
  owner cannot see stops that owner without a snapshot, and snapshots are written only while
  their aggregate is stored at their version.
- A squash and a permanent delete take every row they may change in one ordered statement, and
  a duplication takes its base's chain in the same order; their later statements touch only
  rows already held.
- The in-memory store is a test double, but its divergences hid real semantics: an undo of an
  undo (F97), a command's undos (F106) and undone first events (F105) differed from
  PostgreSQL. `EventStoreStateSpec` runs every new store behaviour on both.
- The type key (`AggregateType.of`) equals the stored name of every Neula aggregate root (all
  top-level classes, checked by the audit in Neula's sources), so no stored name changes.
- Test hygiene: the test helper `??` gives a "dead code" warning at every call, as its type
  parameter is taken as `Nothing` while the call is typed (an implicit `ClassTag` does not
  help: it is resolved first). Harmless, but it buries real warnings; typed asks can replace
  it when M4 touches the specs.

**Plan refinements** (recorded in the implementation plan): M4: F98 (a delivery that starts
with a squash event starts the gate's sequence; the rebuild reads each aggregate from its
squash event); the restore module is the source of replay states; the new projection runtime
puts its actors on the blocking dispatcher itself. M5: item 7 (Scala 3 groundwork) is mostly
done and F78's cross-node part is done; the squash scheduler joins the singletons; id pools of
10 000 per node; F99. M6: export, import and the complete rewrite use the squash event factory
and take the chain lock; the retention squash builds on the scheduler. M7: the runbook carries
M3's host steps (H8) and squash's operations notes.

**Looking ahead:**
- *M4 rebuild*: the restore module gives states at any version from a snapshot plus a tail
  and refuses reads below a squash point; a rebuild reads each aggregate from its squash event,
  which the gate must accept as a start (F98). Squash events are ordinary events of their
  contexts, so projections handle them like any other.
- *M5 cluster*: the owner's stop protocol (`StopOwner`, `StopOwnerStale`, the kept messages) is
  passivation; `HistoryChanged` and the delete notice go to the command bus, which forwards the
  stops to wherever the owners run. The id pools are per node; the squash scheduler and the
  outbox poller become singletons; the blocking dispatcher is configured per node.
- *M6*: the squash event factory (`SquashPolicy.toEvent`, checked by D85) is what export,
  import and the complete rewrite reuse; any operation that changes a base's history takes the
  chain lock and tells running owners (`HistoryChanged`).
- *Watch*: `SquashMayChangeState` is the one way stored history can give a different state; it
  reaches live owners and snapshots, not projections (D51), which only a rebuild repairs.

### Review 6, 17/10/2026: closing M4 (architecture description, plan and code)

Scope: the projection runtime as built (builds and the registry D57, D70, D81; the builder
D58–D62; targeted rebuilds D63; the supervisor), the live groups (D61 on the live path), the
subscription state in one mode (F3), the removal of the 0.12 replayer, `docs/ARCHITECTURE.md`
§5–§8, and the remaining milestones. Inputs: the bench (`baseline-M4.md` against
`baseline-M3.md`), the equivalence property test, two read-only audits of the M4 code by
separate agents (the rebuild package; the live path and its state), and our own reading; every
finding was checked in the code, and every fix has a test that failed against the code before
it, or under a mutation that restores the old behaviour.

**Outcome.** The M4 decisions hold (D57–D63, D70, D81), and D61 now covers the live path.
Refinements: an instance that stops is replaced without acknowledging what it did not take, and
a build that cannot start leaves the active build serving (F110); a removal during a build
reaches the build (F111); a targeted rebuild lists its target where it resets it (F112);
`rebuilding` covers the history a builder gives a live instance (F114). One problem stays open:
flow control on the live path (F93), which moves to M5.

**Fixed now:** F108–F118: F108 and F109 by our reading, F110–F118 by the audits (F110 and F112
by both).

**New open issues:** none; F93's flow control is scheduled in M5.

**Measured** (`baseline-M4.md`; single runs on one machine, against `baseline-M3.md`):
- Rebuild: 112 211 events built in 16.6 s, 6 769 events/s, both projections switched together,
  peak heap 150 MB (M3's 0.12 replayer: 36.9 s, 3 044 events/s, 167 MB).
- Delivery soak, 1 000 000 events, a tenth of the listeners failing for 30 s: every event
  projected 330 s after the load started (M3: 663 s); no delivery waiting, an empty outbox, no
  recorded failure; 22–23 MB after each of 5 restarts.
- Commands with two projections, now with their versions in the database: the projections keep
  pace during the load (2 732 events/s, 0.5 s behind; M3: 1 630 events/s, 1.4 s behind), so they
  compete with it: 4 780 commands/s (M3: 6 112), and the load with its projection ends after
  1.2 s instead of 2.0 s. 3.4 statements and 1.0 committed transaction per command (M3: 5.0 and
  3.0, with subscriptions in memory; 7.0 statements with them in the database before the groups).
- Commands without subscribers, serial mode, restores and startup: as in M3 (10 691 commands/s,
  serial 11 176 and 2 384 on one aggregate, a snapshot restore in 1–5 ms, 502/560 ms).
- Memory soak: 475 403 commands in 60 s, projected 201 s after the load, the heap back at 20 MB.

**Observations:**
- The audits' findings sit where a managed projection meets failure: an instance that stops
  (F110), a supervisor that fails (F108), a crash between a switch and a drop (F116), a store
  that fails for a moment (F117), and where a build runs beside live traffic (F111, F112, F114).
  The rule now (`ARCHITECTURE.md` §8.2): what a projection has not taken stays in the outbox until
  a replacement has it; the supervisor never restarts; a build never takes down the active one.
- The equivalence test caught nothing in the code, but its first version proved too little: the
  state-only list projection was built from final states (D62), so a squash start was never
  exercised, and its squashes were refused while their events waited in the outbox. Each test
  now asserts that the history it generates has every feature, and every new test was run
  against a mutation of what it checks.
- The bench's rebuild scenario, rewritten in step 6, had not run until the baseline: its managed
  projections reused classes the earlier scenarios ran unmanaged, so they were adopted as 0.12
  builds (D81). Bench changes now run before they are committed.
- Pushes of a subscribable projection go out before its transaction commits: a failed group,
  applied again one batch at a time, sends them again, and may announce a write that a later
  failure rolls back (at least once, as before groups). A drop of a retired build's tables runs
  on the supervisor's thread, so queries wait for it.

**Plan refinements** (recorded in the implementation plan): M5: flow control per subscription
(F93), with the delivery across nodes; the sweep of retired builds (F116) must not drop a build
another node still serves during a rolling upgrade, so it waits for the registry to know every
node's build; the protocol messages that carry functions (`ResetForRebuild`) stay node-local, as
the supervisor and its instances are one unit (D53). M6: an organization's targeted rebuild needs
its aggregates' spaces (refused otherwise, F112). M7: the migration guide carries H9 with the
removed members of step 6.

**Looking ahead:**
- *M5 cluster*: the supervisor, its instances and its builder are one unit, the projection's
  singleton; the bus's subscriber removal with or without the outbox (F110) is what a node
  leaving needs too. The live groups and the builder's chunks are per instance, so they stay
  as they are across nodes.
- *M6*: imports and purges work by organization through the targeted rebuild, which now lists
  the target where it resets it; a purge of an organization clears its documents in every build
  the registry lists.
- *Watch*: listeners that make their work before returning it, and listeners with side effects,
  are the two shapes the runtime now handles explicitly (F109, F114); a new listener kind must
  keep both.

### Review 7, 17/10/2026: closing M5 (architecture description, plan and code)

Scope: the cluster as built (sharded owners D43, D72; serialization D45, D68; delivery across
nodes and the singletons D44, D46, D53–D55; ids D39; rolling upgrades D65–D73), the high-load
work (`docs/SCALING.md`: F121–F127, flow control F93), metrics (R19), the Scala 3 groundwork
(D75), `docs/ARCHITECTURE.md` §5, §13 and §14, and the remaining milestones. Inputs: the bench
(`baseline-M5.md` against `baseline-M4.md`, with the M5 scenarios `load-M5`, `large-M5`,
`flow-M5` and `heavy-M5`), two read-only audits by separate agents (delivery and flow control;
the cluster), the full suites run normally and as a cluster of one, and our own reading. Every
finding was checked in the code, and every fix has a test that failed against the code before
it, or under a mutation that restores the old code.

**Outcome.** The M5 decisions hold. Refinements:
- I1 (one writer per aggregate) holds for sharded types. A `Local` type has an owner on every
  node that serves it; the store's version check decides between their writes, and since F131
  each compares its version with the store's before it answers what must be current. Types read
  often in a cluster of several nodes are better `Sharded`; a change of placement or
  `numberOfShards` makes two owners per aggregate while old and new nodes run, so it needs a full
  stop.
- Delivery: a message holds consecutive versions only (F128); a bus asks a moved singleton every
  tick until it answers (F129); flow control keeps half the window for pulls while aggregates are
  behind, and counts what pulls asked for (F130). The window is one bus's, so a projection's
  mailbox can hold one window per node (documented).
- Rolling upgrades: `Projections.retire` waits for the older nodes (F132); a permanent delete's
  detach events pass the write switch (F134); nothing new is written before a node knows its
  cluster (F138); a field added to an application class must be an `Option` (F135, H10 and D68
  corrected).
- Sagas: callers are stored with their node's address (F137); answers are at least once (F136,
  accepted and documented).
- The builder and the poller: ranges read by version start at the squash point (F139), runs and
  chunks are bounded and a range restores its state once (F140), retries of given-up aggregates
  take at most half a poll (F141); event sizes are UTF-8 bytes on both paths (F142).

**Fixed now:** F121–F123 and F125–F127 (the high-load analysis), F93's flow control, and
F128–F142 (the audits: F128, F130, F139–F142 from the delivery audit, F129, F131–F138 from the
cluster audit), except F136, accepted. Two test faults found by the full runs: an assertion of
the default dispatcher left from before F127, and a start-up test that a tick could ask twice.

**Deferred, with reasons:** AR5 (saga definitions instead of subclassing) to M7: the runner
already runs as a singleton and answers across nodes, and the change of API belongs with the
migration guide for Neula's sagas. The docker-compose verification (a killed writer, a
partition, a rebalance with a client ledger) to M7's E2: the machine has no Docker. F124 stays
in M6.

**Measured** (`baseline-M5.md`; single runs on one machine, against `baseline-M4.md`):
- Commands without subscribers: 9 123/s in the baseline run and 9 801/s in a re-run; M4's code,
  run the same day from its tag, gave 9 788/s (`baseline-M4`: 10 691/s). The difference is the
  machine's, not the code's.
- Commands with two projections: 5 250/s (M4: 4 780/s), p99 7.2 ms (9.7 ms), max 16.8 ms (306.5
  ms); the projections keep pace at 2 788 events/s, 0.5 s behind; 3.3 statements per command.
- One contended aggregate (F99): 1 753 commands/s (M4: 1 133/s), p99 13.6 ms (339 ms), max 15.6 ms
  (613 ms). Serial mode: 10 698/s, and 2 031/s on one aggregate (M4: 11 176 and 2 384).
- Rebuild: 112 211 events in 17.1 s, 6 544 events/s, peak heap 155 MB (M4: 16.6 s, 6 769/s,
  150 MB).
- Restores as in M4: 500 000 events in 3.4 s in the full run and 2.07 s in the re-run (M4's code
  the same day: 2.12 s; a full run's earlier scenarios fill the database); from a snapshot 1–6
  ms; one statement more, the history generation (F119).
- Memory soak: 695 806 commands in 60 s (M4: 475 403), projected 97 s after the load (M4: 201 s),
  the heap right after the load 136 MB (M4: 177 MB) and back at 23 MB: flow control keeps the
  projections' backlog in the store (F93), as `flow-M5` showed.
- Delivery soak, 1 000 000 events, a tenth of the listeners failing for 30 s: every event
  projected 257 s after the load started (M4: 330 s); no delivery waiting, an empty outbox, no
  recorded failure; 26 MB after each of 5 restarts. Right after the projections caught up the
  heap held 214–216 MB (M4: 44 MB): likely the 100 000 counters' owners, which the idle sweep
  (every 5 minutes) had not yet stopped after 4.3 minutes, where M4's run took 5.5 (not
  verified).
- Startup: the first command after 668 ms and the first projected event after 767 ms (M4: 502 and
  560 ms; M4's code the same day: 574 and 660 ms). The extra 100 ms or so comes from the cluster
  libraries on the classpath, whose configuration and serializers load at every start: M4's code
  with only those libraries added took 730 and 783 ms, M5's first commit without the schema lock
  655 and 713 ms. Every deployment is a cluster (D42), so it stays.

**Observations:**
- Both audits found problems where M5 met an assumption of "one": one writer per aggregate for
  `Local` types (F131), one ask of a moved singleton (F129), one window per subscriber (F130's
  per-node note), one squash point read when a range starts (F139). The rule now: what the code
  assumes to be one is checked against the cluster, where a node, a version or an owner can be
  two.
- The full suites found what the specs run per work item did not: F127's commit ran only its new
  spec, and an older assertion of the default dispatcher failed from then on. Each work item now
  ends with the full suites, in both modes.
- The baseline needed M4's code run the same day: the machine's state moved the numbers by up to
  9% (commands without subscribers), and one 500 000-event restore took 3.4 s once and 2.07 s the
  next time. A difference beyond the noise is now checked against the previous milestone's code
  run the same day before it is called a regression (`docs/perf/README.md`).
- Flow control, the parts by bytes and the frame limit (F121, F126, F93) bound memory by
  configuration; what is still unbounded is documented in `docs/SCALING.md` §9 (commands in
  flight, `GetEventsForAggregate` answers).

**Plan refinements** (recorded in the implementation plan): M6: purges, imports and the complete
rewrite in a cluster (owners on other nodes, running once, the write switch), and F124. M7: E2
carries the Docker part of M5's verification; AR5 with the saga migration; a multi-node harness
for hosts in the test kit (R20); the runbook carries H10 and the cluster's operations notes.

**Looking ahead:**
- *M6*: organizations are purged, exported and imported while the cluster runs; each operation
  runs once, writes new event classes through the switch, and leaves `Local` owners to catch up
  and sharded ones to be stopped through their shards.
- *M7*: the guide explains the placement choice (`Local` or `Sharded`), the per-node window, the
  at-least-once saga answers and the H10 steps, with the cluster example (E2).
- *Watch*: a new kind of message between nodes must fit the frame (F121) and keep consecutive
  versions (F128); a new background job must run once (a singleton or the schema lock) and
  respect the write switch.
