# ReactiveCQRS — decisions and known issues

The single backlog for the 1.0 line: the decisions that steer the work, the invariants, the open
problems and the architecture reviews; the closed problems are in [`KNOWN-ISSUES-ARCHIVE.md`](KNOWN-ISSUES-ARCHIVE.md).
`docs/ARCHITECTURE.md` describes how the library works; `docs/IMPLEMENTATION-PLAN.md` orders the work
into milestones.

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

Every issue starts as *likely* or *suspected* and becomes *confirmed* when its reproducing test
fails. Line numbers are from the commit an issue was recorded at (the first ones from `4b5060f`).
Each issue is fixed in the milestone where its code is reworked anyway (D84).

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
| D81 | 0.12.47 → 1.0 is a one-off planned stop-and-migrate (0.12 nodes cannot join a 1.0 cluster). Rehearsed on a production copy with measured duration; outbox, saga and subscription rows carried over; heavy jobs (space backfill, mpjsons dry run) done online beforehand (**refined 19/10/2026:** the space backfill runs after the first start, F336; the mpjsons dry run before); changed projections shadow-rebuilt after start. Rolling upgrades apply from 1.0 on. |
| D82 | Work is ordered in milestones M1–M6 (see the implementation plan, which adds M7 for documentation and release). Neula stage-1 needs (R1–R4, R7–R10, R12, R13) are complete at M3. |
| D94 | (24/10/2026, the owner) The library depends on the mpjsons Neula runs it with, 0.7.8 (on Neula's Nexus), instead of only testing against it. mpjsons 0.7.6 and later need Java 25 at run time (its field accessor generators are Java 25 classes), so every runtime of the library needs JDK 25; the library's own classes stay Java 21 bytecode (D74). The data gate of D78 applies again: `StoredFormatsSpec` unchanged, then `StoredDataCheck` on a production copy (Neula's step). Refines D74. |
| D95 | (26/10/2026, the owner) The event keys of F124 are two columns of `events`, `type_id` and `space_id` (nullable, so adding them rewrites nothing), indexed `(type_id, id)` and `(space_id, type_id, id)`, instead of the table `event_keys` of M6 (option 1 of `docs/design/M6-organizations.md`, rejected then under D28): a build reads a page's ids and sizes in one index scan per type, without a second lookup per event, and the table, its cascade and its 4 GB per 100 million events go. The procedures write the keys in the event's row (procedures version 3). `backfillEventKeys` updates the older rows in id batches, a one-time rewrite of each (the table and its indexes grow until VACUUM), and the keys' indexes are built after it (by the event keys' online job, or by `buildIndexesConcurrently` once the step is recorded), so the backfill does not maintain them; builds read by the keys once the step is recorded and both indexes are valid (`buildReadiness().eventKeys`). A store of 1.0.0–1.0.4 (never in production, Neula goes from 0.12.47) loses its `event_keys` table at start, with the step and the `indexes` and `event-keys` online jobs, which run again. No public name changes; `StoredDataCheck.spaceRows` counts `events` in place of `event_keys`. |
| D74 | Library baseline: Scala 2.13 (latest), Pekko 1.7.x, ScalikeJDBC 4.3.x, PostgreSQL JDBC 42.7.x (latest), mpjsons 0.7.0 (0.7.8 since D94, which needs a Java 25 runtime), Java 21 bytecode; Pekko Management (cluster bootstrap, Kubernetes lease, health checks) for the cluster. |
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
| D87 | (18/10/2026, F187, F188, F255) The event bus does not wait forever for its subscribers: one that has not answered within `DeliveryOptions.registrationTimeout` (2 minutes) or stops before it answered is left *unanswered*, and the bus starts, with an error naming it. While any is unanswered, no delivery is complete, so every event keeps its outbox row; the bus keeps the aggregates published (at most 100 000) and has their owners re-publish once the last one answered or was removed. Publications are never stashed. A release that adds a singleton thus costs a rollout up to the timeout per node, instead of a hang; Neula may lower it. Rejected: telling proxies from other subscribers while older versions run (the bus cannot tell them apart), and starting at once without the stopped ones (their events would be deleted unseen). |

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
| D86 | (17/10/2026, the owner's decision on AR5) A saga is a `SagaDefinition` (name, steps, reverts, optionally its organization and write retries), data and functions with what its steps need taken in its constructor, run by the library's `SagaRunner` (`props`, or `start` as a singleton). `SagaActor` stays in 1.0, deprecated, on the same state machine, so sagas written for 0.12 run unchanged and move one at a time under the same `name`; it goes after 1.0. **Done in M7.** |
| D85 | (16/10/2026, at the user's request) Every squash event is checked before it is stored: the event handlers apply it to the initial state as a restore will (by the user it is stored by, at the time of the event it replaces), and the result must equal (`==`) the state it replaces; otherwise the squash is refused, and so is a permanent delete that would detach a duplicate with it (F76). A squash event class that mixes in `SquashMayChangeState` may differ on purpose: it is not compared (its handler must still apply it), it takes every snapshot of its aggregate and of the duplicates reading it, and the owners of the aggregate and of all its duplicates read their states again (F101); projections are not told (D51). |
| D32 | Metadata envelope (R13): a new nullable JSONB `metadata` column on `events`; old rows get a default envelope built from `user_id` on read; commands supply metadata through a trait; `EventInfo` carries it; aggregate event handlers keep their signature. |
| D33 | Outbox hook (R12): a typed writer per context, `(aggregateId, version, events, session) => Unit`, called inside the persist transaction; a failure rolls back the events; idempotent replays never call it twice. |
| D17 | History rewrite is safe by contract: a rewritten event must keep its class; the in-memory aggregate is rebuilt from the store after a rewrite; rewrite + compensating event stays the documented way to notify projections. |
| D76 | A complete history rewrite (R15) also covers squash events, performance snapshots, outbox-hook rows on request, duplicates (automatically while attached, through recorded lineage when detached), triggers a targeted rebuild of affected projections for the aggregate and its duplicates, and writes an audit entry (who, when, which classes and fields, no values). **Done in M6 (17/10/2026):** a `HistoryChange` applied by the context's `historyChanges` (and `metadataChanges`) in the owner (`RewriteEverywhere`) to the aggregate's own events and archive, with the host's `OutboxWriter.rewrite`, an audit row in `history_rewrites`, snapshots dropped through the history generation; `HistoryRewriter` follows the copies (chain rows, and `aggregate_lineage`, which every detach now writes and an export carries) and rebuilds the projections given, one aggregate at a time; ARCHITECTURE §4.9. |
| D77 | Ephemeral aggregates (R16): the same context running only in memory on one node, never written, not published, negative ids; `Materialize` persists one squash-style first event under a new positive id returned to the caller; loss on node failure is reported. **Done in M6 (17/10/2026):** `AggregateCommandBusActor.ephemeral(...)` with an `EphemeralEventStoreState` and an event bus of its own; ids `-(token × 2^24 + n)` with a random token per start (|id| below 2^53, D39); `EphemeralAggregateLost` and `EphemeralAggregateUnknown`; `Materialize` through an idempotent `StoreFirstEvent`, commands rejected meanwhile; `maxAggregates` and an idle time no shorter than the command timeout; ARCHITECTURE §4.10. |
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
| D88 | (21/10/2026, Neula's MR-11, CR-1) Orphan events (an aggregate's events without its own `aggregates` row) are reported, never deleted or rewritten by the library: by `backfillEventKeys` (`EventKeysBackfill.orphans`) and `StoredDataCheck` (`Report.orphans`). They do not keep the event keys' step from being recorded, as no read path reads them; the operator decides what happens to them (chapter 18). |
| D93 | (23/10/2026, Neula's CR-8) A build stays one actor, and what it delivers does not depend on how many threads made it (D59–D62 unchanged). A final-state build restores a page's states on `BuilderSettings.restoreThreads` threads of its own (default 1). A commit-order replay stays on one thread: Neula's measurements put the build at about 380 µs of CPU per event against about 53 µs for the same handlers and parsing in `RestoreCheck`, so its time goes elsewhere first (blocking lookups in the projection, the projection's own work); `BuildStatus` shows reading, replaying and waiting time, and parallel replay (CR-8 A) is taken up only once those show replay bounding a build. A parallel apply of a build's chunks (CR-8 C), if taken up, goes through `ProjectionActorOptions.parallelUpdateProcessing`, which declares thread-safe listeners already, not a second option. |
| D92 | (22/10/2026, Neula's MR-11, CR-6) An aggregate's versions are committed in order: each command's events are written in one transaction by the procedures, which compare and move the aggregate's own row's `base_version` under its row lock, so version N+1 commits only after N did. A read by version that sees a stored version above N (in one statement) therefore sees every committed version below it: when N is not there and is not below the squash point (read again after the part), N is missing from the store, never late, and no wait is needed. Only stores changed by hand break the rule; builds fail such an aggregate visibly, and an operator moves past it (`SkipFailedVersion(pastMissing = true)`) or skips it (CR-4's decision stands: nothing is skipped automatically). |
| D91 | (21/10/2026, Neula's MR-11, CR-3) The online jobs may run inside the application, in the background after it started: `OnlineJobs.start` (opt-in) runs the indexes, the event keys (once no member runs an older app-version) and the space backfill in order, resumable from `online_jobs`, one runner at a time (a singleton and an advisory lock), its long statements on a connection of its own; full builds follow it through `BuildHold.untilStoreReady`, an operator may release them for good (`releaseBuildHold()`). Consistent with D69 (heavy migrations stay out of start-up initialization: they run after it, online). Supersedes Neula's owner decision 7 ("a run mode with a script, not an endpoint") for Neula's upgrade; calling the jobs by hand stays supported. |
| D90 | (21/10/2026, Neula's MR-11, CR-4; the owner's decision) Aggregates that cannot be restored are recorded and reported (`restore_failures`, `restoreFailures`, a gauge, a warning), so an operator removes them by hand (chapter 18) or fixes the code; nothing is skipped, deleted or version-bumped automatically. An operator may skip a whole aggregate in one build (`SkipAggregate`), which the record shows. |
| D89 | (21/10/2026, Neula's MR-11, CR-2) Full builds can be held until a condition holds: `ProjectionsSettings.holdBuilds` (none by default), checked on the blocking dispatcher while a build waits; `BuildHold.untilStoreReady` holds them until the store is ready for builds (`buildReadiness`: the event keys' step and a valid `aggregates_own_type_idx`). A hold touches neither adoption nor serving; targeted rebuilds and the F202 catch-up are not held. Once released, a `Projections` does not hold again. |

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

The issues still open. The 279 closed ones (fixed, accepted, decided or found not to be a problem) are in
[`KNOWN-ISSUES-ARCHIVE.md`](KNOWN-ISSUES-ARCHIVE.md), by area, as they were recorded; a number named in the code or in
the other documents (`F99`) is found there. F185–F274 were found by the risk assessment of 1.0.0 (18/10/2026), and F275–F335 by
the second one, of 1.0.1-SNAPSHOT ([RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md)), at the end of this section.

### Performance and runtime

#### F184 — A projection rebuild may be 5–8 % slower than at M6
- **Low** (performance), suspected (measured, within the bench's spread) · Open: profile after the release · `ProjectionActor`, `ProjectionBuilder`
- **Symptom:** the bench's rebuild of 112 211 events, 18/10/2026, JDK 25, alternating runs: M7 5 732 and 6 071 events/s, the M6 tag 6 213 and 6 375 events/s (`docs/perf/baseline-M7.md`, `rebuild-check-M7.md`, `m6-on-jdk25.md`, `rebuild-check-M6.md`). The command scenarios of the same runs spread by about ±7 % from run to run, so the gap may be noise; both pairs point the same way.
- **Candidates:** the build's code barely changed in M7: a projection applies each chunk inside the after-commit wrapper of F161 (a thread-local and a buffer per transaction) and counts applied updates for F163; the state-only build walks the aggregates with F173's query, whose index the bench does not build (the events-based build, which the bench times, does not walk).
- **Found:** the bench of the M7 review (review 9).

### Serialization (mpjsons 0.7)

#### F52 — JDK value types stored in the old private-field form cannot be read by 0.7
- **High** if present, suspected · **Waits for the rehearsal** on a copy of production (Neula's, D78; the runbook's checks)
- **Root cause:** mpjsons `docs/upgrading.md:30`; Neula's 2022 release may have run on JDK 8.
- **Fix:** register converters reading the old form, if the dry run finds such rows.
- **Tool (16/10/2026):** `io.reactivecqrs.core.tools.StoredDataCheck.run(mpjsons, typesNamesState, projectionTables)` reads every stored JSON value (events, sagas, idempotent responses, the projection tables given) with the mpjsons on the classpath and reports rows and unreadable rows per table and class, with the first failures; it also reports the F49 count and the D29 distribution. Neula runs it from its application against a production copy. Test: `StoredDataCheckSpec`.

#### F53 — An absent `Option` field reads as `None` instead of `null`
- **Medium**, suspected · **Waits for the rehearsal** on a copy of production (Neula's, D78) · mpjsons `docs/upgrading.md:31`; see H4.

### Code structure

#### F179 — mpjsons 0.7.0 is in no public repository
- **Medium** (the release and every build away from this machine), confirmed · **Deferred by the owner** (18/10/2026: the owner publishes mpjsons 0.7.0 later; until then the build resolves it from the local Maven repository of a machine that has it) · `project/Common.scala` (`resolvers`)
- **Symptom:** the build resolves mpjsons 0.7.0 from this machine's local Maven repository (`~/.m2`). No resolver of the build has it: Maven Central has no mpjsons at all (404, 18/10/2026), Bintray closed in 2021, and the old Sonatype releases repository only redirects to a path that does not exist. A fresh machine (a CI runner, a contributor, a Docker build) cannot build the library, and an application that depends on ReactiveCQRS 1.0 must get mpjsons 0.7.0 from somewhere. E2's image avoids it by copying the jars the host resolved (`examples/stageE2`).
- **Options:** publish mpjsons 0.7.0 to Maven Central; or to Neula's Nexus, where ReactiveCQRS is published, and add that Nexus to the build's resolvers (with credentials); in both cases drop the two dead resolvers (Bintray, the old Sonatype one).
- **Since 24/10/2026:** mpjsons 0.7.8, the version the build uses (D94), is on Neula's Nexus (`neula-releases`); the build still resolves it from `~/.m2`, as Nexus needs credentials the build does not carry.
- **Found:** running E2 with Docker (M7, 18/10/2026).

### Found by the risk assessment of 18/10/2026

Ninety issues found by a deep reading of 1.0.0 for failure scenarios: [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md) holds each
one's scenario, mechanism, validation, fix and regression test under the same number, the compound failures they make
together, and the order to fix them in. None is reproduced yet: "likely" is traced in the code, "suspected" also depends
on a dependency's behaviour, timing or configuration; "scenario" says how probable its trigger is.

#### F215 — Operations on a base with many copies replay the base once per copy and hold every copy's row lock throughout
- **Medium**, suspected · scenario possible · Open; examined 18/10/2026 and left: caching the base's state once per base version must still apply each copy's own duplication event, and locking the copies `FOR KEY SHARE` needs the SQL procedures changed; both are more than step 4 should carry · details under F215 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F217 — A large targeted rebuild, a purge's last step, or a retired build's drop each runs as one long operation on the projection's thread
- **Medium**, likely · scenario possible · Open; partly fixed for 1.0.1 (18/10/2026): a retired build's subscription rows are deleted in transactions of 10 000, and document tables get an index on `space_id` (new ones at once, older ones by `PostgresDocumentStore.buildSpaceIndexesConcurrently()`). Left: the targeted reset still reads its ids and clears in one transaction, and the drop runs on the supervisor's thread · details under F217 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F238 — A node's subscription version cache goes stale for subscribers whose writer runs elsewhere
- **Medium**, likely · scenario unlikely · Open; examined 18/10/2026 and left: it needs per-node subscribers sharing a node's subscription state, which no host does · details under F238 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F239 — Ask paths are reused after a node restarts at the same address, so a late answer can reach another caller
- **Low**, likely · scenario unlikely · Open; examined 18/10/2026 and left: low, and a late answer needs a restart at the same address within an ask · details under F239 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F243 — A conflict answer reveals another organization's aggregate and its version
- **Low**, likely · scenario possible · Open; examined 18/10/2026 and left: low; a host that exposes conflicts to other organizations' users maps the answer · details under F243 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F270 — Clocks and time zones: node clocks drive re-delivery, give-ups, instant reads and the squash policy, and several timestamps are written in the session's zone
- **Medium**, likely · scenario possible · Open; documented 18/10/2026 (chapter 18: the JVM on UTC, clocks in sync); left: bind every timestamp as UTC, take `event_time` from the database, monotonic deadlines · details under F270 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F271 — New application message classes are dropped by old nodes during a rolling upgrade
- **Medium**, likely · scenario possible · Open; documented 18/10/2026 (chapter 17, ARCHITECTURE §13.6: add a message class in one release, send it in the next); left: refusing such messages while older nodes run · details under F271 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

#### F274 — The memory event store diverges from PostgreSQL exactly where several of these findings live
- **Medium**, likely · scenario likely · Open; examined 18/10/2026: the rollback (F256) and the sort and paging of the memory document store (F273) are aligned; the rest stays a test double's gap, and the specs of those findings run on PostgreSQL · details under F274 in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md)

### Found by the second risk assessment of 18/10/2026

#### F276 — The snapshot shape ignores subclasses held through a trait or abstract class: a field added to one is restored as zero, false or null
- **High**, likely · scenario possible · Open; continues F196 · details under F276 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([OWN-3](risk-assessment-2/owner.md))

#### F280 — An erasure sent to another type's bus runs with the wrong context and is reported complete
- **High**, likely · scenario unlikely · Open; continues F197, F245 · details under F280 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([BUS-5](risk-assessment-2/command-bus.md))

#### F281 — A targeted rebuild during a build that started with no active build, under `ServeStale`, makes the only instance acknowledge every live event unapplied
- **High**, likely · scenario unlikely · Open; continues F192 · details under F281 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([RBLD-1](risk-assessment-2/rebuilds.md))

#### F283 — A build's queued targeted rebuild that fails at its reset is dropped, and the build switches in with the erased values
- **High**, likely · scenario unlikely · Open; continues F192 · details under F283 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([RBLD-4](risk-assessment-2/rebuilds.md))

#### F284 — A saga's compare-and-set miss after its own write whose acknowledgement was lost stops the saga; its caller is never answered
- **High**, likely · scenario unlikely · Open; continues F206, F232 · details under F284 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([CLSG-1](risk-assessment-2/cluster-and-sagas.md))

#### F286 — A step's retries span about 15–40 s: a longer failover reverts a step whose command committed, and a revert in the same outage gives up silently
- **High**, likely · scenario unlikely · Open; continues F205 · details under F286 in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §3 and the notes ([CLSG-3](risk-assessment-2/cluster-and-sagas.md))

#### F288 — A non-success answer to a materialization, an unknown outcome included, unfreezes the draft, so F208's lost edits come back
- **Medium**, likely · scenario unlikely · Open; continues F208, F253 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([OWN-2](risk-assessment-2/owner.md), [BUS-1](risk-assessment-2/command-bus.md))

#### F289 — A plain squash keeps the history generation: stale owners and snapshots below the squash point are carried forward through the squash event
- **Medium**, likely · scenario possible · Open; continues F214, F237 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([OWN-4](risk-assessment-2/owner.md))

#### F290 — Conflicts with another node and transient failures replay the whole aggregate; a pool timeout doubles the wait
- **Medium**, likely · scenario possible · Open; continues F214, F253 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([OWN-5](risk-assessment-2/owner.md))

#### F291 — F250's containment covers handlers and restores only: a `StackOverflowError` elsewhere in the owner can end the JVM
- **Medium**, likely · scenario unlikely · Open; continues F250 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([OWN-6](risk-assessment-2/owner.md))

#### F292 — A materialization that succeeds after its timeout tells nobody, and the new id cannot be found
- **Medium**, likely · scenario possible · Open; continues F208 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([BUS-3](risk-assessment-2/command-bus.md))

#### F293 — Event-history and archive reads are not type-checked: another type's events and envelopes are returned
- **Medium**, likely · scenario possible · Open; continues F197 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([BUS-4](risk-assessment-2/command-bus.md), [STORE-4](risk-assessment-2/event-store.md))

#### F294 — A passivation during a materialization makes the bus hold every message for the draft, unanswered and unbounded
- **Medium**, likely · scenario possible · Open; continues F208 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([BUS-6](risk-assessment-2/command-bus.md))

#### F295 — A sequence step lowered after a pool was drawn still gives the same ids twice
- **Medium**, likely · scenario unlikely · Open; continues F204 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([BUS-7](risk-assessment-2/command-bus.md))

#### F296 — The event size limit applies to the detach and squash events the library writes: a base whose copy is larger cannot be deleted for good or purged
- **Medium**, likely · scenario possible · Open; continues F244 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([STORE-3](risk-assessment-2/event-store.md))

#### F297 — An erasure of an unreadable event rewrites its JSON but not its metadata envelope, and reports it rewritten
- **Medium**, likely · scenario unlikely · Open; continues F245 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([STORE-5](risk-assessment-2/event-store.md))

#### F298 — Lost messages keep a subscriber's window shut for good when its traffic is on aggregates held behind
- **Medium**, suspected · scenario unlikely · Open; continues F221 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DLV-1](risk-assessment-2/delivery.md))

#### F299 — Kept aggregates are re-published at 200 a second per node, outside the poller's budget, when a late singleton answers
- **Medium**, likely · scenario possible · Open; continues F188 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DLV-2](risk-assessment-2/delivery.md), [SYS-7](risk-assessment-2/cross-cutting.md))

#### F300 — A slow outbox read is abandoned after `readTimeout` and started again: a large outbox is never polled, and its scans pile up
- **Medium**, likely · scenario unlikely · Open; continues F186, F260; **examined for 1.0.1-RC2 (21/10/2026): not needed for Neula's upgrade** — it needs an outbox whose read outlasts `readTimeout` (5 minutes: millions of waiting aggregates); the rehearsal on `neula_cloud` found the 0.12 outbox empty, and the runbook drains it before the stop. Revisit if an installation's leftover outbox (`SELECT count(DISTINCT aggregate_id) FROM events_to_publish`) runs into the hundreds of thousands · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DLV-3](risk-assessment-2/delivery.md))

#### F301 — `Errors.transient` misclassifies: statement timeouts and other repeating errors are retried for ever without a record; a read-only node records failures for healthy listeners
- **Medium**, suspected · scenario possible · Open; continues F254 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([PROJ-1](risk-assessment-2/projections.md), [DOCS-6](risk-assessment-2/document-store.md), [SYS-9](risk-assessment-2/cross-cutting.md))

#### F302 — F259 applies live events of an aggregate at subscription version 0 as history, so pushes and side effects skip them
- **Medium**, likely · scenario possible · Open; continues F259 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([PROJ-2](risk-assessment-2/projections.md))

#### F303 — A group that fails on the database is run again one batch at a time, each waiting out the pool on the projection's thread
- **Medium**, likely · scenario possible · Open; continues F254, D61 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([PROJ-3](risk-assessment-2/projections.md))

#### F305 — The batched delete of a retired build's subscription rows rescans what it deleted: quadratic while vacuum is held back
- **Medium**, suspected · scenario possible · Open; continues F217 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([PROJ-5](risk-assessment-2/projections.md), [RBLD-7](risk-assessment-2/rebuilds.md))

#### F306 — Offset paging has no stable order on PostgreSQL (no tie-break, none at all unsorted); the memory store's order hides it
- **Medium**, suspected · scenario likely · Open; continues F211, F273 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-2](risk-assessment-2/document-store.md))

#### F307 — F211's cap counts rows only after the driver has buffered and mpjsons has read up to 100 001 documents
- **Medium**, likely · scenario possible · Open; continues F211 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-3](risk-assessment-2/document-store.md))

#### F308 — A read's deferred cache write overwrites a newer absence or presence: deleted documents come back, inserts fail until a restart
- **Medium**, likely · scenario unlikely · Open; continues F266 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-4](risk-assessment-2/document-store.md))

#### F310 — Retiring a projection drops its tables under the schema lock with no lock timeout
- **Medium**, likely · scenario unlikely · Open; continues F185; **examined for 1.0.1-RC2 (21/10/2026): not needed** — it needs `Projections.retire`, and Neula 2.2 retires no projection (MR-11 makes its last 12 projections managed: they are adopted as build 0 and rebuilt into new tables; the switch's drop of a replaced build does not run under the schema lock). Revisit with the first release that retires a projection · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-7](risk-assessment-2/document-store.md))

#### F311 — After a rollback, aggregates deleted for good during a `ServeStale` build keep their documents in the active build
- **Medium**, likely · scenario unlikely · Open; continues F202 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([RBLD-3](risk-assessment-2/rebuilds.md))

#### F313 — A purge is not refused while the F202 catch-up runs, whose chunks can write a purged organization's documents back
- **Medium**, likely · scenario unlikely · Open; continues F202 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([RBLD-6](risk-assessment-2/rebuilds.md))

#### F315 — Finished keyed sagas' rows stay 30 days with their last order: the table grows, every load reads them, erasure does not reach them
- **Medium**, likely · scenario possible · Open; continues F207 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([CLSG-5](risk-assessment-2/cluster-and-sagas.md))

#### F317 — F227 stops early only for library values: one large application value is still serialized whole
- **Medium**, likely · scenario possible · Open; continues F227 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([CLSG-8](risk-assessment-2/cluster-and-sagas.md))

#### F318 — A keyed saga that failed keeps its failure as the answer for 30 days, so a natural key cannot be retried
- **Medium**, likely · scenario possible · Open; continues F207 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([CLSG-9](risk-assessment-2/cluster-and-sagas.md))

#### F319 — The export replays every detached copy's base inside its open snapshot, which can outlast the book's idle-in-transaction timeout
- **Medium**, suspected · scenario possible · Open; continues F225, F215 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-1](risk-assessment-2/data-lifecycle.md))

#### F320 — F248's range check reads a NULL `last_value` for a sequence never drawn from, and protects installation 0's range instead
- **Medium**, suspected · scenario unlikely · Open; continues F248 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-2](risk-assessment-2/data-lifecycle.md))

#### F321 — An import remaps user ids through the aggregate-id map
- **Medium**, likely · scenario possible · Open; continues F248 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-3](risk-assessment-2/data-lifecycle.md))

#### F322 — References to aggregates that were not exported are accepted when the target has that id, whatever its organization
- **Medium**, likely · scenario unlikely · Open · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-4](risk-assessment-2/data-lifecycle.md))

#### F323 — The import logs its whole report, which since F248 can hold every aggregate
- **Medium**, likely · scenario possible · Open; continues F191, F248 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-5](risk-assessment-2/data-lifecycle.md))

#### F324 — A permanent delete or purge deletes the lineage it has just written: a later erasure of the base reports complete while the copies keep the values
- **Medium**, likely · scenario unlikely · Open; continues F193 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-6](risk-assessment-2/data-lifecycle.md))

#### F325 — Nothing detects writes or erasures made in the source between an export and its purge
- **Medium**, likely · scenario possible · Open · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-7](risk-assessment-2/data-lifecycle.md))

#### F326 — A rewrite refuses any change to an undo or duplication event, so a value in one cannot be erased
- **Medium**, likely · scenario unlikely · Open · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([LIFE-8](risk-assessment-2/data-lifecycle.md))

#### F327 — `Ddl`'s retry loop stalls the table's traffic cluster-wide for about 90 s behind a long lock; the book says other nodes keep working
- **Medium**, suspected · scenario possible · Open; continues F185 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([SYS-3](risk-assessment-2/cross-cutting.md))

#### F328 — A document index built at start is bounded only by `socketTimeout`: a build over 10 minutes is retried at every start
- **Medium**, suspected · scenario unlikely · Open; continues F186, F185; **examined for 1.0.1-RC2 (21/10/2026): not needed for Neula 2.2** — it needs a new index declared on a large table that already exists; the rehearsal's first 1.0 start on `neula_cloud` dropped and rebuilt no document index (3 633 before, none changed), and MR-11's version bumps build into new tables, whose indexes are made while they are empty. The condition to check in 2.2's rehearsal is the same index diff (`pg_indexes` before and after the first start): any index built on an adopted table there would take this path · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([SYS-4](risk-assessment-2/cross-cutting.md))

#### F329 — Every command of a sharded type is serialized whole on the bus's thread to be measured
- **Low**, likely · scenario possible · Open; continues F227 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([BUS-8](risk-assessment-2/command-bus.md))

#### F330 — `InFlight` keeps an entry per send for `stallAfter`, also for deliveries answered at once
- **Low**, likely · scenario likely · Open; continues F212 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DLV-4](risk-assessment-2/delivery.md))

#### F331 — The state of every publication to a state listener on another node is measured on the bus's only thread
- **Low**, suspected · scenario possible · Open; continues F222 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DLV-5](risk-assessment-2/delivery.md))

#### F332 — `Ddl.has` follows the search path and counts an invalid index as present
- **Low**, likely · scenario unlikely · Open; continues F185 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-8](risk-assessment-2/document-store.md))

#### F333 — `buildSpaceIndexesConcurrently` takes no lock: two runs drop each other's index in progress
- **Low**, suspected · scenario possible · Open; continues F217 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([DOCS-9](risk-assessment-2/document-store.md))

#### F334 — A load can run once more one step of a saga that ended while the load's query ran
- **Low**, likely · scenario unlikely · Open; continues F232 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([CLSG-7](risk-assessment-2/cluster-and-sagas.md))

#### F335 — The `bus.behind` gauge gets a new series for every managed instance
- **Low**, likely · scenario likely · Open; continues F258 · details in [RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §2 and the notes ([SYS-8](risk-assessment-2/cross-cutting.md))

### Found by Neula's MR-11 (21/10/2026)

#### F346 — A state replay that throws fails the aggregate's `EVENTS` subscriptions of the build too
- **Low**, likely · Open: **a question for the owner** · `ProjectionBuilder.itemsOf`
- **Finding:** in the bulk pass, a run whose state cannot be restored becomes an unreadable item for every listener kind of its type, the `EventsListener`s included, which need no state and could apply the run. So a projection that also listens to the type's events fails on the aggregate there too, and its build waits for it.
- **Proposal:** scope the failure to the state kinds (`AGGREGATES`, `AGGREGATES_WITH_EVENTS`) and deliver the run to the `EVENTS` kind. Not changed unasked (CR-4, review addition 4): it changes what a build applies.

### Found by the Neula migration analysis of 19/10/2026

#### F339 — `buildIndexesConcurrently()` may run on the live 0.12 database before the stop
- **Low** (an opportunity), suspected · scenario possible · Open: to prove in the rehearsal on a copy under 0.12 traffic · it builds on 0.12's columns with `CREATE INDEX CONCURRENTLY`, which would take the heaviest index work out of the window (`UPGRADE-0.12-TO-1.0.md` §2); F336–F341 came from the same analysis, the others closed

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
| H12 | Low, likely | M7 host steps (growing with M7): undo events need no case in an aggregate's event handlers any more (F148; a case left in is never called). A projection with two listeners of one kind for one aggregate type now refuses to start, and the bus waits for it (F164): a scan of Neula's code (`neulaserver`, 17/10/2026) found none. A `RegisterSubscribers` after the bus's start now registers its subscribers, with a warning that they missed what was published before (F162). Inside a listener, `findDocumentByPath(expectedValue)` and `findDocumentByPaths(...)` now read in the listener's transaction: they see what the listener wrote before them in the same batch (F160). A permanent delete held by a failed subscription runs once the failure is retried or skipped (F166). Run `buildIndexesConcurrently()` again after the upgrade: it builds the aggregates' own rows by type (F173), without which a build reads a page of aggregates in about 100 ms instead of 1 ms on a large installation. Removed from the API, unused by Neula: `OnConcurrentModification` (`Retry`, `Fail`) and `SimulateEvent` (F157); `EventHandler`, `AbstractEventHandler`, `IdentifiableEvent`, `EventIdentifier`, `EventsIdentifiers`, `EventWithIdentifier`, `AggregateVersion.upTo` and `isJustAfter`, and the deprecated `LRUCache` (F183). `io.reactivecqrs.api.Nothing` is `NoInfo` (F165): a signature such as `CommandSuccess[Cart, api.Nothing]` becomes `CommandSuccess[Cart, NoInfo]`, and the compiler names each place. Sagas run unchanged as `SagaActor`s, deprecated (D86); move each to a `SagaDefinition` under the same `name`, in five changes (the book's chapter 12), at any time after the upgrade. A cluster running a milestone build moves to 1.0.0 with a full stop: 1.0 writes a delivery's events in a compact form (F170). The bus's "Subscriber ... stopped" is an error only when nothing replaces the subscriber (F176): an alert on errors no longer fires at each node's stop or a singleton's move. Every node's configuration binds the library's serializer, `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }` (F178): otherwise, depending on the order of the classpath, messages between nodes fail and the node stays not ready. On Kubernetes, nodes join through Pekko Management's Cluster Bootstrap, not static seed nodes, whose first one can form a second cluster in a rolling restart (F180); the book's chapter 17 and E2 show how. | M7 (Neula) |
| H11 | Medium, likely | M6 host steps (growing with M6): every first event gives the organization's space (`SpaceId(0)` for the platform); one without is refused with `FailureResponse` (D38). Once every node runs M6, run `PostgresEventStoreState.backfillEventKeys()` once in the background, and `buildIndexesConcurrently()` again (it now also builds `aggregates (space_id)`): until the keys are complete, builds of a few types or of one organization walk the whole store (F124); do not roll back below M6 afterwards, as earlier versions write events without keys. Purge (R17): `PostgresCommandResponseState.initSchema()` adds `commands_responses.aggregate_id` and builds its index concurrently (responses stored before have none and expire with the retention), `PostgresSagaState.initSchema()` adds `sagas.space_id`; sagas whose orders belong to an organization override `spaceOf(order)`; `buildIndexesConcurrently()` now builds `aggregates (space_id, id)` and drops the first M6 steps' `aggregates (space_id)`; a purge is `new SpacePurge(eventStore, commandBuses, projections, hooks).purge(space)`, run from one node while the organization is quiet, with this node's command bus of every type the organization has, every projection that holds its documents (each must implement `clearForRebuild` for `OneSpace`; do not rebuild them meanwhile), and a `PurgeHook` for each table of the host's own (an outbox writer's rows); check its `PurgeReport.complete`, run it again if not, and verify with `StoredDataCheck.spaceRows(space)`. Moving an organization (D40): `new SpaceExport(eventStore, contexts).write(space, writer)` from a quiet organization (`contexts`: those of its types; a copy of another organization's aggregate needs its type's squash event), then `new SpaceImport(eventStore, uidGenerator, rewriters, newMPJsons).read(file, into)` into an empty space of the other installation (give `newMPJsons` an instance set up as yours, with your converters; an `ImportRewriter` for each type that holds ids as plain numbers or text), then `RebuildAggregates(OneSpace(into))` on each projection; if the report is not `complete`, purge `into` and import again. Contexts with audit duties name their audit event classes in `SquashPolicy.keep` before they opt in to squash (R11): those events move to `events_archive` and are read with `GetArchivedEvents(id)`; `AutomaticSquash.keepNewerThan` keeps the recent history; `StoredDataCheck.usersOf(className)` lists who still uses a class before it is removed from the code. Ephemeral aggregates (D77): an `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons)` next to the type's bus; route commands for negative ids to it, on the node that made them; `Materialize` needs the type's squash event; answers `EphemeralAggregateLost` and `EphemeralAggregateUnknown` are new `CustomCommandResponse` cases (exhaustive matches on responses warn). Erasure (D76): a `HistoryChange` per kind of erasure, each context's `historyChanges` (and `metadataChanges` where envelopes hold personal data), `OutboxWriter.rewrite` for the host's rows, then `new HistoryRewriter(commandBus, eventStore, projections).rewrite(id, change, userId)` per aggregate holding the value, naming the managed projections that show it (each implements `clearForRebuild` for `OneAggregate`); check `RewriteReport.complete` and its `bases`. | M6 (Neula) |

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
| M6 | F8 ✔ (complete), F124 ✔, F143 ✔, F144 ✔, F145 ✔, F146 ✔, F147 ✔, F149 ✔, F150 ✔, F151 ✔, F154 ✔, F158 ✔, F159 ✔, F167 ✔, F168 ✔, F172 ✔, F174 ✔; F155 decided (review 8) |
| M7 | AR5 ✔ (moved from M5 at review 7; D86); F148 ✔, F152 ✔, F153 ✔, F156 ✔, F157 ✔, F160 ✔, F161 ✔, F162 ✔, F163 ✔, F164 ✔, F165 ✔, F166 ✔, F169 ✔ (found writing the book), F170 ✔, F171 ✔, F173 ✔, F175 (accepted), F176 ✔ (found writing E10), F177 ✔ (found writing E11), F178 ✔ (found running E2 in Docker), F179 (deferred: the owner publishes mpjsons later), F180 ✔ (found running E2 on Kubernetes), F181 ✔ (found writing chapter 6's spec), F182 ✔ (found by E2's fault tests in Docker), F183 ✔ (found writing the Scaladoc), F184 (open, found by review 9's bench) |
| After 1.0 | F185–F274, found by the risk assessment of 18/10/2026 ([RISK-ASSESSMENT.md](RISK-ASSESSMENT.md) §13 orders them); its step 1, for 1.0.1: F185 ✔, F186 ✔, F187 ✔, F188 ✔, F189 ✔, F190 ✔, F191 ✔, F193 ✔, F200 ✔, F252 (keys ✔), F255 ✔, F272 ✔; D87; step 2: F192 ✔, F194 ✔, F195 ✔, F196 ✔, F197 ✔, F198 ✔, F199 ✔, F225 ✔, F226 ✔, F201 ✔, F237 ✔, F244 ✔, F245 ✔, F248 ✔, F254 ✔, F259 ✔, F261 ✔; step 3: F205 ✔, F206 ✔, F207 ✔, F208 ✔, F209 ✔, F232 ✔, F236 ✔, F256 ✔, F257 ✔; step 4: F210 ✔, F211 ✔, F212 ✔, F221 ✔, F222 ✔, F223 ✔, F228 ✔, F258 ✔, F260 ✔, F264 ✔, F268 ✔, F273 ✔; item 15: F213 ✔, F214 ✔, F216 ✔, F218 ✔, F219 ✔, F220 ✔, F229 ✔, F230 ✔, F231 ✔, F234 ✔, F235 ✔, F217 (part), F215 open; item 16: F203 ✔, F204 ✔, F233 ✔, F240 ✔, F247 (part) ✔, F249 ✔, F250 ✔, F269 ✔, F270 and F271 documented, open; after step 4: F224 ✔, F246 ✔, F251 ✔, F252 ✔, F253 ✔, F263 ✔, F265 ✔, F266 ✔, F267 ✔; quick wins: F202 ✔, F227 ✔, F241 ✔, F242 ✔, F262 ✔, F217 (index) ✔; left open with their reasons: F238, F239, F243, F274; F275–F335, found by the second risk assessment of 18/10/2026 ([RISK-ASSESSMENT-2.md](RISK-ASSESSMENT-2.md) §6 orders them); its regressions: F275 ✔, F279 ✔, F282 ✔, F304 ✔, F312 ✔, F314 ✔; before the release: F285 ✔, F287 ✔; the library's prerequisites of Neula's migration: F278 ✔, F309 ✔; F336–F341 from that analysis (F339 open); F277 and F316 closed, 1.0.0 never ran in production; for 1.0.1-RC2, Neula's MR-11: F342 ✔ (CR-1), F343 ✔ (CR-2), F344 ✔ and F345 ✔ (CR-4), F347 ✔ (CR-3), F346 open (a question), D88, D89, D90, D91; for 1.0.2: F348 ✔ (CR-6), F349 ✔ (CR-7), D92; for 1.0.3, Neula's CR-8: F350 ✔ (B), F351 ✔ (A.6), D93; the rest open |

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
- **When:** M7, with the migration guide for Neula's sagas (moved from M5 at review 7: the runner already runs as a singleton and answers across nodes, F137). The owner confirmed on 17/10/2026: now, before 1.0.
- **Done in M7** (17/10/2026, D86): `SagaDefinition` and `SagaRunner`; the state machine is `SagaRunning`, unchanged; `SagaActor` deprecated on it. The caller's path with its node's address was F137's already. `SagaRunnerSpec`: a definition's saga, a `SagaActor` saga, and a saga a `SagaActor` stored resumed under the definition of its name. The book's chapter 12 shows the move in five changes.

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

### Review 8, 17/10/2026: closing M6 (architecture description, plan and code)

Scope: organizations and data lifecycle as built: the required space and the commit-order pass by
type and space (D38, F124), the archive and retention squash (R11, D52), the purge (R17, D56),
export and import (D40), the complete rewrite (D76, R15), ephemeral aggregates (D77, R16);
`docs/ARCHITECTURE.md` §4.6–§4.10; the book (`docs/book/`, 20 chapters) and the README; the short
JSON keys measurement (IMPLEMENTATION-PLAN §7.1). Inputs: the bench (`baseline-M6.md` against
`baseline-M5.md`, and `jsonnames-M6.md`), the full suites in both modes after every work item,
mutation checks of every work item, the book (writing it found F147–F169), and our own reading.

**Outcome.** The M6 decisions hold. Refinements:
- Organizations: every new aggregate has a space (D38); the purge, the export and the targeted
  rebuild of one space all work by space, the commit-order pass reads only the keys of the types
  and space it builds (F124), and its page no longer depends on the planner's estimate (F172).
- The history of an aggregate now has four places, and every operation that changes or moves it
  covers each: the events (with squash events), the archive (R11), the snapshots (through the
  history generation, F119) and the copies (attached through the chain rows, detached through the
  lineage that every detach records, D76). Purge, export and import, the complete rewrite and the
  permanent delete were checked against that list.
- Erasure (D76): data in, the context's function out (`historyChanges`), so a host writes what an
  erasure means once per type; the library reaches what it stores and names what it cannot (other
  aggregates' events, saga orders, stored responses, logs, exported files).
- Ephemeral aggregates (D77) are the usual owners on a store of their own: the same rules, limits
  and answers, nothing written. Their ids changed from the first sketch (start second) to a random
  token per start, as D39 bounds ids at 2^53 for Neula's UI and two nodes may start in one second.
- F155 decided: D85 holds; a squash event carries the users and times its state keeps.

**Fixed in M6:** F8 (complete), F124, F143–F147, F149–F151, F154, F158, F159, F167, F168, F172,
F174. Found and recorded for M7: F148, F152, F153, F156, F157, F160–F166, F169 (the book), F170,
F171, F173 (the measurement), F175 (the close's bench).

**Deferred, with reasons:**
- A test that an imported organization's projections, rebuilt, equal the source's (the plan's
  first verification item): the states of every aggregate are compared through the id remap
  (`ExportImportSpec`), and a projection is a function of the events, which are compared too; a
  test across two schemas with managed projections running asynchronously needs the test kit's
  second-installation support (M7, E-items).
- The docker-compose verification of the cluster stays with M7's E2 (no Docker here).

**Measured** (single runs on one machine, the same day):
- `baseline-M6` (one run) against `baseline-M5`: commands without subscribers 9 156/s (M5: 9 123/s);
  with two projections 4 658/s (5 250/s), p99 8.8 ms (7.2 ms), the projections 2 698 events/s,
  0.5 s behind; one contended aggregate 1 640/s (1 753/s); serial mode 9 573/s (10 698/s) and
  1 922/s on one aggregate (2 031/s); rebuild 6 695 events/s (6 544/s); a restore of 500 000
  events 2.07 s (3.4 s in M5's full run); the memory soak 651 861 commands, projected 107 s after
  the load (97 s), 129 MB right after it and 23 MB idle; the delivery soak's 1 000 000 events all
  projected 250 s after the load started (257 s), nothing waiting, 26 MB after each of five
  restarts; startup 719 and 793 ms (668 and 767 ms).
- Same day, M6 and the M5 tag alternately, two runs each: with two projections level (M6 5 127
  and 5 138/s, M5 4 967 and 5 166/s); without subscribers 9 196 and 8 423/s against 10 140 and
  9 010/s (about −8 %); serial mode 11 044 and 11 747/s against 12 805 and 12 414/s (about −10 %);
  contended −3 %, serial on one aggregate −5 %. M6 with the event key's insert taken out of the
  procedures: 9 918/s without subscribers and 12 206/s in serial mode, M5's level. So F124's event
  key costs about 8 % of the plain write path (F175); its log said "nothing measurable".
- `jsonnames-M6`: short keys −56 % JSON, −34 % events table, deserialization −54 %; commands and
  rebuilds unchanged. F172: a rebuild's page took 786 ms over 2 000 000 analyzed events, 5 ms
  since.

**Observations:**
- The measurement found what the specs could not: a plan that changes once PostgreSQL analyzes
  the tables (F172). Specs run on small, fresh schemas, where the planner's guesses are harmless;
  the bench's data is the first place where a misestimate costs. The rule: queries that join an
  aggregate's own row look it up by key, and every new query of the builder or a walk is checked
  with `EXPLAIN ANALYZE` on the bench's data.
- Writing the book was a review: twenty-three findings, three of them fixed the same day, the
  rest recorded with their milestone. The book stays current with each change (the working rule).

**Next:** M7, the release: the test kit (with a second installation), the API clean-up (F157,
F165), the remaining findings, Java 21, the README brought to the release, the book polished, the
delivery envelope between nodes (F170) and the walk over a type's aggregates (F173); AR5.

### Review 9, 18/10/2026: M7, before the release (architecture description, plan and code)

Scope: the release's work as built: the findings left for M7 (F148–F183), the test kit (E11, R20),
the examples E1–E11 and the book's code taken from them, the saga runner (AR5, D86), E2's deployments
with Docker Compose and on Kubernetes and its fault tests, Java 21 bytecode, the Scaladoc, the upgrade
runbook (D81), the CHANGELOG, the Polish translation of the book; `docs/ARCHITECTURE.md` and the book.
Inputs: the full suites in both modes after every work item (on JDK 25 at the end), E2's fault tests (two runs),
the bench (`baseline-M7` against `baseline-M6`, and the M6 tag on the same JDK the same day), the
book's 216 compiled blocks, and our own reading of M7's code changes (the saga runner, the projection
actor, the event bus, the document store's sessions, readiness, the stored data check).

**Outcome.** The M7 decisions hold. Refinements:
- The book's code is the examples' code (the M7 design). Compiling it found blocks that were wrong as
  written (chapters 10, 12, 14 to 17, the runbook) and F181; checking the Polish translation block by
  block against the English found three more slips in the English text. Documentation that compiles,
  and two books checked against each other, are the rule from here (`BookSnippetsSpec`,
  `DocLinksSpec`).
- Real processes found what one JVM could not: the order of the classpath (F178), seed nodes in a
  rolling restart (F180), and the log of a node the resolver downs (F182). The in-JVM specs stay the
  first line; E2's fault tests (`examples/deploy/e2/faults`) run before each release.
- AR5: a saga is data and functions that the library's runner runs; its state machine (`SagaRunning`)
  is the one place where a saga's decisions are taken, each step's result and each write coming back
  to the actor as a message, and the deprecated `SagaActor` runs on it unchanged.
- The API is smaller and documented: F157, F165 and F183 removed or renamed what nothing used or what
  misled; every public type of `api` and the public parts of `core` have doc comments, and `sbt doc`
  gives no warning.
- The backlog is split: `KNOWN-ISSUES.md` keeps what is open, and the 180 closed issues are in
  `KNOWN-ISSUES-ARCHIVE.md`.

**Fixed in M7:** AR5; F148, F152, F153, F156, F157, F160–F166, F169–F171, F173, F176–F178,
F180–F183. Accepted: F175. Deferred by the owner: F179. Open: F184 (found by this review's bench).

**Deferred, with reasons:**
- The fresh-reader test of the plan's verification (the quick start and the cluster example on a clean
  machine): it needs the library and mpjsons published (F179); it runs after the owner publishes them.
- The rehearsal on a copy of production (D81), for which F52 and F53 wait: Neula's.
- A slow database, the toolkit's third fault: the fault tests cover kills, graceful stops and a
  partition; a database that answers slowly is covered by the in-JVM timeouts only.

**Measured** (single runs on one machine; the bench on JDK 25, where `baseline-M6` ran on JDK 17):
- `baseline-M7` (one run, JDK 25) against `baseline-M6` (JDK 17): commands without subscribers
  8 884/s (M6: 9 156/s); with two projections 4 916/s (4 658/s), p99 7.3 ms (8.8 ms), the projections
  2 661 events/s, 0.55 s behind; one contended aggregate 1 666/s (1 640/s); serial mode 11 000/s
  (9 573/s) and 2 160/s on one aggregate (1 922/s); rebuild 5 732 events/s (6 695/s); a restore of
  500 000 events 3.6 s (2.07 s); the memory soak 556 271 commands, projected 93 s after the load
  (107 s), 113 MB right after it and 23 MB idle; the delivery soak's 1 000 000 events all projected
  304 s after the load started (250 s), nothing waiting, 26 to 27 MB after each of five restarts;
  startup 767 and 816 ms.
- The same day on the same JDK, the M6 tag (`m6-on-jdk25.md`): without subscribers 7 717/s, two
  projections 4 448/s, serial 10 450/s, contended 1 757/s, rebuild 6 213 events/s, a restore of
  500 000 events 3.46 s, the delivery soak 295 s. So the slower restore and soak against
  `baseline-M6` come from the JDK and the machine's day, not from M7's code. A second pair
  (`rebuild-check-M7.md`, `rebuild-check-M6.md`): commands level (4 611 and 4 688/s with two
  projections, 8 731 and 8 745/s without), rebuild 6 071 against 6 375 events/s; the rebuild is
  5–8 % slower in both pairs, within the ±7 % the command scenarios spread between runs (F184, open).
- E2's fault tests (two runs, Docker Desktop): about 100 000 commands each in nine minutes, every
  check held; a lost node's aggregates and singletons elsewhere 48 and 49 s after a kill; what a
  killed node had not delivered re-published 3.5 minutes later, or about 1 minute after a kill of
  the oldest (`docs/SCALING.md` §12).
- The full suites on JDK 25: core 325, examples 107, testdomain 194 scenarios, in 4 min 31 s; as a
  cluster of one in 4 min 48 s.

**Observations:**
- Each kind of check found its own kind of problem: compiling the book found examples that did not
  do what they said; running real processes found deployment problems; documenting the API found code
  nothing used; translating found prose that contradicted itself. None of them would have been found by
  more unit tests.

**Next:** the release: version `1.0.0` and the tag `v1.0.0` (the owner's go-ahead), then the owner
publishes mpjsons 0.7.0 (F179) and the library to Nexus; the fresh-reader test after that; Neula's
rehearsal on a copy of production plans the move (D81).
