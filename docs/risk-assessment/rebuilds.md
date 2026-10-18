# Working notes: rebuilds

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (RBLD-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Read in full:**
- `core/.../rebuild/`: `Projections.scala` (the `Projections` class, `BuildSlots`, `RegistryBuildPositions`, `ProjectionSupervisor`), `ProjectionBuilder.scala`, `ProjectionRegistry.scala` (Postgres and Memory), `ReplayStates.scala`, `ProjectionBuild.scala`.
- `core/.../projection/ProjectionActor.scala` and `SubscriptionsState.scala`.
- `SequenceGate.scala`, `AggregateRestorer.scala`, `TypesNamesState.scala`, `SchemaLock.scala`, `Singletons.scala`, `HistoryRewriter.scala`, `SpacePurge.scala`.

**Read in part:**
- `SubscribableProjectionActor.scala`: push and hand-over code.
- `EventStoreState.scala`, plus `PostgresEventStoreState`: `persist`, `processEvents`, `readCommittedPage`, `pageOf`, `aggregatesOf`, `backfillEventKeys`.
- `PostgresEventStoreSchemaInitializer`: `add_events`, `add_undo_event`, `add_duplication_event`.
- `AggregateOwner`: `persist`, `nextRoot`, `rewriteHistory`.
- `EventsBusActor`: the start-up (`registering`) state.
- `PostgresDocumentStore`: name check, `init`, index DDL.

**Docs:** CLAUDE.md; ARCHITECTURE §8 and §4.7; `M4-rebuild.md`; the purge and rewrite parts of `M6-organizations.md`; the decisions, open issues and review 6 in KNOWN-ISSUES.md; the archive entries for Rebuild (F54–F153), F118, F171, F124, F139 and F140; SCALING §4, §9 and §10.

**Not examined:** the spec suites; `MemoryEventStoreState` in depth; `FlowControl`; `RemoveSubscriber` in the bus's running state; `SpaceImport` internals; `Members`.

Nothing was built or run. No file was written.

## 2. Findings

### RBLD-1: The active build's instance runs the new code's store setup on the old tables; when that fails, the projection, its build and the node's event bus stay down
- **Severity:** High. **Likelihood:** Possible. **Confidence:** Confirmed (code). That the DDL fails on old data is PostgreSQL behaviour.
- **Location:**
  - `Projections.scala:304-307`: `decideBuilds` creates `active = registered.active.map(instanceOf)`, then `serving.foreach(_.ref ! GetListeners)`.
  - `Projections.scala:325`: `instanceOf` does `context.actorOf(Props(create(build))…)` with the current code.
  - `PostgresDocumentStore.scala:41,43-66,119-122`: `init()` runs in the store's constructor; it drops undeclared indexes, creates missing ones (`CREATE [UNIQUE] INDEX`, not concurrently), and rebuilds changed ones (F171).
  - `Projections.scala:230`: the supervisor strategy is Stop.
  - `Projections.scala:606-624`: an active instance that stopped makes the supervisor stop everything, `become(starting)`, and send `Start` after 10 s.
  - `Projections.scala:268-280`: in `starting`, `GetSubscriptionRequests` is only recorded, and everything else is stashed (`case _ => stash()`).
  - `EventsBusActor.scala:202-233,251-258`: the bus stashes all publications until every registered subscriber answered, and keeps the node not ready (`Readiness`).
  - `SchemaLock.scala:20,40-46`: one advisory lock per database, `MaxWait` 5 minutes.
- **Preconditions:** a managed projection gets a new version (or a new store, or new state-class setup) whose store declares a UNIQUE index, or a typed index (`::bigint`/`::int`), that the old build's documents violate. Or the new code's constructor cannot run on the old tables.
- **Triggering scenario:** V5 → V6 adds `UniqueTextIndex(3, Seq("email"))`, or `MultipleLongIndex` on a field that V5 stored as a string. This is exactly the kind of change that motivates a version bump. Deploy:
  - On one node, at the restart.
  - In a cluster, when the singleton moves to a V6 node.
- **Failure mechanism:**
  1. At `Start`, the active V5 instance is constructed with V6 code against `projection_x_v5`.
  2. `CREATE UNIQUE INDEX … ON projection_x_v5` fails, so the constructor throws (ActorInitializationException). The instance is stopped.
  3. The supervisor takes the "active instance stopped" branch: it stops the V6 build instance too and returns to `starting`. Ten seconds later the same thing happens again, forever.
  4. `Listeners` never arrives, so `connect()` never runs. The V6 builder never starts, the bus is never answered, and every query is stashed without bound.
  5. `ServeStale` does not help: the active instance is still constructed with the new code.
  6. Even when the index can be built, a non-concurrent build on a large active table holds the start for minutes under the global `SchemaLock`. Other stores' and schema initialisations on any node give up after 5 minutes ("The schema lock was not free") and fail in turn.
- **Expected impact:**
  - On a single node, or on any node restarted afterwards: the bus never starts. No event reaches any projection or saga, the node is never ready, and publications stash without bound.
  - In a running cluster: the projection has no instance at all. Its queries hang and the supervisor's stash grows. Outbox rows of its types are kept and keep growing. The new version is never built.
  - Detection: a "stopped; the projection starts again" error log every 10 s and "Event bus waits for…". Recovery needs a code change.
- **Validation:** V1 projection holding documents with duplicate `email` → stop → start with V2 declaring a unique index on `email` → expect: `GetBuilds` never answers, the bus is stuck in `registering`, and `Readiness` is false.
- **Recommended fix:**
  1. A build whose version differs from the code must not synchronise indexes. Expose `build.isCurrentVersion` and give the store a `syncIndexes = false` mode. Alternatively, build indexes `CONCURRENTLY` outside constructors.
  2. Decouple the supervisor from the active instance: take `Listeners` from the build instance when the active one cannot be made; answer the bus with the instances that exist; let the build proceed.
  3. Bound the `starting` stash, or answer queries with `Status.Failure` while starting keeps failing.
  4. Add escalating back-off and a status that names the error.
  - Trade-off: the old build serves without the new indexes, so the new code's queries may be slower until the switch.
- **Regression test:** the scenario above. Expect the V2 build to run and switch, the bus to start, and queries to get answers or explicit failures within `restartAfter`.

### RBLD-2: An erasure (or an import) during a build, or during another targeted rebuild, is refused; the build switches in with the erased personal data, and re-running the rewriter reports "complete"
- **Severity:** High (GDPR). **Likelihood:** Possible. **Confidence:** Confirmed (code).
- **Location:**
  - `Projections.scala:512-514`: `case (Some(b), _, _) => requester ! RebuildRefused("build " + … + " is in progress, and replaces the active one whole")`, and `case (_, Some(t), _) => RebuildRefused("the rebuild of … is in progress")`.
  - `ProjectionBuilder.scala:216`: the bulk pass reads by `events.id > position`.
  - `ProjectionBuilder.scala:314`: the reconciliation compares versions only (`a.version > applied`).
  - `HistoryRewriter.scala:49,68-69,78-92`.
  - `AggregateOwner.scala:780-787`.
- **Preconditions:** a full build is in progress (hours on a large store), or another targeted rebuild is running on the projection, for example two erasures run concurrently.
- **Triggering scenario:** `HistoryRewriter.rewrite(A, EraseCustomer)` while P's build B2 has already passed A's event ids, or has caught up and is waiting for failures to be retried or for its switch.
- **Failure mechanism:**
  1. The rewrite updates A's events in place: same ids, same versions.
  2. `RebuildAggregates(OneAggregate(A))` is refused, and the rewriter records the projection as `failed`.
  3. B2 never re-reads A: the bulk pass is past A's ids, and the reconciliation sees no version change. The active B1 also keeps the old values until the switch.
  4. On a re-run, `rewriteHistory` changes nothing and answers `HistoryRewritten(A, 0, 0, …)`, so A is put in `unchanged`.
  5. `rebuild(walked.rewritten.map(_.id), …)` then runs over an empty set, and the fold marks every projection `rebuilt`, so `complete = true`.
  6. The same refusal hits `RebuildAggregates(OneSpace(into))` after a `SpaceImport` (no outbox rows). If B2 has caught up, the imported organization is missing after the switch.
- **Expected impact:**
  - Erased personal data stays in the newly active read model indefinitely.
  - The first report's reason says the build "replaces the active one whole", and the second report says complete.
  - Hard to detect.
- **Validation:**
  1. Start a build with a slow builder and let it pass A.
  2. Rewrite A.
  3. After the switch, query P: the old value is still there.
  4. Re-run the rewriter: the report is `complete` with P in `rebuilt`.
- **Recommended fix:**
  1. Do not refuse: record targets in the registry (a set per projection) and queue them behind a running targeted rebuild.
  2. For a build in progress, apply the target to the build as well: reset the build subscriber's versions for the target, and run a targeted builder on the build instance after `BuildCaughtUp` and before the switch. Rebuild the active one now too.
  3. In `HistoryRewriter`, persist which projections still owe a rebuild. On a re-run, rebuild `rewritten ++ unchanged` for those projections, and never mark a projection `rebuilt` for an empty set.
  - Trade-off: extra targeted work at the switch.
- **Regression test:** the validation scenario. Expect the new build to lack the value, and the re-run to perform or require the rebuild.

### RBLD-3: With `ServeStale`, an abandoned build (a rollback) leaves the active build silently missing every event committed during the build
- **Severity:** High. **Likelihood:** Possible to Unlikely (opt-in option plus a rollback during a build). **Confidence:** Confirmed (code).
- **Location:**
  - `Projections.scala:351-353`: `takesDeliveries(i) = building.isEmpty || building.exists(_.ref == i.ref) || spec.whileBuilding == ServeLive`.
  - `Projections.scala:575-579`: `serveUnchanged` (a build begun at run time).
  - `ProjectionActor.scala:583-585`: while building, a delivery is acked with `MessageAck` without being applied.
  - `Projections.scala:291-296,304`: abandon and drop, then the active instance serves live.
- **Preconditions:** `whileBuilding = ServeStale`; a build in progress is abandoned.
- **Triggering scenario:**
  1. Deploy V+1 of a `ServeStale` projection; the build runs for hours.
  2. Roll back to V. (The same applies to `RebuildProjection` at run time, then a rollback.)
- **Failure mechanism:**
  1. During the build, only the build's instance is subscribed for P. It acks every delivery unapplied, so the outbox rows are deleted.
  2. At the rollback, `decideBuilds` abandons and drops the V+1 build. The active V build matches the code and is subscribed again, but nothing reconciles it.
- **Expected impact:**
  - Each aggregate changed during the build stays stale until its next event, when a gap makes its owner re-publish.
  - Aggregates without later events stay stale indefinitely.
  - There is no failure, no gap and no status entry.
- **Validation:** `ServeStale` projection; start a V+1 build; keep it from finishing (for example, hold both build slots elsewhere); commit events; restart with V. The active documents lack those events, and `GetProjectionStatus` is clean.
- **Recommended fix:**
  - When an active build that took no deliveries serves live again (abandon, or a switch from `ServeStale` to `ServeLive`), run a reconciliation-only builder against it: `ProjectionBuilder` with positions reading `bulkDone = true` and `targeted = Some(AggregateScope.All)`.
  - Alternatively, record in the registry from which event id the active build stopped taking deliveries.
  - Trade-off: a full aggregate walk after a rollback.
- **Regression test:** as validation. Expect every aggregate's document current after the restart.

### RBLD-4: Build table and sequence names are not checked against PostgreSQL's 63-byte identifier limit; two builds can share one physical table, and the retirement drops the live one
- **Severity:** High. **Likelihood:** Unlikely. **Confidence:** Confirmed (code); the truncation is documented PostgreSQL behaviour.
- **Location:**
  - `ProjectionBuild.scala:27-41`: `recorded` returns `checked(base) + record.suffix`, with no length check.
  - `ProjectionRegistry.scala:73`: the suffix grows with the build number (`"_v" + version + "_b" + build`).
  - `ProjectionRegistry.scala:188-196`: `drop` runs `DROP TABLE IF EXISTS <full name>`.
  - By contrast, `PostgresDocumentStore.scala:34-38` guards its own names ("PostgreSQL cuts longer names silently").
- **Preconditions:** a `build.table` or `build.sequence` base name of about 56 characters or more; builds accumulate (`RebuildProjection` repairs, context-version rebuilds).
- **Triggering scenario:** a 60-character base at version 3: `<base>_v3` (63 characters, active) and `<base>_v3_b2` (66, truncated to `<base>_v3`) are the same table. With 59 characters, builds `_v3_b2` and `_v3_b5` collide.
- **Failure mechanism:**
  1. The new build's `CREATE TABLE IF NOT EXISTS` silently reuses the active build's table and replays history into it.
  2. At the switch, the old build's drop runs `DROP TABLE <base>_v3…`, which PostgreSQL truncates the same way, and removes the table the new active build uses.
  3. For document stores, a name that crosses 63 at a later build makes the store refuse in its constructor, and the build instance restarts every 10 s forever (F110 loop). The projection can no longer be rebuilt.
- **Expected impact:** during the build, a corrupted live read model; after the switch, its state table is lost, every listener fails ("relation does not exist"), and failures pile up. A repair rebuild collides again.
- **Validation:** a projection with a 60-character `build.table` name, then `RebuildProjection`. Inspect `pg_class` (one table), then switch: the table is gone.
- **Recommended fix:**
  - Refuse names over 63 bytes in `ProjectionBuild` at instance creation, keeping the active build serving.
  - Shorten the suffix (`_b<build>`; the version lives in the registry) or hash long bases.
  - In `drop`, refuse an object whose first 63 bytes match a name recorded by a live build.
- **Regression test:** as validation. Expect a refusal before any write, and the active table intact.

### RBLD-5: A targeted rebuild of a type or organization lists and resets every target aggregate in one transaction on the serving instance's thread
- **Severity:** Medium. **Likelihood:** Possible (it is the documented step after an import, and a repair tool). **Confidence:** Confirmed (code); durations estimated.
- **Location:**
  - `Projections.scala:524-527`: the listing closure `Iterator.unfold(...)…flatten.toVector`.
  - `ProjectionActor.scala:430-439`: `aggregatesOf()`, then `localTx { clearForRebuild; resetVersions }`, then `toSet` and `forget`.
  - `SubscriptionsState.scala:463-474`: groups of 10 000 `DELETE … aggregate_id IN (…)` in the caller's transaction, then an after-commit pass under the shared monitor.
- **Preconditions:** `RebuildAggregates(OneType | OneSpace)` with a large target.
- **Triggering scenario:** `OneType` over 10 million aggregates.
- **Failure mechanism:**
  1. 10 000 `aggregatesOf` pages (about 1 ms each with F173's index, about 100 ms without it, so up to about 17 minutes), on the active instance's thread.
  2. A Vector and a Set of 10 million ids (estimated at hundreds of MB).
  3. About 2 000 DELETEs over 10–30 million subscription rows in one transaction.
- **Expected impact (estimated):** for minutes up to about an hour:
  - The serving instance answers no query and applies no live delivery.
  - The long transaction holds back vacuum database-wide, so hot tables bloat.
  - Possible OOM.
  - Separately, re-publications triggered by live events of reset aggregates add load (see Known, re-checked: F114).
- **Validation:** `OneType` with 1 million aggregates. Measure query latency, heap and the transaction's duration during the reset.
- **Recommended fix:**
  - Stream the target in pages: each page runs `clearForRebuild` (by ids) and `resetVersions` in its own transaction, driven by self-messages so the instance stays responsive.
  - Let the builder follow the pages.
  - Trade-off: the target is partly cleared for a while; it already is while being rebuilt.
- **Regression test:** query latency stays under 1 s and heap stays bounded during a large reset.

### RBLD-6: The reconciliation and gap fills hold every missing event of a page of up to 1 000 aggregates in memory at once
- **Severity:** Medium. **Likelihood:** Unlikely to Possible. **Confidence:** Strongly suspected (depends on volumes).
- **Location:**
  - `ProjectionBuilder.scala:219-223`: `behind(...)`, then all chunks are queued in `ready`.
  - `ProjectionBuilder.scala:310-319`.
  - `ProjectionBuilder.scala:334-348`: every part of each range is collected.
  - `ProjectionBuilder.scala:364-371`: `fill`.
  - `Projections.scala:343-347`: the build instance is subscribed before it has a slot.
  - `ProjectionBuilder.scala:122-124`: when `bulkDone`, the builder goes straight to `GoLive` and the reconciliation.
- **Preconditions:** a build restarted after its bulk pass, waiting for a slot. With `buildsAtOnce = 2` after a deployment that rebuilds many projections, the wait can be hours, while its instance acks every delivery unapplied.
- **Failure mechanism:** each reconciliation page reads all missing versions of up to 1 000 aggregates (up to 10 000 versions each), plus restored states, into `ready`. Byte limits bound only individual chunks.
- **Expected impact:** a heap spike. An OOM exits the JVM, and at restart the same build resumes with a larger backlog, which can become a crash loop.
- **Validation:** a reconciliation over 1 000 aggregates each 5 000 versions behind; measure peak heap and `ready.size`.
- **Recommended fix:** keep a cursor (aggregate, kind, next version) and produce items lazily, at most `chunksInFlight + 1` chunks ahead. Do the same for gap fills.
- **Regression test:** peak heap stays bounded regardless of the backlog.

### RBLD-7: A switch the registry reports as not done (a lost commit acknowledgement, or another supervisor) leaves the supervisor stuck until restart
- **Severity:** Medium. **Likelihood:** Unlikely. **Confidence:** Confirmed (code) for the stuck state.
- **Location:**
  - `Projections.scala:463-486`: on an exception it schedules `CheckSwitch`; on `false` it only logs "no longer the build in progress… it does not switch".
  - Refusals while the supervisor believes a build is in progress: `Projections.scala:399-401` (`PurgeCheck`), `512-514` (`rebuildTarget`) and `561-563` (`rebuild`).
- **Triggering scenario:** the `UPDATE` commits in autocommit but the client sees an exception (a network blip). The retry then returns `false`.
- **Failure mechanism:** the registry says the new build is active, while the supervisor keeps `active` = old build and `building` = new build (`caughtUp`) indefinitely. Nothing re-reads the registry.
- **Expected impact:** until restart:
  - Queries go to the old build (stale, under `ServeStale`).
  - Purges, targeted rebuilds (erasure) and `RebuildProjection` are refused.
  - `GetBuilds` contradicts the registry.
- **Validation:** a registry double whose `switch` commits and then throws. Expect the supervisor to remain stuck.
- **Recommended fix:** on `false` or an exception, re-read `registry.projection(name)`.
  - If the build is active: complete the switch in memory (`Activate`, retire the old instance).
  - Otherwise: restart via `Start`.
- **Regression test:** the double above. Expect the new build served within `restartAfter`.

### RBLD-8: Every build permanently consumes a SMALLINT `types_names` id
- **Severity:** Medium (when exhausted). **Likelihood:** Unlikely (Possible with scheduled rebuilds). **Confidence:** Confirmed (code).
- **Location:**
  - `ProjectionRegistry.scala:76,153`: a new subscriber `projection:<name><suffix>` per build.
  - `TypesNamesState.scala:59-62` (`id SMALLINT`) and `86` (`nextval`, never reused).
  - Subscription reads and writes call `typeIdByClassName(subscriberName)`.
- **Triggering scenario:** a nightly `RebuildProjection` of 60 projections uses about 22 000 ids a year. The limit is 32 767 names, shared with event and aggregate classes.
- **Failure mechanism:** past 32 767 the INSERT fails ("smallint out of range").
- **Expected impact:**
  - Any new event class can no longer be stored; commands producing it fail.
  - A new build's `applyChunk` throws in its prologue, so the builder resends every 5 s forever.
- **Validation:** count `types_names` rows across N builds.
- **Recommended fix:**
  - Reuse two subscriber identities per projection (A/B) and clear them at retirement, or delete the retired build's name row.
  - Alert on `types_names_seq`; widen the column in a later contract step.
- **Regression test:** the number of ids stays bounded across 100 builds.

## 3. Lesser findings
- **Possible premature switch from a stale `BuildCaughtUp`:**
  - The supervisor never checks that `BuildCaughtUp` comes from the current builder (`Projections.scala:393-396`).
  - A copy stashed during a restart-in-place (`:280`) can switch to an instance still in `BuildFromStore`, which then acks every delivery unapplied forever.
  - No realistic trigger found: instances catch non-fatal errors, and fatal ones exit the JVM. Hypothesis.
- **Retirement answers parked queries with `None`** (`ProjectionActor.scala:456-461`), so clients see "not found" at the switch. Async searches still in flight report to a stopped instance, and their askers time out (`:1162-1168`).
- **No lock timeout on the drop:** `DROP TABLE` runs without `lock_timeout` on the supervisor's thread (`ProjectionRegistry.scala:188-196`). A session holding a lock on a retired table stalls every query of the projection.
- **Unbounded stash** in the supervisor's `starting` state (`Projections.scala:280`).
- **No progress for the reconciliation:** it saves no position. A builder stopped by a transient error in a gap fill restarts it from aggregate 0 (`ProjectionBuilder.scala:138,205,364-371`).
- **Widespread listener failures during a build:** each newly failing aggregate makes its chunk fail whole and be re-applied one item at a time, about 1 000 transactions (`ProjectionActor.scala:976-986`). Throughput collapses for a build that cannot switch anyway.
- **Targeted builders:** they take no build slot, and their contexts are not checked (`Projections.scala:538-543` vs `447-451`). A state listener without its context fails every target aggregate.
- **Replay states and chunks are bounded by count only:** states are not counted in chunk bytes (`ReplayStates.scala:38-40`, `ProjectionBuilder.scala:247,301-305`).
- **Quadratic cost after a handler error:** a handler that throws on one replayed event makes every later run of that aggregate restore its whole history and fail again, with an error log each time (`ReplayStates.scala:47-51`).
- **Status inaccuracies:**
  - `BuildStatus.position` is an event id or an aggregate id depending on the phase, with no totals.
  - `eventsSent` counts re-sent chunks.
  - `Builds.caughtUp` can stay true.
- **No way to cancel a build in progress:** with `ServeStale`, the active build stays frozen while a failing build restarts in a loop (`Projections.scala:387-391,595-605`).
- **Orphan tables:** `recordObject` runs in its own autocommit, so a table created at run time whose record failed is never dropped (`ProjectionRegistry.scala:182-185`).
- **Re-adopting a retired projection:**
  - Re-adding a retired projection under its old name re-adopts build 0 from `components_versions`, which `retire` never clears.
  - It gets empty tables and no build when the versions match (`Projections.scala:62-73,312-321`).
- **Personal data in failures and logs:** stored in `subscription_failures.error` and in logs, from exception messages (mpjsons errors, PostgreSQL "Where: JSON data …", listener messages) (`ProjectionBuilder.scala:271,289-291,358-359`). Targeted resets clear the rows; logs stay.
- **Disk:** a build doubles the projection's tables and its `subscriptions` rows until the retired build is dropped. SCALING.md does not say so.

## 4. Known, re-checked
- **F114 (incomplete):** after a targeted reset, a live event of a target aggregate makes its owner re-publish from version 1 (`ProjectionActor.scala:722`). Those batches are not `fromBuilder` (`:527,807`), so `rebuilding` is false and push and other side effects run for history. It also triggers a full re-publication per active aggregate.
- **Review 6 observation "drop runs on the supervisor's thread" (worse than recorded):** the cost is mostly `clearSubscriptionsInfo`, one `DELETE` of all the retired build's subscription rows (about 10–20 million for a large projection), in one transaction (`SubscriptionsState.scala:443-455`, `Projections.scala:631-638`). Queries stall for minutes after the switch. The same happens at start (F116 sweep, while queries are stashed) and in `retire`, under the global `SchemaLock`.
- **F171 (mitigation ineffective):** the recommended "new projection version (new tables) for a large table" still rebuilds the index on the old large table, because the active build's instance runs the new store's `init` (RBLD-1).
- **F110 (incomplete):** a build instance that cannot start no longer blocks the bus, but an active instance that cannot be made still keeps the supervisor in `starting`, never answering the bus (RBLD-1).
- **M6 "do not rebuild during a purge" (worse than recorded):** a build or targeted rebuild started mid-purge makes the next pass's `PurgeCheck` refuse. The purge then ends without `PurgeSpace`, so even the active builds keep the organization's documents. `PurgeReport.refused` is documented as "did not start, having deleted nothing", although everything was deleted (`SpacePurge.scala:34-35,61-62,78-82`).

## 5. Checked and safe
- **Rows committed late (commit order):** ids are taken after the aggregate's row lock (`add_events`), so per-aggregate order holds. Rows the bulk pass skips are caught by the reconciliation, which lists after `GoLive` is processed (READ COMMITTED).
- **Resume:** the saved position covers applied chunks only. Handed-over or split-kind items lost in a crash are recovered by the reconciliation.
- **Retries after `Status.Failure`:** re-sent chunks keep their order; a lost `ChunkApplied` save is harmless.
- **The `outstanding` count:** every handed-over item is answered exactly once (applied, already applied, given up, forgotten, or its queue gone).
- **Builder restarts:** a builder restarted in GoingLive or Reconciling always gets `ProjectionLive` back, since `GoLive` is idempotent.
- **Build slots:** `BuildSlots` de-duplicates holders and waiters.
- **Registry races:** `switch` is a compare-and-set; `startBuild` takes `FOR UPDATE`.
- **Permanent delete during a build (F111):** it reaches the build, and the builder fills its gap.
- **Squash during the bulk pass:** runs start at the squash event (F98, F139); an undo or a duplication takes the state the store gives at its version.
- **Parallel batches during a targeted reset:** the subscription row compare-and-set keeps documents and versions consistent.
- **Orphan events in the `event_keys` path:** they cannot arise (keys are written only with own rows; the backfill skips orphans).
- **SQL in `drop`:** names are checked by regex; everything else is bound.
- **Overflow:** version arithmetic is bounded by `aggregateVersionLimit`, and `next + chunkEvents` is capped.
- **Queries at the switch:** they go to the new build at once; the old instance answers its mailbox before `Retire`.

## 6. Open questions
- Neula's longest `build.table`, `build.sequence` and document-store base names, and its expected build counts per projection (RBLD-4, RBLD-8). Also the current maximum `types_names` id in production.
- Do Neula's planned version bumps add unique or typed indexes, and how large are those tables (RBLD-1)?
- Will any projection use `ServeStale`? Are rollbacks possible during multi-hour builds (RBLD-3)?
- Node heap sizes, and how long builds wait for slots after a deployment that bumps a context version (RBLD-6).
- Does Neula's erasure tooling act on `RewriteReport.failed`, and do erasures run concurrently (RBLD-2)?
- Do mpjsons error messages include JSON fragments, which would put personal data in logs and `subscription_failures`?
- Does any legacy aggregate have id ≤ 0? Full builds and reconciliations start after id 0; targeted rebuilds start at `Long.MinValue`.
