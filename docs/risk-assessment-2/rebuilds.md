# Working notes: rebuilds (second risk assessment)

- **Area:** managed projections and rebuilds (prefix R2-RBLD)
- **Scope:** `core/.../rebuild/*` (`Projections`, `ProjectionSupervisor` with the F202 catch-up (`ReconcileOnly`) and the
  queued targeted rebuilds (jobs), `ProjectionBuilder`, `ProjectionRegistry`, `ProjectionBuild`, `ReplayStates`), and
  the parts of `ProjectionActor`, `SubscriptionsState`, `EventsBusActor`, `HistoryRewriter`, `SpacePurge` and
  `PostgresDocumentStore` they drive. The fixes since v1.0.0 in this area: F189, F192, F202, F203, F217 (part), F223,
  F235, F247 (part).
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT)
- **Method:** static reading only; nothing built or run. Line numbers are at 2453711. Durations and sizes are estimated
  unless a source is named.

## Findings

### R2-RBLD-1 — A targeted rebuild during a projection's first build, with `ServeStale`, makes its only instance acknowledge every live event unapplied until the node restarts
- **Severity** High · **Likelihood** Possible (a projection declared `ServeStale`, and an erasure, an import's space
  rebuild or a repair while its first build runs; first builds of large projections take hours) · **Confidence**
  Confirmed (code)
- **Where:**
  - `Projections.scala:565-567`: with only a first build, `forBuild` is empty (active and build are one instance), so
    the job is an active job (`build = None`).
  - `Projections.scala:600`: `if (job.build.isEmpty && building.isDefined && spec.whileBuilding == ServeStale) instance.ref ! ServeUnchanged`:
    `building.isDefined` is true, and the instance is the build itself.
  - `ProjectionActor.scala:424-429`: `ServeUnchanged` sets `unchanged = true` and `eventBus = None`.
  - `ProjectionActor.scala:593-595`: `unchanged` is checked before the build check and before F111's removal exception:
    every delivery is acknowledged unapplied, permanent deletes too.
  - `ProjectionActor.scala:420-423`: only `ConnectedTo` clears `unchanged`; the supervisor sends it only to instances
    not yet subscribed (`Projections.scala:381-383`), and the first build's instance is subscribed from the start.
  - `Projections.scala:512-516`: the switch sends `Activate`, which does not clear it either.
- **Scenario:** a new projection P declared `whileBuilding = ServeStale` (chosen once, for later versions). Its first
  build runs. `HistoryRewriter.rewrite` (or `RebuildAggregates(OneSpace)` after an import) sends `RebuildAggregates`.
- **Mechanism:** before F192 the request was refused while a build ran; now it is queued and runs at once as an active
  job. `startJob` takes the first build for an active build serving stale beside another build, and sends it
  `ServeUnchanged`. From then on the instance acknowledges every bus delivery without applying it. The builder still
  applies its chunks (`applyChunk` ignores `unchanged`), so the bulk pass and the reconciliation complete, the job
  completes, the build catches up and switches in (`switch` of a first build succeeds). After the reconciliation passed
  an aggregate, its later events are acknowledged and their outbox rows deleted. Nothing re-delivers them.
- **Impact:**
  - The projection's only build silently stops following the store; queries serve ever older data; no failure, gap or
    status entry. Until a restart.
  - After a restart, aggregates changed meanwhile heal at their next event (gap, re-publication); the others stay stale.
  - Permanent deletes acknowledged unapplied are lost for good: the store no longer lists the aggregate, so no
    reconciliation finds its document (personal data kept).
- **Validation:** `ProjectionsSpec`-style: a new `ServeStale` projection with a slow builder; send
  `RebuildAggregates(OneAggregate(a))` during its first build; after `switchedTo(…, 1)`, change another aggregate:
  the document never changes; `GetProjectionStatus` is clean.
- **Suggested fix:** send `ServeUnchanged` only when the active instance is a different instance from the build
  (`building.exists(_.ref != instance.ref)`), as `forBuild` already distinguishes. Better: send it only if the active is
  not subscribed already, or drop it: an active build serving stale beside a build is unsubscribed already
  (`takesDeliveries`, `serveUnchanged`).
- **Regression test:** the validation above, expecting the later change applied; plus a permanent delete during the
  first build applied.
- **Continues:** F192 (its queueing made this path reachable).

### R2-RBLD-2 — The F202 catch-up depends on the rolled-back code's `whileBuilding`, not on what the abandoned build did; the realistic rollback gets none
- **Severity** High · **Likelihood** Possible (a rollback during a `ServeStale` build, the case F202 is about) ·
  **Confidence** Confirmed (code)
- **Where:** `Projections.scala:321` `catchUpDue = spec.whileBuilding == ServeStale && registered.active.isDefined`;
  `spec` is the code now starting (the older code after a rollback). Chapter 10 (`docs/book/10-rebuilding-projections.md:255-271`)
  tells users to declare `ServeStale` on the version whose code cannot read the older documents, that is the new one.
  The F202 test (`ProjectionsSpec.scala:529-556`) declares `ServeStale` on version 1 too, so it passes.
- **Scenario:** version 2 declares `ServeLive` (the default); version 3 declares `ServeStale` (book example). Version 3
  is deployed and builds for hours; the active version-2 build serves unchanged. The team rolls back to version 2
  before the switch.
- **Mechanism:** the version-2 code's spec says `ServeLive`, so `catchUpDue` is false; the build is abandoned and dropped,
  the active build is subscribed again and nothing reconciles it. It is exactly F202's original failure.
- **Impact:** every aggregate changed during the build stays stale until its next event; those without one stay stale
  indefinitely; permanent deletes during the build are lost (see R2-RBLD-3). Silent.
- **Also (same fix):** the catch-up is not recorded (`Projections.scala:627-629`): the abandon and the drop are committed
  first, so a stop during the catch-up (hours over 10M+ aggregates, estimated) loses it for good.
- **Validation:** the F202 scenario with version 1 started `ServeLive` and version 2 `ServeStale`: after the rollback the
  note still lacks "two".
- **Suggested fix:** decide from what happened, not from the code: always run the catch-up when a build is abandoned
  while another build is active (the cost is one reconciliation), or record in the registry when the active build
  stopped taking deliveries (set when a `ServeStale` build starts or `serveUnchanged` runs; cleared when a catch-up
  ends), and run the catch-up from that record at every start until it is cleared.
- **Regression test:** the validation, plus a stop during the catch-up followed by a start: the active build catches up.
- **Continues:** F202.

### R2-RBLD-3 — After a rollback, aggregates permanently deleted during a `ServeStale` build keep their documents in the active build forever
- **Severity** Medium (High where permanent deletes serve erasure) · **Likelihood** Unlikely (rollback during a
  `ServeStale` build, with permanent deletes meanwhile) · **Confidence** Confirmed (code)
- **Where:** `Projections.scala:380` (the active build serving stale is not subscribed); `ProjectionActor.scala:596`
  (the build instance applies removals, F111); the owner deletes the aggregate once every subscriber had it, and
  `PostgresEventStoreState.scala:541` removes its `aggregates` row; the catch-up lists aggregates from the store
  (`ProjectionBuilder.scala:248`, `PostgresEventStoreState.scala:427-437`).
- **Mechanism:** only the build instance saw the `PermanentDeleteEvent`; its tables are dropped at the abandon. The
  catch-up (when it runs, R2-RBLD-2) reconciles only aggregates the store still lists, so the deleted aggregate's
  document and subscription row stay in the active build.
- **Impact:** a deleted aggregate's document (possibly personal data) served indefinitely; no status entry.
- **Validation:** `ServeStale` projection; start a build; permanently delete an aggregate; roll back. The active build
  still returns its document.
- **Suggested fix:** record permanent deletes that happen while the active build serves stale (or read them from the
  outbox/audit), and apply them to the active build at the catch-up; or, at the catch-up, also walk the active build's
  subscription rows of its types and remove documents of aggregates the store no longer has (needs a per-projection
  removal hook).
- **Regression test:** the validation; expect the document gone after the rollback.
- **Continues:** F202.

### R2-RBLD-4 — A build's queued targeted rebuild that fails at its reset is dropped, and the build switches in without it
- **Severity** High (erasure) · **Likelihood** Unlikely (a transient database failure, or a failing `clearForRebuild` in
  the new code, at the reset of a build's job) · **Confidence** Confirmed (code)
- **Where:**
  - `Projections.scala:447-454`: any `Status.Failure` from the job's instance before its builder starts drops the job
    (`targeted = None`, `saveJobs()` removes it from the registry), then `trySwitch()`.
  - `Projections.scala:567`: a build's job has no requester when an active build exists; the requester was answered by
    the active job (`HistoryRewriter` reported the projection `rebuilt`).
  - `ProjectionActor.scala:435-444`: the reset lists the target and runs a transaction; a lost connection or a full
    pool fails it with a transient `SQLException`, answered as `Status.Failure` (`ProjectionActor.scala:384-389`).
- **Scenario:** an erasure while build B2 is in progress. The active job completes and the rewriter reports complete.
  After B2 catches up its job resets the target; the database blips at that moment.
- **Mechanism:** the job is dropped as if refused, with no one to tell; `buildJobsWaiting` is false, so B2 switches in.
  B2's bulk pass read the aggregate before the rewrite, so the active build now holds the erased values (F192's
  failure). A rerun of the rewriter would fix it, but the report said complete.
- **Impact:** erased personal data served after the switch, with a "complete" report; only an error log line.
- **Validation:** a projection whose `clearForRebuild` throws a transient `SQLException` once for build 2's instance;
  rewrite during build 2; after the switch the old value is served and the report was complete.
- **Suggested fix:** retry a transient failure of the reset after `restartAfter` (as `RunTargetedBuilder` does for the
  builder); for a build's job that fails permanently, do not switch: keep the job and report the build blocked in
  `Builds`/`GetBuildStatus`, or fail the build visibly. Answer the original requester only when the build's job is done
  too, or keep the duty in the registry with its outcome.
- **Regression test:** the validation; expect the reset retried and the new value after the switch.
- **Continues:** F192.

### R2-RBLD-5 — Since F223, a transient database error during a reconciliation stops the builder, and the reconciliation (or a targeted rebuild, or the catch-up) starts again from the beginning
- **Severity** Medium · **Likelihood** Possible (pool timeouts and connection drops are routine under high load; a
  reconciliation over 10M+ aggregates runs for a long time, estimated tens of minutes to hours) · **Confidence**
  Confirmed (code)
- **Where:**
  - `ProjectionBuilder.scala:341-344`: `behind` now enqueues closures that read events later.
  - `ProjectionBuilder.scala:207,212-222`: they run in `nextChunk`/`aChunkOfWork`, inside `pump`, outside
    `readNextPage`'s `try` (`:240-259`), which used to retry the page after `retryAfter`.
  - `ProjectionBuilder.scala:381-386`: `itemsFrom` catches only non-transient errors; `Errors.transient`
    (`Errors.scala`) counts SQLStates 08/40/53/57 and a null SQLState (a pool timeout) as transient.
  - `Projections.scala:250`: the supervisor strategy stops a failing child; `:691-697` starts a new builder, which reads
    `bulkDone = true` and goes live and reconciles from aggregate 0; `:682-686` restarts the catch-up from 0; `:687-690`
    restarts a targeted builder with `NoPositions`, so its whole bulk pass is read again.
- **Mechanism:** before F223, a transient error while reading what an aggregate lacks retried that page; now it escapes
  the actor, which is stopped. No reconciliation position exists, so all progress is lost.
- **Impact:** under load, long reconciliations, catch-ups and large targeted rebuilds (`OneType`, `OneSpace` after an
  import) restart repeatedly; builds switch late or not at all, the active build stays stale longer, and the store is
  read again each time. Error logs per restart.
- **Validation:** an event store double whose `readPart` throws an `SQLException` with SQLState 08006 once during the
  reconciliation; observe the builder stopped and `GetBuildStatus` back at the start of the reconciliation.
- **Suggested fix:** catch transient errors around the closures in `nextChunk` (put the closure back at the head of
  `work` and `retryLater()`), as `readNextPage` does; save the reconciliation's aggregate position too.
- **Regression test:** the validation; expect the builder to keep running and finish after one retry.
- **Continues:** F223 (and the first assessment's lesser item on reconciliation progress).

### R2-RBLD-6 — A purge is not refused while the F202 catch-up runs, and the catch-up's chunks can write a purged organization's documents back
- **Severity** Medium (personal data of a removed organization) · **Likelihood** Unlikely (a purge while a catch-up runs
  after a rollback) · **Confidence** Confirmed (code) that nothing excludes it; Strongly suspected for the interleaving,
  which depends on timing
- **Where:** `Projections.scala:433-438`: `PurgeCheck` refuses for a build or a job, not for `catchingUp`;
  `ProjectionActor.scala:590` checks `purged` for bus deliveries only; `applyChunk` (`ProjectionActor.scala:1002-1049`)
  does not; `SpacePurge.scala:66-75` clears the space (`PurgeSpace`) as its last step.
- **Mechanism:** the catch-up reads an aggregate's missing events before the purge deletes them and sends the chunk
  after `PurgeSpace` was applied (the chunk waits in `first` behind the chunks in flight). Its items start a sequence or
  continue from version 0 after `PurgedAggregates` reset it, so they are applied: the document and its subscription row
  come back, and no later step removes them.
- **Impact:** documents of a purged organization in the active read model, with a "complete" `PurgeReport`.
- **Validation:** a catch-up with a slow projection; purge a space whose aggregates are behind; compare documents after
  the report.
- **Suggested fix:** refuse `PurgeCheck` while `catchingUp` is set (as for builds); and have `applyChunk` skip items of
  `purged` aggregates, as `receiveDelivery` does, which also closes the same window for targeted builders.
- **Regression test:** `PurgeCheck` answers a refusal during a catch-up; a chunk for a purged aggregate writes nothing.
- **Continues:** F202 (and R17's purge guard).

### R2-RBLD-7 — The batched delete of a retired build's subscription rows rescans what it deleted: quadratic on the supervisor's thread
- **Severity** Medium · **Likelihood** Possible (every switch of a large projection, every start that sweeps a retired
  build, every `retire`) · **Confidence** Strongly suspected (depends on the plan PostgreSQL picks and on the xmin
  horizon)
- **Where:** `SubscriptionsState.scala:447-452`:
  `DELETE FROM subscriptions WHERE id IN (SELECT id FROM subscriptions WHERE subscriber_type_id = $x LIMIT 10000)` in a
  loop; called from `Projections.scala:745` (supervisor thread, after a switch and in the start sweep) and `:72`
  (`retire`, under the global `SchemaLock`).
- **Mechanism:** each batch's subquery starts at the beginning of the subscriber's index range (or of the heap, with a
  sequential scan) and walks every entry deleted by earlier batches before it finds live ones. Dead entries stay until
  vacuum; they are skipped cheaply only if they can be marked dead, which a long transaction anywhere (a targeted
  reset, F217's left part, a replica with `hot_standby_feedback`) prevents. For n rows the work is about n²/20 000
  entries: 2·10^10 for 20M rows (estimated), minutes to hours.
- **Impact:** after a switch, the projection's queries wait behind the drop on the supervisor's thread for longer than
  before the fix; at start, every message waits; in `retire`, other nodes' schema steps give up after `SchemaLock`'s 5
  minutes.
- **Validation:** 5M rows for one subscriber; run `clearSubscriptionsInfo` with an open `REPEATABLE READ` transaction in
  another session; measure batch times growing linearly.
- **Suggested fix:** keyset batches on the unique index: delete `WHERE subscriber_type_id = $x AND (subscription_type,
  aggregate_id) > ($t, $last) ORDER BY … LIMIT 10000` via a CTE returning the last key; and move the drop off the
  supervisor's thread (a future on the blocking dispatcher, result as a message).
- **Regression test:** batch duration stays flat across the batches of a large clear.
- **Continues:** F217 (its partial fix).

## Lesser items
- **F189 continues:** only the index trigger is fixed. An active instance whose constructor fails for another reason
  under the new code still stops the whole projection every `restartAfter` (`Projections.scala:716-736`), queries stash
  without bound in `starting` (`:307`), and the build never starts.
- **F203 + `ServeStale`:** a build refused for a long name (`ProjectionBuild.scala:48-52`), including one started
  automatically for a changed context (`Projections.scala:360-366`), loops every `restartAfter`; with `ServeStale` the
  active build stays frozen with no way to cancel the build.
- **Stale `BuildCaughtUp` (Hypothesis, now three senders):** the full-build case (`Projections.scala:426-430`) does not
  check `builder.contains(sender())`, and `rebuild()` (`:659-669`) does not clear `caughtUp`. A catch-up's
  `BuildCaughtUp` stashed during a restart (`:716-736` does not stop the catch-up or clear `catchingUp`) sets `caughtUp`
  with no build; a later `RebuildProjection`'s build could then switch at the next `trySwitch` before its bulk pass. No
  realistic trigger found: instances rarely stop by themselves.
- **Catch-up unchecked contexts:** `catchUp` (`Projections.scala:633`) skips `startBuilder`'s missing-context check
  (`:486-489`); a state listener without its context records a failure per aggregate behind.
- **Catch-up takes no build slot** (`Projections.scala:630-636`): several `ServeStale` projections rolled back at once
  reconcile all their aggregates together.
- **Catch-up not visible:** `GetBuildStatus` reports `NoBuild` during a catch-up (`Projections.scala:471-474`); the book
  says to wait for it, but only a log line tells when it ends.
- **Needless catch-up after a switch:** a catch-up whose active build retires during it (a `RebuildProjection` allowed
  meanwhile, `:659-669`) is restarted on the new active build (`:682-686`), a full walk for nothing.
- **Active job across a switch:** a running active job whose old build retires is restarted by `RunTargetedBuilder`
  against the new active build without a reset (`Projections.scala:638-643,687-690`): harmless, but it re-reads the
  whole target (all of a type for `OneType`).
- **Status.Failure match too wide:** `Projections.scala:447` treats any `Status.Failure` from the job's instance before
  its builder starts as the reset's; nothing else the supervisor sends answers one today.
- **F235 residual:** a switch answered `false` with another build active (a second supervisor) still only logs
  (`Projections.scala:527-529`); the projection stays stuck until restart.
- **A targeted job of an active build serving stale runs a builder** (`Projections.scala:598-600,638-643`), although the
  comment says it only clears the target; with gaps it asks a republisher it does not have (`eventBus = None`,
  `ProjectionActor.scala:580,752`) and may record false "versions missing" failures.

## Fixes checked and found sound
- **F203:** names checked against 63 characters before anything is recorded (`ProjectionBuild.scala:24-52`); the
  store checks its index names too (`PostgresDocumentStore.scala:64-68`). Names are ASCII, so characters equal bytes.
- **F189 (index trigger):** `OutdatedTables` is keyed by the build's table name, marked before the store is made in
  the same constructor; build names never repeat while a record of the version exists.
- **F235:** a lost commit acknowledgement is found done through the registry and completed in memory.
- **F223 (memory):** at most the chunks in flight, one more, and one aggregate's backlog are held; gaps go first;
  positions are saved only for bulk chunks. (Its transient handling: R2-RBLD-5.)
- **F192 (structure):** jobs are kept in order in the registry, restored at start, de-duplicated while waiting,
  restarted with the build instance, filtered when their build ends, and block the switch while waiting. (Gaps:
  R2-RBLD-1, R2-RBLD-4.)
- **F217 (index on `space_id`):** created with new tables, `CONCURRENTLY` for older ones.
- **F247 (part):** the catch-up and targeted rebuilds use the existing subscriber name; only builds take a new id.
- **F116:** the start sweep drops builds not active or in progress, versions before tables, so a stop between them is
  repeated.
