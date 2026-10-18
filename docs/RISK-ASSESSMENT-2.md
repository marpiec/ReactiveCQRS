# Second risk assessment of ReactiveCQRS (1.0.1-SNAPSHOT)

How the library could fail now, after the fixes of [the first assessment](RISK-ASSESSMENT.md): bugs, corruption, lost
erasures, hangs, unbounded growth, performance cliffs and security problems, under realistic, extreme and hostile
conditions. Made on 18/10/2026 at the owner's request, from `master` at `2453711` (1.0.1-SNAPSHOT). Nothing was fixed.

**How it was made.**

- Nothing was run. Every finding comes from reading the code, the docs, the `v1.0.0` tag and the sources of the
  dependencies (ScalikeJDBC 4.3.5, Pekko 1.7.1, mpjsons 0.7.0).
- The same ten areas as the first assessment were read in parallel, each against the 274 issues already known
  (KNOWN-ISSUES.md, its archive, the first assessment and its working notes). The reading focused on the ~5 000 lines
  that the fixes of F185–F274 changed since 1.0.0, and asked of each fix whether it closes its scenario, and what it
  breaks. Only what is new is reported; a fix found incomplete gets a new number that names the one it continues.
- Every High finding was then checked again by an independent verifier that tried to refute it (three verifiers;
  F275, F279 and F282 were also checked by hand). All of them held; seven were adjusted, mostly to a lower likelihood,
  and the adjustments are applied here. The Medium and Low findings were not verified again.
- The findings of several areas that describe one problem are merged: 67 findings in the notes are 61 here.

**Confidence and numbers.** As in the first assessment: **Confirmed (code)** (the path was traced end to end),
**Strongly suspected** (it also depends on a dependency's documented behaviour, on timing or on configuration),
**Hypothesis**. In KNOWN-ISSUES' terms all are "likely" (confirmed in code) or "suspected"; nothing is reproduced by a
test yet. Numbers are estimates unless they name their source; `docs/perf` and SCALING.md hold the measurements.

**Where the findings live.** F275–F335, recorded open in [KNOWN-ISSUES.md](KNOWN-ISSUES.md) §3; each area's working
notes, with the full traces and the lesser items, are in [risk-assessment-2/](risk-assessment-2/) (§7).

## 1. Executive summary

**The core still holds.** No finding loses committed events, breaks the optimistic lock or reorders an aggregate's
events. Most of the first assessment's fixes were found sound (§5).

**But some fixes have gaps, and a few made things worse.** 38 of the first assessment's fixes have a residual here,
and five findings are regressions that 1.0.0 did not have:

- **F275:** since F195, a command whose events put an undo before another event is always refused on PostgreSQL. It
  worked in 1.0.0, and the memory store hides it.
- **F279:** F218's paced passivation stops at most 200 owners a second, so under sustained traffic the owners of a
  `Local` type grow without bound. 1.0.0 stopped them all at once.
- **F314:** F240's reader refuses values sent on their own that are not case classes (a `LocalDate`, an array, an
  enum), which 1.0.0 read, and drops them silently, also during the rolling upgrade.
- **F304:** since F201, a "versions missing" failure can no longer be skipped past versions that can never arrive.
- **F312:** since F223, a transient database error during a reconciliation stops the builder, which starts again
  from the beginning.

| Severity | Count | Of which likely or possible |
|---|---|---|
| Critical | 0 | — |
| High | 13 | 7 |
| Medium | 41 | 24 |
| Low | 7 | 5 |

**Why no Critical finding.** As before: none loses committed events in normal operation, and none can be exploited
from outside without a mistake in the host's code.

**The High findings fall into six themes:**

1. **Regressions of 1.0.1** (F275, F279, F282). F282 is the quick win of this morning: the F202 catch-up follows the
   rolled-back code's `whileBuilding`, and the book's pattern puts `ServeStale` only on the new version, so the usual
   rollback gets no catch-up.
2. **Snapshots that restore a wrong state silently** (F276, F277). F196's shape check misses fields of subclasses held
   through a trait, and during the 1.0.0 → 1.0.1 rollout a 1.0.0 node's snapshot keeps a 1.0.1 shape tag.
3. **Erasures reported done while the data stays** (F280, F283; Medium: F296, F297, F311, F313, F324, F326). A wrong
   bus, a failed queued rebuild, a deleted base's lineage: each lets `HistoryRewriter` or a build report success.
4. **Sagas** (F284, F285, F286). Step re-runs, on by default since F205 and for `SagaActor` too, resend commands; at
   Neula, whose commands have no idempotency keys (H3), a step applied but answered as failed is applied again. A
   lost commit acknowledgement stops a saga for good, and a failover longer than the retries reverts a committed step.
5. **Start-up** (F278, F287). Schema-lock waiters hold pooled connections, so many projections starting at once stall
   the blocking dispatcher; and in E2 a start that fails after the actor system exists leaves a live member that is
   never ready, so a database outage at start outlasts its cause.
6. **Managed projections** (F281, F283). A targeted rebuild during a build that started with no active build, under
   `ServeStale`, makes the only instance acknowledge every live event without applying it.

**Release advice.** F275, F279, F282 and F314 are regressions of 1.0.1 and should be fixed before it is released;
F277 and F314 matter only during the rolling upgrade from 1.0.0, so they must be fixed before it, or the upgrade
notes must say how to avoid them. F285 needs at least an upgrade warning (§6).

## 2. All findings

Sorted by severity, then by area. "Notes" names the finding in the working notes; "Continues" the issue whose fix it
completes. L/P/U: Likely, Possible, Unlikely. C: Confirmed (code); S: Strongly suspected.

| F | Severity | Likelihood | Conf. | Finding | Notes | Continues |
|---|---|---|---|---|---|---|
| F275 | High | P | C | A command whose events put an undo before another event is always refused on PostgreSQL | OWN-1, STORE-1 | F195 |
| F276 | High | P | C | The snapshot shape ignores subclasses held through a trait or abstract class: a field added to one is restored as zero, false or null | OWN-3 | F196 |
| F277 | High | U | C | During the 1.0.0 → 1.0.1 rollout, a 1.0.0 node's snapshot update keeps the shape a 1.0.1 node wrote | STORE-2, SYS-5 | F196 |
| F278 | High | P | C | Schema-lock waiters each hold a pooled connection: many store set-ups at once make lock holders fail in turn and saturate the blocking dispatcher | DOCS-1, SYS-2 | F272 |
| F279 | High | P | C | F218's pacing stops at most 200 owners a second: under sustained traffic `Local` owners grow without bound | BUS-2 | F218 |
| F280 | High | U | C | An erasure sent to another type's bus runs with the wrong context and is reported complete | BUS-5 | F197, F245 |
| F281 | High | U | C | A targeted rebuild during a build that started with no active build, under `ServeStale`, makes the only instance acknowledge every live event unapplied | RBLD-1 | F192 |
| F282 | High | P | C | The F202 catch-up follows the rolled-back code's `whileBuilding`: the usual rollback gets none | RBLD-2 | F202 |
| F283 | High | U | C | A build's queued targeted rebuild that fails at its reset is dropped, and the build switches in with the erased values | RBLD-4 | F192 |
| F284 | High | U | C | A saga's compare-and-set miss after its own write whose acknowledgement was lost stops the saga; its caller is never answered | CLSG-1 | F206, F232 |
| F285 | High (at Neula; Medium elsewhere) | P | C | Step re-runs, on by default and for `SagaActor` too, resend commands: without keys, a step applied but answered as failed is applied again | CLSG-2, SYS-6 | F205, H3 |
| F286 | High | U | C | A step's retries span about 15–40 s: a longer failover reverts a step whose command committed, and a revert in the same outage gives up silently | CLSG-3 | F205 |
| F287 | High | P | C | A start that fails after the actor system exists leaves a live, never-ready member (E2): an outage at start outlasts its cause | SYS-1 | F233, F188 |
| F288 | Medium | U | C | A non-success answer to a materialization, an unknown outcome included, unfreezes the draft, so F208's lost edits come back | OWN-2, BUS-1 | F208, F253 |
| F289 | Medium | P | C | A plain squash keeps the history generation: stale owners and snapshots below the squash point are carried forward through the squash event | OWN-4 | F214, F237 |
| F290 | Medium | P | C | Conflicts with another node and transient failures replay the whole aggregate; a pool timeout doubles the wait | OWN-5 | F214, F253 |
| F291 | Medium | U | C | F250's containment covers handlers and restores only: a `StackOverflowError` elsewhere in the owner can end the JVM | OWN-6 | F250 |
| F292 | Medium | P | C | A materialization that succeeds after its timeout tells nobody, and the new id cannot be found | BUS-3 | F208 |
| F293 | Medium | P | C | Event-history and archive reads are not type-checked: another type's events and envelopes are returned | BUS-4, STORE-4 | F197 |
| F294 | Medium | P | C | A passivation during a materialization makes the bus hold every message for the draft, unanswered and unbounded | BUS-6 | F208 |
| F295 | Medium | U | C | A sequence step lowered after a pool was drawn still gives the same ids twice | BUS-7 | F204 |
| F296 | Medium | P | C | The event size limit applies to the detach and squash events the library writes: a base whose copy is larger cannot be deleted for good or purged | STORE-3 | F244 |
| F297 | Medium | U | C | An erasure of an unreadable event rewrites its JSON but not its metadata envelope, and reports it rewritten | STORE-5 | F245 |
| F298 | Medium | U | S | Lost messages keep a subscriber's window shut for good when its traffic is on aggregates held behind | DLV-1 | F221 |
| F299 | Medium | P | C | Kept aggregates are re-published at 200 a second per node, outside the poller's budget, when a late singleton answers | DLV-2, SYS-7 | F188 |
| F300 | Medium | U | C | A slow outbox read is abandoned after `readTimeout` and started again: a large outbox is never polled, and its scans pile up | DLV-3 | F186, F260 |
| F301 | Medium | P | S | `Errors.transient` misclassifies: statement timeouts and other repeating errors are retried for ever without a record; a read-only node records failures for healthy listeners | PROJ-1, DOCS-6, SYS-9 | F254 |
| F302 | Medium | P | C | F259 applies live events of an aggregate at subscription version 0 as history, so pushes and side effects skip them | PROJ-2 | F259 |
| F303 | Medium | P | C | A group that fails on the database is run again one batch at a time, each waiting out the pool on the projection's thread | PROJ-3 | F254, D61 |
| F304 | Medium | U | C | Since F201, a "versions missing" failure cannot be skipped past versions that can never arrive (an event over the frame between nodes) | PROJ-4 | F201, F121 |
| F305 | Medium | P | S | The batched delete of a retired build's subscription rows rescans what it deleted: quadratic while vacuum is held back | PROJ-5, RBLD-7 | F217 |
| F306 | Medium | L | S | Offset paging has no stable order on PostgreSQL (no tie-break, none at all unsorted); the memory store's order hides it | DOCS-2 | F211, F273 |
| F307 | Medium | P | C | F211's cap counts rows only after the driver has buffered and mpjsons has read up to 100 001 documents | DOCS-3 | F211 |
| F308 | Medium | U | C | A read's deferred cache write overwrites a newer absence or presence: deleted documents come back, inserts fail until a restart | DOCS-4 | F266 |
| F309 | Medium | P | C | An empty value list makes invalid SQL on PostgreSQL and an empty result in memory | DOCS-5 | — |
| F310 | Medium | U | C | Retiring a projection drops its tables under the schema lock with no lock timeout | DOCS-7 | F185 |
| F311 | Medium | U | C | After a rollback, aggregates deleted for good during a `ServeStale` build keep their documents in the active build | RBLD-3 | F202 |
| F312 | Medium | P | C | Since F223, a transient error during a reconciliation stops the builder, which starts again from the beginning | RBLD-5 | F223 |
| F313 | Medium | U | C | A purge is not refused while the F202 catch-up runs, whose chunks can write a purged organization's documents back | RBLD-6 | F202 |
| F314 | Medium | P | C | F240's reader refuses values sent on their own that are not case classes (java.time, plain classes, arrays, enums), which 1.0.0 read | CLSG-4 | F240 |
| F315 | Medium | P | C | Finished keyed sagas' rows stay 30 days with their last order: the table grows, every load reads them, erasure does not reach them | CLSG-5 | F207 |
| F316 | Medium | U | C | A rollback to 1.0.0 after keyed sagas finished leaves the pending sagas of that name unresumed | CLSG-6 | F252, F151 |
| F317 | Medium | P | C | F227 stops early only for library values: one large application value is still serialized whole | CLSG-8 | F227 |
| F318 | Medium | P | C | A keyed saga that failed keeps its failure as the answer for 30 days, so a natural key cannot be retried | CLSG-9 | F207 |
| F319 | Medium | P | S | The export replays every detached copy's base inside its open snapshot, which can outlast the book's idle-in-transaction timeout | LIFE-1 | F225, F215 |
| F320 | Medium | U | S | F248's range check reads a NULL `last_value` for a sequence never drawn from, and protects installation 0's range instead | LIFE-2 | F248 |
| F321 | Medium | P | C | An import remaps user ids through the aggregate-id map | LIFE-3 | F248 |
| F322 | Medium | U | C | References to aggregates that were not exported are accepted when the target has that id, whatever its organization | LIFE-4 | — |
| F323 | Medium | P | C | The import logs its whole report, which since F248 can hold every aggregate | LIFE-5 | F191, F248 |
| F324 | Medium | U | C | A permanent delete or purge deletes the lineage it has just written: a later erasure of the base reports complete while the copies keep the values | LIFE-6 | F193 |
| F325 | Medium | P | C | Nothing detects writes or erasures made in the source between an export and its purge | LIFE-7 | — |
| F326 | Medium | U | C | A rewrite refuses any change to an undo or duplication event, so a value in one cannot be erased | LIFE-8 | — |
| F327 | Medium | P | S | `Ddl`'s retry loop stalls the table's traffic cluster-wide for about 90 s behind a long lock; the book says other nodes keep working | SYS-3 | F185 |
| F328 | Medium | U | S | A document index built at start is bounded only by `socketTimeout`: a build over 10 minutes is retried at every start | SYS-4 | F186, F185 |
| F329 | Low | P | C | Every command of a sharded type is serialized whole on the bus's thread to be measured | BUS-8 | F227 |
| F330 | Low | L | C | `InFlight` keeps an entry per send for `stallAfter`, also for deliveries answered at once | DLV-4 | F212 |
| F331 | Low | P | S | The state of every publication to a state listener on another node is measured on the bus's only thread | DLV-5 | F222 |
| F332 | Low | U | C | `Ddl.has` follows the search path and counts an invalid index as present | DOCS-8 | F185 |
| F333 | Low | P | S | `buildSpaceIndexesConcurrently` takes no lock: two runs drop each other's index in progress | DOCS-9 | F217 |
| F334 | Low | U | C | A load can run once more one step of a saga that ended while the load's query ran | CLSG-7 | F232 |
| F335 | Low | L | C | The `bus.behind` gauge gets a new series for every managed instance | SYS-8 | F258 |

## 3. The High findings

#### F275 — A command whose events put an undo before another event is always refused on PostgreSQL
- **Where:** `PostgresEventStoreState.persist` (undo and duplication events go one by one through `persistOne`),
  `add_undo_event` (raises `history_generation`), `add_events` (compares it with the command's `_expected_generation`).
- **Mechanism:** within one transaction the undo raises the generation; the next event's `add_events` sees a
  generation other than the one the command was read at and raises "its history changed". The owner restores and
  retries (`ConcurrentCommand`, for ever) or answers a conflict. The memory store checks once per command, so no spec
  sees it. Verified by hand.
- **Impact:** a command shape that worked in 1.0.0 fails always. Regression of F195.
- **Fix:** pass the generation the undo leaves (or skip the check after the command's own undo); a spec on PostgreSQL.

#### F276 — The snapshot shape ignores subclasses held through a trait or abstract class
- **Where:** `StateShape.scala:33-47`: only declared field types are visited.
- **Mechanism:** a state field of a trait type (an ADT that mpjsons writes with its class through `markTypedClass`)
  contributes only the trait's name; a field added to one of its subclasses leaves the shape unchanged, and the
  snapshot is read with that field as zero, false or null. The book (chapter 13) says a field added anywhere needs
  nothing from the user. Verified.
- **Fix:** include the known subclasses (sealed hierarchies, the classes registered with mpjsons), or mark the shape
  unknown for abstract fields and skip the snapshot.

#### F277 — During the 1.0.0 → 1.0.1 rollout, a 1.0.0 node's snapshot update keeps the shape a 1.0.1 node wrote
- **Where:** 1.0.0's `writeSnapshot` upsert (`v1.0.0:PostgresEventStoreState.scala:236-241`) sets every column but
  `shape`; 1.0.1's `AggregateRestorer` trusts it.
- **Impact:** F196's silent wrong state returns when a state class changes in the same deploy. Unlikely after
  adjustment: snapshots on, a changed state class without a raised `schemaVersion`, and the one-time mixed window.
- **Fix:** upgrade notes (raise `schemaVersion` with 1.0.1, or delete snapshots after the rollout), or a shape the
  old upsert cannot leave behind (a new table or a version column the old code nulls).

#### F278 — Schema-lock waiters each hold a pooled connection
- **Where:** `SchemaLock.scala:26-46` borrows a connection, then polls `pg_try_advisory_lock` on it for up to
  5 minutes; the holder's work (`PostgresDocumentStore` set-up, `Ddl`) borrows more. Instances are built on the
  32-thread blocking dispatcher; the book, E1, E2 and the test kit use a pool of 32.
- **Impact (verified, adjusted):** not a loop but a stall: about one holder fails every 3 s (the pool's checkout
  timeout) while 32 or more set-ups wait, the failed instances come back after 10 s, and meanwhile the blocking
  dispatcher is saturated, so owners and commands stall too. Singleton supervisors put every instance on the oldest
  node.
- **Fix:** wait for the lock without holding a pooled connection (a dedicated connection, or one lock connection per
  JVM that serializes the waiters in memory).

#### F279 — F218's pacing stops at most 200 owners a second
- **Where:** `Owners.scala:43, 106-116`: `StopsPerSweep = 200`, the rest a second later.
- **Mechanism:** with more than 200 new aggregates a second per type and node, sustained, more owners are created
  than stopped, and each 1-s sweep sorts the whole activity map on the bus's thread. The load bench measured
  4 269 new aggregates/s (`docs/perf/load-M5.md`); at 1 000/s the owners grow by about 2.9 million an hour
  (estimate). 1.0.0 stopped everything due at once. Verified by hand.
- **Fix:** pace by the snapshot writes, not by the stops (stop without a snapshot beyond the pace), or scale the pace
  with the backlog; keep the sweep's sort incremental.

#### F280 — An erasure sent to another type's bus runs with the wrong context and is reported complete
- **Where:** `AggregateOwner.scala:265` (a failed restore still takes `Rewrite`), `rewriteEverywhere` (no type
  check), `HistoryRewriter.scala:73` (`HistoryRewritten(0, 0)` counts as unchanged).
- **Impact:** an erasure (GDPR Art. 17, Regulation (EU) 2016/679) reported complete while nothing was erased.
  Unlikely: it needs a host routing mistake or a duplication across types.
- **Fix:** refuse `Rewrite` in `failedRestore` with a type mismatch, check the type in `rewriteEverywhere`, and make
  the rewriter count a refusal as incomplete.

#### F281 — A targeted rebuild during a build that started with no active build, under `ServeStale`
- **Where:** a build with no active build becomes active at once (`ProjectionRegistry.scala:150-152`), one instance
  serves both; `Projections.scala:565-600` treats the job as an active one and sends that instance `ServeUnchanged`;
  `ProjectionActor.scala:593-595` then acknowledges every delivery unapplied, permanent deletes included, until a
  restart (`Activate` does not clear it).
- **Fix:** never send `ServeUnchanged` to the building instance; clear it on `Activate`.

#### F282 — The F202 catch-up follows the rolled-back code's `whileBuilding`
- **Where:** `Projections.scala:321`: `catchUpDue = spec.whileBuilding == ServeStale && …`, where `spec` is the code
  now running. Whether the active build served unchanged was decided by the abandoned version's code. The default is
  `ServeLive` (`:51`) and the book puts `ServeStale` on the new version only, so after the usual rollback there is no
  catch-up: F202 again. The F202 spec passes because both its versions declare `ServeStale`. Verified by hand.
- **Also:** the catch-up is not recorded, so a stop during it loses it (F202's entry says so).
- **Fix:** catch up after every abandoned build (the reconciliation is cheap where nothing was missed), or record
  `whileBuilding` with the build; record the catch-up as a job.

#### F283 — A build's queued targeted rebuild that fails at its reset is dropped
- **Where:** `Projections.scala:447-454` drops the job on a `Status.Failure` from the reset (`targeted = None`,
  `saveJobs()`); `:501` then lets the build switch in. The rewriter has already counted the projection as rebuilt.
- **Impact:** erased values served again after the switch. Unlikely (a transient failure at that moment).
- **Fix:** keep the job and retry it after `restartAfter`; never switch a build with a failed job.

#### F284 — A saga stops when its own write's acknowledgement was lost
- **Where:** `SagaRunner.scala:336-346`: a compare-and-set miss always stops the saga ("this copy stops"); a retried
  write whose first attempt did commit misses the same way (`PostgresSagaState.scala:52-72`).
- **Impact:** the saga runs nowhere until the runner starts again, its caller is never answered, an unkeyed retry
  starts a second saga. Unlikely (a lost acknowledgement, or the race at a hand-over).
- **Fix:** on a miss, read the row: if it holds the position just written, go on.

#### F285 — Step re-runs resend commands
- **Where:** `SagaRunner.scala:239-266`: a step runs again after any failed or throwing future, up to 5 attempts,
  by default (`SagaDefinition.scala:48, 62-63`), `SagaActor` sagas included.
- **Impact:** a `ConcurrentCommand` or `FirstCommand` without an idempotency key, applied but answered as failed (a
  timeout, an unknown outcome, a mapping that threw), is applied again. The book documents that steps need keys, but
  Neula's commands have none (H3), and the CHANGELOG has no upgrade warning. High at Neula while H3 is open.
- **Fix:** re-run only steps whose command is keyed (or opt in per step); warn in the upgrade notes.

#### F286 — A step's retries span about 15–40 s
- **Where:** `SagaDefinition.scala:63` (waits of 1+2+4+8 s), `SagaRunner.scala:266-283`.
- **Impact:** a failover longer than that reverts a step whose command committed with an unknown outcome (F253's
  answer); the reverts get the same five attempts and the saga then finishes without them. Unlikely.
- **Fix:** treat an unknown outcome apart: retry it until the database answers (with a cap in hours), never revert on
  it.

#### F287 — A failed start leaves a live, never-ready member
- **Where:** E2's `ClusterSetup.scala:112-117` starts the actor system, Management and Bootstrap before `startNode`,
  and nothing ends the JVM when the start fails; `/alive` answers 200 for ever.
- **Impact:** with the database down at a full start, every node stays stuck and is never restarted, so the outage
  outlasts its cause; a stuck oldest member keeps the singletons and shard coordinators from running anywhere.
- **Fix:** E2 (and the book's host step) ends the JVM, or fails liveness, when the start fails.

## 4. Themes among the Medium findings

- **Drafts (D77):** F288, F292, F294: the F208 data loss returns through unknown outcomes and passivation, and a
  late success answers no one.
- **Error classification (F254, F253):** F301, F303, F290: what counts as transient decides between retrying for
  ever unrecorded, recording healthy listeners, and replaying whole aggregates.
- **Erasure and organizations:** F296, F297, F311, F313, F315, F319–F326. F324 is a design decision (M6 deletes the
  lineage with the aggregate): it needs a choice before a fix.
- **Scale:** F299, F300, F305, F306, F307, F330 — each needs a measurement before its likelihood is known.
- **The cluster serializer:** F314 (a regression, also during the rolling upgrade) and F317 (F227's gap).

## 5. The first assessment's fixes

**Found incomplete or wrong** (each continued above): F185, F186, F188, F192, F193, F195, F196, F197, F201, F202,
F204, F205, F206, F207, F208, F211, F212, F214, F217, F218, F221, F222, F223, F225, F227, F232, F240, F244, F245,
F248, F250, F253, F254, F258, F259, F260, F266, F272.

**Found sound** by at least one area (some with a residual above): F187, F189, F190, F191 (in most places), F193,
F194, F197 (restores), F198, F199, F200, F203, F209, F210, F213, F216, F219, F220, F224, F226, F228, F229, F230,
F231, F233, F234, F235, F236, F237, F241, F242, F244, F245, F246, F247, F249, F251, F252, F255, F256, F257, F261,
F262, F263, F264, F265, F267, F268, F269.

## 6. Recommended order

1. **Before 1.0.1 is released** (regressions and the rolling upgrade): F275, F279, F282, F314, F277 (or its upgrade
   note), an upgrade warning for F285, and F287's E2 fix.
2. **Erasure reported done while the data stays:** F280, F283, F281, F324 (decision first), F296, F297, F311, F313.
3. **Silent wrong state:** F276, F289, F308.
4. **Sagas:** F284, F285, F286, F318, F315.
5. **Start-up and operations:** F278, F327, F310, F328, F301, F303.
6. **Drafts:** F288, F292, F294.
7. The rest, scale findings after measuring them.

Every fix should come with a spec on PostgreSQL: several of these (F275, F306, F309) hide behind the memory doubles
(F274).

## 7. Questions for Neula, measurements and the working notes

**Facts needed from Neula:**

- whether any context's command returns an undo followed by another event (F275);
- which snapshot-enabled states hold trait-typed fields (F276), and whether a state class changes in the 1.0.1 deploy
  (F277);
- the new aggregates per second per type and node at peak (F279);
- `ServeStale` use (F281, F282), and saga steps whose commands are concurrent or first commands (F285);
- `statement_timeout` on the application role (F301), and values sent between nodes on their own that are java.time
  values, arrays or enums (F314);
- how many document stores and managed projections start on one node (F278).

**Measurements needed:** the passivation backlog at 1 000 new aggregates/s (F279); the start of 40 projections with a
pool of 32 (F278); the retired-build drop with 10 million rows under a held snapshot (F305); paging a large model by
offset (F306).

**The working notes**, with the full traces and the lesser items:

- [aggregate owner](risk-assessment-2/owner.md) (OWN)
- [command bus and ids](risk-assessment-2/command-bus.md) (BUS)
- [event store](risk-assessment-2/event-store.md) (STORE)
- [delivery](risk-assessment-2/delivery.md) (DLV)
- [projections](risk-assessment-2/projections.md) (PROJ)
- [document store and configuration](risk-assessment-2/document-store.md) (DOCS)
- [rebuilds](risk-assessment-2/rebuilds.md) (RBLD)
- [cluster and sagas](risk-assessment-2/cluster-and-sagas.md) (CLSG)
- [data lifecycle](risk-assessment-2/data-lifecycle.md) (LIFE)
- [cross-cutting](risk-assessment-2/cross-cutting.md) (SYS)

The notes' severities are the reviewers'; where a verifier adjusted one, this report's (§2) applies.
