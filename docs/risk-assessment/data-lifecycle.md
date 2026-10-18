# Working notes: data lifecycle

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (LIFE-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

- `core/.../space/`: `SpaceImport.scala`, `SpaceExport.scala`, `SpaceFormat.scala` and `SpacePurge.scala`, every line.
- `core/.../rewrite/HistoryRewriter.scala`, and the paths it drives:
  - `AggregateOwner`: `unrestored`/`restored`, `attempt`, `runHandler`, `persistOrAnswer`, `persist`, `squashHistory`, `rewriteHistory`, `materialize`, `caughtUp`.
  - `AggregateCommandBusActor.main`: `RewriteEverywhere`, `ForgetAggregates`, `HistoryChanged`.
  - `PostgresEventStoreState`: persist, `overwriteEvents`, restores, snapshots, `readCommittedPage`, purge, squash, `rewriteEverywhere`, `copiesOf`, detach.
  - `PostgresEventStoreSchemaInitializer`: tables, indexes and the three procedures.
  - `AggregateRestorer`; the `Projections` supervisor (`PurgeCheck`, `PurgeSpace`, `RebuildAggregates`); `ProjectionActor`'s purge handlers and `giveUp`; `SubscriptionsState.resetVersions`; `ProjectionRegistry` subscriber names; `SquashScheduler`; uid `IdRange`/`PostgresUidGenerator` (grep only).
- `tools/StoredDataCheck.scala`; `types/TypesNamesState.scala` and every caller (grep).
- `testkit`: `TestDatabase`, `TestSystems`, `TestCluster`, `TestIds`, `TestClock`, `ActorAskSupport`, `SchemaReference`, `CommonSpec`. Also the bench's `BenchDatabase` (its schema names).
- mpjsons 0.7.0 sources in `C:\Marcin\Projects\mpjsons`: `BeanDeserializer`, `ObjectConstructionUtil`, `MPJsons.extractTypeFromName`, `ErrorMessageFormatter`, `TypedConverter`.
- Docs:
  - CLAUDE.md; ARCHITECTURE §4.1–§4.10 and §13.5; `docs/design/M6-organizations.md`; SCALING.md §2–§4.
  - KNOWN-ISSUES.md (decisions, H11, reviews 8–9) and the archive entries F8, F65, F78, F126, F131, F144–F147, F150, F167, F181.
  - Git history of `types_names_seq` and `events_to_route`.
- **Not examined:**
  - hosts' `clearForRebuild` code, and how `PostgresDocumentStore`'s cache is cleared when a space is cleared;
  - `FlowControl`'s held entries of purged aggregates;
  - the response-retention job, and the saga runner's handling of a deleted saga row;
  - the bodies of `ExportImportSpec`, `PurgeSpec` and `CompleteRewriteSpec`.
  
  Nothing was built or run, per the rules.

## 2. Findings

### LIFE-1 — Erasure: a run that could not rebuild a projection reports "complete" when run again, and the projection keeps the erased value
- **Ratings:** Severity High · Likelihood Likely · Confidence Confirmed (code)
- **Location:** `HistoryRewriter.scala:49` (`rebuild(walked.rewritten.map(_.id), …)`), `:67-69`, `:78-93`, `:25`; `Projections.scala:512-514` (`RebuildRefused` while a build or a targeted rebuild runs); `AggregateOwner.scala:780-787`.
- **Preconditions:** an erasure through `HistoryRewriter` with at least one managed projection given.
- **Triggering scenario:** any of these:
  - (a) A full build of projection P is running. Builds follow version changes, and SCALING.md:183 estimates about 4.1 h per 100M events.
  - (b) Two erasures run at once. The second one's `RebuildAggregates` is refused with "the rebuild of … is in progress".
  - (c) The 5-minute ask times out.
  - (d) The node stops between the walk and the rebuild.
- **Failure mechanism:**
  - Run 1 commits the rewrites, then P refuses. The report gets `failed = Map(P -> …)`, and the fold stops for P at its first failure.
  - Run 2 is the documented remedy ("Run it again for the rest"). It finds nothing to change: `case _: HistoryRewritten => unchanged`, so `rewritten` is empty. `rebuild(Nil, …)` folds over an empty list, which yields `None`, so every projection is appended to `rebuilt` and `complete == true`.
  - In case (a) the refusal text says the new build "replaces the active one whole". But the builder reads events by id (`readCommittedPage`, `PostgresEventStoreState.scala:333-347`), and the rewrite updates rows in place. A build already past those ids keeps the old values and becomes active at the switch.
- **Expected impact:** personal data stays in read models after a report that says complete (GDPR). Hard to detect: only `history_rewrites` shows the change.
- **Validation:** use a probe projection that answers `RebuildRefused` the first time. `rewrite(A)` gives `failed` non-empty. `rewrite(A)` again gives `complete == true`, and the probe got no second `RebuildAggregates`.
- **Recommended fix:**
  - Make the rebuild duty durable, for example per audit row and projection. Or rebuild every visited aggregate (rewritten ∪ unchanged) on each run; a template with 10 000 copies takes minutes (estimate).
  - Let the supervisor queue targeted rebuilds, including onto a build in progress, instead of refusing them.
  - Never count a projection as rebuilt when nothing was asked of it.
  - Trade-off: more rebuild work on reruns.
- **Regression test:** the Validation scenario must rebuild A in P on the second run. Also: two rewriters at once, on two aggregates, with one projection; both aggregates must be rebuilt.

### LIFE-2 — Erasure: a command whose handler is running when the rewrite commits writes the erased value back
- **Ratings:** Severity High · Likelihood Possible (a narrow window, wider with async handlers and on other nodes) · Confidence Confirmed (code)
- **Location:**
  - `AggregateOwner.scala:281-282`: `Rewrite` is run with no check of running handlers. Compare `:740-741`, where `Materialize` refuses while commands are under way.
  - `:419-443`: the handler runs as a Future on the state of that moment; rewrite commands read their events at `:428`.
  - `:467`: only `baseVersion != version` detects a change.
  - `:770-787`: the rewrite keeps the version and `restore()` changes `root`.
  - `PostgresEventStoreSchemaInitializer.scala:293`: `add_events` checks `base_version` only, not `history_generation`.
- **Preconditions:** a command (concurrent or serial mode) is in flight on the aggregate, and its event copies the erased field from the state.
- **Triggering scenario:** an async handler (1–5 s) builds `InvoiceSent(email = state.email)`, and `RewriteEverywhere` arrives meanwhile.
- **Failure mechanism:**
  - The rewrite commits: generation +1, version V unchanged. The owner restores the erased state.
  - `HandlerFinished`: V == V, so the owner persists. `add_events(expected V)` succeeds, and event V+1 carries the old value. It is published, including after HistoryRewriter's rebuild.
  - A `RewriteHistoryCommand` in flight is worse: `overwriteEvents` (`:496`) writes back events derived from the rows read before the erasure.
  - Another node's `Local` owner: F145's `caughtUp()` runs before the handler. A rewrite that commits between that check and the commit is missed, because the procedure does not compare the generation.
- **Expected impact:** erased data is back in the event store and the projections; the report says complete. It cannot be detected without a scan.
- **Validation:** a latch-blocked async handler copies a state field; send the command, then the rewrite, then release the latch. Stored event V+1 holds the old value. Repeat with a `RewriteHistoryCommand`: the stored event reverts.
- **Recommended fix:**
  - Defer `Rewrite` (and a `SquashMayChangeState` squash) while any handler runs, bounded by `commandTimeout`. Or keep `generation` in `Pending` and treat a changed generation like a changed version (`ownConflict`, a rerun or a conflict).
  - Across nodes, check the generation at commit: an `_expected_generation` in a new `add_events` overload, or a `SELECT … FOR UPDATE` in the persist transaction.
  - Trade-off: one more key comparison per command, or a short wait.
- **Regression test:** both Validation cases, and a two-node `LocalOwnersSpec` case with the rewrite committed between the other owner's catch-up and its persist.

### LIFE-3 — SpaceExport is not a consistent snapshot; a write during the export leads to silently corrupted aggregates after import
- **Ratings:** Severity High · Likelihood Possible (a "quiet" organization still has the hourly SquashScheduler, sagas and host timers) · Confidence Confirmed (code)
- **Location:**
  - `SpaceExport.scala:37-46`: chains, constants, types and detach events are separate reads.
  - `:58-60`, then `:111`, `:125`, `:143`: three `DB.localTx` at READ COMMITTED.
  - `:62-65`: the outbox count is taken afterwards.
  - The docs say one read transaction: ARCHITECTURE.md:616, SCALING.md:210-211, design M6:344.
- **Triggering scenario:** a command commits on aggregate X (version N to N+1), or the scheduler squashes X up to U, after `writeAggregates` read X and before the events query starts.
- **Failure mechanism:**
  - The file says version N but holds events 1..N+1. The import writes `base_version = N` and every event.
  - The restore reads only `events.version <= aggregates.base_version` (`PostgresEventStoreState.scala:188`), so the owner is at N. The next command passes the version check and inserts a second version N+1; nothing makes `(aggregate_id, version)` unique.
  - Restores from a snapshot do not cap the version (`:213-218`), so the two restore paths differ.
  - The space rebuild (no cap) gives projections N+1 from the file, and the new N+1 is then acknowledged as a duplicate. The read model diverges for good.
  - Squash case: X arrives with `squashed_version = NULL` and events starting at U. The D49/D50 guards (`add_undo_event`, `add_duplication_event` `:338/:386`) compare against NULL and do nothing. Reads below U answer "did not exist", and the rebuild's gap 1..U-1 can never be filled.
- **Expected impact:** corrupt aggregates and read models in the moved organization; nothing in `ExportReport` or `ImportReport` shows it.
- **Validation:** a Writer blocks at the first event line until a command on the space commits. Import with `TestDatabase.inSchema`, then compare `base_version` with max(version) per aggregate.
- **Recommended fix:**
  - Run every read in one `REPEATABLE READ READ ONLY` transaction.
  - Build the detach events from the same snapshot: pass the session, or use `pg_export_snapshot()`.
  - Optionally re-check (base_version, squashed_version, generation) at the end and refuse if anything changed.
  - Trade-off: a long snapshot on production, as the events cursor already holds.
- **Regression test:** the Validation scenario must give a consistent file or a refusal.

### LIFE-4 — SpaceImport cannot tell a complete file from a truncated or inconsistent one
- **Ratings:** Severity High · Likelihood Unlikely · Confidence Confirmed (code)
- **Location:**
  - `SpaceFormat.scala:42-44`: no trailer line.
  - `SpaceImport.scala:96-115`: pass 1 checks format, classes, sizes, and that each event's aggregate is in the file — never the versions.
  - `:186-189`, `:208`: the file's versions are written as given.
  - `SpaceExport.scala:57-61`: a failed export leaves its lines so far. The cursor fails between lines, so the file ends on a newline.
- **Triggering scenario:** a partial file from a failed export is imported; or a transfer is cut at a newline; or the file is one of LIFE-3's.
- **Failure mechanism:**
  - The aggregate line says N, but events run only to M < N. The owner restores M.
  - Every command: expected M against stored N gives "Concurrent aggregate modification". The reload gives M again; concurrent commands fail after 10 retries.
  - A cut in the archive section drops R11 audit events silently.
  - The report says `complete`.
- **Expected impact:** aggregates unwritable until repaired in SQL; audit events lost, and the source organization may be purged after the move.
- **Validation:** cut the last lines of an export and import it. `complete` is true; every command fails with a concurrency error.
- **Recommended fix:**
  - A trailer with counts (and optionally a checksum); refuse a file without it.
  - In pass 1, per aggregate, require each version from `squashedVersion.getOrElse(1)` to `version` exactly once, in order (a `LongMap[Int]`), and archived versions ≤ version.
  - Trade-off: format version 2.
- **Regression test:** truncated events section, truncated archive section and an extra version must each be refused before any write.

### LIFE-5 — SpaceImport trusts the file's chain/lineage references and class names: an imported copy can read another organization's aggregate
- **Ratings:** Severity High (cross-tenant exposure) · Likelihood Unlikely (needs a crafted or corrupt file; the threat model includes files from other installations) · Confidence Confirmed (code)
- **Location:**
  - `SpaceImport.scala:101-107`: pass 1 never reads `a.chain` or `a.lineage`.
  - `:209-210`: rows are written with `mapped(row.base)`, and an id that was neither exported nor colliding stays as it is.
  - `:90`: `Class.forName(name)` initializes the class.
  - `:161`: any class is instantiated; mpjsons calls a public no-arg constructor or `Unsafe.allocateInstance` (`ObjectConstructionUtil.scala:31-41,66-70`).
  - `:205/:218/:227`: each name becomes a `types_names` row (SMALLINT, `TypesNamesState.scala:60`).
- **Triggering scenario:** an aggregate line with a chain row whose base is V, an aggregate of another space in the target.
- **Failure mechanism:**
  - X's restore joins `events.aggregate_id = aggregates.base_id` (`:188`), so X shows V's state, and the space rebuild publishes it to the importing organization.
  - A later squash, delete or purge of V detaches X, copying V's data into X for good.
  - Arbitrary static initializers and Scala `object` bodies run in pass 1.
  - Root classes with no command bus make the space unpurgeable (`SpacePurge.scala:101-102`).
- **Expected impact:** one tenant reads another's data; side effects of class initialization; pollution of the type ids.
- **Validation:** a crafted single-aggregate file with a chain row to an existing aggregate of another space. After import, `GetAggregate(X)` equals V's state.
- **Recommended fix:**
  - In pass 1, require chain and lineage bases to be aggregates of the file, with chain orders 1..n.
  - Load classes with `Class.forName(n, false, loader)`. Require event and archived classes to extend `Event`, and root classes to be types of contexts given to the import.
  - Report SpaceIds other than the source and 0.
  - Trade-off: the import must take the contexts, as the export already does.
- **Regression test:** crafted files must be refused in pass 1, with no class initialized and no row written.

### LIFE-6 — SpacePurge does not guard space 0 (the platform) or -1 (every legacy aggregate before `backfillSpaces`)
- **Ratings:** Severity High (irreversible, installation-wide) · Likelihood Unlikely (an operator or admin-form error) · Confidence Confirmed (code)
- **Location:** `SpacePurge.scala:57-72` (no check), `:112-126`; `PostgresEventStoreState.scala:543-547`; legacy rows default to -1 (`PostgresEventStoreSchemaInitializer.scala:134`, `SpaceId.scala:5-7`).
- **Preconditions:** the purge is called with `SpaceId(0)` or `SpaceId(-1)`, for example from an admin form whose empty value becomes 0, or from `SpaceId.unknown`.
- **Failure mechanism:**
  - `purge(SpaceId(0))` first detaches every other organization's copy of a platform aggregate (their first events are rewritten), then deletes the platform's aggregates.
  - `purge(SpaceId(-1))` deletes every aggregate stored before spaces existed.
  - Batches commit one by one.
- **Expected impact:** data loss across the whole installation, with no undo.
- **Validation:** call `purge(SpaceId(0))` on a test schema with platform aggregates; rows are deleted.
- **Recommended fix:** refuse 0 and negative spaces unless an explicit option allows them, and/or take the expected aggregate count as a confirmation argument.
- **Regression test:** `purge(0)` and `purge(-1)` are refused with no row changed.

### LIFE-7 — `PostgresTypesNamesState` takes a second pooled connection under a JVM-wide lock, and burns ids since the F78 fix
- **Ratings:** Severity Medium · Likelihood Possible · Confidence Strongly suspected (the stall); Confirmed (code) (the burn)
- **Location:**
  - `TypesNamesState.scala:82-93`: `synchronized`, `loadAll`, and `DB.localTx { INSERT … nextval … ON CONFLICT (name) DO NOTHING }` on a new connection; `:105-108`.
  - Callers inside an open transaction: `PostgresEventStoreState.scala:105,110`, within the owner's `localTx` (`AggregateOwner.scala:495-498`).
  - SCALING.md:111 recommends a pool "at least as large as" the 32 blocking threads; the default wait is 5 s (SCALING.md:33).
- **Triggering scenario:** after a deploy, under load, many owners persist a new event class at once.
- **Failure mechanism:**
  - **Stall:** each owner holds its connection while queued on the monitor. The thread inside needs one more connection; if the pool is exhausted, it waits 5 s and fails (`EventHandlingError`), and the next one repeats this. The same can happen at a node's start through `loadAll`.
  - **Burn:** the F78 fix removed the SELECT before the INSERT, and PostgreSQL draws `nextval` before it detects the conflict. Every node that first meets a name another node created burns one id.
  - **Growth:** every managed build registers a new name (`ProjectionRegistry.scala:76`). Past 32 767, any new class or build fails ("smallint out of range").
- **Expected impact:** bursts of failed commands after deploys; unmonitored consumption of the type-id space. Exhaustion is estimated at years for typical use (estimate, not measured).
- **Validation:** a pool of maxSize 2 and two owners persisting a never-seen class concurrently. Separately: two state instances, where one asks for a name the other just created; the sequence advances twice.
- **Recommended fix:**
  - Resolve type ids before `localTx`, or on the caller's session.
  - SELECT before the INSERT.
  - Report `types_names_seq` usage (in StoredDataCheck or the metrics).
- **Regression test:** both cases above, with no failure and one sequence step.

### LIFE-8 — SpaceExport builds every cross-space detach event before writing, and puts their ids in one IN list
- **Ratings:** Severity Medium · Likelihood Possible (organizations whose aggregates are copies of platform templates) · Confidence Confirmed (code); sizes estimated
- **Location:** `SpaceExport.scala:41-46` (`WHERE a.id IN ($outside)`; `outside.map(… detachEventOf …).toMap`), held until `:66`.
- **Triggering scenario:** an organization with many aggregates copied from another space.
- **Failure mechanism:**
  - N copies cost N restores and N full-state squash events, all in memory before the first line is written.
  - The IN list has N bind parameters, and pgjdbc refuses statements with more than 65 535 parameters. That limit is pgjdbc's documented behaviour, not tested here.
- **Expected impact:**
  - Above 65 535 such copies the export is impossible: it throws.
  - Below that, heap is about N × state size: 50 000 × 20 KB ≈ 1 GB (estimate).
  - A long phase passes before any output.
- **Validation:** a space with 70 000 copies of a base in another space; the export throws at the type query.
- **Recommended fix:** use a join instead of the IN list, and build each detach event when its version-1 row is reached. A dry pass can refuse early if needed, at the cost of computing twice.
- **Regression test:** 70 000 copies export with bounded heap.

### LIFE-9 — SpaceImport's write batches are bounded by count only
- **Ratings:** Severity Medium · Likelihood Possible (classes with raised size limits; the bench uses 6 000 000 characters) · Confidence Confirmed (code)
- **Location:** `SpaceImport.scala:53` (`eventsPerTransaction = 10000`), `:216-221`, `:230`. Design M6:368 says "one event in memory at a time"; F126 already bounded bus parts by bytes.
- **Triggering scenario:** an organization whose events are large, close to a raised per-class limit.
- **Failure mechanism:**
  - Up to 10 000 serialized events, plus their batch parameters, are held before a flush: 10 000 × 100 000 characters ≈ 1–2 GB (estimate).
  - An OutOfMemoryError is fatal, so the `Try` at `:201` does not catch it.
  - `read` throws, earlier batches stay committed, and there is no report.
- **Expected impact:** a failed import with no record of where it stopped.
- **Validation:** import 1 000 events of 1 000 000 characters with a 64 MB heap.
- **Recommended fix:** also flush on bytes (for example 16 MB, as F126 does).
- **Regression test:** the Validation scenario completes.

### LIFE-10 — Archived events are not checked in pass 1, and sizes are checked before ids are remapped
- **Ratings:** Severity Medium · Likelihood Possible (the target's per-class limits are smaller, or events hold long id lists) · Confidence Confirmed (code)
- **Location:** `SpaceImport.scala:114` (archived lines: class only); `:163` (`plan.exported(aggregate)` throws for an archived event of an aggregate missing from the file); `:110-111` against `:165-166` (pass 1 measures the source JSON, pass 2 the re-serialized and remapped JSON; a 6-digit legacy id can become 13 digits).
- **Triggering scenario:** the target has a smaller limit for an archived class, or remapping makes an event grow past its limit.
- **Failure mechanism:** everything else is written, then pass 2 stops. The advised purge-and-retry stops at the same line.
- **Expected impact:** hours lost; a purge is needed; the failure repeats until the limits change.
- **Validation:** give the target a smaller limit for an archived class and import.
- **Recommended fix:** check archived lines like events in pass 1, and check sizes on the remapped JSON there (CPU ×2).
- **Regression test:** the Validation scenario is refused in pass 1.

### LIFE-11 — Personal data and whole reports reach the logs from the lifecycle operations
- **Ratings:** Severity Medium (logs are outside erasure) · Likelihood Possible · Confidence Confirmed (code)
- **Location:**
  - mpjsons puts an 80-character window of the input into every read error (`mpjsons/.../MPJsons.scala:21-34`).
  - `SpaceImport.scala:237-239` (logged at ERROR, and in the report), `:249` (the whole report, including every remapped id pair).
  - `AggregateOwner.scala:789-791` and `HistoryRewriter.scala:51-52` (the erasure's own report and log); `SpacePurge.scala:68-69`; `StoredDataCheck.scala:166`.
- **Triggering scenario:** an unreadable event during an import, a rewrite or a stored-data check; or an import where most ids collide.
- **Expected impact:**
  - Fragments of event data (names, e-mail addresses) in logs and in erasure reports.
  - Multi-megabyte log lines: 1M remapped ids ≈ 50 MB (estimate).
- **Validation:** import a file with a marker string next to a JSON error; the marker appears in the log.
- **Recommended fix:** log counts and the first few ids; strip the input window from the messages the library logs; keep full detail in the returned report only.
- **Regression test:** the marker appears in no log line.

### LIFE-12 — SpaceImport keeps ids that the target's own generator will hand out later
- **Ratings:** Severity Medium · Likelihood Unlikely (two databases with one installation number: both legacy 0 by default, or a staging copy of production) · Confidence Confirmed (code)
- **Location:** `SpaceImport.scala:92-95,124-136` (only ids already present are remapped; the header's `installation` is ignored); `SpaceExport.scala:30` (`installation = 0` by default); the uid sequence is never moved past imported ids.
- **Triggering scenario:** an organization exported from a database with the same installation number as the target.
- **Failure mechanism:**
  - A later pool contains an imported id I. The `fresh` owner (or its `caughtUp`) meets I's row: a conflict, or a failed restore if I is of another type.
  - The FirstCommand fails.
  - Creations keep failing while the sequence passes the imported range.
- **Expected impact:** intermittent creation failures for months, with misleading concurrency errors on aggregates that are new.
- **Validation:** the target's sequence is at S; import id S+5; draw ids until S+5; the FirstCommand fails.
- **Recommended fix:** give the import the target's `IdRange`, and remap ids in its own range at or above the sequence; or `setval` the sequence past them under `SchemaLock`, never moving it back.
- **Regression test:** the id is remapped, or never handed out.

## 3. Lesser findings

- Concurrent imports into one space are check-then-write with no lock (`SpaceImport.scala:86` against `:174-199`). Both can pass, then a primary-key violation stops one mid-way, or the organization is doubled; the advised purge removes both.
- The import plan holds a separate class-name String per aggregate and boxed Shorts (`SpaceImport.scala:74,104,157,206`). That is about 160 bytes per aggregate, so about 1.6 GB at 10M aggregates (estimate). SCALING.md:212-213 says only ids are held.
- `ImportReport.unknownReferences` counts occurrences, including ids that exist in the target, once 1 000 distinct ids have been seen (`SpaceImport.scala:146-152,247-248`).
- `readLine` does not bound a line's length (`SpaceImport.scala:253-258`): a file without newlines runs out of memory in pass 1.
- `SpaceId(source)` maps to `into`, but `AggregateId(source)` maps to `mapped(source)` (`SpaceImport.scala:151-154`). Under the documented convention "space id = organization aggregate id" (`SpaceId.scala:9-10`) they diverge unless `into` equals the possibly remapped id. Design M6:321 says SpaceIds are remapped like aggregate ids.
- SpaceExport's inner joins with `types_names` (`SpaceExport.scala:113-114,128,148`) silently drop rows whose type id has no name.
- SpaceExport reads `creation_time` and `archived` as UTC (`:117,151`), but they are written with `current_timestamp` in the session's zone, which pgjdbc sets to the JVM's (`add_events` `:284`, `archived()` `:651`). Harmless while Neula runs in UTC.
- Purge failures give an exception, not a report:
  - A type with no command bus that appears after the pass's plan throws at `SpacePurge.scala:173`, after its batch has committed.
  - A hook or database failure in a batch fails the Future in the same way.
- `PurgedAggregates` and `PurgeSpace` go to the active build only (`Projections.scala:405-408`). The design says every build (M6:202-203). A build started after the last `PurgeCheck` keeps the organization's documents.
- `ForgetAggregates` is answered before any sharded owner stops (`AggregateCommandBusActor.scala:227-229`), so shards do not pace the batches.
- Erased or purged state stays in other nodes' `Local` owners until their next use or idle stop, up to 12 h (ARCHITECTURE.md:460-464). This is documented for purge; for erasure it is only implied.
- StoredDataCheck does not remember an unknown type id as unknown: each such row costs a synchronized DB query and an exception (`StoredDataCheck.scala:61`, `TypesNamesState.scala:95-103`).
- `spaceRows` does not count rows whose aggregate is gone, though design M6:271-272 says it does (`StoredDataCheck.scala:123-132`).
- The legacy `events_to_route.message bytea` (written until 21/07/2016) is reached by neither purge, erasure nor StoredDataCheck.
- TestDatabase takes a schema's age from each JVM's own clock (`:29,45`):
  - A machine whose clock is more than 6 h fast drops every live run's schema.
  - A run longer than 6 h loses its own schema.
  - Nothing checks whether a schema is in use (an advisory lock per run would).
- `LIKE 'test_run_%'` (`TestDatabase.scala:46`): the numeric parse guards against the `_` wildcard, but a non-test schema named `test_run_<n>` would still be dropped.
- In fixed-schema mode `TestIds` restarts at 500 000 000 001 in each JVM (`TestIds.scala:8`), so ids collide with an earlier run's rows. Two modules started in the same second share a schema with a chance of 1 in 9 000 (`TestDatabase.scala:29`).

## 4. Known, re-checked

- **F8 (archive), "Done in M6":** the complete rewrite's projection step cannot be resumed (LIFE-1), and a command in flight can undo the erasure (LIFE-2).
- **F145:** the catch-up before a command is check-then-act. `add_events` never compares the history generation, so a rewrite committed after the check is missed (LIFE-2).
- **F78:** the fix removed the SELECT before the INSERT, so every miss on a node burns an id from the SMALLINT sequence. The INSERT also runs on a second connection under the lock (LIFE-7).
- **F126:** the byte bound on batches was not carried to SpaceImport (LIFE-9).
- **F65:** only `event_time` is bound as UTC. The export reads the `current_timestamp` columns as UTC (lesser findings).

## 5. Checked and safe

- **The import, file and ids:**
  - It reads the file line by line in both passes; a line that does not parse in pass 1 throws before any write.
  - Classes cross installations by name only.
  - Ids that already exist in the target are remapped, and typed references follow.
  - A non-empty target space is refused; the same file imported into another space gives a fully remapped copy.
- **The purge:**
  - It deletes every library table it should (`PostgresEventStoreState.scala:584-601`).
  - A purged aggregate cannot come back: `add_events` raises without its row (schema `:282-291`), and a snapshot needs the row (`:236-241`).
  - Purges on two nodes, or run again, are safe: chains are locked in key order (`:572-580`), pages strictly go down, passes are bounded, batches are bounded and paced.
- **The rewrite:**
  - Per aggregate, everything is read first, then written in one transaction under the chain lock. A refusal writes nothing.
  - Snapshots of the aggregate and of its readers are dropped and the generation is raised (`:670-713,256-259`), so stale owners cannot write snapshots.
  - A duplicate's rewrite never touches its base's events.
  - Copies are found once each.
  - A targeted rebuild also deletes that projection's `subscription_failures` rows for the aggregate (`SubscriptionsState.scala:463-469`).
- **Types:** two nodes registering one name get one id (unique index, `ON CONFLICT`, read back). The cache never needs invalidating.
- **StoredDataCheck:** it streams each table through a cursor and keeps per-class counts only. Missing tables are reported (F181). The table names in `spaceRows` come from the catalog, and the document store only makes `[a-zA-Z0-9_]` names, so the unquoted SQL is safe.
- **TestDatabase:** it drops only `test_run_<epoch>…` names older than 6 h; the bench's `bench_<epoch>` schemas never match.
- **Time:** event times in the export format are written and read with a UTC calendar (`SpaceFormat.scala:47-52`).
- **mpjsons:** polymorphic fields resolve only subclasses of their marked class (`TypedConverter.resolveClass`). The open risk is the top-level class only (LIFE-5).

## 6. Open questions

- Does Neula have more than one database with installation number 0, such as a staging copy or a second region? This decides LIFE-12.
- How many of an organization's aggregates are copies of platform (space 0) templates? This decides LIFE-8.
- Does Neula derive a document's space from a stored field, or from a `def` over an organization AggregateId? Are UserIds aggregate ids? Both decide what the import's id mapping means.
- Neula's per-class event size limits (LIFE-9), and its JDBC pool size against its threads (LIFE-7).
- Does Neula run erasures concurrently, or during managed builds? This decides how often LIFE-1 occurs.
- Does Neula's database still have rows in `events_to_route`?
- What does FlowControl do with an aggregate held for a subscriber after that aggregate is purged?
- The 30-day response retention against the erasure deadline is a policy decision.
