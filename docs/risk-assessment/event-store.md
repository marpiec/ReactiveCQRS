# Working notes: event store

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (STORE-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

- **Read in full:** `core/.../eventstore/`:
  - `PostgresEventStoreState.scala`
  - `PostgresEventStoreSchemaInitializer.scala`: every init step, and `add_events`, `add_event`, `add_undo_event`, `add_duplication_event`
  - `EventStoreState.scala`, `OutboxDeletes.scala`, `EphemeralEventStoreState.scala`, `MemoryEventStoreState.scala`, `EventStoreExceptions.scala`
  - `core/.../util/SchemaLock.scala`
- **Callers, read for the store's contracts:**
  - `AggregateOwner`: persist 464-543, squashHistory 697-728, rewriteHistory 770-799, stopNow and writeSnapshot 676-680 and 805-810, republish 865-879
  - `AggregateRestorer` (all)
  - `AggregateCommandBusActor` 394-484: historical reads, delete, detach
  - `SquashScheduler` (all), `ProjectionBuilder` 66-405, `HistoryRewriter` 25-75
  - `SpaceExport` 25-157, `SpaceImport` 60-259
  - The type-name lookups in `TypesNamesState`, the failure insert in `SubscriptionsState`, `ProjectionActor`:956-965
  - The 0.12.47 store (`git show v0.12.47`), and commit 9eaa0c2 "Events limits added" of 09/04/2025
  - The mpjsons 0.7.0 sources from the local `~/.m2`, extracted to the scratchpad: `StringSerializer`, `ErrorMessageFormatter`
- **Docs:** CLAUDE.md; ARCHITECTURE §4, §5.1 and §13.2; KNOWN-ISSUES (decisions and open issues); the archive's entries on the store and commands; SCALING §1–4 and §7.
- **Not examined:** `StoredDataCheck`; `SpacePurge` beyond its store calls; `OutboxPollerActor`; the projection side beyond the lines named; the specs, except `EventStoreStateSpec` 305-331 and 700-731 and `CompleteRewriteSpec` 110-163.
- **Nothing was run.** The PostgreSQL lock and foreign-key behaviour cited below comes from PostgreSQL's documented semantics, not from execution here.

## 2. Findings

### STORE-1 — Squashing a duplicate drops its chain without recording lineage, so a later erasure of its base skips it and reports "complete"
- **Severity:** High (GDPR, Regulation (EU) 2016/679 Art. 17). **Likelihood:** Possible; Likely for any type that uses both copies and squash. **Confidence:** Confirmed (code).
- **Location:**
  - `PostgresEventStoreState.squash` (758-785) ends with `DELETE FROM aggregates WHERE id = $id AND base_id <> $id` (782) and writes no `aggregate_lineage` row.
  - `detachDuplicate`, by contrast, inserts lineage before the same delete (751-754).
  - `copiesOf` (715-719) finds copies only through chain rows or lineage; `HistoryRewriter.walk` (HistoryRewriter.scala:58-73) relies on it.
  - The memory store behaves the same (`squash` 244-261 sets no lineage), so the stores agree and tests pass.
- **Preconditions:** a context with squash enabled whose aggregates include copies (`DuplicationEvent`), squashed manually or by `AutomaticSquash` (`squashCandidates` selects copies like any aggregate).
- **Triggering scenario:**
  1. Template B holds a customer's name. Copy D is made at B's version 10.
  2. D collects own events, and the scheduler squashes D up to version 40. D's squash event is D's state at 40, which includes B's name.
  3. Later, `HistoryRewriter.rewrite(B, EraseCustomer)` runs.
- **Failure mechanism:** D50 says squashing a duplicate detaches it. The squash deletes D's chain rows (D,B) but records no lineage, while ARCHITECTURE §4.1 says "every detach writes them". `copiesOf(B)` then finds neither a chain row nor a lineage row for D, so D is never rewritten. `basesOf(D)` is empty too.
- **Expected impact:**
  - The erased value stays in D's squash event, and in D's projections.
  - `RewriteReport.complete` is `true`, so the operator believes the erasure is done.
  - Undetectable without scanning every event for the value.
  - Copies squashed before a fix have already lost the chain rows that would rebuild their lineage.
- **Validation:** copy D of B; `squash(D, 2)`; assert `copiesOf(B)` contains D. It is empty today. Then rewrite B and grep D's events for the value.
- **Recommended fix:**
  - In `squash`, run the `INSERT INTO aggregate_lineage … SELECT id, base_id, base_version … WHERE id = $id AND base_id <> $id` of `detachDuplicate` before deleting the chain rows. Mirror it in the memory store.
  - For copies squashed already, document a manual audit, or keep the original duplication event in the archive.
  - Trade-off: none material.
- **Regression test:** `EventStoreStateSpec` (both stores): copy, squash the copy, assert `copiesOf(base)`. `CompleteRewriteSpec`: a squashed copy of an erased base holds the value no more.

### STORE-2 — An export taken while the organization changes is imported with two events at one version; the store has nothing that refuses duplicate versions
- **Severity:** High. **Likelihood:** Possible. **Confidence:** Confirmed (code). This sits at the SpaceExport/SpaceImport boundary.
- **Location:**
  - `SpaceExport.write` runs separate passes (58-60): aggregates and `base_version` in one transaction (109-122), events in another (125-141). Under READ COMMITTED every statement has its own snapshot anyway.
  - `SpaceImport.check` compares no versions (101-113). The writes take the file's values as they are (176-177, 186-189).
  - Store: `events (aggregate_id, version)` is a non-unique index (SchemaInitializer:173-177; PostgresEventStoreState:266).
  - `add_events` derives versions only from the `aggregates` row (281-299). Restores read `events.version <= aggregates.base_version` (PostgresEventStoreState:188).
- **Preconditions:** a space exported while something writes to it. The docs ask for a quiet organization (SpaceExport.scala:27), but nothing enforces it. The `waiting` count (62-65) misses events delivered within milliseconds. A large organization makes the gap between the passes minutes long.
- **Triggering scenario:**
  1. The aggregates pass records cart A at version 5.
  2. A command commits A's version 6.
  3. The events pass writes A's events 1–6.
  4. Import: A' is stored with `base_version = 5` and events 1–6.
  5. The first command on A' expects version 5, the procedure finds 5, and it inserts a second version 6.
- **Failure mechanism:** the export's passes are not one snapshot, the import does not check that each aggregate's events end at its recorded version, and the store relies on the row lock alone for unique versions. Two variants:
  - A squash between the passes imports `squashed_version = NULL` over an already squashed history. `add_undo_event` and `add_duplication_event` then no longer refuse below the squash point (F96 and F102 reappear).
  - An undo between the passes imports noop marks that the aggregate row does not reach yet.
- **Expected impact:**
  - Silent corruption in the target installation: restores apply both version-6 events in undefined order, and `readPart` returns both.
  - The sequence gate gets version 6 twice.
  - `overwriteEvents`, `squash` and `detachDuplicate` later fail on `updated != 1`.
  - It is found only when a state looks wrong.
- **Validation:** export with a `Writer` that blocks on the first aggregate line until a command on that aggregate commits; import; send a command to A'. Then `SELECT version, count(*) FROM events WHERE aggregate_id = A' GROUP BY 1 HAVING count(*) > 1` returns (6, 2).
- **Recommended fix:**
  - Read every pass of the export (chains, lineage, the detach restores, aggregates, events, archive) in one `REPEATABLE READ READ ONLY` transaction.
  - In the import's check pass, refuse an aggregate whose events are not exactly its recorded range, or contain duplicates.
  - Build a unique index on `events (aggregate_id, version)` concurrently, after checking production has no duplicates, and drop the non-unique one.
  - Trade-off: the export's snapshot holds back vacuum for its whole length; the events pass already does.
- **Regression test:** an `ExportImportSpec` scenario with the blocking writer: the file is consistent, or the import refuses it. A store spec: a direct duplicate insert fails.

### STORE-3 — Every node start takes ACCESS EXCLUSIVE and SHARE locks on the hot tables with no timeout; behind any long transaction, the whole cluster's access to `events` queues
- **Severity:** High. **Likelihood:** Possible. **Confidence:** Confirmed (code) for the statements and the missing timeout. The queueing is PostgreSQL's documented behaviour (PostgreSQL docs: §13.3 "Explicit Locking"; ALTER TABLE "acquires ACCESS EXCLUSIVE unless explicitly noted"; CREATE INDEX takes SHARE).
- **Location:**
  - `initSchema` (SchemaInitializer:12-41) runs at every start (book 02-setting-up.md:175).
  - ACCESS EXCLUSIVE: `ALTER TABLE events DROP COLUMN IF EXISTS command_id` (58-62), and `ADD COLUMN IF NOT EXISTS` on `aggregates` and `events` (132-136, 156-158, 161-163, 167-169).
  - SHARE: `CREATE INDEX IF NOT EXISTS` on `events`, `aggregates`, `events_to_publish` and `event_keys` (64-86, 112-116, 206-207).
  - SchemaLock sets no `lock_timeout` (SchemaLock.scala:39-46), and each statement runs on its own `DB.autoCommit` connection.
- **Preconditions:** a node starts (a rolling deploy, a crash restart, a maintenance restart) while some session holds a lock on these tables for long:
  - pg_dump (ACCESS SHARE for the whole dump)
  - SpaceExport's events pass
  - the anti-joins of `backfillEventKeys` (293, 308)
  - a long report query
  - for the SHARE steps: a long writer, or an anti-wraparound autovacuum, which does not yield
- **Triggering scenario:** an on-premise cluster of one (D42), a nightly pg_dump of 1 h, and a nightly restart at 01:00.
- **Failure mechanism:**
  - PostgreSQL grants the table lock before it checks `IF [NOT] EXISTS`, so the ALTER waits for ACCESS EXCLUSIVE although the column exists.
  - While it waits, every later conflicting request (each `add_events`, restore and page read on `events`, from every node) queues behind it.
  - The SchemaLock holder is stuck too, so nodes starting meanwhile give up after `MaxWait` (5 min, SchemaLock.scala:20, :43) and fail to start.
- **Expected impact:**
  - Commands, restores, re-publications and builds stall across the cluster for the length of the long transaction.
  - It looks like a database outage; `pg_stat_activity` shows `wait_event_type = Lock` behind the ALTER.
  - The same pattern exists in CommandResponseState:93-121, PostgresDocumentStore:83-88, SubscriptionsState:250-272, ProjectionRegistry:98, PostgresSagaSchemaInitializer:19 and TypesNamesState:70 (not examined in depth).
- **Validation:** psql A: `BEGIN; SELECT count(*) FROM events;`, left open. Run `initSchema()` on a complete schema. psql C: `SELECT … FROM events LIMIT 1` blocks until A commits.
- **Recommended fix:**
  - Read the catalog first (`information_schema.columns`, `to_regclass`; no table lock) and skip DDL that is already in place.
  - Record completed steps in `schema_steps`.
  - Run the rest with `SET LOCAL lock_timeout = '2s'` on the SchemaLock's connection, retrying with back-off.
  - Trade-off: the catalog checks must be exact, and retries can delay a start.
- **Regression test:** with a read transaction on `events` held open, `initSchema()` on a complete schema, and a concurrent `add_events` that completes within 1 s.

### STORE-4 — The per-aggregate version limit applies to rewrites and squashes, which add no version: erasure and squash fail above the limit
- **Severity:** Medium (GDPR Art. 17). **Likelihood:** Possible; depends on the stored data and the configured limit. **Confidence:** Confirmed (code).
- **Location:**
  - `serializedChecked` (86-95): `else if (version.asInt >= aggregateVersionLimit) throw new TooManyEventsException`.
  - It is called by `overwriteEvents` with `e.version - 1` (144), `rewriteEverywhere` with `c.version - 1` (701) and `squash` with `n - 1` (773).
  - The owner answers `RewriteRefused("the rewrite failed: Too many versions …")` (AggregateOwner:789-791) and `SquashRefused` (724-726).
- **Preconditions:** an aggregate above the store's limit (10 000 by default):
  - 0.12.47 checked the version before the event (`aggregateVersion > aggregateVersionLimit`, F125), so 0.12 aggregates that hit the limit sit at 10 001.
  - Histories older than the limit (09/04/2025) may be longer; F22 speaks of up to 500 000 events in Neula.
  - A host may lower the limit, or a tool may build a store with the default.
- **Triggering scenario:** erasing a name held by the last event of an aggregate at version 10 001; or `AutomaticSquash` on an aggregate of 50 000 events.
- **Failure mechanism:**
  - The guard against growth throws for an event that only replaces a row.
  - The rewrite is all-or-nothing.
  - The squash event is built, with a restore, before the store throws, and the scheduler asks again every run (SquashScheduler:88-91).
- **Expected impact:**
  - Such aggregates cannot be erased or squashed, and squash is the documented remedy for long histories.
  - The error blames "too many versions", which misleads the operator.
  - Every scheduler run wastes a restore.
  - The memory store has no such check, so tests cannot show it.
- **Validation:** write 5 events through a store with limit 10. Through a store with limit 3, call `overwriteEvents(v5)`, `rewriteEverywhere` altering v5 and `squash(5)`: each throws `TooManyEventsException`.
- **Recommended fix:** check sizes everywhere, and the version limit only in `persistPlain` and `persistOne`, where versions are added. Trade-off: none.
- **Regression test:** the three calls above succeed, and a new event on that aggregate is still refused.

### STORE-5 — Stored event fragments go into durable failure records and logs, and an unreadable event blocks the erasure of its aggregate
- **Severity:** Medium (GDPR). **Likelihood:** Possible; F52 and F53 make unreadable rows plausible at the 1.0 migration. **Confidence:** Confirmed (code).
- **Location:**
  - `readCommittedPage` turns a read failure into `e.getClass.getSimpleName + ": " + e.getMessage` (343-344).
  - mpjsons 0.7.0 builds that message with ±40 characters of the JSON around the failing position (MPJsons.scala:13-35).
  - The builder logs it and makes an unreadable item (ProjectionBuilder:271, 287-291).
  - The projection stores it as `"a stored event cannot be read: " + error` (ProjectionActor:965) in `subscription_failures.error` (SubscriptionsState:278).
  - `rewriteEverywhere` deserializes every row first (678-680), so one failure aborts it. The owner logs the exception and returns its message (AggregateOwner:789-791).
- **Preconditions:** an event that mpjsons 0.7 cannot read (an old JDK form, a changed field type, a hand repair), with personal data near the failure.
- **Triggering scenario:** a rebuild after the upgrade hits F52-style rows: each projection stores `…"name":"<name>","birth":{…`. The erasure of that person is refused, and its reason carries the same text.
- **Failure mechanism:** the failure rows stay until retried or skipped. They are outside the erasure's reach (ARCHITECTURE §4.9 does not list them), and they block permanent deletes and squashes (D20a, D51).
- **Expected impact:** personal data in a table and in logs that erasure does not reach, and an aggregate that can only be erased with manual SQL.
- **Validation:** corrupt one event (`UPDATE events SET event = '{"name":"<name>","x":'`); build; read `subscription_failures.error`. Send `RewriteEverywhere` and get `RewriteRefused` containing the name.
- **Recommended fix:**
  - Store and return only the class, event id, version and position. Keep the window for DEBUG, or drop it.
  - Give erasure a path for unreadable rows: a raw-JSON change, or a tombstone event class.
  - Clear the failure rows' text on erasure.
  - Trade-off: less diagnostic detail.
- **Regression test:** the scenario above, with no stored or returned error containing the name.

### STORE-6 — The squash scheduler assumes event times grow with ids; an import, or a node with a wrong clock, makes it squash history the policy keeps
- **Severity:** Medium; the state and the `keep` classes survive. **Likelihood:** Possible; needs `keepNewerThan` plus an import or a clock error. **Confidence:** Confirmed (code).
- **Location:**
  - `firstEventNotOlderThan` binary-searches ids on `event_time < at` (810-822).
  - `squashCandidates` then squashes each aggregate up to its newest event with `e.id < boundary` (793-798).
  - SpaceImport inserts new `events_seq` ids with the original times (SpaceImport:186-189, 218).
  - The comment at 807-809 says imports "count as new … never squashed early". A binary search over a predicate that is not monotonic gives no such guarantee.
- **Triggering scenario** (illustrative estimate):
  - 100 million events, `keepNewerThan` of 30 days, and a true boundary near id 90 million.
  - 10 days ago an import wrote ids 93–98 million with times from 2019.
  - The probes at 50, 75 and 87.5 million are "older". The probe at 93.75 million lands inside the import and reads as older too. The search ends at about 98 million.
- **Failure mechanism:** native events between 30 and 10 days old count as older than 30 days, so their aggregates are squashed up to them. This repeats every run until the import is older than `keepNewerThan`.
- **Expected impact:**
  - Recent events are deleted; only the `keep` classes are archived.
  - Reads and undos in that range fail with `AggregateSquashedException`.
  - No signal. The memory store uses exact times (459-470), so tests cannot show it.
- **Validation:** ids 1–899 with recent native times, 900–1 400 with times from 2019, 1 401–2 000 with current times, and a true boundary of about 800. `firstEventNotOlderThan` returns 1 401.
- **Recommended fix:**
  - Per candidate, compute `upTo = min(version) − 1` over the events with `event_time >= at`, using an index on `events (aggregate_id, event_time)` built concurrently. It reads only the recent events.
  - Trade-off: one more index on `events`.
- **Regression test:** the validation data in `PostgresEventStoreStateSpec`.

### STORE-7 — The chain lock is held across long work and stops every copy's commands; each stopped command pins a blocking-dispatcher thread and a connection
- **Severity:** Medium (a node-wide stall while it lasts). **Likelihood:** Possible. **Confidence:** Strongly suspected (the locking is confirmed in code; the durations are estimated).
- **Location:**
  - `lockChain` locks every row of every aggregate reading the base `FOR UPDATE` (470-472), including each copy's own row, which `add_events` must update (SchemaInitializer:281).
  - It is held through `rewriteEverywhere` (674): every event is streamed and deserialized, and the host's `alter` and `OutboxWriter.rewrite` run in the same transaction (AggregateOwner:774-779).
  - It is held through `deletingPermanently` and `squash`, which run about 6 statements per detached copy (496, 767), and through `detachDuplicates` (522).
  - Owners persist synchronously on the 32-thread blocking dispatcher (ARCHITECTURE §4.4), with no lock timeout.
- **Triggering scenario** (estimated): permanently deleting, erasing or squashing a template with 10 000 copies (the example in SCALING §4) means about 60 000 statements, tens of seconds, while 32 commands arrive for copies on one node.
- **Failure mechanism:**
  1. Each such command waits in `UPDATE aggregates … WHERE id = copy`.
  2. At 32 waiting commands, the node's blocking dispatcher stops: every owner of every type, projections made on it, the buses' futures, the poller.
  3. The JDBC pool empties, and other borrowers fail after the 5 s wait (SCALING §1).
- **Expected impact:** a stall across a node, or across all nodes with sharded copies, and command timeouts.
- **Validation:** a base with 2 000 copies; run `RewriteEverywhere` on a base of 20 000 events while 64 commands hit 64 copies; compare their latency with the transaction's length and check `pg_locks`.
- **Recommended fix:**
  - Lock the base's own rows `FOR UPDATE` and the readers `FOR KEY SHARE`, which stops duplications but not commands, and raise the readers' generation in one short final statement.
  - Detach copies in short batches before the squash or delete; a detach keeps the copy's state (D85).
  - Give owners' persist transactions a `lock_timeout`.
  - Trade-off: batched detaches are no longer atomic with the squash. That is harmless, as each copy keeps its state.
- **Regression test:** copy commands finish within a bound while a long rewrite of their base runs.

### STORE-8 — Any node's start replaces the stored procedures with its own version's bodies
- **Severity:** Medium. **Likelihood:** Possible, at the first procedure change after 1.0 or at any rollback. **Confidence:** Confirmed (code).
- **Location:**
  - `dropOldFunctions` and an unconditional `CREATE OR REPLACE` of the four procedures (SchemaInitializer:35-39, 237-407).
  - SchemaLock only serializes (ARCHITECTURE §13.2). No version is stored.
  - H11's "do not roll back below M6" is the only guard.
- **Triggering scenario:**
  - 1.1 changes `add_events`. Mid-rollout a 1.0 pod crash-restarts and installs the 1.0 body for every node, until the next 1.1 start. This is how M6's keys would be lost (F124).
  - Or 1.1 renames a signature, and its `dropOldFunctions` drops the one the 1.0 pods still call.
- **Failure mechanism:** whichever node starts last decides the procedure bodies; nothing compares versions.
- **Expected impact:** silent gaps in data the new code relies on, or failing commands during a rollout.
- **Validation:** install the current bodies, run an initializer carrying an older body, call `add_events`, and see no `event_keys` row.
- **Recommended fix:**
  - Store a procedure version in `schema_steps` and replace only to a newer one.
  - Give a changed signature a new name, and drop old names in a later contract release (D69).
  - Skip DDL, with a log line, when the database is newer than the code.
  - Trade-off: one more version to maintain.
- **Regression test:** an older initializer leaves `pg_get_functiondef('add_events'::regproc)` unchanged.

### STORE-9 — `backfillEventKeys` on a large store: a row lock and WAL record per old event, a later full re-freeze, long scans that pin the vacuum horizon, and aborts on concurrent deletes
- **Severity:** Medium (operational, once per installation). **Likelihood:** Likely, as H11 asks every upgraded installation to run it. **Confidence:** Strongly suspected (PostgreSQL internals; not measured).
- **Location:**
  - `event_keys … REFERENCES events (id) ON DELETE CASCADE` (SchemaInitializer:204-205).
  - `backfillEventKeys` (291-314): unpaced batches of 10 000, whole-table anti-joins (293, 307-309), no retry.
- **Failure mechanism:**
  - Each insert's foreign-key check is a `SELECT … FOR KEY SHARE` on its old event row. It marks the tuple, writes WAL and clears the page's all-frozen bit, so the next aggressive vacuum re-reads the whole table.
  - The two anti-joins run for minutes and hold back vacuum on `events_to_publish`, `aggregates` and `subscriptions`.
  - An event deleted by a concurrent squash or purge fails the check, and the exception ends the run.
- **Expected impact** (estimated): WAL in the order of the table's size, replica lag, bloat on the hot tables, aborted runs to restart by hand, and a later full freeze vacuum.
- **Validation:** on a copy, measure `pg_current_wal_lsn()` and `pg_visibility_map_summary('events')` before and after; squash concurrently and watch the run abort.
- **Recommended fix:**
  - Pace the batches.
  - Retry a failed batch.
  - Drop the whole-table scans; the walk finds key-less rows itself.
  - Measure it in the D81 rehearsal.
  - Trade-off: a slower backfill.
- **Regression test:** a backfill with concurrent deletes in its range completes.

## 3. Lesser findings

- **Memory double hides bugs:**
  - `writeSnapshot` is atomic, which hides the F119 race (MemoryEventStoreState:165-169).
  - `squashCandidates` uses exact times, which hides STORE-6 (459-470).
  - Rewrites and squashes skip the size and limit checks, which hides STORE-4 (244-261, 353-381, 473-489).
- **Memory double detaches differently:** `duplicatesReading(below)` returns only direct copies, while PostgreSQL returns every depth (229-242 vs PostgresEventStoreState:474-477). A squash detaches different copies, so F143's blockers for deeper copies go untested.
- **Memory double accepts what PostgreSQL refuses:**
  - It stores a `DuplicationEvent` on an existing aggregate, and a non-`FirstEvent` creating one; PostgreSQL raises (SchemaInitializer:287-290, 396).
  - A version mismatch surfaces as `IllegalStateException`, so the owner's conflict and reload path (AggregateOwner:531-536) never runs (Memory:45-58).
- **Metadata envelopes:** no size limit; stored again with every event of a command; not counted in F126's byte sizes (PostgresEventStoreState:63, 111-114, 336-340, 840-845).
- **IN-list limits:**
  - `detachBlocker(…, only)` binds one parameter per copy (729), so a purge detaching more than 32 767 copies of one base fails.
  - `purgeAggregates` binds the batch twice (597), so `aggregatesPerBatch` above 16 383 fails.
  - `detachDuplicates`' `forall(duplicates(…).contains)` is quadratic (525).
- **Nested connections:** a type name not yet cached opens its own transaction (TypesNamesState:85, 98) inside the persist, squash and rewrite transactions (PostgresEventStoreState:105, 110, 746, 773). With a full pool, the first use of a new class waits for the borrow timeout.
- **F120 variant for duplicates:** `add_duplication_event` racing an already committed row raises "… but such was found" (SchemaInitializer:396), which `movedOnWithoutThisOwner` (AggregateOwner:548-550) does not take for a conflict. An idempotent copy command gets `EventHandlingError` instead of its stored response.
- **`reltuples = -1`:** a never-analyzed table (PostgreSQL 14+) counts as "small" in the `*WhenSmall` steps (SchemaInitializer:173-196), so its index is built non-concurrently at start.
- **`backfillSpaces`:** any read failure, a transient one included, becomes "no space found" (422).
- **Lineage after a permanent delete:** the delete removes the lineage that names the deleted aggregate (506), right after detaching copies that hold its values, so a later erasure cannot find them.
- **`backfillEventKeys` never finishes** while an event lacks its own aggregate row (`missing`, 307-312), and no remedy is documented: builds by type or organization walk `events` for good.
- **Outbox deletes:** the queue is unbounded and a failed batch is dropped, a design choice of F26 (OutboxDeletes:34, 52-66). With a saturated pool, each flush waits 5 s and drops its batch, which turns into re-publication load.
- **Ephemeral rewrites:** `EphemeralEventStoreState` does not override `overwriteEvents`, so plain rewrites go through without the size check, against its doc (10-16).
- **Reads at an instant:** they take `max(version)` of events with `event_time <= instant` (173), so with nodes' clock skew the result includes later-stamped events.
- **Unpaired UTF-16 surrogates:** in event strings they are encoded as `?`, so the stored event differs from the applied state (I6). Hypothesis.
- **Undo on a duplicate:** `add_undo_event` counts only the duplicate's own events, and can mark its `DuplicationEvent` itself (SchemaInitializer:335-345). The intended semantics are to be confirmed.
- **User ids in the server log:** store conflicts are logged by PostgreSQL at ERROR with the user id in the message (SchemaInitializer:294, 333).
- **Constructor parameters are not validated:** `fetchSize = 0` disables streaming, and a non-positive limit refuses every event (31-34).

## 4. Known, re-checked

- **F119 — the fix is racy.**
  - The generation guard is a non-locking `WHERE EXISTS` inside `INSERT … ON CONFLICT` (236-241), and `snapshots` keeps no generation.
  - Take a passivation snapshot (AggregateOwner:679) whose statement starts before another writer's undo or rewrite commits: Local owners on two nodes, or a copy's owner while its base is rewritten.
  - That snapshot either waits on the row the writer deleted and then inserts, or commits unseen by the writer's `DELETE`. Restores then start from the stale state: an undone event, or an erased value, comes back.
  - The F119 test is sequential (EventStoreStateSpec:308-331).
  - Fix: add `FOR SHARE` on the own row in the EXISTS, or store the generation with the snapshot and check it on read.
- **F65 — only `event_time` is UTC.**
  - `creation_time`, `events_archive.archived`, `snapshots.created`, `aggregate_lineage.detached`, `history_rewrites.at` and `schema_steps.completed` are written with `current_timestamp` in the session zone (pgjdbc sets it to the JVM's).
  - `readArchive` (661-662) and SpaceExport (117, 151) read them as UTC, and SpaceImport writes them as UTC.
  - On a non-UTC JVM the values are shifted and ambiguous in the autumn DST hour. Neula runs UTC.

## 5. Checked and safe

- **No lost update:** the procedures update the own `aggregates` row first; READ COMMITTED re-reads the locked row, so a second writer sees the new version and raises. Two first events collide on the primary key (F120).
- **No deadlock found among squash, delete, rewrite and duplication:** one ordered `lockChain` statement against `FOR KEY SHARE` in key order (F104). Commands on a base do not conflict with its duplication (`KEY SHARE` against `NO KEY UPDATE`).
- **SQL injection:** every `SQL(s"…")` and `createUnsafely` takes a constant; values are bound.
- **Readers that might rely on id order:**
  - The builder's late commits are handled: runs end at a missing version, and reconciliation compares versions (ProjectionBuilder:79-81, 242-258).
  - The poller orders by `MIN(event_time)`, not ids.
  - The backfill's id ranges hold under its stated precondition.
- **Noop marks:** reads at a version are correct for own events (`from_version`) and for base events (the chain row's `base_version`).
- **The squash's archive move:** archive first, `ON CONFLICT DO NOTHING`, one transaction, and the replaced event's id and time are kept.
- **Legacy event type versions:** detach and squash keep a non-zero `event_type_version`, but the replaced event is read first (AggregateRestorer:101), which fails, so nothing is written.
- **Permanent delete:** retried after success, it deletes nothing and answers success. Noop rows go before events, and keys go by the cascade.
- **Restores stream** in a transaction with a fetch size, and the squash check runs after the read (F102).
- **`event_time`** is bound and compared as UTC (F65).
- **Overflow:** versions are capped, byte sums are bigint, and the binary search's midpoint cannot overflow.
- **The library's own IN lists** are bounded: `Only(Set(id))`, purge batches of 1 000, import lookups by 1 000.
- **`backfillSpaces`** keeps the event keys in step with a concurrent command, through `RETURNING space_id` after the row lock.
- **An instant read** of a copy from before it existed answers `None` (AggregateCommandBusActor:406-409).
- **NUL characters** are refused by mpjsons before reaching PostgreSQL.
- **Concurrent `initSchema` calls** are serialized by SchemaLock (F63).

## 6. Open questions

- **Aggregates above the limit in Neula:** `SELECT count(*), max(base_version) FROM aggregates WHERE id = base_id AND base_version > <limit>`, and which limit Neula configures (STORE-4).
- **Duplicate versions today:** `SELECT aggregate_id, version FROM events GROUP BY 1, 2 HAVING count(*) > 1 LIMIT 10` (STORE-2 fix).
- **Copies:** the widest fan-out, from `SELECT base_id, count(*) FROM aggregates WHERE id <> base_id GROUP BY 1 ORDER BY 2 DESC LIMIT 10`, and whether those types squash (STORE-1, STORE-7).
- **Long transactions:** the backup method (pg_dump or snapshots), long report queries, anti-wraparound vacuum durations, and any role-level `lock_timeout` or `statement_timeout` (STORE-3).
- **Feature use:** whether exports run on live organizations, and whether `keepNewerThan` is combined with imports (STORE-2, STORE-6).
- **Unreadable rows:** the unreadable rows in production (StoredDataCheck on the copy) and what their error windows show (STORE-5).
- **Backfill cost:** WAL volume and duration of `backfillEventKeys` on the production copy (STORE-9).
