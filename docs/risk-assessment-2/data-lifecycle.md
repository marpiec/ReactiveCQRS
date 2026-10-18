# Working notes: data lifecycle (second risk assessment)

- **Area:** organizations, erasure, squash, export and import (prefix R2-LIFE).
- **Scope:** `core/.../space/*` (SpacePurge, SpaceExport, SpaceImport, SpaceFormat with format 2's trailer and digest),
  `rewrite/HistoryRewriter.scala`, `AggregateOwner.rewriteHistory / squashHistory / materialize`, `PostgresEventStoreState`
  (`rewriteEverywhere`, `squash`, permanent delete, purge, backfills, snapshot guard), `AggregateRestorer.squashEvent /
  detachEvents`, `tools/StoredDataCheck.scala`, `uid/PostgresUidGenerator.scala`, the targeted-rebuild queue in
  `rebuild/Projections.scala`; book chapters 6, 15, 16, 18, 20.
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT). Read only; nothing was built or run. Numbers are
  estimates unless a source is named.
- Already known and not repeated: LIFE-1…12 of the first notes (F191, F192, F195, F198, F199, F200, F225, F226, F230,
  F247, F248) and their lesser items, assumption A10 ("an organization is quiet"), F215, F217, F243, F270.

## Findings

### R2-LIFE-1 — The export now replays every detached copy's base inside its open snapshot; a batch of copies can outlast the book's idle-in-transaction timeout
- **Severity** Medium · **Likelihood** Possible (organizations made of copies of platform templates, the F225 case) · **Confidence** Strongly suspected (PostgreSQL's documented session timeout; pgjdbc sends a Sync after each cursor fetch)
- **Where:** `SpaceExport.scala:39-41` (one `REPEATABLE READ` transaction), `:142-152` (detach event made when a copy's version-1 row is read, fetch size 1000), `:118-124`; `AggregateRestorer.scala:125-145` (each detach restores the copy at version 1, so replays its base); `docs/book/18-production.md:52` (`idle_in_transaction_session_timeout = '10min'`).
- **Scenario:** an onboarding duplicated 1 000+ platform templates in one burst, so their version-1 events are adjacent in commit order and land in one fetch of 1 000 rows.
- **Mechanism:** while the client builds the 1 000 lines, the export's connection is idle in its transaction. Each detach is a full replay of the base (F215: no reuse between copies of one base) on a second connection, plus the D85 check. A base of 10 000 events (the default version limit) at about 50 µs per event is about 0.5 s per copy (estimate), so about 8 min per batch, and more with larger states. Past 10 minutes the server ends the session: the export throws, the file has no trailer.
- **Impact:** the organization cannot be exported with the settings the book recommends. Below the timeout, an export of N copies costs N base replays under one snapshot, for example 50 000 × 0.5 s ≈ 7 h (estimate), holding back vacuum on production for that long. F225 moved this work inside the snapshot.
- **Validation:** set `idle_in_transaction_session_timeout = '5s'` on the role; build a base of 2 000 events and 200 copies in one transaction; export. The export fails with "terminating connection due to idle-in-transaction timeout".
- **Suggested fix:** `SET LOCAL idle_in_transaction_session_timeout = 0` in the export's transaction. Cache the base state per (base, chain version) so copies of one template cost one replay plus their own version-1 event. Or prepare each fetched batch's detach events before the next fetch with a smaller fetch size.
- **Regression test:** the Validation case completes; a spec counts restores of the base: one per (base, version), not one per copy.
- **Continues:** F225 (and F215).

### R2-LIFE-2 — F248's range check reads `pg_sequences.last_value`, which is NULL for a sequence moved into its range but never drawn from: the import then protects installation 0's range instead
- **Severity** Medium · **Likelihood** Unlikely (a new installation with the source's installation number, imported into before any node drew a pool, for example a restore from per-organization exports) · **Confidence** Strongly suspected (PostgreSQL: `pg_sequence_last_value` is NULL while `is_called` is false)
- **Where:** `SpaceImport.scala:205-212` (`rs.longOpt(1).getOrElse(0L)`, range = `last / Span`); `PostgresUidGenerator.scala:56` (`setval(seq, range.from, false)` at start).
- **Scenario:** installation k ≥ 1 is created anew. Its generator moves `aggregates_uids_seq` to k·10^12 with `is_called = false`. An organization exported from installation k is imported before any pool is drawn.
- **Mechanism:** `last_value` is NULL, so it is read as 0 and the import protects [0, 10^12). The exported ids, in k's range, are neither existing nor "in range", so they are kept. The first pools start at k·10^12, exactly where the source's ids started.
- **Impact:** F248's symptoms come back: new aggregates get ids of imported ones, so creations fail intermittently, with misleading concurrency errors. If the source is legacy (range 0), every id is remapped for nothing (see R2-LIFE-5).
- **Validation:** in a fresh schema, construct `PostgresUidGenerator("aggregates_uids_seq", Some(IdRange(1)))`, then import a file whose ids are 10^12 + n. `remapped` is empty, and the first `nextIdsPool` returns 10^12 + 1001… which overlaps the ids.
- **Suggested fix:** read `last_value, is_called` from the sequence itself (as `moveInto` does), or pass the import the installation's `IdRange`. With `is_called = false`, the next value is `last_value`.
- **Regression test:** the Validation case remaps every id in k's range.
- **Continues:** F248.

### R2-LIFE-3 — `UserId`s are remapped through the aggregate-id map; since F248, any user id equal to an exported aggregate id is changed
- **Severity** Medium · **Likelihood** Possible (an import with legacy or same-installation ids, by a host whose user ids are not aggregate ids) · **Confidence** Confirmed (code)
- **Where:** `SpaceImport.scala:245` (`UserId(mapped(user.asLong))`), `:345`, `:355` (the events' and archive's `user_id` columns); `UserId.scala` ("it *may be* the id of an aggregate"); F248 at `:196`, `:205-212`.
- **Scenario:** user ids come from a numbering of the host (for example 1..50 000), and the organization's legacy aggregate ids start at 1001. Before F248, only colliding ids moved. Now every exported id in the target's range moves, which is every id for an import within installation 0.
- **Mechanism:** the mapping does not know whether a `UserId` is an aggregate. A user 5 432 is mapped to the new id of exported aggregate 5 432.
- **Impact:** the events' authorship, and user fields in state (owner, assignee), point to ids that are no user. Audit trails are wrong and owners lose access. Nothing in the report shows it.
- **Validation:** export an organization with aggregate X and an event whose `user_id` equals X; import it into the same installation. The event's `user_id` becomes `remapped(X)`.
- **Suggested fix:** a parameter (`usersAreAggregates: Boolean`, or a `UserId => UserId` mapper); by default, map a `UserId` only when it names an exported aggregate whose root class is a user type the host declares. Report the user ids that were changed.
- **Regression test:** with users that are not aggregates, user ids are unchanged after a fully remapped import.
- **Continues:** F248.

### R2-LIFE-4 — References to aggregates that were not exported are accepted when the target has an aggregate of that id, whatever its organization
- **Severity** Medium (one tenant's read models can show another's data) · **Likelihood** Unlikely (both installations hold legacy-range ids, or a crafted file) · **Confidence** Confirmed (code)
- **Where:** `SpaceImport.scala:238-244` (only ids neither exported nor here are "unknown"), `:371-375` (ids found here are dropped from `someUnknown`); book `16-organizations.md:509-511` ("ids from before 1.0 can [clash]").
- **Scenario:** source and target both have aggregates from before 1.0, numbered from 1001. An exported event references aggregate R of the source's platform or of another organization, which is not exported. The target's R is an unrelated aggregate, perhaps of another organization.
- **Mechanism:** R is kept as it is, and since the target has R, it is not reported.
- **Impact:** projections that resolve references (names, contacts) show the other organization's aggregate in the imported organization. It is silent, and the report says nothing.
- **Validation:** target aggregate R in space B; a file with an event referencing R, with R not in the file. Import into space C: `unknownReferences = 0`, and a projection that joins R shows B's data.
- **Suggested fix:** report references that resolve to an aggregate of a space other than 0 or `into` (one query per 1 000 ids, as now), and add a refusal option for them.
- **Regression test:** the Validation case reports R.

### R2-LIFE-5 — The import logs its whole report, and since F248 that report can hold every aggregate of the organization
- **Severity** Medium · **Likelihood** Possible (large organizations moved within installation 0, restored after a purge, or cloned) · **Confidence** Confirmed (code); sizes estimated
- **Where:** `SpaceImport.scala:376-378` (`log.info("Imported …: " + report)`, `log.warn` likewise); `:82` and `:196` (all ids in range are collisions); `:376` (a `Map[AggregateId, AggregateId]` of every pair); `AggregateId.toString` is `Id(n)`.
- **Mechanism:** with every id remapped, `remapped` has one entry per aggregate. The log line is about 40 characters per pair: 1M aggregates ≈ 40 MB, 10M ≈ 400 MB, and the StringBuilder doubles it while growing. The map alone is about 100 B per entry (estimates).
- **Impact:** log shippers drop or truncate such lines. At 10M aggregates an OutOfMemoryError after the last commit loses the report: the `remapped` map the host needs to move its own references is gone, and a rerun is refused ("space has aggregates already"), so the host must purge and import again (hours). LIFE-11's "whole report" part was not changed by the F191 fix. HistoryRewriter (`:55-56`) and SpacePurge (`:77-78`) log whole reports too: several MB for a template with 100 000 copies.
- **Validation:** import 1M aggregates into the same installation and measure the log line.
- **Suggested fix:** log the counts and the first 10 pairs. Return `remapped` as a `LongMap`, or write it to a file next to the import.
- **Regression test:** no library log line exceeds, for example, 10 KB for an import of 100 000 remapped ids.
- **Continues:** F191 (LIFE-11), F248.

### R2-LIFE-6 — A permanent delete or purge deletes the lineage it has just written, so a later erasure of the base reports "complete" while the detached copies keep its values
- **Severity** Medium (GDPR Art. 17: erasure reported done, data still stored) · **Likelihood** Possible · **Confidence** Confirmed (code)
- **Where:** `PostgresEventStoreState.scala:540` (`detachDuplicate` records lineage copy→base, `:813`, `:819-822`), then `:550` (`DELETE FROM aggregate_lineage WHERE aggregate_id = $id OR copied_from = $id`); purge `:643`; `copiesOf` `:778-782`; `HistoryRewriter.scala:66-73` (a missing aggregate answers `HistoryRewritten(0, 0)`: "unchanged", then rebuilt); `AggregateRestorer.scala:49-66` (a missing id restores as version 0).
- **Scenario:** template B holding a person's data is copied 200 times, then permanently deleted (book ch. 6: "when the data must really go"). Later that person asks for erasure, and the host runs `HistoryRewriter.rewrite(B, change)`.
- **Mechanism:** the delete detaches the copies: their version-1 event becomes the squash event of B's state. The same transaction then deletes the lineage rows that F193 relies on. The rewrite of B finds no rows, answers unchanged, and `copiesOf(B)` is empty. The report is `complete`. The same happens for copies in other organizations after a purge of B's organization. A rewrite of a mistyped id reports complete too.
- **Impact:** personal data remains in up to N aggregates after an erasure the library reports as complete.
- **Validation:** B, copy C, permanently delete B, then `rewrite(B)`. `complete == true`, and C's version-1 event still holds the value.
- **Suggested fix:** keep the `copied_from = deleted` rows (ids only; delete only `aggregate_id = deleted`). Make the rewriter report `refused`, or a flag, when the root aggregate does not exist, while still walking `copiesOf`.
- **Regression test:** the Validation case rewrites C; a rewrite of an id that never existed is not `complete`.
- **Continues:** F193.

### R2-LIFE-7 — Nothing detects writes or erasures made in the source between the export and its purge
- **Severity** Medium · **Likelihood** Possible (a move takes hours: export, import, rebuild, switch of users) · **Confidence** Confirmed (code)
- **Where:** `ExportReport` (`SpaceExport.scala:17-21`, no high-water mark); `SpacePurge.purge` (`:60-81`) checks nothing against an export; book `16-organizations.md:527-533` (import, rebuild, point users, purge). This extends assumption A10 from "during" to "after" the export.
- **Scenario:** after the export, users keep working on the source until step 2, or an erasure is run there.
- **Mechanism:** the file is a snapshot. Nothing records what changed after it, and the documented purge of the source deletes it.
- **Impact:** the organization's later commands are lost silently. An erasure made in the source in that window is undone, as the target holds the old values, and the purge then removes its audit row (GDPR Art. 17, Art. 5(2)).
- **Validation:** export, then send a command and a rewrite to the source, then import and purge the source. The target lacks both, and no report says so.
- **Suggested fix:** put a watermark in `ExportReport` and the trailer (the space's max event id and max `history_rewrites` id). Add `SpaceExport.changedSince(space, report)`, and an optional `expectedUnchangedSince` on the purge, which refuses when something changed. The book should say "no writes until the source is purged".
- **Regression test:** the Validation case is reported (or the purge refused).

### R2-LIFE-8 — A rewrite refuses any change to an undo or duplication event, even of fields the store keeps nowhere else: such an aggregate cannot be erased
- **Severity** Medium (an erasure blocked) · **Likelihood** Unlikely (a host's duplication or undo event carrying a value: a copy's title, a reason, an actor) · **Confidence** Confirmed (code)
- **Where:** `PostgresEventStoreState.scala:746-747`; `Event.scala:17-30` (only `eventsCount`, `baseAggregateId`, `baseAggregateVersion`, `spaceId` are the store's).
- **Mechanism:** `newEvent != event` on any `UndoEvent` or `DuplicationEvent` is a refusal, whatever field changed. Every attached copy holds its duplication event at version 1.
- **Impact:** `RewriteReport.refused` for every such aggregate. The only way out is squashing the copy, if its type has a squash policy.
- **Validation:** `case class Copied(spaceId, baseAggregateId, baseAggregateVersion, title: String) extends DuplicationEvent`; rewrite `title`; the rewrite is refused.
- **Suggested fix:** refuse only when the store's fields differ (`eventsCount`, `baseAggregateId`, `baseAggregateVersion`, `spaceId`).
- **Regression test:** the Validation case rewrites `title`; changing `baseAggregateVersion` is still refused.

## Lesser items

- The export drops the undo mark of a detached copy's version-1 event (`SpaceExport.scala:155-157`), while a live detach refuses such a copy (`PostgresEventStoreState.scala:687,797`). After the import the state differs when the duplication handler changes the state. It should be refused, as `cannotDetach` refuses.
- `history_rewrites` (the erasure audit) is not exported, and the documented purge of the source deletes it (`PostgresEventStoreState.scala:644`): after a move the organization's erasure record is gone (Art. 5(2)).
- A line that does not parse in pass 1 throws out of `read`, not into `refused`, with mpjsons' 80-character input window unredacted (`SpaceImport.scala:382-388`); hosts that log it log event data (F191).
- The trailer's digest is an unkeyed SHA-256: it catches accidents, not tampering. A crafted file recomputes it, but the book's "a file … changed … is refused" (`16-organizations.md:494-496`) reads as protection.
- Pass 1 does not check that `squashedVersion` is between 1 and `version`, that an event's class belongs to its aggregate's root type, that version 1 (or the squash point) is a `FirstEvent`, or that aggregate ids are unique. A duplicated aggregate line passes pass 1 and stops pass 3 on a primary-key violation after earlier batches committed (`SpaceImport.scala:140-161`, `:296-305`).
- Pass 1 keeps each `ExportedAggregate` whole (`created` text, chain, lineage) plus boxed `bases` tuples, and when every id is remapped also boxed `collisions` with `distinct`/`sorted` copies (`SpaceImport.scala:82,95-98,145-150,196`). That is about 300–400 B per aggregate, so 3–4 GB at 10M aggregates (estimate).
- After an import, `RebuildAggregates(OneSpace(into))` is refused while any aggregate of the types anywhere still has space -1 (`Projections.scala:561-563`), for example those `backfillSpaces` left. The new space cannot hold such aggregates, so the documented next step can fail for good.
- The "space id = organization's aggregate id" convention breaks on every same-installation import since F248: the organization's aggregate is remapped, but the space becomes `into` (`SpaceImport.scala:243-246`). This continues the first notes' lesser item; it is now every time, not only on a collision.
- SpacePurge and HistoryRewriter do not reach ephemeral drafts. A draft of a purged organization (idle up to 12 h by default, `AggregateCommandBusActor.scala:86`) can be materialized into the purged space after a complete report, and a draft holding an erased value materializes it later. Chapters 16 and 20 do not say so.
- F192's rebuild of every visited aggregate is sequential: one ask and one registry write per aggregate and projection (`HistoryRewriter.scala:83-98`, `Projections.scala:558-570`). A base with 100 000 copies and 5 projections means 500 000 targeted rebuilds, hours (estimate). Asks to a build in progress time out at 5 min while their jobs stay queued, so the report says failed for work that will be done.

## Fixes checked and found sound

- **F192:** every visited aggregate is rebuilt; jobs are queued for the active build and for the build in progress, and kept in the registry.
- **F193:** a squash and a detach record lineage before the chain rows go (but see R2-LIFE-6 for deletes and purges).
- **F194:** duplicates' owners are told after every rewrite; the copies' generation is raised through `base_id`.
- **F195:** the generation is kept in `Pending`; `add_events` compares `_expected_generation`; `overwriteEvents` locks the row and compares.
- **F198:** one `REPEATABLE READ, READ ONLY` transaction; the out-of-snapshot detach gives the same or erased state; versions are unique.
- **F199:** trailer counts and digest, lines after the trailer, each aggregate's version sequence, chain orders and bases in the file, classes looked up uninitialized, root classes from the contexts. Sound for files the export wrote.
- **F200:** spaces ≤ 0 are refused before any read.
- **F225:** a join instead of the IN list; the detach event is made at its row, so heap is bounded (cost: R2-LIFE-1).
- **F226:** batches are cut by count or bytes.
- **F237:** the guard is `FOR SHARE`; changes raise the generation before deleting snapshots.
- **F244:** the version limit applies only to events that add a version.
- **F245:** an unreadable event is rewritten as text, or the rewrite is refused writing nothing.
- **F246:** each aggregate is judged by its own events' times.
- **F248:** sound when the sequence has been drawn from (gaps: R2-LIFE-2, R2-LIFE-3, R2-LIFE-5).
- **F261:** a permanent delete clears the responses' info and keeps their keys.
- **F190/F191** in this area: sessions use `withoutValues`; refusal and failure texts go through `Errors.describe` (gaps: pass-1 parse errors, whole reports logged).
