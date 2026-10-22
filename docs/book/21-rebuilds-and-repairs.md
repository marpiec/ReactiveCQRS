# Chapter 21. Rebuilds and Repairs in Depth

[Chapter 10](10-rebuilding-projections.md) showed how to ship a new version of a read model while the
old one serves. That is the everyday case, and most of the time it is all you need. This chapter is
for the other times: a big store, an upgrade, a build that will not switch, data your code no longer
understands. By the end you will know what a build goes through step by step, what can stop it at
each step and how you see it, how to plan a rebuild on a large installation, and what to do with
each kind of stored data that does not fit.

It is a reference as much as a chapter. Read it once from the top; come back to the tables when
something happens.

---

## Part 1: the life of a build

### The registry remembers, the supervisor decides

Think of a building site with a site office. The office keeps the plans: which building is in use,
which one is going up next door, and how far the work got. The site manager reads the plans every
morning and decides what to do that day.

The **registry** is the site office: two small tables, per projection the active build and the
build in progress, and per build its number, version, table suffix, subscriber name, the context
versions it was made with and its saved position. The **supervisor** is the site manager. Each time
it starts (the application starts, or the supervisor moves to another node in a cluster), it reads
the registry and decides:

| What the registry says | What the supervisor does |
|---|---|
| it never heard of the name | builds it from the store: build 1, which also serves while it is built |
| the active build is at the code's version, nothing in progress | serves the active build; nothing to build |
| the active build is at another version | starts a build of the code's version |
| a build in progress of the code's version | continues it from its saved position |
| a build in progress of another version | abandons it, drops its tables, and starts one of the code's version |
| a context whose states the projection receives has another `version` | starts a build (only for projections with state listeners) |
| builds that are neither active nor in progress | drops them (a stop came between a switch and a drop) |

A projection that ReactiveCQRS 0.12 built is **adopted** as build 0, with its tables and
subscription rows as they are, at the version 0.12 recorded: nothing is rebuilt at the upgrade unless
the version differs ([chapter 18](18-production.md), "When you upgrade to 1.0").

> **Remember:** the version you pass to `start` is a promise about the tables. Raise it whenever the
> documents your code writes change in meaning or shape. Keep it when they do not, even if the code
> changed: a version bump costs a full build.

### Waiting for a slot

A build that is due does not always start at once. It asks the runtime's **build slots** for its
turn: at most `buildsAtOnce` builds (two) run together, the others wait in the order they asked. With
a hold (`holdBuilds`, [chapter 10](10-rebuilding-projections.md)), no build gets a slot until the
hold is over. While it waits, `GetBuildStatus` answers phase `Waiting`, with `heldBy` saying why when
a hold keeps it.

A waiting build is not idle: it is already subscribed to the event bus, and it acknowledges every
delivery without applying it. That is safe because the bulk pass will read those events from the
store anyway. So a long wait costs nothing but time.

### The three phases

Once it has its slot, a **builder** fills the build. Picture moving a library to a new building
while it stays open: first you carry over the shelves in order, then you open the new doors, then you
walk the aisles with the catalogue and fetch the few books that were borrowed and returned while you
were moving.

1. **Bulk** (`Bulk`). The builder reads the events of the projection's aggregate types in the order
   they were committed, a page at a time (1 000 events, at most 64 MB), cuts them into chunks (1 000
   events, at most 16 MB), and sends each chunk to the build, which applies it in one transaction
   together with the subscription versions it reaches. Two chunks are in flight; the next page is
   read while they are applied. After each applied chunk the position is saved in the registry.
   A projection whose listeners all take states only (`AggregateListener`) gets each aggregate's
   final state instead, one item per aggregate.
2. **Going live** (`GoingLive`). The build starts applying the bus's deliveries itself, and retries
   the failures it recorded during the pass.
3. **Reconciliation** (`Reconciling`). The builder walks every aggregate of the types and compares
   the version the store has with the version the build has. What is missing (events committed after
   the pass read past them, rows that committed late) is read again by version and delivered.

Then the build is **caught up** (`CaughtUp`). Before it may serve, two more things: the targeted
rebuilds requested meanwhile run on it too (its bulk pass may have read those aggregates before they
changed), and it must have no failed subscription. Then the supervisor **switches**, in one update of
the registry.

### The switch and what follows

The switch is a single row update: from that moment, queries go to the new build. Then:

- the new build's `onActivated()` runs, and `rebuilding` turns false for it;
- the old build leaves every node's event bus, hands its push subscriptions to the new build
  ([chapter 11](11-subscribable-projections.md)), answers its parked queries with `None`, and stops;
- its tables and sequences are dropped, its subscription versions deleted (in batches of 10 000),
  its failure records and its records of aggregates it could not restore removed.

> **Under the hood:** the bulk pass reads by event id. Ids are handed out when a transaction writes,
> not when it commits, so a row with a smaller id can appear after the pass read past its place. Such
> a row shows up as a gap (the build has version 4, the next run starts at 6): the builder reads the
> missing versions again by version. The reconciliation catches whatever is left. That is why a build
> ends up equal to a projection kept live, which `RebuildEquivalenceSpec` checks on random histories
> ([ARCHITECTURE.md, section 8](../ARCHITECTURE.md)).

### When things stop half-way

A build is made to be interrupted. Here is what each interruption does:

| What happens | What the build does |
|---|---|
| the application stops, or a node leaves and the supervisor moves | the next supervisor reads the saved position and continues the bulk pass from there; the chunks in flight are applied again, which the subscription versions make harmless |
| the builder fails (the store cannot be read for a while) | it reads the page again after `retryAfter` (5 s); a builder that stops altogether is started again after `restartAfter` (10 s), from the saved position |
| the build's instance stops (its stores cannot be made) | the active build serves on; a new instance of the build starts after `restartAfter` |
| the database is down | batches are retried without counting as failures; nothing is recorded against your listeners |
| you deploy a newer version mid-build | the half-done build is abandoned and dropped; the newer version builds |
| you roll back to the older code mid-build | the new build is abandoned; the active build catches up with what it missed, by a reconciliation of every aggregate of its types |

> **Warning:** state that your listener keeps in memory between calls, outside its tables, does not
> survive these interruptions. A build commits chunk by chunk and may resume from any of them.

### Many nodes

In a cluster ([chapter 17](17-cluster.md)) each managed projection runs once, as a singleton: the
supervisor, its builds and its builders live on one node, and every node's event bus delivers to it
through a proxy. A build reads the store directly, so it never travels between nodes. When that node
leaves, the singleton starts on another node, reads the registry and continues as after a stop. A
hold reads the database, so every node agrees on it.

## Part 2: what stops a build, and how you see it

### A listener that fails

A listener that throws on an aggregate is retried for about two minutes, as for live deliveries
([chapter 9](09-projection-failures.md)); then the failure is recorded under the build's subscriber
and the build goes on with the other aggregates. When it catches up with failures, it does not
switch: the supervisor logs "caught up with N failed subscriptions" and looks again every 30 seconds.

What you do: look (`ToBuildInProgress(GetProjectionStatus)`), fix the listener, deploy, retry
(`ToBuildInProgress(RetryAllFailedSubscriptions)`); or, for an aggregate whose documents can wait,
skip it (`SkipFailedVersion` for one version, `SkipAggregate` for all of it).

### Data the build cannot use

Four things look alike from far away, and the failure tells them apart:

| The failure says | What it means | Where else you see it |
|---|---|---|
| `a stored event cannot be read: ...` | an event's JSON no longer reads into its class (a field changed type, a class was renamed) | `StoredDataCheck`'s `unreadable` |
| `the aggregate cannot be restored: ...` | the events read, but your event handler throws on one of them while the state is replayed | `eventStore.restoreFailures()`, `RestoreCheck` |
| `versions from N to M are missing from the store` | the aggregate's stored versions have a hole: those versions are not in the store at all | `eventStore.restoreFailures()` (`missing versions`), `RestoreCheck` (`report.gaps`) |
| `versions from N are missing: ...` | versions were asked for and never came (rare during a build: the builder reads the store itself) | `ProjectionStatus.waitingForGap` before it is recorded |

The first three are recorded at once, with the aggregate, the event it failed at and the cause, and the
build goes on without that aggregate. Part 3 says what to do about each.

> **Tip:** a projection that only listens to events (`EventsListener`) needs no states, so a handler
> that throws on replay does not touch its builds. A projection with state listeners for the type is
> the one that cannot be built past such an aggregate.

### A store that is not ready

On an installation upgraded from an older version, a build may run but run slowly:

- until every older event has its key (`backfillEventKeys`), each bulk pass reads the whole `events`
  table, whatever types the projection needs;
- until the index of the aggregates' own rows by type exists (`buildIndexesConcurrently`), each
  page of aggregates (final states, reconciliation) takes about 100 ms instead of 1 ms.

`eventStore.buildReadiness()` tells you which is missing. The cure is to run the online jobs
([chapter 18](18-production.md), step 7), and to hold builds until they are done.

## Part 3: stored data that does not fit

### The rule: the library finds, you decide

Old data has corners. A field removed three releases ago, a script that deleted half of something in
2020, an event your handler stopped accepting. ReactiveCQRS takes one stance about all of them: it
never repairs, deletes or skips stored data on its own. It **finds** it (with a check you run, or as
it meets it), **reports** it (with ids, classes, versions and causes, never the values), keeps
**everything else running**, and lets **you** choose the repair. The table is the map; each row is
explained below it.

| What is wrong | How you find it | Your options |
|---|---|---|
| an event's JSON no longer reads | `StoredDataCheck` (`unreadable`); builds: "a stored event cannot be read" | fix the class or add a converter; erase the value ([chapter 15](15-rewriting-history.md)) |
| an event of a non-zero event type version | `StoredDataCheck` (`eventsByTypeVersion`) | migrate it by hand before 1.0 reads it (D29) |
| a handler throws while the state is replayed | `restoreFailures()`, `RestoreCheck`, a command's `AggregateRestoreFailedException` | fix the handler; skip it in a build; remove the aggregate |
| events whose aggregate has no row of its own (orphans) | `backfillEventKeys` (`orphans`), `StoredDataCheck` (`orphans`) | leave them (nothing reads them); remove them |
| two events of one version | `StoredDataCheck` (`repeatedVersions`) | decide which one is real and delete the other by hand |
| a version missing from an aggregate's history | `RestoreCheck` (`gaps`), the query of [chapter 18](18-production.md); builds: "versions from N to M are missing from the store" | skip the hole in each build (`SkipFailedVersion(id, pastMissing = true)`); skip the aggregate; remove it |
| aggregates without an organization | `backfillSpaces` (`left`), `StoredDataCheck` (`aggregatesWithoutSpace`) | a `spaceOf` function that places them ([chapter 16](16-organizations.md)) |
| aggregates of a type no context has | `StoredDataCheck` (`typesWithoutContext`, with `contexts`) | give the type its context back, or leave them unread |
| a document that is wrong although the events are right | your users, or a comparison with a fresh build | `RebuildAggregates` for it ([chapter 10](10-rebuilding-projections.md)) |
| a listener failing on an aggregate | `GetProjectionStatus` | fix and retry; skip ([chapter 9](09-projection-failures.md)) |

### Events that no longer read

The usual cause is a class change: a field's type changed, a class moved package. Each such event is
unreadable for every reader, so a command on its aggregate fails, and a build fails the aggregate's
subscription. Find them all before an upgrade with `StoredDataCheck` ([chapter 18](18-production.md),
step 8), on a copy of production, with the new code. The cure is in the code: keep the old class
next to a `V2` ([chapter 6](06-events.md)), or register a converter for the old form. When the event
holds personal data that must go anyway, a history rewrite can change the unreadable JSON as text
(`HistoryChange.inUnreadableEvent`, [chapter 15](15-rewriting-history.md)).

### Aggregates your code cannot restore

The events read fine, but replaying them throws: a `MatchError` for an event your handlers forgot, a
check that old data fails. Such an aggregate takes no command (its owner answers
`AggregateRestoreFailedException`) and blocks the builds of projections with state listeners for its
type. Every place that meets it records it, once per aggregate and place:

```scala
import io.reactivecqrs.core.eventstore.RestoreFailureFilter

eventStore.restoreFailures(RestoreFailureFilter(projection = Some("carts")))
// RestoreFailure(aggregateId, aggregateType, spaceId, version, eventId, errorClass, error,
//                source, firstSeen, lastSeen, times, skipped)
```

`source` says where: `Owner`, `Check` (`RestoreCheck`), or `Build(projection, build, subscriber)`.
Before an upgrade, `RestoreCheck.run(eventStore, contexts)` restores every aggregate with the new code
and fills the same records, so you know the list before any build meets it.

Then, in order of preference:

1. **Fix the handler.** If the event is real history, the handler is what is wrong: make it accept
   the event (often one more `case`), deploy, and retry the failures. The records clear themselves as
   the aggregate restores again.
2. **Skip it in one build.** If the read model can live without that aggregate for now, send
   `SkipAggregate(id)` to the projection (through `ToBuildInProgress` for the build in progress). The
   build switches; the record says skipped; the events stay.
3. **Remove it.** If the aggregate should not exist at all (test data, the remains of a bug), remove
   it in one transaction with the statements of [chapter 18](18-production.md) ("Removing an
   aggregate by hand"), then retry the failures. The projections' documents derived from it stay
   until you clear them.

### Orphan events

An orphan is an event whose aggregate has no row of its own in `aggregates`, usually left by an old
delete that removed the row and not the events. Nothing reads an orphan: a restore, a build and a
reconciliation all start from the aggregate's row. So orphans are harmless where they are, except
for the disk they take. `backfillEventKeys` and `StoredDataCheck` list them (how many, of which
aggregates, their classes, first and last event); the key backfill records its step without them.
Keep them, or remove them with the same statements as an aggregate (the `aggregates` lines then delete
nothing).

### Versions stored twice

One event per version is the store's first rule, and 1.0.1 enforces it with a unique index. A store
written by older versions, or an import gone wrong, may still hold two events of one version for an
aggregate. Restores then read both. `StoredDataCheck` names them (`repeatedVersions`), and until they
are gone `buildIndexesConcurrently` keeps the old, non-unique index and logs an error. Look at both
events, decide which one the aggregate really had, and delete the other by its event id: its
`noop_events` row first, if it has one, as the removal statements do (its key goes with it).

### Read models that drifted

The events are right, the document is wrong: a hand-written SQL fix, a listener bug fixed since, a
document deleted by mistake. That is what targeted rebuilds are for:
`RebuildAggregates(RebuildTarget.OneAggregate(id))` for one aggregate, `OneType` or `OneSpace` for
more, in place, while the projection serves. For a bug that touched everything, `RebuildProjection`
builds the whole projection again next door and switches.

> **Warning:** never fix a read model by editing its tables only. The next targeted rebuild or full
> build writes the documents again from the events, and your fix is gone. Fix the events (or the
> code), then rebuild.

## Part 4: planning a rebuild on a large installation

A small projection rebuilds in seconds and needs no plan. On a store of tens of millions of events,
think it through first.

1. **Estimate the time.** On the measured machine a build applied about 6 800 events per second
   (events-based, every state in memory; `docs/perf/baseline-M4.md`, 17/10/2026); `docs/SCALING.md`
   estimates about 4.1 hours for 100 million events. A state-only projection reads final states, one
   per aggregate, and does far less work. Measure on a copy of production: your events, your listeners
   and your database decide.
2. **Make the store ready first.** On an upgraded installation, run the online jobs and hold the
   builds until they are done (`OnlineJobs.start` and `BuildHold.untilStoreReady`, chapter 18).
   Without event keys, every build reads every event, whatever types it needs.
3. **Look for data that will not fit**, on the copy, with the new code: `StoredDataCheck` and
   `RestoreCheck`. Each aggregate they list will stop a build until you decide about it.
4. **Choose what the old build does**: `ServeLive` if the new code can work on the old documents,
   `ServeStale` if not (chapter 10). With `ServeStale` the old build serves frozen data for the
   whole build, hold included.
5. **Mind the load.** Each running build reads the store and writes its own tables; two run at once.
   Bump versions of several big projections in one release only if the database can carry it, or
   lower `buildsAtOnce` to 1.
6. **Watch it run.** `GetBuilds` (`building`, `caughtUp`, `heldBy`), `GetBuildStatus` (phase,
   position, events sent, unreadable), `ToBuildInProgress(GetProjectionStatus)` (failures), the
   gauge `reactivecqrs.builds.held` and `eventStore.restoreFailures()`.
7. **Know what you will do with a failure** before it happens: who decides between a code fix, a
   skip and a removal, and how fast.

> **Tip:** while a build runs, the active build serves as usual. A long build is a cost in time and
> database load, not in downtime. The things that make it go wrong are almost always in the data,
> which is why steps 3 and 7 come before you deploy.

## What you learned

- The registry remembers builds; the supervisor reads it at every start and decides what to build,
  continue, abandon or drop.
- A build waits for a slot (and a hold), then goes through the bulk pass, going live and the
  reconciliation, and switches only when caught up with no failures; every interruption resumes.
- A failure tells you what kind of data stopped it: an event that does not read, an aggregate that
  does not restore, versions that never came.
- The library never repairs stored data on its own: `StoredDataCheck`, `RestoreCheck`,
  `restoreFailures()` and the backfills find it; you fix the code, skip in a build, or remove.
- A rebuild on a large store is planned: the store made ready, the bad data found on a copy, the
  load and the decisions settled before the deploy.

That is the end of the book. The appendices hold the glossary, the cheat sheet of messages, the
troubleshooting guide, the architecture in pictures and every setting.
