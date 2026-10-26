# Upgrading an installation from 0.12.47 to 1.0: the runbook

How to move a production installation of ReactiveCQRS 0.12.47 to 1.0, once (decision D81). It is written for the
people who run the upgrade, and it is rehearsed on a copy of production before the real one; the rehearsal fills in
the durations left blank below.

The move is a **planned stop**: a 0.12 node cannot join a 1.0 cluster, so every 0.12 node stops before the first 1.0
node starts. Stored data stays where it is: 1.0 reads every table 0.12 wrote, its schema changes at start are small
steps under a lock, and the large ones run in the background afterwards, while the application serves. From 1.0 on,
upgrades are rolling ([chapter 17 of the book](book/17-cluster.md)).

---

## 1. Before: the code (weeks ahead)

The application moves to 1.0's API and wiring first, on a branch. Every step is a row of
[KNOWN-ISSUES.md, section 4](KNOWN-ISSUES.md) (H1 to H12) and an item of the book's
[chapter 18, "When you upgrade to 1.0"](book/18-production.md); in short:

- events: no event versioning (a changed shape is a new class), first events give their space, undo events need no
  handler case, `Option`s instead of `null` checks (H4);
- wiring: `initSchema()` on every PostgreSQL component, one `RegisterSubscribers` with every subscriber, the outbox
  poller, managed projections through `Projections.start`, sagas' commands idempotent with `SagaStep` (H3);
- the cluster: `pekko.actor.provider = cluster` with the split brain resolver on every node, the library's
  serializer bound in the configuration (F178), the singletons through `Singletons.start`, an app-version per
  release, and `ReadinessCheck` in the health checks;
- the API renames the compiler names (`NoInfo` for `api.Nothing`, `askFor` in tests).

The application's own specs pass on 1.0, against a copy of production's schema too.

## 2. Before: the rehearsal (on a copy of production)

On a restored copy of the production database, with the 1.0 build of the application:

1. **Read every stored value with 1.0's JSON library**, before anything is changed: `StoredDataCheck.run(mpjsons,
   typesNamesState, projectionTables)` from the application, which reads only. Every row must read; every
   event must be at event type version 0 (D29). The report names the tables it did not find (F181); on a 0.12
   database that is `events_archive`, which 1.0 makes. Unreadable rows are fixed (converters for old forms, F52) before the
   date.
2. **Run the pre-check queries**, which must all answer zero:

   ```sql
   -- duplicate saga rows stop the saga state's initSchema() (F34)
   SELECT name, saga_id FROM sagas GROUP BY 1, 2 HAVING count(*) > 1;
   -- copies whose base was deleted for good (F76)
   SELECT count(*) FROM aggregates a
    WHERE a.base_id <> a.id AND NOT EXISTS (SELECT 1 FROM aggregates b WHERE b.id = a.base_id AND b.base_id = b.id);
   ```

   And note this one's answer, the aggregates without a space (F49), which the space backfill gives one:

   ```sql
   SELECT count(*) FROM aggregates WHERE space_id = -1;
   ```

3. **Run the upgrade on the copy**, sections 4 to 6 of this runbook, timing each step.
4. **Check the result** as section 5 does, and run the application's smoke tests against it.

| Step (filled in by the rehearsal) | Duration | Notes |
|---|---|---|
| `StoredDataCheck` | | |
| first 1.0 start (schema steps) | | |
| `buildIndexesConcurrently()` | | |
| `backfillSpaces(...)` | | |
| `backfillEventKeys()` | | |
| rebuilds of the projections whose version changed | | |

The space backfill runs after the start, online: on a 0.12 schema it can do nothing, as it reads `events.metadata` and
writes the events' keys, columns 1.0's first start adds (F336). `buildIndexesConcurrently()`, on the other hand, may run
against production before the stop, while 0.12 serves: it builds on 0.12's columns, without blocking writes, and takes
the heaviest index work out of the window. Try it first in the rehearsal, on the copy under test traffic (F339).

## 3. The day before

- The rehearsal's checks pass, and its durations fit the window.
- A backup of production is tested (restored once).
- The 1.0 build of the application is deployed nowhere yet, and ready to be.
- The clients know about the window: commands sent during it are refused or time out, and a client sends them again
  afterwards, with the same idempotency key where it has one.

## 4. The stop

1. **Stop taking commands** at the entry points (maintenance mode). Let in-flight work finish for a minute: commands
   answer, deliveries reach the projections, sagas reach a stored step.
2. **Stop every 0.12 node.** None may run while a 1.0 node starts.
3. **Take the backup** (or a storage snapshot) and note when.
4. **Deploy 1.0** with its configuration: the cluster settings of every node, the serializer binding
   (`pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }`), `app-version`.
5. **Start one node.** At start every PostgreSQL component's `initSchema()` makes its changes: each an idempotent
   step under the schema lock, so it is safe if nodes start together; the large index builds are not among them
   (step 7). Watch the log for errors; the node becomes ready once its event bus knows all its subscribers.
6. **Start the other nodes**, and open the entry points again once they are ready (`/ready`).

What carries over by itself:

- **The outbox.** Rows 0.12 left in `events_to_publish` are published again by the outbox poller at its first poll
  (10 seconds after its start).
- **Projections.** A projection 0.12 built is adopted as build 0 of its managed projection (D81): at the version 0.12
  recorded, or at the code's version when only its subscription rows exist. One whose code's version differs is built
  again in tables of its own while build 0 serves, and replaces it once caught up.
- **Sagas.** Stored sagas resume where they were, under the saga definition of their name, or as `SagaActor`s.
- **Subscriptions and idempotent responses** stay as they are.
- **0.12's `event_bus` and `events_to_route` tables** stay unused; do not drop them yet (D24).

## 5. Right after: is it working?

- Every node is ready (`/ready`), and `Readiness(system).waitingFor` is empty.
- `GetDeliveryStatus` on each node's bus: no stopped subscribers, waiting deliveries going down.
- The outbox poller's status: the rows from before the stop are going out.
- `GetBuilds` of each managed projection: build 0 active, and a build in progress only for projections whose version
  changed; `GetBuildStatus` shows it at work.
- `GetProjectionStatus` of each projection: no failed subscriptions.
- The application's smoke tests: a command per main aggregate type, and its read model answering.

If something is wrong and no command of the new version was taken yet, the way back is the backup of step 3 with the
0.12 build. Once 1.0 has taken commands, fix forward: a 0.12 node does not read everything 1.0 writes.

## 6. Afterwards, in the background

While the application serves, each once, in this order (the book's
[chapter 18, "After an upgrade: the online jobs"](book/18-production.md)). Since 1.0.1-RC2 the application can run
steps 1 to 4 itself, in the background, with `OnlineJobs.start`, and hold full builds of projections until they are
done (`BuildHold.untilStoreReady`): no manual step, and no build that reads the whole `events` table.

1. `buildIndexesConcurrently()` (unless it ran before the stop): the indexes too large to build at start, without
   blocking writes.
2. `PostgresDocumentStore.buildSpaceIndexesConcurrently()`, from one process: each document table's index on its
   space, by which a purge or a rebuild of one organization clears it (F217).
3. `backfillSpaces(spaceOf)`: gives the aggregates without a space the space of their first event, in batches;
   run it again while `left` shrinks, and decide on what stays. Rebuilds of one organization's read models wait for it.
4. `backfillEventKeys()`: every event's type and space, so builds of a few types or one organization read only their
   events; until it is done they walk the whole store. It writes each older event row once more (D95): `events` and its
   indexes grow by about the table's size until VACUUM takes the old row versions; plan the disk for it.
5. `buildIndexesConcurrently()` again: the indexes the event keys (built only now, after their backfill) and the
   aggregates' own rows need.
6. `Projections.retire(name)` for each projection the new code removed.

From here on, upgrades are rolling: a new app-version per release, the oldest node last
([chapter 17](book/17-cluster.md)).
