# M7 design: documentation, examples, migration and the 1.0 release

How milestone M7 brings the 1.0 line to its release: the findings left for it, the test kit and
the examples, the documentation a user needs, the migration from 0.12.47, and the release itself.
Written at the start of the work, 17/10/2026; the plan's M7 section lists the items, and this note
orders them and says where each can fail.

## What the milestone must give Neula

- **A release Neula can plan for**: version `1.0.0`, tagged; the migration from 0.12.47 rehearsed
  on a copy of production, with its duration measured.
- **An API without traps**: what is declared works, and what does nothing is gone (F157, F165).
- **Documentation a newcomer can follow**: the book teaches with code that compiles, runnable
  examples back every chapter, the architecture appendix draws the moving parts, and Scaladoc
  covers the public API.
- **Every High and Medium issue fixed, or deferred with a stated reason.**

## Order of the work

Code first, so that the examples and the book describe the final API.

1. **The findings left for M7**, in batches, each with a test that fails without its fix:
   - Behaviour: F148 (undo events need no handler), F152 (a build's progress), F153 (`ServeStale`
     for a rebuild started at run time), F160 (finders inside the listener's transaction), F161
     (subscribable projections push after the commit, each subscription on its own, a reused id),
     F162 (a late `RegisterSubscribers`, a subscriber that answers with a failure), F163 (delayed
     queries with a `Future`), F164 (a second listener of one kind refused), F166 (a permanent
     delete held by a failed subscription runs once the failure is cleared), F169 (the outbox
     status counts what waits), F171 (an index whose path changed is built again).
   - Performance: F173 (the walk over a type's aggregates), F170 (a compact envelope for a
     delivery's events between nodes), F175 (the event key's cost: a decision first).
   - API clean-up: F157 (`OnConcurrentModification` and `SimulateEvent` go; F157's note that
     `NoEventsForAggregateException` is never thrown no longer holds since D77 throws it), F165
     (`api.Nothing` renamed, as it hides `scala.Nothing`).
   - AR5 (saga definitions run by a library runner): its scope is a decision first.
2. **The test kit** (E11): the in-memory adapters made faithful where tests rely on them (F156:
   spaces in the memory document store), a second installation for import and export tests, and
   a multi-node harness for hosts (R20's multi-node part).
3. **The examples** (`examples` module, E1–E11): each runnable and tested in the local test run.
   E2 (a cluster) also as docker-compose and Kubernetes manifests, run where Docker is available.
4. **The book**: its code included from the compiled example sources, never hand-copied; the
   chapters and sections still missing (upgrades, operations, the configuration reference, the
   test kit, the migration guide); the architecture appendix with Mermaid diagrams.
5. **Scaladoc** for `api` and the public parts of `core`; `sbt doc` without warnings for them.
6. **Contributor documentation**: a CHANGELOG from 0.12.47 to 1.0.0, KNOWN-ISSUES' closed items
   archived, CLAUDE.md and ARCHITECTURE.md describing 1.0.
7. **The migration** (D81): the runbook, with every milestone's host steps (H1–H11), then the
   rehearsal on a copy of production.
8. **Java 21 bytecode** (D74), once JDK 21 is on the build machine.
9. **The release**: version `1.0.0`, tag `v1.0.0`; the owner publishes to Nexus.

## Decisions for the owner

- **F175**: accept that the event key costs about 8 % of the plain write path, as the price of
  builds by type and organization that read only their events, or slim the key (one index fewer,
  or the type and space as columns of `events`, which rewrites the table, D69).
- **AR5**: saga definitions run by a library runner now, with the migration of Neula's sagas in
  the guide, or after 1.0 with today's `SagaActor` kept.
- **JDK 21 on the build machine**: D74 asks for Java 21 bytecode; only JDK 17 is installed here.
  Until then the library is built for Java 17, which runs on Neula's Java 21.
- **The rehearsal**: it needs a copy of Neula's production database, and someone to run it.
- **F165's rename** changes every place Neula names `io.reactivecqrs.api.Nothing`; the migration
  guide lists it.

## Failure circumstances

| Circumstance | What happens | Handled by |
|---|---|---|
| The book's code drifts from the library | a reader copies code that no longer compiles | the book includes code from the compiled example sources; the README's quick start is compiled already (`ReadmeQuickStartSpec`) |
| An API rename or removal reaches Neula | Neula's build breaks on upgrade | each change is listed in the migration guide with its replacement |
| A compact envelope (F170) during a rolling upgrade | a node of the version before cannot read it | sent only once every node runs the version that reads it (D67, `Members`), as new event classes are |
| An index built again (F171, F173) on a large table | a plain `CREATE INDEX` blocks writes for minutes | built concurrently on a large table (D69), in the background step, as `buildIndexesConcurrently` does |
| Subscribable projections push after the commit (F161) | a push lost if the node stops between the commit and the push | the listener re-reads on renewal; documented: pushes are at most once, the store is the truth |
| The examples need PostgreSQL | they cannot run without it | they use the test database the suites use; the README says so |
| Docker is missing (this machine) | E2's compose file and the fresh-reader test cannot run here | written and checked by reading; run where Docker is, before the release |
| The rehearsal finds unreadable rows or a slow step | the migration date moves | the runbook's dry run (`StoredDataCheck`) comes first, and its findings are fixed before the date |
