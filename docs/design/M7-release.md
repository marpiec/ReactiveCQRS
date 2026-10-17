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

## The test kit (E11, R20)

What an application's tests get from the library, refined before it is built (17/10/2026). The book
has promised it since M6 ("a test kit for applications is coming in 1.0").

- **A published module, `reactivecqrs-testkit`** (package `io.reactivecqrs.testkit`): today's
  `testutils`, whose classes are test-scoped and so not published, renamed, with what an
  application can use moved to its main sources. It depends on the core module, Pekko's cluster,
  ScalikeJDBC, the PostgreSQL driver and mpjsons, and not on a test framework: its helpers return
  values or throw. The library's own ScalaTest base class, `CommonSpec`, stays test-scoped in it.
- **What it holds:**
  - `SchemaReference.verify(mpjsons, dir, samples)`: the golden test of stored formats (R20, built in
    M1), for the application's events, saga orders and responses;
  - `TestDatabase`: a schema of its own per test run on a PostgreSQL for tests, its address, user and
    password from system properties (the library's own as defaults), and `inSchema(suffix)` for a
    second installation;
  - `TestSystems.newSystem()`: an actor system for a test, a cluster of one when
    `-Dreactivecqrs.test.clusterOfOne=true`, so an application can run its specs both ways as the
    library does;
  - `TestCluster`: several nodes in one JVM (configuration, start, join, wait until up, stop), each
    node built by the application's own function (R20's multi-node part; the library's own carts
    stay in its testdomain);
  - `TestClock`, `TestIds` and `ActorAskSupport` (`??`);
  - the in-memory doubles stay in the core module, made faithful where tests rely on them (F156).
- **The second installation:** the import specs compare the projections rebuilt in the target
  installation with the source's (M6's verification gap), with the source's actors stopped while the
  pool points to the target.

| Circumstance | What happens | Handled by |
|---|---|---|
| An application's suites run in parallel against one database | their tables mix in one schema | a schema per test run, suites one at a time (`concurrentRestrictions`, as this build does); the book says so |
| `inSchema` while actors of the first installation work | they write into the second | its block runs with them stopped or idle; documented on the method |
| `TestDatabase` pointed at a database that is not for tests | schemas created there, old `test_run_` ones dropped | only schemas named `test_run_<epoch>_<n>` are dropped; the book says to give it a test database |
| Several nodes in one JVM | ports, JMX beans and shutdown hooks collide | port 0, `multi-mbeans-in-same-jvm`, coordinated shutdown without exiting the JVM, as `TestCluster` sets them |
| The rename of `testutils` | the library's specs and docs name it | one mechanical change of the package; no application depended on it, as it was not published |

## The examples and the book's code (E1–E11)

Refined before they are written (17/10/2026).

- **An `examples` module**, not published, depending on `api`, `core` and the test kit (in its tests):
  one package per example (`io.reactivecqrs.examples.e1` ...), each with a runnable `main` that
  sets up what a host sets up and prints what it does, and a spec that runs the same code against
  the test database and checks its outcome. The examples run in `sbt test` with the other suites, one
  at a time, both as a single node and as a cluster of one.
- **The book's code comes from the examples.** A region of an example source between `// #name` and
  `// #name` is a snippet; a code block in the book preceded by
  `<!-- snippet: e1/BasicSetup.scala#setup -->` must be that region, character for character after
  the region's indentation is removed. `BookSnippetsSpec` checks every such block and fails naming
  the chapter and the snippet; with `-Dreactivecqrs.book.write=true` it writes the regions into the
  book instead, so a change to an example is carried over by one run and reviewed in the diff. Code
  blocks without a marker are fragments that illustrate one call (a message, a configuration line);
  a block a reader would copy to run is a snippet.
- **Order:** E1 first (it fixes the shape every example follows), then the examples of the chapters
  in the book's order. E4 (sagas) waits for the decision on AR5; E2's Docker and Kubernetes files
  are written and read, and run where Docker is.

| Circumstance | What happens | Handled by |
|---|---|---|
| An example changes and its chapter does not | a reader copies code that no longer is the example | `BookSnippetsSpec` fails in `sbt test` until the chapter is written again |
| A snippet region is renamed or removed | its marker in the book names nothing | the spec fails naming the marker |
| Examples share the test database with the suites | their aggregates and projections mix with the specs' | each example names its own tables and takes its ids from the pools; the specs never count all rows |
| An example's `main` run by a reader | it needs PostgreSQL and the connection settings | each `main` reads them like `TestDatabase` (system properties, the library's defaults) and says so when it cannot connect |

## Decisions for the owner

Decided on 17/10/2026: F175 accepted, AR5 now, F165 renamed to `NoInfo`.

- **F175**: accept that the event key costs about 8 % of the plain write path, as the price of
  builds by type and organization that read only their events, or slim the key (one index fewer,
  or the type and space as columns of `events`, which rewrites the table, D69). **Accepted.**
- **AR5**: saga definitions run by a library runner now, with the migration of Neula's sagas in
  the guide, or after 1.0 with today's `SagaActor` kept. **Now, in M7**; it gets a design of its own
  first, and E4 and the saga chapter follow it.
- **JDK 21 on the build machine**: D74 asks for Java 21 bytecode; only JDK 17 is installed here.
  Until then the library is built for Java 17, which runs on Neula's Java 21.
- **The rehearsal**: it needs a copy of Neula's production database, and someone to run it.
- **F165's rename** changes every place Neula names `io.reactivecqrs.api.Nothing`; the migration
  guide lists it. **Renamed to `NoInfo`.**

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
