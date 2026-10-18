# Chapter 2. Setting Up Your Workshop

By the end of this chapter you will have a Scala project that compiles against ReactiveCQRS, a
PostgreSQL database ready for it, and the handful of start-up lines every application needs: a
connection pool, the library's tables and a configuration file. It is the least glamorous chapter
of the book, and the one that saves you the most head-scratching later. Let's clear the workbench.

---

## What you need on your desk

- **A JDK, version 21 or newer.** The library is compiled for Java 21 (`-release 21` in its
  build), and its suites run on Java 25 too.
- **Scala 2.13.** The library is built with Scala 2.13.18 and published for 2.13 only.
- **sbt**, the Scala build tool. Any 1.x release will do; the library's own build uses 1.11.7.
- **PostgreSQL**, running somewhere your machine can reach. The library's measurements (see
  [docs/SCALING.md](../SCALING.md)) ran on PostgreSQL 17.4, so a recent release is a safe choice.
- **A SQL client** such as `psql`, to peek at what the library writes. Optional, but half the fun.

You do not install Pekko, ScalikeJDBC or the PostgreSQL driver yourself: they come with the library.

## Step 1: a database for the library

ReactiveCQRS keeps everything in PostgreSQL: events, aggregates, read models, saga progress. Give
it a database and a user. In `psql`, connected as a superuser:

```sql
CREATE USER reactivecqrs WITH PASSWORD 'reactivecqrs';
CREATE DATABASE reactivecqrs OWNER reactivecqrs;
```

These are the very names the library's own test suite expects: a database `reactivecqrs` on
`localhost:5432`, with user and password `reactivecqrs` (see `TestDatabase` in the `testkit`
module). So this one database serves your experiments and, whenever you like, a run of the
library's tests. For your real application, pick your own names and a password nobody can guess.

> **Tip:** the library creates its tables in whatever schema your connection uses, so one database
> can hold several independent copies. Run `CREATE SCHEMA shop AUTHORIZATION reactivecqrs;` and add
> `?currentSchema=shop` to the JDBC URL of step 3. The test suite plays the same trick: every run
> gets a fresh schema named `test_run_<seconds>_<number>`, and schemas older than six hours are
> dropped.

## Step 2: add the library to your build

ReactiveCQRS is published as two artifacts. Here are its coordinates, straight from the library's
own build (`project/Common.scala` and `build.sbt`):

| | |
|---|---|
| Organization | `io.reactivecqrs` |
| Artifacts | `reactivecqrs-api` and `reactivecqrs-core` |
| Version | `1.0.0-SNAPSHOT` |
| Scala suffix | yes (`_2.13`), so use `%%` |

A minimal `build.sbt` for an application:

```scala
ThisBuild / scalaVersion := "2.13.18"

resolvers += "Neula releases" at "https://nexus.neula.in/repository/neula-releases"

libraryDependencies ++= Seq(
  "io.reactivecqrs" %% "reactivecqrs-core" % "1.0.0-SNAPSHOT",
  "ch.qos.logback"  %  "logback-classic"   % "1.5.32"   // or any other slf4j backend
)

run / fork := true   // run the application in a JVM of its own
```

What do the two artifacts give you?

- **`reactivecqrs-api`** is everything your domain code touches: `AggregateContext`, the command and
  event base classes, command results and answers, ids. It is small: it needs Pekko's actor module
  (with its slf4j bridge), mpjsons, the JSON library you will meet in step 4, and a few helpers.
- **`reactivecqrs-core`** is the machinery: the actors, the event store, delivery, projections,
  sagas. It brings `reactivecqrs-api` along, plus Pekko's cluster modules, ScalikeJDBC and the
  PostgreSQL JDBC driver.

**Where the artifacts come from.** The library's build publishes them to the Maven repository
`https://nexus.neula.in/repository/neula-releases` (the `publishTo` setting in
`project/Common.scala`); that is the `resolvers` line above. Ask your team whether you need
credentials for it. If you work from a clone of the library instead, run `sbt publishLocal` in the
clone and drop the `resolvers` line: sbt looks in its local repository by default.

Parts of the library log through slf4j directly, and with the configuration of step 5 Pekko's log
lines (those of the library's actors among them) go there too. slf4j needs a backend to print
anything; the library's own tests and benchmarks use `logback-classic` 1.5.32, as above.
`run / fork := true` is what the library's benchmark module does too: your program gets a JVM of its
own, so it can stop cleanly and is not mixed up with sbt's.

> **Warning:** Pekko refuses to start when its modules come in different versions (its
> `pekko.fail-mixed-versions` setting is on). ReactiveCQRS uses Pekko **1.7.1**. If you add more
> Pekko modules to your build (streams, testkit, cluster tools), give them that same version.

## Step 3: the connection pool

ReactiveCQRS never opens a database connection by itself. All of its SQL goes through
**ScalikeJDBC's default connection pool**, the one ScalikeJDBC calls the *singleton* pool. Your
application owns that pool: it creates it, sizes it and closes it. And it must create it **before
any library code runs**, because some library classes talk to the database the moment they are
made.

This is how the library's own tests do it (`TestDatabase`), with the pool enlarged from their 20
connections to 32 (more on that number below):

```scala
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

Class.forName("org.postgresql.Driver")
ConnectionPool.singleton(
  "jdbc:postgresql://localhost:5432/reactivecqrs",
  "reactivecqrs",
  "reactivecqrs",
  ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
```

- `initialSize`: connections opened at start.
- `maxSize`: the most connections the pool will ever open.
- `connectionTimeoutMillis`: how long a thread waits for a free connection before it gives up.

Why 32? ScalikeJDBC's own default is a pool of 8 connections, but the library runs its database work
on 32 threads of its own (the *blocking dispatcher* of step 5). [docs/SCALING.md](../SCALING.md),
section 2, recommends a pool at least that large; its benchmarks use 40. PostgreSQL's
`max_connections` must then cover the pools of all your machines together.

When the application stops, close the pool with `ConnectionPool.closeAll()`.

> **Warning:** forget the pool, or create it too late, and the first library call fails with
> `java.lang.IllegalStateException: Connection pool is not yet initialized.(name:default)`.
> A pool that is too small fails more quietly: under load, a thread that waits longer than
> `connectionTimeoutMillis` for a connection fails its work, and a command that hits this is
> answered with an error. The order is always the same: **pool first, then tables, then
> everything else.**

> **Tip:** if your application already has a pooled `DataSource` (HikariCP, say), hand it to
> ScalikeJDBC instead: `ConnectionPool.singleton(new DataSourceConnectionPool(dataSource))`. And
> read the URL, user and password from your configuration rather than writing them into the code.

## Step 4: the tables

Each part of the library that stores something has a **state class** with an `initSchema()`
method. `initSchema()` creates the tables, sequences, indexes and stored procedures that part
needs. Most of them take two helpers, so let's meet those first.

### The JSON helper: MPJsons

Events, idempotent command responses, saga progress and read-model documents are all stored as
JSON. The library writes and reads that JSON with **mpjsons** (`io.mpjsons.MPJsons`), a small
library that looks at your case classes by reflection: you write no codecs at all. Make one instance
at start and hand it to every class that asks for it. The library's specs and benchmarks all use it
with its default settings:

```scala
import io.mpjsons.MPJsons

val mpjsons = new MPJsons
```

### The class-name dictionary: PostgresTypesNamesState

Class names are long, and the library writes one with every event. So it keeps a dictionary in a
table called `types_names`: each class name gets a small number once, and the other tables store the
number. That dictionary is `PostgresTypesNamesState`, and nearly every other state class takes it
as a parameter, so it comes first.

### All five, in order

This is the order E1's `SetupCheck` uses, the program at the end of this chapter:

<!-- snippet: e1/SetupCheck.scala#tables -->
```scala
val mpjsons = new MPJsons

val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema()   // this one returns nothing, so it takes a line of its own

val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
```

Four of the five `initSchema()` methods hand back the object itself, which is why they fit on one
line. Only the dictionary has to come first; after it, the order does not matter.

| Call | Creates | Needed for |
|---|---|---|
| `PostgresTypesNamesState.initSchema()` | `types_names` | everything: the class-name dictionary |
| `PostgresEventStoreState.initSchema()` | `events`, `aggregates`, `events_to_publish` (the outbox), `noop_events`, `snapshots`, `event_keys`, `events_archive`, `schema_steps`, `aggregate_lineage`, `history_rewrites`, and the stored procedures that write events | everything: this is the event store |
| `PostgresCommandResponseState.initSchema()` | `commands_responses` | every command bus; idempotent commands ([chapter 5](05-command-flavors.md)) |
| `PostgresSagaState.initSchema()` | `sagas` | sagas ([chapter 12](12-sagas.md)) |
| `PostgresSubscriptionsState.initSchema()` | `subscriptions`, `subscription_failures` | projections ([chapter 8](08-projections.md)) |

A few things set up their own storage and need no call from you: the id generator creates its
three sequences when you make it ([chapter 4](04-first-command.md)), and every read-model store
creates its own table ([chapter 8](08-projections.md)). If you use managed projections, their
registry has an `initSchema()` too: `new PostgresProjectionRegistry().initSchema()`
([chapter 10](10-rebuilding-projections.md)).

> **Tip:** `PostgresEventStoreState` takes a few optional settings after its two parameters.
> Two are worth knowing from day one: `eventSizeLimit` (at most 100 000 characters of JSON per
> event) and `aggregateVersionLimit` (at most 10 000 events per aggregate). A command that would
> break either is answered with an `EventHandlingError`, and nothing is stored. The defaults suit
> most domains; [chapter 6](06-events.md) tells more about both limits, and chapters 13 and 14 show
> how to live with long histories.

> **Warning:** do not skip `PostgresCommandResponseState.initSchema()`, even if you have no
> idempotent commands yet. It creates the unique index that idempotent commands rely on; without
> it, every idempotent command fails.

### Safe to run at every start

Call all of these **every time your application starts**. They are written to be idempotent: every
statement is a `CREATE ... IF NOT EXISTS` or its cousin, so on a database that has everything they
change nothing. On a database made by an older version of the library they add whatever is missing
(a column here, an index there), which is how most upgrades happen.

Several machines starting at the same moment are fine too. Every `initSchema()` runs under a
PostgreSQL advisory lock, the **schema lock**, so the machines take turns instead of tripping over
each other. A machine waits up to five minutes for its turn before it gives up.

> **Under the hood:** the stored procedures (`add_event`, `add_events`, `add_undo_event`,
> `add_duplication_event`) are where the optimistic lock lives: they refuse to write an event when
> the aggregate's stored version is not the one the command expected. The tables are described in
> [ARCHITECTURE.md, section 4](../ARCHITECTURE.md), and the schema lock in section 13.2. The few
> heavy jobs a very large, older database may need (building an index without blocking writes,
> filling in data for old rows) are separate calls, never part of `initSchema()`.

You will also find `Memory...` versions of several of these classes (`MemoryEventStoreState`,
`MemoryUidGenerator` and friends). They are test doubles for the library's own tests, not a way to
run without PostgreSQL.

## Step 5: configuration

ReactiveCQRS reads its settings the way Pekko does, with Typesafe Config. Its defaults live in the
`reference.conf` inside `reactivecqrs-core`, and your `application.conf` overrides whatever you
need. To get started you need no `application.conf` at all. Here is what the defaults say:

| Key | Default | What it does |
|---|---|---|
| `reactivecqrs.installation-number` | `0` | Which range of aggregate ids this installation hands out (the number × 10^12 onwards). Every machine of one installation uses the same number. |
| `reactivecqrs.blocking-dispatcher` | 32 threads | Where aggregate owners and the library's database work run. Keep it close to the size of your connection pool. Your projections will run here too ([chapter 8](08-projections.md)). |
| `reactivecqrs.handler-dispatcher` | 32 threads | Where your command handlers run, so a slow handler holds up commands only, never the event bus, projections or sagas. |

The installation number matters when several installations of your application might one day
share their data: each then makes ids no other installation makes, so an organization can move
between them ([chapter 16](16-organizations.md)). If you run a single installation, the default `0` is fine; chapter
17 tells the rest of the story.

The rest of `reference.conf` prepares the ground for a cluster (the serializer of messages between
machines and the timeouts of a hand-over). Leave it alone until [chapter 17](17-cluster.md).

A good first `src/main/resources/application.conf`:

```hocon
pekko {
  loglevel = "INFO"
  loggers = ["org.apache.pekko.event.slf4j.Slf4jLogger"]
  logging-filter = "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
}

reactivecqrs {
  installation-number = 0

  # Keep these near the size of your connection pool.
  blocking-dispatcher.thread-pool-executor.fixed-pool-size = 32
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 32
}
```

The `pekko` block sends the log lines written through Pekko (those of the library's actors among
them) to slf4j, where the rest of the library logs, so everything ends up in one place. And a
matching `src/main/resources/logback.xml`:

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

ScalikeJDBC logs every SQL statement at `DEBUG` level; a root level of `INFO` keeps them out of
your way. Turn it down to `DEBUG` on a day you want to watch the library at work.

## A home for your code

The test domain in this repository keeps one package per aggregate, with one file per kind of
thing. It is a good shape to copy:

```
src/main/scala/com/example/shop/
  Main.scala                            the wiring: pool, tables, actors (chapter 4)
  cart/
    Model.scala                         the state: ShoppingCart, Item
    Commands.scala                      CreateShoppingCart, AddItem, RemoveItem
    Events.scala                        ShoppingCartCreated, ItemAdded, ItemRemoved
    CommandsHandlers.scala              the rules, as plain functions (optional)
    EventsHandlers.scala                how each event changes the state (optional)
    ShoppingCartContext.scala           ties it all together (chapter 3)
    ShoppingCartsListProjection.scala   a read model (chapter 8)
  invoice/
    ...                                 the next aggregate, same shape
```

The two handler files are optional: small aggregates keep their handlers right inside the context,
as chapter 3 does. Compare with `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`,
which has this shape (its context is called `ShoppingCartAggregateContext`).

> **Tip:** in a larger application, give the domain a module of its own. Aggregates, commands,
> events and contexts only need `reactivecqrs-api`, so the domain module depends on that alone,
> and the application module (wiring, projections, sagas) adds `reactivecqrs-core`. The domain
> then cannot reach for the database by accident.

```scala
lazy val domain = project.settings(
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-api" % "1.0.0-SNAPSHOT")

lazy val app = project.dependsOn(domain).settings(
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-core" % "1.0.0-SNAPSHOT")
```

## Checking that it all works

Let's prove the workshop is ready before we build anything in it. This little program, E1's
`SetupCheck`, does steps 3 and 4 and nothing else:

<!-- snippet: e1/SetupCheck.scala#check -->
```scala
import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object SetupCheck {

  final case class Tables(eventStore: PostgresEventStoreState, responses: PostgresCommandResponseState,
                          sagaState: PostgresSagaState, subscriptions: PostgresSubscriptionsState)

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    try {
      initTables()
      println("The tables are ready.")
    } finally {
      ConnectionPool.closeAll()
    }
  }

  def initTables(): Tables = {
    val mpjsons = new MPJsons

    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    val sagaState = new PostgresSagaState(mpjsons, typesNames)
    sagaState.initSchema()   // this one returns nothing, so it takes a line of its own

    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    Tables(eventStore, responses, sagaState, subscriptions)
  }
}
```

Put it in your project's package and run it with `sbt "runMain com.example.shop.SetupCheck"` (in this
repository, `sbt "examples/runMain io.reactivecqrs.examples.e1.SetupCheck"`). It prints `The tables are
ready.`, and `\dt` in `psql` lists fifteen tables: `aggregate_lineage`, `aggregates`,
`commands_responses`, `event_keys`, `events`, `events_archive`, `events_to_publish`,
`history_rewrites`, `noop_events`, `sagas`, `schema_steps`, `snapshots`, `subscription_failures`,
`subscriptions` and `types_names`. All empty, all waiting. (`SetupCheckSpec` checks this list.)

Now run it a second time. Same message, no errors, nothing changed: that is the idempotence of
step 4 in action.

## What you learned

- You need a JDK 21 or newer, Scala 2.13, sbt and PostgreSQL; the library brings Pekko, ScalikeJDBC
  and the JDBC driver along.
- The library is `io.reactivecqrs %% reactivecqrs-api` (for your domain) and
  `io.reactivecqrs %% reactivecqrs-core` (the machinery), version `1.0.0-SNAPSHOT`.
- Your application must create ScalikeJDBC's default connection pool **before** any library code
  runs, with at least 32 connections.
- Each state class creates its tables with `initSchema()`; the class-name dictionary comes first,
  and all of them are safe to call at every start, even from several machines at once.
- One `MPJsons` instance, made with `new MPJsons`, serves the whole application.
- The defaults in `reference.conf` (installation number, the blocking and handler dispatchers) work
  out of the box; `application.conf` overrides them.

Next: [chapter 3](03-first-aggregate.md) puts the first aggregate on the workbench.
