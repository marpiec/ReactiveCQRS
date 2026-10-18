# Appendix E. Configuration Reference

Everything you can set, in one place. ReactiveCQRS has two kinds of settings:

- a few keys of **`application.conf`**, the Typesafe Config file Pekko reads at start;
- **parameters in code**: the constructor parameters of the stores and actors you create, the option
  classes you pass them, and the members of your aggregate contexts.

Each table gives the default, what the setting does, and the chapter that explains it. The defaults
were read from the code of ReactiveCQRS 1.0 (`1.0.0`) on 18/10/2026. The ones you are most
likely to change are also in [Appendix B](appendix-b-cheat-sheet.md), "Defaults worth knowing".

Sizes: 64 MB is 64 × 1 024 × 1 024 bytes; times are written as Scala's `FiniteDuration`s.

---

## `application.conf`

| Key | Default | What it does | Chapter |
|---|---|---|---|
| `reactivecqrs.installation-number` | `0` | The installation's range of aggregate ids: installation *n* makes them from *n* × 10^12 on. Every node of an installation sets the same number. | [5](05-command-flavors.md) |
| `reactivecqrs.blocking-dispatcher.thread-pool-executor.fixed-pool-size` | `32` | Threads for aggregate owners and the library's database work; projections run here too (`BlockingDispatcher.id`). Keep it near the size of the JDBC pool. | [2](02-setting-up.md), [8](08-projections.md) |
| `reactivecqrs.handler-dispatcher.thread-pool-executor.fixed-pool-size` | `32` | Threads for your command handlers. | [5](05-command-flavors.md) |
| `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }` | set by the library's `reference.conf` | Messages between nodes go through the library's serializer. Set it in your own `application.conf` too: depending on the order of the classpath, Pekko's binding may win otherwise, and the node stays not ready (F178). | [17](17-cluster.md) |
| `pekko.cluster.app-version` | Pekko's `0.0.0` | The release a node runs. Give every release its own, for rolling upgrades and `writtenFrom`. | [17](17-cluster.md) |
| `pekko.cluster.sharding.handoff-timeout` | `90 s` (set by the library) | How long a hand-over of sharded owners may take; above the default command timeout. | [17](17-cluster.md) |
| `pekko.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout` | `90 s` (set by the library) | How long a leaving node may take to hand its shards over. | [17](17-cluster.md) |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | Pekko's `120 s` | When an idle sharded owner is stopped. | [17](17-cluster.md) |
| `pekko.remote.artery.advanced.maximum-frame-size` | Pekko's `256 KiB` | The largest message between nodes; the library refuses one larger than this less 8 KiB. | [17](17-cluster.md) |

The cluster's own settings (the provider, the split brain resolver and its lease, Cluster Bootstrap)
are Pekko's, and [chapter 17](17-cluster.md) shows them with E2.

## The stores

| Class and parameter | Default | What it does | Chapter |
|---|---|---|---|
| `PostgresEventStoreState(mpjsons, typesNames, ...)` `fetchSize` | `1000` | Rows read at a time from the events. Lower it for large events. | [18](18-production.md) |
| `eventSizeLimit` | `100000` | The largest event, in characters of JSON; a larger one fails its command with `EventHandlingError`. | [6](06-events.md) |
| `eventSizeLimitPerClassName` | none | A limit per event class, in place of `eventSizeLimit`. | [6](06-events.md) |
| `aggregateVersionLimit` | `10000` | The most events one aggregate may hold; a squash does not lower a version. | [6](06-events.md), [13](13-snapshots.md) |
| `outboxDeleteDelay` | `20 ms` | How long delivered outbox rows wait, at most, to be deleted together with others. | — |
| `PostgresCommandResponseState(mpjsons, typesNames, retention)` | `30 days` | How long the answers of idempotent commands are kept. | [5](05-command-flavors.md) |
| `PostgresSubscriptionsState(typesNames, eventsLogger, maxCachedAggregates)` | no logger, `10000` | The projections' notebook: a debug logger of its writes, and how many aggregates' versions it keeps in memory. | [8](08-projections.md) |
| `PostgresDocumentStore[T](name, mpjsons, cache, indexes)` | no indexes | A read model's table, `projection_<name>`, its cache (`NoopDocumentStoreCache` keeps nothing) and its JSON indexes. | [8](08-projections.md) |
| `PostgresSagaState(mpjsons, typesNames)` | — | The sagas' progress. | [12](12-sagas.md) |
| `new PostgresProjectionRegistry()` | — | The managed projections' builds. | [10](10-rebuilding-projections.md) |

## Command buses

| Parameter | Default | What it does | Chapter |
|---|---|---|---|
| `AggregateCommandBusActor(context, uidGenerator, eventStore, responses, eventBus, ...)` `maxInactivityMillis` | `43200000` (12 hours) | An owner idle this long is stopped. | [4](04-first-command.md) |
| `keepAliveLimit` | `200` | Beyond this many owners in memory, those idle for over 2 minutes are stopped, the least recently used first. | [4](04-first-command.md) |
| `failedRestoreRetryAfter` | `5 s` | How long an aggregate that could not be read answers with that failure before the next request tries again. | [4](04-first-command.md) |
| `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons, ...)` `maxAggregates` | `100000` | The most drafts in memory; one more is answered `CommandRejected`. | [20](20-ephemeral-aggregates.md) |
| `idleTime` | `12 hours` | A draft unused this long is dropped. | [20](20-ephemeral-aggregates.md) |
| `sweepEvery` | `5 minutes` | How often idle drafts are looked for. | [20](20-ephemeral-aggregates.md) |
| `eventSizeLimit`, `eventSizeLimitPerClassName`, `aggregateVersionLimit` | as the event store's | The drafts' limits, as the store's (above). | [20](20-ephemeral-aggregates.md) |

## Aggregate contexts

Members of your `AggregateContext`; override the ones you need.

| Member | Default | What it does | Chapter |
|---|---|---|---|
| `version` | (you give it) | The version of the context's behaviour; raise it when event handlers change what they make. | [3](03-first-aggregate.md) |
| `commandTimeout` | `60 s` | How long a command may take before the caller gets `CommandHandlingError`. | [5](05-command-flavors.md) |
| `commandProcessing` | `Concurrent` | `Serial(maxQueuePerAggregate = 1000, onOverflow = OverflowPolicy.Reject)` runs one command at a time per aggregate. | [5](05-command-flavors.md) |
| `snapshots` | `None` | `SnapshotPolicy(every = 200, onPassivation = true, schemaVersion = 1)`. | [13](13-snapshots.md) |
| `squash` | `None` | `SquashPolicy(toEvent, automatic = None, keep = empty)`, with `AutomaticSquash(afterEvents, keepLast = 0, keepNewerThan = None)`. | [14](14-squash.md) |
| `placement` | `Placement.Local` | `Placement.Sharded(numberOfShards = 100)`: one owner per aggregate in the cluster. | [17](17-cluster.md) |
| `writtenFrom` | empty | New event classes, each with the app-version from which every node reads it. | [17](17-cluster.md) |
| `rewriteHistoryCommandHandlers` | refuses every rewrite | The handlers of history rewrites. | [15](15-rewriting-history.md) |
| `historyChanges`, `metadataChanges` | none | What a complete rewrite's change does to the events and their metadata. | [15](15-rewriting-history.md) |
| `WithOutboxWriter` (a mix-in) | none | Rows of your own, written in the transaction of the events. | [5](05-command-flavors.md) |

## Delivery

| Class and parameter | Default | What it does | Chapter |
|---|---|---|---|
| `EventsBusActor(options, eventsLogger)` | `DeliveryOptions()`, no logger | The bus's timing and flow control; a debug logger of its deliveries. | [9](09-projection-failures.md) |
| `DeliveryOptions` `deadline` | `10 minutes` | A delivery neither acknowledged nor failed by then ends as undelivered; its outbox row stays. | [9](09-projection-failures.md) |
| `stallAfter`, `summaryInterval` | `1 minute`, `5 minutes` | When a waiting delivery is logged as stalled, and how often stalls are summed up. | [9](09-projection-failures.md) |
| `registrationTimeout` | `2 minutes` | How long the bus waits at start for a registered subscriber's answer; then it starts without it and keeps every event in the outbox until it answers. | [17](17-cluster.md) |
| `tickInterval`, `stallLinesPerTick` | `5 s`, `10` | How often the bus looks at its deliveries, and how many stall lines it logs per look. | — |
| `window`, `windowBytes` | `10000` events, `64 MB` | The most a subscriber is sent and has not answered; the rest is held, and pulled from the owners later. | [SCALING.md](../SCALING.md), section 5 |
| `maxPulls`, `pullTimeout` | `100`, `1 minute` | How many held aggregates are pulled at once, and when an unanswered pull is asked again. | — |
| `pullPartEvents`, `pullPartBytes` | `1000`, `16 MB` | The size of the parts an owner sends a pull in. | — |
| `OutboxPollerActor(eventStore, eventBus, options, clock)` | `OutboxPollerOptions()`, the system clock | The poller's timing; a `TestClock` in tests. | [9](09-projection-failures.md), [19](19-testing.md) |
| `OutboxPollerOptions` `firstPollAfter`, `pollInterval` | `10 s`, `30 s` | The first poll, and the polls after it. | [4](04-first-command.md) |
| `OutboxPollerOptions` `readTimeout` | `5 minutes` | A read of the outbox that runs longer is given up, logged and done again (a connection that no longer answers). | [18](18-production.md) |
| `maxTracked` | `100000` | The most waiting aggregates a poll reads. | — |
| `RedeliveryPolicy.Options` `attemptsAfter` | `3 minutes`, `6 minutes` | When a waiting event is published again. | [9](09-projection-failures.md) |
| `giveUpAfter`, `retryGivenUpEvery` | `10 minutes`, `30 minutes` | When it is reported undelivered, and how often it is tried after that. | [9](09-projection-failures.md) |
| `maxRepublishPerPoll` | `1000` | The most aggregates published again per poll. | [9](09-projection-failures.md) |

## Projections

| Setting | Default | What it does | Chapter |
|---|---|---|---|
| `ProjectionActorOptions.DEFAULT` `groupUpdatesDelayMillis` | `0` | How long a cart's deliveries wait, so that several are applied together. | [8](08-projections.md) |
| `minimumDelayVersion` | `1` | Deliveries that start below this version never wait. | [8](08-projections.md) |
| `eventsToProcessImmediately` | none | Event classes that never wait. | [8](08-projections.md) |
| `parallelUpdateProcessing` | `0` | Above 1, that many threads apply updates. | [8](08-projections.md) |
| `eventsLogger` | none | A debug logger of deliveries and batches. | [8](08-projections.md) |
| `retries` | `Backoff(1 s, 30 s, maxAttempts = 9)`, about 2 minutes | How a failing listener is retried before its failure is recorded. | [9](09-projection-failures.md) |
| `retryFailuresOnStart`, `failureRetryInterval` | `true`, `50 ms` | Recorded failures are retried at start, one every 50 ms. | [9](09-projection-failures.md) |
| `gapRequestRepeat`, `gapGiveUpAfter` | `10 s`, `10 minutes` | How often missing versions are asked for, and when the gap becomes a recorded failure. | [9](09-projection-failures.md) |
| `maxBatchesPerTransaction` | `100` | The most aggregates whose updates share one transaction. | [8](08-projections.md) |
| `purgedDropWindow` | `30 minutes` | After a purge, how long deliveries of the purged aggregates are acknowledged without being applied. | [16](16-organizations.md) |
| `SubscribableProjectionActor(updatesCacheTTL, subscriptionTTL, options)` | `Duration.ZERO`, `600000` ms, `DEFAULT` | How long sent updates are kept for new subscribers; how long a subscription lives without renewal. | [11](11-subscribable-projections.md) |
| `ProjectionsSettings(eventStore, subscriptionsState, registry, contexts, builder, buildsAtOnce, restartAfter)` | `BuilderSettings()`, `2`, `10 s` | Managed projections: how builds read the store, how many run at once, and how long the supervisor waits after a failure. | [10](10-rebuilding-projections.md) |
| `BuilderSettings` `pageSize`, `pageBytes` | `1000` events, `64 MB` | How much of the store a build reads at a time. | [10](10-rebuilding-projections.md), [18](18-production.md) |
| `chunkEvents`, `chunkBytes`, `chunksInFlight` | `1000`, `16 MB`, `2` | The parts a build hands its projection, and how many are on their way at once. | [10](10-rebuilding-projections.md) |
| `maxReplayStates`, `retryAfter` | `10000`, `5 s` | The most aggregate states a build keeps open while it replays the store in commit order; the wait after the store or the projection failed. | — |
| `Projections.start(name, version, actorName, whileBuilding)` | `ServeLive` | Whether the active build keeps taking deliveries while a new one is built (`ServeStale`: it does not). | [10](10-rebuilding-projections.md) |

## Sagas, squash, organizations and rewrites

| Setting | Default | What it does | Chapter |
|---|---|---|---|
| `SagaRunner.start(system, definition, uidGenerator, state, actorName)` | the definition's `name` | The name of the runner's singleton. | [12](12-sagas.md) |
| `SagaDefinition.persistRetries` | `Backoff(100 ms, 30 s, maxAttempts = 10)` | How a failed write of a saga's progress is retried. | [12](12-sagas.md) |
| `SagaDefinition.spaceOf(order)` | `None` | The organization a saga belongs to, for purges. | [12](12-sagas.md) |
| `SquashSchedulerOptions` `firstRunAfter`, `interval` | `1 minute`, `1 hour` | When the scheduler's runs come. | [14](14-squash.md) |
| `SquashSchedulerOptions` `readTimeout` | `5 minutes` | A read of the aggregates to squash that runs longer is given up, logged and done again. | [18](18-production.md) |
| `perRun`, `answerTimeout` | `100`, `1 minute` | The most aggregates per type in one run, and how long it waits for each answer. | [14](14-squash.md) |
| `SpacePurge(eventStore, commandBuses, projections, hooks, options)` | no hooks, `SpacePurgeOptions()` | A purge of an organization, with your own tables' hooks. | [16](16-organizations.md) |
| `SpacePurgeOptions` `aggregatesPerBatch`, `eventsPerBatch` | `1000`, `100000` | The bites a purge takes, one transaction each. | [16](16-organizations.md) |
| `maxPasses`, `answerTimeout` | `3`, `2 minutes` | How many passes a purge makes, and how long it waits for each command bus and projection. | [16](16-organizations.md) |
| `SpaceExport(eventStore, contexts, installation)` | `0` | The installation an export comes from. | [16](16-organizations.md) |
| `SpaceImport(eventStore, uidGenerator, rewriters, newMPJsons, eventsPerTransaction)` | none, a new `MPJsons`, `10000` | Per aggregate type, an `ImportRewriter` that maps ids held as plain numbers or text (typed ids are mapped by the import itself); how many events the import writes per transaction. | [16](16-organizations.md) |
| `HistoryRewriter(commandBus, eventStore, projections, answerTimeout)` | none, `5 minutes` | The managed projections to rebuild after a complete rewrite, and how long it waits for each answer. | [15](15-rewriting-history.md) |

## Maintenance calls

| Call | Default | What it does | Chapter |
|---|---|---|---|
| `StoredDataCheck.run(mpjsons, typesNames, projections, examplesPerClass, fetchSize)` | none, `5`, `1000` | Reads every stored value; lists up to 5 unreadable rows per class. | [6](06-events.md), [18](18-production.md) |
| `StoredDataCheck.usersOf(className, limit)` | `100` | The aggregates that still have events of a class. | [6](06-events.md), [14](14-squash.md) |
| `eventStore.backfillSpaces(spaceOf, batchSize)` | none, `1000` | Gives aggregates stored before spaces their space. | [16](16-organizations.md) |
| `eventStore.backfillEventKeys(batchSize)` | `10000` | Gives events stored before event keys their key. | [16](16-organizations.md) |

## System properties

| Property | Default | What it does | Chapter |
|---|---|---|---|
| `reactivecqrs.log-values` | `false` | For diagnosis only: the library's statements are logged with their values, and failures in full on `io.reactivecqrs.detail`. Those lines carry personal data. | [18](18-production.md) |

## The test kit

Set as system properties, for example `sbt -Dreactivecqrs.test.clusterOfOne=true test`.

| Property | Default | What it does | Chapter |
|---|---|---|---|
| `reactivecqrs.test.url`, `.user`, `.password` | `jdbc:postgresql://localhost:5432/reactivecqrs`, `reactivecqrs`, `reactivecqrs` | The database `TestDatabase` connects to. | [19](19-testing.md) |
| `reactivecqrs.test.schema` | a new `test_run_<seconds>_<n>` per test run | The schema of the run; schemas of runs older than six hours are dropped. | [19](19-testing.md) |
| `reactivecqrs.test.clusterOfOne` | `false` | `TestSystems.newSystem()` makes a cluster of one. | [19](19-testing.md) |
| `reactivecqrs.schema.record` | `false` | `SchemaReference` records the golden files instead of checking them. | [19](19-testing.md) |

---

Back to the [table of contents](README.md).
