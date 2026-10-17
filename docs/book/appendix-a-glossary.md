# Appendix A. Glossary

Every special word this book uses, in alphabetical order, with a one- or two-sentence meaning and
the chapter that explains it properly. Names in `code font` are what you will meet in the code:
classes, messages, settings and tables. When a word sends you to another entry ("see *Space*"),
that entry has the full story.

---

## A

- **Ack** (`MessageAck`): a subscriber's "got it" for a delivery of events. Once every
  subscription has acknowledged an event, or reported `MessageFailed` for it, the event leaves the
  outbox. ([chapter 9](09-projection-failures.md))
- **Actor**: a small worker with a mailbox that handles one message at a time. Everything in
  ReactiveCQRS that runs is an Apache Pekko actor. ([chapter 1](01-cqrs-without-the-headache.md))
- **Aggregate**: one thing with its own diary of events, such as one shopping cart. It has an id, a
  version and a state, and its rules are checked one aggregate at a time.
  ([chapter 1](01-cqrs-without-the-headache.md), [chapter 3](03-first-aggregate.md))
- **Aggregate context** (`AggregateContext`): the class you write for one aggregate type: its
  initial state (`initialAggregateRoot`), command handlers, event handlers, its `version`, and the
  optional features (snapshots, squash, serial mode, placement). ([chapter 3](03-first-aggregate.md))
- **Aggregate id** (`AggregateId`): the number (a `Long`) that names one aggregate. The command bus
  gives a new aggregate its id, unless the command chose one. ([chapter 4](04-first-command.md))
- **Aggregate owner** (`AggregateOwner`): the actor that looks after one aggregate. It restores the
  state, runs commands through your handlers, stores the events and publishes them; it starts with
  the first message and is stopped when idle. ([chapter 1](01-cqrs-without-the-headache.md),
  [chapter 4](04-first-command.md))
- **Aggregate root**: the immutable case class that holds an aggregate's state, such as
  `ShoppingCart(name, items)`. An event handler that returns `null` marks the aggregate deleted.
  ([chapter 3](03-first-aggregate.md))
- **Aggregate type** (`AggregateType`): the full class name of the aggregate root. It keys routing,
  subscriptions and stored rows, so renaming or moving the root class makes a new type.
  ([chapter 3](03-first-aggregate.md))
- **App-version**: the release number each node of a cluster announces in
  `pekko.cluster.app-version`. During a rolling upgrade it decides where new shards go and when new
  event classes may be written. ([chapter 17](17-cluster.md))
- **Archive** (`events_archive`): where a squash keeps the events of the audit classes named in
  `SquashPolicy.keep`, instead of deleting them. Only `GetArchivedEvents` reads it.
  ([chapter 14](14-squash.md))
- **Ask**: sending a message with `?` and getting back a `Future` of the answer; it needs an
  implicit `Timeout`. See also *Tell*. ([chapter 1](01-cqrs-without-the-headache.md))
- **Async command result** (`AsyncCommandResult`): a command handler's result that arrives later,
  wrapped around a `Future`. Inside an aggregate context a `Future` of a result is turned into one
  for you. ([chapter 3](03-first-aggregate.md), [chapter 5](05-command-flavors.md))
- **At least once**: the delivery promise. Every event reaches every subscriber at least once,
  sometimes more; projections drop the repeats by version, so each event is applied once.
  ([chapter 1](01-cqrs-without-the-headache.md), [chapter 9](09-projection-failures.md))
- **Automatic squash** (`AutomaticSquash`): the rule in `SquashPolicy.automatic` that lets the
  squash scheduler choose aggregates to squash: after `afterEvents` events, keeping the newest
  `keepLast` and those younger than `keepNewerThan`. ([chapter 14](14-squash.md))

## B

- **Backfill**: an online job that fills in data for rows written before a feature existed.
  `backfillSpaces` gives old aggregates their space; `backfillEventKeys` gives old events their
  keys. ([chapter 16](16-organizations.md), [chapter 18](18-production.md))
- **Base aggregate**: the aggregate a duplicate was copied from. See *Duplication*.
  ([chapter 6](06-events.md))
- **Blocking dispatcher** (`reactivecqrs.blocking-dispatcher`): the pool of 32 threads where
  aggregate owners and the library's database work run. Create your projections on it with
  `Props(...).withDispatcher(BlockingDispatcher.id)`. ([chapter 8](08-projections.md),
  [chapter 18](18-production.md))
- **Build**: one version of a managed projection, with its own tables and its own progress. A new
  build is filled from the event store while the active build keeps serving, then replaces it in
  one switch. ([chapter 10](10-rebuilding-projections.md))

## C

- **Chosen aggregate id** (`chosenAggregateId`): an id you reserve first with `ReserveAggregateId`
  and put in a `FirstCommand`, so sending the creation twice can never make two aggregates.
  ([chapter 5](05-command-flavors.md))
- **Cluster**: several nodes (JVMs) running one installation together, on one database, through
  Pekko Cluster. ([chapter 17](17-cluster.md))
- **Cluster serializer** (`ClusterSerializer`): the one serializer of every message between nodes,
  built on mpjsons; Java serialization stays off. ([chapter 17](17-cluster.md))
- **Command**: a request to change one aggregate, written as a case class. Unlike an event, it can
  be refused. `Command` is also the name of the kind that carries an expected version.
  ([chapter 1](01-cqrs-without-the-headache.md), [chapter 5](05-command-flavors.md))
- **Command bus** (`AggregateCommandBusActor`): the front door of one aggregate type. It takes
  commands and queries, hands out ids, routes each message to the aggregate's owner, and answers
  historical reads itself. ([chapter 1](01-cqrs-without-the-headache.md),
  [chapter 4](04-first-command.md))
- **Command handler**: your function that gets the current state and a command, and returns
  `CommandSuccess` (with events), `CommandFailure` (a refusal) or an `AsyncCommandResult`.
  ([chapter 3](03-first-aggregate.md))
- **Command response**: what the caller gets back for a command: `SuccessResponse`,
  `CustomSuccessResponse`, `FailureResponse`, `AggregateConcurrentModificationError`,
  `CommandHandlingError`, `EventHandlingError` or `CommandRejected`. Appendix B lists them all.
  ([chapter 4](04-first-command.md), [chapter 5](05-command-flavors.md))
- **Command timeout** (`commandTimeout`): how long a command may take before its caller gets
  `CommandHandlingError`, 60 seconds by default. A result that comes later is never stored.
  ([chapter 5](05-command-flavors.md))
- **Compensating event**: a new event you record next to a history rewrite, such as
  `CartNameRewritten`, so that projections hear about the change. ([chapter 15](15-rewriting-history.md))
- **Concurrent command** (`ConcurrentCommand`): a command without an expected version. It runs on
  whatever version the aggregate has, and after a clash the library runs it again, at most 10
  attempts in all. ([chapter 5](05-command-flavors.md))
- **Connection pool**: the ScalikeJDBC pool of database connections. Your application sets it up
  before any library code runs. ([chapter 2](02-setting-up.md))
- **CQRS** (Command Query Responsibility Segregation): one model for making decisions (the write
  side) and other models, shaped for questions, for reading (the read side).
  ([chapter 1](01-cqrs-without-the-headache.md))

## D

- **Delayed query**: a projection query that waits, up to a deadline, for the data it looks for;
  you write one with `delayIfNotAvailable`. It answers `None` when time runs out.
  ([chapter 8](08-projections.md))
- **Delivery**: the event bus handing one aggregate's events to one subscriber. It is tracked until
  the subscriber answers, the subscriber stops, or 10 minutes pass. ([chapter 8](08-projections.md),
  [chapter 9](09-projection-failures.md))
- **Detach**: what a squash or a permanent delete does to a duplicate that still reads the history
  being removed. The duplicate's first event becomes a squash event of its own state, so it no
  longer needs its base. ([chapter 14](14-squash.md))
- **Document store** (`DocumentStore`, `PostgresDocumentStore`): a simple store of JSON documents by
  id, in a `projection_<name>` table, where projections keep their read models.
  ([chapter 8](08-projections.md))
- **Duplication** (`DuplicationEvent`): making a new aggregate as a copy of another one at a given
  version. The copy shares its base's history up to that version and goes its own way after it.
  ([chapter 6](06-events.md))

## E

- **Event** (`Event`): a fact about one aggregate, in the past tense, such as `ItemAdded("milk")`.
  Events are kept forever, as JSON stored under their class name.
  ([chapter 1](01-cqrs-without-the-headache.md), [chapter 6](06-events.md))
- **Event bus** (`EventsBusActor`): the actor, one per node, that hands published events to every
  subscriber of their aggregate type and waits for each to answer.
  ([chapter 1](01-cqrs-without-the-headache.md), [chapter 4](04-first-command.md))
- **Event handler**: your pure function that applies one event to the state and returns the new
  state. It runs live, on restore and during rebuilds, so it must give the same result every time.
  ([chapter 3](03-first-aggregate.md), [chapter 6](06-events.md))
- **Event info** (`EventInfo`): an event as listeners and queries see it: its version, the event,
  the user, the time and the metadata envelope. ([chapter 8](08-projections.md))
- **Event keys** (`event_keys`): a table that records each event's aggregate type and space, so a
  rebuild of a few types, or of one organization, reads only its own events.
  ([chapter 16](16-organizations.md), [chapter 18](18-production.md))
- **Event sourcing**: keeping the history of events as the source of truth, and getting the current
  state by replaying them. ([chapter 1](01-cqrs-without-the-headache.md))
- **Event store** (`PostgresEventStoreState`): the part of the library that keeps events and
  aggregates in PostgreSQL, together with the outbox. ([chapter 2](02-setting-up.md))
- **Eventually consistent**: the read side catches up a moment after a command succeeds, usually
  within milliseconds. ([chapter 1](01-cqrs-without-the-headache.md))
- **Expected version** (`expectedVersion`): the version a `Command` says it is changing. When the
  aggregate has moved on, the command is answered `AggregateConcurrentModificationError`.
  ([chapter 5](05-command-flavors.md))

## F

- **Failed subscription** (`SubscriptionFailure`): a projection's record that it gave up on one
  aggregate, after about two minutes of retries. Deliveries of that aggregate are answered as failed
  until you retry or skip it. ([chapter 9](09-projection-failures.md))
- **First command** (`FirstCommand`): a command that creates a new aggregate.
  ([chapter 3](03-first-aggregate.md), [chapter 5](05-command-flavors.md))
- **First event** (`FirstEvent`): the event that starts an aggregate's history. It names the
  aggregate's space (`SpaceId(0)` for the platform). ([chapter 3](03-first-aggregate.md),
  [chapter 6](06-events.md))
- **Flow control**: the event bus never sends a subscriber more than a window of unanswered events
  (10 000 events and 64 MB by default). The rest is held in the store and pulled for the subscriber
  as it catches up. ([chapter 9](09-projection-failures.md))

## G

- **Gap**: versions a projection is missing for an aggregate (it has version 5 and receives
  version 7). It asks the owner to publish them again, and records a failure if they do not come
  within 10 minutes. ([chapter 9](09-projection-failures.md))

## H

- **Handler dispatcher** (`reactivecqrs.handler-dispatcher`): the pool of 32 threads where command
  handlers run, so slow handlers slow down only commands. ([chapter 5](05-command-flavors.md),
  [chapter 18](18-production.md))
- **Hand-over**: in a cluster, a sharded owner moving from one node to another (a node leaves, or
  shards are rebalanced). The owner finishes the commands it started before it moves.
  ([chapter 17](17-cluster.md))
- **Historical read**: asking the command bus for an aggregate at a past version or instant, or for
  its events (`GetAggregateForVersion`, `GetAggregateAtInstant`, `GetEventsForAggregate`,
  `GetEventsForAggregateForVersion`). It is read from the store. ([chapter 7](07-reading-the-past.md))
- **History rewrite** (`RewriteHistoryCommand`, `RewriteHistoryConcurrentCommand`): a command that
  changes values inside past events, each keeping its class, for example to remove personal data.
  Its handler returns `RewriteCommandSuccess`. ([chapter 15](15-rewriting-history.md))
- **Host**: your application, which creates the library's stores and actors and wires them
  together. ([chapter 4](04-first-command.md))

## I

- **Idempotency key** (`CommandIdempotencyId`): the key an idempotent command carries; its
  `asDbKey` must be unique across all aggregate types. `SagaStep` is one, and you can write your
  own. Stored responses are kept for 30 days by default. ([chapter 5](05-command-flavors.md))
- **Idempotent command** (`IdempotentCommand`): a command that takes effect once per idempotency
  key, however often it is sent. Every copy gets the response of the copy that won.
  ([chapter 5](05-command-flavors.md))
- **`initSchema()`**: the method that creates the library's tables, or adds what a new version
  needs. It is safe to call at every start. ([chapter 2](02-setting-up.md))
- **In-memory doubles** (`MemoryEventStoreState`, `MemorySubscriptionsState`, `MemoryDocumentStore`
  and others): stand-ins for the PostgreSQL stores, for fast tests. They keep objects instead of
  JSON and roll nothing back, so they prove less than a test on PostgreSQL. ([chapter 19](19-testing.md))
- **Installation number** (`reactivecqrs.installation-number`): gives one installation its own
  range of aggregate ids (the number × 10^12 onwards), so ids stay unique when the data of
  installations meet. 0, the default, is the range of every id made before 1.0.
  ([chapter 2](02-setting-up.md), [chapter 18](18-production.md))

## L

- **Listener** (`EventsListener`, `AggregateListener`, `AggregateWithEventsListener`): a
  projection's callback for one aggregate type. The three kinds receive the new events, the latest
  state, or both; each has a `noDBSession` variant for work outside the database.
  ([chapter 8](08-projections.md))
- **Listener actor** (`AggregateListenerActor`): a projection without queries, for side effects
  such as notifications. It gets the same deliveries and guarantees. ([chapter 8](08-projections.md))
- **Local type** (`Placement.Local`): the default placement. Every node keeps owners of its own,
  and the database's version check decides when two nodes write the same aggregate.
  ([chapter 17](17-cluster.md))

## M

- **Managed projection**: a projection started with `Projections.start`, so that a supervisor runs
  it and can rebuild it from the event store without downtime. Projections with side effects stay
  unmanaged. ([chapter 10](10-rebuilding-projections.md))
- **`MessageTooLargeException`**: in a cluster, what an asker gets instead of an answer that is too
  large to travel between nodes (about 248 KiB with Pekko's default frame size).
  ([chapter 17](17-cluster.md))
- **Metadata envelope** (`EventMetadata`): who acted and why: the actor, correlation and causation
  ids, the trace context, the space and entries of your own. It is stored with every event of a
  command that mixes in `WithMetadata`; listeners see it, event handlers do not.
  ([chapter 6](06-events.md))
- **Metrics** (`Metrics`, `ReactiveMetrics`): the library's measures (command times, events stored,
  deliveries waiting, and more), sent to your metrics system once you call
  `ReactiveMetrics(system).use(...)`. ([chapter 18](18-production.md))
- **Min-version query** (`GetAggregateMinVersion`): asks for an aggregate as soon as it reaches a
  version, so a screen can show the change it just made. ([chapter 7](07-reading-the-past.md))
- **mpjsons**: the JSON library that stores events, states and documents, and carries messages
  between nodes. ([chapter 2](02-setting-up.md))

## O

- **Optimistic locking**: nobody locks anything up front; a write that expected an older version is
  refused when it is stored. ([chapter 1](01-cqrs-without-the-headache.md),
  [chapter 5](05-command-flavors.md))
- **Organization**: see *Space*. ([chapter 16](16-organizations.md))
- **Outbox** (`events_to_publish`): a table of events that some subscriber has not confirmed yet.
  It is written in the same transaction as the events, so nothing is lost when a machine stops
  halfway. ([chapter 1](01-cqrs-without-the-headache.md), [chapter 9](09-projection-failures.md))
- **Outbox poller** (`OutboxPollerActor`): one per system. Every 30 seconds it looks at the outbox
  and has the owners publish waiting events again: 3 and 6 minutes after the first try; at 10
  minutes it reports them undelivered, then tries every 30 minutes. ([chapter 4](04-first-command.md),
  [chapter 9](09-projection-failures.md))
- **Outbox writer** (`WithOutboxWriter`, `OutboxWriter`): lets an aggregate context write rows of
  its own (jobs, messages to send) in the same transaction as its events.
  ([chapter 18](18-production.md))

## P

- **Passivation**: stopping an idle aggregate owner to free memory; the next message starts a new
  owner, which restores the state. You can ask for it with `PassivateAggregate`.
  ([chapter 13](13-snapshots.md), [chapter 18](18-production.md))
- **Permanent delete** (`PermanentDeleteEvent`): an event that, once every subscriber has it,
  removes the aggregate's events and rows from the database for good. ([chapter 6](06-events.md))
- **Placement** (`Placement`): where the owners of an aggregate type run: `Placement.Local` (the
  default) or `Placement.Sharded`. ([chapter 17](17-cluster.md))
- **Projection** (`ProjectionActor`): an actor that listens to events, keeps a read model up to
  date, and answers queries about it. ([chapter 1](01-cqrs-without-the-headache.md),
  [chapter 8](08-projections.md))
- **Pull**: under flow control, the event bus asking owners to publish held events again to a
  subscriber, once half of its window is free. ([chapter 9](09-projection-failures.md))
- **Push subscription**: a client's subscription to a subscribable projection's updates; it
  expires after 10 minutes without `RenewSubscription`. ([chapter 11](11-subscribable-projections.md))

## R

- **Read model**: data shaped for one kind of question, kept by a projection: a list of carts per
  customer, a count of items per product. ([chapter 1](01-cqrs-without-the-headache.md),
  [chapter 8](08-projections.md))
- **Read side**: the projections and their read models. See *CQRS*.
  ([chapter 1](01-cqrs-without-the-headache.md))
- **Readiness** (`Readiness`, `ReadinessCheck`): a node is ready for traffic once every event bus
  on it has heard from all its subscribers. `ReadinessCheck` plugs this into Pekko Management.
  ([chapter 17](17-cluster.md))
- **Rebuild**: filling a projection again from the event store, either whole (a new build) or in
  place for some aggregates (a targeted rebuild). ([chapter 10](10-rebuilding-projections.md))
- **`RecordingMetrics`**: a `Metrics` implementation that keeps every measure in memory, so a test can
  check what the library measured. ([chapter 19](19-testing.md))
- **Registry** (`ProjectionRegistry`, `PostgresProjectionRegistry`): the tables where managed
  projections record their active build and their build in progress.
  ([chapter 10](10-rebuilding-projections.md))
- **Re-publication**: an owner sending events again: those still waiting in its outbox, or a range
  a projection asked for to fill a gap. ([chapter 9](09-projection-failures.md))
- **Restore**: how an owner gets its aggregate's state when it starts: by replaying the events, or
  from a snapshot and the events after it. ([chapter 6](06-events.md), [chapter 13](13-snapshots.md))
- **Rolling upgrade**: upgrading a cluster one node at a time, so old and new versions run side by
  side for a while, without downtime. ([chapter 17](17-cluster.md))

## S

- **Saga** (`SagaActor`): a process manager for work that spans several aggregates. It runs steps,
  stores its progress after each one, and runs revert steps when a step fails.
  ([chapter 12](12-sagas.md))
- **Saga step** (`SagaStep`): a saga's id and step number. It is the idempotency key of the
  commands a step sends, so a saga resumed after a crash never applies a command twice.
  ([chapter 12](12-sagas.md))
- **Schema lock** (`SchemaLock`): a PostgreSQL advisory lock that makes processes initialize the
  schema one at a time. A process waits up to five minutes for it. ([chapter 2](02-setting-up.md))
- **Serial mode** (`CommandProcessing.Serial`): runs one aggregate's commands one at a time, in the
  order they arrived, so every handler sees the latest state. When 1 000 commands already wait, the
  next is answered `CommandRejected`. ([chapter 5](05-command-flavors.md))
- **ServeLive / ServeStale**: what the active build of a managed projection does while a new build
  is made: keep applying live events (`ServeLive`, the default) or serve its tables unchanged
  (`ServeStale`). ([chapter 10](10-rebuilding-projections.md))
- **Sharded type** (`Placement.Sharded`): one owner per aggregate in the whole cluster, placed by
  Pekko Cluster Sharding; commands from any node reach it. ([chapter 17](17-cluster.md))
- **Singleton** (`Singletons.start`): an actor that runs once in the whole cluster, such as a
  projection's supervisor, the outbox poller, the squash scheduler or a saga runner. Every node
  talks to it through a proxy. ([chapter 17](17-cluster.md))
- **Snapshot** (`SnapshotPolicy`): a saved copy of an aggregate's state, by default written every
  200 events and when the owner stops, so a restore reads the snapshot and only the events after
  it. Opt-in, and safe to throw away. ([chapter 13](13-snapshots.md))
- **Space** (`SpaceId`): the organization (tenant) an aggregate belongs to, named by its first
  event. Space 0 is the platform itself. ([chapter 16](16-organizations.md))
- **Squash** (`SquashAggregate`): replacing an aggregate's events 1 to N with one squash event at
  version N; the version and the state stay as they were. Opt-in per aggregate context with
  `SquashPolicy`. ([chapter 14](14-squash.md))
- **Squash event**: the `FirstEvent` that `SquashPolicy.toEvent` makes from a state. Your event
  handlers must turn it back into exactly the same state, or the squash is refused, unless its class
  mixes in `SquashMayChangeState`. ([chapter 14](14-squash.md))
- **Squash point**: the version an aggregate is squashed up to. Nothing below it can be read any
  more, except the audit events kept in the archive. ([chapter 14](14-squash.md))
- **Squash scheduler** (`SquashScheduler`): one per system. Once an hour it squashes the aggregates
  that their contexts' `AutomaticSquash` rule selects, one at a time. ([chapter 14](14-squash.md))
- **State**: what you get by replaying an aggregate's events, held in its aggregate root.
  ([chapter 1](01-cqrs-without-the-headache.md))
- **`StoredDataCheck`**: a read-only tool that reads every stored JSON value with the classes your
  application has now, and reports what no longer reads. Run it on a copy of production before an
  upgrade. ([chapter 6](06-events.md), [chapter 18](18-production.md))
- **Subscribable projection** (`SubscribableProjectionActor`): a projection that also pushes
  changes to the clients that subscribed, as `SubscriptionUpdated` messages.
  ([chapter 11](11-subscribable-projections.md))
- **Subscriber**: an actor registered with the event bus through `RegisterSubscribers`, usually a
  projection or a listener actor. ([chapter 8](08-projections.md))
- **Subscription**: one subscriber's interest in one aggregate type, for one listener kind. Its
  progress, the last version applied for each aggregate, is stored in the `subscriptions` table.
  ([chapter 8](08-projections.md))
- **Supervisor** (`ProjectionSupervisor`): the actor `Projections.start` returns for a managed
  projection. Use it wherever you would use the projection: for queries and in
  `RegisterSubscribers`. ([chapter 10](10-rebuilding-projections.md))

## T

- **Targeted rebuild** (`RebuildAggregates`): rebuilding, in place, what a projection made from one
  aggregate, one aggregate type or one organization (`RebuildTarget.OneAggregate`, `OneType`,
  `OneSpace`). ([chapter 10](10-rebuilding-projections.md))
- **Tell**: sending a message with `!` and walking away, without waiting for an answer. See also
  *Ask*. ([chapter 1](01-cqrs-without-the-headache.md))

## U

- **Uid generator** (`UidGeneratorActor`): the actor that hands out pools of ids for aggregates,
  commands and sagas, from PostgreSQL sequences. Make it with `UidGeneratorActor.postgres(system)`.
  ([chapter 4](04-first-command.md), [chapter 18](18-production.md))
- **Undelivered**: an event that a subscriber has not answered for 10 minutes. It is logged, stays
  in the outbox, and is published again later. ([chapter 9](09-projection-failures.md))
- **Undo** (`UndoEvent`): an event that cancels the effect of the last N events of an aggregate.
  They stay in the history, marked as undone. ([chapter 6](06-events.md))

## V

- **Version** (`AggregateVersion`): how many events an aggregate's history holds: 0 before the first
  event, one more with each. It never goes down, not even after a squash.
  ([chapter 1](01-cqrs-without-the-headache.md))

## W

- **Window**: see *Flow control*. ([chapter 9](09-projection-failures.md))
- **Write side**: the commands, aggregates and events, where decisions are made. See *CQRS*.
  ([chapter 1](01-cqrs-without-the-headache.md))
- **`writtenFrom`**: a map in an aggregate context from a new event class to the app-version from
  which every node can read it. Until every node runs that version, a command that would write the
  class is answered `CommandRejected`. ([chapter 17](17-cluster.md))

---

Next: [Appendix B](appendix-b-cheat-sheet.md) lists every message you can send, and what comes back.
