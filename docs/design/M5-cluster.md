# M5 design: the cluster

How milestone M5 implements D39, D42–D46, D53–D56 and D65–D73 of `KNOWN-ISSUES.md`, and fixes
F46, F48, F63, F93 and F99. Written at the start of the work, 17/10/2026; `ARCHITECTURE.md`
describes the result.

## What the spike showed (Pekko 1.7.1)

`ClusterBehaviourSpec` (three nodes in one JVM, kept as a test so an upgrade that changes this is
noticed):

- New shards go only to the nodes with the newest `pekko.cluster.app-version`; an older node gets
  none (D66: during a rollout work moves only from old to new nodes).
- A singleton runs on the oldest node and moves to the next oldest when that one leaves, so a
  rollout that restarts the oldest node last moves each singleton once (D66).
- The split brain resolver downs a node lost without leaving: `keep-majority` with three nodes,
  and `lease-majority` with two, where the side that holds the lease stays (D46). The Kubernetes
  lease is configuration on Neula's side (`pekko-lease-kubernetes`); the test uses a lease in the
  JVM.
- Java serialization is off by default: every library message that crosses nodes needs the
  serializer of D45.

Docker is not available on the development machine, so the plan's docker-compose scenarios
(Toxiproxy partitions, killing the writing node) are prepared as files and run on a machine
that has it; the in-JVM multi-node tests cover what one JVM can show.

## The shape

```
node A                                         node B
 command bus (type T) ──► shard region T ───────► owner of aggregate 7 (shard entity)
 event bus A ◄── owners on A publish            event bus B ◄── owner 7 publishes
   │ delivers to the projection's instances         │
   ▼ (on whichever node the singleton runs)          ▼
 projection supervisor (singleton) ─ instances, builder (one node)
 outbox poller, squash scheduler, sagas, response clean-up: singletons
```

- Every deployment is a cluster: one node on-premise, several in the cloud (D42). A cluster of
  one behaves like today; the test suites run in one (verification).
- **Owners** of a context that opts in are shard entities placed by aggregate id (D43); the
  command bus stays one per type per node and routes to its region; idle owners are passivated
  by the shard, which replaces the bus's reaping (F46). A context that does not opt in keeps
  local owners (each node its own; the SQL version check arbitrates), for ephemeral aggregates
  (M6) and gradual migration.
- **The event bus** stays per node: an owner publishes to its node's bus, which delivers to the
  subscriptions registered there (D44). A subscriber that runs once in the cluster (a projection,
  a saga) is registered on every node's bus through a singleton proxy, and subscribes its
  instances on every bus (below).
- **Singletons** (D46, D53): each managed projection's supervisor with its instances and builder,
  the outbox poller, the squash scheduler, each saga runner, and the response clean-up.
  Queries keep their messages and go through the proxy (D55).

## Order of the work

1. **Spike** (done): the cluster modules, `ClusterBehaviourSpec`.
2. **Schema lock (D69, F63)** (done): every schema initialization (the components' `initSchema()` and
   the document stores' tables) runs one process at a time, under a PostgreSQL advisory lock held
   on a connection of its own, so nodes that start together no longer fail on concurrent DDL. The
   heavy jobs stay separate calls (`buildIndexesConcurrently`, `backfillSpaces`); readiness checks
   the schema (D71, step 7). Needed first: in-JVM nodes start together.
3. **Serialization (D45, D68)** (done): one mpjsons-based serializer for every library message and the
   domain's commands, events, states and query answers; round-trip tests.
4. **Sharded owners (D43, D72; F46, F99)** with the in-JVM multi-node harness.
5. **Delivery in the cluster and singletons (D44, D46, D53–D55; F48)**: registration through
   proxies, supervisors on every bus, the singletons, saga replies across nodes.
6. **Ids (D39)**: installation ranges below 2^53.
7. **Rolling upgrades (D65–D73)**: app-version, the write switch for new event classes,
   readiness; the rolling-upgrade test.
8. **Metrics (D56, R19)** with a no-op default.
9. **Flow control (F93)**.
10. **Scala 3 groundwork (D75)**: the remaining `TypeTag` uses.

Each step ends with `sbt test` green, the docs updated and a commit; the plan's M5 verification
follows.

## Sharded owners (D43, D72)

- **Opting in.** `AggregateContext.placement` (default `Local`) or a `Sharded(numberOfShards)`
  given to the command bus: the bus starts its type's region (`owner-<type>`), whose entity is
  the owner, on the blocking dispatcher.
- **The envelope.** Everything the bus sends an owner travels as `ToOwner(aggregateId, message)`;
  the entity id is the aggregate id, the shard `abs(id % numberOfShards)`. The owner takes its id
  from its entity name; `fresh` (a FirstCommand with a pool id: nothing to restore) travels in
  the command envelope instead of the constructor.
- **Stopping.** The owner's stop message is the region's hand-off stop message too, so a
  rebalance, idle passivation and an operator's `PassivateAggregate` all end the same way: after
  the command at work, with the serial queue drained (D72), callers answered, the passivation
  snapshot written; the shard keeps the messages that arrive meanwhile for the next owner (what
  the bus's kept messages did). Idle passivation uses the shard's strategy, which replaces the
  bus's keep-alive sweep (F46).
- **Only the shard keeps messages for a stopping owner**, so every stop goes through it. The
  shard's own stops (hand-off, idle passivation) send `StopOwner`. A stop the bus asks for (an
  operator's `PassivateAggregate`, a history change) reaches the owner through the region as a
  request, which the owner turns into `Passivate(StopOwner)` to its shard (marking itself stale
  first when the history changed); the shard then keeps what arrives and sends `StopOwner`
  back. A failed restore asks the same way (D11). An owner started only to receive a stop stops
  without restoring.
- **A stop must not write a stale snapshot.** A stale stop (`HistoryChanged`: a rewrite, a
  squash's detach) sent while the owner is already passivating is kept by the shard for the next
  owner, so the old one could write the old state at an unchanged version. Snapshot writes are
  therefore conditional on the aggregate's history being the one the owner restored: a history
  generation in `aggregates`, raised by every change that rewrites stored history.
- **Acknowledgements.** An owner still publishes to its node's bus with its node's command bus as
  the publisher, which deletes the outbox rows (F91). The command bus forwards the ack to the
  owner only when that owner runs on its node (owners tell their node's bus when they start and
  stop), so an ack never wakes a passivated owner.
- **A chosen id on two owners** (a FirstCommand with `chosenAggregateId` retried during a
  hand-over): the `aggregates` key violation is a conflict, answered with the stored response or
  the conflict error.
- **F99**: a conflict with a commit of the owner's own runs again at once, one at a time; only a
  conflict with another writer waits the back-off.

## Delivery in the cluster (D44, D53, D54)

- A bus asks each registered subscriber for its subscriptions (D15). A singleton proxy forwards
  the question; the supervisor answers with its instances' requests and names the registered ref
  it answers for, so the bus can tell the answer of a proxy from one of a local subscriber.
- The supervisor keeps every bus that asked and sends each of them its later changes
  (`AddSubscriptions`, `RemoveSubscriber`).
- When the instances of a subscription stop (the singleton moved or crashed), the bus keeps them
  as stopped subscribers (their events stay in the outbox, F110) and asks the registered proxy
  again; the new supervisor answers with its new instances, and the bus drops the stopped ones
  without acknowledging. The outbox poller then publishes the kept rows again.
- Re-publication requests (`RequestRepublish`) go to the bus of the node that delivered the
  event, which sends them to its command bus, which routes them to the owner through the region
  (D54).
- Single writer (F48): a projection runs once, so its caches are its own; the subscription
  version cache is per node and the database decides (F29, F115).

## Singletons (D46, D53, D55)

- `Projections.start` runs the supervisor as a singleton when the system is a cluster of more
  than one possible node (always, with the cluster provider) and returns its proxy, which the
  host uses as before (queries, `RegisterSubscribers`).
- The outbox poller, the squash scheduler, the response clean-up and each saga runner run as
  singletons through small factories in core, with proxies where callers need them.
- Sagas store the caller to answer as a serialized actor path with its node's address, so an
  answer reaches it from any node.

## Serialization (D45, D68)

What crosses nodes (a survey of the code at the start): the bus's envelope to sharded owners
and what owners answer (command responses, `Try` replies with aggregates, events or library
exceptions); the deliveries, acknowledgements and re-publication requests between a node's bus
and a singleton's instances; the subscription set-up; the supervisors', poller's, scheduler's and
sagas' questions and answers through their proxies; the application's commands, events, states,
saga orders and responses, and query messages and answers. Supervisor ↔ instance ↔ builder
traffic stays inside the singleton, as do the two messages that carry functions
(`ResetForRebuild`, `SubscriptionsHandedOver`).

What mpjsons does (0.7.0): reflection on the declared type; by class name a type parameter stays
unbound (F64), `Any`, traits, `ActorRef`, `Try` and `Throwable` are refused; sealed hierarchies
work with `markTypedClass`; `ignoreNonExistingFields` skips unknown fields, and a missing field
reads as zero, null or `None` (Scala defaults are not applied).

So:

- **One serializer**, `ClusterSerializer` (a string-manifest serializer), bound to
  `java.io.Serializable`: case classes and Scala collections that Pekko has no more specific
  binding for (Pekko keeps `Some`, `None`, `Status`, `ActorRef`). Java serialization stays off.
  The manifest is the class name; the payload is JSON.
- **The application's values** (commands, events, states, saga orders and responses, query
  messages and answers) go by their runtime class through mpjsons, as stored events do. A
  top-level `Option`, collection or `Try` carries each element's class.
- **Library messages**, which mpjsons cannot type (the generic deliveries, envelopes with `Any`,
  `ActorRef`s, `Try` replies, `CustomSuccessResponse[INFO]`), are written field by field with
  the fields' names, each value tagged: an `ActorRef` as its serialized path, a polymorphic value
  as its runtime class with its JSON, a case object (`DeliveryKind`, `RebuildTarget`, ...) by
  name, tuples by position.
- **Tolerant reading** (D68): a library message is matched by field name (a field the reader
  lacks is skipped, a missing one takes its Scala default, else `None`, zero or null); the
  application's JSON is read with `ignoreNonExistingFields`. A field added to a message must be
  an `Option` or have a default (or accept its zero value), and a changed field type gets a new
  class. Stored events keep the strict reader.
- **Exceptions**: the library's own by class with their fields; any other as
  `RemoteFailure(className, message)`. Pekko binds `java.lang.Throwable` to Java serialization,
  so the library's exceptions get bindings of their own and keep their class inside a
  `Status.Failure` too.
- `SubscriptionType` gets value equality, as a deserialized copy must match the constants.

## Ids (D39)

`installationNumber × 10^12 + sequence`, below 2^53; the number comes from configuration; a
pool that would leave the range or pass 2^53 is refused; existing ids stay as a legacy range;
`chosenAggregateId` is checked against the range.

## Rolling upgrades (D65–D73)

- `pekko.cluster.app-version` is the host's configuration; the spike shows shards and
  singletons follow it.
- A new event class ships first with its handlers and is written only once every node runs
  that version: a write switch per class (`AggregateContext.writableFrom` or configuration),
  checked when a command's events are persisted.
- Readiness (D71): a check class for Pekko Management's health checks: the node's bus registered
  its subscribers, the regions and proxies are up, the schema version matches.

## Tests

The plan's M5 verification; in-JVM multi-node tests (several `ActorSystem`s on local ports, one
PostgreSQL schema) for sharding, singletons, delivery across nodes, hand-over under load, a lost
node and a rolling upgrade between app-versions; the full suites in a cluster of one; round trips
of every message.
