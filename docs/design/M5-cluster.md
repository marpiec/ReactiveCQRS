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
4. **Sharded owners (D43, D72; F46, F99)** (done) with the in-JVM multi-node harness; found on the way: F119 (stale snapshots), F120 (a chosen id on two owners).
5. **Delivery in the cluster and singletons (D44, D46, D53–D55; F48)** (done): registration through
   proxies, supervisors on every bus, the singletons, saga replies across nodes.
6. **Ids (D39)** (done): installation ranges below 2^53.
7. **Rolling upgrades (D65–D73)** (done): app-version, the write switch for new event classes,
   readiness; the rolling-upgrade test.
8. **Metrics (D56, R19)** with a no-op default (done).
9. **Flow control (F93)** (done, with the issues the high-load analysis found: F121 messages that fit the
   frame, F122 and F123 in the outbox poller, F126 batches by bytes).
10. **Scala 3 groundwork (D75)** (done): the remaining `TypeTag` uses (done for `StoredDataCheck`; the document
    store and `SubscribableProjectionActor` stay with mpjsons, which reads by `TypeTag`, until it is replaced).
11. **Audits at the close** (done): delivery and flow control (F128, F130), the cluster (F129, F131–F138;
    below).

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
  generation in `aggregates`, raised by every change that deletes snapshots because their state
  may be wrong (F119).
- **Acknowledgements.** An owner still publishes to its node's bus with its node's command bus as
  the publisher, which deletes the outbox rows (F91). The command bus forwards the ack to the
  owner only when that owner runs on its node (owners tell their node's bus when they start and
  stop), so an ack never wakes a passivated owner.
- **A chosen id on two owners** (a FirstCommand with `chosenAggregateId` retried during a
  hand-over): the `aggregates` key violation is a conflict, answered with the stored response or
  the conflict error.
- **F99**: a conflict with a commit of the owner's own runs again at once, one at a time; only a
  conflict with another writer waits the back-off.

### Where it could go wrong, and what handles it (step 4, reviewed before committing)

| Circumstance | What happens | Handled by |
|---|---|---|
| A stale stop reaches an owner that is already passivating | the shard keeps it for the next owner; the old one writes its passivation snapshot of the old history | the history generation: the write is refused (F119); the next owner gets the stop first and stops without reading |
| The owner's history changes between reading the generation and reading the events | the state is newer than the generation | the generation is read first: the owner only loses its snapshot writes until it reads again |
| A `Local` type on several nodes: an undo by one node's owner | another node's owner holds the state of before | the undo raises the generation; that owner's snapshot write is refused |
| A rewrite now updates `aggregates` rows early in its transaction | a writer waiting on that row inside the same process would deadlock (a test's simulated writer did) | a real other writer is another process and waits for the commit; the test writes before the transaction's first statement |
| Two owners of one aggregate (a node not yet downed, or `Local` on two nodes) create it under a chosen id | the loser breaks the primary key of `aggregates` | F120: a conflict, matched by table (an older database may name the key otherwise) |
| An owner runs a long command when a node leaves or a shard moves | Pekko stops the entity after `handoff-timeout` - 5 s, and a leaving node gives its regions 10 s | both 90 s in `reference.conf`, above the default command timeout; the host's process manager must wait longer still |
| Messages wait in a region during a hand-over | a command runs later than its caller sent it; the caller may time out first | idempotency keys for retries; a deadline from the sending node was left out, as clock skew between nodes could time out everything one node routes |
| A stop or history change for an owner that is not running | the region starts the owner only to stop it | it stops without reading (cheap); a burst for many duplicates is rare |
| An ack for an owner that moved to another node | the old node's bus has no owner to pass it to | the outbox poller asks for the rest of the re-publication later |
| An exception escapes a sharded owner | the shard restarts it (the bus would have stopped it, AR7) | the owner guards its receive; nothing escapes but fatal errors |
| A rolling upgrade: an envelope without `fresh` | read with the default, `false` | the owner reads its state: correct, only slower |
| Contended concurrent commands with plain commands committing in between | the command in turn conflicts again | it keeps its place and runs again at once; the command timeout bounds it |

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

**The protocol, as built in step 5:** `GetSubscriptionRequests(registered)` names the ref the
bus asked (the subscriber itself, or a proxy), and `SubscriptionRequests(requests, registered)`
echoes it, so an answer from the supervisor behind a proxy completes the proxy's registration.
The bus remembers which subscribers each registered ref answered with; when one of them stops
while the registered ref lives, it asks that ref again, and a new answer replaces the stopped
subscribers of that ref (they leave without acknowledging, as `RemoveSubscriber(keepOutbox)`).
While a registration is awaited, the bus asks again every tick. The supervisor answers every bus
that asks, keeps them all (watched), and sends each its later changes; a build it retires is
told to stop only once every bus confirmed the removal.

**Where it could go wrong, and what handles it (written before step 5's code):**

| Circumstance | What happens | Handled by |
|---|---|---|
| The singleton is not up yet when a bus asks, or the proxy's buffer overflows | the ask is lost or waits; the bus publishes nothing (D15) | the bus asks every tick until answered; a supervisor answers every ask (idempotent) |
| An answer comes from the supervisor, not from the proxy the bus asked | the bus would wait for the proxy forever | the answer names the registered ref |
| The singleton moves or crashes | its instances stop: each bus ends their deliveries incomplete (rows stay in the outbox) | the bus asks the proxy again, every tick until it answers (F129); the new supervisor's instances replace the stopped ones; the outbox poller re-publishes what waited (a lag of up to its first attempt, 3 min) |
| An answer to an old ask arrives after registration | a running bus ignored it before | the running bus takes answers too (they replace or add) |
| A build retired while several buses deliver to it | its tables would go while another bus still delivers | the supervisor waits for `SubscriberRemoved` from every bus it answered; a bus that ends is no longer waited for |
| Two supervisors at once (a partition before the resolver downs a side) | both subscribe their instances; both write | the subscription versions' compare-and-set and the document versions decide (F48), as for two writers today |
| A subscriber registered on one node only, not a singleton | it sees only the events of the owners on its node | documented: whatever must see every event runs once in the cluster |
| Several nodes start `Projections.retire` together | concurrent drops of one build | it runs under the schema lock (D69) |
| A saga runner that moved answers a caller stored as a local path | the path resolves on the new node: the answer is lost | the caller is stored with its node's address, an ask's temporary actor too (F137); rows stored before resolve as before |
| Two saga runners (one per node) resume the same pending sagas | each step runs twice | the runner is a singleton; saga commands are idempotent (H3) |
| The outbox poller and the squash scheduler on every node | every row re-published, every candidate squashed, N times | both are singletons |
| The response clean-up on every node's every command bus | the same idempotent `DELETE` N times an hour, at random minutes | left as it is (a singleton would add wiring for no gain); recorded as a refinement of D53 |

## Singletons (D46, D53, D55)

- `Projections.start` runs the supervisor as a singleton when the system is a cluster of more
  than one possible node (always, with the cluster provider) and returns its proxy, which the
  host uses as before (queries, `RegisterSubscribers`).
- The outbox poller, the squash scheduler and each saga runner run as singletons through a small
  factory in core (`Singletons.start`), with proxies where callers need them. The response
  clean-up stays with the command buses (above).
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
  application's JSON is read with `ignoreNonExistingFields`. A field added to a library message
  must be an `Option` or have a default; one added to an application class must be an `Option`
  (or accept its zero value), as mpjsons applies no default (F135). A changed field type gets a
  new class. Stored events keep the strict reader.
- **Exceptions**: the library's own by class with their fields; any other as
  `RemoteFailure(className, message)`. Pekko binds `java.lang.Throwable` to Java serialization,
  so the library's exceptions get bindings of their own and keep their class inside a
  `Status.Failure` too.
- `SubscriptionType` gets value equality, as a deserialized copy must match the constants.

## Ids (D39)

`installationNumber × 10^12 + sequence`, below 2^53; the number comes from configuration; a
pool that would leave the range or pass 2^53 is refused; existing ids stay as a legacy range;
`chosenAggregateId` is checked against the range.

As built in step 6: `IdRange(installation)` (`reactivecqrs.installation-number`, 0 by default: the
legacy range `[1, 10^12)`, where every id made before 1.0 lives); the aggregate id generator moves
its sequence to the range's start when it is behind, and refuses a pool past the range's end or
2^53; the command bus refuses a chosen id that is not positive, not below 2^53, or in another
installation's range (system and legacy ids, below 10^12, stay allowed). Command and saga ids
are internal and keep their sequences.

**Where it could go wrong, and what handles it (written before step 6's code):**

| Circumstance | What happens | Handled by |
|---|---|---|
| A node starts again with the sequence already in its range | moving it again would reuse nothing, but must not move it back | it moves only forward, and only when behind the range's start (under the schema lock) |
| The sequence is past the range's end: a wrong installation number, two numbers on one database, or the range used up | ids of another installation | the generator refuses at start, with the reason; a pool past the end is refused too |
| A pool that would pass 2^53 | ids a JavaScript client reads wrong | refused; the bus answers the command with an error (F75) |
| A rolling upgrade: old nodes take pools from a sequence a new node moved forward | larger ids than before | still unique; old nodes do not check the range, new ones do |
| A chosen id that is negative (ephemeral, D77), not below 2^53, or in another installation's range | an ephemeral id stored, a misread id, a collision when installations' data meet | refused with `FailureResponse`, before anything runs |
| A chosen id among the system ids (below 1001) or reserved earlier | the host's own ids | allowed: below 10^12, or in the installation's range |
| Installation 0 (no number set) | the legacy range | as before 1.0, with the end and 2^53 checks |

## Rolling upgrades (D65–D73)

- `pekko.cluster.app-version` is the host's configuration; the spike shows shards and
  singletons follow it.
- A new event class ships first with its handlers and is written only once every node runs
  that version: a write switch per class (`AggregateContext.writableFrom` or configuration),
  checked when a command's events are persisted.
- Readiness (D71): a check class for Pekko Management's health checks: the node's bus registered
  its subscribers, the regions and proxies are up, the schema version matches.

**As designed for step 7 (before its code):**

- **The write switch:** `AggregateContext.writtenFrom: Map[Class[_], String]`, an event class and the
  app-version from which every node reads it. Each node keeps the lowest app-version among the
  cluster's members (`Members`, from cluster events) in the contexts of its command buses; an
  owner refuses to persist an event of such a class earlier (`CommandRejected`, retryable), and so
  does a squash with such a squash event (`SquashRefused`); a command handler asks
  `everyNodeRuns(version)` to write the older event meanwhile. When every member's app-version is
  Pekko's default (0.0.0, not managed) or the system is not a cluster, everything is written.
- **Readiness:** `Readiness(system)` lists what a node waits for (each event bus until its
  subscribers registered); `ReadinessCheck` is a Pekko Management readiness check over it. The
  schema is initialized before the actors start, and a failure stops the start.
- **Migrations (D69):** the heavy jobs stay separate calls (`buildIndexesConcurrently`,
  `backfillSpaces`); schema changes stay expand-then-contract.

| Circumstance | What happens | Handled by |
|---|---|---|
| A new event class written while an old node is still a member | the old node's projections (the singletons run on the oldest, old node) or owners cannot read it | the persist guard refuses until every member runs the version; handlers fall back with `everyNodeRuns` |
| The singleton projections run on the oldest node, which is old until it is restarted last | they would meet the new class first | the guard counts every member, the oldest included |
| A host that never sets `pekko.cluster.app-version` (every member at 0.0.0) | the new class would never be written | all at 0.0.0 counts as not managed: everything is written (documented: set app-versions before using `writtenFrom`) |
| Some members at 0.0.0, others versioned | an unversioned node may be old | it counts as the lowest version: nothing new is written |
| Membership views differ between nodes for a moment | one node writes the new class while another still sees an old node leave | the old node is leaving or gone; the guard is per writer, so a node that sees an old member refuses |
| A rollback to the old version after the new class was written | old nodes cannot read the stored events | documented: no rollback past a version whose new classes were written |
| A squash event or a detach with a new class | the same | the squash checks its event like a command's |
| A saga order of a new class | stored, then resumed by a runner on an old node | the runner is a singleton on the oldest node, which writes orders itself: new order classes appear only after it moved; documented |
| A node whose bus never gets every subscription | it publishes nothing (D15) | it never becomes ready, so a load balancer sends it no traffic |

## Messages that fit the frame (F121)

Found by the high-load analysis (`docs/SCALING.md` §6): Artery drops a message larger than its
frame (`pekko.remote.artery.advanced.maximum-frame-size`, 256 KiB by default) and logs an error;
the sender is not told. A re-publication part of 1 000 events with 200-character notes is
470 KiB serialized, so a projection on another node never receives it, and its gap request asks
for the same part again.

**As designed (before the code):**

- **`MessageSize`** (core, cluster): the largest message the library sends between nodes, which is
  the frame (or the large frame, when the host routes large messages to destinations of their
  own) less 8 KiB for Artery's header; and the serialized size of a value, through the serializer
  Pekko would use (the cluster serializer counts its bytes without keeping them).
- **Deliveries in parts:** the bus sends a subscriber on another node several events in parts
  whose events (and state, for the kind that carries both) take at most half the limit, each
  event measured once per publication. One event, and a delivery of the state only, go as they
  are. Only the first part may start at the squash event.
- **`GetEventsForAggregate` on the asking node:** the command bus reads the history from the
  store, as it does for the historical reads; the owner no longer answers it (nor starts for it).
- **A command too large to cross nodes:** for a sharded type, the command bus answers
  `FailureResponse` at once when the command is larger than the limit, wherever its owner runs,
  so the outcome does not depend on placement.
- **The serializer's guard:** a message larger than the limit is written as a marker that the
  receiving node reads as `Status.Failure(MessageTooLargeException(class, size, limit))`, so an
  asker fails at once instead of timing out; the sending node logs the class and the size, at most
  once per class every 10 seconds.

| Circumstance | What happens | Handled by |
|---|---|---|
| One event, or a state, larger than the limit | it cannot be split | the guard fails it at the subscriber and the sender logs it; the delivery ends at the bus's deadline and re-publications fail the same way; documented (SCALING §6): raise the frame, or keep states and events small |
| A state too large for the kind that carries state and events | halving the events does not help | parts of one event each; the guard fails what still does not fit |
| An event that cannot be serialized (measuring throws) | the bus would fail on the whole publication, for every subscriber | a failed measurement counts as nothing; the event goes unsplit and Artery fails it as before |
| A part is lost between nodes (a full outbound queue, a quarantine) | the next part leaves a gap | the subscriber's sequence gate asks the owner to re-publish from the gap, in parts again |
| Parts and later live events of the aggregate | — | the bus sends a publication's parts at once, in version order, and Artery keeps the order between two actors; the gate applies by version anyway |
| A later part marked as starting at the squash event | it would reset the subscriber's version | only the first part carries the flag |
| Measuring on the bus (one actor per node) | every event serialized once more | only for subscribers on another node and publications of several events; a single event (the common case) is not measured |
| A subscriber reached through a forwarder on the bus's node (a proxy) | it looks local, so nothing is split | subscriptions name the subscriber itself (D15, step 5); documented |
| Large-message destinations set by the host | the limit would be too low for them | the limit takes the large frame when `large-message-destinations` is set; a message to another destination between the two frames is dropped by Artery as before (documented) |
| A message just under the frame but over the limit (the header is usually far smaller than 8 KiB) | it fails although Artery might take it | accepted: the failure names the size and the limit |
| A command that fits only because its owner runs on the bus's node | it would succeed there and fail elsewhere | the command bus refuses it wherever the owner runs (sharded types) |
| A reply too large for an asker on another node (a state, a query answer, a custom response) | it was dropped; the asker timed out | the asker fails at once with `MessageTooLargeException`; a large state is read on the asking node with `GetAggregateAtInstant` (documented) |
| A marker read by a node without F121 (an upgrade from an earlier version) | the unknown tag fails to deserialize: dropped as before | no installed version precedes it (M5 comes before 1.0) |
| `GetEventsForAggregate` while the owner writes | — | the store has every committed event, and a command's caller is answered after the commit, so it reads its own writes |
| The whole history of a very long aggregate | memory on the asking node (168 MB for 50 000 events, measured) | documented (SCALING §3) |

Built as designed. `ClusterMessageSizeSpec` failed in all five scenarios before the change, with
Pekko's "Discarding oversized payload … max allowed size 262144 bytes" in the log, and passes
after it.

## Flow control (F93, with F122 and F123)

A projection slower than the commands lets its mailbox hold every event behind it, with the
states that come with them, and the bus a delivery per event (F93; `docs/SCALING.md` §5). Under a
lag of more than 3 minutes, the outbox poller re-publishes what still waits, adding duplicates to
the same mailbox.

**As designed (before the code):** the bus keeps a window per subscription (subscriber and kind)
over the events it has sent and not yet had answered. Beyond the window it holds events back, and
the outbox, which has every one of them, is the queue.

- **The window:** `DeliveryOptions.window` events (10 000) and `windowBytes` (64 MB) sent to a
  subscriber and not yet answered, whichever is reached first; the subscriber's mailbox is what
  holds them, so the window is the subscriber's, across its kinds. The bytes are the events'
  stored JSON, which the event store reports when it writes and reads them (F126); the owner
  passes them with each publication. Each message sent is recorded by aggregate, first version and
  kind, and the answer that names them takes it off. A record older than the deadline is taken off
  too, so a lost message cannot shut the window.
- **Parts by bytes too (F126):** a re-publication part is at most 1 000 events and 16 MB, and a
  rebuild page and chunk at most their events and 64 MB and 16 MB; each takes at least one event.
  The store cuts a range by the stored sizes (`octet_length`, which PostgreSQL reads without
  detoasting), so it never reads more than the part.
- **Holding:** a live publication that does not fit is held for that subscription. Its events'
  deliveries wait for it as held, not sent, and the aggregate is behind for it from the first
  held version. Every later event of an aggregate that is behind is held too, so the subscriber
  never sees a gap of the bus's making. When nothing is in flight to the subscription, one
  publication goes whatever its size.
- **Pulling:** while the subscription's in-flight events are at most half the window, the bus asks
  the owners of the aggregates behind, oldest first, to re-publish to that subscriber
  (`RepublishRange`, the gap request's path), at most `maxPulls` (100) at once and about as many
  events as the window has room for. A re-published part goes out whatever the window, as a
  subscriber's own gap fill does. It turns held deliveries into sent ones, and what remains past
  it is behind again. A pull unanswered after `pullTimeout` (1 minute) goes back to the queue.
- **Acknowledgement:** a delivery completes when every subscription answered, held ones included,
  so the owner is acknowledged and its outbox rows go as soon as the slow subscriber has the
  events; the poller has nothing to clean up.
- **Status:** `DeliveryStatus` also gives the held events and, per subscriber, the aggregates
  behind; the summary line says the same.
- **The poller:** an aggregate it gave up on is re-published again every `retryGivenUpEvery`
  (30 minutes) instead of waiting for the next start (F123). It reads at most `maxTracked`
  (100 000) waiting aggregates per poll, the oldest first, so its memory no longer grows with
  the outbox (F122).

| Circumstance | What happens | Handled by |
|---|---|---|
| A sent message is never answered (lost between nodes, a marker for a message too large, F121) | its events would hold the window shut | records older than the deadline are taken off; the deliveries end as undelivered and the rows stay for the poller |
| A subscriber answers a message twice, or one the bus does not know (a duplicate from a gap fill) | the count would go wrong | answers are matched by aggregate and first version; an unmatched answer changes nothing; a message sent again under the same key keeps one record (it counts once, which errs open) |
| An aggregate's event held while a later one is sent | the subscriber sees a gap and asks for it as well | once an aggregate is behind for a subscription, every later event of it is held until it is pulled |
| One publication larger than the window (a command with 20 000 events) | it would never fit | with nothing in flight to the subscription, it goes |
| The pulled part is published through another node's bus (the owner moved) | this bus's held deliveries never see their answers, and it would pull the aggregate again every minute | the held deliveries end at the deadline (the rows stay), and the aggregate stops being behind then (found in review, before committing); the poller re-publishes it, and the subscriber acknowledges duplicates |
| A pull is lost, or its owner cannot read the store | the aggregate stays behind | an unanswered pull goes back to the queue after `pullTimeout`; held deliveries end at the deadline |
| Many aggregates behind, each with one event | thousands of pulls at once, each restoring an owner | at most `maxPulls` pulls at once |
| A part ends before the last held version (1 000 events per part) | the rest would wait | the remainder goes back to the queue as behind from the next version |
| The subscriber stops while events are held | — | its held deliveries end like its sent ones (incomplete, rows stay, D20b), and its window, behind and pulls are dropped |
| The subscriber is removed (a retired build) | — | held deliveries stop waiting for it, as sent ones do |
| The outbox poller re-publishes an aggregate that is behind | — | for the behind subscription it is held again (the range grows); the others take it as a duplicate |
| Held deliveries are still in memory | a delivery per held event (keys, about 200 bytes) | ended at the deadline (10 min): bounded by the rate times the deadline, as before; the payloads are in the store, not in memory |
| A projection with many gaps of its own | its gap fills are not held | it asks for one gap per aggregate at a time, repeating with back-off (D20c) |
| A fast subscriber next to a slow one | — | each subscription has its own window; the fast one is never held |
| The stall log fills with held deliveries | held is expected under load | stall lines are for sent deliveries; held ones appear in the summary by subscriber |
| The poller reads only the oldest waiting aggregates | aggregates past them wait until the oldest are delivered | the oldest are retried (and given-up ones every 30 minutes), and subscribers' gap requests fill any aggregate that gets new events; documented (SCALING §4) |
| An event whose size is not known (a store that does not report it, the memory store) | the byte window cannot count it | it counts as 1 KB; the event window still bounds it |
| One event larger than the byte window or a part's bytes | it would never fit | a part, a page and a chunk take at least one event, and with nothing in flight a publication goes whatever its size |
| The sizes are the JSON's, not the objects' in memory | the heap holds more (about twice for text, measured: 10 MB per 5 MB event at a restore) | the byte window is a budget for comparison, documented with that factor |

Built as designed, with three things found on the way:
- The window is the subscriber's, counted across its kinds, since one mailbox holds them all;
  records are kept per kind, as each kind is answered on its own.
- The builder took a short page for its last (`readAll`): a page its bytes cut short now says so
  (`CommittedPage.cut`), or the bulk pass would end early and leave the rest to the
  reconciliation (correct, but per aggregate and slower).
- The reconciliation and gap fills read an aggregate's missing range at once: they read it in
  parts of a chunk's bytes now, one item each. The whole range is still read before its items are
  chunked, so a very long missing range of large events is held at once (documented).

The audit at the M5 close found circumstances the table missed:
- A subscriber with aggregates behind took live publications of others up to its whole window,
  and pulls start at half: under steady load the held ones waited for the deadline. While
  aggregates are behind, a live publication now takes at most half the window, so pulls always
  find the other half, and what pulls asked for counts as in flight until it comes (F130).
- An aggregate held again was dropped a deadline after its first hold, while later held
  deliveries still waited; it is now dropped a deadline after its latest hold. Events held again
  counted their bytes again; only versions new to a range count now, and a part and what it leaves
  each take their share. A held delivery that its pull sent was logged as stalled at once; it
  stalls from when it was sent (F130).
- A re-publication could send a subscription added later versions with a hole in one message,
  which the sequence gate applies as a range: messages hold consecutive versions only (F128).
- The window is one bus's: a projection's singleton that takes events from the buses of several
  nodes can hold one window from each. Documented (SCALING §5); a window across nodes would need
  the subscriber to grant credit, which waits for a need.
- The builder: a range read by version stopped at a squash far above its start (F139); a run of
  one aggregate in a page made one item, so a chunk could hold the whole page, and a range read in
  parts restored its state per part (F140).
- The poller: retries of given-up aggregates could take a poll's whole budget, so newer rows'
  attempts waited; they take at most half while others are due (F141).

Tests: `FlowControlSpec` (the window, holding, pulls, expiry), `EventsBusActorSpec` (a slow
subscriber gets its window and the rest by pulls, a fast one is not held back, an aggregate stays
held while pulled, bytes count), `DeliveryEndToEndSpec` (a slow projection behind real owners),
`ProjectionBuilderSpec` (pages and chunks by bytes), `PostgresEventStoreStateSpec` (sizes, cuts,
the poller's limit), `RedeliveryPolicySpec` (retries after giving up). Mutations caught: a window
that admits everything (four bus scenarios), a bus that never pulls (the end-to-end scenario), a
builder that takes a cut page for its last, a poller without the limit or the retries.

## The audit at the close (cluster)

A read-only audit of the cluster code found nine circumstances the tables above missed; each was
checked against the code, and each fix has a test that fails without it:

| Circumstance | What happened | Now |
|---|---|---|
| A singleton moves, and the bus's ask to its proxy is lost (a full buffer, a stopping supervisor) | the bus asked once: the new instances stayed unsubscribed on that node until it restarted | asked every tick until answered (F129) |
| A `Local` type's owners on several nodes | each answered from its own state: reads, min-version queries, re-publications and gap fills through a node whose owner was behind missed the others' writes | on a node that is not alone, an owner compares the stored version and generation before such answers and reads the aggregate again (F131) |
| A new version removes a projection, and its first node calls `Projections.retire` | the projection was dropped while the oldest node, of the old version, still served it | the drop waits until every member runs the new version (F132) |
| A projection's singleton moves | its push subscriptions were lost, and renewals were ignored | a renewal of an unknown subscription is answered as cancelled (F133) |
| A permanent delete detaches duplicates during a rollout | its squash events skipped the write switch | checked like a squash's (F134) |
| A command of version 2 adds a field with a default, read on another node | the application's classes get no default: the field read as null | documented: added fields must be `Option`s (F135) |
| A saga's runner stops between its answer and its write | the next runner answers again | accepted, documented: answers are at least once (F136) |
| A saga ordered by an ask on the runner's own node | its path was stored without the node's address | written with it (F137) |
| A node takes a command before it knows its cluster | the write switch allowed every class | nothing new is written until a member is known (F138) |

Changing a type's placement or `numberOfShards` makes two owners per aggregate while old and new
nodes run together (sharded owners do not compare with the store): such a change needs a full
stop, documented in H10.

## Verification

The plan's M5 verification, at the close (17/10/2026):

| Item | How it is covered |
|---|---|
| A command on any node reaches its owner; a projection singleton receives every owner's events; queries from any node | in-JVM multi-node tests (several `ActorSystem`s on local ports, one PostgreSQL schema): `ShardedOwnersSpec`, `ClusterDeliverySpec`, `LocalOwnersSpec`, `ClusterMessageSizeSpec` |
| A node that leaves under load; a singleton that moves | `ShardedOwnersSpec` (a twelve-second command during the hand-over, nothing lost or applied twice), `ClusterDeliverySpec` (the projection, the poller and the saga runner move with the oldest node), `RollingUpgradeSpec` |
| A killed writer, a rebalance under load with a client ledger, a network partition, singleton failover after a loss | the docker-compose cluster with Toxiproxy: not run, as the machine has no Docker; moved to M7 (E2). The in-JVM tests cover graceful leaving only |
| Cluster of one | the full suites pass with `-Dreactivecqrs.test.clusterOfOne=true` (core 274, testdomain 164) |
| Rolling upgrade N → N+1 with a new event class under load | `RollingUpgradeSpec` (no failed command but retryable ones, the projection caught up, no deserialization error, a removed projection retired only after); a projection version switch across nodes in `ClusterDeliverySpec` |
| Serialization | `ClusterSerializationSpec` (every conversation's messages), `ClusterSerializerSpec` (messages of another version) |
| Id ranges | `IdRangeSpec`, `ChosenIdRaceSpec` |
| Neula's acceptance (R18) | Neula's cluster tests, with the host steps of H10 |
