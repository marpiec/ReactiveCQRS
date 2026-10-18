# Chapter 17. Going Distributed: Running in a Cluster

So far your application has lived on one machine. In this chapter it moves into a shared house:
several machines, called **nodes**, that work as one. By the end you will know what changes when
you run on several nodes, which parts run on every node and which run exactly once, how to decide
where the owners of each aggregate type live, what may travel between nodes (and how big it may
be), and how to upgrade the nodes one at a time while the application keeps serving.

---

## Why more than one node?

Picture a busy restaurant with one cook. When the cook takes a break, the kitchen stops. Hire a
second cook and the kitchen keeps going through breaks, and serves more tables at dinner time. The
two cooks share one order book, and there is one house rule: an order ticket is in the hands of one
cook at a time.

Running ReactiveCQRS on several nodes gives you the same three things:

- **Staying up.** When one node restarts, or a machine dies, the others keep serving.
- **More hands.** Command handlers, aggregate owners and projections share the CPUs of several
  machines.
- **Upgrades without a "closed" sign.** You replace nodes one at a time (a *rolling upgrade*, at the
  end of this chapter).

And a lot stays exactly the same:

- **One PostgreSQL database** for all nodes, with the same tables as before.
- **Your code**: the aggregates, commands, events, projections and sagas of the earlier chapters.
- **The version check** (the optimistic lock of [chapter 1](01-cqrs-without-the-headache.md)). The
  database still lets only one writer store each version of an aggregate, whatever happens between
  the nodes.

> **Remember:** all the nodes of a cluster share one database. The cluster spreads the *work*; the
> database keeps the *truth*.

## Every installation is a cluster, even a cluster of one

ReactiveCQRS runs on **Apache Pekko Cluster**. You turn it on with configuration, not code. Here is
an `application.conf` for a node of a three-node installation whose actor system is called `shop`
(`ActorSystem("shop", config)`):

```hocon
pekko {
  actor.provider = cluster
  actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }
  remote.artery.canonical {
    hostname = "10.0.0.11"     # this node's address
    port = 17355               # Pekko's default
  }
  cluster {
    seed-nodes = ["pekko://shop@10.0.0.11:17355", "pekko://shop@10.0.0.12:17355"]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver.active-strategy = keep-majority
    app-version = "1.4.0"      # your release's version; see "Rolling upgrades" below
  }
}
```

Four of these lines deserve a word:

- **`serialization-bindings`** sends every message between nodes through the library's serializer
  (see "Messages between nodes" below). The library's `reference.conf` binds it too, but Pekko's
  binds `java.io.Serializable` to Java serialization, which is off, and of two `reference.conf` files
  the one earlier on the classpath wins. A packaged application's classpath may come in any order, so
  your configuration binds it. A node that can talk to others but would serialize otherwise logs an
  error saying so, and stays not ready.

- **`seed-nodes`** are the addresses a starting node contacts to join. They suit machines whose
  addresses stay put. On Kubernetes, join through Pekko Management's **Cluster Bootstrap** instead,
  as E2 does at the end of this chapter: a restarted pod gets a new IP under its old name, and the
  first seed node, which forms a new cluster when no other seed answers within five seconds, can
  then split the cluster in a rolling restart.
- **The split brain resolver** decides what happens when the network splits the cluster into two
  groups that cannot see each other: one side stays, the other shuts down. `keep-majority` keeps the
  bigger side. With two nodes there is no majority, so use Pekko's `lease-majority` strategy with a
  lease (on Kubernetes, the Kubernetes lease from Pekko Management): the side that holds the lease
  stays.
- **`app-version`** names your release. It matters during rolling upgrades.

On a single machine you use the very same settings, with the node itself as its only seed. That is a
**cluster of one**, and it behaves like the single node you had before. The library's own test
suites pass when every test runs on a cluster of one.

> **Tip:** use the cluster settings from day one, even on one machine. Going from one node to three
> later is then a change of configuration, not of code.

> **Under the hood:** "every deployment is a cluster: one node on-premise, several in the cloud" is
> decision D42; the split brain resolver and the lease are D46, checked by `ClusterBehaviourSpec`.
> The full story is in [ARCHITECTURE.md, section 13](../ARCHITECTURE.md).

## What runs on every node, and what runs once

In a shared house everybody has a toothbrush of their own, but only one person does the weekly
shopping, or the fridge fills up with five bags of potatoes. A cluster is the same:

| On every node | Once in the whole cluster |
|---|---|
| the uid generator (`UidGeneratorActor.postgres(system)`) | each managed projection (`Projections.start`, [chapter 10](10-rebuilding-projections.md)) |
| an event bus (`EventsBusActor`) | the outbox poller (`OutboxPollerActor`) |
| a command bus per aggregate type (`AggregateCommandBusActor`) | the squash scheduler (`SquashScheduler`, [chapter 14](14-squash.md)) |
| | each saga runner ([chapter 12](12-sagas.md)) |

The one fact to keep in mind: **there is an event bus per node.** An aggregate owner publishes its
events to the bus of the node it runs on. A subscriber registered on one node's bus therefore sees
only the events of that node's owners. So whatever must see *every* event (a projection, a saga that
listens) runs once in the cluster and is registered on *every* node's bus.

### Starting what runs once: `Singletons.start`

`Singletons.start(system, name, props)` is how you start something that must run once. Every node
calls it, with the same name. Pekko then runs the actor on the **oldest** node of the cluster, and
`start` gives each node a **proxy**: an `ActorRef` that forwards whatever you send it to wherever the
actor runs, keeping the sender, so answers come back to you. When the oldest node leaves, the actor
moves to the next oldest one, once; while it moves, the proxy holds the messages sent to it (up to
Pekko's buffer of 1 000).

On an actor system that is not a cluster, `Singletons.start` simply starts the actor itself, so the
same code works in both.

`Projections.start` ([chapter 10](10-rebuilding-projections.md)) does this for you: in a cluster,
the `ActorRef` it returns is the node's proxy to the projection's supervisor.

Here is what every node of E2, the cluster example, runs at start: E1's carts (sharded, see the next
section), their carts list as a managed projection, and the outbox poller:

<!-- snippet: e2/ClusterSetup.scala#node -->
```scala
// The same code on every node (system: this node's ActorSystem)
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

// One of each on every node
val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
val carts = system.actorOf(
  AggregateCommandBusActor(new ShardedCartContext, uidGenerator, eventStore, responses, eventBus), "carts")

// Once in the cluster: every node starts them, one node runs them
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions,
  new PostgresProjectionRegistry().initSchema(), contexts = Seq(new ShardedCartContext)))
val cartsList: ActorRef = projections.start[CartsListProjection]("e2-carts-list", 1, "cartsList") { build =>
  new CartsListProjection(subscriptions,
    new PostgresDocumentStore[CartSummary](build.documentStore("e2_carts_list"), mpjsons, new NoopDocumentStoreCache))
}
Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))

// Every node registers the projection's proxy on its own bus
eventBus ! RegisterSubscribers(Seq(cartsList))
```

The imports are at the top of `ClusterSetup.scala`, in `examples/src/main/scala/io/reactivecqrs/examples/e2/`.

What happens when this runs on three nodes:

- Each node has its own uid generator, event bus and cart command bus. A command sent to the cart
  command bus of *any* node works.
- One supervisor of the `e2-carts-list` projection runs, on the oldest node. Each node's bus knows it
  through that node's proxy, so the projection gets the events of the owners on every node.
- One outbox poller runs, also on the oldest node.
- A saga runner runs once too: every node calls `SagaRunner.start(system, ...)` ([chapter 12](12-sagas.md))
  and sends saga orders to the proxy it returns, from whichever node it is on.

E2's `ClusterSpec` runs this on three nodes in one JVM: a cart filled through every node has one
owner, and the carts list answers on every node; when the oldest node leaves, the carts list moves
and still gets the events published meanwhile. The library's `ClusterDeliverySpec` checks the same
with a saga runner too.

> **Warning:** never start the outbox poller or the squash scheduler with a plain `system.actorOf` on
> every node. With three nodes, every event waiting in the outbox would be re-published three times,
> and every squash would be tried three times. Start them with `Singletons.start`.

> **Remember:** register the projection's proxy on *every* node's bus. A projection registered on one
> node only would see one node's events, and quietly miss the rest.

A few things work slightly differently once something has moved to another node:

- **Saga answers come at least once.** A saga runner stores who asked, with the asker's node
  address, so the answer reaches a caller on any node. If the runner moves between sending an answer
  and recording it, the next runner may answer again: a long-lived caller should treat a second answer
  for the same saga as a repeat.
- **Push subscriptions** of subscribable projections
  ([chapter 11](11-subscribable-projections.md)) are lost when the projection's singleton moves. A
  renewal is then answered with `ProjectionSubscriptionsCancelled`: subscribe again.

> **Under the hood:** a bus asks each registered subscriber for its subscriptions with
> `GetSubscriptionRequests(registered)`, and the supervisor behind a proxy answers for it. When the
> singleton moves, its instances stop; the bus asks the proxy again, and what was published meanwhile
> stays in the outbox until the outbox poller re-publishes it. See
> [ARCHITECTURE.md, section 13.4](../ARCHITECTURE.md) (D44, D46, D53, D55, F110, F129).

## Where owners live: Local or Sharded

Each aggregate has an **owner**, the actor that keeps its state and runs its commands
([chapter 1](01-cqrs-without-the-headache.md)). In a cluster you choose, per aggregate type, where
those owners live. Think of the order tickets again:

- **Local:** every cook who is asked about table 7 takes a copy of its ticket. When two cooks write
  on it at once, the order book (the database) decides who was first.
- **Sharded:** every ticket has one desk in the kitchen. Whoever takes an order for table 7 walks it
  over to that desk.

### Local, the default

With `Placement.Local`, each node keeps owners of its own, for the aggregates it is asked about.
Cart 42 can have an owner on node A *and* one on node B. That is safe: the database's version check
lets only one of them store each version.

It costs a little, though. On a node that is not alone in its cluster, an owner does not know what
the other nodes wrote, so before it answers the state, or runs a command, it compares its version
with the stored one (one key lookup) and reads the aggregate again when another node changed it.

Idle Local owners are put to sleep by their node's command bus. At most every 5 minutes it sweeps:
beyond 200 owners, the least recently used ones idle for 2 minutes are stopped, and any owner idle for
12 hours is stopped. Both numbers are parameters of `AggregateCommandBusActor(...)`:
`keepAliveLimit` (200) and `maxInactivityMillis` (12 hours).

### Sharded: one owner per aggregate in the whole cluster

A context opts in by overriding `placement`. E2 makes E1's carts sharded:

<!-- snippet: e2/ShardedCarts.scala#sharded -->
```scala
class ShardedCartContext extends ShoppingCartContext {
  override val placement: Placement = Placement.Sharded(numberOfShards = 30) // about ten per node of three
}
```

That is the whole change in your code. Now:

- **There is one owner per cart in the whole cluster**, placed by Pekko Cluster Sharding. Every
  node's command bus starts the type's shard region, and sends the owners everything through it.
  A command or a query sent to the cart command bus of any node reaches the one owner, wherever it
  runs.
- **An aggregate's shard** comes from its id (the id modulo `numberOfShards`). Pekko spreads the
  shards over the nodes and moves them when nodes come and go. The default is 100 shards; aim for
  about ten shards per node of the largest cluster you plan.
- **Idle owners are passivated by their shard** after 120 seconds without a message (Pekko's
  `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout`).
- **The actor system must be a cluster** (`pekko.actor.provider = cluster`), of one node or more.
- **Commands travel** to the owner's node, so they must fit a message between nodes (see "Messages
  between nodes" below).

`PassivateAggregate(id)`, sent to the command bus of any node, puts an aggregate's owner to sleep in
both placements.

Here is what "one owner" buys you, from `ShardedOwnersSpec`: three nodes send 15 concurrent
`AddItemConcurrently` commands each to one cart. All 45 succeed, the cart ends at version 46, and
exactly one node runs its owner.

### How to choose

| | Local | Sharded |
|---|---|---|
| Owners of one aggregate | one on each node asked about it | one in the whole cluster |
| Two writers at once | possible; the version check decides | not in normal running |
| Extra cost per read or command | one key lookup, on a node that is not alone | a trip to the owner's node, when it runs elsewhere |
| Idle owners | swept by the command bus | passivated by the shard after 120 s |
| Commands must fit a message between nodes | no | yes |

A simple rule:

- **One node:** stay with `Local`. Alone in its cluster, an owner needs no lookups.
- **Several nodes, a type that is read or written often:** make it `Sharded`.
- **Several nodes, a type that is rarely touched:** `Local` is fine.

> **Warning:** changing a type's placement, or its `numberOfShards`, needs a full stop of the whole
> cluster, never a rolling upgrade. While old and new nodes run side by side, an aggregate would have
> a Local owner *and* a sharded one, and sharded owners do not check the store before they answer.
> You can move types to `Sharded` one at a time, each with a full stop.

> **Under the hood:** the owners' side of placement is `Owners.scala` (`LocalOwners`,
> `ShardedOwners`); messages to sharded owners travel as `ToOwner(aggregateId, message)`. See
> [ARCHITECTURE.md, section 13.3](../ARCHITECTURE.md) (D43, D72, F131, F145).

## When a node leaves

A cook who goes home first finishes the dish in the pan, then hands the tickets over. A sharded owner
does the same when its node leaves the cluster (or when Pekko moves its shard to balance the load):

1. It finishes the commands it is running (with serial processing, the queue drains).
2. It answers the queries that wait for it, and writes its snapshot if the type takes snapshots on
   passivation ([chapter 13](13-snapshots.md)).
3. It stops. Messages for its shard wait in the sending node's shard region (Pekko buffers up to
   100 000 messages per region) and reach the new owner on another node.

Finishing takes time, so the library's `reference.conf` gives it time:

```hocon
pekko.cluster.sharding.handoff-timeout = 90 s
pekko.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout = 90 s
```

Both are above the default command timeout of 60 seconds (`AggregateContext.commandTimeout`). Pekko
stops an owner forcefully 5 seconds before `handoff-timeout`.

> **Warning:** your process manager must wait *longer* than these 90 seconds before it kills a node
> that is leaving (on Kubernetes: `terminationGracePeriodSeconds`). If you raise a context's
> `commandTimeout`, raise both settings above it, and the grace period above them.

A command that waited in a buffer during a hand-over runs later than it was sent, and its 60 seconds
start when it reaches the new owner. A caller with a short timeout may therefore give up on a command
that is still applied. The cure is the one from [chapter 5](05-command-flavors.md): **retry with
an idempotency key**, and the command takes effect once, however often it was sent.

`ShardedOwnersSpec` checks this the hard way: a node leaves while one of its owners runs a command
that takes twelve seconds, and ten carts whose owners run there get five items each, one every 150
milliseconds, through the other nodes. Every command succeeds, the slow one included, and every item
is in its cart once.

What if a node does not leave politely, but vanishes? The split brain resolver notices and removes it
from the cluster, and its shards start again on the remaining nodes. For a short moment two owners of
one aggregate could exist; the database's version check still lets only one of them write each
version.

> **Remember:** the library's own multi-node tests run several nodes inside one JVM, and cover nodes
> that leave gracefully. Tests that kill a node or cut the network are planned before the 1.0
> release. Until then, test such failures in your own environment.

## Messages between nodes

Inside one node, a message is handed over like a note across the desk. Between nodes it must be
written down, put in an envelope and posted: **serialized**. And the envelope has a size limit.

### One serializer for everything

The library's `reference.conf` binds one serializer, `ClusterSerializer`, to `java.io.Serializable`,
so the case classes and Scala collections that cross nodes go through it (Pekko keeps a few of its
own, such as `Some` and `None`). Java serialization stays off (Pekko's default). You configure
nothing.

- The library's own messages are written field by field; the events in deliveries take about 33
  bytes each besides their JSON.
- **Your values** (commands, events, aggregate states, query messages and their answers, saga orders
  and responses) are written as the JSON of their class, by mpjsons, just as events are stored.
- The library's exceptions, such as `AggregateInIncorrectVersionException`, keep their class. Any
  other exception arrives as a `RemoteFailure`, with the original class name and message.

That gives your classes four rules:

1. **mpjsons must be able to write and read them by their class:** no fields typed `Any`, a trait,
   an `ActorRef` or a `Throwable`.
2. **Define messages at the top level or in an object, never inside an actor class.** A class
   defined inside another one carries its enclosing instance, which cannot travel to another node.
3. **A field you add later must be an `Option`.** During a rolling upgrade a node of the older
   version sends the message without the new field, and the reader fills a missing field with
   `None`, zero or `null`, never with the field's Scala default value. Read `{"id":1}` as an
   `Item(id: Int, name: String)` and you get `Item(1, null)`.
4. **A field that changes its type needs a new class.**

> **Tip:** a round-trip test of your messages through the serializer takes a few lines;
> [chapter 19](19-testing.md) shows one. If you registered mpjsons converters of your own, give the
> cluster serializer an instance configured the same way, through
> `ClusterSerialization(system).mpjsons`, before the nodes exchange messages.

### How big a message may be

Pekko's transport (Artery) sends a message in one frame, of 256 KiB (a quarter of a megabyte) by
default, set by `pekko.remote.artery.advanced.maximum-frame-size`. Pekko drops a larger message and
only logs "Discarding oversized payload"; the sender is never told.

The library's limit, `MessageSize(system).limit`, is that frame less 8 KiB for Pekko's own header:
248 KiB by default. Here is what happens to each kind of message that could be too large:

| Message | When it crosses nodes | Too large? |
|---|---|---|
| a command of a `Sharded` type | its owner runs on another node | refused at once with `FailureResponse` ("too large to be sent between nodes"), wherever its owner runs |
| a command's answer, a `GetAggregate` answer (the state) | the owner runs on another node | the asker fails at once with `MessageTooLargeException` |
| a projection's query answer | the projection runs on another node | the asker fails at once with `MessageTooLargeException` |
| a delivery of several events | the subscriber runs on another node | split into parts of at most half the limit |
| a single event, or a state alone, in a delivery | the subscriber runs on another node | fails at the subscriber |
| `GetEventsForAggregate` (the whole history) | never | read on the asking node, from the store |

Every message that is too large is logged on the sending node with its class and size (at most once
per class every 10 seconds), and counted in the metric `reactivecqrs.message.too.large`
([chapter 18](18-production.md)).

For a feel of the sizes, measured on 17/10/2026 (the `load-M5` report in `docs/perf/`): one event's
delivery took 561 bytes; a re-publication part of 1 000 events with 200-character notes took
470 KiB, which is why such parts are split.

What to do about it:

- **Keep commands, answers, states and query answers well under 248 KiB.** A state that subscribers
  take with every event travels with every event.
- **Read a large state on the asking node** with `GetAggregateAtInstant(id, Instant.now)`: the
  command bus restores it from the store right there, instead of the owner sending it across.
- **If larger messages are unavoidable**, raise `pekko.remote.artery.advanced.maximum-frame-size`.
  Pekko keeps up to 128 buffers of the frame size in direct memory, so 1 MiB frames can take up to
  128 MiB (estimated by that arithmetic). Or route the large messages' destinations through Pekko's
  `large-message-destinations` (2 MiB frames, 32 buffers by default).

[Chapter 18](18-production.md) has more on large events and documents.

> **Under the hood:** the serializer, its tolerant reading, and the size guard are described in
> [ARCHITECTURE.md, section 13.1](../ARCHITECTURE.md) (D45, D68, F121, F135); the measured sizes
> and recommendations are in [SCALING.md, section 6](../SCALING.md).

## Is this node ready?

A node that has just started should not take traffic before its event bus knows all its
subscribers: until every registered subscriber has answered (the projections behind their proxies
included), the bus publishes nothing.

`Readiness(system)` keeps track of that:

<!-- snippet: cart/NodeVersionsSpec.scala#readiness -->
```scala
import io.reactivecqrs.core.cluster.Readiness

Readiness(system).ready        // false while an event bus still waits for its subscribers
Readiness(system).waitingFor   // the event buses that still wait
```

If you use Pekko Management's health checks (add Pekko Management to your application; the library
does not bring it), one line makes it a readiness check, so load balancers send traffic only to ready
nodes:

```hocon
pekko.management.health-checks.readiness-checks {
  reactivecqrs = "io.reactivecqrs.core.cluster.ReadinessCheck"
}
```

`Members(system)` tells you what this node knows of the cluster's members:

<!-- snippet: cart/NodeVersionsSpec.scala#members -->
```scala
import io.reactivecqrs.core.cluster.Members

val members = Members(system)
members.oldestAppVersion   // the lowest app-version among the members, e.g. Some(Version("1.0.0"))
members.olderVersionsRun   // true while a member runs an older version than this node
members.alone              // true outside a cluster, and in a cluster of one
```

## Rolling upgrades

Changing the cooks one at a time during dinner service is fine, as long as nobody writes a new dish
on the order tickets before every cook can read it. That is the whole idea of a rolling upgrade.

The steps:

1. **Give every release its own app-version**, on every node of that release:
   `pekko.cluster.app-version = "2.0.0"`.
2. **Start the nodes of the new version.** They join the cluster. New shards go only to the nodes of
   the newest version, so work moves from old nodes to new ones, never back.
3. **Stop the old nodes one at a time, the oldest last.** The things that run once live on the oldest
   node, so they move only once.
4. **Keep the clients retrying.** A client sends to nodes that are up; a command that gets no answer
   in time is sent again through another node, with the same idempotency key.

### A new event class: write it only when every node can read it

Say version 2.0.0 of the carts records where each item came from, with a new event class. Nodes of
1.0.0 have never heard of it. If a 2.0.0 node stored one while a 1.0.0 node still runs, the old node's
owners and projections could not read it.

So you tell the context from which version every node can read the new class, and the handler writes
the old event until then. Here is version 2.0.0 of the test domain's carts (`CartsV2.scala`, in
`examples/src/test/scala/io/reactivecqrs/examples/cart/`; `NodeVersionsSpec` runs it on nodes of
both versions, and the cluster specs' `ShardedCartContextV2` does the same for sharded carts):

<!-- snippet: cart/CartsV2.scala#contextV2 -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._

case class ItemAddedV2(name: String, source: String) extends Event[ShoppingCart]

class ShoppingCartContextV2 extends ShoppingCartAggregateContext {
  override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

  override def commandHandlers = cart => {
    case c: AddItemConcurrently if everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
    case other => super.commandHandlers(cart)(other) // ItemAdded, as in 1.0.0
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case ItemAddedV2(name, _) => EventsHandlers.itemAdded(cart, ItemAdded(name))
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
```

What happens during the upgrade:

- While a node of 1.0.0 is a member, `everyNodeRuns("2.0.0")` is false and the handler writes
  `ItemAdded`, as before.
- If something tries to store an `ItemAddedV2` anyway, the owner answers the command with
  `CommandRejected` (send it again later); a squash whose squash event is of such a class is answered
  with `SquashRefused`.
- Once the last 1.0.0 node has left, `ItemAddedV2` is written.

Ship the event handlers *and* the projections' handling of the new class in the same release that
lists it in `writtenFrom`.

`RollingUpgradeSpec` does all of this under load: two nodes of 1.0.0, two of 2.0.0 joining later,
commands from every node. No `ItemAddedV2` is stored while an old node is a member; it is written once
both old nodes are gone; no command is lost or applied twice, and the projection has every item once.

A few more rules of the road:

- **Set app-versions before you rely on `writtenFrom`.** When no member set one (all at Pekko's
  `0.0.0`), every class is written at once; a member without one counts as the lowest version, so
  nothing new is written while it runs.
- **A node that does not know its cluster yet writes nothing new.**
- **Never roll back past a version whose new event classes were written.** The older version cannot
  read them.
- **A new command class:** send it only once every node runs its version, as an owner on an old node
  would not know it.
- **A projection removed in the new version:** `Projections.retire(name)` waits until every member
  runs that version before it drops the projection (it looks again every 10 seconds).
- **Placement changes** still need a full stop (see above).
- **A cluster that runs a milestone build** (1.0.0-M5 or -M6) moves to 1.0.0 with a full stop: 1.0
  writes the events of a delivery in a form those builds cannot read.

> **Under the hood:** app-versions, the write switch, readiness and the rolling-upgrade test are
> described in [ARCHITECTURE.md, section 13.6](../ARCHITECTURE.md) (D65–D73, F132, F138).

## Three nodes for real: Docker Compose and Kubernetes

E2 comes with what it takes to run its nodes as separate processes, in `examples/deploy/e2/`:

| File | What it is |
|---|---|
| `Dockerfile` | an image with a Java 21 runtime and the examples' jars, which runs E2's `ClusterSetup` |
| `docker-compose.yml` | three nodes and a PostgreSQL on one machine |
| `kubernetes.yaml` | three nodes on Kubernetes, with the split brain resolver's lease |

Every node runs the same `ClusterSetup.main`, and takes what differs from node to node from its
environment: `NODE_HOST` (its address), `CONTACT_POINTS` (the nodes it asks first, with Docker
Compose), `REQUIRED_CONTACT_POINTS` (how many must answer before a new cluster forms), `APP_VERSION`,
the database (`DB_URL`, `DB_USER`, `DB_PASSWORD`) and `SHOP_CONFIG`, the configuration to start
from:

<!-- snippet: e2/ClusterSetup.scala#config -->
```scala
/** A node's configuration: `e2.conf` (or `e2-kubernetes.conf`, as SHOP_CONFIG says), with this node's address, its
  * contact points for Cluster Bootstrap, how many of them must answer before a new cluster forms, and the release's
  * version, from the environment. Alone, a node is its own contact point: a cluster of one. */
def nodeConfig(setting: String => Option[String]): Config = {
  val host = setting("NODE_HOST").getOrElse("127.0.0.1")
  val contactPoints = setting("CONTACT_POINTS").fold(Seq(host))(_.split(",").map(_.trim).toSeq)
  val required = setting("REQUIRED_CONTACT_POINTS").getOrElse(contactPoints.size.toString)
  ConfigFactory.parseString(
    s"""pekko.remote.artery.canonical.hostname = "$host"
       |pekko.management.http.hostname = "$host"
       |pekko.discovery.config.services.shop.endpoints = [${contactPoints.map(p => s"""{host = "$p", port = 8558}""").mkString(", ")}]
       |pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr = $required
       |pekko.cluster.app-version = "${setting("APP_VERSION").getOrElse("1.0.0")}"
       |""".stripMargin)
    .withFallback(ConfigFactory.load(setting("SHOP_CONFIG").getOrElse("e2")))
}
```

The two configurations are in `examples/src/main/resources`:

- **`e2.conf`**, for Docker Compose: the cluster provider on port 17355, the split brain resolver
  with `keep-majority` (three nodes always have a majority), `coordinated-shutdown.exit-jvm = on` (a
  node that leaves the cluster, or is downed, ends its process, and the container is started again),
  Pekko Management on port 8558 with its health checks (the library's `ReadinessCheck` among them),
  and Cluster Bootstrap with a fixed list of contact points.
- **`e2-kubernetes.conf`**, for Kubernetes: the same, with the contact points found through the
  Kubernetes API (the pods labelled `app=shop`), and `lease-majority`: the side of a split that
  holds a Kubernetes Lease stays, also with two nodes, where there is no majority. The lease is a
  native Kubernetes `Lease` object named after the actor system (`shop-pekko-sbr`), from Pekko
  Management's `pekko-lease-kubernetes`.

`main` starts Pekko Management, then `ClusterBootstrap(system).start()`, and no seed nodes are set:
with seed nodes, Bootstrap does not run. A starting node asks every contact point it finds which
cluster it belongs to, and joins the one they report. Only when the required contact points, all of
them, report none do they form a new cluster, the one with the lowest address first.

The Kubernetes manifest has four parts worth knowing:

1. **A service account with a role**: `get`, `watch` and `list` on `pods`, so Bootstrap finds the
   contact points, and `get`, `create`, `update` and `list` on `leases` of
   `coordination.k8s.io`, so the split brain resolver can take the lease. Without the lease, in a
   split, no side stays.
2. **The pod's IP as the node's address** (`NODE_HOST` from `status.podIP`): a restarted pod is a new
   node at a new address, and no other node holds an old one in a cache.
3. **A StatefulSet of three pods, started in parallel**: forming the first cluster needs the three
   contact points. A rolling update still replaces one pod at a time, the newest first, each once the
   one before is ready.
4. **Probes on Pekko Management**: `/ready` keeps traffic away until the node's event bus knows its
   subscribers; `terminationGracePeriodSeconds: 120` gives a leaving node time to hand its owners
   over (90 seconds, above).

On Docker Desktop's Kubernetes, E2's three pods formed one cluster this way, kept it through a
rolling restart, and when one node stopped answering (its process frozen, not stopped), the split
brain resolver took the lease and downed it; the node, thawed, found itself downed, ended, and joined
again when its container restarted.

> **Warning:** a local cluster keeps an image it pulled once. After building a new image, give it a
> new tag, or the pods go on running the old one.

To try it on one machine, from the repository's root:

```bash
sbt examples/stageE2
docker compose -f examples/deploy/e2/docker-compose.yml up --build
```

`stageE2` copies the examples' jars, with every library they need, to `examples/target/e2/lib`, and
the image runs them with `java -cp "/app/lib/*"`: no sbt, and no repository to reach, in the image.
Each node prints the cart it made and what the carts list, wherever it runs, says about it. Stop
`node1`, the oldest, and the carts list and the outbox poller move to `node2`; restart `node3`, and
it makes a cart again, read from `node2`.

> **Remember:** `java -cp "lib/*"` puts the jars on the classpath in no particular order, which is
> exactly why `e2.conf` binds the serializer itself (the first item of this chapter's configuration).

## What you learned

- Every installation runs as a Pekko cluster, of one node or many, and all nodes share one
  PostgreSQL database whose version check guards every write.
- There is an event bus per node. What must see every event runs once (`Singletons.start`,
  `Projections.start`), and its proxy is registered on every node's bus.
- A type's owners are `Local` (one per node, the database decides) or `Sharded` (one in the cluster);
  make busy types `Sharded` in a cluster of several nodes, and change placement only with a full stop.
- A leaving node waits up to 90 seconds for the commands at work; your process manager must wait
  longer, and clients retry with idempotency keys.
- Messages between nodes go through one serializer: keep your classes mpjsons-friendly, add fields as
  `Option`s, and keep messages well under 248 KiB.
- Rolling upgrades: an app-version per release, the oldest node last, and `writtenFrom` with
  `everyNodeRuns` for new event classes.

Next: [chapter 18](18-production.md) takes all of this to production: the start-up checklist,
watching the system run, and what to do under heavy load.
