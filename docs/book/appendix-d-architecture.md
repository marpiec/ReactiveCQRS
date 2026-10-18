# Appendix D. The Architecture in Pictures

The chapters explain ReactiveCQRS one piece at a time. This appendix puts the pieces side by side, as
diagrams: what runs where, what happens to a command, how an event reaches a read model, what is in
the database, how a read model is built again, and how the nodes of a cluster share the work. GitHub
draws them from their text; elsewhere, paste a block into any Mermaid viewer.

Each diagram names the chapter that explains it. [ARCHITECTURE.md](../ARCHITECTURE.md) has the full
story, for contributors.

---

## The parts

Your application gives the library its aggregate contexts and read models, and creates a handful of
actors when it starts ([chapter 2](02-setting-up.md)). Everything durable lives in one PostgreSQL
database.

```mermaid
flowchart LR
  subgraph app["Your application"]
    caller["Callers: REST, UI, jobs"]
    contexts["Aggregate contexts<br/>(rules and events)"]
    readmodels["Projections<br/>(read models)"]
    sagas["Saga definitions"]
  end
  subgraph node["The library, on each node"]
    uid["UidGeneratorActor"]
    bus["EventsBusActor"]
    cmdbus["AggregateCommandBusActor<br/>one per aggregate type"]
    owner["AggregateOwner<br/>one per aggregate"]
    poller["OutboxPollerActor<br/>once in the cluster"]
    supervisor["Projection supervisors<br/>(Projections.start)"]
    runner["SagaRunner"]
  end
  db[("PostgreSQL<br/>events, outbox,<br/>subscriptions, documents")]

  caller -- commands, queries --> cmdbus
  caller -- queries --> supervisor
  cmdbus --> owner
  contexts -.->|used by| owner
  owner -- commit --> db
  owner -- PublishEvents --> bus
  bus -- deliveries --> supervisor
  readmodels -.->|run in| supervisor
  supervisor -- documents --> db
  poller -- re-publish --> bus
  poller -- reads the outbox --> db
  sagas -.->|run by| runner
  runner -- commands --> cmdbus
  uid -- id pools --> cmdbus
```

## A command, from the caller to the read model

What chapter 4 tells in words: one transaction stores the events and their outbox rows, the caller
gets the new version, and the read model gets the events after the commit.

```mermaid
sequenceDiagram
  autonumber
  participant C as Caller
  participant B as Command bus
  participant O as Aggregate owner
  participant DB as PostgreSQL
  participant E as Event bus
  participant P as Projection

  C->>B: AddItem(cartId, expectedVersion 1)
  B->>O: CommandEnvelope (creates the owner when needed)
  O->>O: version check, then the command handler
  O->>DB: one transaction: events + outbox rows (+ the idempotent response)
  DB-->>O: committed, version 2
  O-->>C: SuccessResponse(cartId, 2)
  O->>E: PublishEvents(events, state)
  E->>P: the events, or the state, or both
  P->>DB: one transaction: the document + the subscription's version
  P-->>E: MessageAck
  E-->>B: PublishEventsAck
  B->>DB: deletes the outbox rows (in batches)
```

## Delivery: acknowledged, failed, missing or late

A projection answers every delivery ([chapter 9](09-projection-failures.md)). What it does not
acknowledge stays in the outbox, and the outbox poller has it published again.

```mermaid
sequenceDiagram
  participant E as Event bus
  participant P as Projection
  participant O as Owner
  participant Q as Outbox poller

  E->>P: versions 5–6 of cart 42
  alt applied
    P-->>E: MessageAck
  else the listener keeps failing (about 2 minutes of retries)
    P-->>E: MessageFailed (recorded in subscription_failures)
  else version 4 never arrived (a gap)
    P->>E: RequestRepublish(cart 42, from 4)
    E->>O: RepublishRange
    O->>E: versions 4–6 again
    E->>P: versions 4–6
    P-->>E: MessageAck
  end
  Note over E: no answer by the deadline (10 minutes): "undelivered", the outbox row stays
  Q->>E: Republish after 3 and 6 minutes, then every 30 minutes
```

## The sequence gate

Each projection keeps the last version it applied per aggregate (and listener kind). Every delivery
goes through one decision ([chapter 8](08-projections.md)):

```mermaid
flowchart TD
  d["Delivery: versions from–to"] --> q1{"to ≤ applied?"}
  q1 -- yes --> ack["Acknowledge: already applied"]
  q1 -- no --> q2{"from ≤ applied + 1,<br/>or it starts at a squash event?"}
  q2 -- yes --> apply["Apply the new versions, in one transaction<br/>with the subscription's new version"]
  q2 -- no --> gap["Wait: a gap. Ask the owner for the missing versions"]
  apply --> ok{"Listener succeeded?"}
  ok -- yes --> acked["MessageAck"]
  ok -- "no, after its retries" --> failed["Record the failure, MessageFailed;<br/>later versions of the aggregate wait"]
```

## What is in the database

The tables the library writes ([chapters 2](02-setting-up.md), [10](10-rebuilding-projections.md) and
[18](18-production.md)). Your read models add one `projection_<name>` table per document store.

```mermaid
erDiagram
  aggregates ||--o{ events : "has"
  events ||--o| noop_events : "undone by"
  events ||--o| events_to_publish : "waits in the outbox"
  events ||--|| event_keys : "type and space"
  aggregates ||--o| snapshots : "latest state"
  aggregates ||--o{ events_archive : "kept by a squash"
  aggregates ||--o{ subscriptions : "applied versions"
  subscriptions ||--o| subscription_failures : "failed at"
  aggregates ||--o{ commands_responses : "idempotent answers"
  projections ||--|{ projection_builds : "active and building"
  projection_builds ||--o{ projection_build_objects : "tables of a build"

  aggregates {
    bigint id
    bigint base_id "duplication chain"
    bigint space_id "organization"
    int base_version "current version"
    int squashed_version
  }
  events {
    bigint id
    bigint aggregate_id
    int version
    text event "JSON"
    jsonb metadata
  }
  events_to_publish {
    bigint event_id
    bigint aggregate_id
    int version
  }
  subscriptions {
    smallint subscriber_type_id
    smallint subscription_type
    bigint aggregate_id
    int aggregate_version
  }
  sagas {
    varchar name
    bigint saga_id
    varchar phase
    int step
    text saga_order
  }
```

## Copies, detaching and squash

A copy (`DuplicationEvent`, [chapter 6](06-events.md)) reads its base's history up to the version
it was made at. A squash ([chapter 14](14-squash.md)) replaces the oldest events with one squash event;
a copy that still reads them is detached first: it gets a first event of its own.

```mermaid
flowchart LR
  subgraph before["Before the squash"]
    b1["List A: v1 … v6"]
    c1["Copy B: reads A's v1–v3,<br/>then its own v4, v5"]
    b1 -.->|history up to v3| c1
  end
  subgraph after["After squashing A up to v5"]
    b2["List A: squash event at v5, then v6"]
    c2["Copy B: detached, its own v1 holds its state at v3;<br/>then v4, v5 as before"]
  end
  before --> after
```

## Building a read model again

A managed projection builds a new version in tables of its own while the active build serves, then
switches in one update ([chapter 10](10-rebuilding-projections.md)):

```mermaid
stateDiagram-v2
  [*] --> Waiting: a new version, a changed context, or RebuildProjection
  Waiting --> Bulk: a build slot is free
  Bulk --> GoingLive: the store is read
  GoingLive --> Reconciling: live deliveries applied
  Reconciling --> CaughtUp: every aggregate's version matches
  CaughtUp --> Switched: no failed subscription
  CaughtUp --> CaughtUp: failed subscriptions, retried or skipped with ToBuildInProgress
  Switched --> [*]: the old build leaves the buses, its tables are dropped
  Bulk --> Waiting: stopped (resumes from the saved position)
```

## A cluster of three nodes

Every node runs a uid generator, an event bus and a command bus per type. What must run once runs on
the oldest node, and every node reaches it through a proxy ([chapter 17](17-cluster.md)).

```mermaid
flowchart TB
  subgraph A["Node A (oldest)"]
    busA["Event bus"]
    cartsA["Carts command bus<br/>+ shard region"]
    ownersA["Owners of some shards"]
    single["Singletons: projection supervisors,<br/>outbox poller, squash scheduler, saga runners"]
  end
  subgraph B["Node B"]
    busB["Event bus"]
    cartsB["Carts command bus<br/>+ shard region"]
    ownersB["Owners of other shards"]
    proxyB["Proxies to the singletons"]
  end
  subgraph C["Node C"]
    busC["Event bus"]
    cartsC["Carts command bus<br/>+ shard region"]
    ownersC["Owners of the rest"]
    proxyC["Proxies to the singletons"]
  end
  db[("PostgreSQL")]

  cartsA --- ownersA
  cartsB --- ownersB
  cartsC --- ownersC
  cartsB -.->|a command for a shard on A| ownersA
  ownersA --> busA
  ownersB --> busB
  ownersC --> busC
  busA --> single
  busB --> proxyB --> single
  busC --> proxyC --> single
  ownersA & ownersB & ownersC --> db
  single --> db
```

## A rolling upgrade

New nodes join, old ones leave one at a time, the oldest last, so what runs once moves once. A new
event class is written only when every node can read it ([chapter 17](17-cluster.md)).

```mermaid
sequenceDiagram
  participant Old1 as Node 1 (1.0, oldest)
  participant Old2 as Node 2 (1.0)
  participant New3 as Node 3 (2.0)
  participant New4 as Node 4 (2.0)

  New3->>Old1: joins (app-version 2.0)
  New4->>Old1: joins
  Note over New3,New4: new shards go only to 2.0 nodes,<br/>and handlers still write the event classes of 1.0
  Old2->>Old2: leaves: hands its shards over
  Old1->>Old1: leaves last: shards and singletons move to Node 3
  Note over New3,New4: every member runs 2.0:<br/>everyNodeRuns("2.0") is true, new event classes are written
```

## Moving an organization

An organization's events leave one installation as a file and arrive in an empty space of another,
under ids of its own ([chapter 16](16-organizations.md)):

```mermaid
flowchart LR
  subgraph source["Installation A"]
    spaceA["Space 7: aggregates, events,<br/>archived events"]
  end
  file[/"JSON lines file:<br/>header, aggregates, events<br/>(classes by name)"/]
  subgraph target["Installation B"]
    check["1. Check: format, empty space,<br/>every class, every size; new ids for taken ones"]
    write["2. Write: aggregates, then events in commit order,<br/>ids and space mapped"]
    rebuild["3. Rebuild the space's read models:<br/>RebuildAggregates(OneSpace(target))"]
  end
  spaceA -- "SpaceExport.write" --> file
  file -- "SpaceImport.read" --> check --> write --> rebuild
  spaceA -.->|"afterwards, if it moved for good: SpacePurge"| purge(["Space 7 removed"])
```

---

Next: back to [the contents](README.md).
