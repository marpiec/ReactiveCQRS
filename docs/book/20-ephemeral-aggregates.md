# Chapter 20. Ephemeral Aggregates: Drafts That Live in Memory

Not everything deserves a place in the diary. A customer fills a cart and walks away; a manager
plays with a draft budget and closes the tab. Storing every one of those clicks forever, with
projections and sagas hearing about each, is a lot of ink for scratch paper. By the end of this
chapter you will be able to run aggregates that live only in one machine's memory, keep the ones
that turn out to matter, and let the others fade away.

---

## Scratch paper

An **ephemeral aggregate** is an ordinary aggregate (your context, your commands, your events, your
rules) that lives in the memory of one node only. Nothing is written to PostgreSQL, nothing is
published to projections or sagas, and when the node stops, it is gone. Think of a pencil draft on
scratch paper: you can change it as often as you like, and when it is good you copy it, cleanly,
into the diary. That copy is called **materializing**.

## Step 1: an ephemeral command bus

Ephemeral aggregates have a command bus of their own, next to the usual one for the same type. E8,
the example of this chapter, keeps drafts of the shopping lists of chapter 13's E5:

<!-- snippet: e8/DraftsSetup.scala#buses -->
```scala
val lists = system.actorOf(
  AggregateCommandBusActor(new ShoppingListContext, uidGenerator, eventStore, responses, eventBus), "lists")
val drafts = system.actorOf(
  AggregateCommandBusActor.ephemeral(new ShoppingListContext, persistentBus = lists, uidGenerator, mpjsons), "draftLists")
```

It takes the same context, the persistent bus of the type (that is where materialized lists go), the
id generator (for command ids) and your `MPJsons` (to check event sizes, below). It has no event
store, response state or event bus parameter: it keeps all of those in memory, for itself.

## Step 2: use it like any other bus

Send it the very same commands:

<!-- snippet: e8/DraftsSetup.scala#draft -->
```scala
val draft = send(buses.drafts, CreateList(user, "Maybe this weekend")).asInstanceOf[SuccessResponse] // a negative id
send(buses.drafts, WriteItem(user, draft.aggregateId, draft.aggregateVersion, "tent"))
send(buses.drafts, WriteItem(user, draft.aggregateId, AggregateVersion(2), "stove"))
```

Notice the **negative id** of the draft's `SuccessResponse`: that is how you, and your user interface,
can tell a draft from a stored list. Everything else works as you know it: your command handlers
decide, your event handlers change the state, a stale `expectedVersion` gets
`AggregateConcurrentModificationError`, `GetAggregate` answers the state, and even
`GetEventsForAggregate` shows the draft's events.

> **Remember:** the ephemeral bus checks what PostgreSQL would check: an event's JSON may not be
> longer than the limit for its class, and an aggregate may not have more events than the version
> limit ([chapter 6](06-events.md)). A draft that works in memory can therefore always be stored.

## Step 3: keep it, with Materialize

When the list turns out to be worth keeping:

<!-- snippet: e8/DraftsSetup.scala#keep -->
```scala
val kept = Await.result((buses.drafts ? Materialize(draft.aggregateId, user)).mapTo[MaterializeResponse], timeout.duration)
```

The answer is `Materialized(draftId, persistentId)`, the draft's negative id and the new list's id.
The library takes the draft's current state, turns it into the type's **squash event**
([chapter 14](14-squash.md)), checks that the event gives exactly that state back, and has the
persistent bus store it as the first event of a brand-new list. The draft is then dropped: commands
for its negative id get `EphemeralAggregateUnknown`. From now on you work with the new id on the
persistent bus, and projections and sagas hear about it like any new list. `DraftsSpec` checks all
of it: nothing of the draft in `events`, the stored list at version 1 with both items, and the draft
gone.

A few details that keep it safe:

- While the materialization runs, commands for the draft are answered `CommandRejected`, so what is
  stored is exactly what the caller saw. Send them again to the new list.
- If commands are still running on the draft, or the type has no squash event, the answer is
  `MaterializeRefused(id, reason)` and the draft stays as it was.
- The materialization is idempotent: should an answer come late and the request be sent again, the
  library recognizes it and answers the same new id instead of storing a second list.

> **Tip:** the stored list starts at version 1, with one summary event: the draft's clicks are not
> kept. That is the point of scratch paper. If you need the clicks, they belong in the diary from the
> start.

## Step 4: let it go

Drafts nobody touches do not pile up forever:

- **Idle time**: a draft unused for `idleTime` (12 hours by default) is dropped. The bus never drops
  one sooner than the context's command timeout, so a command that is still running cannot write
  into a draft that is gone.
- **A maximum**: the bus holds at most `maxAggregates` drafts (100 000 by default); a new one beyond
  that is answered `CommandRejected`.

<!-- snippet: e8/DraftsSpec.scala#limits -->
```scala
AggregateCommandBusActor.ephemeral(new ShoppingListContext, persistentBus = lists, uidGenerator, mpjsons,
  maxAggregates = 10000, idleTime = 2.hours)
```

## When a draft goes missing

Memory is fragile, and the library says so plainly:

| Answer | Means |
|---|---|
| `EphemeralAggregateLost(id)` | The draft is not on this node: the node restarted since it was made, or the command reached another node. |
| `EphemeralAggregateUnknown(id)` | This node never made it, dropped it while idle, or materialized it. |

Queries fail the same way: `GetAggregate` answers `Failure(EphemeralAggregateLostException)` or
`Failure(NoEventsForAggregateException)`.

> **Warning:** in a cluster ([chapter 17](17-cluster.md)), a draft lives on the node whose ephemeral
> bus made it. Send every command for it to that node, for example by keeping the user's session on
> one node. A node that restarts loses its drafts; tell your users, or materialize early.

> **Under the hood:** each start of an ephemeral bus draws a random token, and its ids are
> `-(token × 2^24 + n)`. So an id from before a restart, or from another node, is recognized as lost,
> two nodes almost never share ids (once in 2^29 pairs of starts), and every id stays a number a
> JavaScript user interface can hold (below 2^53). See [ARCHITECTURE.md, section 4.10](../ARCHITECTURE.md).

## What drafts do not do

- They are never published, so projections and sagas know nothing of them until they are
  materialized.
- They are not squashed, copied or rewritten: materialize them first.
- They do not survive a restart. Nothing about them is in the database.

## What you learned

- `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons)` runs your usual
  aggregates in one node's memory, with negative ids, writing and publishing nothing.
- They answer commands exactly as stored aggregates do, with the same size and version limits.
- `Materialize(id, userId)` stores a draft as a new aggregate, from one squash event of its state,
  and drops the draft; it is refused without a squash event or while commands run.
- Unused drafts are dropped after `idleTime`, at most `maxAggregates` are held, and a lost or unknown
  draft is answered as such.

That is the end of the chapters. The appendices are for looking things up:
[the glossary](appendix-a-glossary.md),
[the cheat sheet of messages and answers](appendix-b-cheat-sheet.md), and
[troubleshooting](appendix-c-troubleshooting.md).
