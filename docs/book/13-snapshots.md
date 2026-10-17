# Chapter 13. Keeping Aggregates Fast with Snapshots

By the end of this chapter you will know why an aggregate with a long history is slow to wake up,
how one line in its context makes it quick again, when the library throws a snapshot away, and
what stops an aggregate from growing forever. Snapshots are the gentlest feature in this part of the
book: they change nothing in your history, and you can turn them off again at any time.

---

## Why long histories get slow

Back to the diary of chapter 1. To know the current shopping list, you read the diary from the top
and replay every line. For one page that takes a moment. For a diary of a thousand pages, you will
want a cup of coffee first.

An aggregate owner does exactly that. While an aggregate is busy, its owner keeps the state in
memory, and commands are quick. But whenever an owner starts, it **restores** the aggregate by
replaying every one of its events:

- on the first message for the aggregate after the application starts;
- after the owner was stopped for being idle, to free memory: by default a node stops the owners
  beyond its 200 most recently used once they have been idle for 2 minutes, and any owner idle for
  12 hours;
- after a hand-over to another node of a cluster (chapter 17);
- after something went wrong and the state had to be read again from the store.

The cost grows with the history. Measured on 17/10/2026 (the bench's `load-M5` run, one run on one
machine: Windows 10, 12 CPU threads, OpenJDK 17 with a 2 GB heap, PostgreSQL 17.4 on the same
machine), a new owner restoring an aggregate of 50 000 events, each with a 200-character note, took
392 ms, and the heap went from 17 to 52 MB meanwhile. A command after the restore took 3 ms.

So commands do not get slower as the history grows: once restored, the owner keeps the state, not
the events. It is the waking up that gets slow. And while an owner restores, it holds one of the
blocking dispatcher's 32 threads and a database connection, so every other owner on the node waits
a little longer too.

## A bookmark with a note

Now picture a bookmark in the diary at page 200, with a sticky note: "as of here, the list is
bread, eggs and coffee". Next time you open the diary at the bookmark, read the note, and replay
only the lines after it.

That is a **snapshot**: the aggregate's state at a version, saved so that a restore can start from
it and read only the events after it.

Two things a snapshot is not:

- **Not part of the history.** The events stay the truth. Throw every snapshot away and nothing is
  lost: restores just read all the events again. That is why a snapshot is called *disposable*.
- **Not a squash.** A squash (chapter 14) replaces old events with one event. A snapshot changes
  nothing in the history, and nothing in the versions.

## Turning snapshots on

One line in the context:

```scala
import io.reactivecqrs.api.SnapshotPolicy

class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {
  // ... handlers and the rest, as in chapter 3

  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy())
}
```

`snapshots` is `None` by default. `SnapshotPolicy` has three parameters, all with defaults:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `every` | `200` | The owner writes a snapshot right after a commit that takes the aggregate's version to the next multiple of `every`: at 200, 400, 600 and so on. |
| `onPassivation` | `true` | The owner also writes one when it is stopped (passivated), if the aggregate has moved on since its last snapshot. |
| `schemaVersion` | `1` | Raise it when the state class changes shape (see below). |

A few things worth knowing about the writing:

- **One snapshot per aggregate.** The newest replaces the one before; the `snapshots` table holds one
  row per aggregate.
- **The passivation snapshot matters most.** An owner is stopped when it was idle, when it is handed
  over to another node, or when you ask with `PassivateAggregate`. The next owner restores right
  from that snapshot: often no events at all to read.
- **A write never gets in the way.** The snapshot goes in a short transaction of its own, after the
  command's commit. If it fails, it is only logged, and a later restore simply reads more events.

> **Tip:** `SnapshotSpec` turns snapshots on for carts with `SnapshotPolicy(every = 3)`, so a
> snapshot appears after a few commands. Use a small `every` like that to watch them in a test;
> keep the default in production.

## Watching it work

A small experiment, with `SnapshotPolicy(every = 3)` on the cart context:

1. Create a cart with `CreateShoppingCart`: version 1.
2. Add two items with `AddItem`: versions 2 and 3. The commit that reaches version 3 also writes a
   snapshot at version 3.
3. Look into the table: `SELECT aggregate_id, version FROM snapshots` shows one row for the cart,
   at version 3.
4. Put the owner to sleep:

   ```scala
   import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate

   carts ! PassivateAggregate(cartId)
   ```

   The aggregate has not moved on since version 3, so no new snapshot is written.
5. Send the next command. A new owner starts, reads the snapshot, and finds no event after it to
   replay.

In production you can watch the same thing through the metrics (chapter 18): every restore is timed
as `reactivecqrs.restore`, with the tag `from` set to `snapshot` or `events`.

## What a restore does with a snapshot

A restore uses the aggregate's snapshot when:

- it was written with the context's current `version`;
- it was written with the policy's current `schemaVersion`;
- for a read of an older version, it lies at or before that version.

Then it reads only the aggregate's events after the snapshot. Otherwise (no snapshot, one of another
version, one that cannot be read) it reads every event, exactly as without snapshots. A snapshot
never makes a restore fail; one that cannot be read is logged as a warning and skipped.

The same rules hold beyond the owner:

- **Reads of the past** (chapter 7): `GetAggregateForVersion` may start from a snapshot at or below
  the version asked for. `GetAggregateAtInstant` never uses one, since a snapshot does not record
  when its version was reached.
- **Projection builds** (chapter 10) restore the states they need the same way, so snapshots speed
  up the rebuild of projections that receive states too.

`SnapshotSpec` checks that this is invisible: for random cart histories with undo and duplication, a
restore with snapshots gives the same state as a full replay, at every version. It also restores a
cart of 100 000 events from one snapshot without reading a single event, and, after 150 more events,
reads just those 150.

## When snapshots are thrown away

The history can change under a snapshot, and then the snapshot would lie. So every change that
could make one wrong deletes it, in the change's own transaction:

| Change | Snapshots deleted |
| --- | --- |
| an undo (chapter 6) | the aggregate's |
| a history rewrite (chapter 15) | the aggregate's, and those of its duplicates |
| a squash (chapter 14) | the aggregate's, when it lies at or below the squash point; with a squash event that may change the state, the aggregate's and those of the duplicates that read it |
| a permanent delete (chapter 6) | the aggregate's |

Two other changes leave the snapshot in the table but make it unusable: a new `version` of the
context, and a new `schemaVersion` of the policy. The old snapshot is then ignored, and the
aggregate's next snapshot replaces it.

> **Under the hood:** each of those changes also raises the aggregate's *history generation*, a
> counter in the `aggregates` table. A restore reads the generation before the history, and a
> snapshot is written only while the aggregate is stored at the snapshot's version or later and at
> the generation its state was read at. So an owner still holding a state from before the change
> cannot write it back as a snapshot (D30, F119). The full story:
> [ARCHITECTURE.md, section 4.3](../ARCHITECTURE.md).

## Changing the state class or the event handlers

A snapshot stores the state as JSON of your aggregate root class, `ShoppingCart` here. Two kinds of
change make old snapshots wrong without the library noticing:

- **The root class changes shape** (a field added, removed or renamed): an old snapshot may not read
  back as the new class. Raise `schemaVersion`, and old snapshots are ignored:

  ```scala
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(schemaVersion = 2))
  ```

- **An event handler changes what it makes of the events**: the old snapshots hold states made by
  the old handler. Raise the context's `version`. That makes the old snapshots unusable too, and it
  rebuilds the managed projections that receive the aggregate's states (chapter 10), which hold
  states made by the old handler as well.

> **Warning:** forget to raise either one, and restores start from a state the current code would
> not make. Nothing fails: the aggregate is quietly wrong, and its next snapshot carries the mistake
> on. When in doubt, raise `schemaVersion`: it costs one full replay per aggregate, once.

## How long may a history get?

An aggregate cannot grow forever. The event store refuses to store an event past
`aggregateVersionLimit`, 10 000 by default. A command whose events would go past it fails: the
caller gets `EventHandlingError`, and the log names `TooManyEventsException`. The limit is a guard
against runaway aggregates, such as a bug that adds an event in a loop, or one aggregate that should
really have been several.

It is a parameter of the event store, so it applies to every aggregate type that store keeps. To
allow longer histories, construct the store with a larger one:

```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000).initSchema()
```

(`SnapshotSpec` uses `aggregateVersionLimit = 200000` for its cart of 100 000 events.)

> **Remember:** the limit counts versions, and a squash does not lower a version (chapter 14). A
> squashed aggregate still stops at the limit. Snapshots make a long history quick to restore; they
> do not make it allowed.

## When to turn them on

`docs/SCALING.md` (section 3) recommends snapshots for every type whose aggregates grow past a few
thousand events. The measurements show why. Restore times of the bench's own aggregate, events of a
200-character note, measured on 17/10/2026 in the `baseline-M4` run (one run on the machine named
above, with the per-aggregate limit lifted):

| Events | Full replay | From a snapshot |
| --- | --- | --- |
| 1 000 | 16 ms | 5 ms |
| 10 000 | 58 ms | 2 ms |
| 100 000 | 356 ms | 2 ms |
| 500 000 | 2 088 ms | 1 ms |

`load-M5`, the same day, gives 392 ms for a full replay of 50 000 events against 12 ms from the
snapshot. These are single runs on one machine: good for comparison, not capacity figures for yours.

From those numbers `docs/SCALING.md` estimates that one node can restore about 80 aggregates of
50 000 events per second without snapshots, and about 2 700 with them (estimated, not measured: 32
threads divided by 0.392 s and by 0.012 s). For short aggregates, a few milliseconds are all you
save.

What snapshots cost you:

- one extra write every `every` events, and one when an owner is stopped;
- one row per aggregate, holding its whole state as JSON. A large state makes a large snapshot: keep
  big documents out of aggregate states (chapter 18).

And what helps besides snapshots: keep aggregates short by design (split them by a natural
boundary, such as one per month or one per document version), squash old history (chapter 14),
and never ask for the whole history of a long aggregate at once (chapter 18).

## What you learned

- An owner restores its aggregate by replaying every event whenever it starts; restores grow with
  the history, commands do not.
- A snapshot is the state at a version; a restore reads it and only the events after it. It is
  disposable and never part of the history.
- `override val snapshots = Some(SnapshotPolicy())` turns them on: a snapshot every 200 events, and
  one when the owner is stopped.
- Undo, history rewrite, squash and permanent delete delete the snapshots they make wrong; a new
  context `version` or `schemaVersion` makes old ones ignored.
- Raise `schemaVersion` when the state class changes shape, and the context's `version` when an
  event handler changes what it makes of the events.
- `aggregateVersionLimit` (10 000 by default) caps every aggregate's version, and a squash does not
  lower it.

Next: [chapter 14](14-squash.md) folds old history away for good.
