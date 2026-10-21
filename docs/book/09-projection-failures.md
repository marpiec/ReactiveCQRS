# Chapter 9. When Things Go Wrong in Projections

Projections are code, and code breaks: a bug in a listener, a database that hiccups, a projection
slower than the commands it follows, a machine that dies a millisecond after a commit. In this
chapter you will see what ReactiveCQRS promises about delivering events, what it does by itself when
something goes wrong, and the handful of messages you use to look at the damage and repair it. By
the end you will be able to read the log lines, ask a projection what is wrong with it, and get a
stuck cart moving again.

---

## The promise, in plain words

Picture registered mail. The postman rings until somebody signs, and if nobody does, he comes back
another day. Every letter from a sender carries a number. And the household keeps a notebook: "from
the bank, last letter opened: number 7". A letter 7 that turns up twice is recognised and put
aside. A letter 9 that turns up while 8 never came makes the household phone the bank: "please send
number 8 again".

That is exactly what ReactiveCQRS promises a projection:

1. **At least once.** Every event reaches every projection that listens to its aggregate type, even
   across crashes and restarts. Sometimes more than once.
2. **In order, per aggregate.** The events of cart 42 are applied in version order. There is no
   promise across carts: cart 7's version 5 may be applied before cart 42's version 2, even if
   cart 42's was committed first.
3. **Applied once.** The notebook is the `subscriptions` table: for each projection, listener kind
   and aggregate, the last version applied. It is written in the same transaction as your read
   model. A delivery at or below that version is a duplicate: acknowledged, not applied again.
4. **No holes.** A version beyond the next expected one means some went missing. The projection
   asks the aggregate's owner to publish them again, and applies everything in order once they
   arrive.

> **Remember:** order is per aggregate only. A read model that combines several carts (a total over
> all carts, say) must not assume that events of different carts arrive in the order they were
> committed.

## Why your listener must be safe to run twice

"Applied once" holds for what your listener writes through its session: those writes commit with
the notebook or roll back with it. Anything else your listener does is not covered, and it may
happen more than once:

- the updates of up to 100 carts share one transaction; when one cart fails, all of them are rolled
  back and each runs again on its own;
- a failing listener is tried up to nine times (next section);
- a `noDBSession` listener runs before its version is written, so if that write fails, it sees the
  same events again;
- a rebuild (chapter 10) replays history through your listeners.

So keep e-mails, HTTP calls and messages to other actors out of ordinary listeners. They belong in a
saga (chapter 12), or in a listener of their own that uses `noDBSession` (no transaction, never
grouped with other carts), does nothing while `rebuilding` is true, and is idempotent: doing the
same thing twice must be harmless.

<!-- snippet: cart/CartDeletedMailer.scala#mailer -->
```scala
import io.reactivecqrs.core.projection.{AggregateListenerActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCart, ShoppingCartDeleted}

trait Mailer { def sendOnce(key: String, text: String): Unit } // your own service: one mail per key

class CartDeletedMailer(val subscriptionsState: SubscriptionsState, mailer: Mailer) extends AggregateListenerActor {
  override protected val projectionName: String = "CartDeletedMailer"

  protected val listeners = List(
    EventsListener.noDBSession[ShoppingCart] { (cartId, events) =>
      if (!rebuilding && events.exists(_.event == ShoppingCartDeleted()))
        mailer.sendOnce("cart-deleted-" + cartId.asLong, "Your cart " + cartId.asLong + " was deleted.")
    })
}
```

`AggregateListenerActor` is a projection whose `receiveQuery` ignores every message: a listener
with nothing to ask. You start and register it like any projection (chapter 8).

## When a listener throws

Let's break something on purpose. Suppose we decided that cart names must be unique in our list,
and gave it a store like chapter 8's with a unique index (in a table of its own here):

<!-- snippet: cart/FailuresSpec.scala#uniqueStore -->
```scala
val cartsStore = new PostgresDocumentStore[CartSummary]("unique_carts", mpjsons, new NoopDocumentStoreCache,
  Seq(UniqueTextIndex(1, Seq("name"))))
```

This chapter's blocks run in `FailuresSpec` (`examples/src/test/scala/io/reactivecqrs/examples/cart/`),
which tells this story with three quick retries instead of nine slow ones. Alice has a cart called
"Groceries". Then Bob creates one with the same name. The write side does
not mind, because cart names are not unique there, but our listener's `insertDocument` for Bob's
cart (cart 43) breaks the index. Here is what happens next:

1. The insert throws. The transaction rolls back: no document, and the notebook still says "nothing
   applied" for cart 43. The log gets a warning:

   ```
   Projection CartsList failed on ShoppingCart 43 versions 1-1 (attempt 1), retrying: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint ...
   ```

2. The projection tries again after 1, 2, 4, 8, 16, 30, 30 and 30 seconds: nine attempts in about
   two minutes. Later events of cart 43 wait behind the failing one. Every other cart goes on as
   usual.
3. If an attempt succeeds (after a database hiccup, say), that is the end of it: no record, nothing
   to repair.
4. After the ninth failure, the projection gives up on cart 43. It writes a row to
   `subscription_failures` (the version it could not apply, the error, and since when) and logs an
   error:

   ```
   Projection CartsList gave up on ShoppingCart 43 at version 1 (PSQLException: ERROR: duplicate key value ...); deliveries of it fail until it is retried or skipped
   ```

5. From now on, it answers every delivery of cart 43 with `MessageFailed`, at once, without trying.
   The event bus counts that as an answer (and logs a warning each time), so cart 43's outbox rows
   are deleted as usual. The row in `subscription_failures` is now the reminder of what is left to do.

One more case: if even the failure cannot be written down, because the database itself is down, the
projection answers nothing and tries the batch again every 30 seconds, until it can either apply it
or record the failure. And an error that is the database's, not the listener's (a lost or refused
connection, a transaction the server rolled back, a server short of resources), is not counted as
an attempt at all: the batch is tried again every 30 seconds for as long as the database is away,
with one warning a minute, and no failure is recorded for a database restart.

> **Remember:** a failure belongs to one projection, one listener kind and one aggregate. Cart 43 is
> stuck in `CartsList`; the same cart in other projections, and every other cart in this one, are
> fine.

> **Tip:** a read model is the wrong place for business rules. It hears about a change only after
> the change is stored, so all it can do is fail. Rules belong on the write side, where a command
> can still be refused.

## Looking at the damage: GetProjectionStatus

Ask the projection itself:

<!-- snippet: cart/FailuresSpec.scala#status -->
```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, ProjectionStatus}

val status = Await.result((cartsList ? GetProjectionStatus).mapTo[ProjectionStatus], 5.seconds)
status.failures.foreach { f =>
  println(f.aggregateType.simpleName + " " + f.aggregateId.asLong + " stuck at version " + f.version.asInt + ": " + f.error)
}
// ShoppingCart 43 stuck at version 1: PSQLException: ERROR: duplicate key value violates unique constraint ...
```

The answer is `ProjectionStatus(projectionName, failures, retrying, waitingForGap)`:

- **`failures`**: one `SubscriptionFailure` per stuck subscription, with its `subscriptionType`
  (which listener kind), `aggregateType`, `aggregateId`, `version` (the first version not applied),
  `error` and `since`;
- **`retrying`**: the carts whose listener failed and is being retried right now (step 2 above);
- **`waitingForGap`**: the carts waiting for missing versions (later in this chapter).

Three empty lists mean all is well. The failures live in the `subscription_failures` table, so they
survive restarts: a stuck cart stays stuck until somebody does something about it. So that the answer
can always reach you, also from another node of a cluster, it lists the oldest 100 failures, each
error cut to 500 characters, and `failuresTotal` says how many there are; the other two lists hold
at most 1 000 carts each.

> **Tip:** put `GetProjectionStatus` behind an admin page or a health check, for every projection.
> Chapter 18 has more on watching a running system.

## Fixing it: retry or skip

First fix the cause. Here we swap the unique index for an ordinary one, under a new number (at
start, the store drops index 1 and creates index 2), and restart:

<!-- snippet: cart/FailuresSpec.scala#indexSwap -->
```scala
Seq(MultipleTextIndex(2, Seq("name")))
```

You may not even need to do anything more: failures recorded earlier are retried automatically when
the projection starts, as soon as the bus registers it, 20 per second. So after this restart, cart
43 gets its document without you lifting a finger. When you fix a cause without a restart, or a
retry at start did not help, these three messages, sent to the projection, do it by hand:

| Message | Answer | What it does |
|---|---|---|
| `RetryFailedSubscription(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | asks the cart's owner for the failed version and the ones after it, again and again until they come; once the failed version is applied, the failure record is deleted |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | the same for every failure of the projection, 20 per second |
| `SkipFailedVersion(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | marks the failed version as applied without applying it, deletes the record, and asks once for the versions after it; for a "versions missing" failure it marks nothing (those versions may not exist yet) and asks for them again, unless sent as `SkipFailedVersion(cartId, pastMissing = true)`, for versions that exist but cannot arrive (an event larger than a message between nodes may be) |

`count` says how many failed subscriptions were handled; 0 means there was nothing to do.

<!-- snippet: cart/FailuresSpec.scala#retry -->
```scala
import io.reactivecqrs.core.projection.{FailedSubscriptionsHandled, RetryFailedSubscription}

val handled = Await.result((cartsList ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.seconds)
// FailedSubscriptionsHandled(Id(43),1)
```

When it works, the log says `Projection CartsList recovered from its failure on ShoppingCart 43`.
If the listener still fails, you are back at step 1: retries, then a new record.

> **Warning:** skipping loses something, and what it loses depends on the listener. A state
> listener receives the whole state with the next version, so its document is right again as soon
> as the cart changes (if it ever does). An events listener never sees the skipped event again: its
> effect is gone for good. And there is a trap for state listeners that rely on `created`: if the
> skipped version is the one that created the document, the next version tries to
> `overwriteDocument` a document that does not exist, and fails. A listener written with
> `updateDocument(0, cartId.asLong, _ => Document(summary))`, which inserts when there is nothing
> yet, does not fall into it.

## Missing versions

Sometimes a delivery never arrives: a message lost when a node went down, or a projection that
joined after a cart's first events (chapter 8). The projection notices when the next delivery
comes: "I am at version 3, and this is version 5". Then:

1. It asks the cart's owner, through the bus, to publish everything from version 4 again, to this
   projection only. The log says `Projection CartsList asks for versions from 4 of ShoppingCart 42`.
   Version 5 waits.
2. When the versions arrive, it applies 4 and 5 in order and acknowledges the delivery that waited.
3. Without an answer, it asks again after 10 seconds, then after 20, 40 and so on, up to once every
   5 minutes.
4. If after 10 minutes the versions still have not come, it records a failure at version 4 ("versions
   from 4 are missing: asked … times in 10 minutes without an answer"), which you handle like any
   other failure.

`ProjectionStatus.waitingForGap` lists the carts in steps 1 to 3. When the owner has nothing after
what the projection asks for (the cart is at version 3, and a stale record asked for 4), it says so,
and the projection stops waiting; a failure record of versions that never existed is then deleted.
A failure record whose version the projection has already applied (its deletion failed after the
retry that applied it) is deleted as soon as it is read, instead of becoming a gap. And when the
owner cannot restore the cart at all (an event your handlers throw on), it answers that at once
(`CannotRestore`): the projection records the failure then, "the aggregate cannot be restored: ..."
with the cause, instead of waiting ten minutes for versions that cannot come. Chapter 18 says what to
do about such a cart.

When a projection has nothing of a cart yet (its targeted rebuild reset it, or it was added late)
and a live event arrives, the owner gives it the cart's history first. That history is applied as a
rebuild applies it: `rebuilding` is true, so a subscribable projection pushes nothing for it
(chapter 11). The live event that came with it is applied the same way; the events after it are live.

## The failure options

These options of chapter 8's `ProjectionActorOptions` shape everything above:

| Option (default) | What it controls |
|---|---|
| `retries` (`Backoff(1.second, 30.seconds, maxAttempts = 9)`) | how a failing listener is retried before the failure is recorded: the wait starts at the first duration and doubles up to the second, for that many attempts in all |
| `retryFailuresOnStart` (true) | retry the failures recorded before the start |
| `failureRetryInterval` (50 milliseconds) | the pace of retrying many failures: at start, and for `RetryAllFailedSubscriptions` |
| `gapRequestRepeat` (10 seconds) | how long a request for missing versions waits before it is sent again, doubling up to 5 minutes |
| `gapGiveUpAfter` (10 minutes) | how long missing versions are asked for before the gap becomes a recorded failure |

<!-- snippet: cart/FailuresSpec.scala#retries -->
```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions
import io.reactivecqrs.core.util.Backoff

val options = ProjectionActorOptions.DEFAULT
  .withRetries(Backoff(1.second, 10.seconds, maxAttempts = 4)) // four attempts, about 7 seconds of waiting
  .copy(gapGiveUpAfter = 30.minutes)
```

## Meanwhile, at the event bus

The event bus keeps a small record for every event it delivers, per subscriber and listener kind,
until the subscriber answers with `MessageAck` (applied, now or before) or `MessageFailed` (gave
up). Then:

- When every subscriber has answered, the bus tells the cart's command bus, which deletes the
  event's row from the outbox.
- A delivery without an answer for a minute is logged once, for example
  `Delivery waits for 1 minute: ShoppingCart 42 version 3 to CartsList (AggregatesDelivery)`. At
  most 10 such lines are written every 5 seconds, and a summary every 5 minutes.
- A delivery without an answer for 10 minutes ends:
  `Undelivered after 10 minutes: ShoppingCart 42 version 3 to CartsList (AggregatesDelivery); it stays in the outbox until it is published again`.
  The bus forgets it, and the outbox row stays.

(The bus names a subscriber by its actor's name, the one you gave `system.actorOf`; the projection's
own log lines use its `projectionName`. We called both "CartsList".)

Ask the bus how it is doing with `GetDeliveryStatus`:

<!-- snippet: cart/FailuresSpec.scala#delivery -->
```scala
import io.reactivecqrs.core.eventbus.EventsBusActor.{DeliveryStatus, GetDeliveryStatus}

val delivery = Await.result((eventBus ? GetDeliveryStatus).mapTo[DeliveryStatus], 5.seconds)
```

| Field | Meaning |
|---|---|
| `waiting` | deliveries waiting for an answer |
| `undelivered` | the latest 100 that ended at the deadline, each an `Undelivered(aggregateType, aggregateId, version, subscriber, kind, at)`; the log counts the others |
| `stopped` | subscribers that stopped (later in this chapter) |
| `held`, `behind` | flow control (later in this chapter) |

The bus takes its timings from `DeliveryOptions`, which you can pass when you create it, for
example `Props(new EventsBusActor(DeliveryOptions(deadline = 20.minutes)))`. The fields for this
section are `deadline` (10 minutes), `stallAfter` (1 minute), `summaryInterval` (5 minutes),
`tickInterval` (5 seconds) and `stallLinesPerTick` (10).

## The outbox: a to-do list that survives a crash

Every event gets a row in `events_to_publish`, the **outbox**, in the very transaction that stores
the event. The row is deleted only when every subscriber has answered. So when a machine dies right
after a commit, or a delivery ends at the deadline, the row is still there, and somebody has to
deliver the event again: the `OutboxPollerActor`, one per system, which you started in chapter 4.

Every 30 seconds (the first time 10 seconds after start), the poller reads which aggregates still
have rows waiting, at most 100 000 of them, the longest waiting first, and decides:

| The oldest waiting event was stored | The poller |
|---|---|
| 3 minutes ago | asks the cart's owner to publish it again |
| 6 minutes ago | asks again |
| 10 minutes ago | logs `Undelivered: ShoppingCart 42 from version 3 still waits in the outbox after 10 minutes; it is re-published again every 30 minutes`, once |
| longer ago | asks again every 30 minutes, in case the subscriber is back |

Rows written before the process started are published at the first poll, then 3 and 6 minutes
later. Subscribers that already have the events simply acknowledge them again. At most 1 000
aggregates are published again per poll, the longest waiting first, so that a backlog after an
outage does not wake every owner at once.

<!-- snippet: cart/FailuresSpec.scala#outboxStatus -->
```scala
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus}

val outbox = Await.result((outboxPoller ? GetOutboxStatus).mapTo[OutboxStatus], 5.seconds)
// outbox.waiting: aggregates with rows waiting at the last poll; outbox.gaveUp: those reported undelivered
```

You can change the timings with `OutboxPollerOptions` when you create the poller:

<!-- snippet: cart/FailuresSpec.scala#pollerOptions -->
```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
```

The defaults: `firstPollAfter` 10 seconds, `pollInterval` 30 seconds, `maxTracked` 100 000, and in
`RedeliveryPolicy.Options`: `attemptsAfter` 3 and 6 minutes, `giveUpAfter` 10 minutes,
`maxRepublishPerPoll` 1 000 and `retryGivenUpEvery` 30 minutes. When more carts than `maxTracked` wait,
the poller reads the 50 000 newest too, so carts waiting for a subscriber that stays away cannot hide
a newer lost delivery. A cart the poller first sees late gets as much time after its re-publication
as any other before it is reported. `OutboxStatus.gaveUp` lists the 100 waiting longest, and
`gaveUpTotal` counts them all.

> **Remember:** a listener that fails for good does *not* keep outbox rows waiting: its
> `MessageFailed` is an answer. The poller is there for silence (lost messages, crashed machines,
> stopped subscribers); failures are there for the failure records and the messages above.

> **Under the hood:** "publish it again" means the poller sends `Republish` to the event bus, which
> passes `RepublishOutbox` to the cart type's command bus, which passes it to the cart's owner
> (waking it up if needed). The owner publishes from its oldest waiting version to its current one,
> with its current state; a long range goes in parts of 1 000 events or 16 MB. See
> [ARCHITECTURE.md, section 5.1](../ARCHITECTURE.md).

## A slow projection: flow control

Picture a waiter serving a table of eight. The kitchen does not send all forty dishes at once: the
waiter brings what the table can hold, and the rest stays warm in the kitchen until there is room.
The event bus does the same for a projection that cannot keep up:

- A projection is sent at most a **window** of events it has not answered yet: 10 000 events, or
  64 MB of their stored size, whichever comes first.
- What does not fit is **held**: the events stay in the database, their outbox rows too, and the bus
  only remembers which carts are behind.
- Once the projection has answered half its window, the bus asks the owners of the held carts to
  publish them again to that projection, oldest first, at most 100 carts at a time. While carts are
  held, new events take at most half the window, so the held ones keep moving.
- A fast projection next to a slow one is not held back: every subscriber has a window of its own.
- A projection that answers nothing for the whole deadline gets no new window: what it was sent
  still counts until it answers something sent later. It gets one message per deadline meanwhile, so
  messages that were lost cannot keep it shut out.
- In a cluster, a state listener on another node gets a copy of the cart's state in every message,
  so the window counts the state's size too.

`GetDeliveryStatus` shows it all: `held` counts the deliveries held, and `behind` maps each
subscriber with carts held to how many. The bus's 5-minute summary names the subscribers behind too.
A managed projection's instance is named after the projection (`carts-build-2-1`), so these names,
the `bus.behind` metric and the logs tell the projections apart.

To give your projections a bigger or a smaller window, pass `DeliveryOptions` to the bus, for example
`DeliveryOptions(window = 20000, windowBytes = 128L * 1024 * 1024)`. A bigger window lets a
projection take more at once, at the cost of memory in its mailbox. In a cluster, each node's bus
has a window of its own (chapter 17).

How fast is fast enough? The library's own measurements give an idea, not a capacity figure: single
runs on one Windows machine (12 CPU threads, PostgreSQL on the same machine), measured on 17/10/2026.
An events-based projection applied 2 732 events per second while commands were running
(`docs/perf/baseline-M4.md`), and after a 60-second burst of 671 768 commands, two projections had
caught up 89 seconds after the burst ended (`docs/perf/flow-M5.md`).

> **Tip:** keep listeners cheap, leave headroom between your command rate and your slowest
> projection, and watch `held` and `behind`. A projection that has fallen far behind is often
> quicker to rebuild (chapter 10) than to feed. [SCALING.md, section 5](../SCALING.md) has the
> details.

## A projection that stops

A projection does not stop because a listener or a query fails: it answers with the failure and
goes on. But an actor can still stop: somebody stops it, or part of the system shuts down. The bus
watches its subscribers, and when one stops, the bus logs an error,
`Subscriber …/user/CartsList stopped; its events stay in the outbox until the next start`, lists it
in `DeliveryStatus.stopped`, and sends it nothing more. Its events wait in the outbox (the poller
keeps trying, and the other subscribers acknowledge the repeats) until the system starts again with
it. The other subscribers are not affected. A managed projection (chapter 10) is replaced
automatically after a short wait.

Two stops are no error, and the bus logs them at INFO instead: a subscriber that stops because its
node shuts down, and one whose replacement is already on its way, such as the instance of a
managed projection (its supervisor starts a new one) or of a projection that runs once in a cluster
and moves to another node (chapter 17).

## What waits for a failed subscription

A failure record is a promise: "this cart still owes this projection something". Two operations
that would break that promise wait until the record is gone:

- **Squash** (chapter 14): `SquashAggregate(cartId, upTo)` is answered with
  `SquashRefused(cartId, "a subscription of the aggregate is failed")`.
- **Permanent delete** (chapter 6): the cart is not deleted yet. The command bus logs
  `Aggregate 43 of type ShoppingCart is not deleted yet: a subscription of it is failed`, and the
  delete event's outbox row stays. Once the failure is retried or skipped, the projection has the
  cart's owner publish that row again at once, and the delete finishes. A failure record removed
  another way (`clearSubscriptionsInfo` below) is not noticed: the delete then waits for the
  poller's next turn, at most 30 minutes.

> **Warning:** a projection you remove from the code leaves its failure records behind, and they
> keep squashes refused and permanent deletes waiting. For a managed projection,
> `Projections.retire(name)` cleans up (chapter 10). For one you started yourself, call
> `subscriptionsState.clearSubscriptionsInfo("com.example.CartsListProjection")` with its full class
> name: it forgets that projection's versions and failures.

## What you learned

- Delivery is **at least once, in order per aggregate, applied once**, thanks to the notebook
  (`subscriptions`) written in your listener's transaction. Writes through the session are safe to
  repeat; other side effects are not, and belong in sagas or idempotent listeners of their own.
- A listener that throws is **retried for about two minutes**; then the failure is **recorded**,
  that cart is answered as failed in that projection, and everything else keeps flowing.
- **`GetProjectionStatus`** shows failures, retries and gaps; **`RetryFailedSubscription`**,
  **`RetryAllFailedSubscriptions`** and **`SkipFailedVersion`** repair them, and failures are
  retried at every start.
- Missing versions are asked from the aggregate's owner; after 10 minutes the gap becomes a failure.
- The **outbox** keeps what is not delivered; the **outbox poller** publishes it again at 3 and 6
  minutes, reports it at 10, then retries every 30 minutes. **`GetDeliveryStatus`** and
  **`GetOutboxStatus`** show where things stand.
- **Flow control** sends a slow projection at most 10 000 events or 64 MB at a time; the rest waits
  in the database.
- A failed subscription holds up the **squash** and the **permanent delete** of its aggregate.

Next: [chapter 10](10-rebuilding-projections.md) shows how to rebuild a projection from the event
store while the old one keeps serving.
