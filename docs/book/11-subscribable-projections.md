# Chapter 11. Live Updates with Subscribable Projections

By the end of this chapter, a cart page will update itself when somebody else changes the cart:
Bob has the family's "Groceries" cart open, Alice adds milk on her phone, and the milk appears on
Bob's screen without him lifting a finger. No reload button, no asking every few seconds. You will
use a `SubscribableProjectionActor`: a projection from chapter 8 that also keeps a list of
subscribers and pushes them the news.

---

## From asking to being told

You ordered a birthday cake. You can phone the bakery every ten minutes to ask whether it is ready,
or you can leave your number and let them text you. The second way is less work for everybody, as
long as the bakery remembers your number, and as long as you tell them now and then that you still
care (a bakery that texts every number it was ever given would soon be texting strangers).

That is a **push subscription**:

- you **subscribe**: "tell me about cart 42";
- the projection **pushes** an update whenever cart 42 changes;
- you **renew** now and then: "still interested";
- you **cancel** when the page closes.

A `SubscribableProjectionActor` is an ordinary projection (listeners, a document store, queries,
everything from chapter 8) that does all four.

```
page session                                        LiveCarts projection
     │── WatchCart("7f3c...", 42) ──────────────────────▶│ handleSubscribe
     │◀────────────── SubscribedForProjectionUpdates("7f3c...")
     │                                                   │◀── cart 42 changed (from the event bus):
     │                                                   │    the listener writes, then sendUpdate(...)
     │◀────────────── SubscriptionUpdated("7f3c...", CartPageUpdate(...))
     │── RenewSubscription("7f3c...") ──────────────────▶│ every few minutes
     │── CancelProjectionSubscriptions(List("7f3c...")) ▶│ when the page closes
```

## How an update finds its subscribers

A subscribable projection adds three things to a projection:

- **`receiveSubscriptionRequest`**, a `Receive` you write, which handles your own subscribe
  messages, much like `receiveQuery` handles queries;
- **`handleSubscribe[DATA, UPDATE](subscriptionId, listener, filter)`**, which you call there to
  add a subscription;
- **`sendUpdate[DATA](data)`**, which your listeners call to announce a change.

`DATA` is what the projection announces; `UPDATE` is what a subscriber receives. Every
`sendUpdate[DATA](data)` is offered to every subscription made with the same `DATA` type, and each
subscription's **filter**, a function `DATA => Option[UPDATE]`, decides: `None` means "not for
me", `Some(update)` means "send this". The subscriber then receives
`SubscriptionUpdated(subscriptionId, update)`.

The messages a subscriber sends and receives live in the `SubscribableProjectionActor` companion:
`SubscribedForProjectionUpdates`, `SubscriptionUpdated`, `RenewSubscription`,
`CancelProjectionSubscriptions` and `ProjectionSubscriptionsCancelled`.

## Building a live cart page, step by step

We reuse `CartSummary` from chapter 8 (name, item count and the cart's version) and everything
chapter 4 wired up.

1. **Write down the update and the messages.**

   ```scala
   import io.reactivecqrs.api.id.AggregateId

   case class CartPageUpdate(cartId: AggregateId, summary: Option[CartSummary]) // None: no such cart (any more)

   case class WatchCart(subscriptionId: String, cartId: AggregateId) // subscribe to one cart
   case class GetCartPage(cartId: AggregateId)                       // answered with a CartPageUpdate
   ```

   The query answers with the same type as the pushes, so the page can treat both alike.

2. **Write the projection.**

   ```scala
   import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
   import io.reactivecqrs.core.projection.{SubscribableProjectionActor, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   class LiveCartsProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartSummary]) extends SubscribableProjectionActor {

     override protected val projectionName: String = "LiveCarts"

     protected val listeners = List(
       AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
         val summary = cart.map(c => CartSummary(c.name, c.items.size, version.asInt))
         summary match {
           case Some(s) => store.updateDocument(0, cartId.asLong, _ => Document(s))
           case None => store.removeDocument(cartId.asLong)
         }
         if (!rebuilding) sendUpdate[CartPageUpdate](CartPageUpdate(cartId, summary))
       })

     override protected def receiveSubscriptionRequest: Receive = {
       case WatchCart(subscriptionId, cartId) =>
         handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
           update => if (update.cartId == cartId) Some(update) else None)
     }

     override protected def receiveQuery: Receive = {
       case GetCartPage(cartId) =>
         sender() ! CartPageUpdate(cartId, store.getDocument(cartId.asLong).map(_.document))
     }
   }
   ```

   Line by line:

   - The listener keeps the document up to date as in chapter 8 (`updateDocument` inserts the
     document when there is none yet), and then announces the change with `sendUpdate`.
   - `if (!rebuilding)`: while a projection is rebuilt from history (chapter 10), its listeners
     replay old events. Nobody wants last year's milk pushed to their screen.
   - `handleSubscribe[CartPageUpdate, CartPageUpdate]` names both types: what `sendUpdate` announces
     and what the subscriber receives. Here they are the same.
   - The filter keeps the updates of the one cart this subscriber watches.
   - The listener is `sender()`: whoever sent `WatchCart` gets the updates. The projection confirms
     with `SubscribedForProjectionUpdates(subscriptionId)`, sent to that listener.

3. **Start it and register it**, exactly as in chapter 8, with a table of its own:

   ```scala
   val liveCarts = system.actorOf(
     Props(new LiveCartsProjection(subscriptionsState,
       new PostgresDocumentStore[CartSummary]("live_carts", mpjsons, new NoopDocumentStoreCache)))
       .withDispatcher(BlockingDispatcher.id),
     "LiveCarts")

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList, liveCarts))
   ```

   It goes on the same list as every other subscriber; the bus takes that list once, at start.

4. **Write the page's side.** Each open page gets a small actor of its own, a session, which
   subscribes when it starts, passes the news on to the browser, renews the subscription, and
   cancels it when it stops:

   ```scala
   import java.util.UUID
   import org.apache.pekko.actor.{Actor, ActorRef, Timers}
   import io.reactivecqrs.core.projection.SubscribableProjectionActor._
   import scala.concurrent.duration._

   object CartPageSession {
     private case object Renew
   }

   /** One open cart page; `push` sends a change to the browser (that part is your web layer). */
   class CartPageSession(liveCarts: ActorRef, cartId: AggregateId, push: CartPageUpdate => Unit)
     extends Actor with Timers {
     import CartPageSession.Renew

     private var subscriptionId = ""
     private var shownVersion = 0

     override def preStart(): Unit = subscribe()

     private def subscribe(): Unit = {
       subscriptionId = UUID.randomUUID().toString
       liveCarts ! WatchCart(subscriptionId, cartId) // first subscribe,
       liveCarts ! GetCartPage(cartId)               // then load what is there now
     }

     override def receive: Receive = {
       case SubscribedForProjectionUpdates(id) if id == subscriptionId =>
         timers.startTimerWithFixedDelay(Renew, Renew, 3.minutes)
       case SubscriptionUpdated(id, update: CartPageUpdate) if id == subscriptionId =>
         show(update)
       case update: CartPageUpdate => // the answer to GetCartPage
         show(update)
       case Renew =>
         liveCarts ! RenewSubscription(subscriptionId)
       case ProjectionSubscriptionsCancelled(ids) if ids.contains(subscriptionId) =>
         subscribe() // the projection forgot us: subscribe again, and load again
     }

     // Pushes and answers can overtake each other: show only what is newer than the page.
     private def show(update: CartPageUpdate): Unit = update.summary match {
       case Some(summary) if summary.version <= shownVersion => ()
       case Some(summary) =>
         shownVersion = summary.version
         push(update)
       case None => push(update)
     }

     override def postStop(): Unit = liveCarts ! CancelProjectionSubscriptions(List(subscriptionId))
   }
   ```

   When a page opens, start a session; when it closes, stop it:

   ```scala
   val session = system.actorOf(Props(new CartPageSession(liveCarts, cartId, update => browser.send(update))))
   // ... and when the page closes:
   system.stop(session)
   ```

   (`browser.send` stands for your web layer: a WebSocket, server-sent events, whatever you use.)

> **Tip:** subscribe first, then load, and keep the highest version. Subscribing first means no change
> can slip through between the load and the subscription; the version tells the page which of two
> messages is newer when a push and the answer to `GetCartPage` cross. This is why `CartSummary`
> carries the cart's version.

> **Tip:** give every subscription a fresh id, such as a UUID, as the session does. An id used twice
> in one projection mixes the two subscriptions up.

## What happens when Alice adds milk

1. Bob's page shows cart 42 at version 3: "Groceries", two items. His session subscribed with the
   id `"7f3c..."`.
2. Alice sends `AddItem(alice, cartId, AggregateVersion(3), "milk")`. The cart moves to version 4.
3. The event bus delivers the new state to `LiveCarts`. The listener writes the document and calls
   `sendUpdate[CartPageUpdate](CartPageUpdate(cartId, Some(CartSummary("Groceries", 3, 4))))`.
4. The projection offers the update to every `CartPageUpdate` subscription. Bob's filter answers
   `Some(update)`; Carol's, who watches cart 7, answers `None`.
5. Bob's session receives `SubscriptionUpdated("7f3c...", CartPageUpdate(...))`, sees that version
   4 is newer than the 3 it shows, and pushes it to the browser. Milk!

## Renewing, expiring and cancelling

Subscriptions live in the projection's memory, not in the database, and they do not live forever:

- A subscription that has not been renewed for **10 minutes** expires. The projection checks once a
  minute and drops expired subscriptions without a word.
- **`RenewSubscription(id)`** for a subscription the projection knows renews it, and gets no answer.
  For one it does not know, the answer is **`ProjectionSubscriptionsCancelled(List(id))`**: the
  subscription expired, or the projection restarted, or, in a cluster, it moved to another node
  (chapter 17). The session above then subscribes again and loads the cart again, since it may
  have missed updates in between.
- **`CancelProjectionSubscriptions(ids)`** ends subscriptions and is answered with
  `ProjectionSubscriptionsCancelled(ids)`. Ids the projection does not know are simply ignored. The
  session sends it as it stops, so the answer goes nowhere, which is fine.

The time to live is a constructor parameter, in milliseconds:

```scala
class LiveCartsProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  extends SubscribableProjectionActor(subscriptionTTL = 5 * 60 * 1000) { // 5 minutes instead of 10
  // ... as before
}
```

> **Remember:** renew well within the time to live (every 3 minutes for 10, say), and treat
> `ProjectionSubscriptionsCancelled` as "subscribe again, and load again".

## Catching up on missed updates: the updates cache

Sometimes a subscriber arrives a moment too late: a page that sends a command and subscribes right
after it may miss the push of its own change. For that, a subscribable projection can keep a short
memory of the updates it sent, and replay them to new subscribers. Two things switch it on: the
constructor's `updatesCacheTTL`, a `java.time.Duration` (`Duration.ZERO`, the default, keeps
nothing), and a `missedUpdatesFilter` in `handleSubscribe`, which picks the remembered updates this
subscriber wants (by default, none):

```scala
class LiveCartsProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  extends SubscribableProjectionActor(updatesCacheTTL = java.time.Duration.ofSeconds(30)) {

  // ... listeners and queries as before

  override protected def receiveSubscriptionRequest: Receive = {
    case WatchCart(subscriptionId, cartId) =>
      handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
        filter = update => if (update.cartId == cartId) Some(update) else None,
        missedUpdatesFilter = update => update.cartId == cartId)
  }
}
```

A new subscription now gets, right after `SubscribedForProjectionUpdates`, every update of the last
30 seconds that passes `missedUpdatesFilter`, each through its `filter` as usual. The projection
keeps every update announced within that time in memory, so keep the time short on a busy
projection.
For our page, "subscribe first, then load" already covers the gap; the cache is for subscribers
that have nothing to load.

## Things to keep in mind

> **Warning:** subscriptions and updates are matched by the `DATA` type, compared by name, and it
> must be the very same type on both sides. A `sendUpdate` of a subtype or a supertype of the type
> you subscribed with reaches nobody, without an error. Writing the type parameter on both calls,
> as in `handleSubscribe[CartPageUpdate, CartPageUpdate]` and `sendUpdate[CartPageUpdate]`, keeps
> them in step.

> **Warning:** a push can arrive twice, or a little early. `sendUpdate` puts the update on its way at
> once, while your listener's transaction is still open. If that transaction is then rolled back
> (another cart in the same group failed, or your listener threw after calling `sendUpdate`), the
> listener runs again, and the page may get the same update twice; if the listener keeps failing,
> the page may even see a change that the read model never stored. So push whole documents with
> their version rather than differences, and call `sendUpdate` as the last thing your listener does.

> **Warning:** every update runs the filter of every subscription of its type, one after another,
> on the projection's thread. Keep filters to a quick comparison. A filter that throws is logged,
> and that update stops there: the subscriptions after it in line do not get it.

> **Remember:** in a cluster, a projection started through `Projections.start` runs on one node, and
> messages to it may cross the network (chapter 17). Your own messages must not carry `ActorRef`
> fields, which is one more reason to take the listener from `sender()` rather than from a field of
> `WatchCart`. The updates travel inside the library's `SubscriptionUpdated`, so `CartPageUpdate`
> must be a plain case class that the cluster's serializer can write, like any query answer.

> **Under the hood:** `sendUpdate` reaches the subscriptions through a message the projection sends
> to itself, so a listener may call it even from the threads of `parallelUpdateProcessing`. When a
> managed projection switches to a new build (chapter 10), the old instance hands its
> subscriptions, filters included, to the new one, and pages do not notice; that is also why a
> filter must not use the old build's stores. The once-a-minute tick that expires subscriptions
> also trims the updates cache. See [ARCHITECTURE.md, section 6.6](../ARCHITECTURE.md).

The library's own tests use subscribable projections in `SubscribableSpec` (in the `core` module)
and in `ProjectionsSpec`, whose `ManagedNotes` pushes each note's lines unless it is rebuilding, and
keeps its subscribers through a rebuild.

## What you learned

- A **`SubscribableProjectionActor`** is a projection that also pushes updates to subscribers.
- A subscriber sends your own subscribe message; `receiveSubscriptionRequest` handles it with
  **`handleSubscribe(subscriptionId, listener, filter)`**, and the listener gets
  `SubscribedForProjectionUpdates`.
- Listeners announce changes with **`sendUpdate[DATA](data)`**, skipped while `rebuilding`; each
  subscription's filter turns an update into `SubscriptionUpdated(id, update)`, or into nothing.
- Subscriptions live in memory: **renew** them within 10 minutes with `RenewSubscription`, treat
  `ProjectionSubscriptionsCancelled` as "subscribe again and reload", and **cancel** with
  `CancelProjectionSubscriptions`.
- **Subscribe first, then load, and keep the highest version**: pushes can repeat and can cross
  the answers to queries.
- `updatesCacheTTL` with a `missedUpdatesFilter` replays recent updates to new subscribers.

Next: [chapter 12](12-sagas.md) leaves read models behind and gets several aggregates to work
together.
