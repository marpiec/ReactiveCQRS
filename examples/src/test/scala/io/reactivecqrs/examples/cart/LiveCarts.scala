package io.reactivecqrs.examples.cart

// Chapter 11's live cart page: a subscribable projection of the test domain's carts, and an open page's session.

// #messages
import io.reactivecqrs.api.id.AggregateId

case class CartPageUpdate(cartId: AggregateId, summary: Option[CartSummary]) // None: no such cart (any more)

case class WatchCart(subscriptionId: String, cartId: AggregateId) // subscribe to one cart
case class GetCartPage(cartId: AggregateId)                       // answered with a CartPageUpdate
// #messages

// #projection
import java.time.Duration
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{SubscribableProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

class LiveCartsProjection(val subscriptionsState: SubscriptionsState,
                          store: DocumentStore[CartSummary],
                          updatesCacheTTL: Duration = Duration.ZERO,
                          subscriptionTTL: Int = SubscribableProjectionActor.defaultSubscriptionTTL)
  extends SubscribableProjectionActor(updatesCacheTTL, subscriptionTTL) {

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

  // #subscribe
  override protected def receiveSubscriptionRequest: Receive = {
    case WatchCart(subscriptionId, cartId) =>
      handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
        filter = update => if (update.cartId == cartId) Some(update) else None,
        missedUpdatesFilter = update => update.cartId == cartId)
  }
  // #subscribe

  override protected def receiveQuery: Receive = {
    case GetCartPage(cartId) =>
      sender() ! CartPageUpdate(cartId, store.getDocument(cartId.asLong).map(_.document))
  }
}
// #projection

// #session
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
// #session
