package io.reactivecqrs.examples.cart

// Chapter 9's listener with a side effect: a mail when a cart is deleted, sent once, never during a rebuild.

// #mailer
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
// #mailer
