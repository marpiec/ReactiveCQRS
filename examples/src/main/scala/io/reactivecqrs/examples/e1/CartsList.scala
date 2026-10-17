package io.reactivecqrs.examples.e1

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}

// E1's read model: one document per cart, and the questions it answers.

// #projection
case class CartSummary(name: String, itemCount: Int, version: Int)

case class GetCartSummary(cartId: AggregateId)                                   // answered with Option[CartSummary]
case class GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // the same, once it has the version

class CartsListProjection(val subscriptionsState: SubscriptionsState,
                          store: DocumentStore[CartSummary]) extends ProjectionActor {

  override protected val projectionName: String = "CartsList"

  protected val listeners = List(
    AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
      cart match {
        case Some(c) =>
          val summary = CartSummary(c.name, c.items.size, version.asInt)
          if (created) store.insertDocument(0, cartId.asLong, summary)
          else store.overwriteDocument(cartId.asLong, summary)
        case None =>
          store.removeDocument(cartId.asLong)
      }
    })

  override protected def receiveQuery: Receive = {
    case GetCartSummary(cartId) =>
      sender() ! store.getDocument(cartId.asLong).map(_.document)
    case GetCartSummaryAtLeast(cartId, version) => // waits up to 2 seconds for the projection to catch up
      delayIfNotAvailable(sender(), () =>
        store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt), 2000)
  }
}
// #projection
