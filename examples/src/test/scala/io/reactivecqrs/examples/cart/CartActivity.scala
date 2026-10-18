package io.reactivecqrs.examples.cart

// Chapter 8's activity log: a read model of the test domain's carts, made from their events.

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart._

// #activity
import io.reactivecqrs.api._
import io.reactivecqrs.core.documentstore.Document

case class CartActivity(lines: Vector[String])
case class GetCartActivity(cartId: AggregateId) // answered with Option[CartActivity]

class CartActivityProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartActivity]) extends ProjectionActor {

  override protected val projectionName: String = "CartActivity"

  protected val listeners = List(
    EventsListener[ShoppingCart] { (cartId, events) => implicit session =>
      val lines = events.map(e => "v" + e.version.asInt + ", user " + e.userId.asLong + ": " + describe(e.event))
      store.updateDocument(0, cartId.asLong, existing =>
        Document(CartActivity(existing.map(_.document.lines).getOrElse(Vector.empty) ++ lines)))
    })

  private def describe(event: Event[ShoppingCart]): String = event match {
    case ShoppingCartCreated(name) => "created \"" + name + "\""
    case ShoppingCartDuplicated(_, baseId, baseVersion) => "copied from cart " + baseId.asLong + " at version " + baseVersion.asInt
    case ItemAdded(name) => "added " + name
    case ItemRemoved(itemId) => "removed item " + itemId
    case ShoppingCartChangesUndone(count) => "undid the last " + count + " change(s)"
    case ShoppingCartDeleted() => "deleted the cart"
    case other => other.getClass.getSimpleName // anything else, the squash event included
  }

  override protected def receiveQuery: Receive = {
    case GetCartActivity(cartId) => sender() ! store.getDocument(cartId.asLong).map(_.document)
  }
}
// #activity
