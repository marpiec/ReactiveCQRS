package io.reactivecqrs.examples.cart

// Chapter 8's cart list: a read model of the test domain's carts, made from their states.

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.core.documentstore.{DocumentStoreQuery, ExpectedGreaterThanIntValue, SortDescInt}

// #documents
import io.reactivecqrs.api.id.AggregateId

case class CartSummary(name: String, itemCount: Int, version: Int)

case class GetCartSummary(cartId: AggregateId)   // answered with Option[CartSummary]
case object GetAllCarts                          // answered with Map[AggregateId, CartSummary]
case class FindCartsByName(name: String)         // answered with Vector[CartSummary]
// #documents

// #findBigCarts
case class FindBigCarts(moreThan: Int) // answered with Seq[(AggregateId, CartSummary)]
// #findBigCarts

// #atLeast
case class GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // answered with Option[CartSummary]
// #atLeast

// #projection
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

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
    case GetAllCarts =>
      sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
    case FindCartsByName(name) =>
      sender() ! store.findDocumentByPath(Seq("name"), name).values.map(_.document).toVector
    // #bigCartsCase
    case FindBigCarts(moreThan) =>
      val query = DocumentStoreQuery(
        where = Seq(ExpectedGreaterThanIntValue(Seq("itemCount"), moreThan)),
        sortBy = Seq(SortDescInt("itemCount")),
        offset = 0,
        limit = 20)
      sender() ! store.findDocument(query).map { case (id, doc) => AggregateId(id) -> doc.document }
    // #bigCartsCase
    // #atLeastCase
    case GetCartSummaryAtLeast(cartId, version) =>
      delayIfNotAvailable(sender(), () =>
        store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt),
        2000)
    // #atLeastCase
  }
}
// #projection
