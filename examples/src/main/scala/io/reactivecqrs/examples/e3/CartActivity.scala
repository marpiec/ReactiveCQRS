package io.reactivecqrs.examples.e3

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{ProjectionActor, ProjectionActorOptions, SubscriptionsState}
import io.reactivecqrs.examples.e1.{ItemAdded, ShoppingCart, ShoppingCartCreated}
import scalikejdbc.DBSession

// E3's read model: one document per cart, filled by the three kinds of listener, each told what it needs.

// #documents
case class CartActivity(name: String, itemCount: Int, version: Int, log: Vector[String], lastItem: Option[String])

case class GetCartActivity(cartId: AggregateId)                                   // answered with Option[CartActivity]
case class GetCartActivityAtLeast(cartId: AggregateId, version: AggregateVersion) // the same, once it has the version
// #documents

// #projection
class CartActivityProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartActivity],
                             options: ProjectionActorOptions) extends ProjectionActor(options) {

  override protected val projectionName: String = "CartActivity"

  private def update(cartId: AggregateId, spaceId: Long)(change: CartActivity => CartActivity)(implicit session: DBSession): Unit =
    store.updateDocument(spaceId, cartId.asLong, existing => // retried when another listener wrote meanwhile
      Document(change(existing.map(_.document).getOrElse(CartActivity("", 0, 0, Vector.empty, None)))))

  protected val listeners = List(
    // The state after each change: the name, the item count and the version.
    AggregateListener[ShoppingCart] { (cartId, version, _, cart) => implicit session =>
      cart.foreach { c =>
        if (c.name == "Broken") throw new IllegalStateException("This listener cannot take a cart named Broken")
        update(cartId, 0)(_.copy(name = c.name, itemCount = c.items.size, version = version.asInt))
      }
    },
    // The events: a line of the log for each.
    EventsListener[ShoppingCart] { (cartId, events) => implicit session =>
      update(cartId, 0)(activity => activity.copy(log = activity.log ++ events.map(_.event match {
        case ShoppingCartCreated(name) => "created as " + name
        case ItemAdded(name) => "added " + name
        case other => other.getClass.getSimpleName
      })))
    },
    // Both: the last item added, read from the events with the state at hand.
    AggregateWithEventsListener[ShoppingCart] { (cartId, _, cart, events) => implicit session =>
      val added = events.collect { case e if e.event.isInstanceOf[ItemAdded] => e.event.asInstanceOf[ItemAdded].name }
      if (added.nonEmpty && cart.isDefined) update(cartId, 0)(_.copy(lastItem = added.lastOption))
    })

  override protected def receiveQuery: Receive = {
    case GetCartActivity(cartId) =>
      sender() ! store.getDocument(cartId.asLong).map(_.document)
    case GetCartActivityAtLeast(cartId, version) =>
      delayIfNotAvailable(sender(), () =>
        store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt), 2000)
  }
}
// #projection
