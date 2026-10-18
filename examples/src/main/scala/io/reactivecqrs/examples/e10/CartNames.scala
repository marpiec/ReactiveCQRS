package io.reactivecqrs.examples.e10

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, RebuildTarget, SubscriptionsState}
import io.reactivecqrs.examples.e1.ShoppingCart
import scalikejdbc.DBSession

// E10's managed projection: each cart's name, as typed in version 1 and in capitals from version 2 (chapter 10).

// #projection
case class GetCartName(id: AggregateId) // answered with Option[String], once the cart's document is there

class CartNames(val subscriptionsState: SubscriptionsState, store: DocumentStore[String], capitals: Boolean) extends ProjectionActor {

  override protected val projectionName: String = "CartNames"

  protected val listeners = List(AggregateListener[ShoppingCart] { (id, _, created, cart) => implicit session =>
    cart match {
      case Some(c) if created => store.insertDocument(0, id.asLong, written(c.name))
      case Some(c)            => store.overwriteDocument(id.asLong, written(c.name))
      case None               => store.removeDocument(id.asLong) // the cart was deleted
    }
  })

  private def written(name: String): String = if (capitals) name.toUpperCase else name

  // #query
  override protected def receiveQuery: Receive = {
    case GetCartName(id) => delayIfNotAvailable(sender(), () => store.getDocument(id.asLong).map(_.document), 2000)
  }
  // #query

  // #clear
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space)  => store.clearSpace(space.asLong)
    case RebuildTarget.OneType(_)       => store.clearAllData() // CartNames receives carts only
  }
  // #clear
}
// #projection
