package io.reactivecqrs.testdomain.shoppingcart

import akka.actor.ActorRef
import io.reactivecqrs.api.{Event, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.projection.EventBasedProjectionActor
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames

object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjection(val eventBusActor: ActorRef) extends EventBasedProjectionActor[ShoppingCart] {

  private var shoppingCartsNames = Map[AggregateId, String]()

  override protected def newEventReceived(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart]): Unit = event match {
    case ShoppingCartCreated(name) => shoppingCartsNames += aggregateId -> name
    case ItemAdded(name) => ()
    case ItemRemoved(id) => ()
    case ShoppingCartDeleted() => shoppingCartsNames -= aggregateId
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}
