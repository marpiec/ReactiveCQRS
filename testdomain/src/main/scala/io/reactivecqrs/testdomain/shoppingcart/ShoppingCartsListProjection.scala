package io.reactivecqrs.testdomain.shoppingcart

import akka.actor.ActorRef
import io.reactivecqrs.api.{Event, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.projection.EventBasedProjectionActor
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames

object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjection(val eventBusActor: ActorRef) 
  extends EventBasedProjectionActor {

  protected val listeners = Map[Class[_], (AggregateId, AggregateVersion, Event[_]) => Unit](classOf[ShoppingCart] -> shoppingCartUpdate _)
  
  private var shoppingCartsNames = Map[AggregateId, String]()

  def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[_]): Unit = event match {
    case ShoppingCartCreated(name) => shoppingCartsNames += aggregateId -> name
    case ItemAdded(name) => ()
    case ItemRemoved(id) => ()
    case ShoppingCartDeleted() => shoppingCartsNames -= aggregateId
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}
