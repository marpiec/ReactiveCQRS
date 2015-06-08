package io.reactivecqrs.testdomain.shoppingcart

import akka.actor.ActorRef
import io.reactivecqrs.api.{Event, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.projection.{EventListener, AggregateRootBasedProjectionActor, EventBasedProjectionActor}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames


object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjectionEventsBased(val eventBusActor: ActorRef) extends EventBasedProjectionActor {

  override protected val listeners:List[EventListener[Any]] = List(shoppingCartUpdate _)

  private var shoppingCartsNames = Map[AggregateId, String]()

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart]): Unit = event match {
    case ShoppingCartCreated(name) => shoppingCartsNames += aggregateId -> name
    case ItemAdded(name) => ()
    case ItemRemoved(id) => ()
    case ShoppingCartDeleted() => shoppingCartsNames -= aggregateId
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}



class ShoppingCartsListProjectionAggregatesBased(val eventBusActor: ActorRef) extends AggregateRootBasedProjectionActor {
  protected val listeners = Map[Class[_], (AggregateId, AggregateVersion, Option[_]) => Unit](classOf[ShoppingCart] -> shoppingCartUpdate _)

  private var shoppingCartsNames = Map[AggregateId, String]()

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, aggregateRoot: Option[_]): Unit =
    aggregateRoot.asInstanceOf[Option[ShoppingCart]] match {
    case Some(a) => shoppingCartsNames += aggregateId -> a.name
    case None => shoppingCartsNames -= aggregateId
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}
