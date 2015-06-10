package io.reactivecqrs.testdomain.shoppingcart

import akka.actor.ActorRef
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, Event}
import io.reactivecqrs.core.projection.ProjectionActor
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames


object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjectionEventsBased(val eventBusActor: ActorRef) extends ProjectionActor {

  protected val listeners = List(EventListener(shoppingCartUpdate))

  private var aggregatesApplied = Map[AggregateId, AggregateVersion]()
  private var shoppingCartsNames = Map[AggregateId, String]()

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart]): Unit = event match {
    case ShoppingCartCreated(name) =>
      shoppingCartsNames += aggregateId -> name
      aggregatesApplied += aggregateId -> version
    case ItemAdded(name) =>
      aggregatesApplied += aggregateId -> version
    case ItemRemoved(id) =>
      aggregatesApplied += aggregateId -> version
    case ShoppingCartDeleted() =>
      shoppingCartsNames -= aggregateId
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}



class ShoppingCartsListProjectionAggregatesBased(val eventBusActor: ActorRef) extends ProjectionActor {
  protected val listeners =  List(AggregateListener(shoppingCartUpdate))

  private var aggregatesApplied = Map[AggregateId, AggregateVersion]()
  private var shoppingCartsNames = Map[AggregateId, String]()

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, aggregateRoot: Option[ShoppingCart]): Unit = aggregateRoot match {
    case Some(a) =>
      shoppingCartsNames += aggregateId -> a.name
      aggregatesApplied += aggregateId -> version
    case None =>
      shoppingCartsNames -= aggregateId
      aggregatesApplied += aggregateId -> version
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! shoppingCartsNames.values.toVector
  }
}
