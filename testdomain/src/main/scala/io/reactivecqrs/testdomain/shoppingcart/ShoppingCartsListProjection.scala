package io.reactivecqrs.testdomain.shoppingcart

import akka.actor.ActorRef
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, Event}
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.ProjectionActor
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames


object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjectionEventsBased(val eventBusActor: ActorRef, documentStore: DocumentStore[String, AggregateVersion]) extends ProjectionActor {

  protected val listeners = List(EventListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart]): Unit = event match {
    case ShoppingCartCreated(name) =>
      documentStore.insertDocument(aggregateId.asLong, name, version)
    case ShoppingCartDuplicated(baseId, baseVersion) => println("sorry :(")
      //documentStore.insertDocument(aggregateId.asLong, "???", version)
    case ItemAdded(name) =>
      val document = documentStore.getDocument(aggregateId.asLong)
      documentStore.updateDocument(aggregateId.asLong, document.get.document, version)
    case ItemRemoved(id) =>
      val document = documentStore.getDocument(aggregateId.asLong)
      documentStore.updateDocument(aggregateId.asLong, document.get.document, version)
    case ShoppingCartDeleted() =>
      documentStore.removeDocument(aggregateId.asLong)
    case ShoppingCartChangesUndone(count) => println("Sorry :(")
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! documentStore.findAll().values.map(_.document).toVector
  }
}



class ShoppingCartsListProjectionAggregatesBased(val eventBusActor: ActorRef, documentStore: DocumentStore[String, AggregateVersion]) extends ProjectionActor {
  protected val listeners =  List(AggregateListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, aggregateRoot: Option[ShoppingCart]): Unit = aggregateRoot match {
    case Some(a) =>
      val document = documentStore.getDocument(aggregateId.asLong)
      document match {
        case Some(d) => documentStore.updateDocument(aggregateId.asLong, a.name, version)
        case None => documentStore.insertDocument(aggregateId.asLong, a.name, version)
      }
    case None =>
      documentStore.removeDocument(aggregateId.asLong)
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! documentStore.findAll().values.map(_.document).toVector
  }
}
