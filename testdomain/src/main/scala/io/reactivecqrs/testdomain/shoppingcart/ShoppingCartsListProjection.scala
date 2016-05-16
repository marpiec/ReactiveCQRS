package io.reactivecqrs.testdomain.shoppingcart

import java.time.Instant

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{Aggregate, AggregateVersion, Event, GetAggregateForVersion}
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.ProjectionActor
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjectionEventsBased(val eventBusActor: ActorRef,
                                             shoppingCartCommandBus: ActorRef,
                                             documentStore: DocumentStore[String, AggregateVersion]) extends ProjectionActor {

  protected val listeners = List(EventListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart], userId: UserId, timestamp: Instant): Unit = event match {
    case ShoppingCartCreated(name) =>
      documentStore.insertDocument(aggregateId.asLong, name, version)
    case ShoppingCartDuplicated(baseId, baseVersion) =>
      implicit val timeout = Timeout(5 seconds)
      val future = (shoppingCartCommandBus ? GetAggregateForVersion(baseId, baseVersion)).mapTo[Try[Aggregate[ShoppingCart]]] // TODO try to do this without ask
      val baseShoppingCart: Try[Aggregate[ShoppingCart]]= Await.result(future, 5 seconds)
      documentStore.insertDocument(aggregateId.asLong, baseShoppingCart.get.aggregateRoot.get.name, version)
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
