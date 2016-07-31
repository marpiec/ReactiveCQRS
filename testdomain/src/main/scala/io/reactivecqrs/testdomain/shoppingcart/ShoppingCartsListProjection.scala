package io.reactivecqrs.testdomain.shoppingcart

import java.time.Instant

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{Aggregate, AggregateVersion, Event, GetAggregateForVersion}
import io.reactivecqrs.core.documentstore.{DocumentStore, DocumentWithMetadata}
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManagerApi
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames
import scalikejdbc.DBSession

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


object ShoppingCartsListProjection {
  case class GetAllCartsNames()
}

class ShoppingCartsListProjectionEventsBased(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                             val subscriptionsState: SubscriptionsState,
                                             shoppingCartCommandBus: ActorRef,
                                             documentStore: DocumentStore[String, AggregateVersion]) extends ProjectionActor {

  protected val listeners = List(EventListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[ShoppingCart], userId: UserId, timestamp: Instant) = { implicit session: DBSession =>
    event match {
      case ShoppingCartCreated(name) =>
        documentStore.insertDocument(aggregateId.asLong, name, version)
      case ShoppingCartDuplicated(baseId, baseVersion) =>
        implicit val timeout = Timeout(60 seconds)
        val future = (shoppingCartCommandBus ? GetAggregateForVersion(baseId, baseVersion)).mapTo[Try[Aggregate[ShoppingCart]]] // TODO try to do this without ask
      val baseShoppingCart: Try[Aggregate[ShoppingCart]] = Await.result(future, 60 seconds)
        documentStore.insertDocument(aggregateId.asLong, baseShoppingCart.get.aggregateRoot.get.name, version)
      case ItemAdded(name) =>
        documentStore.updateDocument(aggregateId.asLong, documentWithMetadata => {
          DocumentWithMetadata(documentWithMetadata.document, version)
        })
      case ItemRemoved(id) =>
        documentStore.updateDocument(aggregateId.asLong, documentWithMetadata => {
          DocumentWithMetadata(documentWithMetadata.document, version)
        })
      case ShoppingCartDeleted() =>
        documentStore.removeDocument(aggregateId.asLong)
      case ShoppingCartChangesUndone(count) => println("Sorry :(")
    }
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! documentStore.findAll().values.map(_.document).toVector
  }
}



class ShoppingCartsListProjectionAggregatesBased(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                                 val subscriptionsState: SubscriptionsState,
                                                 documentStore: DocumentStore[String, AggregateVersion]) extends ProjectionActor {
  protected val listeners =  List(AggregateListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion, eventsCount: Int, aggregateRoot: Option[ShoppingCart]) = { implicit session: DBSession =>
    aggregateRoot match {
      case Some(a) =>
        if(version.asInt == eventsCount) {
          documentStore.insertDocument(aggregateId.asLong, a.name, version)
        } else {
          documentStore.overwriteDocument(aggregateId.asLong, a.name, version)
        }
      case None =>
        documentStore.removeDocument(aggregateId.asLong)
    }
  }

  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! documentStore.findAll().values.map(_.document).toVector
  }
}
