package io.reactivecqrs.core.projection

import akka.actor.ActorRef
import akka.event.LoggingReceive
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.projection.Subscribable.{SubscribedForAggregateInfo, InfoSubscriptionsCanceled, SubscribeForAggregateInfoUpdates, CancelInfoSubscriptions}
import io.reactivecqrs.core.util.RandomUtil

import scala.collection.mutable

object Subscribable {
  case class SubscribeForAggregateInfoUpdates(subscriptionCode: String, listener: ActorRef, aggregateId: AggregateId)

  case class SubscribedForAggregateInfo(subscriptionCode: String, subscriptionId: String)

  case class CancelInfoSubscriptions(subscriptions: List[String])

  case class InfoSubscriptionsCanceled(subscriptions: List[String])
}

trait Subscribable[PROJECTION_MODEL] extends ProjectionActor {

  abstract override protected def receiveUpdate: Receive = LoggingReceive {
      case SubscribeForAggregateInfoUpdates(subscriptionCode, listener, aggregateId) => handleSubscription(subscriptionCode, listener, aggregateId)
      case CancelInfoSubscriptions(subscriptions) => sender ! InfoSubscriptionsCanceled(subscriptions.flatMap(handleUnsubscribe))
  } orElse super.receiveUpdate

  protected def sendUpdate(aggregateId: AggregateId, state: PROJECTION_MODEL) = {
    aggregateIdToSubscriptionIds.get(aggregateId) foreach {
      _ foreach {
        subscriptionIdToListener.get(_) foreach {
          _ ! state
        }
      }
    }
  }

  private var subscriptionIdToListener = mutable.HashMap[String, ActorRef]()
  private var aggregateIdToSubscriptionIds = mutable.HashMap[AggregateId, List[String]]()

  private val randomUtil = new RandomUtil

  private def generateNextSubscriptionId: String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while (subscriptionIdToListener.contains(subscriptionId))
    subscriptionId
  }

  private def handleSubscription(subscriptionCode: String, listener: ActorRef, aggregateId: AggregateId) = {
    val subscriptionId: String = generateNextSubscriptionId
    val subscriptions: List[String] = aggregateIdToSubscriptionIds.getOrElse(aggregateId, List())
    subscriptionIdToListener += subscriptionId -> listener
    aggregateIdToSubscriptionIds += aggregateId -> (subscriptionId :: subscriptions)
    listener ! SubscribedForAggregateInfo(subscriptionCode, subscriptionId)
  }

  private def handleUnsubscribe(subscriptionId: String): Option[String] = {
    val aggregate: Option[(AggregateId, List[String])] = aggregateIdToSubscriptionIds find { _._2.contains(subscriptionId) }
    aggregate match {
      case Some((aggregateId, subscriptions)) =>
        aggregateIdToSubscriptionIds += aggregateId -> subscriptions.filterNot(_ == subscriptionId)
      case None => ()
    }
    subscriptionIdToListener.get(subscriptionId) match {
      case Some(subscriber) =>
        subscriptionIdToListener -= subscriptionId
        Some(subscriptionId)
      case None => None
    }
  }
}