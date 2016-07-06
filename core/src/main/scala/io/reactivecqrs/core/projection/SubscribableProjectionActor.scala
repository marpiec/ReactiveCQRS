package io.reactivecqrs.core.projection

import akka.actor.ActorRef
import io.reactivecqrs.core.util.RandomUtil
import io.reactivecqrs.core.projection.SubscribableProjectionActor._

import scala.collection.mutable
import scala.reflect.runtime.universe._

object SubscribableProjectionActor {
  case class SubscribedForProjectionUpdates(subscriptionCode: String, subscriptionId: String)

  case class CancelProjectionSubscriptions(subscriptions: List[String])

  case class ProjectionSubscriptionsCancelled(subscriptions: List[String])

  case class SubscriptionUpdated[UPDATE, METADATA](subscriptionId: String, data: UPDATE, metadata: METADATA)
}

trait SubscribableProjectionActor extends ProjectionActor {

  protected def receiveSubscriptionRequest: Receive

  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptions) => sender ! ProjectionSubscriptionsCancelled(subscriptions.flatMap(handleUnsubscribe))
  }

  abstract override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptionIdToListener = mutable.HashMap[String, ActorRef]()
  private val subscriptionIdToAcceptor = mutable.HashMap[String, _ => Option[_]]()
  private val typeToSubscriptionId = mutable.HashMap[String, List[String]]()

  private val updatesCache: mutable.Queue[Any] = mutable.Queue.empty
  private val MAX_SUBSCRIPTION_CACHE_SIZE = 100

  protected def handleSubscribe[DATA: TypeTag, UPDATE, METADATA](code: String, listener: ActorRef,
                                                                 filter: (DATA) => Option[(UPDATE, METADATA)],
                                                                 missedUpdatesFilter: DATA => Boolean = (d: DATA) => false): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val typeTagString = typeTag[DATA].toString()

    subscriptionIdToListener += subscriptionId -> listener
    subscriptionIdToAcceptor += subscriptionId -> filter
    typeToSubscriptionId += typeTagString -> (subscriptionId :: typeToSubscriptionId.getOrElse(typeTagString, Nil))

    listener ! SubscribedForProjectionUpdates(code, subscriptionId)

    updatesCache.filter(d => missedUpdatesFilter(d.asInstanceOf[DATA]))
      .map(d => filter(d.asInstanceOf[DATA])).filter(_.isDefined)
      .foreach(result => listener ! SubscriptionUpdated(subscriptionId, result.get._1, result.get._2))
  }

  protected def sendUpdate[DATA: TypeTag](u: DATA) = {

    updatesCache += u
    if(updatesCache.size > MAX_SUBSCRIPTION_CACHE_SIZE) {
      updatesCache.dequeue()
    }

    typeToSubscriptionId.get(typeTag[DATA].toString()) foreach { subscriptions =>
      for {
        subscriptionId: String <- subscriptions
        listener: ActorRef <- subscriptionIdToListener.get(subscriptionId)
        filter <- subscriptionIdToAcceptor.get(subscriptionId)
        result <- filter.asInstanceOf[DATA => Option[(_, _)]](u)
      } yield listener ! SubscriptionUpdated(subscriptionId, result._1, result._2)
    }
  }

  private def handleUnsubscribe(subscriptionId: String): Option[String] = {
    typeToSubscriptionId.find { _._2.contains(subscriptionId) } match {
      case Some((aggregateId, subscriptions)) =>
        val filtered = subscriptions.filterNot(_ == subscriptionId)
        if (filtered.isEmpty) {
          typeToSubscriptionId -= aggregateId
        } else {
          typeToSubscriptionId += aggregateId -> filtered
        }
      case None => ()
    }
    subscriptionIdToAcceptor.get(subscriptionId) match {
      case Some(_) =>
        subscriptionIdToAcceptor -= subscriptionId
      case None => ()
    }
    subscriptionIdToListener.get(subscriptionId) match {
      case Some(subscriber) =>
        subscriptionIdToListener -= subscriptionId
        Some(subscriptionId)
      case None => None
    }
  }

  // utilities

  private val randomUtil = new RandomUtil

  private def generateNextSubscriptionId: String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while (subscriptionIdToListener.contains(subscriptionId))
    subscriptionId
  }
}
