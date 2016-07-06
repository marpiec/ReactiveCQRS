package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}

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

case class UpdateCacheEntry(arrived: Instant, value: Any)

abstract class SubscribableProjectionActor(updatesCacheTTL: Duration = Duration.ZERO) extends ProjectionActor {

  protected def receiveSubscriptionRequest: Receive

  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptions) => sender ! ProjectionSubscriptionsCancelled(subscriptions.flatMap(handleUnsubscribe))
  }

  override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptionIdToListener = mutable.HashMap[String, ActorRef]()
  private val subscriptionIdToAcceptor = mutable.HashMap[String, _ => Option[_]]()
  private val typeToSubscriptionId = mutable.HashMap[String, List[String]]()

  private val updatesCache: mutable.Queue[UpdateCacheEntry] = mutable.Queue.empty

  protected def handleSubscribe[DATA: TypeTag, UPDATE, METADATA](code: String, listener: ActorRef,
                                                                 filter: (DATA) => Option[(UPDATE, METADATA)],
                                                                 missedUpdatesFilter: DATA => Boolean = (d: DATA) => false): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val typeTagString = typeTag[DATA].toString()

    subscriptionIdToListener += subscriptionId -> listener
    subscriptionIdToAcceptor += subscriptionId -> filter
    typeToSubscriptionId += typeTagString -> (subscriptionId :: typeToSubscriptionId.getOrElse(typeTagString, Nil))

    listener ! SubscribedForProjectionUpdates(code, subscriptionId)

    if(updatesCacheTTL != Duration.ZERO) {
      val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
      updatesCache.filter(d => d.arrived.isAfter(shouldArriveAfter) && missedUpdatesFilter(d.value.asInstanceOf[DATA]))
        .map(d => filter(d.value.asInstanceOf[DATA])).filter(_.isDefined)
        .foreach(result => listener ! SubscriptionUpdated(subscriptionId, result.get._1, result.get._2))
    }
  }

  protected def sendUpdate[DATA: TypeTag](u: DATA) = {


    if(updatesCacheTTL != Duration.ZERO) {
      updatesCache += UpdateCacheEntry(Instant.now(), u)
      val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
      updatesCache.dequeueAll(e => e.arrived.isBefore(shouldArriveAfter))
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
