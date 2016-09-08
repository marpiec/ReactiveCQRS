package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}

import akka.actor.ActorRef
import io.reactivecqrs.core.util.RandomUtil
import io.reactivecqrs.core.projection.SubscribableProjectionActor._

import scala.collection.mutable
import scala.concurrent.duration.DurationDouble
import scala.reflect.runtime.universe._

object SubscribableProjectionActor {

  val subscriptionTTL = 600000 // 10 minutes

  case class SubscribedForProjectionUpdates(subscriptionCode: String, subscriptionId: String)

  case class CancelProjectionSubscriptions(subscriptions: List[String])

  case class RenewSubscription(subscriptionId: String)

  case class ProjectionSubscriptionsCancelled(subscriptions: List[String])

  case class SubscriptionUpdated[UPDATE, METADATA](subscriptionId: String, data: UPDATE, metadata: METADATA)

  case object ClearIdleSubscriptions
}

case class UpdateCacheEntry(arrived: Instant, value: Any)

case class SubscriptionInfo(subscriptionId: String, listener: ActorRef, acceptor: _ => Option[_], typeName: String, renewal: Instant)

abstract class SubscribableProjectionActor(updatesCacheTTL: Duration = Duration.ZERO) extends ProjectionActor {

  protected def receiveSubscriptionRequest: Receive

  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptionsToCancel) =>
      subscriptionsToCancel.foreach(handleUnsubscribe)
      sender ! ProjectionSubscriptionsCancelled(subscriptionsToCancel)
    case RenewSubscription(subscriptionId) => renewSubscription(subscriptionId)
    case ClearIdleSubscriptions => clearIdleSubscriptions()
  }

  override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptions = mutable.HashMap[String, SubscriptionInfo]()
  private val subscriptionsPerType = mutable.HashMap[String, List[String]]()
  private val updatesCache: mutable.Queue[UpdateCacheEntry] = mutable.Queue.empty

  context.system.scheduler.schedule(1.minute, 1.minute, self, ClearIdleSubscriptions)(context.dispatcher)

  protected def handleSubscribe[DATA: TypeTag, UPDATE, METADATA](code: String, listener: ActorRef,
                                                                 filter: (DATA) => Option[(UPDATE, METADATA)],
                                                                 missedUpdatesFilter: DATA => Boolean = (d: DATA) => false): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val typeName = typeTag[DATA].toString()

    subscriptions += subscriptionId -> SubscriptionInfo(subscriptionId, listener, filter, typeName, Instant.now())
    subscriptionsPerType += typeName -> (subscriptionId :: subscriptionsPerType.getOrElse(typeName, Nil))

    listener ! SubscribedForProjectionUpdates(code, subscriptionId)

    if(updatesCacheTTL != Duration.ZERO) {
      val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
      updatesCache.filter(d => d.arrived.isAfter(shouldArriveAfter) && missedUpdatesFilter(d.value.asInstanceOf[DATA]))
        .map(d => filter(d.value.asInstanceOf[DATA])).filter(_.isDefined)
        .foreach(result => listener ! SubscriptionUpdated(subscriptionId, result.get._1, result.get._2))
    }

    if(log.isDebugEnabled) {
      log.debug("New subscription for " + listener.path.toStringWithoutAddress + " on " + typeName+", id: "+subscriptionId+", subscriptions count: "+subscriptions.size)
    }
  }

  protected def sendUpdate[DATA: TypeTag](u: DATA) = {

    if(updatesCacheTTL != Duration.ZERO) {
      updatesCache += UpdateCacheEntry(Instant.now(), u)
      val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
      updatesCache.dequeueAll(e => e.arrived.isBefore(shouldArriveAfter))
    }

    // Find subscriptions for given type
    //TODO what to do if subscriptions.get rturns none?
    subscriptionsPerType.getOrElse(typeTag[DATA].toString(), List.empty).filter(s => {
      if(subscriptions.contains(s)) {
        true
      } else {
        log.warning("Subscription not found for key " + s + "!")
        false
      }
    }).map(subscriptions) foreach { subscription =>
      for {
        result <- subscription.acceptor.asInstanceOf[DATA => Option[(_, _)]](u) // and translate data to message for subscriber
      } yield subscription.listener ! SubscriptionUpdated(subscription.subscriptionId, result._1, result._2) // and send message if not None
    }
  }

  private def renewSubscription(subscriptionId: String): Unit = {
    subscriptions.get(subscriptionId) match {
      case None => () // ??? Should we send info that subscription is no longer valid? Or should we create new one?
      case Some(subscription) => subscriptions += subscriptionId -> subscription.copy(renewal = Instant.now())
    }
  }

  private def clearIdleSubscriptions(): Unit = {
    val now = Instant.now()
    val subscriptionsToInvalidate = subscriptions.values.filter(_.renewal.plusMillis(subscriptionTTL).isBefore(now))

    subscriptions --= subscriptionsToInvalidate.map(_.subscriptionId)

    subscriptionsToInvalidate.foreach(subscription => {
      val subscriptionsForType = subscriptionsPerType(subscription.typeName)

      val subscriptionId = subscription.subscriptionId
      if(subscriptionsForType.length == 1) {
        subscriptionsPerType -= subscriptionId
      } else {
        subscriptionsPerType += subscriptionId -> subscriptionsForType.filterNot(_ == subscriptionId)
      }
    })

    if(subscriptionsToInvalidate.nonEmpty && log.isDebugEnabled) {
      log.debug("Invalidated idle subscriptions " + subscriptionsToInvalidate+ ", remains " + subscriptions.size+" subscriptions")
    }

  }

  private def handleUnsubscribe(subscriptionId: String): Unit = {

    subscriptions.get(subscriptionId) match {
      case None => () // nothing to do
      case Some(subscription) =>
        subscriptions -= subscriptionId

        val subscriptionsForType = subscriptionsPerType(subscription.typeName)

        if(subscriptionsForType.length == 1) {
          subscriptionsPerType -= subscription.typeName
        } else {
          subscriptionsPerType += subscription.typeName -> subscriptionsForType.filterNot(_ == subscriptionId)
        }
    }

    if(log.isDebugEnabled) {
      log.debug("Subscription cancelled for subscription id " + subscriptionId+", subscriptions count: "+subscriptions.size)
    }
  }

  // utilities

  private val randomUtil = new RandomUtil

  private def generateNextSubscriptionId: String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while (subscriptions.contains(subscriptionId))
    subscriptionId
  }
}
