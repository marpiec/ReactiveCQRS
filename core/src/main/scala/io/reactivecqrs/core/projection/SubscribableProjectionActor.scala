package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}

import org.apache.pekko.actor.ActorRef
import io.reactivecqrs.core.projection.SubscribableProjectionActor._

import scala.collection.mutable
import scala.concurrent.duration.DurationDouble
import scala.reflect.runtime.universe._

object SubscribableProjectionActor {

  val defaultSubscriptionTTL = 600000 // 10 minutes

  case class SubscribedForProjectionUpdates(subscriptionId: String)

  case class CancelProjectionSubscriptions(subscriptions: List[String])

  case class RenewSubscription(subscriptionId: String)

  case class ProjectionSubscriptionsCancelled(subscriptions: List[String])

  case class SubscriptionUpdated[UPDATE](subscriptionId: String, data: UPDATE)

  case object ClearIdleSubscriptions

  /** From a retired build to its successor (D57): the push subscriptions it held, which the successor keeps serving.
    * Their filters move with them, so a filter must not use the old build's stores. */
  case class SubscriptionsHandedOver(subscriptions: Seq[SubscriptionInfo])

  private case class SendUpdate(update: Any, typeName: String)
}

case class UpdateCacheEntry(arrived: Instant, value: Any)

case class SubscriptionInfo(subscriptionId: String, listener: ActorRef, acceptor: _ => Option[_], typeName: String, renewal: Instant)

abstract class SubscribableProjectionActor(updatesCacheTTL: Duration = Duration.ZERO, subscriptionTTL: Int = SubscribableProjectionActor.defaultSubscriptionTTL, options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends ProjectionActor(options) {

  protected def receiveSubscriptionRequest: Receive

  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptionsToCancel) =>
      subscriptionsToCancel.foreach(handleUnsubscribe)
      sender() ! ProjectionSubscriptionsCancelled(subscriptionsToCancel)
    case RenewSubscription(subscriptionId) => renewSubscription(subscriptionId)
    case ClearIdleSubscriptions => clearIdleSubscriptions()
    case SendUpdate(u, typeName) => deliverUpdate(u, typeName)
    case SubscriptionsHandedOver(handed) => handed.foreach { subscription =>
      subscriptions += subscription.subscriptionId -> subscription
      subscriptionsPerType += subscription.typeName -> (subscription.subscriptionId :: subscriptionsPerType.getOrElse(subscription.typeName, Nil))
    }
  }

  override protected def onRetired(successor: Option[ActorRef]): Unit =
    if (subscriptions.nonEmpty) successor.foreach(_ ! SubscriptionsHandedOver(subscriptions.values.toVector))

  override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptions = mutable.HashMap[String, SubscriptionInfo]()
  private val subscriptionsPerType = mutable.HashMap[String, List[String]]()
  private val updatesCache: mutable.Queue[UpdateCacheEntry] = mutable.Queue.empty

  // Expires idle subscriptions and trims the updates cache; created with each instance and cancelled when it stops (F95).
  private val ticks = context.system.scheduler.scheduleWithFixedDelay(1.minute, 1.minute, self, ClearIdleSubscriptions)(context.dispatcher)

  override def postStop(): Unit = {
    ticks.cancel()
    super.postStop()
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  protected def handleSubscribe[DATA: TypeTag, UPDATE](subscriptionId: String, listener: ActorRef,
                                                                 filter: DATA => Option[UPDATE],
                                                                 missedUpdatesFilter: DATA => Boolean = (d: DATA) => false): Unit = {
    try {
      val typeName = typeTag[DATA].toString()

      subscriptions += subscriptionId -> SubscriptionInfo(subscriptionId, listener, filter, typeName, Instant.now())
      subscriptionsPerType += typeName -> (subscriptionId :: subscriptionsPerType.getOrElse(typeName, Nil))

      listener ! SubscribedForProjectionUpdates(subscriptionId)

      if (updatesCacheTTL != Duration.ZERO) {
        val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
        updatesCache.filter(d => d.arrived.isAfter(shouldArriveAfter) && missedUpdatesFilter(d.value.asInstanceOf[DATA]))
          .map(d => filter(d.value.asInstanceOf[DATA])).filter(_.isDefined)
          .foreach(result => listener ! SubscriptionUpdated(subscriptionId, result.get))
      }

      if (log.isDebugEnabled) {
        log.debug("New subscription for " + listener.path.toStringWithoutAddress + " on " + typeName + ", id: " + subscriptionId + ", subscriptions count: " + subscriptions.size)
      }
    } catch {
      case e: Exception => log.error(e, "Exception while handling subscription") //log only because projection update is more important than subscription
    }
  }

  // Listeners call this, and in parallel mode they run on pool threads: the update reaches the subscriptions through a
  // message, so the subscription maps are only touched on the actor's thread.
  protected def sendUpdate[DATA: TypeTag](u: DATA): Unit = self ! SendUpdate(u, typeTag[DATA].toString())

  private def trimUpdatesCache(): Unit = {
    val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
    updatesCache.dequeueAll(e => e.arrived.isBefore(shouldArriveAfter))
  }

  private def deliverUpdate(u: Any, typeName: String): Unit = {
    try {
      if(updatesCacheTTL != Duration.ZERO) {
        updatesCache += UpdateCacheEntry(Instant.now(), u)
        trimUpdatesCache()
      }

      // Find subscriptions for given type
      //TODO what to do if subscriptions.get returns none?
      subscriptionsPerType.getOrElse(typeName, List.empty).filter(s => {
        if(subscriptions.contains(s)) {
          true
        } else {
          log.warning("Subscription not found for key " + s + "!")
          false
        }
      }).map(subscriptions) foreach { subscription =>
        val result = subscription.acceptor.asInstanceOf[Any => Option[_]](u) // and translate data to message for subscriber
        result.foreach(r => subscription.listener ! SubscriptionUpdated(subscription.subscriptionId, r))// and send message if not None
      }
    } catch {
      case e: Exception => log.error(e, "Exception while handling subscription update") //log only because projection update is more important than subscription
    }
  }

  // F133: a subscription this instance does not know expired, or was held by an instance that stopped (the projection's
  // singleton moved, M5): the renewer is told it is cancelled, so it subscribes again instead of waiting for updates.
  private def renewSubscription(subscriptionId: String): Unit = {
    subscriptions.get(subscriptionId) match {
      case None => sender() ! ProjectionSubscriptionsCancelled(List(subscriptionId))
      case Some(subscription) => subscriptions += subscriptionId -> subscription.copy(renewal = Instant.now())
    }
  }

  private def clearIdleSubscriptions(): Unit = {
    trimUpdatesCache()
    val now = Instant.now()
    val subscriptionsToInvalidate = subscriptions.values.filter(_.renewal.plusMillis(subscriptionTTL).isBefore(now))

    subscriptions --= subscriptionsToInvalidate.map(_.subscriptionId)

    subscriptionsToInvalidate.foreach(subscription => {
      val subscriptionsForType = subscriptionsPerType(subscription.typeName)

      val subscriptionId = subscription.subscriptionId
      if(subscriptionsForType.length == 1) {
        subscriptionsPerType -= subscription.typeName
      } else {
        subscriptionsPerType += subscription.typeName -> subscriptionsForType.filterNot(_ == subscriptionId)
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
}
