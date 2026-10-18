package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}

import org.apache.pekko.actor.{ActorRef, Terminated}
import io.reactivecqrs.core.projection.SubscribableProjectionActor._

import scala.collection.mutable
import scala.concurrent.duration.DurationDouble
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

/** The messages of a projection that pushes its updates to subscribers (chapter 11). */
object SubscribableProjectionActor {

  /** How long a subscription lives without a renewal, in milliseconds: 10 minutes. */
  val defaultSubscriptionTTL = 600000

  /** F216: the most subscriptions a projection keeps, and the most updates its cache keeps. */
  val MaxSubscriptions = 100000
  val MaxCachedUpdates = 10000

  /** To a subscriber: its subscription is registered under `subscriptionId`. */
  case class SubscribedForProjectionUpdates(subscriptionId: String)

  /** Ends these subscriptions; answered `ProjectionSubscriptionsCancelled`. */
  case class CancelProjectionSubscriptions(subscriptions: List[String])

  /** Keeps a subscription alive for another `subscriptionTTL`. */
  case class RenewSubscription(subscriptionId: String)

  /** To a subscriber: these subscriptions ended (cancelled, or their id was taken by another subscriber). */
  case class ProjectionSubscriptionsCancelled(subscriptions: List[String])

  /** To a subscriber: an update its filter let through, as the filter made it. */
  case class SubscriptionUpdated[UPDATE](subscriptionId: String, data: UPDATE)

  /** The projection's own tick (every minute): ends the subscriptions idle for longer than `subscriptionTTL` and trims
    * the updates cache. */
  case object ClearIdleSubscriptions

  /** From a retired build to its successor (D57): the push subscriptions it held, which the successor keeps serving.
    * Their filters move with them, so a filter must not use the old build's stores. */
  case class SubscriptionsHandedOver(subscriptions: Seq[SubscriptionInfo])

  private case class SendUpdate(update: Any, typeName: String)
}

/** An update kept in the updates cache, with when it arrived. */
case class UpdateCacheEntry(arrived: Instant, value: Any)

/** A push subscription: who gets the updates of the type `typeName` that `acceptor` lets through, renewed last at
  * `renewal`. */
case class SubscriptionInfo(subscriptionId: String, listener: ActorRef, acceptor: _ => Option[_], typeName: String, renewal: Instant)

/**
  * A projection that pushes what its listeners send (`sendUpdate`) to the subscribers whose filters take it, after the
  * listener's transaction commits (F161).
  *
  * @param updatesCacheTTL how long updates are kept, so that a new subscription gets those it missed
  *                        (`missedUpdatesFilter`); `Duration.ZERO` keeps none
  * @param subscriptionTTL how long a subscription lives without a `RenewSubscription`, in milliseconds
  *
  * F216: at most `MaxSubscriptions` subscriptions (one beyond is answered `ProjectionSubscriptionsCancelled` at once) and
  * `MaxCachedUpdates` cached updates; a subscriber that stops loses its subscriptions at once; a cancel or an expiry costs
  * the same however many there are. Each update still runs every filter of its type.
  */
abstract class SubscribableProjectionActor(updatesCacheTTL: Duration = Duration.ZERO, subscriptionTTL: Int = SubscribableProjectionActor.defaultSubscriptionTTL, options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends ProjectionActor(options) {

  /** The projection's own subscription messages: each calls `handleSubscribe` with its filter. */
  protected def receiveSubscriptionRequest: Receive

  /** `receiveSubscriptionRequest` and the library's subscription messages. */
  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptionsToCancel) =>
      subscriptionsToCancel.foreach(handleUnsubscribe)
      sender() ! ProjectionSubscriptionsCancelled(subscriptionsToCancel)
    case RenewSubscription(subscriptionId) => renewSubscription(subscriptionId)
    case ClearIdleSubscriptions => clearIdleSubscriptions()
    case SendUpdate(u, typeName) => deliverUpdate(u, typeName)
    case SubscriptionsHandedOver(handed) => handed.foreach(add)
    case Terminated(listener) if byListener.contains(listener) => byListener.get(listener).toList.flatten.foreach(handleUnsubscribe)
  }

  override protected def onRetired(successor: Option[ActorRef]): Unit =
    if (subscriptions.nonEmpty) successor.foreach(_ ! SubscriptionsHandedOver(subscriptions.values.toVector))

  override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptions = mutable.HashMap[String, SubscriptionInfo]()
  private val subscriptionsPerType = mutable.HashMap[String, mutable.LinkedHashSet[String]]()
  private val byListener = mutable.HashMap[ActorRef, Set[String]]() // watched, so a stopped one's go (F216)
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

  /** Registers `listener` for the updates of type `DATA` that `filter` turns into an `UPDATE`, under `subscriptionId`
    * (an id used again replaces its subscription), answers `SubscribedForProjectionUpdates`, and sends the cached
    * updates that `missedUpdatesFilter` picks. */
  protected def handleSubscribe[DATA: TypeTag, UPDATE](subscriptionId: String, listener: ActorRef,
                                                                 filter: DATA => Option[UPDATE],
                                                                 missedUpdatesFilter: DATA => Boolean = (d: DATA) => false): Unit = {
    try {
      val typeName = typeTag[DATA].toString()

      // F161: an id used again replaces its subscription; a listener it took the id from is told it is cancelled.
      subscriptions.get(subscriptionId).filter(_.listener != listener)
        .foreach(replaced => replaced.listener ! ProjectionSubscriptionsCancelled(List(subscriptionId)))
      if (subscriptions.size >= MaxSubscriptions && !subscriptions.contains(subscriptionId)) {
        log.warning("Projection " + projectionName + " holds " + MaxSubscriptions + " subscriptions; " + subscriptionId + " is refused")
        listener ! ProjectionSubscriptionsCancelled(List(subscriptionId))
        return
      }
      add(SubscriptionInfo(subscriptionId, listener, filter, typeName, Instant.now()))

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

  private def add(subscription: SubscriptionInfo): Unit = {
    handleUnsubscribe(subscription.subscriptionId)
    subscriptions += subscription.subscriptionId -> subscription
    subscriptionsPerType.getOrElseUpdate(subscription.typeName, mutable.LinkedHashSet()) += subscription.subscriptionId
    if (!byListener.contains(subscription.listener)) context.watch(subscription.listener)
    byListener(subscription.listener) = byListener.getOrElse(subscription.listener, Set.empty) + subscription.subscriptionId
  }

  // Listeners call this, and in parallel mode they run on pool threads: the update reaches the subscriptions through a
  // message, so the subscription maps are only touched on the actor's thread. F161: it is sent once the listener's
  // transaction has committed, so a batch that rolls back, or is tried again, pushes nothing it did not store.
  /** Pushes `u` to the subscriptions of its type, once the listener's transaction commits. */
  protected def sendUpdate[DATA: TypeTag](u: DATA): Unit = {
    val update = SendUpdate(u, typeTag[DATA].toString())
    afterCommit(self ! update)
  }

  private def trimUpdatesCache(): Unit = {
    val shouldArriveAfter = Instant.now().minus(updatesCacheTTL)
    while (updatesCache.headOption.exists(e => e.arrived.isBefore(shouldArriveAfter)) || updatesCache.size > MaxCachedUpdates) updatesCache.dequeue()
  }

  private def deliverUpdate(u: Any, typeName: String): Unit = {
    try {
      if(updatesCacheTTL != Duration.ZERO) {
        updatesCache += UpdateCacheEntry(Instant.now(), u)
        trimUpdatesCache()
      }

      subscriptionsPerType.get(typeName).toList.flatten.flatMap(subscriptions.get) foreach { subscription =>
        // F161: each subscription on its own: a filter that throws keeps the update from no other.
        Try(subscription.acceptor.asInstanceOf[Any => Option[_]](u)) match { // and translate data to message for subscriber
          case Success(result) => result.foreach(r => subscription.listener ! SubscriptionUpdated(subscription.subscriptionId, r)) // and send message if not None
          case Failure(e) => log.error(e, "Projection " + projectionName + ": the filter of subscription " + subscription.subscriptionId + " failed on an update")
        }
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
    val idle = subscriptions.values.filter(_.renewal.plusMillis(subscriptionTTL).isBefore(now)).map(_.subscriptionId).toVector
    idle.foreach(handleUnsubscribe)
    if (idle.nonEmpty && log.isDebugEnabled) log.debug("Invalidated idle subscriptions " + idle + ", remains " + subscriptions.size + " subscriptions")
  }

  private def handleUnsubscribe(subscriptionId: String): Unit = subscriptions.remove(subscriptionId).foreach { subscription =>
    subscriptionsPerType.get(subscription.typeName).foreach { ids =>
      ids -= subscriptionId
      if (ids.isEmpty) subscriptionsPerType -= subscription.typeName
    }
    val left = byListener.getOrElse(subscription.listener, Set.empty) - subscriptionId
    if (left.nonEmpty) byListener(subscription.listener) = left else {
      byListener -= subscription.listener
      context.unwatch(subscription.listener)
    }
    if (log.isDebugEnabled) log.debug("Subscription cancelled for subscription id " + subscriptionId + ", subscriptions count: " + subscriptions.size)
  }
}
