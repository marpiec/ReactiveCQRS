package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.pekko.actor.{Actor, ActorRef, Cancellable, Status}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api._
import io.reactivecqrs.core.delivery.SequenceGate
import io.reactivecqrs.core.delivery.SequenceGate.{AlreadyApplied, Apply, Gap}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.{Backoff, MyActorLogging}
import org.slf4j.Logger
import scalikejdbc.DBSession

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

sealed trait DelayedQuery {
  val until: Instant
  val respondTo: ActorRef
}
private case class SyncDelayedQuery(until: Instant, respondTo: ActorRef, search: () => Option[Any]) extends DelayedQuery
private case class AsyncDelayedQuery(until: Instant, respondTo: ActorRef, search: () => Future[Option[Any]]) extends DelayedQuery
private case class AsyncDelayedQueryCustom[T](until: Instant, respondTo: ActorRef, search: () => Future[T], found: T => Boolean) extends DelayedQuery

case object ClearProjectionData
case object ProjectionDataCleared

case object GetSubscribedAggregates
case class SubscribedAggregates(projectionName: String, projectionVersion: Int, aggregates: Set[AggregateType])

// D20a: a projection's failed subscriptions, and what an operator can do about them.
case object GetProjectionStatus
/** `retrying`: aggregates whose listener failed and is retried now; `waitingForGap`: aggregates waiting for missing
  * versions to be re-published. */
case class ProjectionStatus(projectionName: String, failures: Seq[SubscriptionFailure], retrying: Seq[AggregateId], waitingForGap: Seq[AggregateId])
/** Has the aggregate's failed versions delivered again; each failure is cleared once its version is applied. */
case class RetryFailedSubscription(aggregateId: AggregateId)
/** Retries every failed subscription, a few per second; answered with `FailedSubscriptionsRetried`. */
case object RetryAllFailedSubscriptions
case class FailedSubscriptionsRetried(count: Int)
/** Marks each failed version of the aggregate as applied without applying it, and continues from the version after. */
case class SkipFailedVersion(aggregateId: AggregateId)
/** The answer to both: how many failed subscriptions of the aggregate were retried or skipped. */
case class FailedSubscriptionsHandled(aggregateId: AggregateId, count: Int)


object ProjectionActorOptions {
  /** About two minutes of retries (D20a): 1, 2, 4, 8, 16, 30, 30 and 30 seconds between nine attempts. */
  val defaultRetries: Backoff = Backoff(1.second, 30.seconds, maxAttempts = 9)
  val DEFAULT = ProjectionActorOptions(0, 1, Set.empty, 0, None)
}

/**
  * @param retries how a listener that fails on an aggregate is retried before the failure is recorded (D20a)
  * @param retryFailuresOnStart retry the failures recorded before the start (D20a)
  * @param gapRequestRepeat how long a request for missing versions waits before it is sent again, doubling up to five
  *                         minutes (D20c)
  * @param gapGiveUpAfter how long missing versions are asked for before the gap is recorded as a failure
  * @param failureRetryInterval the pace of retrying many failures (at start, `RetryAllFailedSubscriptions`)
  */
case class ProjectionActorOptions (groupUpdatesDelayMillis: Long,
                                   minimumDelayVersion: Int,
                                   eventsToProcessImmediately: Set[Class[_]],
                                   parallelUpdateProcessing: Int,
                                   eventsLogger: Option[Logger],
                                   retries: Backoff = ProjectionActorOptions.defaultRetries,
                                   retryFailuresOnStart: Boolean = true,
                                   gapRequestRepeat: FiniteDuration = 10.seconds,
                                   gapGiveUpAfter: FiniteDuration = 10.minutes,
                                   failureRetryInterval: FiniteDuration = 50.millis) {

  def withGroupUpdatesDelayMillis(millis: Long): ProjectionActorOptions = {
    copy(groupUpdatesDelayMillis = millis)
  }

  def withMinimumDelayVersion(version: Int): ProjectionActorOptions = {
    copy(minimumDelayVersion = version)
  }

  def withEventsLogger(logger: Logger): ProjectionActorOptions = {
    copy(eventsLogger = Some(logger))
  }

  /**
   * eventsToProcessImmediately - only works for listeners receiving events, will not work for aggregate only listeners, due to lack of information
   **/
  def withEventsToProcessImmediately(classes: Class[_]*): ProjectionActorOptions = {
    copy(eventsToProcessImmediately = classes.toSet)
  }

  /**
   * Zero means no paralleism. Handling will be done in different threads, but on the same objects, so thread safety must be considered.
   */
  def withParallelUpdateProcessing(parallel: Int): ProjectionActorOptions = {
    copy(parallelUpdateProcessing = parallel)
  }

  def withRetries(retries: Backoff): ProjectionActorOptions = copy(retries = retries)

}

object ListenerOptions {
  val DEFAULT = ListenerOptions(false)
  val DISABLED_TRANSACTION = ListenerOptions(true)
}
case class ListenerOptions(noTransaction: Boolean = false)

/**
  * A subscriber that keeps a read model (D3). Every delivery goes through the sequence gate (D4) on the subscription's
  * stored version, one aggregate at a time per listener kind: duplicates are acknowledged, new versions are applied in
  * one transaction with the new version, and missing versions are asked from the aggregate's owner (D20c). A listener
  * that fails is retried with back-off; then the failure is recorded and deliveries of that aggregate are answered
  * `MessageFailed` until it is retried or skipped (D20a). See docs/design/M2-delivery.md.
  */
abstract class ProjectionActor(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends Actor with MyActorLogging {

  private val className = this.getClass.getSimpleName
  private val subscriberName = this.getClass.getName

  protected val projectionName: String
  protected val subscriptionsState: SubscriptionsState

  protected val listeners:List[Listener[Any]]

  protected val version: Int

  private val executor: Option[ExecutorService] =
    if (options.parallelUpdateProcessing > 1) Some(Executors.newFixedThreadPool(options.parallelUpdateProcessing)) else None
  private val executionContext: Option[ExecutionContext] = executor.map(ExecutionContext.fromExecutor)

  override def postStop(): Unit = executor.foreach(_.shutdown())

  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateType: AggregateType
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventsListener[+AGGREGATE_ROOT: ClassTag](callback: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, Seq[EventInfo[Any]]) => DBSession => Unit]
    val aggregateType: AggregateType = AggregateType.of[AGGREGATE_ROOT]
  }

  protected object EventsListener {
    def apply[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](
        (id: AggregateId, events: Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => listener(id, events),
        ListenerOptions.DISABLED_TRANSACTION)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateListener[+AGGREGATE_ROOT: ClassTag](callback: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, AggregateVersion, Boolean, Option[Any]) => DBSession => Unit]
    val aggregateType: AggregateType = AggregateType.of[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](
        (id: AggregateId, version: AggregateVersion, created: Boolean, root: Option[AGGREGATE_ROOT]) => DBSession => listener(id, version, created, root),
        ListenerOptions.DISABLED_TRANSACTION)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateWithEventsListener[+AGGREGATE_ROOT: ClassTag](callback: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, AggregateVersion, Option[Any], Seq[EventInfo[Any]]) => DBSession => Unit]
    val aggregateType: AggregateType = AggregateType.of[AGGREGATE_ROOT]
  }

  protected object AggregateWithEventsListener {
    def apply[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: ClassTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](
        (id: AggregateId, version: AggregateVersion, root: Option[AGGREGATE_ROOT], events: Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => listener(id, version, root, events),
        ListenerOptions.DISABLED_TRANSACTION)
  }

  object ReplayQueries



  private lazy val eventListenersMap = listenersOfKind[EventsListener[Any]]
  private lazy val aggregateListenersMap = listenersOfKind[AggregateListener[Any]]
  private lazy val aggregateWithEventListenersMap = listenersOfKind[AggregateWithEventsListener[Any]]

  private def listenersOfKind[L <: Listener[Any] : ClassTag]: Map[AggregateType, L] = {
    validateListeners()
    listeners.collect { case l: L => l.aggregateType -> l }.toMap
  }

  // AR7, F80: a message that fails, such as a query, is answered with the failure; a throw would restart the actor
  // and lose the deliveries it holds.
  override def receive: Receive = {
    case message if handlers.isDefinedAt(message) =>
      try handlers(message) catch {
        case NonFatal(e) =>
          log.error(e, "Projection " + projectionName + " failed on " + message.getClass.getSimpleName)
          sender() ! Status.Failure(e)
      }
  }

  private lazy val handlers: Receive = receiveUpdate orElse receiveQuery

  private def validateListeners() = {
    if (listeners.exists(l => l.aggregateType == AggregateType.of[Any] || l.aggregateType == AggregateType.of[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(_.aggregateType.typeName))
    }
  }

  def getSubscribedAggregates: SubscribedAggregates = {
    SubscribedAggregates(projectionName, version, aggregateListenersMap.keySet ++ eventListenersMap.keySet ++ aggregateWithEventListenersMap.keySet)
  }

  protected def receiveUpdate: Receive = {
    case GetSubscriptionRequests =>
      eventBus = Some(sender())
      sender() ! SubscriptionRequests(subscriptionRequests)
      loadFailures()
    case GetSubscribedAggregates => sender() ! getSubscribedAggregates
    case e: IdentifiableEvents[_] =>
      receiveDelivery(Key(SubscriptionType.EVENTS, e.aggregateId), e.aggregateType,
        Pending(e.events.map(_.version), e.events.asInstanceOf[Seq[EventInfo[Any]]], None, e.replayed, sender()))
    case a: AggregateWithType[_] =>
      receiveDelivery(Key(SubscriptionType.AGGREGATES, a.id), a.aggregateType, Pending(a.events, Seq.empty, a.aggregateRoot, a.replayed, sender()))
    case ae: AggregateWithTypeAndEvents[_] =>
      receiveDelivery(Key(SubscriptionType.AGGREGATES_WITH_EVENTS, ae.id), ae.aggregateType,
        Pending(ae.events.map(_.version), ae.events.asInstanceOf[Seq[EventInfo[Any]]], ae.aggregateRoot, ae.replayed, sender()))
    case Drain(key, token) => queues.get(key).foreach(queue => queue.scheduled match {
      // While a batch runs, the queue is drained when it ends: one batch per queue at a time.
      case Some((`token`, _)) if queue.processing => queue.scheduled = None
      case Some((`token`, reason)) => drain(key, queue, gapRecheck = reason == GapRecheck)
      case _ => () // replaced by a later schedule
    })
    case Applied(key, queueId, batch, outcome) => queues.get(key).filter(_.id == queueId).foreach(queue => afterBatch(key, queue, batch, outcome))
    case GetProjectionStatus => sender() ! status
    case RetryFailedSubscription(aggregateId) => sender() ! FailedSubscriptionsHandled(aggregateId, retryFailures(aggregateId))
    case RetryAllFailedSubscriptions =>
      val count = failed.size
      retryRateLimited(failed.keys.toVector)
      sender() ! FailedSubscriptionsRetried(count)
    case SkipFailedVersion(aggregateId) => sender() ! FailedSubscriptionsHandled(aggregateId, skipFailedVersions(aggregateId))
    case RetryNextFailure => retryNextFailure()
    case q: AsyncDelayedQuery =>
      delayedQueries ::= q
      scheduleNearest()
    case q: AsyncDelayedQueryCustom[_] =>
      delayedQueries ::= q
      scheduleNearest()
    case ReplayQueries =>
      queriesReplayRequested = false
      replayQueries()
    case ClearProjectionData => clearProjectionData(sender())
  }

  // ************** Deliveries (D3, D4, D20a, D20c)

  /** One subscription of this projection to one aggregate: its listener kind and the aggregate. */
  private case class Key(subscriptionType: SubscriptionType, aggregateId: AggregateId) {
    def kind: DeliveryKind = subscriptionType match {
      case SubscriptionType.EVENTS => EventsDelivery
      case SubscriptionType.AGGREGATES => AggregatesDelivery
      case _ => AggregatesWithEventsDelivery
    }
  }

  /** A delivery waiting in its queue: its versions, its events (none for state-only listeners), the state after its
    * last version, and the bus to answer. */
  private case class Pending(versions: Seq[AggregateVersion], events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], replayed: Boolean, respondTo: ActorRef) {
    def first: AggregateVersion = versions.head
    def last: AggregateVersion = versions.last
  }

  /** Versions `from` to `to`, gathered from one or more deliveries (`parts`), applied in one transaction; `covered` are
    * deliveries whose versions all lie in the batch. All of them are acknowledged once the batch is applied. */
  private case class Batch(aggregateType: AggregateType, applied: AggregateVersion, from: AggregateVersion, to: AggregateVersion,
                           events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], parts: Seq[Pending], covered: Seq[Pending]) {
    def deliveries: Seq[Pending] = parts ++ covered
  }

  private sealed trait DrainReason
  private case object GroupingDelay extends DrainReason // ends early when a delivery that must not wait arrives
  private case object RetryBackOff extends DrainReason
  private case object GapRecheck extends DrainReason     // ends early when any delivery arrives: it may fill the gap

  private case class Drain(key: Key, token: Int)
  private case class Applied(key: Key, queueId: Int, batch: Batch, outcome: Try[Unit])
  private case object RetryNextFailure

  /** The deliveries of one subscription waiting to be applied, and what happens to them now. Removed when idle, so the
    * queues hold only aggregates with work in progress (F15). `retryOf` is a recorded failure being retried: it is
    * cleared once a batch reaches its version. */
  private class Queue(val aggregateType: AggregateType, val id: Int) {
    var pending = Vector[Pending]()
    var processing = false
    var failedAttempts = 0
    var scheduled: Option[(Int, DrainReason)] = None
    var gapRequestedFrom: Option[AggregateVersion] = None
    var gapRequestedAt = 0L // System.nanoTime of the last request
    var gapRequests = 0     // requests for the current gap
    var gapSince = 0L       // System.nanoTime when the current gap was found
    var retryOf: Option[SubscriptionFailure] = None
    def idle: Boolean = pending.isEmpty && !processing && scheduled.isEmpty && gapRequestedFrom.isEmpty && retryOf.isEmpty
  }

  private val queues = mutable.HashMap[Key, Queue]()
  private val failed = mutable.LinkedHashMap[Key, SubscriptionFailure]()
  private var failuresToRetry = Vector[Key]()
  private var eventBus: Option[ActorRef] = None
  private var drainTokens = 0
  private var queueIds = 0

  private def queueFor(key: Key, aggregateType: AggregateType): Queue = queues.getOrElseUpdate(key, {
    queueIds += 1
    new Queue(aggregateType, queueIds)
  })

  private def receiveDelivery(key: Key, aggregateType: AggregateType, delivery: Pending): Unit = {
    if (delivery.versions.isEmpty) {
      log.warning("Projection " + projectionName + " received a delivery without events for aggregate " + key.aggregateId.asLong)
    } else if (failed.contains(key)) {
      delivery.respondTo ! MessageFailed(self, key.aggregateId, delivery.versions, failed(key).error, key.kind)
    } else {
      eventBus = Some(delivery.respondTo)
      val queue = queueFor(key, aggregateType)
      queue.pending :+= delivery
      options.eventsLogger.filter(_.isDebugEnabled).foreach(_.debug(s"$className ${key.aggregateId.asLong} received ${delivery.versions.map(_.asInt).mkString(",")}"))
      if (!queue.processing) queue.scheduled.map(_._2) match {
        case None => if (mayWait(delivery)) schedule(key, queue, options.groupUpdatesDelayMillis.millis, GroupingDelay) else drain(key, queue)
        case Some(GroupingDelay) => if (!mayWait(delivery)) drain(key, queue)
        case Some(GapRecheck) => drain(key, queue)
        case Some(RetryBackOff) => () // re-deliveries during the retries wait for them (D20a)
      }
    }
  }

  // Grouping (groupUpdatesDelayMillis): live deliveries wait to be applied together, except early versions and
  // events that must be applied at once.
  private def mayWait(delivery: Pending): Boolean =
    options.groupUpdatesDelayMillis > 0 && !delivery.replayed && delivery.first.asInt >= options.minimumDelayVersion &&
      !delivery.events.exists(e => options.eventsToProcessImmediately.contains(e.event.getClass))

  private def schedule(key: Key, queue: Queue, after: FiniteDuration, reason: DrainReason): Unit = {
    drainTokens += 1
    queue.scheduled = Some((drainTokens, reason))
    context.system.scheduler.scheduleOnce(after, self, Drain(key, drainTokens))(context.dispatcher)
  }

  private def drain(key: Key, queue: Queue, gapRecheck: Boolean = false): Unit = {
    queue.scheduled = None
    Try(appliedVersion(key)) match {
      case Success(applied) => drainFrom(key, queue, applied, gapRecheck)
      case Failure(e) =>
        // The store is unavailable, not the listener at fault: try again later, without counting it as a failure.
        log.warning("Projection " + projectionName + " could not read its version of aggregate " + key.aggregateId.asLong + ", trying again: " + e)
        schedule(key, queue, options.retries.max, RetryBackOff)
    }
  }

  // Runs the gate over the waiting deliveries in the order of their first versions: deliveries applied before are
  // acknowledged, the versions after the applied one form one batch, and deliveries after a gap wait for the missing
  // versions. A retried failure waits for its version like a gap until a batch reaches it.
  private def drainFrom(key: Key, queue: Queue, applied: AggregateVersion, gapRecheck: Boolean): Unit = {
    var next = applied
    var parts = Vector[(Pending, AggregateVersion)]() // a delivery and the first version it adds
    var covered = Vector[Pending]()
    var waiting = Vector[Pending]()
    var gap: Option[AggregateVersion] = None

    queue.pending.sortBy(_.first.asInt).foreach { delivery =>
      if (gap.isDefined) {
        waiting :+= delivery
      } else if (delivery.last <= applied) {
        delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind)
      } else SequenceGate.decide(next, delivery.first, delivery.last) match {
        case AlreadyApplied => covered :+= delivery // all its versions are in this batch
        case Apply(from, to) =>
          parts :+= (delivery -> from)
          next = to
        case Gap(from) =>
          gap = Some(from)
          waiting :+= delivery
      }
    }
    queue.pending = waiting
    if (gap.isEmpty && parts.isEmpty && queue.retryOf.isDefined) gap = Some(next.increment)

    gap match {
      case Some(from) => requestMissingVersions(key, queue, from, gapRecheck, mayGiveUp = parts.isEmpty) // apply what can be applied first
      case None => queue.gapRequestedFrom = None
    }

    if (parts.nonEmpty) {
      val batch = Batch(queue.aggregateType, applied, applied.increment, next,
        parts.flatMap { case (delivery, from) => delivery.events.dropWhile(_.version < from) },
        parts.last._1.aggregateRoot, parts.map(_._1), covered)
      queue.processing = true
      executionContext match {
        case Some(ec) =>
          val queueId = queue.id
          Future(applyBatch(key, batch))(ec).onComplete(outcome => self ! Applied(key, queueId, batch, outcome))(context.dispatcher)
        case None => afterBatch(key, queue, batch, Try(applyBatch(key, batch)))
      }
    } else {
      forgetIfIdle(key, queue)
    }
  }

  // D20c: the owner re-publishes from `from` to this projection only, in parts (a long gap is filled part by part, each
  // new gap start asked at once). An unanswered request is repeated after `gapRequestRepeat`, then doubling up to five
  // minutes, however many deliveries arrive meanwhile: a busy projection may not have reached the answer yet. A gap
  // still open after `gapGiveUpAfter` is recorded as a failure, so it is visible and can be retried or skipped.
  private def requestMissingVersions(key: Key, queue: Queue, from: AggregateVersion, gapRecheck: Boolean, mayGiveUp: Boolean): Unit = {
    val now = System.nanoTime()
    val sameGap = queue.gapRequestedFrom.contains(from)
    if (!sameGap) {
      queue.gapRequests = 0
      queue.gapSince = now
    }
    if (mayGiveUp && sameGap && now - queue.gapSince >= options.gapGiveUpAfter.toNanos) {
      giveUp(key, queue, queue.aggregateType, from, "versions from " + from.asInt + " are missing: asked " + queue.gapRequests +
        " times in " + options.gapGiveUpAfter + " without an answer", None)
    } else {
      if (!sameGap || gapRecheck) {
        eventBus.foreach(_ ! RequestRepublish(self, queue.aggregateType, key.aggregateId, from))
        if (!sameGap) log.info("Projection " + projectionName + " asks for versions from " + from.asInt + " of " + queue.aggregateType.simpleName + " " + key.aggregateId.asLong)
        queue.gapRequestedFrom = Some(from)
        queue.gapRequestedAt = now
        queue.gapRequests += 1
      }
      val wait = gapRequestRepeats.delay(queue.gapRequests + 1)
      schedule(key, queue, math.max(wait.toNanos - (now - queue.gapRequestedAt), 0L).nanos, GapRecheck)
    }
  }

  private val gapRequestRepeats = Backoff(options.gapRequestRepeat, 5.minutes, maxAttempts = Int.MaxValue)

  private def afterBatch(key: Key, queue: Queue, batch: Batch, outcome: Try[Unit]): Unit = {
    queue.processing = false
    outcome match {
      case Success(_) =>
        batch.deliveries.foreach(delivery => delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind))
        options.eventsLogger.filter(_.isDebugEnabled).foreach(_.debug(s"$className ${key.aggregateId.asLong} applied ${batch.from.asInt}-${batch.to.asInt}"))
        queue.failedAttempts = 0
        queue.retryOf.filter(_.version <= batch.to).foreach { failure =>
          queue.retryOf = None
          Try(subscriptionsState.autoCommit(subscriptionsState.clearFailure(subscriberName, key.subscriptionType, key.aggregateId)(_))).failed
            .foreach(e => log.error(e, "Projection " + projectionName + " applied the failed version of aggregate " + key.aggregateId.asLong +
              " but could not clear its failure record; it is retried again at the next start"))
          log.info("Projection " + projectionName + " recovered from its failure on " + failure.aggregateType.simpleName + " " + key.aggregateId.asLong)
        }
        requestQueriesReplay()
        // Deliveries that arrived meanwhile may fill a gap, so they are looked at now, not at the gap's next check.
        if (queue.pending.nonEmpty || queue.retryOf.isDefined) drain(key, queue) else forgetIfIdle(key, queue)
      case Failure(e) =>
        queue.failedAttempts += 1
        queue.pending = batch.deliveries.toVector ++ queue.pending
        if (options.retries.exhausted(queue.failedAttempts)) {
          giveUp(key, queue, batch.aggregateType, batch.from, e.getClass.getSimpleName + ": " + e.getMessage, Some(e))
        } else {
          // One warning per failing batch; its retries log at debug level, so a problem across many aggregates does not flood the log.
          val message = "Projection " + projectionName + " failed on " + batch.aggregateType.simpleName + " " + key.aggregateId.asLong +
            " versions " + batch.from.asInt + "-" + batch.to.asInt + " (attempt " + queue.failedAttempts + "), retrying: " + e
          if (queue.failedAttempts == 1) log.warning(message) else log.debug(message)
          schedule(key, queue, options.retries.delay(queue.failedAttempts + 1), RetryBackOff)
        }
    }
  }

  // D20a: the failure is stored with the first version not applied, then the waiting deliveries and every later one of
  // the aggregate get MessageFailed, so the owner's outbox rows can go, until the failure is retried or skipped. While
  // the failure cannot be stored (the database is down) nothing is answered: the rows stay, and the batch is tried again.
  private def giveUp(key: Key, queue: Queue, aggregateType: AggregateType, version: AggregateVersion, error: String, cause: Option[Throwable]): Unit = {
    val failure = SubscriptionFailure(key.subscriptionType, aggregateType, key.aggregateId, version, error, Instant.now)
    Try(subscriptionsState.recordFailure(subscriberName, failure)) match {
      case Success(_) =>
        val message = "Projection " + projectionName + " gave up on " + aggregateType.simpleName + " " + key.aggregateId.asLong + " at version " +
          version.asInt + " (" + error + "); deliveries of it fail until it is retried or skipped"
        cause match {
          case Some(e) => log.error(e, message)
          case None => log.error(message)
        }
        failed += key -> failure
        queue.pending.foreach(delivery => delivery.respondTo ! MessageFailed(self, key.aggregateId, delivery.versions, error, key.kind))
        queues -= key
      case Failure(storeError) =>
        log.error(storeError, "Projection " + projectionName + " could not record its failure on aggregate " + key.aggregateId.asLong +
          "; its deliveries wait and it tries again")
        queue.failedAttempts = options.retries.maxAttempts - 1 // the next failure gives up at once
        schedule(key, queue, options.retries.max, RetryBackOff)
    }
  }

  private def forgetIfIdle(key: Key, queue: Queue): Unit = if (queue.idle) queues -= key

  private def appliedVersion(key: Key): AggregateVersion = subscriptionsState.lastVersion(subscriberName, key.subscriptionType, key.aggregateId)

  private def advance(key: Key, from: AggregateVersion, to: AggregateVersion)(session: DBSession): Unit =
    subscriptionsState.newVersion(subscriberName, key.subscriptionType, key.aggregateId, from, to)(session)

  // The listener and the subscription's new version commit together; a listener without a transaction runs first and
  // the version is written after it, so such a listener may see a batch again if that write fails (at least once).
  private def applyBatch(key: Key, batch: Batch): Unit = {
    val id = key.aggregateId
    val (noTransaction, work): (Boolean, DBSession => Unit) = key.subscriptionType match {
      case SubscriptionType.EVENTS =>
        val listener = eventListenersMap(batch.aggregateType)
        (listener.options.noTransaction, listener.listener(id, batch.events))
      case SubscriptionType.AGGREGATES =>
        val listener = aggregateListenersMap(batch.aggregateType)
        (listener.options.noTransaction, listener.listener(id, batch.to, batch.from.isOne, batch.aggregateRoot))
      case _ =>
        val listener = aggregateWithEventListenersMap(batch.aggregateType)
        (listener.options.noTransaction, listener.listener(id, batch.to, batch.aggregateRoot, batch.events))
    }
    if (noTransaction) {
      work(null) // no session: the listener does not write to the database
      subscriptionsState.autoCommit(advance(key, batch.applied, batch.to))
    } else {
      subscriptionsState.localTx { session =>
        work(session)
        advance(key, batch.applied, batch.to)(session)
      }
    }
  }

  // ************** Failed subscriptions (D20a)

  private def loadFailures(): Unit = {
    Try(subscriptionsState.failures(subscriberName)) match {
      case Success(stored) =>
        stored.foreach(failure => failed += Key(failure.subscriptionType, failure.aggregateId) -> failure)
        if (options.retryFailuresOnStart && stored.nonEmpty) {
          log.info("Projection " + projectionName + " retries " + stored.size + " failed subscriptions")
          retryRateLimited(stored.sortBy(_.since).map(failure => Key(failure.subscriptionType, failure.aggregateId)).toVector)
        }
      case Failure(e) => log.error(e, "Projection " + projectionName + " could not read its failed subscriptions")
    }
  }

  // Many retries at once would start and restore every owner together: one per `failureRetryInterval`.
  private def retryRateLimited(keys: Vector[Key]): Unit = {
    val wasIdle = failuresToRetry.isEmpty
    failuresToRetry = failuresToRetry ++ keys.filterNot(failuresToRetry.contains)
    if (wasIdle && failuresToRetry.nonEmpty) self ! RetryNextFailure
  }

  private def retryNextFailure(): Unit = {
    failuresToRetry.headOption.foreach { key =>
      failuresToRetry = failuresToRetry.tail
      retry(key)
      if (failuresToRetry.nonEmpty) context.system.scheduler.scheduleOnce(options.failureRetryInterval, self, RetryNextFailure)(context.dispatcher)
    }
  }

  private def retryFailures(aggregateId: AggregateId): Int = failed.keys.filter(_.aggregateId == aggregateId).toSeq.count(retry)

  // The failed version is asked again, repeatedly, like a gap; the record is cleared once a batch reaches it (afterBatch).
  // Deliveries are taken again meanwhile; the gate drops what was applied before.
  private def retry(key: Key): Boolean = failed.remove(key).exists { failure =>
    val queue = queueFor(key, failure.aggregateType)
    queue.retryOf = Some(failure)
    if (!queue.processing) drain(key, queue, gapRecheck = true)
    true
  }

  // Marks each failed version as applied, then asks once for the versions after it: there may be none, and a later
  // event of the aggregate asks for any that went missing.
  private def skipFailedVersions(aggregateId: AggregateId): Int = failed.keys.filter(_.aggregateId == aggregateId).toSeq.count { key =>
    val failure = failed(key)
    Try(subscriptionsState.localTx { session =>
      advance(key, AggregateVersion(failure.version.asInt - 1), failure.version)(session)
      subscriptionsState.clearFailure(subscriberName, key.subscriptionType, key.aggregateId)(session)
    }) match {
      case Success(_) =>
        log.warning("Projection " + projectionName + " skipped version " + failure.version.asInt + " of " + failure.aggregateType.simpleName + " " + aggregateId.asLong)
        failed -= key
        eventBus.foreach(_ ! RequestRepublish(self, failure.aggregateType, failure.aggregateId, failure.version.increment))
        true
      case Failure(e) =>
        log.error(e, "Projection " + projectionName + " could not skip version " + failure.version.asInt + " of aggregate " + aggregateId.asLong)
        false
    }
  }

  private def status: ProjectionStatus = ProjectionStatus(projectionName, failed.values.toSeq ++ queues.values.flatMap(_.retryOf),
    queues.collect { case (key, queue) if queue.failedAttempts > 0 || queue.retryOf.isDefined => key.aggregateId }.toSeq.distinct,
    queues.collect { case (key, queue) if queue.gapRequestedFrom.isDefined && queue.retryOf.isEmpty => key.aggregateId }.toSeq.distinct)

  protected def receiveQuery: Receive

  private def clearProjectionData(replyTo: ActorRef): Unit = {
    subscriptionsState.clearSubscriptionsInfo(subscriberName)
    queues.clear()
    failed.clear()
    failuresToRetry = Vector.empty
    replyTo ! ProjectionDataCleared
    onClearProjectionData()
  }

  protected def onClearProjectionData(): Unit

  // Asked by the event bus at start (D15): one subscription per listener.
  private def subscriptionRequests: Seq[SubscribeRequest] =
    aggregateListenersMap.keySet.toSeq.map(aggregateType => SubscribeForAggregates(aggregateType, self)) ++
      eventListenersMap.keySet.toSeq.map(aggregateType => SubscribeForEvents(aggregateType, self)) ++
      aggregateWithEventListenersMap.keySet.toSeq.map(aggregateType => SubscribeForAggregatesWithEvents(aggregateType, self))

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  // ************** Queries delay - needed if query is for document that might not been yet updated, but update is in it's way

  private var delayedQueries = List[DelayedQuery]()
  private var queriesReplayRequested = false

  // F28: one replay of the waiting queries for a burst of applied updates, not one per update.
  private def requestQueriesReplay(): Unit = {
    if (delayedQueries.nonEmpty && !queriesReplayRequested) {
      queriesReplayRequested = true
      self ! ReplayQueries
    }
  }

  private def replayQueries(): Unit = {

    if(delayedQueries.nonEmpty) {
      val now = Instant.now()

      val queries = delayedQueries
      delayedQueries = List.empty

      queries.foreach(query => {
        if (query.until.isAfter(now)) {
          query match {
            case q: SyncDelayedQuery => delayIfNotAvailable(q.respondTo, q.search, q.until)
            case q: AsyncDelayedQuery => delayIfNotAvailableAsync(q.respondTo, q.search, q.until)
            case q: AsyncDelayedQueryCustom[_] => delayIfNotAvailableAsyncCustom(q.respondTo, q.search, q.found, q.until)
          }
        } else {
          query.respondTo ! None
        }
      })

      scheduleNearest()
    }
  }


  protected def delayIfNotAvailable[T](respondTo: ActorRef, search: () => Option[T], forMaximumMillis: Int): Unit = {
    delayIfNotAvailable[T](respondTo, search, Instant.now().plusMillis(forMaximumMillis))
  }

  protected def delayIfNotAvailableAsync[T](respondTo: ActorRef, search: () => Future[Option[T]], forMaximumMillis: Int): Unit = {
    delayIfNotAvailableAsync[T](respondTo, search, Instant.now().plusMillis(forMaximumMillis))
  }

  protected def delayIfNotAvailableAsyncCustom[T](respondTo: ActorRef, search: () => Future[T], found: T => Boolean, forMaximumMillis: Int): Unit = {
    delayIfNotAvailableAsyncCustom[T](respondTo, search, found, Instant.now().plusMillis(forMaximumMillis))
  }

  // A search that fails is answered with the failure (F80): rethrowing would restart the actor.
  protected def delayIfNotAvailable[T](respondTo: ActorRef, search: () => Option[T], until: Instant): Unit = {
    Try(search()) match {
      case Success(result@Some(_)) => respondTo ! result
      case Success(None) =>
        delayedQueries ::= SyncDelayedQuery(until, respondTo, search)
        scheduleNearest()
      case Failure(ex) =>
        log.error(ex, "Query of projection " + projectionName + " failed")
        respondTo ! Status.Failure(ex)
    }
  }

  protected def delayIfNotAvailableAsync[T](respondTo: ActorRef, search: () => Future[Option[T]], until: Instant): Unit = {
    search().onComplete {
      case Success(result@Some(_)) => respondTo ! result
      case Success(None) => self ! AsyncDelayedQuery(until, respondTo, search)
      case Failure(ex) => respondTo ! Status.Failure(ex)
    }(context.system.dispatcher)
  }

  protected def delayIfNotAvailableAsyncCustom[T](respondTo: ActorRef, search: () => Future[T], found: T => Boolean, until: Instant): Unit = {
    search().onComplete {
      case Success(result) if found(result) => respondTo ! result
      case Success(result) => self ! AsyncDelayedQueryCustom[T](until, respondTo, search, found)
      case Failure(ex) => respondTo ! Status.Failure(ex)
    }(context.system.dispatcher)
  }

  private var replayQueriesScheduled: Option[Cancellable] = None

  private def scheduleNearest(): Unit = {

    replayQueriesScheduled.foreach(_.cancel())
    replayQueriesScheduled = None

    if(delayedQueries.nonEmpty) {
      val earliest = delayedQueries.minBy(_.until)

      val duration = FiniteDuration(earliest.until.toEpochMilli - Instant.now().toEpochMilli, TimeUnit.MILLISECONDS)
      replayQueriesScheduled = Some(context.system.scheduler.scheduleOnce(duration, self, ReplayQueries)(context.system.dispatcher))
    }
  }
}
