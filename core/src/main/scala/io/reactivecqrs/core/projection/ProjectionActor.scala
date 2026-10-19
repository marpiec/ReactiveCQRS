package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.pekko.actor.{Actor, ActorRef, Cancellable, Status}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.delivery.{NothingToRepublish, SequenceGate}
import io.reactivecqrs.core.delivery.SequenceGate.{AlreadyApplied, Apply, Gap}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.metrics.ReactiveMetrics
import io.reactivecqrs.core.util.{Backoff, MyActorLogging}
import org.slf4j.Logger
import scalikejdbc.DBSession

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import io.reactivecqrs.core.util.Errors

/** A query parked until the read model can answer it (`delayIfNotAvailable`), or until `until`. */
sealed trait DelayedQuery {
  /** When it is answered with what there is. */
  val until: Instant
  /** Who gets the answer. */
  val respondTo: ActorRef
}
private case class SyncDelayedQuery(until: Instant, respondTo: ActorRef, search: () => Option[Any]) extends DelayedQuery
/** `searchedAt`: the count of applied updates when its search began (F163). */
private case class AsyncDelayedQuery(until: Instant, respondTo: ActorRef, search: () => Future[Option[Any]], searchedAt: Long) extends DelayedQuery
private case class AsyncDelayedQueryCustom[T](until: Instant, respondTo: ActorRef, search: () => Future[T], found: T => Boolean, searchedAt: Long) extends DelayedQuery

/** The aggregate types and listener kinds of a projection, one per listener: what a build delivers (D58). */
case object GetListeners
/** The answer to `GetListeners`. */
case class Listeners(subscriptions: Seq[(AggregateType, SubscriptionType)])

// D20a: a projection's failed subscriptions, and what an operator can do about them.

/** Asks a projection for its `ProjectionStatus`. */
case object GetProjectionStatus
/** `retrying`: aggregates whose listener failed and is retried now; `waitingForGap`: aggregates waiting for missing
  * versions to be re-published. An answer stays small enough for another node (F228): `failures` holds the first 100,
  * oldest first, each error cut to 500 characters, and `failuresTotal` counts them all; the lists of aggregates hold at
  * most 1 000 each. */
case class ProjectionStatus(projectionName: String, failures: Seq[SubscriptionFailure], retrying: Seq[AggregateId], waitingForGap: Seq[AggregateId],
                            failuresTotal: Int = 0)
// D57, D70: a managed projection runs one instance per build, under its supervisor.

/** From the supervisor, before anything else: the name the instance's subscriptions are stored under (its build's). */
case class BindSubscriber(subscriber: String)
/** From the supervisor, which subscribed the instance to `bus` (D15): missing versions are asked from it, and the
  * failures recorded before are retried, as at a start (D20a). */
case class ConnectedTo(bus: ActorRef)
/** The built instance serves from now on: `rebuilding` ends and `onActivated()` runs. */
case object Activate
/** The instance's build is retired (D57): its push subscriptions go to `successor`, its parked queries are answered with
  * `None`, and it stops. */
case class Retire(successor: Option[ActorRef])
/** The instance serves its tables unchanged while a new build replaces it (`ServeStale` for a build begun at run time,
  * F153): it was removed from the buses, answers the deliveries still on their way without applying them, and asks for
  * no missing versions, until a `ConnectedTo` connects it again. */
case object ServeUnchanged

/** What a targeted rebuild covers (D63, R14): one aggregate, one aggregate type, or one organization's aggregates. */
sealed trait RebuildTarget
object RebuildTarget {
  /** One aggregate. */
  case class OneAggregate(aggregateId: AggregateId) extends RebuildTarget
  /** Every aggregate of a type. */
  case class OneType(aggregateType: AggregateType) extends RebuildTarget
  /** Every aggregate of an organization. */
  case class OneSpace(spaceId: SpaceId) extends RebuildTarget
}
/** From the supervisor, before a targeted rebuild: the projection lists the target's aggregates (`aggregatesOf`) once it
  * applied every delivery taken before, so an aggregate made meanwhile is either listed or delivered after the reset
  * (F112); then, in one transaction, it clears what it derived from the target (`clearForRebuild`) and forgets its
  * versions of those aggregates (and their failures), so the builder's deliveries apply anew. Answered with
  * `ResetForRebuildDone` and how many aggregates it reset, or `Status.Failure` when the projection does not implement
  * `clearForRebuild`. */
case class ResetForRebuild(target: RebuildTarget, aggregatesOf: () => Seq[AggregateId])
/** The answer to `ResetForRebuild`: how many aggregates were reset. */
case class ResetForRebuildDone(aggregates: Int)

// R17: the purge of an organization.

/** From a purge, before it deletes anything: answered `PurgeCheckAnswer`, with the reason when the projection cannot take
  * part: it does not implement `clearForRebuild`, or (a managed one) a build or a targeted rebuild runs. */
case class PurgeCheck(space: SpaceId)
/** The answer to `PurgeCheck`: `None` when the projection can take part in the purge. */
case class PurgeCheckAnswer(projectionName: String, refusal: Option[String])
/** From a purge, after it deleted these aggregates of the space: the projection forgets them (versions, queues, failures)
  * and, for `ProjectionActorOptions.purgedDropWindow`, acknowledges deliveries of them without applying them, as one may
  * still be on its way. Answered `PurgedAggregatesForgotten`. */
case class PurgedAggregates(space: SpaceId, aggregateIds: Seq[AggregateId])
/** The answer to `PurgedAggregates`: how many aggregates the projection forgot. */
case class PurgedAggregatesForgotten(projectionName: String, count: Int)
/** From a purge, last: the projection clears what it derived from the space (`clearForRebuild(OneSpace(space))`) in one
  * transaction. Answered `SpacePurged`, or `Status.Failure`. */
case class PurgeSpace(space: SpaceId)
/** The answer to `PurgeSpace`. */
case class SpacePurged(projectionName: String, space: SpaceId)

// D57, D58, D61: a projection being built is fed by a builder that reads the event store.

/** The projection is being built from the store by `builder` (D57): until `GoLive`, deliveries from the bus are
  * acknowledged without being applied, as the builder reads their events from the store, and missing versions are
  * asked from the builder. */
case class BuildFromStore(builder: ActorRef)
/** Deliveries from the bus are applied again; the failures recorded meanwhile are retried. Answered with
  * `ProjectionLive`. */
case object GoLive
/** The answer to `GoLive`. */
case object ProjectionLive

/** From a builder (D58, D61): versions of many aggregates, in commit order, applied in one transaction. Answered with
  * `ChunkApplied`. */
case class ApplyChunk(id: Long, items: Seq[ChunkItem])
/** One subscription's versions `first` to `last` of one aggregate: its events (none for a state listener) and the
  * aggregate's state after `last` (for a part of a longer range read by version, after the range, as an owner's
  * re-published parts carry). `startsSequence`: nothing before `first` is needed: it is the aggregate's squash event
  * (F98), or the item holds the aggregate's whole state (D62). `unreadable`: the event at `first` cannot be read from
  * the store, so the subscription fails there (D20a) instead of stopping the build (F82). `bytes`: its events' stored
  * sizes, which bound a chunk (F126); 0 when not known. */
case class ChunkItem(subscriptionType: SubscriptionType, aggregateType: AggregateType, aggregateId: AggregateId,
                     first: AggregateVersion, last: AggregateVersion, events: Seq[EventInfo[Any]], aggregateRoot: Option[Any],
                     startsSequence: Boolean = false, unreadable: Option[String] = None, bytes: Long = 0)
/** `gaps`: subscriptions whose items were not applied because versions before them are missing, with the first missing
  * one. `handedOver`: items applied one aggregate at a time instead, like deliveries (the chunk failed, or the
  * subscription had deliveries at work); each is answered to the builder with `MessageAck` or `MessageFailed`. Items
  * of an aggregate whose subscription is failed are left out (D20a): a retry repairs it. */
case class ChunkApplied(id: Long, gaps: Seq[ChunkGap], handedOver: Int)
/** A subscription whose chunk items wait for versions from `from` on. */
case class ChunkGap(subscriptionType: SubscriptionType, aggregateType: AggregateType, aggregateId: AggregateId, from: AggregateVersion)

/** Has the aggregate's failed versions delivered again; each failure is cleared once its version is applied. */
case class RetryFailedSubscription(aggregateId: AggregateId)
/** Retries every failed subscription, a few per second; answered with `FailedSubscriptionsRetried`. */
case object RetryAllFailedSubscriptions
/** The answer to `RetryAllFailedSubscriptions`: how many failed subscriptions are retried. */
case class FailedSubscriptionsRetried(count: Int)
/** Marks each failed version of the aggregate as applied without applying it, and continues from the version after. A
  * "versions from N are missing" failure is not marked (F201: those versions may not exist yet), only asked for again;
  * `pastMissing` marks N too, for versions that exist but cannot arrive, such as an event larger than a message between
  * nodes may be (F304). */
case class SkipFailedVersion(aggregateId: AggregateId, pastMissing: Boolean = false)
/** The answer to both: how many failed subscriptions of the aggregate were retried or skipped. */
case class FailedSubscriptionsHandled(aggregateId: AggregateId, count: Int)


object ProjectionActorOptions {
  /** About two minutes of retries (D20a): 1, 2, 4, 8, 16, 30, 30 and 30 seconds between nine attempts. */
  val defaultRetries: Backoff = Backoff(1.second, 30.seconds, maxAttempts = 9)
  /** No grouping, no parallel processing, no events logger, and the defaults below. */
  val DEFAULT = ProjectionActorOptions(0, 1, Set.empty, 0, None)
}

/**
  * How a projection applies its deliveries.
  *
  * @param groupUpdatesDelayMillis how long a live delivery may wait for the next ones of its aggregate, to be applied
  *                                with them; 0 applies each at once
  * @param minimumDelayVersion the first version of an aggregate that may wait (`groupUpdatesDelayMillis`); earlier ones,
  *                            such as a new aggregate's first event, are applied at once
  * @param eventsToProcessImmediately event classes whose deliveries never wait (`groupUpdatesDelayMillis`); for event
  *                                   listeners and listeners of aggregates with events
  * @param parallelUpdateProcessing how many threads apply deliveries (above 1), on the same projection object: its
  *                                 listeners must then be thread-safe; 0 or 1 applies them on one thread
  * @param eventsLogger a logger that, at debug level, logs every delivery received and every batch applied
  * @param retries how a listener that fails on an aggregate is retried before the failure is recorded (D20a)
  * @param retryFailuresOnStart retry the failures recorded before the start (D20a)
  * @param gapRequestRepeat how long a request for missing versions waits before it is sent again, doubling up to five
  *                         minutes (D20c)
  * @param gapGiveUpAfter how long missing versions are asked for before the gap is recorded as a failure
  * @param failureRetryInterval the pace of retrying many failures (at start, `RetryAllFailedSubscriptions`)
  * @param maxBatchesPerTransaction how many aggregates' batches one transaction applies at most when deliveries arrive
  *                                 faster than they are applied (D61, F93); 1 applies each alone. Not with
  *                                 `parallelUpdateProcessing`.
  * @param purgedDropWindow how long deliveries of the aggregates an organization's purge deleted are acknowledged
  *                         without being applied (R17)
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
                                   failureRetryInterval: FiniteDuration = 50.millis,
                                   maxBatchesPerTransaction: Int = 100,
                                   purgedDropWindow: FiniteDuration = 30.minutes) {

  /** These options with `groupUpdatesDelayMillis`. */
  def withGroupUpdatesDelayMillis(millis: Long): ProjectionActorOptions = {
    copy(groupUpdatesDelayMillis = millis)
  }

  /** These options with `minimumDelayVersion`. */
  def withMinimumDelayVersion(version: Int): ProjectionActorOptions = {
    copy(minimumDelayVersion = version)
  }

  /** These options with `eventsLogger`. */
  def withEventsLogger(logger: Logger): ProjectionActorOptions = {
    copy(eventsLogger = Some(logger))
  }

  /** These options with `eventsToProcessImmediately`: it works for listeners that receive events, not for listeners of
    * aggregates alone, which do not know the events. */
  def withEventsToProcessImmediately(classes: Class[_]*): ProjectionActorOptions = {
    copy(eventsToProcessImmediately = classes.toSet)
  }

  /** These options with `parallelUpdateProcessing`: zero means no parallelism. Deliveries are applied on several
    * threads, on the same objects, so the listeners must be thread-safe. */
  def withParallelUpdateProcessing(parallel: Int): ProjectionActorOptions = {
    copy(parallelUpdateProcessing = parallel)
  }

  /** These options with `retries`. */
  def withRetries(retries: Backoff): ProjectionActorOptions = copy(retries = retries)

}

object ListenerOptions {
  /** The listener runs in the transaction that records the subscription's new version. */
  val DEFAULT = ListenerOptions(false)
  /** The listener writes nothing to the database: it runs without a transaction (`noDBSession`). */
  val DISABLED_TRANSACTION = ListenerOptions(true)
}
/** How a listener runs. `noTransaction`: it gets no session, runs before its subscription's new version is written,
  * and may so see a batch again if that write fails (at least once). */
case class ListenerOptions(noTransaction: Boolean = false)

/**
  * A subscriber that keeps a read model (D3). Every delivery goes through the sequence gate (D4) on the subscription's
  * stored version, one aggregate at a time per listener kind: duplicates are acknowledged, new versions are applied in
  * one transaction with the new version (with other aggregates' while deliveries keep arriving, D61), and missing
  * versions are asked from the aggregate's owner (D20c). A listener
  * that fails is retried with back-off; then the failure is recorded and deliveries of that aggregate are answered
  * `MessageFailed` until it is retried or skipped (D20a). See docs/design/M2-delivery.md.
  */
abstract class ProjectionActor(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends Actor with MyActorLogging {

  private val className = this.getClass.getSimpleName
  /** The name its subscription versions and failures are stored under: the class name, unless a build gives another. */
  protected def subscriberName: String = boundSubscriber.getOrElse(this.getClass.getName)
  private var boundSubscriber: Option[String] = None

  /** True while this instance is built from the store and does not serve yet (D57), and while it applies events a
    * builder gives it again (a targeted rebuild, D63; F114): a listener with side effects (a message to another actor, a
    * push update, an email) does nothing then, as the events are history. */
  protected final def rebuilding: Boolean = !activated || applyingHistory.get()
  private var activated = true
  // Set on the thread that runs a listener (the actor's, or the pool's in parallel mode) while it applies history.
  private val applyingHistory = ThreadLocal.withInitial[Boolean](() => false)

  private def asHistory[T](history: Boolean)(work: => T): T = {
    applyingHistory.set(history)
    try work finally applyingHistory.set(false)
  }

  /** Called when a built instance starts to serve; a projection that kept build-time state in memory finishes it here. */
  protected def onActivated(): Unit = ()

  /** Called when the instance's build is retired, before it stops. */
  protected def onRetired(successor: Option[ActorRef]): Unit = ()

  /** Removes, in the given transaction, what the projection derived from the target of a targeted rebuild (D63), whose
    * events it is then given again from their first: a document per aggregate, a space's documents (`clearSpace`).
    * A projection that does not implement it refuses targeted rebuilds. */
  protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit =
    throw new UnsupportedOperationException("Projection " + projectionName + " does not implement clearForRebuild, so it cannot rebuild " + target)

  protected val projectionName: String
  protected val subscriptionsState: SubscriptionsState

  protected val listeners:List[Listener[Any]]

  private val executor: Option[ExecutorService] =
    if (options.parallelUpdateProcessing > 1) Some(Executors.newFixedThreadPool(options.parallelUpdateProcessing)) else None
  private val executionContext: Option[ExecutionContext] = executor.map(ExecutionContext.fromExecutor)

  // F27: listener transactions run on this actor's thread (unless in parallel); on the default dispatcher they hold up
  // everything else that runs there.
  if (executor.isEmpty && context.dispatcher == context.system.dispatchers.defaultGlobalDispatcher) {
    log.warning("Projection " + className + " runs its listener transactions on the default dispatcher; create it with " +
      "Props(...).withDispatcher(BlockingDispatcher.id) (F27)")
  }

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

  private object ReplayQueries



  private lazy val eventListenersMap = listenersOfKind[EventsListener[Any]]
  private lazy val aggregateListenersMap = listenersOfKind[AggregateListener[Any]]
  private lazy val aggregateWithEventListenersMap = listenersOfKind[AggregateWithEventsListener[Any]]

  private def listenersOfKind[L <: Listener[Any] : ClassTag]: Map[AggregateType, L] = {
    validateListeners()
    listeners.collect { case l: L => l.aggregateType -> l }.toMap
  }

  // AR7, F80: a message that fails, such as a query, is answered with the failure; a throw would restart the actor
  // and lose the deliveries it holds. The batches grouped while deliveries arrive are applied before any other
  // message, so a query, a reset or a retirement sees every delivery taken before it.
  override def receive: Receive = {
    case message if handlers.isDefinedAt(message) =>
      try {
        if (!takenWhileGrouping(message)) flushGroup()
        handlers(message)
      } catch {
        case NonFatal(e) =>
          log.error(e, "Projection " + projectionName + " failed on " + message.getClass.getSimpleName)
          sender() ! Status.Failure(e)
      }
  }

  private lazy val handlers: Receive = receiveUpdate orElse receiveQuery

  private def validateListeners() = {
    if (listeners.exists(l => l.aggregateType == AggregateType.of[Any] || l.aggregateType == AggregateType.of[scala.Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(_.aggregateType.typeName))
    }
    // F164: a second listener of a kind for a type would replace the first in the maps, silently.
    val repeated = listeners.groupBy(l => (kindOf(l), l.aggregateType.typeName)).collect {
      case ((kind, typeName), declared) if declared.size > 1 => s"${declared.size} ${kind}s of $typeName"
    }
    if (repeated.nonEmpty) {
      throw new IllegalArgumentException("Projection " + projectionName + " declares " + repeated.toSeq.sorted.mkString(", ") +
        ", of which only one would be called: give each aggregate type one listener of each kind")
    }
  }

  private def kindOf(listener: Listener[Any]): String = listener match {
    case _: EventsListener[_] => "EventsListener"
    case _: AggregateListener[_] => "AggregateListener"
    case _: AggregateWithEventsListener[_] => "AggregateWithEventsListener"
    case other => other.getClass.getName
  }

  protected def receiveUpdate: Receive = {
    case GetSubscriptionRequests(registered) =>
      eventBus = Some(sender())
      sender() ! SubscriptionRequests(subscriptionRequests, registered)
      loadFailures()
    case GetListeners => sender() ! Listeners(
      aggregateListenersMap.keys.toSeq.map(_ -> SubscriptionType.AGGREGATES) ++
        eventListenersMap.keys.toSeq.map(_ -> SubscriptionType.EVENTS) ++
        aggregateWithEventListenersMap.keys.toSeq.map(_ -> SubscriptionType.AGGREGATES_WITH_EVENTS))
    case e: IdentifiableEvents[_] =>
      receiveDelivery(Key(SubscriptionType.EVENTS, e.aggregateId), e.aggregateType,
        Pending(e.events.map(_.version), e.events.asInstanceOf[Seq[EventInfo[Any]]], None, sender(), e.startsAtSquash))
    case a: AggregateWithType[_] =>
      receiveDelivery(Key(SubscriptionType.AGGREGATES, a.id), a.aggregateType, Pending(a.events, Seq.empty, a.aggregateRoot, sender(), a.startsAtSquash))
    case ae: AggregateWithTypeAndEvents[_] =>
      receiveDelivery(Key(SubscriptionType.AGGREGATES_WITH_EVENTS, ae.id), ae.aggregateType,
        Pending(ae.events.map(_.version), ae.events.asInstanceOf[Seq[EventInfo[Any]]], ae.aggregateRoot, sender(), ae.startsAtSquash))
    case chunk: ApplyChunk => applyChunk(chunk, sender())
    case BindSubscriber(subscriber) => boundSubscriber = Some(subscriber)
    case ConnectedTo(bus) =>
      eventBus = Some(bus)
      unchanged = false
      loadFailures()
    case ServeUnchanged =>
      unchanged = true
      eventBus = None
      // F201: the failures stay known (they block deletes and squashes, and are in the status); only the queues go.
      forget(queues.keys.map(_.aggregateId).toSet, keepFailures = true)
      failuresToRetry = Vector.empty
    case Activate =>
      if (!activated) {
        activated = true
        onActivated()
      }
    case ResetForRebuild(target, aggregatesOf) =>
      val aggregateIds = aggregatesOf()
      subscriptionsState.localTx { session =>
        clearForRebuild(target)(session)
        subscriptionsState.resetVersions(subscriberName, aggregateIds)(session)
      }
      val reset = aggregateIds.toSet
      forget(reset) // the builder gives their events again
      purged = purged.without(reset)
      sender() ! ResetForRebuildDone(aggregateIds.size)
    case PurgeCheck(_) =>
      sender() ! PurgeCheckAnswer(projectionName, if (clearsForRebuild) None else Some("projection " + projectionName + " does not implement clearForRebuild"))
    case PurgedAggregates(_, aggregateIds) =>
      subscriptionsState.localTx(session => subscriptionsState.resetVersions(subscriberName, aggregateIds)(session))
      forget(aggregateIds.toSet)
      purged = purged.adding(aggregateIds, System.nanoTime(), options.purgedDropWindow.toNanos)
      sender() ! PurgedAggregatesForgotten(projectionName, aggregateIds.size)
    case PurgeSpace(space) =>
      Try(subscriptionsState.localTx(session => clearForRebuild(RebuildTarget.OneSpace(space))(session))) match {
        case Success(_) =>
          log.info("Projection " + projectionName + " cleared the purged space " + space.asLong)
          sender() ! SpacePurged(projectionName, space)
        case Failure(e) =>
          log.error(e, "Projection " + projectionName + " could not clear the purged space " + space.asLong)
          sender() ! Status.Failure(e)
      }
    case Retire(successor) =>
      log.info("Projection " + projectionName + " (" + subscriberName + ") is retired")
      onRetired(successor)
      delayedQueries.foreach(_.respondTo ! None)
      delayedQueries = Nil
      context.stop(self)
    case BuildFromStore(builder) =>
      building = Some(builder)
      activated = false
    case GoLive =>
      val wasBuilt = building.isDefined
      building = None
      if (wasBuilt && options.retryFailuresOnStart && failed.nonEmpty) {
        log.info("Projection " + projectionName + " is live and retries " + failed.size + " subscriptions that failed while it was built")
        retryRateLimited(failed.keys.toVector)
      }
      sender() ! ProjectionLive
    case Drain(key, token) => queues.get(key).foreach(queue => queue.scheduled match {
      // While a batch runs, the queue is drained when it ends: one batch per queue at a time.
      case Some((`token`, _)) if queue.processing => queue.scheduled = None
      case Some((`token`, reason)) => drain(key, queue, gapRecheck = reason == GapRecheck)
      case _ => () // replaced by a later schedule
    })
    case Applied(key, queueId, batch, outcome) => queues.get(key).filter(_.id == queueId) match {
      case Some(queue) => afterBatch(key, queue, batch, outcome)
      // Its queue was reset for a targeted rebuild meanwhile, whose builder gives the events again.
      case None => batch.deliveries.foreach(delivery => delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind))
    }
    case FlushGroup =>
      flushRequested = false
      flushGroup()
    case GetProjectionStatus => sender() ! status
    case RetryFailedSubscription(aggregateId) => sender() ! FailedSubscriptionsHandled(aggregateId, retryFailures(aggregateId))
    case RetryAllFailedSubscriptions =>
      val count = failed.size
      retryRateLimited(failed.keys.toVector)
      sender() ! FailedSubscriptionsRetried(count)
    case SkipFailedVersion(aggregateId, pastMissing) => sender() ! FailedSubscriptionsHandled(aggregateId, skipFailedVersions(aggregateId, pastMissing))
    case RetryNextFailure => retryNextFailure()
    case LoadFailures => loadFailures()
    case NothingToRepublish(aggregateId, version) => nothingAfter(aggregateId, version)
    case q: AsyncDelayedQuery => parkOrSearchAgain(q, q.searchedAt)(delayIfNotAvailableAsync(q.respondTo, q.search, q.until))
    case q: AsyncDelayedQueryCustom[_] => parkOrSearchAgain(q, q.searchedAt)(delayIfNotAvailableAsyncCustom(q.respondTo, q.search, q.found, q.until))
    case ReplayQueries =>
      queriesReplayRequested = false
      replayQueries()
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
    * last version, and the bus (or builder) to answer. `startsAtSquash`: see `SequenceGate` (F98). */
  private case class Pending(versions: Seq[AggregateVersion], events: Seq[EventInfo[Any]], aggregateRoot: Option[Any],
                             respondTo: ActorRef, startsAtSquash: Boolean, fromBuilder: Boolean = false) {
    def first: AggregateVersion = versions.head
    def last: AggregateVersion = versions.last
  }

  /** Versions `from` to `to`, gathered from one or more deliveries (`parts`), applied in one transaction; `covered` are
    * deliveries whose versions all lie in the batch. All of them are acknowledged once the batch is applied. */
  private case class Batch(aggregateType: AggregateType, applied: AggregateVersion, from: AggregateVersion, to: AggregateVersion,
                           events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], parts: Seq[Pending], covered: Seq[Pending],
                           history: Boolean = false) {
    def deliveries: Seq[Pending] = parts ++ covered
    /** Every delivery came from a builder (a chunk's batch has none), or the batch fills, from the owner, the history
      * below a live delivery of an aggregate the subscription had nothing of (F259): the events are history. The owner
      * re-publishes up to its version, so such a batch may hold the live versions too: they are applied as history as
      * well (no push for them), and what follows is live. */
    def fromBuilder: Boolean = history || deliveries.forall(_.fromBuilder)
  }

  private sealed trait DrainReason
  private case object GroupingDelay extends DrainReason // ends early when a delivery that must not wait arrives
  private case object RetryBackOff extends DrainReason
  private case object GapRecheck extends DrainReason     // ends early when any delivery arrives: it may fill the gap

  private case class Drain(key: Key, token: Int)
  private case class Applied(key: Key, queueId: Int, batch: Batch, outcome: Try[Unit])
  private case object RetryNextFailure
  private case object LoadFailures

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
    var liveFrom: Option[AggregateVersion] = None // F259: the first version delivered live after nothing was applied
    def idle: Boolean = pending.isEmpty && !processing && scheduled.isEmpty && gapRequestedFrom.isEmpty && retryOf.isEmpty
  }

  private val queues = mutable.HashMap[Key, Queue]()
  private val failed = mutable.LinkedHashMap[Key, SubscriptionFailure]()
  private var failuresToRetry = Vector[Key]()
  private var purged = PurgedIds.empty // R17
  private var eventBus: Option[ActorRef] = None
  private var unchanged = false // F153: ServeUnchanged
  private var failuresLoaded = false
  private var building: Option[ActorRef] = None // the builder, while the projection is built from the store (D57)
  private var drainTokens = 0
  private var queueIds = 0

  // Missing versions come from the builder while the projection is built, and from the aggregate's owner otherwise.
  private def republisher: Option[ActorRef] = building.orElse(eventBus)

  private def queueFor(key: Key, aggregateType: AggregateType): Queue = queues.getOrElseUpdate(key, {
    queueIds += 1
    new Queue(aggregateType, queueIds)
  })

  private def receiveDelivery(key: Key, aggregateType: AggregateType, delivery: Pending): Unit = {
    if (delivery.versions.isEmpty) {
      log.warning("Projection " + projectionName + " received a delivery without events for aggregate " + key.aggregateId.asLong)
    } else if (purged.contains(key.aggregateId, System.nanoTime())) {
      // R17: the aggregate's organization was purged; a delivery still on its way writes nothing.
      delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind)
    } else if (unchanged) {
      // F153: the new build, subscribed before this one left, takes it.
      delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind)
    } else if (building.isDefined && !removesAggregate(key, delivery)) {
      // D57: the builder reads these events from the store, the bulk pass or the reconciliation after GoLive.
      delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind)
    } else if (failed.contains(key)) {
      delivery.respondTo ! MessageFailed(self, key.aggregateId, delivery.versions, failed(key).error, key.kind)
    } else {
      eventBus = Some(delivery.respondTo)
      enqueue(key, aggregateType, delivery)
    }
  }

  // The aggregates' waiting deliveries are answered, and their queues and failures dropped.
  private def forget(aggregateIds: Set[AggregateId], keepFailures: Boolean = false): Unit = {
    queues.filter { case (key, _) => aggregateIds.contains(key.aggregateId) }.foreach { case (key, queue) =>
      queue.pending.foreach(delivery => delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind))
      if (keepFailures) queue.retryOf.foreach(failure => failed += key -> failure)
    }
    queues --= queues.keys.filter(key => aggregateIds.contains(key.aggregateId))
    if (!keepFailures) failed --= failed.keys.filter(key => aggregateIds.contains(key.aggregateId))
    failuresToRetry = failuresToRetry.filterNot(key => aggregateIds.contains(key.aggregateId))
  }

  // F201: the owner has nothing after `version`. A gap asked from beyond it closes; a failure being retried whose version
  // the aggregate never reached was stale (recorded as missing versions that do not exist), and is cleared.
  private def nothingAfter(aggregateId: AggregateId, version: AggregateVersion): Unit =
    queues.filter { case (key, queue) => key.aggregateId == aggregateId && queue.gapRequestedFrom.exists(_ > version) }.foreach { case (key, queue) =>
      queue.gapRequestedFrom = None
      queue.retryOf.filter(_.version > version).foreach(clearRetried(key, queue, _))
      if (queue.scheduled.exists(_._2 == GapRecheck)) queue.scheduled = None
      forgetIfIdle(key, queue)
    }

  // R17: whether the projection's class implements clearForRebuild; the default refuses every target.
  private def clearsForRebuild: Boolean =
    getClass.getMethod("clearForRebuild", classOf[RebuildTarget], classOf[DBSession]).getDeclaringClass != classOf[ProjectionActor]

  // F111: a delivery that removes its aggregate is applied even while the projection is built: a permanent delete takes
  // the aggregate's events out of the store once every subscriber has it, so no builder could deliver it later. A state
  // listener sees a removal as no state.
  private def removesAggregate(key: Key, delivery: Pending): Boolean = key.subscriptionType match {
    case SubscriptionType.AGGREGATES => delivery.aggregateRoot.isEmpty
    case _ => delivery.events.exists(_.event.isInstanceOf[PermanentDeleteEvent[_]])
  }

  private def enqueue(key: Key, aggregateType: AggregateType, delivery: Pending): Unit = {
    val queue = queueFor(key, aggregateType)
    queue.pending :+= delivery
    options.eventsLogger.filter(_.isDebugEnabled).foreach(_.debug(s"$className ${key.aggregateId.asLong} received ${delivery.versions.map(_.asInt).mkString(",")}"))
    if (!queue.processing && !due.contains(key)) queue.scheduled.map(_._2) match {
      case None => if (mayWait(delivery)) schedule(key, queue, options.groupUpdatesDelayMillis.millis, GroupingDelay) else drain(key, queue)
      case Some(GroupingDelay) => if (!mayWait(delivery)) drain(key, queue)
      case Some(GapRecheck) => drain(key, queue)
      case Some(RetryBackOff) => () // re-deliveries during the retries wait for them (D20a)
    }
  }

  // Grouping (groupUpdatesDelayMillis): live deliveries wait to be applied together, except early versions and
  // events that must be applied at once.
  private def mayWait(delivery: Pending): Boolean =
    options.groupUpdatesDelayMillis > 0 && !delivery.fromBuilder && delivery.first.asInt >= options.minimumDelayVersion &&
      !delivery.events.exists(e => options.eventsToProcessImmediately.contains(e.event.getClass))

  private def schedule(key: Key, queue: Queue, after: FiniteDuration, reason: DrainReason): Unit = {
    drainTokens += 1
    queue.scheduled = Some((drainTokens, reason))
    context.system.scheduler.scheduleOnce(after, self, Drain(key, drainTokens))(context.dispatcher)
  }

  // With groups the queue is drained at the next flush, its version read with the others' (D61); a gap check reads it
  // at once, as it may ask for the missing versions again. A version that cannot be read is the store's fault, not the
  // listener's: it is read again later, without counting as a failure.
  private def drain(key: Key, queue: Queue, gapRecheck: Boolean = false): Unit = {
    queue.scheduled = None
    if (grouping && !gapRecheck) {
      due += key
      if (due.size >= options.maxBatchesPerTransaction) flushGroup() else requestFlush()
    } else Try(appliedVersion(key)) match {
      case Success(applied) => drainFrom(key, queue, applied, gapRecheck)
      case Failure(e) =>
        log.warning("Projection " + projectionName + " could not read its version of aggregate " + key.aggregateId.asLong + ", trying again: " + Errors.describe(e))
        schedule(key, queue, options.retries.max, RetryBackOff)
    }
  }

  // Runs the gate over the waiting deliveries in the order of their first versions: deliveries applied before are
  // acknowledged, the versions after the applied one form one batch, and deliveries after a gap wait for the missing
  // versions. A retried failure waits for its version like a gap until a batch reaches it.
  private def drainFrom(key: Key, queue: Queue, applied: AggregateVersion, gapRecheck: Boolean): Unit = {
    // F201: a failure being retried whose version is applied already (its record outlived the batch that applied it) is
    // cleared, not asked for again: there may be nothing after it to ask for.
    queue.retryOf.filter(_.version <= applied).foreach(clearRetried(key, queue, _))
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
      } else SequenceGate.decide(next, delivery.first, delivery.last, delivery.startsAtSquash) match {
        case AlreadyApplied => covered :+= delivery // all its versions are in this batch
        case Apply(from, to) =>
          parts :+= (delivery -> from)
          next = to
        case Gap(from) =>
          gap = Some(from)
          waiting :+= delivery
          // F259: a subscription that has nothing of the aggregate (reset for a targeted rebuild, or never had it) gets
          // its history from the owner: what comes below this delivery is history, applied as a rebuild applies it.
          if (applied == AggregateVersion.ZERO && !delivery.fromBuilder && queue.liveFrom.isEmpty) queue.liveFrom = Some(delivery.first)
      }
    }
    queue.pending = waiting
    if (gap.isEmpty && parts.isEmpty && queue.retryOf.exists(_.version > applied)) gap = Some(next.increment)

    gap match {
      case Some(from) => requestMissingVersions(key, queue, from, gapRecheck, mayGiveUp = parts.isEmpty) // apply what can be applied first
      case None => queue.gapRequestedFrom = None
    }

    if (parts.nonEmpty) {
      // The batch starts after the applied version, or at a squash event the subscriber never reached (F98).
      val batch = Batch(queue.aggregateType, applied, parts.head._2, next,
        parts.flatMap { case (delivery, from) => delivery.events.dropWhile(_.version < from) },
        parts.last._1.aggregateRoot, parts.map(_._1), covered, history = queue.liveFrom.exists(parts.head._2 < _))
      queue.processing = true
      executionContext match {
        case Some(ec) =>
          val queueId = queue.id
          Future(run(workOf(key, batch)))(ec).onComplete(outcome => self ! Applied(key, queueId, batch, outcome))(context.dispatcher)
        case None => addToGroup(key, queue, batch)
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
      giveUp(key, queue, queue.aggregateType, from, MissingVersions + from.asInt + " are missing: asked " + queue.gapRequests +
        " times in " + options.gapGiveUpAfter + " without an answer", None)
    } else {
      if (!sameGap || gapRecheck) {
        republisher.foreach(_ ! RequestRepublish(self, queue.aggregateType, key.aggregateId, from))
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

  // How a failure of versions that never came starts: it is not a version the listener failed on (F201).
  private val MissingVersions = "versions from "

  private def afterBatch(key: Key, queue: Queue, batch: Batch, outcome: Try[Unit]): Unit = {
    queue.processing = false
    outcome match {
      case Success(_) =>
        batch.deliveries.foreach(delivery => delivery.respondTo ! MessageAck(self, key.aggregateId, delivery.versions, key.kind))
        ReactiveMetrics(context.system).metrics.count("reactivecqrs.projection.applied", Map("projection" -> projectionName), batch.to.asInt - batch.from.asInt + 1L)
        options.eventsLogger.filter(_.isDebugEnabled).foreach(_.debug(s"$className ${key.aggregateId.asLong} applied ${batch.from.asInt}-${batch.to.asInt}"))
        queue.failedAttempts = 0
        if (queue.liveFrom.exists(_ <= batch.to.increment)) queue.liveFrom = None // F259: what follows is live
        queue.retryOf.filter(_.version <= batch.to).foreach(clearRetried(key, queue, _))
        requestQueriesReplay()
        // Deliveries that arrived meanwhile may fill a gap, so they are looked at now, not at the gap's next check.
        if (queue.pending.nonEmpty || queue.retryOf.isDefined) drain(key, queue) else forgetIfIdle(key, queue)
      case Failure(e) if Errors.transient(e) =>
        // F254: the database's failure, not the listener's: tried again, without counting it, so a database that is down
        // for a while leaves no failure records behind.
        queue.pending = batch.deliveries.toVector ++ queue.pending
        val now = System.nanoTime()
        if (now - lastTransientLog >= TransientLogInterval) {
          lastTransientLog = now
          log.warning("Projection " + projectionName + " cannot reach its database (" + Errors.describe(e) + "); its batches are tried again")
        }
        schedule(key, queue, options.retries.max, RetryBackOff)
      case Failure(e) =>
        queue.failedAttempts += 1
        queue.pending = batch.deliveries.toVector ++ queue.pending
        if (options.retries.exhausted(queue.failedAttempts)) {
          giveUp(key, queue, batch.aggregateType, batch.from, Errors.describe(e), Some(e))
        } else {
          // One warning per failing batch; its retries log at debug level, so a problem across many aggregates does not flood the log.
          val message = "Projection " + projectionName + " failed on " + batch.aggregateType.simpleName + " " + key.aggregateId.asLong +
            " versions " + batch.from.asInt + "-" + batch.to.asInt + " (attempt " + queue.failedAttempts + "), retrying: " + Errors.describe(e)
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
        ReactiveMetrics(context.system).metrics.count("reactivecqrs.projection.failed", Map("projection" -> projectionName), 1L)
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

  private var lastTransientLog = 0L
  private val TransientLogInterval = 1.minute.toNanos

  // F201: a retried failure whose version is applied is cleared; until its record goes, the queue keeps it, so the clear
  // is tried again (a record left behind would come back at the next start as a gap that never fills).
  private def clearRetried(key: Key, queue: Queue, failure: SubscriptionFailure): Unit =
    Try(subscriptionsState.autoCommit(subscriptionsState.clearFailure(subscriberName, key.subscriptionType, key.aggregateId)(_))) match {
      case Success(_) =>
        queue.retryOf = None
        failureCleared(failure)
        log.info("Projection " + projectionName + " recovered from its failure on " + failure.aggregateType.simpleName + " " + key.aggregateId.asLong)
      case Failure(e) =>
        log.error(e, "Projection " + projectionName + " applied the failed version of aggregate " + key.aggregateId.asLong +
          " but could not clear its failure record; it tries again")
        schedule(key, queue, options.retries.max, RetryBackOff)
    }

  private def forgetIfIdle(key: Key, queue: Queue): Unit = if (queue.idle) queues -= key

  private def appliedVersion(key: Key): AggregateVersion = subscriptionsState.lastVersion(subscriberName, key.subscriptionType, key.aggregateId)

  private def advance(key: Key, from: AggregateVersion, to: AggregateVersion)(session: DBSession): Unit =
    subscriptionsState.newVersion(subscriberName, key.subscriptionType, key.aggregateId, from, to)(session)

  /** A batch with its listener's work. */
  private case class Work(key: Key, batch: Batch, noTransaction: Boolean, apply: DBSession => Unit)

  // A state listener is told the aggregate is created when the subscription starts with this batch: at version 1, at
  // the squash event (F98) or with a whole state (D62). F262: what the listener's first part (F109) asks to run after the
  // commit goes with its work, into the transaction of its second part; work made again leaves it behind.
  private def workOf(key: Key, batch: Batch): Work = {
    val early = mutable.ArrayBuffer[() => Unit]()
    val outer = afterCommitActions.get
    afterCommitActions.set(early)
    val work = try madeWork(key, batch) finally afterCommitActions.set(outer)
    if (early.isEmpty) work else work.copy(apply = session => { early.foreach(action => afterCommit(action())); work.apply(session) })
  }

  private def madeWork(key: Key, batch: Batch): Work = asHistory(batch.fromBuilder) {
    val id = key.aggregateId
    key.subscriptionType match {
      case SubscriptionType.EVENTS =>
        val listener = eventListenersMap(batch.aggregateType)
        Work(key, batch, listener.options.noTransaction, listener.listener(id, batch.events))
      case SubscriptionType.AGGREGATES =>
        val listener = aggregateListenersMap(batch.aggregateType)
        Work(key, batch, listener.options.noTransaction, listener.listener(id, batch.to, batch.applied == AggregateVersion.ZERO, batch.aggregateRoot))
      case _ =>
        val listener = aggregateWithEventListenersMap(batch.aggregateType)
        Work(key, batch, listener.options.noTransaction, listener.listener(id, batch.to, batch.aggregateRoot, batch.events))
    }
  }

  // The listener and the subscription's new version commit together; a listener without a transaction runs first and
  // the version is written after it, so such a listener may see a batch again if that write fails (at least once).
  private def run(work: Work): Unit = if (work.noTransaction) committing {
    asHistory(work.batch.fromBuilder)(work.apply(null)) // no session: the listener does not write to the database
    subscriptionsState.autoCommit(advance(work.key, work.batch.applied, work.batch.to))
  } else {
    runTogether(Seq(work))
  }

  // Many batches in one transaction (D61): each listener, then its subscription's new version.
  private def runTogether(works: Seq[Work]): Unit = committing(subscriptionsState.localTx { session =>
    works.foreach { work =>
      asHistory(work.batch.fromBuilder)(work.apply(if (work.noTransaction) null else session))
      advance(work.key, work.batch.applied, work.batch.to)(session)
    }
  })

  // F161: what listeners ask to run after their transaction, kept for the attempt at work on its thread (the actor's,
  // or a pool's in parallel mode, where the listener runs too).
  private val afterCommitActions = new ThreadLocal[mutable.ArrayBuffer[() => Unit]]

  /** Runs `action` once the transaction of the listener calling it has committed, and never if it rolls back: a batch
    * may be tried several times, or applied again on its own after its group failed (D61). In either part of a two-part
    * listener (F109, F262); outside a listener it runs at once. Subscribable projections push their updates this way (F161). */
  protected def afterCommit(action: => Unit): Unit = Option(afterCommitActions.get) match {
    case Some(actions) => actions += (() => action)
    case None => action
  }

  private def committing(attempt: => Unit): Unit = {
    val actions = mutable.ArrayBuffer[() => Unit]()
    afterCommitActions.set(actions)
    try attempt finally afterCommitActions.remove()
    actions.foreach(action => Try(action()).failed.foreach(e => log.error(e, "Projection " + projectionName + " failed on an action after a commit")))
  }

  // ************** Groups of batches (D61, F93)

  // Without parallel processing, the queues that get deliveries while deliveries keep arriving are drained together:
  // once the deliveries already in the mailbox are taken (a FlushGroup sent after them), before any other message, or
  // when `maxBatchesPerTransaction` queues are due. Their versions are read at once, one read per listener kind, and
  // their batches run in transactions of up to `maxBatchesPerTransaction`. A batch stays its queue's batch at work until
  // then, so a queue still applies one batch at a time. A group that fails runs again one batch at a time, so each batch
  // keeps its own retries and failure (D20a). A listener without a transaction runs alone: another aggregate's failure
  // must not repeat its side effects.
  private case object FlushGroup
  private case class Grouped(queue: Queue, work: Work)
  private val grouping = executionContext.isEmpty && options.maxBatchesPerTransaction > 1
  private val due = mutable.LinkedHashSet[Key]() // queues to drain at the next flush
  private var group = Vector[Grouped]()
  private var flushRequested = false
  private var flushing = false

  private def takenWhileGrouping(message: Any): Boolean = message match {
    case _: IdentifiableEvents[_] | _: AggregateWithType[_] | _: AggregateWithTypeAndEvents[_] | _: Drain | RetryNextFailure | FlushGroup => true
    case _ => false
  }

  private def requestFlush(): Unit = if (!flushRequested && !flushing) {
    flushRequested = true
    self ! FlushGroup
  }

  // A listener that fails while it makes its work fails the batch, as one that fails in it.
  private def addToGroup(key: Key, queue: Queue, batch: Batch): Unit = Try(workOf(key, batch)) match {
    case Failure(e) => afterBatch(key, queue, batch, Failure(e))
    case Success(work) if work.noTransaction || !grouping => afterBatch(key, queue, batch, Try(run(work)))
    case Success(work) =>
      group :+= Grouped(queue, work)
      if (group.size >= options.maxBatchesPerTransaction) flushGroup() else requestFlush()
  }

  // Applying a batch may make its queue due again (deliveries that waited behind it), so this goes on until nothing is
  // left; no message arrives meanwhile.
  private def flushGroup(): Unit = if (!flushing) {
    flushing = true
    try {
      while (due.nonEmpty || group.nonEmpty) {
        drainDue()
        applyGroup()
      }
    } finally {
      flushing = false
    }
  }

  private def drainDue(): Unit = if (due.nonEmpty) {
    val waiting = due.toVector.flatMap(key => queues.get(key).filterNot(_.processing).map(key -> _))
    due.clear()
    Try(waiting.groupBy(_._1.subscriptionType).flatMap { case (subscriptionType, ofKind) =>
      subscriptionsState.lastVersions(subscriberName, subscriptionType, ofKind.map(_._1.aggregateId), cache = true)
        .map { case (id, version) => Key(subscriptionType, id) -> version }
    }) match {
      case Success(applied) => waiting.foreach { case (key, queue) => drainFrom(key, queue, applied(key), gapRecheck = false) }
      case Failure(e) =>
        log.warning("Projection " + projectionName + " could not read its versions of " + waiting.size + " aggregates, trying again: " + Errors.describe(e))
        waiting.foreach { case (key, queue) => schedule(key, queue, options.retries.max, RetryBackOff) }
    }
  }

  private def applyGroup(): Unit = while (group.nonEmpty) {
    val (grouped, rest) = group.splitAt(options.maxBatchesPerTransaction)
    group = rest
    Try(runTogether(grouped.map(_.work))) match {
      case Success(_) => grouped.foreach(g => afterBatch(g.work.key, g.queue, g.work.batch, Success(())))
      case Failure(e) if grouped.size > 1 =>
        log.debug("Projection " + projectionName + " failed on a group of " + grouped.size + " batches; they are applied one at a time: " + Errors.describe(e))
        grouped.foreach(g => afterBatch(g.work.key, g.queue, g.work.batch, Try(run(workOf(g.work.key, g.work.batch)))))
      case failure => grouped.foreach(g => afterBatch(g.work.key, g.queue, g.work.batch, failure))
    }
  }

  // ************** Chunks from a builder (D58, D61)

  // The gate decides each item against the version the chunk left its subscription at; the decided batches run in one
  // transaction with their versions. A subscription with deliveries at work takes its items in its queue, so its
  // versions are applied one batch at a time; one with a gap or a failure skips the rest of the chunk. A chunk that
  // fails is handed to the queues whole, which apply, retry and record failures one aggregate at a time (D20a).
  private def applyChunk(chunk: ApplyChunk, builder: ActorRef): Unit = {
    // One read per listener kind for the chunk's aggregates, not one per aggregate (D61).
    val leftAt = mutable.HashMap[Key, AggregateVersion]()
    chunk.items.groupBy(_.subscriptionType).foreach { case (subscriptionType, items) =>
      subscriptionsState.lastVersions(subscriberName, subscriptionType, items.map(_.aggregateId)).foreach { case (id, version) =>
        leftAt(Key(subscriptionType, id)) = version
      }
    }
    val stopped = mutable.HashSet[Key]()
    var gaps = Vector[ChunkGap]()
    var toQueues = Vector[(Key, ChunkItem)]()
    var batches = Vector[(Key, ChunkItem, Batch)]()
    chunk.items.foreach { item =>
      val key = Key(item.subscriptionType, item.aggregateId)
      if (failed.contains(key) || stopped.contains(key)) {
        () // a failure is repaired by a retry; a gap is read again by the builder
      } else if (queues.contains(key) && item.unreadable.isEmpty) {
        toQueues :+= key -> item
      } else {
        val applied = leftAt(key)
        SequenceGate.decide(applied, item.first, item.last, item.startsSequence) match {
          case AlreadyApplied => ()
          case Apply(from, _) if item.unreadable.isDefined =>
            // F82: a stored event that cannot be read fails its subscription, visibly, instead of stopping the build;
            // its queue, if it has one, is answered as failed too (D20a).
            giveUp(key, queueFor(key, item.aggregateType), item.aggregateType, from, "a stored event cannot be read: " + item.unreadable.get, None)
            stopped += key
          case Apply(from, to) =>
            batches :+= ((key, item, Batch(item.aggregateType, applied, from, to, item.events.dropWhile(_.version < from), item.aggregateRoot, Nil, Nil)))
            leftAt(key) = to
          case Gap(from) =>
            gaps :+= ChunkGap(item.subscriptionType, item.aggregateType, item.aggregateId, from)
            stopped += key
        }
      }
    }
    val committed = batches.isEmpty || Try(runTogether(batches.map { case (key, _, batch) => workOf(key, batch) })).fold(e => {
      log.warning("Projection " + projectionName + " failed on a chunk of " + batches.size + " batches; they are applied one aggregate at a time: " + Errors.describe(e))
      false
    }, _ => true)
    if (committed) requestQueriesReplay() else toQueues ++= batches.map { case (key, item, _) => key -> item }
    // The builder learns how many answers to expect before the first of them.
    builder ! ChunkApplied(chunk.id, gaps, toQueues.size)
    toQueues.foreach { case (key, item) =>
      val versions = if (item.events.nonEmpty) item.events.map(_.version) else (item.first.asInt to item.last.asInt).map(AggregateVersion(_))
      enqueue(key, item.aggregateType, Pending(versions, item.events, item.aggregateRoot, builder, item.startsSequence, fromBuilder = true))
    }
  }

  // ************** Failed subscriptions (D20a)

  // Once per start: the bus of every node asks (M5), some of them again.
  private def loadFailures(): Unit = if (!failuresLoaded) {
    Try(subscriptionsState.failures(subscriberName)) match {
      case Success(stored) =>
        failuresLoaded = true
        stored.foreach(failure => failed += Key(failure.subscriptionType, failure.aggregateId) -> failure)
        if (options.retryFailuresOnStart && stored.nonEmpty) {
          log.info("Projection " + projectionName + " retries " + stored.size + " failed subscriptions")
          retryRateLimited(stored.sortBy(_.since).map(failure => Key(failure.subscriptionType, failure.aggregateId)).toVector)
        }
      case Failure(e) =>
        // F201: read again later; on a single node no bus asks again, and unread failures would block nothing visibly.
        log.error(e, "Projection " + projectionName + " could not read its failed subscriptions; it reads them again in " + options.retries.max)
        context.system.scheduler.scheduleOnce(options.retries.max, self, LoadFailures)(context.dispatcher)
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

  // F166: what waits in the aggregate's outbox may wait for this failure: a permanent delete is not carried out while a
  // subscription of the aggregate is failed. The owner re-publishes it now, through the bus, not at the poller's next
  // turn (up to 30 minutes). Without a bus yet (a build before it goes live), the poller does it.
  private def failureCleared(failure: SubscriptionFailure): Unit =
    eventBus.foreach(_ ! Republish(failure.aggregateType, failure.aggregateId))

  // Marks each failed version as applied, then asks once for the versions after it: there may be none, and a later
  // event of the aggregate asks for any that went missing. F201: a failure of versions that never came marks nothing:
  // those versions may not exist yet, and the next event would be taken for applied; it asks for them again instead,
  // unless asked to move past them (F304).
  private def skipFailedVersions(aggregateId: AggregateId, pastMissing: Boolean): Int = failed.keys.filter(_.aggregateId == aggregateId).toSeq.count { key =>
    val failure = failed(key)
    val waitForMissing = failure.error.startsWith(MissingVersions) && !pastMissing
    Try(subscriptionsState.localTx { session =>
      // From the version applied, which is below the one before the failure where the failure is at a squash start (F113).
      val applied = appliedVersion(key)
      if (applied < failure.version && !waitForMissing) advance(key, applied, failure.version)(session)
      subscriptionsState.clearFailure(subscriberName, key.subscriptionType, key.aggregateId)(session)
    }) match {
      case Success(_) =>
        log.warning("Projection " + projectionName + " skipped version " + failure.version.asInt + " of " + failure.aggregateType.simpleName + " " + aggregateId.asLong)
        failed -= key
        val next = if (waitForMissing) failure.version else failure.version.increment
        republisher.foreach(_ ! RequestRepublish(self, failure.aggregateType, failure.aggregateId, next))
        failureCleared(failure)
        true
      case Failure(e) =>
        log.error(e, "Projection " + projectionName + " could not skip version " + failure.version.asInt + " of aggregate " + aggregateId.asLong)
        false
    }
  }

  private def status: ProjectionStatus = {
    val failures = failed.values.toSeq ++ queues.values.flatMap(_.retryOf)
    ProjectionStatus(projectionName,
      failures.sortBy(f => (f.since, f.aggregateId.asLong)).take(100).map(f => f.copy(error = f.error.take(500))),
      queues.collect { case (key, queue) if queue.failedAttempts > 0 || queue.retryOf.isDefined => key.aggregateId }.toSeq.distinct.take(1000),
      queues.collect { case (key, queue) if queue.gapRequestedFrom.isDefined && queue.retryOf.isEmpty => key.aggregateId }.toSeq.distinct.take(1000),
      failures.size)
  }

  protected def receiveQuery: Receive

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
  private var updatesApplied = 0L // F163

  // F28: one replay of the waiting queries for a burst of applied updates, not one per update.
  private def requestQueriesReplay(): Unit = {
    updatesApplied += 1
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
          answerAtDeadline(query)
        }
      })

      scheduleNearest()
    }
  }

  // F163: a search under way when an update was applied may have missed it, and no replay was asked for its query, which
  // was not parked yet: it searches again, until its deadline, when it is answered (F234: updates may never stop).
  private def parkOrSearchAgain(query: DelayedQuery, searchedAt: Long)(searchAgain: => Unit): Unit =
    if (!query.until.isAfter(Instant.now())) answerAtDeadline(query)
    else if (searchedAt < updatesApplied) searchAgain else {
      delayedQueries ::= query
      scheduleNearest()
    }

  // F163: at its deadline a query is answered with a last search, what an update with no replay for it may have changed;
  // the custom variant answers what that search found, of its type, as on success.
  private def answerAtDeadline(query: DelayedQuery): Unit = query match {
    case q: SyncDelayedQuery => q.respondTo ! Try(q.search()).fold(Status.Failure(_), identity)
    case q: AsyncDelayedQuery => answerWith(q.respondTo, q.search)
    case q: AsyncDelayedQueryCustom[_] => answerWith(q.respondTo, q.search)
  }

  private def answerWith(respondTo: ActorRef, search: () => Future[Any]): Unit =
    started(search).onComplete {
      case Success(result) => respondTo ! result
      case Failure(e) => respondTo ! Status.Failure(e)
    }(context.system.dispatcher)


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

  // F234: a search that throws fails its query only, also while the parked queries are searched again.
  private def started[T](search: () => Future[T]): Future[T] = try search() catch { case NonFatal(e) => Future.failed(e) }

  protected def delayIfNotAvailableAsync[T](respondTo: ActorRef, search: () => Future[Option[T]], until: Instant): Unit = {
    val searchedAt = updatesApplied
    started(search).onComplete {
      case Success(result@Some(_)) => respondTo ! result
      case Success(None) => self ! AsyncDelayedQuery(until, respondTo, search, searchedAt)
      case Failure(ex) => respondTo ! Status.Failure(ex)
    }(context.system.dispatcher)
  }

  protected def delayIfNotAvailableAsyncCustom[T](respondTo: ActorRef, search: () => Future[T], found: T => Boolean, until: Instant): Unit = {
    val searchedAt = updatesApplied
    started(search).onComplete {
      case Success(result) if found(result) => respondTo ! result
      case Success(_) => self ! AsyncDelayedQueryCustom[T](until, respondTo, search, found, searchedAt)
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
