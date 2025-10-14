package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import org.apache.pekko.actor.{Actor, ActorRef, Cancellable, Status}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api._
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManagerApi, EventsBusActor}
import EventsBusActor._
import io.reactivecqrs.core.util.MyActorLogging
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.DBSession

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}
import scala.concurrent.duration.FiniteDuration

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

case class InitRebuildForTypes(aggregatesTypes: Iterable[AggregateType])

case class TriggerDelayedUpdateAggregate(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateAggregateWithEvents(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateEvents(id: AggregateId, respondTo: ActorRef)


object ProjectionActorOptions {
  val DEFAULT = ProjectionActorOptions(0, 1, Set.empty, 0, None)
}
case class ProjectionActorOptions (groupUpdatesDelayMillis: Long,
                                   minimumDelayVersion: Int,
                                   eventsToProcessImmediately: Set[Class[_]],
                                   parallelUpdateProcessing: Int,
                                   eventsLogger: Option[Logger]) {

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

}

object ListenerOptions {
  val DEFAULT = ListenerOptions(false)
  val DISABLED_TRANSACTION = ListenerOptions(true)
}
case class ListenerOptions(noTransaction: Boolean = false)

case class MessageAggregateProcessed(aggregateId: AggregateId, respondTo: ActorRef)
case class MessageAggregateWithEventsProcessed(aggregateId: AggregateId, respondTo: ActorRef)
case class MessageEventsProcessed(aggregateId: AggregateId, respondTo: ActorRef)

abstract class ProjectionActor(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends Actor with MyActorLogging {

  private val className = this.getClass.getSimpleName

  protected val projectionName: String
  protected val subscriptionsState: SubscriptionsState

  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]

  protected val version: Int

  // Used for grouping updates
  private val delayedAggregateUpdates = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val delayedAggregateWithEventsUpdate = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val delayedEventsUpdate = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()


  private val triggerScheduledForAggregate = mutable.Map[AggregateId, Boolean]()

  private val processedAggregates = new ConcurrentHashMap[Long, Boolean]()


  private val executionContext: Option[ExecutionContext] = options.parallelUpdateProcessing match {
    case threads if threads <= 1 => None
    case threads => Some(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threads)))
  }

  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateRootType: Type
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventsListener[+AGGREGATE_ROOT: TypeTag](callback: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, Seq[EventInfo[Any]]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object EventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](
        (id: AggregateId, events: Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => listener(id, events),
        ListenerOptions.DISABLED_TRANSACTION)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateListener[+AGGREGATE_ROOT: TypeTag](callback: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, AggregateVersion, Boolean, Option[Any]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](
        (id: AggregateId, version: AggregateVersion, created: Boolean, root: Option[AGGREGATE_ROOT]) => DBSession => listener(id, version, created, root),
        ListenerOptions.DISABLED_TRANSACTION)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateWithEventsListener[+AGGREGATE_ROOT: TypeTag](callback: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit, val options: ListenerOptions) extends Listener[AGGREGATE_ROOT] {
    def listener = callback.asInstanceOf[(AggregateId, AggregateVersion, Option[Any], Seq[EventInfo[Any]]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateWithEventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](listener, ListenerOptions.DEFAULT)

    def noDBSession[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](
        (id: AggregateId, version: AggregateVersion, root: Option[AGGREGATE_ROOT], events: Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => listener(id, version, root, events),
        ListenerOptions.DISABLED_TRANSACTION)
  }

  object ReplayQueries



  private lazy val eventListenersMap: Map[AggregateType, EventsListener[Any]] = {
    validateListeners()
    listeners.filter(_.isInstanceOf[EventsListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[EventsListener[Any]])).toMap
  }

  private lazy val aggregateListenersMap: Map[AggregateType, AggregateListener[Any]] ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateListener[Any]])).toMap
  }

  private lazy val aggregateWithEventListenersMap: Map[AggregateType, AggregateWithEventsListener[Any]] ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateWithEventsListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateWithEventsListener[Any]])).toMap
  }

  override def receive: Receive = logReceive {receiveUpdate orElse receiveQuery}

  private def validateListeners() = {
    if(listeners.exists(l => l.aggregateRootType == typeOf[Any] || l.aggregateRootType == typeOf[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(l => l.aggregateRootType))
    }
  }

  def getSubscribedAggregates: SubscribedAggregates = {
    SubscribedAggregates(projectionName, version, aggregateListenersMap.keySet ++ eventListenersMap.keySet ++ aggregateWithEventListenersMap.keySet)
  }

  private val delayDuration = scala.concurrent.duration.Duration(options.groupUpdatesDelayMillis, TimeUnit.MILLISECONDS)

  protected def receiveUpdate: Receive =  {
    case q: InitRebuildForTypes => subscribe(Some(q.aggregatesTypes))
    case GetSubscribedAggregates => sender() ! getSubscribedAggregates
    case q: AsyncDelayedQuery =>
      delayedQueries ::= q
      scheduleNearest()
    case q: AsyncDelayedQueryCustom[Any] =>
      delayedQueries ::= q
      scheduleNearest()
    case ReplayQueries => replayQueries()

    case TriggerDelayedUpdateAggregate(id, respondTo) =>
      triggerScheduledForAggregate -= id

      delayedAggregateUpdates.get(id) match {
        case None => () // do nothing
        case Some(waiting) =>
          handleAggregateWithTypeUpdate(self, respondTo, mergeAggregateWithTypeUpdate(self, id, respondTo))
      }

    case TriggerDelayedUpdateAggregateWithEvents(id, respondTo) =>
      triggerScheduledForAggregate -= id

      delayedAggregateWithEventsUpdate.get(id) match {
        case None => () // do nothing
        case Some(waiting) =>
          handleAggregateWithTypeAndEventsUpdate(self, respondTo, mergeAggregateWithTypeAndEventsUpdate(self, id, respondTo))
      }

    case TriggerDelayedUpdateEvents(id, respondTo) =>
      triggerScheduledForAggregate -= id

      delayedEventsUpdate.get(id) match {
        case None => () // do nothing
        case Some(waiting) => handleIdentifiableEventsUpdate(self, respondTo, mergeIdentifiableEventsUpdate(self, id, respondTo))
      }

    case a: AggregateWithType[_] if !a.replayed && (options.groupUpdatesDelayMillis > 0 && a.events.head.asInt >= options.minimumDelayVersion || processedAggregates.contains(a.id.asLong)) => // do not delay first event

      delayedAggregateUpdates += a.id -> (a :: delayedAggregateUpdates.getOrElse(a.id, List.empty))

      if(options.groupUpdatesDelayMillis > 0 && !triggerScheduledForAggregate.contains(a.id)) {
        context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregate(a.id, sender()))(context.dispatcher)
        triggerScheduledForAggregate += a.id -> true
      }

    case ae: AggregateWithTypeAndEvents[_] if !ae.replayed && (options.groupUpdatesDelayMillis > 0 && ae.events.head.version.asInt >= options.minimumDelayVersion && !ae.events.exists(e => options.eventsToProcessImmediately.contains(e.event.getClass)) || processedAggregates.contains(ae.id.asLong)) => // do not delay first event nor events of specific type

      delayedAggregateWithEventsUpdate += ae.id -> (ae :: delayedAggregateWithEventsUpdate.getOrElse(ae.id, List.empty))

      if(options.groupUpdatesDelayMillis > 0 && !triggerScheduledForAggregate.contains(ae.id)) {
        context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregateWithEvents(ae.id, sender()))(context.dispatcher)
        triggerScheduledForAggregate += ae.id -> true
      }

    case e: IdentifiableEvents[_] if !e.replayed && (options.groupUpdatesDelayMillis > 0 && e.events.head.version.asInt >= options.minimumDelayVersion && !e.events.exists(ee => options.eventsToProcessImmediately.contains(ee.event.getClass)) || processedAggregates.contains(e.aggregateId.asLong)) => // do not delay first event nor events of specific type

      delayedEventsUpdate += e.aggregateId -> (e :: delayedEventsUpdate.getOrElse(e.aggregateId, List.empty))

      if(options.groupUpdatesDelayMillis > 0 && !triggerScheduledForAggregate.contains(e.aggregateId)) {
        context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateEvents(e.aggregateId, sender()))(context.dispatcher)
        triggerScheduledForAggregate += e.aggregateId -> true
      }

    case a: AggregateWithType[_] =>
      delayedAggregateUpdates.get(a.id) match {
        case None => handleAggregateWithTypeUpdate(self, sender(), a)
        case Some(waiting) =>
          delayedAggregateUpdates += a.id -> (a :: waiting)
          handleAggregateWithTypeUpdate(self, sender(), mergeAggregateWithTypeUpdate(self, a.id, sender()))
      }
    case ae: AggregateWithTypeAndEvents[_] =>
      delayedAggregateWithEventsUpdate.get(ae.id) match {
        case None =>
          handleAggregateWithTypeAndEventsUpdate(self, sender(), ae)
        case Some(waiting) =>
          delayedAggregateWithEventsUpdate += ae.id -> (ae :: waiting)
          handleAggregateWithTypeAndEventsUpdate(self, sender(), mergeAggregateWithTypeAndEventsUpdate(self, ae.id, sender()))
      }

    case e: IdentifiableEvents[_] =>
      delayedEventsUpdate.get(e.aggregateId) match {
        case None => handleIdentifiableEventsUpdate(self, sender(), e)
        case Some(waiting) =>
          delayedEventsUpdate += e.aggregateId -> (e :: waiting)
          handleIdentifiableEventsUpdate(self, sender(), mergeIdentifiableEventsUpdate(self, e.aggregateId, sender()))
      }

    case e: MessageAggregateProcessed => handleMessageAggregateProcessed(self, e.respondTo, e.aggregateId)
    case e: MessageAggregateWithEventsProcessed => handleMessageAggregateWithEventsProcessed(self, e.respondTo, e.aggregateId)
    case e: MessageEventsProcessed => handleMessageEventsProcessed(self, e.respondTo, e.aggregateId)

    case ClearProjectionData => clearProjectionData(sender())
  }



  private def mergeAggregateWithTypeUpdate[A](mySelf: ActorRef, aggregateId: AggregateId, respondTo: ActorRef): AggregateWithType[A] = {

    val reversed = delayedAggregateUpdates.getOrElse(aggregateId, List.empty).reverse.asInstanceOf[List[AggregateWithType[A]]]
    val ordered = reversed.sortBy(_.events.head.asInt)


    var events: AggregateWithType[A] = ordered.headOption.getOrElse(throw new IllegalArgumentException("Empty list of events"))
    var skipped: List[AggregateWithType[A]] = List.empty

    ordered.tail.foreach { e =>
      if (e.events.head.isJustAfter(events.events.last)) {
        events = AggregateWithType(events.aggregateType, events.id, e.version, events.events ++ e.events, e.aggregateRoot, e.replayed)
      } else {
        skipped = e :: skipped
        log.warning("Events are not in order (Aggregate) in projection "+ projectionName+" for aggregate "+aggregateId.asLong+": " + events.events.map(_.asInt).mkString(",") + " -> " + e.events.map(_.asInt).mkString(","))
      }
    }

    if(skipped.isEmpty) {
      delayedAggregateUpdates -= aggregateId
    } else {
      delayedAggregateUpdates += aggregateId -> skipped
      mySelf ! TriggerDelayedUpdateAggregate(aggregateId, respondTo)
    }

    events
  }

  private def mergeAggregateWithTypeAndEventsUpdate[A](mySelf: ActorRef, aggregateId: AggregateId, respondTo: ActorRef): AggregateWithTypeAndEvents[A] = {

    val reversed = delayedAggregateWithEventsUpdate.getOrElse(aggregateId, List.empty).asInstanceOf[List[AggregateWithTypeAndEvents[A]]]

    val ordered = reversed.sortBy(_.events.head.version.asInt)

    var events: AggregateWithTypeAndEvents[A] = ordered.headOption.getOrElse(throw new IllegalArgumentException("Empty list of events"))
    var skipped: List[AggregateWithTypeAndEvents[A]] = List.empty

    ordered.tail.foreach { e =>
      if (e.events.head.version.isJustAfter(events.events.last.version)) {
        events = AggregateWithTypeAndEvents(events.aggregateType, events.id, e.aggregateRoot, events.events ++ e.events, e.replayed)
//        log.debug("Handling delayed aggregate with events update for aggregate " + e.aggregateType.simpleName + ":" + e.id.asLong + ", events: " + events.events.map(_.version.asInt).mkString(","))
      } else {
        skipped = e :: skipped
        log.warning("Events are not in order (Aggregate with Events) in projection "+ projectionName+" for aggregate "+aggregateId.asLong+": " + events.events.map(_.version.asInt).mkString(",") + " -> " + e.events.map(_.version.asInt).mkString(","))
      }
    }

    if(skipped.isEmpty) {
      delayedAggregateWithEventsUpdate -= aggregateId
    } else {
      delayedAggregateWithEventsUpdate += aggregateId -> skipped
      triggerScheduledForAggregate += aggregateId -> true
      mySelf ! TriggerDelayedUpdateAggregateWithEvents(aggregateId, respondTo)
    }

    events
  }

  private def mergeIdentifiableEventsUpdate[A](mySelf: ActorRef, aggregateId: AggregateId, respondTo: ActorRef): IdentifiableEvents[A] = {

    val reversed = delayedEventsUpdate.getOrElse(aggregateId, List.empty).reverse.asInstanceOf[List[IdentifiableEvents[A]]]

    val ordered = reversed.sortBy(_.events.head.version.asInt)

//    reverse.tail.foldLeft(reverse.head)((a1, a2) => IdentifiableEvents(a1.aggregateType, a1.aggregateId, a1.events ++ a2.events, a2.replayed))

    var events: IdentifiableEvents[A] = ordered.headOption.getOrElse(throw new IllegalArgumentException("Empty list of events"))
    var skipped: List[IdentifiableEvents[A]] = List.empty

    ordered.tail.foreach { e =>
      if (e.events.head.version.isJustAfter(events.events.last.version)) {
        events = IdentifiableEvents(events.aggregateType, events.aggregateId, events.events ++ e.events, e.replayed)
      } else {
        skipped = e :: skipped
        log.warning("Events are not in order (Events) in projection "+ projectionName+" for aggregate "+aggregateId.asLong+": " + events.events.map(_.version.asInt).mkString(",") + " -> " + e.events.map(_.version.asInt).mkString(","))
      }
    }

    if(skipped.isEmpty) {
      delayedEventsUpdate -= aggregateId
    } else {
      delayedEventsUpdate += aggregateId -> skipped
      mySelf ! TriggerDelayedUpdateEvents(aggregateId, respondTo)
    }

    events
  }


  private def handleIdentifiableEventsUpdate(mySelf: ActorRef, respondTo: ActorRef, e: IdentifiableEvents[_]): Unit = {
    val lastVersion = subscriptionsState.lastVersionForEventsSubscription(this.getClass.getName, e.aggregateId)

    val alreadyProcessed = e.events.takeWhile(e => e.version <= lastVersion)
    val newEvents = e.events.drop(alreadyProcessed.length)

    options.eventsLogger.foreach(_.debug(s"${className} events for aggregate ${e.aggregateId.asLong} recived, events: ${e.events.map(_.version.asInt).mkString(",")}"))

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(mySelf, e.aggregateId, alreadyProcessed.map(_.version))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {

      executionContext match {
        case Some(context) =>
          options.eventsLogger.foreach(_.debug(s"${className} processing events update for aggregate ${e.aggregateId.asLong} in async mode"))
          processedAggregates.put(e.aggregateId.asLong, true)
          Future {
            processEventsUpdate(mySelf, respondTo, e, lastVersion, async = true)
          }(context).onComplete {
            case Failure(ex) =>
              log.debug(s"Error on async events update processing for aggregate ${e.aggregateId.asLong}: ${ex.getMessage}", ex)
            case _ => ()
          }(context)
        case None =>
          processEventsUpdate(mySelf, respondTo, e, lastVersion, async = false)
      }

    } else if (newEvents.nonEmpty) { // not just after previous update
//      log.debug("Delaying events update handling for aggregate " + e.aggregateType.simpleName + ":" + e.aggregateId.asLong + ", got update for version " + e.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedEventsUpdate += e.aggregateId -> (e :: delayedEventsUpdate.getOrElse(e.aggregateId, List.empty))
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
    }
  }

  private def processEventsUpdate(mySelf: ActorRef, respondTo: ActorRef, e: IdentifiableEvents[_], lastVersion: AggregateVersion, async: Boolean): Unit = {
    val aggregateId = e.aggregateId
    try {
      eventListenersMap(e.aggregateType) match {
        case listener if listener.options.noTransaction => // Process outside transaction, so DB wont be affected too much, should be used when projectionis not storage based
          listener.listener(aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(null) // null to be passed as dbsession mock
          subscriptionsState.autoCommit { session =>
            subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, aggregateId, lastVersion, e.events.last.version)(session)
          }
        case listener =>
          subscriptionsState.localTx { session =>
            listener.listener(aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
            subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, aggregateId, lastVersion, e.events.last.version)(session)
          }
      }

      respondTo ! MessageAck(mySelf, aggregateId, e.events.map(_.version))

      options.eventsLogger.foreach(_.debug(s"${className} aggregate ${e.aggregateId.asLong} events ACK sent: ${e.events.map(_.version.asInt).mkString(",")} to ${respondTo.path}") )

    } catch {
      case ex: Exception => log.error(ex, "Error handling update " + e.events.head.version.asInt + "-" + e.events.last.version.asInt)
    } finally {

      if(async) {
        mySelf ! MessageEventsProcessed(aggregateId, respondTo)
      } else {
        handleMessageEventsProcessed(mySelf, respondTo, aggregateId)
      }
    }

  }

  private def handleMessageEventsProcessed(mySelf: ActorRef, respondTo: ActorRef, aggregateId: AggregateId): Unit = {
    processedAggregates.remove(aggregateId.asLong)

    if (delayedQueries.nonEmpty) {
      mySelf ! ReplayQueries
    }

    if (delayedEventsUpdate.contains(aggregateId) && !triggerScheduledForAggregate.contains(aggregateId)) {
      mySelf ! TriggerDelayedUpdateEvents(aggregateId, respondTo)
      triggerScheduledForAggregate += aggregateId -> true
    }
  }

  private def handleAggregateWithTypeAndEventsUpdate(mySelf: ActorRef, respondTo: ActorRef, ae: AggregateWithTypeAndEvents[_]): Unit = {
    val lastVersion = subscriptionsState.lastVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id)
    val alreadyProcessed: Seq[EventInfo[_]] = ae.events.takeWhile(e => e.version <= lastVersion)
    val newEvents: Seq[EventInfo[_]] = ae.events.drop(alreadyProcessed.length)

    options.eventsLogger.foreach(_.debug(s"${className} events with aggregate ${ae.id.asLong} received, events: ${ae.events.map(_.version.asInt).mkString(",")}"))

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(mySelf, ae.id, alreadyProcessed.map(_.version))
      log.debug("Sending ACK message for already processed events for aggregate " + ae.id+": "+alreadyProcessed.map(_.version.asInt).mkString(","))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {


      executionContext match {
        case Some(context) =>
          processedAggregates.put(ae.id.asLong, true)
          options.eventsLogger.foreach(_.debug(s"${className} will process asynchronously events for aggregate ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}"))
          Future {
            try {
              processAggregateWithEventsUpdate(mySelf, respondTo, ae, newEvents, alreadyProcessed, lastVersion, async = true)
            } catch {
              case ex: Exception =>
                log.error(ex, "Error handling update " + newEvents.head.version.asInt + "-" + ae.events.last.version.asInt)
                options.eventsLogger.foreach(_.error(s"${className} processing events aggregate exception within future ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}: ${ex.getClass.getSimpleName} ${ex.getMessage}"))
                throw ex
            }
          }(context).onComplete {
            case Failure(ex) =>
              options.eventsLogger.foreach(_.error(s"${className} processing events aggregate failure ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}: ${ex.getClass.getSimpleName} ${ex.getMessage}"))
              log.debug(s"Error on async aggregate with events update processing for aggregate ${ae.id.asLong}: ${ex.getMessage}", ex)
            case _ => ()
          }(context)
        case None =>
          processAggregateWithEventsUpdate(mySelf, respondTo, ae, newEvents, alreadyProcessed, lastVersion, async = false)
      }


    } else if (newEvents.nonEmpty) { // not just after previous update
      log.debug("Delaying aggregate with events update handling for aggregate " + ae.aggregateType.simpleName + ":" + ae.id.asLong + ", got update for version " + ae.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedAggregateWithEventsUpdate += ae.id -> (ae :: delayedAggregateWithEventsUpdate.getOrElse(ae.id, List.empty))
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
    }
  }

  private def processAggregateWithEventsUpdate(mySelf: ActorRef, respondTo: ActorRef, ae: AggregateWithTypeAndEvents[_], newEvents: Seq[EventInfo[_]], alreadyProcessed: Seq[EventInfo[_]], lastVersion: AggregateVersion, async: Boolean): Unit = {
    val aggregateId = ae.id
    options.eventsLogger.foreach(_.debug(s"${className} processing events aggregate ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}"))
    try {

      aggregateWithEventListenersMap(ae.aggregateType) match {
        case listener if listener.options.noTransaction => // Process outside transaction, so DB wont be affected too much, should be used when projectionis not storage based
          options.eventsLogger.foreach(_.debug(s"${className} processing events outside transaction for aggregate ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}"))
          listener.listener(aggregateId, newEvents.last.version, ae.aggregateRoot, newEvents.asInstanceOf[Seq[EventInfo[Any]]])(null) // null to be passed as dbsession mock
          subscriptionsState.autoCommit { session =>
            subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, aggregateId, lastVersion, newEvents.last.version)(session)
          }
        case listener =>
          options.eventsLogger.foreach(_.debug(s"${className} processing events for aggregate in transaction ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}"))
          subscriptionsState.localTx { session =>
            listener.listener(aggregateId, newEvents.last.version, ae.aggregateRoot, newEvents.asInstanceOf[Seq[EventInfo[Any]]])(session)
            subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, aggregateId, lastVersion, newEvents.last.version)(session)
          }
      }

      respondTo ! MessageAck(mySelf, aggregateId, alreadyProcessed.map(_.version) ++ newEvents.map(_.version))

      options.eventsLogger.foreach(_.debug(s"${className} aggregate ${ae.id.asLong} events ACK sent: ${ae.events.map(_.version.asInt).mkString(",")} to ${respondTo.path}") )
    } catch {
      case e: Exception =>
        options.eventsLogger.foreach(_.error(s"${className} processing events for aggregate error ${ae.id}: ${ae.events.map(_.version.asInt).mkString(",")}: ${e.getClass.getSimpleName} ${e.getMessage}"))
        log.error(e, "Error handling update " + newEvents.head.version.asInt + "-" + ae.events.last.version.asInt)
    } finally {

      if(async) {
        mySelf ! MessageAggregateWithEventsProcessed(aggregateId, respondTo)
      } else {
        handleMessageAggregateWithEventsProcessed(mySelf, respondTo, aggregateId)
      }
    }


  }

  private def handleMessageAggregateWithEventsProcessed(mySelf: ActorRef, respondTo: ActorRef, aggregateId: AggregateId): Unit = {
    processedAggregates.remove(aggregateId.asLong)
    if(delayedQueries.nonEmpty) {
      mySelf ! ReplayQueries
    }

    options.eventsLogger.foreach(_.error(s"${className} maybe triggering update of delayed events for ${aggregateId.asLong}, delayed: ${delayedAggregateWithEventsUpdate.contains(aggregateId)}, scheduled: ${triggerScheduledForAggregate.contains(aggregateId)}"))
    if(delayedAggregateWithEventsUpdate.contains(aggregateId) && !triggerScheduledForAggregate.contains(aggregateId)) {
      mySelf ! TriggerDelayedUpdateAggregateWithEvents(aggregateId, respondTo)
      triggerScheduledForAggregate += aggregateId -> true
    }
  }


  private def handleAggregateWithTypeUpdate(mySelf: ActorRef, respondTo: ActorRef,a: AggregateWithType[_]): Unit = {
    val lastVersion = subscriptionsState.lastVersionForAggregateSubscription(this.getClass.getName, a.id)
    val alreadyProcessed: Seq[AggregateVersion] = a.events.takeWhile(e => e <= lastVersion)
    val newEvents: Seq[AggregateVersion] = a.events.drop(alreadyProcessed.length)

    options.eventsLogger.foreach(_.debug(s"${className} aggrgeate ${a.id.asLong} recived, events: ${a.events.map(_.asInt).mkString(",")}"))

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(mySelf, a.id, alreadyProcessed)
    } else if (newEvents.nonEmpty && newEvents.head.isJustAfter(lastVersion)) {

      executionContext match {
        case Some(context) =>
          processedAggregates.put(a.id.asLong, true)
          Future {
            processAggregateUpdate(mySelf, respondTo, a, newEvents, alreadyProcessed, lastVersion, async = true)
          }(context).onComplete {
            case Failure(ex) => log.debug(s"Error on async aggregate ${a.id.asLong} update processing: ${ex.getMessage}", ex)
            case _ => ()
          }(context)
        case None =>
          processAggregateUpdate(mySelf, respondTo, a, newEvents, alreadyProcessed, lastVersion, async = false)
      }

    } else if (newEvents.nonEmpty) { // not just after previous update
//      log.debug("Delaying aggregate update handling for aggregate " + a.aggregateType.simpleName + ":" + a.id.asLong + ", got update for version " + a.events.map(_.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedAggregateUpdates += a.id -> (a :: delayedAggregateUpdates.getOrElse(a.id, List.empty))
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
    }
  }

  private def processAggregateUpdate(mySelf: ActorRef, respondTo: ActorRef, a: AggregateWithType[_], newEvents: Seq[AggregateVersion], alreadyProcessed: Seq[AggregateVersion], lastVersion: AggregateVersion, async: Boolean): Unit = {
    val aggregateId = a.id
    try {

      aggregateListenersMap(a.aggregateType) match {
        case listener if listener.options.noTransaction => // Process outside transaction, so DB wont be affected too much, should be used when projectionis not storage based
          listener.listener(aggregateId, a.version, newEvents.head.isOne, a.aggregateRoot)(null) // null to be passed as dbsession mock
          subscriptionsState.autoCommit { session =>
            subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, aggregateId, lastVersion, newEvents.last)(session)
          }
        case listener =>
          subscriptionsState.localTx { session =>
            listener.listener(aggregateId, a.version, newEvents.head.isOne, a.aggregateRoot)(session)
            subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, aggregateId, lastVersion, newEvents.last)(session)
          }
      }

      respondTo ! MessageAck(mySelf, aggregateId, alreadyProcessed ++ newEvents)

      options.eventsLogger.foreach(_.debug(s"${className} aggregate ${a.id.asLong} events ACK sent: ${a.events.map(_.asInt).mkString(",")} to ${respondTo.path}") )

    } catch {
      case e: Exception => log.error(e, "Error handling update " + newEvents.head.asInt + "-" + a.events.last.asInt)
    } finally {

      if(async) {
        mySelf ! MessageAggregateProcessed(aggregateId, respondTo)
      } else {
        handleMessageAggregateProcessed(mySelf, respondTo, aggregateId)
      }
    }


  }

  private def handleMessageAggregateProcessed(mySelf: ActorRef, respondTo: ActorRef, aggregateId: AggregateId): Unit = {
    processedAggregates.remove(aggregateId.asLong)
    if(delayedQueries.nonEmpty) {
      mySelf ! ReplayQueries
    }

    if(delayedAggregateUpdates.contains(aggregateId) && !triggerScheduledForAggregate.contains(aggregateId)) {
      mySelf ! TriggerDelayedUpdateAggregate(aggregateId, respondTo)
      triggerScheduledForAggregate += aggregateId -> true
    }
  }

  protected def receiveQuery: Receive

  private def clearProjectionData(replyTo: ActorRef): Unit = {
    subscriptionsState.clearSubscriptionsInfo(this.getClass.getName)
    replyTo ! ProjectionDataCleared
    onClearProjectionData()
  }

  protected def onClearProjectionData(): Unit

  override def preStart(): Unit = {
    subscribe(None)
  }

  private def subscribe(aggregates: Option[Iterable[AggregateType]]): Unit = {
    eventBusSubscriptionsManager.subscribe(aggregateListenersMap.keySet.toList.filter(t => aggregates.isEmpty || aggregates.get.exists(_ == t)).map { aggregateType =>
      SubscribeForAggregates("", aggregateType, self)
    })

    eventBusSubscriptionsManager.subscribe(eventListenersMap.keySet.toList.filter(t => aggregates.isEmpty || aggregates.get.exists(_ == t)).map { aggregateType =>
      SubscribeForEvents("", aggregateType, self)
    })

    eventBusSubscriptionsManager.subscribe(aggregateWithEventListenersMap.keySet.toList.filter(t => aggregates.isEmpty || aggregates.get.exists(_ == t)).map { aggregateType =>
      SubscribeForAggregatesWithEvents("", aggregateType, self)
    })
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  // ************** Queries delay - needed if query is for document that might not been yet updated, but update is in it's way

  private var delayedQueries = List[DelayedQuery]()

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

  protected def delayIfNotAvailable[T](respondTo: ActorRef, search: () => Option[T], until: Instant): Unit = {
    val result: Option[Any] = try {
      search()
    } catch {
      case ex: Exception =>
        respondTo ! Status.Failure(ex)
        throw ex
    }
    if (result.isDefined) {
      respondTo ! result
    } else {
      delayedQueries ::= SyncDelayedQuery(until, respondTo, search)
      scheduleNearest()
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

  var replayQueriesScheduled: Option[Cancellable] = None

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
