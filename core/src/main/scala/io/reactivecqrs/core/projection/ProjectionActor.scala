package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import org.apache.pekko.actor.{Actor, ActorRef, Cancellable, Status}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api._
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManagerApi, EventsBusActor}
import EventsBusActor._
import io.reactivecqrs.core.util.MyActorLogging
import scalikejdbc.DBSession

import scala.collection.mutable
import scala.concurrent.Future
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

case class TriggerDelayedUpdateAggregateWithType(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateAggregateWithTypeAndEvents(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateIdentifiableEvents(id: AggregateId, respondTo: ActorRef)


/**
 * eventsToProcessImmediately - only works for listeners receiving events
 **/
abstract class ProjectionActor(groupUpdatesDelayMillis: Long = 0, minimumDelayVersion: Int = 1, eventsToProcessImmediately: Set[Class[_]] = Set.empty) extends Actor with MyActorLogging {

  protected val projectionName: String
  protected val subscriptionsState: SubscriptionsState

  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]

  protected val version: Int

  // Used when updates came not in order
  private val delayedAggregateWithType = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val delayedAggregateWithTypeAndEvent = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val delayedIdentifiableEvent = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()

  // Used for grouping updates
  private val lastAggregateWithTypeUpdateQueueReversed = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val lastAggregateWithTypeAndEventsUpdateQueueReversed = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val lastIdentifiableEventsQueueReversed = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()

  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateRootType: Type
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventsListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, Seq[EventInfo[Any]]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object EventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Boolean, Option[Any]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => DBSession => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateWithEventsListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Option[Any], Seq[EventInfo[Any]]) => DBSession => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateWithEventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => DBSession => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](listener)
  }

  object ReplayQueries



  private lazy val eventListenersMap = {
    validateListeners()
    listeners.filter(_.isInstanceOf[EventsListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[EventsListener[Any]].listener)).toMap
  }

  private lazy val aggregateListenersMap: Map[AggregateType, (AggregateId, AggregateVersion, Boolean, Option[Any]) => DBSession => Unit] ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateListener[Any]].listener)).toMap
  }

  private lazy val aggregateWithEventListenersMap: Map[AggregateType, (AggregateId, AggregateVersion, Option[Any], Seq[EventInfo[Any]]) => DBSession => Unit] ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateWithEventsListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateWithEventsListener[Any]].listener)).toMap
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

  private val delayDuration = scala.concurrent.duration.Duration(groupUpdatesDelayMillis, TimeUnit.MILLISECONDS)

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

    case TriggerDelayedUpdateAggregateWithType(id, respondTo) =>
      handleAggregateWithTypeUpdate(respondTo, mergeAggregateWithTypeUpdate(id, respondTo))
    case TriggerDelayedUpdateAggregateWithTypeAndEvents(id, respondTo) =>
      handleAggreagetWithTypeAndEventsUpdate(respondTo, mergeAggregateWithTypeAndEventsUpdate(id, respondTo))
    case TriggerDelayedUpdateIdentifiableEvents(id, respondTo) =>
      handleIdentifiableEventsUpdate(respondTo, mergeIdentifiableEventsUpdate(id, respondTo))
    case a: AggregateWithType[_] if groupUpdatesDelayMillis > 0 && !a.replayed && a.events.head.asInt >= minimumDelayVersion => // do not delay first event
      lastAggregateWithTypeUpdateQueueReversed.get(a.id) match {
        case None =>
          lastAggregateWithTypeUpdateQueueReversed += a.id -> List(a)
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregateWithType(a.id, sender))(context.dispatcher)
        case Some(waiting) =>
          lastAggregateWithTypeUpdateQueueReversed += a.id -> (a :: waiting)
      }
    case ae: AggregateWithTypeAndEvents[_] if groupUpdatesDelayMillis > 0 && !ae.replayed && ae.events.head.version.asInt >= minimumDelayVersion && !ae.events.exists(e => eventsToProcessImmediately.contains(e.event.getClass)) => // do not delay first event nor events of specific type
      lastAggregateWithTypeAndEventsUpdateQueueReversed.get(ae.id) match {
        case None =>
          lastAggregateWithTypeAndEventsUpdateQueueReversed += ae.id -> List(ae)
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregateWithTypeAndEvents(ae.id, sender))(context.dispatcher)
        case Some(waiting) =>
          lastAggregateWithTypeAndEventsUpdateQueueReversed += ae.id -> (ae :: waiting)
      }
    case e: IdentifiableEvents[_] if groupUpdatesDelayMillis > 0 && !e.replayed && e.events.head.version.asInt >= minimumDelayVersion && !e.events.exists(ee => eventsToProcessImmediately.contains(ee.event.getClass))=> // do not delay first event nor events of specific type
      lastIdentifiableEventsQueueReversed.get(e.aggregateId) match {
        case None =>
          lastIdentifiableEventsQueueReversed += e.aggregateId -> List(e)
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateIdentifiableEvents(e.aggregateId, sender))(context.dispatcher)
        case Some(waiting) =>
          lastIdentifiableEventsQueueReversed += e.aggregateId -> (e :: waiting)
      }
    case a: AggregateWithType[_] =>
      lastAggregateWithTypeUpdateQueueReversed.get(a.id) match {
        case None => handleAggregateWithTypeUpdate(sender, a)
        case Some(waiting) =>
          lastAggregateWithTypeUpdateQueueReversed += a.id -> (a :: waiting)
          handleAggregateWithTypeUpdate(sender, mergeAggregateWithTypeUpdate(a.id, sender))
      }
    case ae: AggregateWithTypeAndEvents[_] =>
      lastAggregateWithTypeAndEventsUpdateQueueReversed.get(ae.id) match {
        case None => handleAggreagetWithTypeAndEventsUpdate(sender, ae)
        case Some(waiting) =>
          lastAggregateWithTypeAndEventsUpdateQueueReversed += ae.id -> (ae :: waiting)
          handleAggreagetWithTypeAndEventsUpdate(sender, mergeAggregateWithTypeAndEventsUpdate(ae.id, sender))
      }

    case e: IdentifiableEvents[_] =>
      lastIdentifiableEventsQueueReversed.get(e.aggregateId) match {
        case None => handleIdentifiableEventsUpdate(sender, e)
        case Some(waiting) =>
          lastIdentifiableEventsQueueReversed += e.aggregateId -> (e :: waiting)
          handleIdentifiableEventsUpdate(sender, mergeIdentifiableEventsUpdate(e.aggregateId, sender))
      }


    case ClearProjectionData => clearProjectionData(sender())
  }



  private def mergeAggregateWithTypeUpdate[A](aggregateId: AggregateId, respondTo: ActorRef): AggregateWithType[A] = {

    val reversed = lastAggregateWithTypeUpdateQueueReversed.getOrElse(aggregateId, List.empty).reverse.asInstanceOf[List[AggregateWithType[A]]]
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
      lastAggregateWithTypeUpdateQueueReversed -= aggregateId
    } else {
      lastAggregateWithTypeUpdateQueueReversed += aggregateId -> skipped
      self ! TriggerDelayedUpdateAggregateWithType(aggregateId, respondTo)
    }

    events
  }

  private def mergeAggregateWithTypeAndEventsUpdate[A](aggregateId: AggregateId, respondTo: ActorRef): AggregateWithTypeAndEvents[A] = {

    val reversed = lastAggregateWithTypeAndEventsUpdateQueueReversed.getOrElse(aggregateId, List.empty).asInstanceOf[List[AggregateWithTypeAndEvents[A]]]

    val ordered = reversed.sortBy(_.events.head.version.asInt)

    var events: AggregateWithTypeAndEvents[A] = ordered.headOption.getOrElse(throw new IllegalArgumentException("Empty list of events"))
    var skipped: List[AggregateWithTypeAndEvents[A]] = List.empty

    ordered.tail.foreach { e =>
      if (e.events.head.version.isJustAfter(events.events.last.version)) {
        events = AggregateWithTypeAndEvents(events.aggregateType, events.id, e.aggregateRoot, events.events ++ e.events, e.replayed)
      } else {
        skipped = e :: skipped
        log.warning("Events are not in order (Aggregate with Events) in projection "+ projectionName+" for aggregate "+aggregateId.asLong+": " + events.events.map(_.version.asInt).mkString(",") + " -> " + e.events.map(_.version.asInt).mkString(","))
      }
    }

    if(skipped.isEmpty) {
      lastAggregateWithTypeAndEventsUpdateQueueReversed -= aggregateId
    } else {
      lastAggregateWithTypeAndEventsUpdateQueueReversed += aggregateId -> skipped
      self ! TriggerDelayedUpdateAggregateWithTypeAndEvents(aggregateId, respondTo)
    }

    events
  }

  private def mergeIdentifiableEventsUpdate[A](aggregateId: AggregateId, respondTo: ActorRef): IdentifiableEvents[A] = {

    val reversed = lastIdentifiableEventsQueueReversed.getOrElse(aggregateId, List.empty).reverse.asInstanceOf[List[IdentifiableEvents[A]]]

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
      lastIdentifiableEventsQueueReversed -= aggregateId
    } else {
      lastIdentifiableEventsQueueReversed += aggregateId -> skipped
      self ! TriggerDelayedUpdateIdentifiableEvents(aggregateId, respondTo)
    }

    events
  }


  private def handleIdentifiableEventsUpdate(respondTo: ActorRef, e: IdentifiableEvents[_]): Unit = {
    val lastVersion = subscriptionsState.localTx { implicit session =>
      subscriptionsState.lastVersionForEventsSubscription(this.getClass.getName, e.aggregateId)
    }

    val alreadyProcessed = e.events.takeWhile(e => e.version <= lastVersion)
    val newEvents = e.events.drop(alreadyProcessed.length)

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(self, e.aggregateId, alreadyProcessed.map(_.version))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          eventListenersMap(e.aggregateType)(e.aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, e.aggregateId, lastVersion, e.events.last.version)(session)
        }
        respondTo ! MessageAck(self, e.aggregateId, e.events.map(_.version))

        delayedIdentifiableEvent.get(e.aggregateId) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(e.events.last.version) =>
            if (delayed.length > 1) {
              delayedIdentifiableEvent += e.aggregateId -> delayed.tail
            } else {
              delayedIdentifiableEvent -= e.aggregateId
            }
            receiveUpdate(delayed.head)
          case _ => ()
        }
        replayQueries()
      } catch {
        case ex: Exception => log.error(ex, "Error handling update " + e.events.head.version.asInt + "-" + e.events.last.version.asInt)
      }
    } else if (newEvents.nonEmpty) {
      log.debug("Delaying events update handling for aggregate " + e.aggregateType.simpleName + ":" + e.aggregateId.asLong + ", got update for version " + e.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedIdentifiableEvent.get(e.aggregateId) match {
        case None => delayedIdentifiableEvent += e.aggregateId -> List(e)
        case Some(delayed) => delayedIdentifiableEvent += e.aggregateId -> (e :: delayed).sortBy(_.events.head.version.asInt)
      }
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
    }
  }

  private def handleAggreagetWithTypeAndEventsUpdate(respondTo: ActorRef,ae: AggregateWithTypeAndEvents[_]): Unit = {
    val lastVersion = subscriptionsState.localTx { implicit session =>
      subscriptionsState.lastVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id)
    }
    val alreadyProcessed = ae.events.takeWhile(e => e.version <= lastVersion)
    val newEvents = ae.events.drop(alreadyProcessed.length)

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(self, ae.id, alreadyProcessed.map(_.version))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          aggregateWithEventListenersMap(ae.aggregateType)(ae.id, newEvents.last.version, ae.aggregateRoot, newEvents.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id, lastVersion, newEvents.last.version)(session)
        }
        respondTo ! MessageAck(self, ae.id, alreadyProcessed.map(_.version) ++ newEvents.map(_.version))
        delayedAggregateWithTypeAndEvent.get(ae.id) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(newEvents.last.version) =>
            if (delayed.length > 1) {
              delayedAggregateWithTypeAndEvent += ae.id -> delayed.tail
            } else {
              delayedAggregateWithTypeAndEvent -= ae.id
            }
            receiveUpdate(delayed.head)
          case _ => ()
        }
        replayQueries()
      } catch {
        case e: Exception => log.error(e, "Error handling update " + newEvents.head.version.asInt + "-" + ae.events.last.version.asInt)
      }
    } else if (newEvents.nonEmpty) {
      log.debug("Delaying aggregate with events update handling for aggregate " + ae.aggregateType.simpleName + ":" + ae.id.asLong + ", got update for version " + ae.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedAggregateWithTypeAndEvent.get(ae.id) match {
        case None => delayedAggregateWithTypeAndEvent += ae.id -> List(ae)
        case Some(delayed) => delayedAggregateWithTypeAndEvent += ae.id -> (ae :: delayed).sortBy(_.events.head.version.asInt)
      }
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
    }
  }

  private def handleAggregateWithTypeUpdate(respondTo: ActorRef,a: AggregateWithType[_]): Unit = {
    val lastVersion = subscriptionsState.localTx { implicit session =>
      subscriptionsState.lastVersionForAggregateSubscription(this.getClass.getName, a.id)
    }
    val alreadyProcessed = a.events.takeWhile(e => e <= lastVersion)
    val newEvents = a.events.drop(alreadyProcessed.length)

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      respondTo ! MessageAck(self, a.id, alreadyProcessed)
    } else if (newEvents.nonEmpty && newEvents.head.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          aggregateListenersMap(a.aggregateType)(a.id, a.version, newEvents.head.isOne, a.aggregateRoot)(session)
          subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, a.id, lastVersion, newEvents.last)(session)
        }
        respondTo ! MessageAck(self, a.id, alreadyProcessed ++ newEvents)

        delayedAggregateWithType.get(a.id) match {
          case Some(delayed) if delayed.head.events.head.isJustAfter(a.version) =>

            if (delayed.length > 1) {
              delayedAggregateWithType += a.id -> delayed.tail
            } else {
              delayedAggregateWithType -= a.id
            }
            receiveUpdate(delayed.head)
          case _ => ()
        }
        replayQueries()
      } catch {
        case e: Exception => log.error(e, "Error handling update " + newEvents.head.asInt + "-" + a.events.last.asInt)
      }
    } else if (newEvents.nonEmpty) {
      log.debug("Delaying aggregate update handling for aggregate " + a.aggregateType.simpleName + ":" + a.id.asLong + ", got update for version " + a.events.map(_.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
      delayedAggregateWithType.get(a.id) match {
        case None => delayedAggregateWithType += a.id -> List(a)
        case Some(delayed) => delayedAggregateWithType += a.id -> (a :: delayed).sortBy(_.events.head.asInt)
      }
    } else {
      throw new IllegalArgumentException("Received empty list of events!")
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

    val now = Instant.now()

    val queries = delayedQueries
    delayedQueries = List.empty

    queries.foreach(query => {
      if(query.until.isAfter(now)) {
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
