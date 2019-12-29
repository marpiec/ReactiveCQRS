package io.reactivecqrs.core.projection

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Cancellable, Status}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api._
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManagerApi, EventsBusActor}
import EventsBusActor._
import io.reactivecqrs.core.util.ActorLogging
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

case class ClearProjectionData(projectionVersions: Map[String, Int])
case object ProjectionDataCleared


case class InitRebuildForTypes(aggregatesTypes: Iterable[AggregateType])

abstract class ProjectionActor extends Actor with ActorLogging {

  protected val subscriptionsState: SubscriptionsState

  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]

  protected val version: Int

  private val delayedAggregateWithType = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val delayedAggregateWithTypeAndEvent = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val delayedIdentifiableEvent = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()


  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateRootType: Type
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventsListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => (DBSession) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, Seq[EventInfo[Any]]) => (DBSession) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object EventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, Seq[EventInfo[AGGREGATE_ROOT]]) => (DBSession) => Unit): EventsListener[AGGREGATE_ROOT] =
      new EventsListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Int, Option[AGGREGATE_ROOT]) => (DBSession) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Int, Option[Any]) => (DBSession) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Int, Option[AGGREGATE_ROOT]) => (DBSession) => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateWithEventsListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => (DBSession) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Option[Any], Seq[EventInfo[Any]]) => (DBSession) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateWithEventsListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT], Seq[EventInfo[AGGREGATE_ROOT]]) => (DBSession) => Unit): AggregateWithEventsListener[AGGREGATE_ROOT] =
      new AggregateWithEventsListener[AGGREGATE_ROOT](listener)
  }

  object ReplayQueries



  private lazy val eventListenersMap = {
    validateListeners()
    listeners.filter(_.isInstanceOf[EventsListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[EventsListener[Any]].listener)).toMap
  }

  private lazy val aggregateListenersMap ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateListener[Any]].listener)).toMap
  }

  private lazy val aggregateWithEventListenersMap ={
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

  protected def receiveUpdate: Receive =  {
    case q: InitRebuildForTypes => subscribe(Some(q.aggregatesTypes))
    case q: AsyncDelayedQuery =>
      delayedQueries ::= q
      scheduleNearest()
    case ReplayQueries => replayQueries()
    case a: AggregateWithType[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForAggregateSubscription(this.getClass.getName, a.id)
      }
      val firstEventVersion: AggregateVersion = AggregateVersion(a.version.asInt - a.eventsCount + 1)
      if (firstEventVersion <= lastVersion.incrementBy(1) && a.version > lastVersion) {
        try {
          subscriptionsState.localTx { session =>
            aggregateListenersMap(a.aggregateType)(a.id, a.version, a.eventsCount, a.aggregateRoot)(session)
            subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, a.id, lastVersion, a.version)(session)
          }
          sender() ! MessageAck(self, a.id, AggregateVersion.upTo(a.version, a.eventsCount))
          replayQueries()
          delayedAggregateWithType.get(a.id) match {
            case Some(delayed) if delayed.head.version.isJustAfter(a.version) =>
              if (delayed.length > 1) {
                delayedAggregateWithType += a.id -> delayed.tail
              } else {
                delayedAggregateWithType -= a.id
              }
              receiveUpdate(delayed.head)
            case _ => ()
          }
        } catch {
          case e: Exception => log.error(e, "Error handling update " + a.version.asInt)
        }
      } else if (a.version <= lastVersion) {
        sender() ! MessageAck(self, a.id, AggregateVersion.upTo(a.version, a.eventsCount))
      } else {
        log.debug("Delaying aggregate update handling for aggregate "+a.aggregateType.simpleName+":"+a.id.asLong+", got update for version " + Range(a.version.asInt - a.eventsCount + 1, a.version.asInt + 1).mkString(", ")+" but only processed version " + lastVersion.asInt)
        delayedAggregateWithType.get(a.id) match {
          case None => delayedAggregateWithType += a.id -> List(a)
          case Some(delayed) => delayedAggregateWithType += a.id -> (a :: delayed).sortBy(_.version.asInt)
        }
      }
    case ae: AggregateWithTypeAndEvents[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id)
      }
      val alreadyProcessed = ae.events.takeWhile(e => e.version <= lastVersion)
      val newEvents = ae.events.drop(alreadyProcessed.length)

      if(newEvents.isEmpty && alreadyProcessed.nonEmpty) {
        sender() ! MessageAck(self, ae.id, alreadyProcessed.map(_.version))
      } else if(newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
        try {
          subscriptionsState.localTx { session =>
            aggregateWithEventListenersMap(ae.aggregateType)(ae.id, newEvents.last.version, ae.aggregateRoot, newEvents.asInstanceOf[Seq[EventInfo[Any]]])(session)
            subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id, lastVersion, newEvents.last.version)(session)
          }
          sender() ! MessageAck(self, ae.id, alreadyProcessed.map(_.version) ++ newEvents.map(_.version))
          replayQueries()
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
        } catch {
          case e: Exception => log.error(e, "Error handling update " + newEvents.head.version.asInt +"-"+ae.events.last.version.asInt)
        }
      } else if(newEvents.nonEmpty) {
        log.debug("Delaying aggregate with events update handling for aggregate "+ae.aggregateType.simpleName+":"+ae.id.asLong+", got update for version " + ae.events.map(_.version.asInt).mkString(", ")+" but only processed version " + lastVersion.asInt)
        delayedAggregateWithTypeAndEvent.get(ae.id) match {
          case None => delayedAggregateWithTypeAndEvent += ae.id -> List(ae)
          case Some(delayed) => delayedAggregateWithTypeAndEvent += ae.id -> (ae :: delayed).sortBy(_.events.head.version.asInt)
        }
      } else {
        throw new IllegalArgumentException("Received empty list of events!")
      }

    case e: IdentifiableEvents[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForEventsSubscription(this.getClass.getName, e.aggregateId)
      }

      val alreadyProcessed = e.events.takeWhile(e => e.version <= lastVersion)
      val newEvents = e.events.drop(alreadyProcessed.length)

      if(newEvents.isEmpty && alreadyProcessed.nonEmpty) {
        sender() ! MessageAck(self, e.aggregateId, alreadyProcessed.map(_.version))
      } else if(newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
        try {
          subscriptionsState.localTx { session =>
            eventListenersMap(e.aggregateType)(e.aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
            subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, e.aggregateId, lastVersion, e.events.last.version)(session)
            //          println("handled event " + e.event)
          }
          sender() ! MessageAck(self, e.aggregateId, e.events.map(_.version))
          replayQueries()
          delayedIdentifiableEvent.get(e.aggregateId) match {
            case Some(delayed) if delayed.head.events.head.version.isJustAfter(e.events.last.version) =>
              if (delayed.length > 1) {
                delayedIdentifiableEvent += e.aggregateId -> delayed.tail
              } else {
                delayedIdentifiableEvent -= e.aggregateId
              }
              //            println("replaying delayed event " + e.event)
              receiveUpdate(delayed.head)
            case _ => ()
          }
        } catch {
          case ex: Exception => log.error(ex, "Error handling update " + e.events.head.version.asInt +"-"+e.events.last.version.asInt)
        }
      } else if(newEvents.nonEmpty) {
        log.debug("Delaying events update handling for aggregate "+e.aggregateType.simpleName+":"+e.aggregateId.asLong+", got update for version " + e.events.map(_.version.asInt).mkString(", ")+" but only processed version " + lastVersion.asInt)
        delayedIdentifiableEvent.get(e.aggregateId) match {
          case None => delayedIdentifiableEvent += e.aggregateId -> List(e)
          case Some(delayed) => delayedIdentifiableEvent += e.aggregateId -> (e :: delayed).sortBy(_.events.head.version.asInt)
        }
      } else {
        throw new IllegalArgumentException("Received empty list of events!")
      }
    case ClearProjectionData(versions) => if(version > versions.getOrElse(this.getClass.getName, 0)) {
      clearProjectionData(sender())
    } // otherwise ignore
  }

  protected def receiveQuery: Receive

  private def clearProjectionData(replyTo: ActorRef): Unit = {
    subscriptionsState.clearSubscriptionsInfo(this.getClass.getName)
    replyTo ! ProjectionDataCleared
    onClearProjectionData()
  }

  protected def onClearProjectionData(): Unit

  override def preStart() {
    subscribe(None)
  }

  private def subscribe(aggregates: Option[Iterable[AggregateType]]) {
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

  override def postRestart(reason: Throwable) {
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
        }
      } else {
        query.respondTo ! None
      }
    })

    scheduleNearest()
  }


  protected def delayIfNotAvailable[T](respondTo: ActorRef, search: () => Option[T], forMaximumMillis: Int): Unit = {
    delayIfNotAvailable(respondTo, search, Instant.now().plusMillis(forMaximumMillis))
  }

  protected def delayIfNotAvailableAsync[T](respondTo: ActorRef, search: () => Future[Option[T]], forMaximumMillis: Int): Unit = {
    delayIfNotAvailableAsync(respondTo, search, Instant.now().plusMillis(forMaximumMillis))
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
