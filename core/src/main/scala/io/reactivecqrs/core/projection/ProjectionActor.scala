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

case object ClearProjectionData
case object ProjectionDataCleared

case object GetSubscribedAggregates
case class SubscribedAggregates(projectionName: String, projectionVersion: Int, aggregates: Set[AggregateType])

case class InitRebuildForTypes(aggregatesTypes: Iterable[AggregateType])

case class TriggerDelayedUpdateAggregateWithType(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateAggregateWithTypeAndEvents(id: AggregateId, respondTo: ActorRef)
case class TriggerDelayedUpdateIdentifiableEvents(id: AggregateId, respondTo: ActorRef)


abstract class ProjectionActor(updateDelayMillis: Long = 0) extends Actor with MyActorLogging {

  protected val projectionName: String
  protected val subscriptionsState: SubscriptionsState

  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]

  protected val version: Int

  private val delayedAggregateWithType = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val delayedAggregateWithTypeAndEvent = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val delayedIdentifiableEvent = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()

  private val lastAggregateWithTypeUpdateQueueReversed = mutable.Map[AggregateId, List[AggregateWithType[_]]]()
  private val lastAggregateWithTypeAndEventsUpdateQueueReversed = mutable.Map[AggregateId, List[AggregateWithTypeAndEvents[_]]]()
  private val lastIdentifiableEventsQueueReversed = mutable.Map[AggregateId, List[IdentifiableEvents[_]]]()

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
  protected class AggregateListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => (DBSession) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Boolean, Option[Any]) => (DBSession) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Boolean, Option[AGGREGATE_ROOT]) => (DBSession) => Unit): AggregateListener[AGGREGATE_ROOT] =
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

  private val delayDuration = scala.concurrent.duration.Duration(updateDelayMillis, TimeUnit.MILLISECONDS)

  protected def receiveUpdate: Receive =  {
    case q: InitRebuildForTypes => subscribe(Some(q.aggregatesTypes))
    case GetSubscribedAggregates => sender() ! getSubscribedAggregates
    case q: AsyncDelayedQuery =>
      delayedQueries ::= q
      scheduleNearest()
    case ReplayQueries => replayQueries()

    case TriggerDelayedUpdateAggregateWithType(id, respondTo) =>
      val events = lastAggregateWithTypeUpdateQueueReversed.getOrElse(id, List.empty).reverse
//      println("triggering aggregate update for aggregate " + id +" "+events.head.events.head.asInt+"->"+events.last.events.last.asInt)
      handleAggregateWithTypeUpdate(respondTo, mergeAggregateWithTypeUpdate(lastAggregateWithTypeUpdateQueueReversed.getOrElse(id, List.empty).reverse.asInstanceOf[List[AggregateWithType[Any]]]))
      lastAggregateWithTypeUpdateQueueReversed -= id
    case TriggerDelayedUpdateAggregateWithTypeAndEvents(id, respondTo) =>
      val events = lastAggregateWithTypeAndEventsUpdateQueueReversed.getOrElse(id, List.empty).reverse.last.events
//      println("triggering aggregate update for aggregate with events " + id +" "+events.head.version.asInt+"->"+events.last.version.asInt)
      handleAggreagetWithTypeAndEventsUpdate(respondTo, mergeAggregateWithTypeAndEventsUpdate(lastAggregateWithTypeAndEventsUpdateQueueReversed.getOrElse(id, List.empty).reverse.asInstanceOf[List[AggregateWithTypeAndEvents[Any]]]))
      lastAggregateWithTypeAndEventsUpdateQueueReversed -= id
    case TriggerDelayedUpdateIdentifiableEvents(id, respondTo) =>
      handleIdentifiableEventsUpdate(respondTo, mergeIdentifiableEventsUpdate(lastIdentifiableEventsQueueReversed.getOrElse(id, List.empty).reverse.asInstanceOf[List[IdentifiableEvents[Any]]]))
      lastIdentifiableEventsQueueReversed -= id

    case a: AggregateWithType[_] if updateDelayMillis > 0 && !a.replayed =>
//      println("delaying aggregate update handling for aggregate " + a.aggregateType.simpleName + ":" + a.id.asLong+" "+a.events.last.asInt)
      lastAggregateWithTypeUpdateQueueReversed.get(a.id) match {
        case None =>
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregateWithType(a.id, sender))(context.dispatcher)
          lastAggregateWithTypeUpdateQueueReversed += a.id -> List(a)
        case Some(waiting) =>
          lastAggregateWithTypeUpdateQueueReversed += a.id -> (a :: waiting)
      }
    case a: AggregateWithTypeAndEvents[_] if updateDelayMillis > 0 && !a.replayed =>
//      println("delaying aggregate with events update handling for aggregate " + a.aggregateType.simpleName + ":" + a.id.asLong+" "+a.events.last.version.asInt)
      lastAggregateWithTypeAndEventsUpdateQueueReversed.get(a.id) match {
        case None =>
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateAggregateWithTypeAndEvents(a.id, sender))(context.dispatcher)
          lastAggregateWithTypeAndEventsUpdateQueueReversed += a.id -> List(a)
        case Some(waiting) =>
          lastAggregateWithTypeAndEventsUpdateQueueReversed += a.id -> (a :: waiting)
      }
    case a: IdentifiableEvents[_] if updateDelayMillis > 0 && !a.replayed =>
      lastIdentifiableEventsQueueReversed.get(a.aggregateId) match {
        case None =>
          context.system.scheduler.scheduleOnce(delayDuration, self, TriggerDelayedUpdateIdentifiableEvents(a.aggregateId, sender))(context.dispatcher)
          lastIdentifiableEventsQueueReversed += a.aggregateId -> List(a)
        case Some(waiting) =>
          lastIdentifiableEventsQueueReversed += a.aggregateId -> (a :: waiting)
      }
    case a: AggregateWithType[_] => handleAggregateWithTypeUpdate(sender, a)
    case ae: AggregateWithTypeAndEvents[_] => handleAggreagetWithTypeAndEventsUpdate(sender, ae)
    case e: IdentifiableEvents[_] => handleIdentifiableEventsUpdate(sender, e)
    case ClearProjectionData => clearProjectionData(sender())
  }



  private def mergeAggregateWithTypeUpdate[A](list: List[AggregateWithType[A]]): AggregateWithType[A] = {
    list.tail.foldLeft(list.head)((a1, a2) => AggregateWithType[A](a1.aggregateType, a1.id, a2.version, a1.events ++ a2.events, a2.aggregateRoot, a2.replayed))
  }

  private def mergeAggregateWithTypeAndEventsUpdate[A](reverse: List[AggregateWithTypeAndEvents[A]]): AggregateWithTypeAndEvents[A] = {
    reverse.tail.foldLeft(reverse.head)((a1, a2) => AggregateWithTypeAndEvents(a1.aggregateType, a1.id, a2.aggregateRoot, a1.events ++ a2.events, a2.replayed))
  }

  private def mergeIdentifiableEventsUpdate[A](reverse: List[IdentifiableEvents[A]]): IdentifiableEvents[A] = {
    reverse.tail.foldLeft(reverse.head)((a1, a2) => IdentifiableEvents(a1.aggregateType, a1.aggregateId, a1.events ++ a2.events, a2.replayed))
  }


  private def handleIdentifiableEventsUpdate(respondTo: ActorRef, e: IdentifiableEvents[_]): Unit = {
    val lastVersion = subscriptionsState.localTx { implicit session =>
      subscriptionsState.lastVersionForEventsSubscription(this.getClass.getName, e.aggregateId)
    }

    val alreadyProcessed = e.events.takeWhile(e => e.version <= lastVersion)
    val newEvents = e.events.drop(alreadyProcessed.length)

    if (newEvents.isEmpty && alreadyProcessed.nonEmpty) {
      println("MessageAck (Events) " + getClass.getSimpleName+" -> " + alreadyProcessed.head.version.asInt+"->"+alreadyProcessed.last.version.asInt)
      respondTo ! MessageAck(self, e.aggregateId, alreadyProcessed.map(_.version))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          eventListenersMap(e.aggregateType)(e.aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, e.aggregateId, lastVersion, e.events.last.version)(session)
          //          println("handled event " + e.event)
        }
        println("MessageAck (Events) 1 " + getClass.getSimpleName+" -> " + (alreadyProcessed ++ newEvents).head.version.asInt+"->"+(alreadyProcessed ++ newEvents).last.version.asInt)
        respondTo ! MessageAck(self, e.aggregateId, e.events.map(_.version))

        delayedIdentifiableEvent.get(e.aggregateId) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(e.events.last.version) =>
            if (delayed.length > 1) {
              delayedIdentifiableEvent += e.aggregateId -> delayed.tail
            } else {
              delayedIdentifiableEvent -= e.aggregateId
            }
            //            println("replaying delayed event " + e.event)
            receiveUpdate(delayed.head)
            replayQueries()
          case _ => ()
        }
      } catch {
        case ex: Exception => log.error(ex, "Error handling update " + e.events.head.version.asInt + "-" + e.events.last.version.asInt)
      }
    } else if (newEvents.nonEmpty) {
      println("Delaying events update handling for aggregate " + e.aggregateType.simpleName + ":" + e.aggregateId.asLong + ", got update for version " + e.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
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
      println("MessageAck (Aggregate with events) " + getClass.getSimpleName+" -> " + alreadyProcessed.head.version.asInt+"->"+alreadyProcessed.last.version.asInt)
      respondTo ! MessageAck(self, ae.id, alreadyProcessed.map(_.version))
    } else if (newEvents.nonEmpty && newEvents.head.version.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          aggregateWithEventListenersMap(ae.aggregateType)(ae.id, newEvents.last.version, ae.aggregateRoot, newEvents.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id, lastVersion, newEvents.last.version)(session)
        }
        println("MessageAck (Aggregate with events) 1 " + getClass.getSimpleName+" -> " + (alreadyProcessed ++ newEvents).head.version.asInt+"->"+(alreadyProcessed ++ newEvents).last.version.asInt)
        respondTo ! MessageAck(self, ae.id, alreadyProcessed.map(_.version) ++ newEvents.map(_.version))
        delayedAggregateWithTypeAndEvent.get(ae.id) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(newEvents.last.version) =>
            if (delayed.length > 1) {
              delayedAggregateWithTypeAndEvent += ae.id -> delayed.tail
            } else {
              delayedAggregateWithTypeAndEvent -= ae.id
            }
            receiveUpdate(delayed.head)
            replayQueries()
          case _ => ()
        }
      } catch {
        case e: Exception => log.error(e, "Error handling update " + newEvents.head.version.asInt + "-" + ae.events.last.version.asInt)
      }
    } else if (newEvents.nonEmpty) {
      println("Delaying aggregate with events update handling for aggregate " + ae.aggregateType.simpleName + ":" + ae.id.asLong + ", got update for version " + ae.events.map(_.version.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
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
      println("MessageAck (Aggregate) " + getClass.getSimpleName+" -> " + alreadyProcessed.head.asInt+"->"+alreadyProcessed.last.asInt)
      respondTo ! MessageAck(self, a.id, alreadyProcessed)
    } else if (newEvents.nonEmpty && newEvents.head.isJustAfter(lastVersion)) {
      try {
        subscriptionsState.localTx { session =>
          aggregateListenersMap(a.aggregateType)(a.id, a.version, newEvents.head.isOne, a.aggregateRoot)(session)
          subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, a.id, lastVersion, newEvents.last)(session)
        }
        println("MessageAck (Aggregate) 1 " + getClass.getSimpleName+" -> " + (alreadyProcessed ++ newEvents).head.asInt+"->"+(alreadyProcessed ++ newEvents).last.asInt)
        respondTo ! MessageAck(self, a.id, alreadyProcessed ++ newEvents)

        delayedAggregateWithType.get(a.id) match {
          case Some(delayed) if delayed.head.events.head.isJustAfter(a.version) =>

            if (delayed.length > 1) {
              delayedAggregateWithType += a.id -> delayed.tail
            } else {
              delayedAggregateWithType -= a.id
            }
            receiveUpdate(delayed.head)
            replayQueries()
          case _ => ()
        }
      } catch {
        case e: Exception => log.error(e, "Error handling update " + newEvents.head.asInt + "-" + a.events.last.asInt)
      }
    } else if (newEvents.nonEmpty) {
      println("Delaying aggregate update handling for aggregate " + a.aggregateType.simpleName + ":" + a.id.asLong + ", got update for version " + a.events.map(_.asInt).mkString(", ") + " but only processed version " + lastVersion.asInt)
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
