package io.reactivecqrs.core.projection

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManagerApi, EventsBusActor}
import EventsBusActor._
import io.reactivecqrs.core.util.ActorLogging
import scalikejdbc.{DB, DBSession}

import scala.collection.mutable
import scala.reflect.runtime.universe._



private case class DelayedQuery(until: Instant, respondTo: ActorRef, search: () => Option[Any])

abstract class ProjectionActor extends Actor with ActorLogging {

  protected val subscriptionsState: SubscriptionsState

  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]

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
    case a: AggregateWithType[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForAggregateSubscription(this.getClass.getName, a.id)
      }
      val firstEventVersion: AggregateVersion = AggregateVersion(a.version.asInt - a.eventsCount + 1)
      if(firstEventVersion.isJustAfter(lastVersion)) {
        subscriptionsState.localTx { session =>
          aggregateListenersMap(a.aggregateType)(a.id, a.version, 1, a.aggregateRoot)(session)
          subscriptionsState.newVersionForAggregatesSubscription(this.getClass.getName, a.id, lastVersion, a.version)(session)
        }
        sender() ! MessageAck(self, a.id, AggregateVersion.upTo(a.version, a.eventsCount))
        replayQueries()
        delayedAggregateWithType.get(a.id) match {
          case Some(delayed) if delayed.head.version.isJustAfter(a.version) =>
            if(delayed.length > 1) {
              delayedAggregateWithType += a.id -> delayed.tail
            } else {
              delayedAggregateWithType -= a.id
            }
            receiveUpdate(delayed.head)
          case _ => ()
        }
      } else if (a.version <= lastVersion) {
        sender() ! MessageAck(self, a.id, AggregateVersion.upTo(a.version, a.eventsCount))
      } else {
        delayedAggregateWithType.get(a.id) match {
          case None => delayedAggregateWithType += a.id -> List(a)
          case Some(delayed) => delayedAggregateWithType += a.id -> (a :: delayed).sortBy(_.version.asInt)
        }
      }

    case ae: AggregateWithTypeAndEvents[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id)
      }
      if(ae.events.head.version.isJustAfter(lastVersion)) {
        subscriptionsState.localTx { session =>
          aggregateWithEventListenersMap(ae.aggregateType)(ae.id, ae.events.last.version, ae.aggregateRoot, ae.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForAggregatesWithEventsSubscription(this.getClass.getName, ae.id, lastVersion, ae.events.last.version)(session)
        }
        sender() ! MessageAck(self, ae.id, ae.events.map(_.version))
        replayQueries()
        delayedAggregateWithTypeAndEvent.get(ae.id) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(ae.events.last.version) =>
            if(delayed.length > 1) {
              delayedAggregateWithTypeAndEvent += ae.id -> delayed.tail
            } else {
              delayedAggregateWithTypeAndEvent -= ae.id
            }
            receiveUpdate(delayed.head)
          case _ => ()
        }
      } else if (ae.events.last.version <= lastVersion) {
        sender() ! MessageAck(self, ae.id, ae.events.map(_.version))
      } else {
        delayedAggregateWithTypeAndEvent.get(ae.id) match {
          case None => delayedAggregateWithTypeAndEvent += ae.id -> List(ae)
          case Some(delayed) => delayedAggregateWithTypeAndEvent += ae.id -> (ae :: delayed).sortBy(_.events.head.version.asInt)
        }
      }
    case e: IdentifiableEvents[_] =>
      val lastVersion = subscriptionsState.localTx { implicit session =>
        subscriptionsState.lastVersionForEventsSubscription(this.getClass.getName, e.aggregateId)
      }
      if(e.events.head.version.isJustAfter(lastVersion)) {
        subscriptionsState.localTx { session =>
          eventListenersMap(e.aggregateType)(e.aggregateId, e.events.asInstanceOf[Seq[EventInfo[Any]]])(session)
          subscriptionsState.newVersionForEventsSubscription(this.getClass.getName, e.aggregateId, lastVersion, e.events.last.version)(session)
//          println("handled event " + e.event)
        }
        sender() ! MessageAck(self, e.aggregateId, e.events.map(_.version))
        replayQueries()
        delayedIdentifiableEvent.get(e.aggregateId) match {
          case Some(delayed) if delayed.head.events.head.version.isJustAfter(e.events.last.version) =>
            if(delayed.length > 1) {
              delayedIdentifiableEvent += e.aggregateId -> delayed.tail
            } else {
              delayedIdentifiableEvent -= e.aggregateId
            }
//            println("replaying delayed event " + e.event)
            receiveUpdate(delayed.head)
          case _ => ()
        }
      } else if (e.events.last.version <= lastVersion) {
        sender() ! MessageAck(self, e.aggregateId, e.events.map(_.version))
//        println("event already handled " + e.event)
      } else {
        delayedIdentifiableEvent.get(e.aggregateId) match {
          case None => delayedIdentifiableEvent += e.aggregateId -> List(e)
          case Some(delayed) => delayedIdentifiableEvent += e.aggregateId -> (e :: delayed).sortBy(_.events.head.version.asInt)
        }
      }
  }

  protected def receiveQuery: Receive


  override def preStart() {
    eventBusSubscriptionsManager.subscribe(aggregateListenersMap.keySet.toList.map { aggregateType =>
      SubscribeForAggregates("", aggregateType, self)
    })

    eventBusSubscriptionsManager.subscribe(eventListenersMap.keySet.toList.map { aggregateType =>
      SubscribeForEvents("", aggregateType, self)
    })

    eventBusSubscriptionsManager.subscribe(aggregateWithEventListenersMap.keySet.toList.map { aggregateType =>
      SubscribeForAggregatesWithEvents("", aggregateType, self)
    })
  }
  // ************** Queries delay - needed if query is for document that might not been yet updated, but update is in it's way

  private var delayedQueries = List[DelayedQuery]()

  private def replayQueries(): Unit = {

    var delayAgain = List[DelayedQuery]()
    val now = Instant.now()
    delayedQueries.filter(_.until.isAfter(now)).foreach(query => {
      val result:Option[Any] = query.search()
      if(result.isDefined) {
        query.respondTo ! result
      } else {
        delayAgain ::= query
      }
    })

    delayedQueries = delayAgain

  }


  protected def delayIfNotAvailable[T](respondTo: ActorRef, search: () => Option[T], forMaximumMillis: Int) {
    val result: Option[Any] = search()
    if (result.isDefined) {
      respondTo ! result
    } else {
      delayedQueries ::= DelayedQuery(Instant.now().plusMillis(forMaximumMillis), respondTo, search)
    }

  }
   
}
