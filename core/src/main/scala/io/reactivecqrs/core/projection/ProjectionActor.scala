package io.reactivecqrs.core.projection

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.IdentifiableEvent
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManagerApi, EventsBusActor}
import EventsBusActor._
import io.reactivecqrs.core.util.ActorLogging

import scala.reflect.runtime.universe._



private case class DelayedQuery(until: Instant, respondTo: ActorRef, search: () => Option[Any])

abstract class ProjectionActor extends Actor with ActorLogging {

  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateRootType: Type
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT], UserId, Instant) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Event[Any], UserId, Instant) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object EventListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT], UserId, Instant) => Unit): EventListener[AGGREGATE_ROOT] =
      new EventListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT]) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Option[Any]) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT]) => Unit): AggregateListener[AGGREGATE_ROOT] =
      new AggregateListener[AGGREGATE_ROOT](listener)
  }


  // ListenerParam and listener are separately so covariant type is allowed
  protected class AggregateWithEventListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT], Option[AGGREGATE_ROOT], UserId, Instant) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Event[Any], Option[Any], UserId, Instant) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object AggregateWithEventListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT], Option[AGGREGATE_ROOT], UserId, Instant) => Unit): AggregateWithEventListener[AGGREGATE_ROOT] =
      new AggregateWithEventListener[AGGREGATE_ROOT](listener)
  }


  protected val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi

  protected val listeners:List[Listener[Any]]


  private lazy val eventListenersMap = {
    validateListeners()
    listeners.filter(_.isInstanceOf[EventListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[EventListener[Any]].listener)).toMap
  }

  private lazy val aggregateListenersMap ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateListener[Any]].listener)).toMap
  }

  private lazy val aggregateWithEventListenersMap ={
    validateListeners()
    listeners.filter(_.isInstanceOf[AggregateWithEventListener[Any]])
      .map(l => (AggregateType(l.aggregateRootType.toString), l.asInstanceOf[AggregateWithEventListener[Any]].listener)).toMap
  }

  override def receive: Receive = receiveUpdate orElse receiveQuery

  private def validateListeners() = {
    if(listeners.exists(l => l.aggregateRootType == typeOf[Any] || l.aggregateRootType == typeOf[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(l => l.aggregateRootType))
    }
  }

  protected def receiveUpdate: Receive = logReceive  {
    case a: AggregateWithType[_] =>
      aggregateListenersMap(a.aggregateType)(a.id, a.version, a.aggregateRoot)
      sender() ! EventAck(self, a.id, a.version)
      replayQueries()
    case ae: AggregateWithTypeAndEvent[_] =>
      aggregateWithEventListenersMap(ae.aggregateType)(ae.id, ae.version, ae.event.asInstanceOf[Event[Any]], ae.aggregateRoot, ae.userId, ae.timestamp)
      sender() ! EventAck(self, ae.id, ae.version)
      replayQueries()
    case e: IdentifiableEvent[_] =>
      eventListenersMap(e.aggregateType)(e.aggregateId, e.version, e.event.asInstanceOf[Event[Any]], e.userId, e.timestamp)
      sender() ! EventAck(self, e.aggregateId, e.version)
      replayQueries()
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
