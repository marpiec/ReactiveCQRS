package io.reactivecqrs.core.projection

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{Event, AggregateType, AggregateVersion, AggregateWithType}
import io.reactivecqrs.core.aggregaterepository.IdentifiableEvent
import io.reactivecqrs.core.eventbus.EventsBusActor
import EventsBusActor._

import scala.reflect.runtime.universe._



private case class DelayedQuery(until: Instant, respondTo: ActorRef, search: () => Option[Any])

abstract class ProjectionActor extends Actor {

  protected trait Listener[+AGGREGATE_ROOT]  {
    def aggregateRootType: Type
  }

  // ListenerParam and listener are separately so covariant type is allowed
  protected class EventListener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit) extends Listener[AGGREGATE_ROOT] {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Event[Any]) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object EventListener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit): EventListener[AGGREGATE_ROOT] =
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


  protected val eventBusActor: ActorRef

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


  override def receive: Receive = LoggingReceive(receiveSubscribed(aggregateListenersMap.keySet, eventListenersMap.keySet))

  private def validateListeners() = {
    if(listeners.exists(l => l.aggregateRootType == typeOf[Any] || l.aggregateRootType == typeOf[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(l => l.aggregateRootType))
    }
  }

  private def receiveSubscribed(aggregateListenersRemaining: Set[AggregateType], eventsListenersRemaining: Set[AggregateType]): Receive = {
    case SubscribedForAggregates(aggregateType) =>
      if(eventsListenersRemaining.isEmpty && aggregateListenersRemaining.size == 1 && aggregateListenersRemaining.head == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(aggregateListenersRemaining.filterNot(_ == aggregateType), eventsListenersRemaining)))
      }
    case SubscribedForEvents(aggregateType) =>
      if(aggregateListenersRemaining.isEmpty && eventsListenersRemaining.size == 1 && eventsListenersRemaining.head == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(aggregateListenersRemaining, eventsListenersRemaining.filterNot(_ == aggregateType))))
      }
  }

  private def receiveUpdate: Receive = {
    case a: AggregateWithType[_] =>
      aggregateListenersMap(a.aggregateType)(a.id, a.version, a.aggregateRoot)
      sender() ! MessageAck(self, a.id, a.version)
      replayQueries()
    case e: IdentifiableEvent[_] =>
      eventListenersMap(e.aggregateType)(e.aggregateId, e.version, e.event.asInstanceOf[Event[Any]])
      sender() ! MessageAck(self, e.aggregateId, e.version)
      replayQueries()
  }

  protected def receiveQuery: Receive

  override def preStart() {
    aggregateListenersMap.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForAggregates(aggregateType, self)
    }

    eventListenersMap.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForEvents(aggregateType, self)
    }

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
