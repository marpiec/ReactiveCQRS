package io.reactivecqrs.core.projection

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api.{AggregateType, Event, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.EventsBusActor.{MessageAck, SubscribeForEvents, SubscribedForEvents}
import io.reactivecqrs.core.api.IdentifiableEvent

import scala.reflect.runtime.universe._



abstract class EventBasedProjectionActor extends Actor {

  // ListenerParam and listener are separately so covariant type is allowed
  protected class Listener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit) {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Event[Any]) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object Listener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit): Listener[AGGREGATE_ROOT] =
      new Listener[AGGREGATE_ROOT](listener)
  }

  protected val eventBusActor: ActorRef

  protected val listeners:List[Listener[Any]]
  
  private lazy val listenersMap = {
    validateListeners()
    listeners.map(l => (AggregateType(l.aggregateRootType.toString), l.listener)).toMap
  }


  override def receive: Receive = LoggingReceive(receiveSubscribed(listenersMap.keySet))

  private def validateListeners() = {
    if(listeners.exists(l => l.aggregateRootType == typeOf[Any] || l.aggregateRootType == typeOf[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(l => l.aggregateRootType))
    }
  }

  private def receiveSubscribed(typesRemaining: Set[AggregateType]): Receive = {
    case SubscribedForEvents(aggregateType) =>
      if(typesRemaining.size == 1 && typesRemaining.head == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(typesRemaining.filterNot(_ == aggregateType))))
      }
  }

  private def receiveUpdate: Receive = {
    case e: IdentifiableEvent[_] =>
      listenersMap(e.aggregateType)(e.aggregateId, e.version, e.event.asInstanceOf[Event[Any]])
      sender() ! MessageAck(self, e.aggregateId, e.version)
  }

  protected def receiveQuery: Receive

  override def preStart() {
    listenersMap.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForEvents(aggregateType, self)
    }
  }

}
