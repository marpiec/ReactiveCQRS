package io.reactivecqrs.core.projection

import _root_.io.reactivecqrs.api.id.AggregateId
import _root_.io.reactivecqrs.api.{AggregateVersion, Event}
import _root_.io.reactivecqrs.core.EventsBusActor.{MessageAck, SubscribeForEvents, SubscribedForEvents}
import _root_.io.reactivecqrs.core.api.IdentifiableEvent
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import scala.reflect.runtime.universe._

// listenerField exist because it could not be covariant type, or something like that
class EventListener[+AGGREGATE_ROOT: TypeTag](listenerField: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit){
  def listener = listenerField.asInstanceOf[(AggregateId, AggregateVersion, Event[_]) => Unit]
  def aggregateRootType = typeOf[AGGREGATE_ROOT]
}

object EventListener {
  implicit def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Event[AGGREGATE_ROOT]) => Unit): EventListener[AGGREGATE_ROOT] =
    new EventListener(listener)
}



abstract class EventBasedProjectionActor extends Actor {

  protected val eventBusActor: ActorRef

  protected val listeners:List[EventListener[Any]]
  
  private lazy val listenersMap =
    listeners.map(l => (l.aggregateRootType.toString, l.listener)).toMap

  override def receive: Receive = LoggingReceive(receiveSubscribed(listenersMap.keySet))

  private def receiveSubscribed(typesRemaining: Set[String]): Receive = {
    case SubscribedForEvents(aggregateType) =>
      if(typesRemaining.size == 1 && typesRemaining.head == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(typesRemaining.filterNot(_ == aggregateType))))
      }
  }

  private def receiveUpdate: Receive = {
    case e: IdentifiableEvent[_] =>
      listenersMap(e.aggregateType)(e.aggregateId, e.version, e.event)
      sender() ! MessageAck(self, e.aggregateId, e.version)
  }

  protected def receiveQuery: Receive

  override def preStart() {
    listenersMap.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForEvents(aggregateType, self)
    }
  }

}
