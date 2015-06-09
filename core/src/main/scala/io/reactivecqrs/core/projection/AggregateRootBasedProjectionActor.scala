package io.reactivecqrs.core.projection

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, AggregateWithType}
import io.reactivecqrs.core.EventsBusActor.{MessageAck, SubscribeForAggregates, SubscribedForAggregates}

import scala.reflect.runtime.universe._

abstract class AggregateRootBasedProjectionActor extends Actor {

  // ListenerParam and listener are separately so covariant type is allowed
  protected class Listener[+AGGREGATE_ROOT: TypeTag](listenerParam: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT]) => Unit) {
    def listener = listenerParam.asInstanceOf[(AggregateId, AggregateVersion, Option[Any]) => Unit]
    def aggregateRootType = typeOf[AGGREGATE_ROOT]
  }

  protected object Listener {
    def apply[AGGREGATE_ROOT: TypeTag](listener: (AggregateId, AggregateVersion, Option[AGGREGATE_ROOT]) => Unit): Listener[AGGREGATE_ROOT] =
      new Listener[AGGREGATE_ROOT](listener)
  }


  protected val eventBusActor: ActorRef

  protected val listeners:List[Listener[Any]]

  private lazy val listenersMap = {
    validateListeners()
    listeners.map(l => (l.aggregateRootType.toString, l.listener)).toMap
  }

  override def receive: Receive = LoggingReceive(receiveSubscribed(listenersMap.keySet))

  private def validateListeners() = {
    if(listeners.exists(l => l.aggregateRootType == typeOf[Any] || l.aggregateRootType == typeOf[Nothing])) {
      throw new IllegalArgumentException("Listeners cannot have type defined as Nothing, Any or _ but were: " + listeners.map(l => l.aggregateRootType))
    }
  }

  private def receiveSubscribed(typesRemaining: Set[String]): Receive = {
    case SubscribedForAggregates(aggregateType) =>
      if(typesRemaining.size == 1 && typesRemaining.head == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(typesRemaining.filterNot(_ == aggregateType))))
      }
  }

  private def receiveUpdate: Receive = {
    case a: AggregateWithType[_] =>
      listenersMap(a.aggregateType)(a.id, a.version, a.aggregateRoot)
      sender() ! MessageAck(self, a.id, a.version)
  }

  protected def receiveQuery: Receive

  override def preStart() {
    listenersMap.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForAggregates(aggregateType, self)
    }

  }
}
