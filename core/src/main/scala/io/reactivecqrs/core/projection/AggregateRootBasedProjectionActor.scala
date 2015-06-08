package io.reactivecqrs.core.projection

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, AggregateWithType}
import io.reactivecqrs.core.EventsBusActor.{MessageAck, SubscribeForAggregates, SubscribedForAggregates}

abstract class AggregateRootBasedProjectionActor extends Actor {

  protected val eventBusActor: ActorRef

  protected val listeners: Map[Class[_], (AggregateId, AggregateVersion, Option[_]) => Unit]

  override def receive: Receive = LoggingReceive(receiveSubscribed(listeners.keySet))

  private def receiveSubscribed(typesRemaining: Set[Class[_]]): Receive = {
    case SubscribedForAggregates(aggregateType) =>
      if(typesRemaining.size == 1 && typesRemaining.head.getName == aggregateType) {
        context.become(LoggingReceive(receiveUpdate orElse receiveQuery))
      } else {
        context.become(LoggingReceive(receiveSubscribed(typesRemaining.filterNot(_.getName == aggregateType))))
      }
  }

  private def receiveUpdate: Receive = {
    case a: AggregateWithType[_] =>
      listeners.find(_._1.getName == a.aggregateType).get._2(a.id, a.version, a.aggregateRoot)
      sender() ! MessageAck(self, a.id, a.version)
  }

  protected def receiveQuery: Receive

  override def preStart() {
    listeners.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForAggregates(aggregateType.getName, self)
    }

  }
}
