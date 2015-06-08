package io.reactivecqrs.core.projection

import _root_.io.reactivecqrs.api.id.AggregateId
import _root_.io.reactivecqrs.api.{AggregateVersion, Event}
import _root_.io.reactivecqrs.core.EventsBusActor.{EventReceived, SubscribeForEvents, SubscribedForEvents}
import _root_.io.reactivecqrs.core.api.IdentifiableEvent
import akka.actor.{Actor, ActorRef}

abstract class EventBasedProjectionActor extends Actor {

  protected val eventBusActor: ActorRef

  protected val listeners: Map[Class[_], (AggregateId, AggregateVersion, Event[_]) => Unit]

  override def receive: Receive = receiveSubscribed(listeners.keySet)

  private def receiveSubscribed(typesRemaining: Set[Class[_]]): Receive = {
    case SubscribedForEvents(aggregateType) =>
      if(typesRemaining.size == 1 && typesRemaining.head.getName == aggregateType) {
        context.become(receiveUpdate orElse receiveQuery)
      } else {
        context.become(receiveSubscribed(typesRemaining.filterNot(_.getName == aggregateType)))
      }
  }

  private def receiveUpdate: Receive = {
    case e: IdentifiableEvent[_] =>
      listeners.find(_._1.getName == e.aggregateType).get._2(e.aggregateId, e.version, e.event)
      sender() ! EventReceived(self, e.aggregateId, e.version)
  }

  protected def receiveQuery: Receive

  override def preStart() {
    listeners.keySet.foreach { aggregateType =>
      eventBusActor ! SubscribeForEvents(aggregateType.getName, self)
    }

  }



}
