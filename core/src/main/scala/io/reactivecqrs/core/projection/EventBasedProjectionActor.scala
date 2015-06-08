package io.reactivecqrs.core.projection

import akka.actor.{ActorRef, Actor}
import io.reactivecqrs.api.{AggregateVersion, Event}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.EventsBusActor.{SubscribedForEvents, SubscribeForEvents, EventReceived}
import io.reactivecqrs.core.api.IdentifiableEvent

abstract class EventBasedProjectionActor[AGGREGATE_ROOT] extends Actor {

  protected val eventBusActor: ActorRef

  override def receive: Receive = receiveSubscribed

  private def receiveSubscribed: Receive = {
    case m: SubscribedForEvents => context.become(receiveUpdate orElse receiveQuery)
  }

  private def receiveUpdate: Receive = {
    case e: IdentifiableEvent[_] =>
      newEventReceived(e.aggregateId, e.version, e.event.asInstanceOf[Event[AGGREGATE_ROOT]])
      sender() ! EventReceived(self, e.aggregateId, e.version)
  }

  protected def newEventReceived(aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

  protected def receiveQuery: Receive

  override def preStart() {
    eventBusActor ! SubscribeForEvents(self)
  }



}
