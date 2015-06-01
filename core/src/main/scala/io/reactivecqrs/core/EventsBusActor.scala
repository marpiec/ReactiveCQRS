package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.core.EventsBusActor.{PublishEvents, PublishEventsAck}
import io.reactivecqrs.core.api.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.db.eventbus.EventBus
import io.reactivecqrs.core.db.eventbus.EventBus.MessageToSend

object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])
  case class PublishEventsAck(eventsIds: Seq[EventIdentifier])
}


class EventsBusActor(eventBus: EventBus) extends Actor {

  override def receive: Receive = {
    case PublishEvents(events) => handlePublishEvents(sender(), events)
  }

  def handlePublishEvents(respondTo: ActorRef, events: Seq[IdentifiableEvent[Any]]): Unit = {
    println("EventsBusActor handlePublishEvents")
    eventBus.persistMessages(events.map(event => MessageToSend[IdentifiableEvent[Any]]("some subscriber", event)))
    respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
  }

}
