package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.EventsBusActor._
import io.reactivecqrs.core.api.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.db.eventbus.EventBus

import scala.concurrent.Future

object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])
  case class PublishEventsAck(eventsIds: Seq[EventIdentifier])


  case class SubscribeForEvents(subscriber: ActorRef) // Todo add message classifier?
  case class SubscribedForEvents()

  case class MessagesPersisted(messages: Seq[EventToSend])

  case class EventToSend(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion, message: AnyRef)
  case class EventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)

}


class EventsBusActor(eventBus: EventBus) extends Actor {

  private var subscribersForEvents: Vector[ActorRef] = Vector()

  override def receive: Receive = {
    case SubscribeForEvents(subscriber) => handlerSubscribeForEvents(subscriber)
    case PublishEvents(events) => handlePublishEvents(sender(), events)
    case MessagesPersisted(messages) => handleMessagesPersisted(messages)
    case EventReceived(subscriber, aggregateId, version) => handleEventReceived(subscriber, aggregateId, version)
  }

  private def handlerSubscribeForEvents(subscriber: ActorRef): Unit = {
    subscribersForEvents :+= subscriber
    subscriber ! SubscribedForEvents()
  }

  private def handlePublishEvents(respondTo: ActorRef, events: Seq[IdentifiableEvent[Any]]): Unit = {
    println("EventsBusActor handlePublishEvents")

    import context.dispatcher
    Future {
      val eventsToSend = subscribersForEvents.flatMap(subscriber => {
        events.map(event => EventToSend(subscriber, event.aggregateId, event.version, event))
      })

      eventBus.persistMessages(eventsToSend)
      respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
      self ! MessagesPersisted(eventsToSend)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }


  }

  private def handleMessagesPersisted(messages: Seq[EventToSend]): Unit = {
    messages.foreach { message =>
      message.subscriber ! message.message
    }
  }

  private def handleEventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    eventBus.deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)
  }
}
