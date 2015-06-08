package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.EventsBusActor._
import io.reactivecqrs.core.api.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.db.eventbus.EventBus

import scala.concurrent.Future

object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: String, events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])
  case class PublishEventsAck(eventsIds: Seq[EventIdentifier])


  case class SubscribeForEvents(aggregateType: String, subscriber: ActorRef) // Todo add message classifier?
  case class SubscribedForEvents(aggregateType: String)

  case class MessagesPersisted(aggregateType: String, messages: Seq[EventToSend])

  case class EventToSend(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion, message: AnyRef)
  case class EventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)

}


class EventsBusActor(eventBus: EventBus) extends Actor {

  private var subscribersForEvents: Map[String, Vector[ActorRef]] = Map()

  override def receive: Receive = {
    case SubscribeForEvents(aggregateType, subscriber) => handlerSubscribeForEvents(aggregateType, subscriber)
    case PublishEvents(aggregateType, events) => handlePublishEvents(sender(), aggregateType, events)
    case MessagesPersisted(aggregateType, messages) => handleMessagesPersisted(aggregateType, messages)
    case EventReceived(subscriber, aggregateId, version) => handleEventReceived(subscriber, aggregateId, version)
  }

  private def handlerSubscribeForEvents(aggregateType: String, subscriber: ActorRef): Unit = {
    val subscribersForAggregateType = subscribersForEvents.getOrElse(aggregateType, Vector())
    subscribersForEvents += aggregateType -> (subscribersForAggregateType :+ subscriber)
    subscriber ! SubscribedForEvents(aggregateType)
  }

  private def handlePublishEvents(respondTo: ActorRef, aggregateType: String, events: Seq[IdentifiableEvent[Any]]): Unit = {
    println("EventsBusActor handlePublishEvents")

    import context.dispatcher
    Future {
      val eventsToSend = subscribersForEvents.getOrElse(aggregateType, Vector.empty).flatMap(subscriber => {
        events.map(event => EventToSend(subscriber, event.aggregateId, event.version, event))
      })

      eventBus.persistMessages(eventsToSend)
      respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
      self ! MessagesPersisted(aggregateType, eventsToSend)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }


  }

  private def handleMessagesPersisted(aggregateType: String, messages: Seq[EventToSend]): Unit = {
    messages.foreach { message =>
      message.subscriber ! message.message
    }
  }

  private def handleEventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    eventBus.deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)
  }
}
