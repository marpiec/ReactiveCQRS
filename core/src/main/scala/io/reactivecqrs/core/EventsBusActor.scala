package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateType, AggregateVersion, AggregateWithType}
import io.reactivecqrs.core.EventsBusActor._
import io.reactivecqrs.core.api.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.db.eventbus.EventBus

import scala.concurrent.Future

object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[IdentifiableEvent[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregate: Option[AGGREGATE_ROOT])
  case class PublishEventsAck(eventsIds: Seq[EventIdentifier])


  case class SubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef) // Todo add message classifier?
  case class SubscribedForEvents(aggregateType: AggregateType)

  case class MessagesPersisted(aggregateType: AggregateType, messages: Seq[MessageToSend])

  case class MessageToSend(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion, message: AnyRef)
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)

  
  case class SubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef) // Todo add message classifier?
  case class SubscribedForAggregates(aggregateType: AggregateType)

}


class EventsBusActor(eventBus: EventBus) extends Actor {

  private var subscribersForEvents: Map[AggregateType, Vector[ActorRef]] = Map()
  private var subscribersForAggregates: Map[AggregateType, Vector[ActorRef]] = Map()

  override def receive: Receive = {
    case SubscribeForEvents(aggregateType, subscriber) => handlerSubscribeForEvents(aggregateType, subscriber)
    case SubscribeForAggregates(aggregateType, subscriber) => handlerSubscribeForAggregates(aggregateType, subscriber)
    case PublishEvents(aggregateType, events, aggregateId, aggregateVersion, aggregate) =>
      handlePublishEvents(sender(), aggregateType, events, aggregateId, aggregateVersion, aggregate)
    case MessagesPersisted(aggregateType, messages) => handleMessagesPersisted(aggregateType, messages)
    case MessageAck(subscriber, aggregateId, version) => handleEventReceived(subscriber, aggregateId, version)
  }

  private def handlerSubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef): Unit = {
    val subscribersForAggregateType = subscribersForEvents.getOrElse(aggregateType, Vector())
    subscribersForEvents += aggregateType -> (subscribersForAggregateType :+ subscriber)
    subscriber ! SubscribedForEvents(aggregateType)
  }

  private def handlerSubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef): Unit = {
    val subscribersForAggregateType = subscribersForAggregates.getOrElse(aggregateType, Vector())
    subscribersForAggregates += aggregateType -> (subscribersForAggregateType :+ subscriber)
    subscriber ! SubscribedForAggregates(aggregateType)
  }

  private def handlePublishEvents(respondTo: ActorRef, aggregateType: AggregateType, events: Seq[IdentifiableEvent[Any]],
                                  aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateRoot: Option[Any]): Unit = {
    import context.dispatcher
    Future {
      val eventsToSend = subscribersForEvents.getOrElse(aggregateType, Vector.empty).flatMap(subscriber => {
        events.map(event => MessageToSend(subscriber, event.aggregateId, event.version, event))
      })

      val aggregatesToSend = subscribersForAggregates.getOrElse(aggregateType, Vector.empty).map(subscriber => {
        MessageToSend(subscriber, aggregateId, aggregateVersion, AggregateWithType(aggregateType, aggregateId, aggregateVersion, aggregateRoot))
      })

      val messagesToSend = eventsToSend ++ aggregatesToSend

      eventBus.persistMessages(messagesToSend)
      respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
      self ! MessagesPersisted(aggregateType, messagesToSend)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }

  }



  private def handleMessagesPersisted(aggregateType: AggregateType, messages: Seq[MessageToSend]): Unit = {
    messages.foreach { message =>
      message.subscriber ! message.message
    }
  }

  private def handleEventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    eventBus.deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)
  }
}
