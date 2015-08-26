package io.reactivecqrs.core.eventbus

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.RandomUtil


object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[IdentifiableEvent[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregate: Option[AGGREGATE_ROOT])
  case class PublishEventsAck(eventsIds: Seq[EventIdentifier])


  case class SubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier)
  case class SubscribedForEvents(aggregateType: AggregateType, subscriptionId: String)

  case class SubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier = AcceptAllAggregateIdClassifier)
  case class SubscribedForAggregates(aggregateType: AggregateType, subscriptionId: String)

  case class SubscribeForAggregatesWithEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier)
  case class SubscribedForAggregatesWithEvents(aggregateType: AggregateType, subscriptionId: String)

  case class MessagesPersisted(aggregateType: AggregateType, messages: Seq[MessageToSend])

  case class MessageToSend(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion, message: AnyRef)
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)

  
  case class CancelSubscription(subscriptionId: String)
  case class SubscriptionCanceled(subscriptionId: String)
}

abstract class Subscription {
  val subscriptionId: String
  val aggregateType: AggregateType
}
case class EventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription
case class AggregateSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier) extends Subscription
case class AggregateWithEventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription

class EventsBusActor(eventBus: EventBusState) extends Actor {

  private val randomUtil = new RandomUtil
  
  private var subscriptions: Map[AggregateType, Vector[Subscription]] = Map()
  private var subscriptionsByIds = Map[String, Subscription]()
  

  override def receive: Receive = LoggingReceive {
    case SubscribeForEvents(aggregateType, subscriber, classifier) => handleSubscribeForEvents(aggregateType, subscriber, classifier)
    case SubscribeForAggregates(aggregateType, subscriber, classifier) => handleSubscribeForAggregates(aggregateType, subscriber, classifier)
    case SubscribeForAggregatesWithEvents(aggregateType, subscriber, classifier) => handleSubscribeForAggregatesWithEvents(aggregateType, subscriber, classifier)
    case CancelSubscription(subscriptionId) => handleCancelSubscription(sender(), subscriptionId)
    case PublishEvents(aggregateType, events, aggregateId, aggregateVersion, aggregate) =>
      handlePublishEvents(sender(), aggregateType, events, aggregateId, aggregateVersion, aggregate)
    case MessagesPersisted(aggregateType, messages) => handleMessagesPersisted(aggregateType, messages)
    case MessageAck(subscriber, aggregateId, version) => handleEventReceived(subscriber, aggregateId, version)
  }
  
  
  // ****************** SUBSCRIPTION

  private def handleSubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = EventSubscription(subscriptionId, aggregateType, subscriber, classifier)
    subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
    subscriptionsByIds += subscriptionId -> subscription
    subscriber ! SubscribedForEvents(aggregateType, subscriptionId)
  }

  private def handleSubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = AggregateSubscription(subscriptionId, aggregateType, subscriber, classifier)
    subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
    subscriptionsByIds += subscriptionId -> subscription
    subscriber ! SubscribedForAggregates(aggregateType, subscriptionId)
  }

  private def handleSubscribeForAggregatesWithEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = AggregateWithEventSubscription(subscriptionId, aggregateType, subscriber, classifier)
    subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
    subscriptionsByIds += subscriptionId -> subscription
    subscriber ! SubscribedForAggregatesWithEvents(aggregateType, subscriptionId)
  }

  def handleCancelSubscription(subscriber: ActorRef, subscriptionId: String): Unit = {
    subscriptionsByIds.get(subscriptionId) match {
      case Some(subscription) =>
        val subscriptionsForType = subscriptions.getOrElse(subscription.aggregateType, Vector()).filter(_.subscriptionId != subscriptionId)
        subscriptions += subscription.aggregateType -> subscriptionsForType
        subscriptionsByIds -= subscriptionId
        subscriber ! SubscriptionCanceled(subscriptionId)
      case None => subscriber ! SubscriptionCanceled(subscriptionId)
    }
  }

  
  // ***************** PUBLISHING
  
  private def handlePublishEvents(respondTo: ActorRef, aggregateType: AggregateType, events: Seq[IdentifiableEvent[Any]],
                                  aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateRoot: Option[Any]): Unit = {
    val messagesToSend = subscriptions.getOrElse(aggregateType, Vector.empty).flatMap {
        case s: EventSubscription =>
          events
            .filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
            .map(event => MessageToSend(s.subscriber, event.aggregateId, event.version, event))
        case s: AggregateSubscription =>
          if(s.classifier.accept(aggregateId)) {
            List(MessageToSend(s.subscriber, aggregateId, aggregateVersion, AggregateWithType(aggregateType, aggregateId, aggregateVersion, aggregateRoot)))
          } else {
            List.empty
          }
        case s: AggregateWithEventSubscription =>
          events
            .filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
            .map(event => MessageToSend(s.subscriber, event.aggregateId, event.version, AggregateWithTypeAndEvent(aggregateType, aggregateId, aggregateVersion, aggregateRoot, event.event)))
      }



//    Future { // FIXME Future is to ensure non blocking access to db, but it broke order in which events for the same aggreagte were persisted, maybe this should ba actor per aggregate instead of future?
      eventBus.persistMessages(messagesToSend)
      respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
      self ! MessagesPersisted(aggregateType, messagesToSend)
//    } onFailure {
//      case e: Exception => throw new IllegalStateException(e)
//    }

  }



  private def handleMessagesPersisted(aggregateType: AggregateType, messages: Seq[MessageToSend]): Unit = {
    messages.foreach { message =>
      message.subscriber ! message.message
    }
  }

  private def handleEventReceived(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    eventBus.deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion)
  }
  
  private def generateNextSubscriptionId:String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while(subscriptionsByIds.contains(subscriptionId))
    subscriptionId
  }
}
