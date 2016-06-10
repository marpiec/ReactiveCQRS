package io.reactivecqrs.core.eventbus

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEvent}
import io.reactivecqrs.core.backpressure.BackPressureActor.{ConsumerAllowMoreStart, ConsumerAllowMoreStop, ConsumerAllowedMore}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.{ActorLogging, RandomUtil}

import scala.concurrent.Await
import scala.concurrent.duration._


object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[IdentifiableEvent[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregate: Option[AGGREGATE_ROOT])
  case class PublishEventAck(eventId: Long)

  case class PublishReplayedEvent[AGGREGATE_ROOT](aggregateType: AggregateType, event: IdentifiableEvent[AGGREGATE_ROOT],
                                                   aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregate: Option[AGGREGATE_ROOT])

  trait SubscribeRequest
  case class SubscribeForEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest
  case class SubscribeForAggregates(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier = AcceptAllAggregateIdClassifier) extends SubscribeRequest
  case class SubscribeForAggregatesWithEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest

  case class MessagesPersisted(aggregateType: AggregateType, messages: Seq[EventToRoute])

  case class EventToRoute(subscriber: Either[ActorRef, String], aggregateId: AggregateId, version: AggregateVersion, eventId: Long, message: AnyRef)
  case class EventAck(eventId: Long, subscriber: ActorRef)


  case object CheckForOldEvents

}

abstract class Subscription {
  val subscriptionId: String
  val aggregateType: AggregateType
}
case class EventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription
case class AggregateSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier) extends Subscription
case class AggregateWithEventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription




class EventsBusActor(val subscriptionsManager: EventBusSubscriptionsManagerApi) extends Actor with ActorLogging {

  private val randomUtil = new RandomUtil

  private var subscriptions: Map[AggregateType, Vector[Subscription]] = Map()
  private var subscriptionsByIds = Map[String, Subscription]()

  private val MAX_BUFFER_SIZE = 1000

  private var backPressureProducerActor: Option[ActorRef] = None
  private var orderedMessages = 0

  private var lastSendMessage: Option[Long] = None
  private var messagesToSend: List[(Long, Vector[EventToRoute])] = List.empty
  private var messagesSent: List[(Long, Vector[EventToRoute])] = List.empty
  private var eventSenders: Map[Long, ActorRef] = Map.empty

  private var eventsTimestamps: Map[Long, Instant] = Map.empty

  import context.dispatcher

  context.system.scheduler.schedule(60.seconds, 60.seconds) {
    self ! CheckForOldEvents
  }

  def initSubscriptions(): Unit = {

    implicit val timeout = Timeout(60.seconds)

    Await.result(subscriptionsManager.getSubscriptions.mapTo[List[SubscribeRequest]], timeout.duration).foreach {
      case SubscribeForEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForEvents(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregates(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregates(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier)
    }
  }

  override def receive: Receive = {
    case CheckForOldEvents =>
      eventsTimestamps.filter(_._2.plusSeconds(60).isBefore(Instant.now)).foreach(e =>
        log.error("Message not broadcasted successfully " + messagesSent.find(m => m._1 == e._1)))
    case anything =>
      initSubscriptions()
      context.become(receiveAfterInit)
      receiveAfterInit(anything)
  }

  def receiveAfterInit: Receive = logReceive {
    case PublishEvents(aggregateType, events, aggregateId, aggregateVersion, aggregate) =>
      handlePublishEvents(sender(), aggregateType, events, aggregateId, aggregateVersion, aggregate)
    case PublishReplayedEvent(aggregateType, event, aggregateId, aggregateVersion, aggregate) =>
      handlePublishEvents(sender(), aggregateType, List(event), aggregateId, aggregateVersion, aggregate)
    case MessagesPersisted(aggregateType, messages) => handleMessagesPersisted(aggregateType, messages)
    case m: EventAck => handleEventReceived(m)
    case ConsumerAllowMoreStart =>
      backPressureProducerActor = Some(sender)
      sender ! ConsumerAllowedMore((MAX_BUFFER_SIZE - messagesToSend.size) / subscriptions.values.map(_.size).sum)
      orderedMessages += MAX_BUFFER_SIZE - messagesToSend.size
    case ConsumerAllowMoreStop => backPressureProducerActor = None
  }


  // ****************** SUBSCRIPTION

  private def handleSubscribeForEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = EventSubscription(subscriptionId, aggregateType, subscriber, classifier)
    if(!subscribersForAggregateType.exists(s => s.isInstanceOf[EventSubscription]
                                             && s.asInstanceOf[EventSubscription].aggregateType == aggregateType
                                             && s.asInstanceOf[EventSubscription].subscriber.path.toString == subscriber.path.toString
                                             && s.asInstanceOf[EventSubscription].classifier == classifier)) {
      subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
      subscriptionsByIds += subscriptionId -> subscription
    }
  }

  private def handleSubscribeForAggregates(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = AggregateSubscription(subscriptionId, aggregateType, subscriber, classifier)
    if(!subscribersForAggregateType.exists(s => s.isInstanceOf[AggregateSubscription]
      && s.asInstanceOf[AggregateSubscription].aggregateType == aggregateType
      && s.asInstanceOf[AggregateSubscription].subscriber.path.toString == subscriber.path.toString
      && s.asInstanceOf[AggregateSubscription].classifier == classifier)) {
      subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
      subscriptionsByIds += subscriptionId -> subscription
    }
  }

  private def handleSubscribeForAggregatesWithEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier): Unit = {
    val subscriptionId = generateNextSubscriptionId
    val subscribersForAggregateType = subscriptions.getOrElse(aggregateType, Vector())
    val subscription = AggregateWithEventSubscription(subscriptionId, aggregateType, subscriber, classifier)
    if(!subscribersForAggregateType.exists(s => s.isInstanceOf[AggregateWithEventSubscription]
      && s.asInstanceOf[AggregateWithEventSubscription].aggregateType == aggregateType
      && s.asInstanceOf[AggregateWithEventSubscription].subscriber.path.toString == subscriber.path.toString
      && s.asInstanceOf[AggregateWithEventSubscription].classifier == classifier)) {
      subscriptions += aggregateType -> (subscribersForAggregateType :+ subscription)
      subscriptionsByIds += subscriptionId -> subscription
    }
  }


  // ***************** PUBLISHING

  private def handlePublishEvents(respondTo: ActorRef, aggregateType: AggregateType, events: Seq[IdentifiableEvent[Any]],
                                  aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateRoot: Option[Any]): Unit = {

    val messagesToSendForEvents = events.map(event => {
      val messagesToSend = subscriptions.getOrElse(aggregateType, Vector.empty).flatMap {
        case s: EventSubscription =>
          Some(event)
            .filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
            .map(event => EventToRoute(Left(s.subscriber), event.aggregateId, event.version, event.eventId, event))
        case s: AggregateSubscription =>
          if(s.classifier.accept(aggregateId)) {
            List(EventToRoute(Left(s.subscriber), aggregateId, aggregateVersion, event.eventId, AggregateWithType(aggregateType, aggregateId, aggregateVersion, aggregateRoot, events.last.eventId)))
          } else {
            List.empty
          }
        case s: AggregateWithEventSubscription =>
          Some(event)
            .filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
            .map(event => EventToRoute(Left(s.subscriber), event.aggregateId, event.version, event.eventId, AggregateWithTypeAndEvent(aggregateType, aggregateId, aggregateVersion, aggregateRoot, event.event, event.eventId, event.userId, event.timestamp)))
      }
      (event.eventId, messagesToSend)
    })


    messagesToSend = (messagesToSendForEvents.toList ::: messagesToSend).sortBy(_._1)
    events.foreach(event => eventSenders += event.eventId -> respondTo)


    val eventsPropagated = sendContinuousEventMessages()


//    Future { // FIXME Future is to ensure non blocking access to db, but it broke order in which events for the same aggregate were persisted, maybe there should be an actor per aggregate instead of a future?
//      state.persistMessages(messagesToSend)
//      respondTo ! PublishEventsAck(events.map(event => EventIdentifier(event.aggregateId, event.version)))
//      self ! MessagesPersisted(aggregateType, messagesToSend)

    if(backPressureProducerActor.isDefined) {
      orderedMessages -= eventsPropagated
    }

    orderMoreMessagesToConsume()

//    } onFailure {
//      case e: Exception => throw new IllegalStateException(e)
//    }

  }


  private def sendContinuousEventMessages(): Int = {
    var eventsPropagated = 0
    messagesToSend.foreach { entry =>
      val (eventId, messages) = entry
      if(lastSendMessage.isEmpty || eventId == lastSendMessage.get + 1) {
        messages.foreach { message =>
          message.subscriber match {
            case Left(s) => s ! message.message
            case Right(path) => context.system.actorSelection(path) ! message.message
          }
        }
        lastSendMessage = Some(eventId)
        messagesSent ::= entry
        eventsTimestamps += eventId -> Instant.now()
        messagesToSend = messagesToSend.tail
        eventsPropagated += 1
      }
    }
    eventsPropagated
  }


  private def orderMoreMessagesToConsume(): Unit = {
    if(backPressureProducerActor.isDefined && messagesToSend.size + orderedMessages < MAX_BUFFER_SIZE / 2) {
      backPressureProducerActor.get ! ConsumerAllowedMore((MAX_BUFFER_SIZE - messagesToSend.size) / subscriptions.values.map(_.size).sum)
      orderedMessages += MAX_BUFFER_SIZE - messagesToSend.size
    }
  }


  private def handleMessagesPersisted(aggregateType: AggregateType, messages: Seq[EventToRoute]): Unit = {

    messages.foreach { message =>
      message.subscriber match {
        case Left(s) => s ! message.message
        case Right(path) => context.system.actorSelection(path) ! message.message
      }
    }
  }

  private def handleEventReceived(ack: EventAck): Unit = {



    val (eventId, messagesForEvent): (Long, Vector[EventToRoute]) = messagesSent.find(_._1 == ack.eventId).get

    val remainingMessagesForEvent = messagesForEvent.filterNot(m => m.subscriber.fold(l => l.path.toString, r => r) == ack.subscriber.path.toString)

    if(remainingMessagesForEvent.isEmpty) {
      eventSenders(eventId) ! PublishEventAck(eventId)
      eventSenders -= eventId
      messagesSent = messagesSent.filterNot(_._1 == eventId)
      eventsTimestamps -= eventId
    } else {
      messagesSent = messagesSent.map(e => {
        if(e._1 == eventId) {
          (eventId, remainingMessagesForEvent)
        } else {
          e
        }
      })
    }

    orderMoreMessagesToConsume()

  }
  
  private def generateNextSubscriptionId:String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while(subscriptionsByIds.contains(subscriptionId))
    subscriptionId
  }
}
