package io.reactivecqrs.core.eventbus

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.backpressure.BackPressureActor.{ConsumerAllowMoreStart, ConsumerAllowMoreStop, ConsumerAllowedMore}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.{ActorLogging, RandomUtil}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT])
  case class PublishEventsAck(aggregateId: AggregateId, versions: Seq[AggregateVersion])

  case class PublishReplayedEvent[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                                   aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT])

  trait SubscribeRequest
  case class SubscribeForEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest
  case class SubscribeForAggregates(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier = AcceptAllAggregateIdClassifier) extends SubscribeRequest
  case class SubscribeForAggregatesWithEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest

  case class MessagesPersisted(aggregateType: AggregateType, messages: Seq[MessagesToRoute])

  case class MessagesToRoute(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], message: AnyRef)
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion])

}

abstract class Subscription {
  val subscriptionId: String
  val aggregateType: AggregateType
}
case class EventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription
case class AggregateSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier) extends Subscription
case class AggregateWithEventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription



/** TODO acess to DB in Future */
class EventsBusActor(val inputState: EventBusState, val subscriptionsManager: EventBusSubscriptionsManagerApi) extends Actor with ActorLogging {

  private val randomUtil = new RandomUtil

  private var subscriptions: Map[AggregateType, Vector[Subscription]] = Map()
  private var subscriptionsByIds = Map[String, Subscription]()

  private val MAX_BUFFER_SIZE = 100

  private var backPressureProducerActor: Option[ActorRef] = None
  private var orderedMessages = 0

  /** Message to subscribers that not yet confirmed receiving message */
  private val messagesSent = mutable.HashMap[EventIdentifier, Vector[ActorRef]]()
  private val eventSenders = mutable.HashMap[EventIdentifier, ActorRef]()

  private val eventsPropagatedNotPersisted = mutable.HashMap[AggregateId, List[AggregateVersion]]()
  private val eventsAlreadyPropagated = mutable.HashMap[AggregateId, AggregateVersion]()

  context.system.scheduler.schedule(5.seconds, 5.seconds, new Runnable {
    override def run(): Unit = {
      inputState.flushUpdates()
    }
  })(context.dispatcher)

  def initSubscriptions(): Unit = {

    implicit val timeout = Timeout(60.seconds)

    Await.result(subscriptionsManager.getSubscriptions.mapTo[List[SubscribeRequest]], timeout.duration).foreach {
      case SubscribeForEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForEvents(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregates(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregates(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier)
    }
  }

  override def receive: Receive = {
    case anything =>
      initSubscriptions()
      context.become(receiveAfterInit)
      receiveAfterInit(anything)
  }

  def receiveAfterInit: Receive = logReceive {
    case PublishEvents(aggregateType, events, aggregateId, aggregate) =>
      handlePublishEvents(sender(), aggregateType, aggregateId, events, aggregate)
    case PublishReplayedEvent(aggregateType, events, aggregateId, aggregate) =>
      handlePublishEvents(sender(), aggregateType, aggregateId, events, aggregate)
    case m: MessageAck => handleMessageAck(m)
    case MessagesPersisted(aggregateType, messages) => ??? //handleMessagesPersisted(aggregateType, messages)
    case ConsumerAllowMoreStart =>
      backPressureProducerActor = Some(sender)
      sender ! ConsumerAllowedMore(MAX_BUFFER_SIZE - messagesSent.size)
      orderedMessages += MAX_BUFFER_SIZE - messagesSent.size
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

  private def handlePublishEvents(respondTo: ActorRef, aggregateType: AggregateType, aggregateId: AggregateId,
                                  events: Seq[EventInfo[Any]], aggregateRoot: Option[Any]): Unit = {


    val lastPublishedVersion = getLastPublishedVersion(aggregateId)

    val (alreadyPublished, eventsToPropagate) = events.span(_.version <= lastPublishedVersion)

    if(alreadyPublished.nonEmpty) {
      respondTo ! PublishEventsAck(aggregateId, alreadyPublished.map(_.version))
    }

    val messagesToSend = subscriptions.getOrElse(aggregateType, Vector.empty).flatMap {
      case s: EventSubscription =>
        val eventsForSubscriber = eventsToPropagate.filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
        if (eventsForSubscriber.nonEmpty) {
          Some(MessagesToRoute(s.subscriber, aggregateId, eventsForSubscriber.map(_.version), IdentifiableEvents(aggregateType, aggregateId, eventsForSubscriber)))
        } else {
          None
        }
      case s: AggregateSubscription =>
        if(s.classifier.accept(aggregateId)) {
          Some(MessagesToRoute(s.subscriber, aggregateId, eventsToPropagate.map(_.version), AggregateWithType(aggregateType, aggregateId, eventsToPropagate.last.version, eventsToPropagate.size, aggregateRoot)))
        } else {
          None
        }
      case s: AggregateWithEventSubscription =>
        val eventsForSubscriber = eventsToPropagate.filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
        if (eventsForSubscriber.nonEmpty) {
          Some(MessagesToRoute(s.subscriber, aggregateId, eventsForSubscriber.map(_.version),
            AggregateWithTypeAndEvents(aggregateType, aggregateId, aggregateRoot, eventsForSubscriber)))
        } else {
          None
        }
    }


    eventsToPropagate.foreach(event => {
      eventSenders += EventIdentifier(aggregateId, event.version) -> respondTo
    })

    messagesToSend.foreach(m => {
      m.subscriber ! m.message
    })

    eventsToPropagate.foreach(event => {
      val receiversForEvent = messagesToSend.filter(m => m.versions.contains(event.version)).map(_.subscriber)
      messagesSent += EventIdentifier(aggregateId, event.version) -> receiversForEvent
    })

    if(backPressureProducerActor.isDefined) {
      orderedMessages -= eventsToPropagate.size
    }

    orderMoreMessagesToConsume()
  }


  private def getLastPublishedVersion(aggregateId: AggregateId): AggregateVersion = {
    eventsAlreadyPropagated.getOrElse(aggregateId, {
      val version = inputState.lastPublishedEventForAggregate(aggregateId)
      eventsAlreadyPropagated += aggregateId -> version
      version
    })
  }



  private def handleMessageAck(ack: MessageAck): Unit = {

    val eventsIds = ack.versions.map(v => EventIdentifier(ack.aggregateId, v)).toSet

    val receiversToConfirm = messagesSent.filterKeys(e => eventsIds.contains(e))

    val withoutConfirmedPerEvent = receiversToConfirm.map {
      case (eventId, receivers) => eventId -> receivers.filterNot(_ == ack.subscriber)
    }


    val (finishedEvents, remainingEvents) = withoutConfirmedPerEvent.partition {
      case (eventId, receivers) => receivers.isEmpty
    }

    if(remainingEvents.nonEmpty) {
      messagesSent ++= remainingEvents
    }

    if(finishedEvents.nonEmpty) {

      finishedEvents.keys.map(eventSenders).toSet.foreach((sender: ActorRef) => {

        val eventsForSender = eventSenders.filter {
          case (eventId, s) => s == sender
        } map {
          case (eventId, s) => eventId
        }

        sender ! PublishEventsAck(ack.aggregateId, eventsForSender.map(_.version).toSeq.sortBy(_.asInt))
      })

      messagesSent --= finishedEvents.keys
      eventSenders --= finishedEvents.keys

      val lastPublishedVersion = getLastPublishedVersion(ack.aggregateId)

      if(eventsIds.head.version.isJustAfter(lastPublishedVersion)) {
        val notPersistedYet = eventsPropagatedNotPersisted.getOrElse(ack.aggregateId, List.empty)
        var newVersion = eventsIds.last.version
        notPersistedYet.foreach(notPersisted => if(notPersisted.isJustAfter(newVersion)) {
          newVersion = notPersisted
        })
        inputState.eventPublished(ack.aggregateId, lastPublishedVersion, newVersion)
        eventsAlreadyPropagated += ack.aggregateId -> newVersion
      } else {
        val newVersions: List[AggregateVersion] = eventsIds.map(_.version).toList
        eventsPropagatedNotPersisted.get(ack.aggregateId) match {
          case None => eventsPropagatedNotPersisted += ack.aggregateId -> newVersions
          case Some(versions) => eventsPropagatedNotPersisted += ack.aggregateId -> (newVersions ::: versions).sortBy(_.asInt)
        }
      }
    }


    withoutConfirmedPerEvent foreach {
      case (eventId, withoutConfirmed) =>
        if(withoutConfirmed.nonEmpty) {

        } else {

        }
    }





    orderMoreMessagesToConsume()

  }

  private def orderMoreMessagesToConsume(): Unit = {
    if(backPressureProducerActor.isDefined && messagesSent.size + orderedMessages < MAX_BUFFER_SIZE / 2) {
      backPressureProducerActor.get ! ConsumerAllowedMore(MAX_BUFFER_SIZE - messagesSent.size)
      orderedMessages += MAX_BUFFER_SIZE - messagesSent.size
    }
  }
  
  private def generateNextSubscriptionId:String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while(subscriptionsByIds.contains(subscriptionId))
    subscriptionId
  }
}
