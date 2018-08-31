package io.reactivecqrs.core.eventbus

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.backpressure.BackPressureActor._
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEventsAck, _}
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



/** TODO get rid of state, memory only */
class EventsBusActor(val inputState: EventBusState, val subscriptionsManager: EventBusSubscriptionsManagerApi, val MAX_BUFFER_SIZE: Int = 1000) extends Actor with ActorLogging {

  private val randomUtil = new RandomUtil

  private var subscriptions: Map[AggregateType, Vector[Subscription]] = Map()
  private var subscriptionsByIds = Map[String, Subscription]()

  private var backPressureProducerActor: Option[ActorRef] = None
  private var orderedMessages = 0
  private var receivedInProgressMessages = 0

  /** Message to subscribers that not yet confirmed receiving message */
  private val messagesSent = mutable.HashMap[EventIdentifier, Vector[(Instant, ActorRef)]]()
  private val eventSenders = mutable.HashMap[EventIdentifier, ActorRef]()

  private val eventsPropagatedNotPersisted = mutable.HashMap[AggregateId, List[AggregateVersion]]()
  private val eventsAlreadyPropagated = mutable.HashMap[AggregateId, AggregateVersion]()

  private var afterFinishRespondTo: Option[ActorRef] = None

  context.system.scheduler.schedule(5.seconds, 5.seconds, new Runnable {
    override def run(): Unit = {
      inputState.flushUpdates()
    }
  })(context.dispatcher)

  private var lastLogged = 0

  // FOR DEBUG PURPOSE
  context.system.scheduler.schedule(10.second, 10.seconds, new Runnable {
    override def run(): Unit = {

      val now = Instant.now()
      val oldMessages = messagesSent.flatMap(m => m._2.map(r => (m._1, r._1, r._2))).filter(e => e._2.plusMillis(120000).isBefore(now))

      if(oldMessages.size != lastLogged) {
        log.warning("Messages propagated, not confirmed: " + oldMessages.size)
//        oldMessages.foreach(m => {
//          log.warning("Message not confirmed: " + m._3.path.toStringWithoutAddress+" "+m._1)
//        })
      }
      lastLogged = oldMessages.size
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
    case ConsumerStart =>
      backPressureProducerActor = Some(sender)
      if(receivedInProgressMessages + orderedMessages < MAX_BUFFER_SIZE) {
        sender ! ConsumerAllowedMore(MAX_BUFFER_SIZE - receivedInProgressMessages - orderedMessages)
        orderedMessages = MAX_BUFFER_SIZE
      }
    case ConsumerStop =>
      println("EventBus Stop, processing "+receivedInProgressMessages)
      afterFinishRespondTo = Some(sender())
      backPressureProducerActor = None
      if(receivedInProgressMessages == 0) {
        afterFinishRespondTo.get ! ConsumerFinished
      }
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

    if (alreadyPublished.nonEmpty) {
      respondTo ! PublishEventsAck(aggregateId, alreadyPublished.map(_.version))
    }

    val messagesToSend = if (eventsToPropagate.nonEmpty) {
      subscriptions.getOrElse(aggregateType, Vector.empty).flatMap {
        case s: EventSubscription =>
          val eventsForSubscriber = eventsToPropagate.filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
          if (eventsForSubscriber.nonEmpty) {
            Some(MessagesToRoute(s.subscriber, aggregateId, eventsForSubscriber.map(_.version), IdentifiableEvents(aggregateType, aggregateId, eventsForSubscriber)))
          } else {
            None
          }
        case s: AggregateSubscription =>
          if (s.classifier.accept(aggregateId)) {
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
    } else {
      Vector.empty
    }



    eventsToPropagate.foreach(event => {
      eventSenders += EventIdentifier(aggregateId, event.version) -> respondTo
    })

    messagesToSend.foreach(m => {
      m.subscriber ! m.message
    })

    eventsToPropagate.foreach(event => {
      val timestasmp = Instant.now()
      val receiversForEvent = messagesToSend.filter(m => m.versions.contains(event.version)).map(e => (timestasmp, e.subscriber))
      messagesSent += EventIdentifier(aggregateId, event.version) -> receiversForEvent
    })

    if(backPressureProducerActor.isDefined) {
      orderedMessages -= events.size // it is possible to receive more messages than ordered
      receivedInProgressMessages += events.size
    }

    orderMoreMessagesToConsume()

    if(messagesToSend.isEmpty) {
      // in case there are no listeners for given events we need to trigger confirmation
      handleMessageAck(MessageAck(self, aggregateId, events.map(_.version)))
    }
  }


  private def getLastPublishedVersion(aggregateId: AggregateId): AggregateVersion = {
    eventsAlreadyPropagated.getOrElse(aggregateId, {
      val version = inputState.lastPublishedEventForAggregate(aggregateId)
      eventsAlreadyPropagated += aggregateId -> version
      version
    })
  }



  private def handleMessageAck(ack: MessageAck): Unit = {

    val aggregateId = ack.aggregateId

    val eventsIds = ack.versions.map(v => EventIdentifier(aggregateId, v)).toSet

    val receiversToConfirm = messagesSent.filterKeys(e => eventsIds.contains(e))

    val withoutConfirmedPerEvent = receiversToConfirm.map {
      case (eventId, receivers) => eventId -> receivers.filterNot(_._2 == ack.subscriber)
    }


    val (finishedEvents, remainingEvents) = withoutConfirmedPerEvent.partition {
      case (eventId, receivers) => receivers.isEmpty
    }

    if(remainingEvents.nonEmpty) {
      messagesSent ++= remainingEvents
    }

    if(finishedEvents.nonEmpty) {

      val eventsToConfirm: Iterable[EventIdentifier] = finishedEvents.keys

      receivedInProgressMessages -= finishedEvents.size

      // TODO optimize by grouping versions ber sender
      finishedEvents.keys.foreach(e =>
        eventSenders(e) ! PublishEventsAck(aggregateId, Seq(e.version))
      )

      messagesSent --= eventsToConfirm
      eventSenders --= eventsToConfirm

      val lastPublishedVersion = getLastPublishedVersion(aggregateId)

      val eventsSorted = eventsIds.toList.sortBy(_.version.asInt)

      if(eventsSorted.head.version.isJustAfter(lastPublishedVersion)) {
        val notPersistedYet = eventsPropagatedNotPersisted.getOrElse(aggregateId, List.empty)
        var newVersion = eventsSorted.last.version
        notPersistedYet.foreach(notPersisted => if(notPersisted.isJustAfter(newVersion)) {
          newVersion = notPersisted
        })
        inputState.eventPublished(aggregateId, lastPublishedVersion, newVersion)
        eventsAlreadyPropagated += aggregateId -> newVersion
      } else {
        val newVersions: List[AggregateVersion] = eventsSorted.map(_.version)
        eventsPropagatedNotPersisted.get(aggregateId) match {
          case None => eventsPropagatedNotPersisted += aggregateId -> newVersions
          case Some(versions) => eventsPropagatedNotPersisted += aggregateId -> (newVersions ::: versions).sortBy(_.asInt)
        }
      }
    }

    if(receivedInProgressMessages == 0 && afterFinishRespondTo.isDefined) {
      afterFinishRespondTo.get ! ConsumerFinished
    } else {
      orderMoreMessagesToConsume()
    }

  }

  private def orderMoreMessagesToConsume(): Unit = {
    if(backPressureProducerActor.isDefined && receivedInProgressMessages + orderedMessages < MAX_BUFFER_SIZE) {
      backPressureProducerActor.get ! ConsumerAllowedMore(MAX_BUFFER_SIZE - receivedInProgressMessages - orderedMessages)
      orderedMessages = MAX_BUFFER_SIZE
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
