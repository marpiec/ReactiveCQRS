package io.reactivecqrs.core.eventbus

import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef}
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.backpressure.BackPressureActor._
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEventsAck, _}
import io.reactivecqrs.core.util.{MyActorLogging, RandomUtil}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


object EventsBusActor {

  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT])
  case class PublishEventsAck(aggregateId: AggregateId, versions: Seq[AggregateVersion])

  case class PublishReplayedEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                                   aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT])

  trait SubscribeRequest {

    def summary: String

  }

  case class SubscribeForEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest {
    override def summary = "for events of " + aggregateType.simpleName+" -> "+ subscriber.path.name
  }
  case class SubscribeForAggregates(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier = AcceptAllAggregateIdClassifier) extends SubscribeRequest {
    override def summary = "for aggregates of " + aggregateType.simpleName+" -> "+ subscriber.path.name
  }
  case class SubscribeForAggregatesWithEvents(messageId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest {
    override def summary = "for aggregates and events of " + aggregateType.simpleName+" -> "+ subscriber.path.name
  }

  case class MessagesToRoute(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], message: AnyRef)
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion])

  object LogDetailedStatus

  object InitEventBus
}

abstract class Subscription {
  val subscriptionId: String
  val aggregateType: AggregateType
}
case class EventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription
case class AggregateSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier) extends Subscription
case class AggregateWithEventSubscription(subscriptionId: String, aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier) extends Subscription



/** TODO get rid of state, memory only */
class EventsBusActor(val inputState: EventBusState, val subscriptionsManager: EventBusSubscriptionsManagerApi, val MAX_BUFFER_SIZE: Int = 1000,
                     eventsLogger: Option[Logger] = None) extends Actor with MyActorLogging {

  private var subscriptions: Map[AggregateType, Vector[Subscription]] = Map()
  private var subscriptionsByIds = Map[String, Subscription]()

  private var eventsAlreadyPublishedTotal = 0
  private var eventsPublishedTotal = 0
  private var messagesSentTotal = 0
  private var messageAckTotal = 0

  private var backPressureProducerActor: Option[ActorRef] = None
  private var receivedTotal = 0
  private var orderedTotal = 0
  private var orderedMessages = 0
  private var orderedMessageTimestamp = 0L
  private var orderedMessagePostponedTimestamp = 0L
  private var receivedInProgressMessages = 0

  private var lastMessageAck = 0L
  private var lastMessageSent = 0L

  /** Message to subscribers that not yet confirmed receiving message */
  private val messagesSent = mutable.HashMap[EventIdentifier, Vector[(Instant, ActorRef)]]()
  private val eventSenders = mutable.HashMap[EventIdentifier, ActorRef]()

  private val eventsPropagatedNotPersisted = mutable.HashMap[AggregateId, List[AggregateVersion]]()
  private val eventsAlreadyPropagated = mutable.HashMap[Long, Int]() // AggregateId -> version

  private var afterFinishRespondTo: Option[ActorRef] = None

  private var lastLogged = 0

  override def preStart(): Unit = {

    context.system.scheduler.schedule(5.seconds, 5.seconds, new Runnable {
      override def run(): Unit = {
        inputState.flushUpdates()
      }
    })(context.dispatcher)

    // FOR DEBUG PURPOSE
    context.system.scheduler.schedule(120.second, 120.seconds, new Runnable {
      override def run(): Unit = {


        // Old messages
        val now = Instant.now()
        val oldMessages = messagesSent.flatMap(m => m._2.map(r => (m._1, r._1, r._2))).count(e => e._2.plusMillis(120000).isBefore(now))
        if(oldMessages != lastLogged) {
          if(oldMessages > 0) {
            log.warning("Messages propagated, not confirmed: " + oldMessages)
          } else {
            log.warning("All messages propagated")
          }
        }
        lastLogged = oldMessages


        // Producer starving

        if(orderedTotal > 0) {
          val nowTimestamp = System.nanoTime() / 1_000_000


          if (nowTimestamp - orderedMessageTimestamp > 240000) { // did not ordered in 4 minutes
            log.warning("Did not ordered messages in " + ((nowTimestamp - orderedMessageTimestamp) / 1000) + " seconds. " +
              "orderedMessages = " + orderedMessages + ", messagesSent=" + messagesSent.size + ", eventsPropagatedNotPersisted=" + eventsPropagatedNotPersisted.size +
              ", lastMessageAck=" + ((nowTimestamp - lastMessageAck) / 1000)+", lastMessageSent="+(nowTimestamp - lastMessageSent)+", orderedTotal=" + orderedTotal + ", receivedTotal=" + receivedTotal)

            if (backPressureProducerActor.isDefined) {
              if ((nowTimestamp - lastMessageAck) < 240000) {
                if (nowTimestamp - orderedMessagePostponedTimestamp > 240000) {
                  backPressureProducerActor.get ! ConsumerPostponed
                  orderedMessagePostponedTimestamp = nowTimestamp
                  log.info("Postponed messages order")
                }
              } else {

                val aggregates = messagesSent.groupBy(_._1.aggregateId).map(e => e._1 -> e._2.size)
                log.warning("No messages acked in " + ((nowTimestamp - lastMessageAck) / 1000) + " seconds. Waiting for " + aggregates.map(e => e._1.asLong + "->" + e._2).mkString(", "))
              }
            } else {
              log.warning("Events producer is not defined")
            }
          }

          backPressureProducerActor.foreach(a => {
            orderMoreMessagesToConsume()
          })

        }
      }
    })(context.dispatcher)
  }

  private def logDetailedStatus(): Unit = {

    log.warning("EventBus status: " +
      "backPressureProducerActor="+backPressureProducerActor.isDefined+", " +
      "receivedTotal="+receivedTotal+", " +
      "orderedTotal="+orderedTotal+", " +
      "orderedMessages="+orderedMessages+", " +
      "orderedMessageTimestamp="+(System.nanoTime() / 1_000_000 - orderedMessageTimestamp)+", " +
      "orderedMessagePostponedTimestamp="+(System.nanoTime() / 1_000_000 - orderedMessagePostponedTimestamp)+", " +
      "receivedInProgressMessages="+receivedInProgressMessages+", " +
      "lastMessageAck="+lastMessageAck+", " +
      "messagesSent="+messagesSent)
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  private def logMessage(message: String): Unit = {
    println(message)
    log.info(message)
  }

  def initSubscriptions(): Unit = {

    implicit val timeout = Timeout(180.seconds)

    Await.result(subscriptionsManager.getSubscriptions.mapTo[List[SubscribeRequest]], timeout.duration).foreach {
      case SubscribeForEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForEvents(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregates(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregates(messageId, aggregateType, subscriber, classifier)
      case SubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier) => handleSubscribeForAggregatesWithEvents(messageId, aggregateType, subscriber, classifier)
    }
  }

  context.system.scheduler.scheduleOnce(10 seconds, self, InitEventBus)(context.dispatcher)

  override def receive: Receive = {
    case anything =>
      initSubscriptions()
      context.become(receiveAfterInit)
      receiveAfterInit(anything)
  }

  def receiveAfterInit: Receive = logReceive {
    case InitEventBus => () // just to perform init
    case PublishEvents(aggregateType, events, aggregateId, aggregate) =>
      handlePublishEvents(sender(), aggregateType, aggregateId, events, aggregate, replayed = false)
    case PublishReplayedEvents(aggregateType, events, aggregateId, aggregate) =>
      handlePublishEvents(sender(), aggregateType, aggregateId, events, aggregate, replayed = true)
    case m: MessageAck =>
      handleMessageAck(m)
    case ConsumerStart =>
      backPressureProducerActor = Some(sender())
      if(receivedInProgressMessages + orderedMessages < MAX_BUFFER_SIZE) {
        sender() ! ConsumerAllowedMore(MAX_BUFFER_SIZE - receivedInProgressMessages - orderedMessages)
        orderedTotal += MAX_BUFFER_SIZE - receivedInProgressMessages - orderedMessages
        orderedMessages = MAX_BUFFER_SIZE
        orderedMessageTimestamp = System.nanoTime() / 1_000_000
        orderedMessagePostponedTimestamp = orderedMessageTimestamp
      }
    case ConsumerStop =>
      logMessage("EventBus Stop, processing "+receivedInProgressMessages)
      afterFinishRespondTo = Some(sender())
      backPressureProducerActor = None
      if(receivedInProgressMessages == 0) {
        afterFinishRespondTo.get ! ConsumerFinished
      }
    case LogDetailedStatus => logDetailedStatus()
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
                                  events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], replayed: Boolean): Unit = {



    receivedTotal += events.size


    // TODO filter out events received again if sender resent them

    val lastPublishedVersion = AggregateVersion(getLastPublishedVersion(aggregateId))

    val (alreadyPublished, eventsToPropagate) = events.span(_.version <= lastPublishedVersion)

    if (alreadyPublished.nonEmpty) {
      respondTo ! PublishEventsAck(aggregateId, alreadyPublished.map(_.version))
    }
    eventsAlreadyPublishedTotal += alreadyPublished.size
    eventsPublishedTotal += eventsToPropagate.size


    val messagesToSend = if (eventsToPropagate.nonEmpty) {
      subscriptions.getOrElse(aggregateType, Vector.empty).flatMap {
        case s: EventSubscription =>
          val eventsForSubscriber = eventsToPropagate.filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
          if (eventsForSubscriber.nonEmpty) {
            Some(MessagesToRoute(s.subscriber, aggregateId, eventsForSubscriber.map(_.version), IdentifiableEvents(aggregateType, aggregateId, eventsForSubscriber, replayed)))
          } else {
            None
          }
        case s: AggregateSubscription =>
          if (s.classifier.accept(aggregateId)) {
            Some(MessagesToRoute(s.subscriber, aggregateId, eventsToPropagate.map(_.version), AggregateWithType(aggregateType, aggregateId, eventsToPropagate.last.version, eventsToPropagate.map(_.version), aggregateRoot, replayed)))
          } else {
            None
          }
        case s: AggregateWithEventSubscription =>
          val eventsForSubscriber = eventsToPropagate.filter(event => s.classifier.accept(aggregateId, EventType(event.event.getClass.getName)))
          if (eventsForSubscriber.nonEmpty) {
            Some(MessagesToRoute(s.subscriber, aggregateId, eventsForSubscriber.map(_.version),
              AggregateWithTypeAndEvents(aggregateType, aggregateId, aggregateRoot, eventsForSubscriber, replayed)))
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

    eventsLogger.foreach(_.debug("EventBus propagating events: " + messagesToSend.map(m => m.aggregateId.asLong+": "+ m.versions.map(_.asInt).mkString(", ") + " to " + m.subscriber.path).mkString("|")))

    messagesToSend.foreach(m => {
       m.subscriber ! m.message
      messagesSentTotal += m.versions.size
    })

    if(messagesToSend.nonEmpty) {
      lastMessageSent = System.nanoTime() / 1_000_000
    }

    val nowTimestamp = Instant.now()
    eventsToPropagate.foreach(event => {
      val receiversForEvent = messagesToSend.filter(m => m.versions.contains(event.version)).map(e => (nowTimestamp, e.subscriber))
      messagesSent += EventIdentifier(aggregateId, event.version) -> receiversForEvent
    })



    orderedMessages -= events.size // it is possible to receive more messages than ordered
    receivedInProgressMessages += events.size

    orderMoreMessagesToConsume()

    if(messagesToSend.isEmpty) {
      // in case there are no listeners for given events we need to trigger confirmation
      handleMessageAck(MessageAck(self, aggregateId, events.map(_.version)))
    }
  }


  private def getLastPublishedVersion(aggregateId: AggregateId): Int = {
    eventsAlreadyPropagated.getOrElse(aggregateId.asLong, {
      val version = inputState.lastPublishedEventForAggregate(aggregateId)
      eventsAlreadyPropagated += aggregateId.asLong -> version.asInt
      version.asInt
    })
  }



   private def handleMessageAck(ack: MessageAck): Unit = {

     eventsLogger.foreach(_.debug("EventBus MessageAck received "+ack.aggregateId.asLong+": "+ack.versions.map(_.asInt).mkString(", ")+" from "+ ack.subscriber.path+" as "+self.path))

//    println("MessageAck received " + ack.subscriber+" "+ack.aggregateId.asLong+": "+ack.versions.map(_.asInt).mkString(", ")+" "+self.path)

    messageAckTotal += ack.versions.size

    lastMessageAck = System.nanoTime() / 1_000_000

    val aggregateId = ack.aggregateId

    // Convert to EventIdentifiers
    val eventsIds = ack.versions.map(v => EventIdentifier(aggregateId, v)).toSet

    // Find sent messages for given events
    val receiversToConfirm = messagesSent.view.filterKeys(e => eventsIds.contains(e))

    val withoutConfirmedPerEvent = receiversToConfirm.map {
      case (eventId, receivers) => eventId -> receivers.filterNot(_._2 == ack.subscriber)
    }

    // Which events received confirmation from all receivers and which not
    val (finishedEvents, remainingEvents) = withoutConfirmedPerEvent.partition {
      case (eventId, receivers) => receivers.isEmpty
    }

    if(remainingEvents.nonEmpty) {
      messagesSent ++= remainingEvents
    }

    if(finishedEvents.nonEmpty) {

      val eventsToConfirm: Iterable[EventIdentifier] = finishedEvents.toMap.keys

      receivedInProgressMessages -= finishedEvents.size

      finishedEvents.groupBy(e => (eventSenders(e._1), e._1.aggregateId)).foreach {
        case ((sender, aggregateId), events) =>
          sender ! PublishEventsAck(aggregateId, events.map(_._1.version).toSeq.sortBy(_.asInt))
      }



      messagesSent --= eventsToConfirm
      eventSenders --= eventsToConfirm

      val lastPublishedVersion = AggregateVersion(getLastPublishedVersion(aggregateId))

      val eventsSorted = eventsIds.toList.sortBy(_.version.asInt)

      if(eventsSorted.head.version.isJustAfter(lastPublishedVersion)) {
        val notPersistedYet = eventsPropagatedNotPersisted.getOrElse(aggregateId, List.empty)
        var newVersion = eventsSorted.last.version
        notPersistedYet.foreach(notPersisted => if(notPersisted.isJustAfter(newVersion)) {
          newVersion = notPersisted
        })
        inputState.eventPublished(aggregateId, lastPublishedVersion, newVersion)
        eventsAlreadyPropagated += aggregateId.asLong -> newVersion.asInt
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

      orderedTotal += MAX_BUFFER_SIZE - receivedInProgressMessages - orderedMessages

      orderedMessages = MAX_BUFFER_SIZE - receivedInProgressMessages
      orderedMessageTimestamp = System.nanoTime() / 1_000_000
      orderedMessagePostponedTimestamp = orderedMessageTimestamp
    }
  }
  
  private def generateNextSubscriptionId:String = {
    var subscriptionId: String = null
    do {
      subscriptionId = RandomUtil.generateRandomString(32)
    } while(subscriptionsByIds.contains(subscriptionId))
    subscriptionId
  }
}
