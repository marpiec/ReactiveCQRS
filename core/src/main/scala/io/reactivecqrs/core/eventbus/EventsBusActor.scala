package io.reactivecqrs.core.eventbus

import java.time.Instant

import org.apache.pekko.actor.{Actor, ActorRef, Stash, Terminated}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.backpressure.BackPressureActor._
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.MyActorLogging
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal


object EventsBusActor {

  /** Events of one aggregate, committed or re-published; `onlyTo` limits a re-publication to one subscriber. */
  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT], onlyTo: Option[ActorRef] = None)

  /** To the publisher: every subscriber has acknowledged (or failed on) these versions, so their outbox rows can go. */
  case class PublishEventsAck(aggregateId: AggregateId, versions: Seq[AggregateVersion])

  case class PublishReplayedEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                                   aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT])

  /** What one subscriber takes of one aggregate type: its events (`IdentifiableEvents`), its state with the versions
    * (`AggregateWithType`), or both (`AggregateWithTypeAndEvents`). */
  sealed trait SubscribeRequest {
    def aggregateType: AggregateType
    def subscriber: ActorRef
  }
  case class SubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest
  case class SubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef, classifier: AggregateSubscriptionClassifier = AcceptAllAggregateIdClassifier) extends SubscribeRequest
  case class SubscribeForAggregatesWithEvents(aggregateType: AggregateType, subscriber: ActorRef, classifier: SubscriptionClassifier = AcceptAllClassifier) extends SubscribeRequest

  // Start-up (D15): the host registers its subscribers; the bus asks each for its subscriptions and publishes nothing
  // until all have answered. Command buses register as the publishers of their aggregate type.
  case class RegisterSubscribers(subscribers: Seq[ActorRef])
  case object GetSubscriptionRequests
  case class SubscriptionRequests(requests: Seq[SubscribeRequest])
  case class RegisterPublisher(aggregateType: AggregateType, publisher: ActorRef)

  // From subscribers: a delivery applied (or already applied), a delivery the subscriber gave up on (D20a), or a
  // request to re-publish from a version, for a gap or a retry (D20c).
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion])
  case class MessageFailed(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], error: String)
  case class RequestRepublish(subscriber: ActorRef, aggregateType: AggregateType, aggregateId: AggregateId, from: AggregateVersion)

  /** From the outbox poller (D8): ask the aggregate's owner to re-publish what waits in its outbox. */
  case class Republish(aggregateType: AggregateType, aggregateId: AggregateId)

  case object GetDeliveryStatus
  case class DeliveryStatus(waiting: Int, undelivered: Seq[Undelivered])
  case class Undelivered(aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, subscriber: String, at: Instant)

  object LogDetailedStatus

  private case object Tick
}

/**
  * Timing of the bus. A delivery that is neither acknowledged nor failed by `deadline` ends as undelivered (its outbox
  * row stays, so a restart delivers it again, D20b); one that waits `stallAfter` is logged once, and stalled deliveries
  * are summed up every `summaryInterval`. `maxReplayBuffer` bounds the replayed events in flight (back-pressure).
  */
case class DeliveryOptions(deadline: FiniteDuration = 10.minutes,
                           stallAfter: FiniteDuration = 1.minute,
                           summaryInterval: FiniteDuration = 5.minutes,
                           tickInterval: FiniteDuration = 5.seconds,
                           maxReplayBuffer: Int = 10000)

/**
  * Routes events from their owners to the subscribers of their aggregate type and keeps one delivery per event until
  * every subscriber has acknowledged or failed it (then the owner gets `PublishEventsAck`), the deadline passes, or the
  * subscriber stops. It keeps no cursor (D7): subscribers drop duplicates by version, and re-deliveries come from
  * the owners, asked by the outbox poller (D8) or by a subscriber with a gap (D20c). See docs/design/M2-delivery.md.
  */
class EventsBusActor(options: DeliveryOptions = DeliveryOptions(), eventsLogger: Option[Logger] = None) extends Actor with Stash with MyActorLogging {

  private case class EventKey(aggregateId: AggregateId, version: AggregateVersion)

  /** One event's delivery to its subscribers. `publisher` is set for full publications only: a re-publication to one
    * subscriber never acknowledges the owner, so it cannot delete outbox rows others still wait for. `incomplete`
    * marks a delivery that some subscriber did not take (deadline, stop): its outbox row must stay. */
  private case class Delivery(aggregateType: AggregateType, publisher: Option[ActorRef], firstSent: Long, awaiting: Set[ActorRef],
                              replayed: Boolean, incomplete: Boolean, stallLogged: Boolean)

  private val deliveries = mutable.HashMap[EventKey, Delivery]()
  private var subscriptions = Map[AggregateType, Vector[SubscribeRequest]]()
  private var publishers = Map[AggregateType, ActorRef]()

  private var awaitingRegistration = Set[ActorRef]()
  private var registrationRequested = false
  private var lastSummary = System.nanoTime()

  private val undelivered = mutable.Queue[Undelivered]()
  private val undeliveredKept = 1000

  // Back-pressure for the replayer (until M4 replaces it): replayed events in flight.
  private var backPressureProducerActor: Option[ActorRef] = None
  private var replayedInFlight = 0
  private var orderedMessages = 0
  private var afterFinishRespondTo: Option[ActorRef] = None

  import context.dispatcher
  private val ticks = context.system.scheduler.scheduleWithFixedDelay(options.tickInterval, options.tickInterval, self, Tick)

  override def postStop(): Unit = ticks.cancel()

  override def receive: Receive = registering

  // ****************** START-UP (D15)

  private def registering: Receive = {
    case RegisterSubscribers(subscribers) =>
      registrationRequested = true
      awaitingRegistration = subscribers.toSet
      subscribers.foreach { subscriber =>
        context.watch(subscriber)
        subscriber ! GetSubscriptionRequests
      }
      startIfRegistered()
    case SubscriptionRequests(requests) =>
      requests.foreach(subscribe)
      awaitingRegistration -= sender()
      startIfRegistered()
    case RegisterPublisher(aggregateType, publisher) => publishers += aggregateType -> publisher
    case GetDeliveryStatus => sender() ! status
    case Terminated(subscriber) =>
      awaitingRegistration -= subscriber
      subscriberStopped(subscriber)
      startIfRegistered()
    case Tick =>
      if (registrationRequested) {
        log.warning("Event bus waits for the subscriptions of: " + awaitingRegistration.map(_.path.name).toSeq.sorted.mkString(", "))
      } else {
        log.warning("Event bus waits for RegisterSubscribers from the host; nothing is published until then")
      }
    case _ => stash()
  }

  private def startIfRegistered(): Unit = {
    if (registrationRequested && awaitingRegistration.isEmpty) {
      log.info("Event bus started with " + subscriptions.values.map(_.size).sum + " subscriptions")
      context.become(running)
      unstashAll()
    }
  }

  private def subscribe(request: SubscribeRequest): Unit = {
    context.watch(request.subscriber) // a request may name another actor than the registered subscriber
    val existing = subscriptions.getOrElse(request.aggregateType, Vector())
    if (!existing.contains(request)) {
      subscriptions += request.aggregateType -> (existing :+ request)
    }
  }

  // ****************** RUNNING

  // AR7: a message the bus fails on (a classifier that throws) is logged and dropped; its outbox rows stay. A restart
  // would lose every delivery and the subscriptions, and the bus would wait for a registration that never comes again.
  private def guarded(handler: Receive): Receive = {
    case message if handler.isDefinedAt(message) =>
      try handler(message) catch {
        case NonFatal(e) => log.error(e, "Event bus dropped " + message.getClass.getSimpleName + " from " + sender().path + ", which failed")
      }
  }

  private def running: Receive = guarded {
    case PublishEvents(aggregateType, events, aggregateId, aggregate, onlyTo) =>
      publish(sender(), aggregateType, aggregateId, events, aggregate, replayed = false, onlyTo)
    case PublishReplayedEvents(aggregateType, events, aggregateId, aggregate) =>
      publish(sender(), aggregateType, aggregateId, events, aggregate, replayed = true, None)
    case MessageAck(subscriber, aggregateId, versions) => finish(subscriber, aggregateId, versions, failed = None)
    case MessageFailed(subscriber, aggregateId, versions, error) => finish(subscriber, aggregateId, versions, failed = Some(error))
    case RequestRepublish(subscriber, aggregateType, aggregateId, from) =>
      routeToPublisher(aggregateType, RepublishRange(aggregateId, from, subscriber))
    case Republish(aggregateType, aggregateId) => routeToPublisher(aggregateType, RepublishOutbox(aggregateId))
    case RegisterPublisher(aggregateType, publisher) => publishers += aggregateType -> publisher
    case GetDeliveryStatus => sender() ! status
    case Terminated(subscriber) => subscriberStopped(subscriber)
    case Tick => checkDeadlines()
    case ConsumerStart =>
      backPressureProducerActor = Some(sender())
      orderedMessages = 0
      orderMoreMessagesToConsume()
    case ConsumerStop =>
      log.info("EventBus Stop, replayed events in flight: " + replayedInFlight)
      afterFinishRespondTo = Some(sender())
      backPressureProducerActor = None
      if (replayedInFlight == 0) {
        sender() ! ConsumerFinished
        afterFinishRespondTo = None
      }
    case LogDetailedStatus => log.warning("EventBus status: " + status + ", replayed in flight: " + replayedInFlight + ", ordered: " + orderedMessages)
  }

  private def routeToPublisher(aggregateType: AggregateType, message: Any): Unit = publishers.get(aggregateType) match {
    case Some(publisher) => publisher ! message
    case None => log.warning("No command bus registered for " + aggregateType.typeName + ", cannot re-publish: " + message)
  }

  private def publish(publisher: ActorRef, aggregateType: AggregateType, aggregateId: AggregateId,
                      events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], replayed: Boolean, onlyTo: Option[ActorRef]): Unit = {
    val now = System.nanoTime()
    val targets = subscriptions.getOrElse(aggregateType, Vector.empty).filter(s => onlyTo.forall(_ == s.subscriber))

    // A new event goes to every subscriber that accepts it; an event that still waits goes again only to the
    // subscribers it waits for. A re-publication to one subscriber sends it everything it asked for.
    def wanted(s: SubscribeRequest, event: EventInfo[Any]): Boolean =
      accepts(s, aggregateId, event) && (onlyTo.isDefined || deliveries.get(EventKey(aggregateId, event.version)).forall(_.awaiting.contains(s.subscriber)))

    val sends = targets.flatMap { s =>
      val forSubscriber = events.filter(e => wanted(s, e))
      if (forSubscriber.isEmpty) None else Some(s -> forSubscriber)
    }

    var finished = Vector[(Delivery, EventKey)]()
    events.foreach { event =>
      val key = EventKey(aggregateId, event.version)
      if (!deliveries.contains(key)) {
        val awaiting = sends.collect { case (s, forSubscriber) if forSubscriber.exists(_.version == event.version) => s.subscriber }.toSet
        val delivery = Delivery(aggregateType, if (onlyTo.isEmpty && !replayed) Some(publisher) else None, now, awaiting, replayed,
          incomplete = false, stallLogged = false)
        if (replayed) replayedInFlight += 1
        if (awaiting.isEmpty) finished :+= (delivery -> key) else deliveries += key -> delivery
      }
    }
    if (replayed) {
      orderedMessages -= events.size
    }

    sends.foreach { case (s, forSubscriber) => s.subscriber ! messageFor(s, aggregateType, aggregateId, forSubscriber, aggregateRoot, replayed) }
    eventsLogger.foreach(_.debug("EventBus publishing " + aggregateId.asLong + ": " + events.map(_.version.asInt).mkString(",") +
      " to " + sends.map(_._1.subscriber.path.name).mkString(", ")))

    completeAll(finished)
    orderMoreMessagesToConsume()
  }

  private def accepts(s: SubscribeRequest, aggregateId: AggregateId, event: EventInfo[Any]): Boolean = s match {
    case e: SubscribeForEvents => e.classifier.accept(aggregateId, EventType(event.event.getClass.getName))
    case a: SubscribeForAggregates => a.classifier.accept(aggregateId)
    case ae: SubscribeForAggregatesWithEvents => ae.classifier.accept(aggregateId, EventType(event.event.getClass.getName))
  }

  private def messageFor(s: SubscribeRequest, aggregateType: AggregateType, aggregateId: AggregateId, events: Seq[EventInfo[Any]],
                         aggregateRoot: Option[Any], replayed: Boolean): AnyRef = s match {
    case _: SubscribeForEvents => IdentifiableEvents(aggregateType, aggregateId, events, replayed)
    case _: SubscribeForAggregates => AggregateWithType(aggregateType, aggregateId, events.last.version, events.map(_.version), aggregateRoot, replayed)
    case _: SubscribeForAggregatesWithEvents => AggregateWithTypeAndEvents(aggregateType, aggregateId, aggregateRoot, events, replayed)
  }

  private def finish(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], failed: Option[String]): Unit = {
    failed.foreach(error => log.warning("Subscriber " + subscriber.path.name + " failed on aggregate " + aggregateId.asLong +
      " versions " + versions.map(_.asInt).mkString(",") + ": " + error))
    var finished = Vector[(Delivery, EventKey)]()
    versions.foreach { version =>
      val key = EventKey(aggregateId, version)
      deliveries.get(key).foreach { delivery =>
        val awaiting = delivery.awaiting - subscriber
        if (awaiting.isEmpty) {
          deliveries -= key
          finished :+= (delivery -> key)
        } else {
          deliveries += key -> delivery.copy(awaiting = awaiting)
        }
      }
    }
    completeAll(finished)
    orderMoreMessagesToConsume()
  }

  // Acknowledges the owners of the completed deliveries, grouped by owner and aggregate; replayed deliveries only
  // count down the back-pressure, and incomplete ones keep their outbox rows.
  private def completeAll(finished: Seq[(Delivery, EventKey)]): Unit = {
    finished.foreach { case (delivery, _) => if (delivery.replayed) replayedInFlight -= 1 }
    finished.collect { case (delivery, key) if delivery.publisher.isDefined && !delivery.incomplete => (delivery.publisher.get, key) }
      .groupBy { case (publisher, key) => (publisher, key.aggregateId) }
      .foreach { case ((publisher, aggregateId), keys) => publisher ! PublishEventsAck(aggregateId, keys.map(_._2.version).sortBy(_.asInt)) }
    if (replayedInFlight == 0) {
      afterFinishRespondTo.foreach(_ ! ConsumerFinished)
      afterFinishRespondTo = None
    }
  }

  // A subscriber that stops takes no more deliveries: its waiting ones end without an ack, so their outbox rows stay
  // for the next start, and its subscriptions are dropped so other subscribers keep flowing (D20b).
  private def subscriberStopped(subscriber: ActorRef): Unit = {
    log.error("Subscriber " + subscriber.path + " stopped; its waiting deliveries stay in the outbox and its subscriptions are dropped")
    subscriptions = subscriptions.map { case (aggregateType, subs) => aggregateType -> subs.filterNot(_.subscriber == subscriber) }
    endDeliveriesTo(Set(subscriber), _ => true)
  }

  private def checkDeadlines(): Unit = {
    val now = System.nanoTime()
    val deadline = options.deadline.toNanos
    val expired = deliveries.collect { case (key, delivery) if now - delivery.firstSent >= deadline => key }.toSet
    if (expired.nonEmpty) {
      deliveries.filter { case (key, _) => expired.contains(key) }.foreach { case (key, delivery) =>
        delivery.awaiting.foreach { subscriber =>
          log.error("Undelivered after " + options.deadline + ": " + delivery.aggregateType.simpleName + " " + key.aggregateId.asLong +
            " version " + key.version.asInt + " to " + subscriber.path.name + "; it stays in the outbox until the next start")
          undelivered.enqueue(Undelivered(delivery.aggregateType, key.aggregateId, key.version, subscriber.path.name, Instant.now))
          while (undelivered.size > undeliveredKept) undelivered.dequeue()
        }
      }
      endDeliveriesTo(deliveries.values.flatMap(_.awaiting).toSet, key => expired.contains(key))
    }

    val stallAfter = options.stallAfter.toNanos
    deliveries.toSeq.foreach { case (key, delivery) =>
      if (!delivery.stallLogged && now - delivery.firstSent >= stallAfter) {
        log.warning("Delivery waits for " + options.stallAfter + ": " + delivery.aggregateType.simpleName + " " + key.aggregateId.asLong +
          " version " + key.version.asInt + " to " + delivery.awaiting.map(_.path.name).mkString(", "))
        deliveries += key -> delivery.copy(stallLogged = true)
      }
    }
    if (now - lastSummary >= options.summaryInterval.toNanos) {
      lastSummary = now
      val stalled = deliveries.values.filter(d => now - d.firstSent >= stallAfter).toSeq
      if (stalled.nonEmpty) {
        log.warning(stalled.size + " deliveries wait longer than " + options.stallAfter + ", by subscriber: " +
          stalled.flatMap(_.awaiting).groupBy(_.path.name).map { case (name, waits) => name + " " + waits.size }.mkString(", "))
      }
    }
  }

  private def endDeliveriesTo(subscribers: Set[ActorRef], selected: EventKey => Boolean): Unit = {
    var finished = Vector[(Delivery, EventKey)]()
    deliveries.toSeq.foreach { case (key, delivery) =>
      if (selected(key) && delivery.awaiting.exists(subscribers.contains)) {
        val awaiting = delivery.awaiting -- subscribers
        val ended = delivery.copy(awaiting = awaiting, incomplete = true)
        if (awaiting.isEmpty) {
          deliveries -= key
          finished :+= (ended -> key)
        } else {
          deliveries += key -> ended
        }
      }
    }
    completeAll(finished)
    orderMoreMessagesToConsume()
  }

  private def status: DeliveryStatus = DeliveryStatus(deliveries.size, undelivered.toList)

  private def orderMoreMessagesToConsume(): Unit = {
    if (backPressureProducerActor.isDefined && replayedInFlight + orderedMessages < options.maxReplayBuffer) {
      val allowed = options.maxReplayBuffer - replayedInFlight - orderedMessages
      backPressureProducerActor.get ! ConsumerAllowedMore(allowed)
      orderedMessages += allowed
    }
  }
}
