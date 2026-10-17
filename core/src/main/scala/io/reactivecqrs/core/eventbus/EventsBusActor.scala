package io.reactivecqrs.core.eventbus

import java.time.Instant

import org.apache.pekko.actor.{Actor, ActorRef, Stash, Terminated}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.cluster.Readiness
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.MyActorLogging
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal


object EventsBusActor {

  /** Events of one aggregate, committed or re-published; `onlyTo` limits a re-publication to one subscriber.
    * `startsAtSquash`: a re-publication that starts at the aggregate's squash event (F98). */
  case class PublishEvents[AGGREGATE_ROOT](aggregateType: AggregateType, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                            aggregateId: AggregateId, aggregate: Option[AGGREGATE_ROOT], onlyTo: Option[ActorRef] = None,
                                            startsAtSquash: Boolean = false)

  /** To the publisher: every subscription has acknowledged (or failed on) these versions, so their outbox rows can go.
    * `permanentDeleteAt` is the version of a `PermanentDeleteEvent` among them: the aggregate is to be deleted for good. */
  case class PublishEventsAck(aggregateId: AggregateId, versions: Seq[AggregateVersion], permanentDeleteAt: Option[AggregateVersion] = None)


  /** What a subscription delivers. A subscriber answers per kind: each kind keeps a version of its own, so one kind's
    * acknowledgement says nothing about another kind of the same subscriber. */
  sealed trait DeliveryKind
  /** `IdentifiableEvents` */
  case object EventsDelivery extends DeliveryKind
  /** `AggregateWithType` */
  case object AggregatesDelivery extends DeliveryKind
  /** `AggregateWithTypeAndEvents` */
  case object AggregatesWithEventsDelivery extends DeliveryKind

  /** What one subscriber takes of one aggregate type, of one kind. */
  sealed trait SubscribeRequest {
    def aggregateType: AggregateType
    def subscriber: ActorRef
    def kind: DeliveryKind
  }
  // AR8: a subscription takes every event of its aggregate type; the sequence gate needs every version (no classifiers).
  case class SubscribeForEvents(aggregateType: AggregateType, subscriber: ActorRef) extends SubscribeRequest {
    def kind: DeliveryKind = EventsDelivery
  }
  case class SubscribeForAggregates(aggregateType: AggregateType, subscriber: ActorRef) extends SubscribeRequest {
    def kind: DeliveryKind = AggregatesDelivery
  }
  case class SubscribeForAggregatesWithEvents(aggregateType: AggregateType, subscriber: ActorRef) extends SubscribeRequest {
    def kind: DeliveryKind = AggregatesWithEventsDelivery
  }

  // Start-up (D15): the host registers its subscribers; the bus asks each for its subscriptions and publishes nothing
  // until all have answered. Command buses register as the publishers of their aggregate type.
  case class RegisterSubscribers(subscribers: Seq[ActorRef])
  /** `registered`: the subscriber the bus asked, which the answer names: an answer may come from another actor, such as
    * a projection's supervisor behind a cluster singleton proxy (M5). */
  case class GetSubscriptionRequests(registered: ActorRef)
  case class SubscriptionRequests(requests: Seq[SubscribeRequest], registered: ActorRef)
  case class RegisterPublisher(aggregateType: AggregateType, publisher: ActorRef)

  // D57: a projection's supervisor adds the subscriptions of a build that starts later, and removes those of a build it
  // retires: the deliveries waiting for a removed subscriber end for it, so no outbox row waits for it. `keepOutbox`: the
  // subscriber is an instance that stopped, replaced by one subscribed already: its deliveries end without acknowledging
  // their owners, so their rows stay in the outbox and are published again to the replacement (F110). `registered`: the
  // registered subscriber the added ones belong to, which the bus asks again when they stop (M5).
  case class AddSubscriptions(requests: Seq[SubscribeRequest], registered: Option[ActorRef] = None)
  case class RemoveSubscriber(subscriber: ActorRef, keepOutbox: Boolean = false)
  case class SubscriberRemoved(subscriber: ActorRef)

  // From subscribers: a delivery of one kind applied (or applied before), a delivery the subscriber gave up on (D20a),
  // or a request to re-publish from a version, for a gap or a retry (D20c).
  case class MessageAck(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], kind: DeliveryKind)
  case class MessageFailed(subscriber: ActorRef, aggregateId: AggregateId, versions: Seq[AggregateVersion], error: String, kind: DeliveryKind)
  case class RequestRepublish(subscriber: ActorRef, aggregateType: AggregateType, aggregateId: AggregateId, from: AggregateVersion)

  /** From the outbox poller (D8): ask the aggregate's owner to re-publish what waits in its outbox. */
  case class Republish(aggregateType: AggregateType, aggregateId: AggregateId)

  case object GetDeliveryStatus
  /** `waiting`: deliveries in flight; `undelivered`: the latest ones that ended at the deadline; `stopped`: subscribers
    * that stopped, whose events stay in the outbox until the next start. */
  case class DeliveryStatus(waiting: Int, undelivered: Seq[Undelivered], stopped: Seq[String])
  case class Undelivered(aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, subscriber: String, kind: DeliveryKind, at: Instant)

  private case object Tick
}

/**
  * Timing of the bus. A delivery that is neither acknowledged nor failed by `deadline` ends as undelivered (its outbox
  * row stays, so a restart delivers it again, D20b); one that waits `stallAfter` is logged once (at most
  * `stallLinesPerTick` lines per tick), and stalled deliveries are summed up every `summaryInterval`.
  */
case class DeliveryOptions(deadline: FiniteDuration = 10.minutes,
                           stallAfter: FiniteDuration = 1.minute,
                           summaryInterval: FiniteDuration = 5.minutes,
                           tickInterval: FiniteDuration = 5.seconds,
                           stallLinesPerTick: Int = 10)

/**
  * Routes events from their owners to the subscribers of their aggregate type and keeps one delivery per event until
  * every subscription has acknowledged or failed it (then the owner gets `PublishEventsAck`), the deadline passes, or the
  * subscriber stops. It keeps no cursor (D7): subscribers drop duplicates by version, and re-deliveries come from
  * the owners, asked by the outbox poller (D8) or by a subscriber with a gap (D20c). See docs/design/M2-delivery.md.
  */
class EventsBusActor(options: DeliveryOptions = DeliveryOptions(), eventsLogger: Option[Logger] = None) extends Actor with Stash with MyActorLogging {

  private case class EventKey(aggregateId: AggregateId, version: AggregateVersion)

  /** One subscription that a delivery waits for. */
  private case class Target(subscriber: ActorRef, kind: DeliveryKind)

  /** One event's delivery to its subscriptions. `publisher` is set for full publications only: a re-publication to one
    * subscriber never acknowledges the owner, so it cannot delete outbox rows others still wait for. `incomplete`
    * marks a delivery that some subscription did not take (deadline, a stopped subscriber): its outbox row must stay. */
  private case class Delivery(aggregateType: AggregateType, publisher: Option[ActorRef], firstSent: Long, awaiting: Set[Target],
                              incomplete: Boolean, stallLogged: Boolean, permanentDelete: Boolean)

  private val deliveries = mutable.HashMap[EventKey, Delivery]()
  private var subscriptions = Map[AggregateType, Vector[SubscribeRequest]]()
  private var publishers = Map[AggregateType, ActorRef]()
  private var stopped = Set[ActorRef]()

  private var awaitingRegistration = Set[ActorRef]()
  private var registrationRequested = false
  // M5: the registered subscribers (watched), the subscribers each answered with, and those asked again since their
  // subscribers stopped (a projection's singleton moved): a new answer replaces the stopped ones.
  private var registered = Set[ActorRef]()
  private var answeredWith = Map[ActorRef, Set[ActorRef]]()
  private var askedAgain = Set[ActorRef]()
  private var lastSummary = System.nanoTime()

  private val undelivered = mutable.Queue[Undelivered]()
  private val undeliveredKept = 1000

  import context.dispatcher
  private val ticks = context.system.scheduler.scheduleWithFixedDelay(options.tickInterval, options.tickInterval, self, Tick)

  // D71: the node is not ready while its bus waits for its subscribers.
  Readiness(context.system).waitFor(self.path.toString)

  override def postStop(): Unit = {
    ticks.cancel()
    Readiness(context.system).done(self.path.toString)
  }

  override def receive: Receive = registering

  // ****************** START-UP (D15)

  private def registering: Receive = {
    case RegisterSubscribers(subscribers) =>
      registrationRequested = true
      awaitingRegistration = subscribers.toSet
      registered ++= subscribers
      subscribers.foreach { subscriber =>
        context.watch(subscriber)
        subscriber ! GetSubscriptionRequests(subscriber)
      }
      startIfRegistered()
    case SubscriptionRequests(requests, registeredSubscriber) =>
      answered(registeredSubscriber, requests)
      awaitingRegistration -= registeredSubscriber
      startIfRegistered()
    case RegisterPublisher(aggregateType, publisher) => publishers += aggregateType -> publisher
    case AddSubscriptions(requests, registeredSubscriber) => added(requests, registeredSubscriber)
    case RemoveSubscriber(subscriber, keepOutbox) =>
      awaitingRegistration -= subscriber
      removeSubscriber(subscriber, keepOutbox)
      sender() ! SubscriberRemoved(subscriber)
      startIfRegistered()
    case GetDeliveryStatus => sender() ! status
    case Terminated(subscriber) =>
      awaitingRegistration -= subscriber
      stoppedOrUnregistered(subscriber)
      startIfRegistered()
    case Tick =>
      if (registrationRequested) {
        log.warning("Event bus waits for the subscriptions of: " + awaitingRegistration.map(_.path.name).toSeq.sorted.mkString(", "))
        // An ask can be lost on the way, in a singleton proxy's buffer among others: it is asked again.
        awaitingRegistration.foreach(subscriber => subscriber ! GetSubscriptionRequests(subscriber))
      } else {
        log.warning("Event bus waits for RegisterSubscribers from the host; nothing is published until then")
      }
    case _ => stash()
  }

  private def startIfRegistered(): Unit = {
    if (registrationRequested && awaitingRegistration.isEmpty) {
      log.info("Event bus started with " + subscriptions.values.map(_.size).sum + " subscriptions")
      Readiness(context.system).done(self.path.toString)
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

  // A registered subscriber's answer, the first or one asked for again (M5). The subscribers of its earlier answers that
  // stopped and are not in this one were replaced: they leave without acknowledging their deliveries, so the rows stay
  // in the outbox for the replacements (F110).
  private def answered(registeredSubscriber: ActorRef, requests: Seq[SubscribeRequest]): Unit = {
    askedAgain -= registeredSubscriber
    val now = requests.map(_.subscriber).toSet
    answeredWith.getOrElse(registeredSubscriber, Set.empty).filter(s => stopped.contains(s) && !now.contains(s))
      .foreach(replaced => removeSubscriber(replaced, keepOutbox = true))
    added(requests, Some(registeredSubscriber))
  }

  private def added(requests: Seq[SubscribeRequest], registeredSubscriber: Option[ActorRef]): Unit = {
    requests.foreach(subscribe)
    registeredSubscriber.foreach { r => answeredWith += r -> (answeredWith.getOrElse(r, Set.empty) ++ requests.map(_.subscriber)) }
  }

  // A subscriber stopped: its deliveries end (D20b). The registered subscribers that answered with it and live, such as a
  // singleton's proxy, are asked again: a projection's singleton that moved answers with its new instances (M5).
  private def stoppedOrUnregistered(subscriber: ActorRef): Unit = {
    registered -= subscriber
    subscriberStopped(subscriber)
    answeredWith.collect { case (r, subscribers) if subscribers.contains(subscriber) && r != subscriber && registered.contains(r) && !askedAgain.contains(r) => r }
      .foreach { r =>
        askedAgain += r
        r ! GetSubscriptionRequests(r)
      }
  }

  // ****************** RUNNING

  // AR7: a message the bus fails on (a malformed one) is logged and dropped; its outbox rows stay. A restart
  // would lose every delivery and the subscriptions, and the bus would wait for a registration that never comes again.
  private def guarded(handler: Receive): Receive = {
    case message if handler.isDefinedAt(message) =>
      try handler(message) catch {
        case NonFatal(e) => log.error(e, "Event bus dropped " + message.getClass.getSimpleName + " from " + sender().path + ", which failed")
      }
  }

  private def running: Receive = guarded {
    case PublishEvents(aggregateType, events, aggregateId, aggregate, onlyTo, startsAtSquash) =>
      publish(sender(), aggregateType, aggregateId, events, aggregate, onlyTo, startsAtSquash)
    case MessageAck(subscriber, aggregateId, versions, kind) => finish(Target(subscriber, kind), aggregateId, versions, failed = None)
    case MessageFailed(subscriber, aggregateId, versions, error, kind) => finish(Target(subscriber, kind), aggregateId, versions, failed = Some(error))
    case RequestRepublish(subscriber, aggregateType, aggregateId, from) =>
      routeToPublisher(aggregateType, RepublishRange(aggregateId, from, subscriber))
    case Republish(aggregateType, aggregateId) => routeToPublisher(aggregateType, RepublishOutbox(aggregateId))
    case RegisterPublisher(aggregateType, publisher) => publishers += aggregateType -> publisher
    case SubscriptionRequests(requests, registeredSubscriber) => answered(registeredSubscriber, requests)
    case AddSubscriptions(requests, registeredSubscriber) => added(requests, registeredSubscriber)
    case RemoveSubscriber(subscriber, keepOutbox) =>
      removeSubscriber(subscriber, keepOutbox)
      sender() ! SubscriberRemoved(subscriber)
    case GetDeliveryStatus => sender() ! status
    case Terminated(subscriber) => stoppedOrUnregistered(subscriber)
    case Tick => checkDeadlines()
  }

  private def routeToPublisher(aggregateType: AggregateType, message: Any): Unit = publishers.get(aggregateType) match {
    case Some(publisher) => publisher ! message
    case None => log.warning("No command bus registered for " + aggregateType.typeName + ", cannot re-publish: " + message)
  }

  private def publish(publisher: ActorRef, aggregateType: AggregateType, aggregateId: AggregateId,
                      events: Seq[EventInfo[Any]], aggregateRoot: Option[Any], onlyTo: Option[ActorRef], startsAtSquash: Boolean): Unit = {
    val now = System.nanoTime()
    val targets = subscriptions.getOrElse(aggregateType, Vector.empty).filter(s => onlyTo.forall(_ == s.subscriber))
    val (live, ofStopped) = targets.partition(s => !stopped.contains(s.subscriber))

    // A new event goes to every subscription; an event that still waits goes again only to the subscriptions it waits
    // for. A re-publication to one subscriber sends it everything it asked for.
    def wanted(s: SubscribeRequest, event: EventInfo[Any]): Boolean =
      onlyTo.isDefined || deliveries.get(EventKey(aggregateId, event.version)).forall(_.awaiting.contains(Target(s.subscriber, s.kind)))
    val everyLive = live.map(s => Target(s.subscriber, s.kind)).toSet

    val sends = live.flatMap { s =>
      val forSubscription = events.filter(e => wanted(s, e))
      if (forSubscription.isEmpty) None else Some(s -> forSubscription)
    }

    var finished = Vector[(Delivery, EventKey)]()
    events.foreach { event =>
      val key = EventKey(aggregateId, event.version)
      if (!deliveries.contains(key)) {
        // An event not waiting yet was just sent to every live subscription. A stopped subscriber takes nothing, so its
        // events stay in the outbox until it is back after a restart.
        val awaiting = everyLive
        val missedByStopped = ofStopped.nonEmpty
        val delivery = Delivery(aggregateType, if (onlyTo.isEmpty) Some(publisher) else None, now, awaiting,
          incomplete = missedByStopped, stallLogged = false, permanentDelete = event.event.isInstanceOf[PermanentDeleteEvent[_]])
        if (awaiting.isEmpty) finished :+= (delivery -> key) else deliveries += key -> delivery
      }
    }

    // A subscription that is sent a later part of the events than the first does not start at the squash event.
    sends.foreach { case (s, forSubscription) =>
      s.subscriber ! messageFor(s, aggregateType, aggregateId, forSubscription, aggregateRoot, startsAtSquash && forSubscription.head.version == events.head.version)
    }
    eventsLogger.filter(_.isDebugEnabled).foreach(_.debug("EventBus publishing " + aggregateId.asLong + ": " + events.map(_.version.asInt).mkString(",") +
      " to " + sends.map(_._1.subscriber.path.name).mkString(", ")))

    completeAll(finished)
  }

  private def messageFor(s: SubscribeRequest, aggregateType: AggregateType, aggregateId: AggregateId, events: Seq[EventInfo[Any]],
                         aggregateRoot: Option[Any], startsAtSquash: Boolean): AnyRef = s match {
    case _: SubscribeForEvents => IdentifiableEvents(aggregateType, aggregateId, events, startsAtSquash)
    case _: SubscribeForAggregates => AggregateWithType(aggregateType, aggregateId, events.last.version, events.map(_.version), aggregateRoot, startsAtSquash)
    case _: SubscribeForAggregatesWithEvents => AggregateWithTypeAndEvents(aggregateType, aggregateId, aggregateRoot, events, startsAtSquash)
  }

  private def finish(target: Target, aggregateId: AggregateId, versions: Seq[AggregateVersion], failed: Option[String]): Unit = {
    failed.foreach(error => log.warning("Subscriber " + target.subscriber.path.name + " failed on aggregate " + aggregateId.asLong +
      " versions " + versions.map(_.asInt).mkString(",") + ": " + error))
    var finished = Vector[(Delivery, EventKey)]()
    versions.foreach { version =>
      val key = EventKey(aggregateId, version)
      deliveries.get(key).foreach { delivery =>
        val awaiting = delivery.awaiting - target
        if (awaiting.isEmpty) {
          deliveries -= key
          finished :+= (delivery -> key)
        } else {
          deliveries += key -> delivery.copy(awaiting = awaiting)
        }
      }
    }
    completeAll(finished)
  }

  // Acknowledges the owners of the completed deliveries, grouped by owner and aggregate; incomplete ones keep their
  // outbox rows.
  private def completeAll(finished: Seq[(Delivery, EventKey)]): Unit =
    finished.collect { case (delivery, key) if delivery.publisher.isDefined && !delivery.incomplete => (delivery.publisher.get, delivery, key) }
      .groupBy { case (publisher, _, key) => (publisher, key.aggregateId) }
      .foreach { case ((publisher, aggregateId), acked) =>
        publisher ! PublishEventsAck(aggregateId, acked.map(_._3.version).sortBy(_.asInt),
          acked.collectFirst { case (_, delivery, key) if delivery.permanentDelete => key.version })
      }

  // A subscriber that stops takes no more deliveries: its waiting ones end without an ack, and so do later ones of
  // its aggregate types, so their outbox rows stay for the next start while the other subscribers keep flowing (D20b).
  private def subscriberStopped(subscriber: ActorRef): Unit = {
    if (!stopped.contains(subscriber)) {
      log.error("Subscriber " + subscriber.path + " stopped; its events stay in the outbox until the next start")
      stopped += subscriber
      endDeliveries(_.subscriber == subscriber, _ => true)
    }
  }

  // A removed subscriber takes nothing more, and no delivery waits for it any more: one it was the last to answer is
  // complete, so its owner is acknowledged (unlike a stopped subscriber's, which keeps its outbox rows), unless the
  // outbox is kept for its replacement: then its deliveries end as a stopped subscriber's do.
  private def removeSubscriber(subscriber: ActorRef, keepOutbox: Boolean): Unit = {
    context.unwatch(subscriber)
    subscriptions = subscriptions.map { case (aggregateType, requests) => aggregateType -> requests.filterNot(_.subscriber == subscriber) }.filter(_._2.nonEmpty)
    answeredWith = answeredWith.map { case (r, subscribers) => r -> (subscribers - subscriber) }
    stopped -= subscriber
    if (keepOutbox) endDeliveries(_.subscriber == subscriber, _ => true) else completeFor(subscriber)
  }

  private def completeFor(subscriber: ActorRef): Unit = {
    var finished = Vector[(Delivery, EventKey)]()
    deliveries.toSeq.foreach { case (key, delivery) =>
      if (delivery.awaiting.exists(_.subscriber == subscriber)) {
        val awaiting = delivery.awaiting.filterNot(_.subscriber == subscriber)
        if (awaiting.isEmpty) {
          deliveries -= key
          finished :+= (delivery.copy(awaiting = awaiting) -> key)
        } else {
          deliveries += key -> delivery.copy(awaiting = awaiting)
        }
      }
    }
    completeAll(finished)
  }

  private def checkDeadlines(): Unit = {
    val now = System.nanoTime()
    val deadline = options.deadline.toNanos
    val expired = deliveries.filter { case (_, delivery) => now - delivery.firstSent >= deadline }.toSeq
    if (expired.nonEmpty) {
      val ended = expired.flatMap { case (key, delivery) => delivery.awaiting.toSeq.map(target => (key, delivery, target)) }
      ended.take(options.stallLinesPerTick).foreach { case (key, delivery, target) =>
        log.error("Undelivered after " + options.deadline + ": " + delivery.aggregateType.simpleName + " " + key.aggregateId.asLong +
          " version " + key.version.asInt + " to " + target.subscriber.path.name + " (" + target.kind + "); it stays in the outbox until the next start")
      }
      if (ended.size > options.stallLinesPerTick) {
        log.error(s"${ended.size - options.stallLinesPerTick} more deliveries ended undelivered after ${options.deadline}")
      }
      ended.foreach { case (key, delivery, target) =>
        undelivered.enqueue(Undelivered(delivery.aggregateType, key.aggregateId, key.version, target.subscriber.path.name, target.kind, Instant.now))
      }
      while (undelivered.size > undeliveredKept) undelivered.dequeue()
      val expiredKeys = expired.map(_._1).toSet
      endDeliveries(_ => true, expiredKeys.contains)
    }

    // One line per stalled delivery, at most a few per tick: under overload the summary says the rest.
    val stallAfter = options.stallAfter.toNanos
    deliveries.toSeq.filter { case (_, delivery) => !delivery.stallLogged && now - delivery.firstSent >= stallAfter }
      .zipWithIndex.foreach { case ((key, delivery), index) =>
        if (index < options.stallLinesPerTick) {
          log.warning("Delivery waits for " + options.stallAfter + ": " + delivery.aggregateType.simpleName + " " + key.aggregateId.asLong +
            " version " + key.version.asInt + " to " + delivery.awaiting.map(t => t.subscriber.path.name + " (" + t.kind + ")").mkString(", "))
        }
        deliveries += key -> delivery.copy(stallLogged = true)
      }
    if (now - lastSummary >= options.summaryInterval.toNanos) {
      lastSummary = now
      val stalled = deliveries.values.filter(d => now - d.firstSent >= stallAfter).toSeq
      if (stalled.nonEmpty) {
        log.warning(s"${stalled.size} deliveries wait longer than ${options.stallAfter}, by subscriber: " +
          stalled.flatMap(_.awaiting).groupBy(_.subscriber.path.name).map { case (name, waits) => name + " " + waits.size }.mkString(", "))
      }
    }
  }

  private def endDeliveries(target: Target => Boolean, selected: EventKey => Boolean): Unit = {
    var finished = Vector[(Delivery, EventKey)]()
    deliveries.toSeq.foreach { case (key, delivery) =>
      if (selected(key) && delivery.awaiting.exists(target)) {
        val awaiting = delivery.awaiting.filterNot(target)
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
  }

  private def status: DeliveryStatus = DeliveryStatus(deliveries.size, undelivered.toList, stopped.toSeq.map(_.path.name).sorted)
}
