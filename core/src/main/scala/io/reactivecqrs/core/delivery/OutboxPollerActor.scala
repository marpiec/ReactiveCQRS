package io.reactivecqrs.core.delivery

import java.time.Clock

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.Republish
import io.reactivecqrs.core.eventstore.{EventStoreState, WaitingInOutbox}
import io.reactivecqrs.core.metrics.ReactiveMetrics
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging}
import org.apache.pekko.actor.{Actor, ActorRef}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object OutboxPollerActor {
  /** Reads the outbox now; the poller's timer sends it, and tests can too. */
  case object Poll
  case object GetOutboxStatus
  /** `waiting`: aggregates with rows in the outbox at the last poll (at most `maxTracked`); `gaveUp`: those reported as
    * undelivered. */
  case class OutboxStatus(waiting: Int, gaveUp: Seq[WaitingInOutbox])
  private case class Read(result: Try[Seq[WaitingInOutbox]])
}

/** @param firstPollAfter gives the host time to start its command buses, which the bus routes re-publications to */
case class OutboxPollerOptions(firstPollAfter: FiniteDuration = 10.seconds,
                               pollInterval: FiniteDuration = 30.seconds,
                               redelivery: RedeliveryPolicy.Options = RedeliveryPolicy.Options(),
                               maxTracked: Int = 100000)

/**
  * The single owner of re-deliveries (D8). Every poll it reads which aggregates have rows waiting in the outbox, and,
  * as the re-delivery policy decides, asks their owners to re-publish them: through the event bus, which passes the
  * request to the aggregate type's command bus. One per system; a cluster singleton from M5 (D46).
  */
class OutboxPollerActor(eventStore: EventStoreState, eventBus: ActorRef, options: OutboxPollerOptions = OutboxPollerOptions(),
                        clock: Clock = Clock.systemUTC()) extends Actor with MyActorLogging {

  import OutboxPollerActor._
  import context.dispatcher

  private val startedAt = clock.instant()
  private val maxLinesPerPoll = 10
  private var memory = Map[AggregateId, RedeliveryPolicy.Tracked]()
  private var lastWaiting = 0 // F169: the memory leaves out new aggregates postponed beyond a poll's re-publications
  private var reading = false

  private val polls = context.system.scheduler.scheduleWithFixedDelay(options.firstPollAfter, options.pollInterval, self, Poll)

  override def postStop(): Unit = polls.cancel()

  override def receive: Receive = {
    case Poll => if (!reading) read()
    case Read(result) =>
      reading = false
      poll(result)
    case GetOutboxStatus => sender() ! OutboxStatus(lastWaiting, memory.values.filter(_.gaveUp).map(_.waiting).toSeq)
  }

  // F27: the read runs on the blocking dispatcher; its result comes back as a message.
  private def read(): Unit = {
    reading = true
    Future(eventStore.readWaitingInOutbox(options.maxTracked))(BlockingDispatcher(context.system)).onComplete(result => self ! Read(result))
  }

  // A failed read is logged and retried at the next poll (AR7: a throw would restart the poller and forget the attempts).
  private def poll(result: Try[Seq[WaitingInOutbox]]): Unit = result match {
    case Success(waiting) =>
      val decision = RedeliveryPolicy.decide(waiting, memory, startedAt, clock.instant(), options.redelivery)
      decision.republish.foreach(w => eventBus ! Republish(w.aggregateType, w.aggregateId))
      // A few lines per poll; under overload the count says the rest (GetOutboxStatus lists them all).
      decision.gaveUp.take(maxLinesPerPoll).foreach(w => log.error("Undelivered: " + w.aggregateType.simpleName + " " + w.aggregateId.asLong +
        " from version " + w.oldestVersion.asInt + " still waits in the outbox after " + options.redelivery.giveUpAfter + "; it is re-published again every " + options.redelivery.retryGivenUpEvery))
      if (decision.gaveUp.size > maxLinesPerPoll) {
        log.error(s"${decision.gaveUp.size - maxLinesPerPoll} more aggregates are undelivered after ${options.redelivery.giveUpAfter}")
      }
      memory = decision.memory
      lastWaiting = waiting.size
      // R19: the outbox's levels at each poll.
      val metrics = ReactiveMetrics(context.system).metrics
      metrics.gauge("reactivecqrs.outbox.waiting", Map.empty, waiting.size)
      metrics.gauge("reactivecqrs.outbox.given.up", Map.empty, memory.valuesIterator.count(_.gaveUp))
      metrics.count("reactivecqrs.outbox.republished", Map.empty, decision.republish.size)
    case Failure(e) =>
      log.error(e, "Reading the outbox failed; the next poll tries again in " + options.pollInterval)
  }
}
