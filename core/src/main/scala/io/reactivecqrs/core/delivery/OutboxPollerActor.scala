package io.reactivecqrs.core.delivery

import java.time.Clock

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.Republish
import io.reactivecqrs.core.eventstore.{EventStoreState, WaitingInOutbox}
import io.reactivecqrs.core.metrics.ReactiveMetrics
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging, ReadWatch}
import org.apache.pekko.actor.{Actor, ActorRef}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** The outbox poller's messages. */
object OutboxPollerActor {
  /** Reads the outbox now; the poller's timer sends it, and tests can too. */
  case object Poll
  /** Asks the poller for its `OutboxStatus`. */
  case object GetOutboxStatus
  /** `waiting`: aggregates with rows in the outbox at the last poll (at most `maxTracked`, and half as many newest);
    * `gaveUp`: the first 100 of those reported as undelivered, the longest waiting first, and `gaveUpTotal` all of
    * them: an answer stays small enough for another node (F228). */
  case class OutboxStatus(waiting: Int, gaveUp: Seq[WaitingInOutbox], gaveUpTotal: Int = 0)
  private case class Read(number: Int, result: Try[Seq[WaitingInOutbox]], restoreFailures: Option[Long])
}

/** When the outbox poller reads the outbox, and what it does with what waits there.
  *
  * @param firstPollAfter gives the host time to start its command buses, which the bus routes re-publications to
  * @param pollInterval how often it reads the outbox
  * @param redelivery when it has an aggregate's events re-published, and when it gives up
  * @param maxTracked how many waiting aggregates a poll reads and keeps track of at most, the oldest first; when there
  *                   are more, it reads half as many of the newest too, so aggregates waiting for a subscriber that
  *                   stays away cannot keep a newer lost delivery from being re-published (F260)
  * @param readTimeout how long a read may run before a poll gives it up, logs it and reads again (F186)
  */
case class OutboxPollerOptions(firstPollAfter: FiniteDuration = 10.seconds,
                               pollInterval: FiniteDuration = 30.seconds,
                               redelivery: RedeliveryPolicy.Options = RedeliveryPolicy.Options(),
                               maxTracked: Int = 100000,
                               readTimeout: FiniteDuration = 5.minutes)

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
  private val StatusItems = 100
  private var memory = Map[AggregateId, RedeliveryPolicy.Tracked]()
  private var lastWaiting = 0 // F169: the memory leaves out new aggregates postponed beyond a poll's re-publications
  private val reads = new ReadWatch(options.readTimeout)

  private val polls = context.system.scheduler.scheduleWithFixedDelay(options.firstPollAfter, options.pollInterval, self, Poll)

  override def postStop(): Unit = polls.cancel()

  override def receive: Receive = {
    case Poll => reads.start().foreach { case (number, gaveUp) =>
      if (gaveUp) {
        log.error(reads.givenUp("Reading the outbox"))
        ReactiveMetrics(context.system).metrics.count("reactivecqrs.outbox.read.abandoned", Map.empty, 1)
      }
      read(number)
    }
    case Read(number, result, restoreFailures) => if (reads.ended(number)) {
      // CR-4: the aggregates that cannot be restored, counted here as the poller is the one periodic reader of a system.
      restoreFailures.foreach(ReactiveMetrics(context.system).metrics.gauge("reactivecqrs.restore.failures", Map.empty, _))
      poll(result)
    }
    case GetOutboxStatus =>
      val gaveUp = memory.values.filter(_.gaveUp).map(_.waiting).toSeq
      sender() ! OutboxStatus(lastWaiting, gaveUp.sortBy(w => (w.since, w.aggregateId.asLong)).take(StatusItems), gaveUp.size)
  }

  // F27: the read runs on the blocking dispatcher; its result comes back as a message.
  private def read(number: Int): Unit =
    Future {
      val oldest = eventStore.readWaitingInOutbox(options.maxTracked)
      val waiting = if (oldest.size < options.maxTracked) oldest else {
        val known = oldest.map(_.aggregateId).toSet
        oldest ++ eventStore.readNewestWaitingInOutbox(options.maxTracked / 2).filterNot(w => known.contains(w.aggregateId))
      }
      (waiting, Try(eventStore.restoreFailuresCount()).toOption)
    }(BlockingDispatcher(context.system)).onComplete(result => self ! Read(number, result.map(_._1), result.toOption.flatMap(_._2)))

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
