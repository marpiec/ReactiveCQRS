package io.reactivecqrs.core.aggregate

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.util.MyActorLogging
import org.apache.pekko.actor.{Actor, ActorRef, Cancellable}

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object SquashScheduler {
  /** Looks for aggregates to squash now, unless the run before is still going; the scheduler's timer sends it, and
    * tests can too. */
  case object Run
  case object GetSquashStatus
  /** Counts since the scheduler started; `waiting`: the squashes of the current run not answered yet. */
  case class SquashStatus(squashed: Long, refused: Long, waiting: Int)

  /** An aggregate type the scheduler squashes: the type's command bus, and its context's rule. */
  case class SquashTarget(aggregateType: AggregateType, commandBus: ActorRef, policy: AutomaticSquash)

  object SquashTarget {
    def of[AGGREGATE_ROOT: ClassTag](context: AggregateContext[AGGREGATE_ROOT], commandBus: ActorRef): SquashTarget = {
      val aggregateType = AggregateType.of[AGGREGATE_ROOT]
      SquashTarget(aggregateType, commandBus, context.squash.flatMap(_.automatic).getOrElse(throw new IllegalArgumentException(
        aggregateType.simpleName + " has no automatic squash rule (SquashPolicy.automatic)")))
    }
  }

  /** Stored with the squash events the scheduler makes. */
  val metadata: EventMetadata = EventMetadata("system:squash-scheduler")

  private case class NotAnswered(aggregateId: AggregateId)
}

/** @param perRun how many aggregates of each type a run squashes at most; the next run goes on after the last one
  * @param answerTimeout how long the scheduler waits for one squash before it goes on with the next */
case class SquashSchedulerOptions(firstRunAfter: FiniteDuration = 1.minute, interval: FiniteDuration = 1.hour,
                                  perRun: Int = 100, answerTimeout: FiniteDuration = 1.minute) {
  require(perRun > 0, "perRun must be positive")
}

/**
  * Squashes aggregates by their contexts' `AutomaticSquash` rule (D52). Each run reads, per type, up to `perRun`
  * aggregates the rule selects, going on from where the run before stopped, so aggregates refused every time (D51's
  * preconditions) do not starve the others; then it asks their owners, through the command bus, to squash them one at
  * a time, so maintenance never holds more than one database connection. A refused aggregate is tried again in a
  * later run. One per system; a cluster singleton from M5 (D46).
  */
class SquashScheduler(eventStore: EventStoreState, targets: Seq[SquashScheduler.SquashTarget], options: SquashSchedulerOptions = SquashSchedulerOptions())
  extends Actor with MyActorLogging {

  import SquashScheduler._
  import context.dispatcher

  private var after = Map[AggregateType, AggregateId]()
  private var waiting = Vector[(SquashTarget, AggregateId, AggregateVersion)]()
  private var current: Option[(AggregateId, Cancellable)] = None
  private var squashed = 0L
  private var refused = 0L

  private val runs = context.system.scheduler.scheduleWithFixedDelay(options.firstRunAfter, options.interval, self, Run)

  override def postStop(): Unit = {
    runs.cancel()
    current.foreach(_._2.cancel())
  }

  override def receive: Receive = {
    case Run =>
      if (current.isEmpty) {
        waiting = targets.toVector.flatMap(candidates)
        next()
      } else log.info("The squash run before is still going; this one is skipped")
    case AggregateSquashed(id, _) if current.exists(_._1 == id) =>
      squashed += 1
      next()
    case SquashRefused(id, reason) if current.exists(_._1 == id) =>
      refused += 1
      log.debug("Squash of aggregate {} refused: {}", id.asLong, reason)
      next()
    case NotAnswered(id) if current.exists(_._1 == id) =>
      log.warning("Squash of aggregate " + id.asLong + " not answered in " + options.answerTimeout + "; the scheduler goes on with the next")
      next()
    case GetSquashStatus => sender() ! SquashStatus(squashed, refused, waiting.size + current.size)
  }

  // A failed read is logged, and the type is looked at again in the next run (AR7).
  private def candidates(target: SquashTarget): Seq[(SquashTarget, AggregateId, AggregateVersion)] = {
    val from = after.getOrElse(target.aggregateType, AggregateId(Long.MinValue))
    Try(eventStore.squashCandidates(target.aggregateType, target.policy, from, options.perRun)) match {
      case Success(found) =>
        after = if (found.size < options.perRun) after - target.aggregateType else after + (target.aggregateType -> found.last._1)
        found.map { case (id, upTo) => (target, id, upTo) }
      case Failure(e) =>
        log.error(e, "Looking for " + target.aggregateType.simpleName + " aggregates to squash failed; the next run tries again")
        Nil
    }
  }

  private def next(): Unit = {
    current.foreach(_._2.cancel())
    current = waiting.headOption.map { case (target, id, upTo) =>
      target.commandBus ! SquashAggregate(id, upTo, metadata = Some(metadata))
      (id, context.system.scheduler.scheduleOnce(options.answerTimeout, self, NotAnswered(id)))
    }
    waiting = waiting.drop(1)
  }
}
