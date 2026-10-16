package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}

case class GetAggregate(id: AggregateId)
case class GetAggregateMinVersion(id: AggregateId, version: AggregateVersion, maxMillis: Int)
case class GetAggregateForVersion(id: AggregateId, version: AggregateVersion)
case class GetAggregateAtInstant(id: AggregateId, instant: Instant)

case class GetEventsForAggregate(id: AggregateId)
case class GetEventsForAggregateForVersion(id: AggregateId, version: AggregateVersion)

/** Asks a command bus for a new aggregate id, for a `FirstCommand` that names its aggregate in `chosenAggregateId`.
  * The reply is an `AggregateId`, taken from the same pool as the ids the bus assigns itself. */
case object ReserveAggregateId

case class SimulateEvent[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

/**
  * Performance snapshots of an aggregate's state (D30): the aggregate's owner stores its state every `every` events,
  * and when it stops if `onPassivation`, so a restore reads the snapshot and only the events after it. Snapshots
  * never change the history or the versions, and are thrown away when they could be wrong: after an undo or a history
  * rewrite, and when `schemaVersion` or the context's `version` changes. Change `schemaVersion` when the state class
  * changes shape.
  */
case class SnapshotPolicy(every: Int = 200, onPassivation: Boolean = true, schemaVersion: Int = 1) {
  require(every > 0, "every must be positive")
}

/** How the owner of an aggregate runs its commands. */
sealed trait CommandProcessing

object CommandProcessing {
  /** Commands of one aggregate run side by side, each on the state it found; a command whose events would follow
    * another's fails with `AggregateConcurrentModificationError`, and a `ConcurrentCommand` runs again (D37). */
  case object Concurrent extends CommandProcessing

  /** Commands of one aggregate run one at a time, in the order they arrived: each starts when the one before it was
    * answered, so every handler sees the latest state and nothing conflicts (R8, D31). At most
    * `maxQueuePerAggregate` commands wait; one more is answered at once as `onOverflow` says. */
  case class Serial(maxQueuePerAggregate: Int = 1000, onOverflow: OverflowPolicy = OverflowPolicy.Reject) extends CommandProcessing {
    require(maxQueuePerAggregate > 0, "maxQueuePerAggregate must be positive")
  }
}

sealed trait OverflowPolicy

object OverflowPolicy {
  /** The command is not run; the caller gets `CommandRejected` and may send it again later. */
  case object Reject extends OverflowPolicy
}

/** An aggregate type's behaviour. Events are stored under their class name; an event class that must change shape is
  * kept, deprecated, and a new class (`...V2`) takes its place (D29: there is no event versioning). */
abstract class AggregateContext[AGGREGATE_ROOT : ClassTag] {

  protected implicit def future2AsyncResult[T](future: Future[CustomCommandResult[T]]): AsyncCommandResult[T] = {
    AsyncCommandResult(future)
  }

  def initialAggregateRoot: AGGREGATE_ROOT

  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]]

  type EventHandler = (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]

  type RewriteHistoryCommandHandler = (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]]

  def eventHandlers: EventHandler

  def commandHandlers: CommandHandler

  def rewriteHistoryCommandHandlers: RewriteHistoryCommandHandler = (events, AGGREGATE_ROOT) => {
    case _ => throw new IllegalStateException("Please implement rewriteHistoryCommandHandlers in AggregateContext "+this.getClass.getSimpleName)
  }

  val version: Int

  /** How long a command may take until the caller gets a `CommandHandlingError` (R5). */
  val commandTimeout: FiniteDuration = 60.seconds

  /** Opt-in performance snapshots (D30, R10); none by default. */
  val snapshots: Option[SnapshotPolicy] = None

  /** One command at a time per aggregate (`Serial`, R8), or side by side (`Concurrent`, the default). */
  val commandProcessing: CommandProcessing = CommandProcessing.Concurrent

  val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

}
