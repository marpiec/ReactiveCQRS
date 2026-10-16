package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

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

/** Replaces the aggregate's events 1..`upTo` with one squash event at `upTo` (D47); the version does not change. Answered
  * with `AggregateSquashed` or `SquashRefused`. `userId` and `metadata` are stored with the squash event. */
case class SquashAggregate(id: AggregateId, upTo: AggregateVersion, userId: UserId = UserId(0), metadata: Option[EventMetadata] = None)
sealed trait SquashResponse
case class AggregateSquashed(id: AggregateId, upTo: AggregateVersion) extends SquashResponse
/** Nothing was changed, for `reason`: squash is not enabled for the type; the version is out of range, squashed
  * already, or deleted; a squash event, the aggregate's or one that detaches a duplicate, does not give back the state
  * it replaces, or its event handler fails (D85); a precondition of D51 does not hold now (rows still wait in the
  * outbox, a subscription is failed, a later undo reaches the squash point); or a duplicate was made meanwhile. The last
  * two may pass later. */
case class SquashRefused(id: AggregateId, reason: String) extends SquashResponse

/**
  * Squash (D47, D48), opt-in per context: `toEvent` makes the context's squash event from the aggregate's state at the
  * squash point. It is an ordinary event class of the context, a `FirstEvent` whose event handler gives that state back,
  * evolved like any other (D29); the history never holds raw state. Every squash checks it (D85): the event handlers
  * apply the event to the initial state, as a restore will, and the result must equal (`==`) the state it replaces, or
  * the squash is refused; see `SquashMayChangeState` for an event that may differ on purpose. `automatic`: the squash
  * scheduler squashes the context's aggregates by this rule (D52).
  */
case class SquashPolicy[AGGREGATE_ROOT](toEvent: AGGREGATE_ROOT => FirstEvent[AGGREGATE_ROOT], automatic: Option[AutomaticSquash] = None)

/**
  * Mixed into a squash event class whose state may differ from the state it replaces, on purpose (D85): for example one
  * that leaves out data the aggregate no longer needs. Such a squash is not compared (its event handler must still
  * apply it), and since the history then gives another state than before: every snapshot of the aggregate and of the
  * duplicates reading it goes, and the owners of the aggregate and of all its duplicates read their states again.
  * Projections are not told (D51): they keep what they made from the events it replaced, and only a rebuild sees the
  * new state.
  */
trait SquashMayChangeState

/** An aggregate is squashed once at least `afterEvents` of its events are older than its newest `keepLast` and not
  * squashed yet; it is squashed up to the version before those `keepLast`. */
case class AutomaticSquash(afterEvents: Int, keepLast: Int = 0) {
  require(afterEvents > 0 && keepLast >= 0, "afterEvents must be positive and keepLast not negative")
}

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

  /** Opt-in squash of old history into one event (D47–D52); none by default. */
  val squash: Option[SquashPolicy[AGGREGATE_ROOT]] = None

  val aggregateType: AggregateType = AggregateType.of[AGGREGATE_ROOT]

  /** The root's class, under whose name its states are stored (snapshots, D30). */
  val aggregateRootClass: Class[_] = implicitly[ClassTag[AGGREGATE_ROOT]].runtimeClass

}
