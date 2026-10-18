package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import org.apache.pekko.util.Version

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

// The reads of an aggregate, sent to its type's command bus. Each is answered with a `Try`.

/** The aggregate's current state: `Success(Aggregate(id, version, root))`, from its owner. `root` is `None` before the
  * aggregate exists and after it was deleted. */
case class GetAggregate(id: AggregateId)
/** The aggregate's state as soon as it reaches `version`, for reading your own write: answered at once if it is there,
  * otherwise when it gets there, or with `Failure(AggregateInIncorrectVersionException)` after `maxMillis`. */
case class GetAggregateMinVersion(id: AggregateId, version: AggregateVersion, maxMillis: Int)
/** The aggregate's state at `version`, restored from the store; `Failure(AggregateSquashedException)` below its squash
  * point. */
case class GetAggregateForVersion(id: AggregateId, version: AggregateVersion)
/** The aggregate's state at `instant`, restored from the store; `Failure(AggregateSquashedException)` before its squash
  * event's time. */
case class GetAggregateAtInstant(id: AggregateId, instant: Instant)

/** Every event of the aggregate, oldest first, undone ones included: `Success(List[EventInfo])`. A duplicate's list
  * starts with its base's events. The whole history comes in one message. */
case class GetEventsForAggregate(id: AggregateId)

/** The events of the aggregate that squashes kept in the archive (R11), oldest first; answered `Success(ArchivedEvents)`. */
case class GetArchivedEvents(id: AggregateId)
/** An archived event as it was stored, `undoneFrom` the version of the undo that cancelled it, if one did, and when the
  * squash archived it. */
case class ArchivedEvent[AGGREGATE_ROOT](info: EventInfo[AGGREGATE_ROOT], undoneFrom: Option[AggregateVersion], archived: Instant)
/** The answer to `GetArchivedEvents`. */
case class ArchivedEvents[AGGREGATE_ROOT](id: AggregateId, events: Seq[ArchivedEvent[AGGREGATE_ROOT]])
/** The aggregate's events up to `version`, oldest first: `Success(List[EventInfo])`; `Failure(AggregateSquashedException)`
  * below its squash point. */
case class GetEventsForAggregateForVersion(id: AggregateId, version: AggregateVersion)

/** Asks a command bus for a new aggregate id, for a `FirstCommand` that names its aggregate in `chosenAggregateId`.
  * The reply is an `AggregateId`, taken from the same pool as the ids the bus assigns itself. */
case object ReserveAggregateId

/** Replaces the aggregate's events 1..`upTo` with one squash event at `upTo` (D47); the version does not change. Answered
  * with `AggregateSquashed` or `SquashRefused`. `userId` and `metadata` are stored with the squash event. */
case class SquashAggregate(id: AggregateId, upTo: AggregateVersion, userId: UserId = UserId(0), metadata: Option[EventMetadata] = None)
/** The answer to `SquashAggregate`. */
sealed trait SquashResponse
/** The aggregate's events 1..`upTo` are one squash event now. */
case class AggregateSquashed(id: AggregateId, upTo: AggregateVersion) extends SquashResponse
/** Nothing was changed, for `reason`: squash is not enabled for the type; the version is out of range, squashed
  * already, or deleted; a squash event, the aggregate's or one that detaches a duplicate, does not give back the state
  * it replaces, or its event handler fails (D85); a precondition of D51 does not hold now (rows still wait in the
  * outbox, a subscription is failed, a later undo reaches the squash point); or a duplicate was made meanwhile. The last
  * two may pass later. */
case class SquashRefused(id: AggregateId, reason: String) extends SquashResponse

/** A change of what an aggregate's history holds (D76), as data: for example `EraseCustomer(customerId)`. The contexts
  * say what it does to their events (`historyChanges`, `metadataChanges`). */
trait HistoryChange extends Serializable {
  /** F245: what the change does to the stored JSON of an event the store cannot read (a class that changed shape, a row
    * repaired by hand), so that erasure still reaches it: `Some(json)` with the value erased in the text, or the same
    * text when it holds none. `None`, the default, says the change cannot tell, and the rewrite is refused. */
  def inUnreadableEvent(json: String): Option[String] = None
}

/** Applies `change` to every stored event of the aggregate, its archived events included, in its owner, between commands
  * (D76): the events the change alters are written again, the version stays and nothing is published. The aggregate's
  * copies and the projections are the host's to follow, with `HistoryRewriter`. Answered with `HistoryRewritten` or
  * `RewriteRefused`; a second request with the same change finds nothing to change. */
case class RewriteEverywhere(id: AggregateId, change: HistoryChange, userId: UserId)
/** The answer to `RewriteEverywhere`. */
sealed trait RewriteResponse
/** `events` and `archived` events were written again; `fields`: the names of the fields the change altered;
  * `unreadable`: of them, events the store could not read, written again as `HistoryChange.inUnreadableEvent` gave their
  * JSON (F245). */
case class HistoryRewritten(id: AggregateId, events: Int, archived: Int, fields: Set[String], unreadable: Int = 0) extends RewriteResponse
/** Nothing was written, for `reason`: the change alters an event's class, an undo or a duplication event, throws, or
  * makes an event larger than the store allows; or an event cannot be read and the change has no form for it. */
case class RewriteRefused(id: AggregateId, reason: String) extends RewriteResponse

/** D77: stores an ephemeral aggregate (one of an ephemeral command bus, whose ids are negative) as a new persistent
  * aggregate, whose first event is the context's squash event of its state; the ephemeral one is then dropped. Sent to
  * the ephemeral bus; answered `Materialized` with the new id, or `MaterializeRefused` (it stays): without a squash
  * policy, while commands run on it, or when the persistent store refuses the event. */
case class Materialize(id: AggregateId, userId: UserId)
/** The answer to `Materialize`. */
sealed trait MaterializeResponse
/** The ephemeral aggregate `id` is stored as `persistentId`, and dropped. */
case class Materialized(id: AggregateId, persistentId: AggregateId) extends MaterializeResponse
/** Nothing was stored, for `reason`; the ephemeral aggregate stays. */
case class MaterializeRefused(id: AggregateId, reason: String) extends MaterializeResponse

/** D77: the idempotency key of a materialization, so that one ephemeral aggregate is never stored twice. */
case class MaterializationKey(ephemeralId: AggregateId) extends CommandIdempotencyId {
  override def asDbKey: String = "materialize|" + ephemeralId.asLong
}

/** D77: writes `event` as the first event of a new aggregate, as given, without the context's command handlers: what a
  * materialization sends the persistent command bus. Idempotent under its key. */
case class StoreFirstEvent[AGGREGATE_ROOT](userId: UserId, event: FirstEvent[AGGREGATE_ROOT], idempotencyId: Option[MaterializationKey])
  extends FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]] with IdempotentCommand[MaterializationKey]

/**
  * Squash (D47, D48), opt-in per context: `toEvent` makes the context's squash event from the aggregate's state at the
  * squash point. It is an ordinary event class of the context, a `FirstEvent` whose event handler gives that state back,
  * evolved like any other (D29); the history never holds raw state. Every squash checks it (D85): the event handlers
  * apply the event to the initial state, as a restore will, and the result must equal (`==`) the state it replaces, or
  * the squash is refused; see `SquashMayChangeState` for an event that may differ on purpose. `automatic`: the squash
  * scheduler squashes the context's aggregates by this rule (D52). `keep`: the classes of audit events a squash moves to
  * the aggregate's archive instead of deleting them (R11), readable with `GetArchivedEvents`.
  */
case class SquashPolicy[AGGREGATE_ROOT](toEvent: AGGREGATE_ROOT => FirstEvent[AGGREGATE_ROOT], automatic: Option[AutomaticSquash] = None,
                                        keep: Set[Class[_]] = Set.empty)

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
  * squashed yet; it is squashed up to the version before those `keepLast`. With `keepNewerThan`, the events younger than
  * that stay too (R11): it is squashed up to its newest version older than that, if that leaves `afterEvents` to squash. */
case class AutomaticSquash(afterEvents: Int, keepLast: Int = 0, keepNewerThan: Option[FiniteDuration] = None) {
  require(afterEvents > 0 && keepLast >= 0, "afterEvents must be positive and keepLast not negative")
}

/**
  * Performance snapshots of an aggregate's state (D30): the aggregate's owner stores its state every `every` events,
  * and when it stops if `onPassivation`, so a restore reads the snapshot and only the events after it. Snapshots
  * never change the history or the versions, and are thrown away when they could be wrong: after an undo or a history
  * rewrite, and when `schemaVersion` or the context's `version` changes. A snapshot of another shape of the state class
  * (its fields' names and types, F196) is not used either; change `schemaVersion` when a field changes its meaning.
  */
case class SnapshotPolicy(every: Int = 200, onPassivation: Boolean = true, schemaVersion: Int = 1) {
  require(every > 0, "every must be positive")
}

/** How the owner of an aggregate runs its commands. */
sealed trait CommandProcessing

/** The ways an owner runs its aggregate's commands. */
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

/** What happens to a command that finds its aggregate's queue full (`CommandProcessing.Serial`). */
sealed trait OverflowPolicy

/** The overflow policies. */
object OverflowPolicy {
  /** The command is not run; the caller gets `CommandRejected` and may send it again later. */
  case object Reject extends OverflowPolicy
}

/** Where the owners of an aggregate type run (D43). */
sealed trait Placement

/** The placements of owners. */
object Placement {
  /** Each node keeps owners of its own for the aggregates it is asked about; between nodes, the store's version check
    * decides which command wins. For aggregates of one node (ephemeral ones) and for moving to `Sharded` gradually. */
  case object Local extends Placement

  /** One owner of each aggregate in the cluster, placed by Cluster Sharding by aggregate id: commands and queries reach it
    * from any node. The actor system must be a cluster (`pekko.actor.provider = cluster`) of one node or more. The shard
    * passivates idle owners (`pekko.cluster.sharding.passivation`). About ten shards per node of the largest cluster. */
  case class Sharded(numberOfShards: Int = 100) extends Placement {
    require(numberOfShards > 0, "numberOfShards must be positive")
  }
}

/** An aggregate type's behaviour: what its commands decide (`commandHandlers`) and what its events do to its state
  * (`eventHandlers`), with the type's options. One context per aggregate type, given to the type's command bus
  * (`AggregateCommandBusActor`). Events are stored under their class name; an event class that must change shape is
  * kept, deprecated, and a new class (`...V2`) takes its place (D29: there is no event versioning). */
abstract class AggregateContext[AGGREGATE_ROOT : ClassTag] {

  /** Lets a command handler return a `Future` of its decision (when it must ask someone first): the owner waits for it,
    * at most `commandTimeout`. */
  protected implicit def future2AsyncResult[T](future: Future[CustomCommandResult[T]]): AsyncCommandResult[T] = {
    AsyncCommandResult(future)
  }

  /** The state before the first event: what the first event's handler receives. */
  def initialAggregateRoot: AGGREGATE_ROOT

  /** Given the aggregate's current state, decides each command: events to store (`CommandSuccess`), a refusal
    * (`CommandFailure`), or a `Future` of either. It runs off the owner's thread and must not change anything itself. */
  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]]

  /** Given who stored an event, when, and the state before it, returns the state after the event (`null` deletes the
    * aggregate). It must be a pure function of these: it runs again at every restore. */
  type EventHandler = (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]

  /** Given the stored events of the classes a history-rewriting command names, and the current state, decides that
    * command: `RewriteCommandSuccess` with the events changed. */
  type RewriteHistoryCommandHandler = (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]]

  /** The type's event handlers: a case for each event class it stores, undo events excepted (F148). */
  def eventHandlers: EventHandler

  /** The type's command handlers: a case for each command class it takes. */
  def commandHandlers: CommandHandler

  /** The handlers of the type's history-rewriting commands (`RewriteHistoryCommand`); none by default. */
  def rewriteHistoryCommandHandlers: RewriteHistoryCommandHandler = (events, AGGREGATE_ROOT) => {
    case _ => throw new IllegalStateException("Please implement rewriteHistoryCommandHandlers in AggregateContext "+this.getClass.getSimpleName)
  }

  /** The version of the context's behaviour. Raise it when the event handlers change what a history gives: snapshots of
    * the older version are thrown away, and the managed projections that receive its states are built again (D63). */
  val version: Int

  /** How long a command may take until the caller gets a `CommandHandlingError` (R5). */
  val commandTimeout: FiniteDuration = 60.seconds

  /** Opt-in performance snapshots (D30, R10); none by default. */
  val snapshots: Option[SnapshotPolicy] = None

  /** One command at a time per aggregate (`Serial`, R8), or side by side (`Concurrent`, the default). */
  val commandProcessing: CommandProcessing = CommandProcessing.Concurrent

  /** Opt-in squash of old history into one event (D47–D52); none by default. */
  val squash: Option[SquashPolicy[AGGREGATE_ROOT]] = None

  /** What a `HistoryChange` does to this type's events (D76, `RewriteEverywhere`): each event it matches is written again
    * as it returns it, of the same class (D17); undo and duplication events must not change. It must be idempotent: run
    * twice, it changes nothing the second time. None by default. */
  def historyChanges: HistoryChange => PartialFunction[Event[AGGREGATE_ROOT], Event[AGGREGATE_ROOT]] = _ => PartialFunction.empty

  /** What a `HistoryChange` does to the metadata stored with this type's events (D32), when its `actor` or `custom` values
    * may hold what the change removes. None by default. */
  def metadataChanges: HistoryChange => PartialFunction[EventMetadata, EventMetadata] = _ => PartialFunction.empty

  /** Owners on each node (`Local`, the default) or one owner per aggregate in the cluster (`Sharded`, D43). */
  val placement: Placement = Placement.Local

  /** D67: event classes added in a rolling upgrade, each with the app-version (`pekko.cluster.app-version`) from which
    * every node reads it. One is persisted only once every node of the cluster runs that version or a later one; a command
    * that would write it earlier is answered with `CommandRejected`. Handlers write the older event meanwhile, asking
    * `everyNodeRuns(version)`. Do not roll back past a version whose new classes were written. */
  val writtenFrom: Map[Class[_], String] = Map.empty

  // The lowest app-version among the cluster's members, kept here by the node's command buses; None when the system is
  // not a cluster or no member set an app-version (all at 0.0.0): then every class is written.
  @volatile private[reactivecqrs] var oldestAppVersion: Option[Version] = None

  /** Whether every node of the cluster runs `version` or a later one (always, outside a cluster or without app-versions). */
  protected final def everyNodeRuns(version: String): Boolean = oldestAppVersion.forall(_.compareTo(Version(version)) >= 0)

  /** Why `event` may not be written yet (D67). */
  private[reactivecqrs] def notWrittenYet(event: Any): Option[String] =
    writtenFrom.get(event.getClass).filterNot(everyNodeRuns).map(version =>
      event.getClass.getSimpleName + " is written once every node runs version " + version + " or later")

  /** The type's key, from the root class (`AggregateType.of`). */
  val aggregateType: AggregateType = AggregateType.of[AGGREGATE_ROOT]

  /** The root's class, under whose name its states are stored (snapshots, D30). */
  val aggregateRootClass: Class[_] = implicitly[ClassTag[AGGREGATE_ROOT]].runtimeClass

}
