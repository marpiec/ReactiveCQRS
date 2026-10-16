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

case class EventTypeVersion(eventType: String, version: Short)

case class EventVersion[AGGREGATE_ROOT](eventBaseType: String, mapping: List[EventTypeVersion])

abstract class AggregateContext[AGGREGATE_ROOT : ClassTag] {

  protected implicit def future2AsyncResult[T](future: Future[CustomCommandResult[T]]): AsyncCommandResult[T] = {
    AsyncCommandResult(future)
  }

  protected def EV[EVENT_BASE <: Event[AGGREGATE_ROOT] : ClassTag](versionedType: (Int, Class[_ <: Event[AGGREGATE_ROOT]])*) = {
    EventVersion[AGGREGATE_ROOT](classTag[EVENT_BASE].toString, versionedType.map(vt => EventTypeVersion(vt._2.getTypeName, vt._1.toShort)).toList)
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

  val eventsVersions: List[EventVersion[AGGREGATE_ROOT]] = List.empty

  val version: Int

  /** How long a command may take until the caller gets a `CommandHandlingError` (R5). */
  val commandTimeout: FiniteDuration = 60.seconds

  val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

}