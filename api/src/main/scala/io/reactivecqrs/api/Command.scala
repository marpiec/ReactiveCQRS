package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, SagaId, UserId}

//
//// First Command


abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId

  /** The id of the new aggregate, when the caller chose it (from `ReserveAggregateId`) so that sending the command
    * again cannot create a second aggregate; the copy gets `AggregateConcurrentModificationError`, or the stored
    * response if the command is idempotent. `None` lets the command bus assign the id. */
  def chosenAggregateId: Option[AggregateId] = None
}

abstract class ConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
}

abstract class Command[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
  val expectedVersion: AggregateVersion
}

abstract class RewriteHistoryCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
  val expectedVersion: AggregateVersion
  def eventsTypes: Set[Class[_]]
}


abstract class RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
  def eventsTypes: Set[Class[_]]
}

trait CommandIdempotencyId {
  def asDbKey: String
}

case class SagaStep(sagaId: SagaId, step: Int) extends CommandIdempotencyId {
  override def asDbKey: String = s"${sagaId.asLong}|$step"
}

/** A command that takes effect once per idempotency key, however often or concurrently it is sent: the response of
  * the first copy is stored with its events, and every other copy gets that response. Keys are global across
  * aggregate types; stored responses are kept for the response state's retention period. */
trait IdempotentCommand[IID <: CommandIdempotencyId] {
  val idempotencyId: Option[IID]
}

/** A command that gives every event it stores this metadata envelope (R13, D32). Event handlers do not see it;
  * listeners get it with each event (`EventInfo.metadata`). */
trait WithMetadata {
  def metadata: EventMetadata
}
