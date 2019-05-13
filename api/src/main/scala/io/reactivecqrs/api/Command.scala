package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, SagaId, UserId}

//
//// First Command


abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
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

trait CommandIdempotencyId {
  def asDbKey: String
}

case class SagaStep(sagaId: SagaId, step: Int) extends CommandIdempotencyId {
  override def asDbKey: String = sagaId.asLong+"|"+step
}

trait IdempotentCommand[IID <: CommandIdempotencyId] {
  val idempotencyId: Option[IID]
}
