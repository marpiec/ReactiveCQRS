package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, SagaId, UserId}

// The kinds of commands. A command is a case class extending one of them, sent to its aggregate type's command bus;
// its kind tells the bus how to route it and the owner how to check its version. `RESPONSE`: what the sender gets.

/** A command that creates an aggregate; its handler returns a `FirstEvent`. The command bus gives the new aggregate an
  * id, unless the command names one. */
abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  /** Who sends it; stored with its events. */
  val userId: UserId

  /** The id of the new aggregate, when the caller chose it (from `ReserveAggregateId`) so that sending the command
    * again cannot create a second aggregate; the copy gets `AggregateConcurrentModificationError`, or the stored
    * response if the command is idempotent. `None` lets the command bus assign the id. */
  def chosenAggregateId: Option[AggregateId] = None
}

/** A command for an aggregate at whatever version it is: its handler sees the latest state, and if another command
  * commits first, it runs again on the new state (D37), up to 10 times. For changes that do not depend on what the
  * sender saw. */
abstract class ConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  /** Who sends it; stored with its events. */
  val userId: UserId
  /** The aggregate it is for. */
  val aggregateId: AggregateId
}

/** A command for an aggregate at `expectedVersion`, the version its sender saw: if the aggregate is at another, the
  * command is refused with `AggregateConcurrentModificationError` and nothing runs (optimistic locking). */
abstract class Command[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  /** Who sends it; stored with its events. */
  val userId: UserId
  /** The aggregate it is for. */
  val aggregateId: AggregateId
  /** The version the sender saw. */
  val expectedVersion: AggregateVersion
}

/** A `Command` that changes stored events of the classes `eventsTypes` (`AggregateContext.rewriteHistoryCommandHandlers`
  * receives them and returns `RewriteCommandSuccess`). */
abstract class RewriteHistoryCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  /** Who sends it; stored with its events. */
  val userId: UserId
  /** The aggregate it is for. */
  val aggregateId: AggregateId
  /** The version the sender saw. */
  val expectedVersion: AggregateVersion
  /** The classes of the events it may change. */
  def eventsTypes: Set[Class[_]]
}

/** A `RewriteHistoryCommand` for the aggregate at whatever version it is, as a `ConcurrentCommand` is. */
abstract class RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  /** Who sends it; stored with its events. */
  val userId: UserId
  /** The aggregate it is for. */
  val aggregateId: AggregateId
  /** The classes of the events it may change. */
  def eventsTypes: Set[Class[_]]
}

/** The idempotency key of an `IdempotentCommand`: the same key means the same command. */
trait CommandIdempotencyId {
  /** The key as the store keeps it; keys are global across aggregate types, so make them unique, with a prefix of your
    * own if several kinds of keys exist. */
  def asDbKey: String
}

/** The key of a saga's step: the commands a step sends carry it, so a step that runs again after a crash does not
  * apply them twice. */
case class SagaStep(sagaId: SagaId, step: Int) extends CommandIdempotencyId {
  override def asDbKey: String = s"${sagaId.asLong}|$step"
}

/** A command that takes effect once per idempotency key, however often or concurrently it is sent: the response of
  * the first copy is stored with its events, and every other copy gets that response. Keys are global across
  * aggregate types; stored responses are kept for the response state's retention period. */
trait IdempotentCommand[IID <: CommandIdempotencyId] {
  /** The key; `None` makes this command an ordinary one. */
  val idempotencyId: Option[IID]
}

/** A command that gives every event it stores this metadata envelope (R13, D32). Event handlers do not see it;
  * listeners get it with each event (`EventInfo.metadata`). */
trait WithMetadata {
  def metadata: EventMetadata
}
