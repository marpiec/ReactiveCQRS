package io.reactivecqrs.api

import io.reactivecqrs.api.id.{CommandId, AggregateId}


object NoInfo {
  /** The one value of `NoInfo`. */
  val empty: NoInfo = NoInfo()
}
/** The information of a command result that carries none: the owner answers such a success with `SuccessResponse`.
  * Named `Nothing` before 1.0, which hid `scala.Nothing` after `import io.reactivecqrs.api._` (F165). */
case class NoInfo()

/** What the sender of a command gets: a success, a refusal or an error. `INFO`: what a success carries besides the
  * aggregate's id and version. */
sealed abstract class CustomCommandResponse[INFO]

/** The command succeeded: the aggregate `aggregateId` is at `aggregateVersion` now. */
case class SuccessResponse(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResponse
/** The command succeeded, and its handler answered `info` too (`CommandSuccess(event, info)`). */
case class CustomSuccessResponse[INFO](aggregateId: AggregateId, aggregateVersion: AggregateVersion, info: INFO) extends CustomCommandResponse[INFO]
/** The command's handler refused it (`CommandFailure`), for these reasons; nothing was stored. */
case class FailureResponse(exceptions: List[String]) extends CustomCommandResponse[NoInfo]
/** The command expected the aggregate at version `expected`, and it `was` at another: nothing was stored. A
  * `ConcurrentCommand` gets it only after its retries; an idempotent copy whose original was applied gets the
  * original's response instead. */
case class AggregateConcurrentModificationError(aggregateId: AggregateId,
                                                aggregateType: AggregateType,
                                                expected: AggregateVersion,
                                                was: AggregateVersion) extends CustomCommandResponse[NoInfo]
/** The command's handler threw, or took longer than the context's `commandTimeout`: nothing was stored. The log has
  * the error under `errorId`. */
case class CommandHandlingError(commandName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[NoInfo]
/** An event of the command could not be applied (its handler threw) or stored: nothing was stored. The log has the
  * error under `errorId`. */
case class EventHandlingError(eventName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[NoInfo]

/** The command was not run, for `reason`, and sending it again later may succeed: the aggregate's queue of waiting
  * commands is full (serial command processing, R8), it would store an event class that not every node reads yet
  * (`writtenFrom`, D67), or, for ephemeral aggregates (D77), the aggregate is being materialized or the node holds as
  * many as it may. */
case class CommandRejected(aggregateId: AggregateId, reason: String) extends CustomCommandResponse[NoInfo]

/** D77: the ephemeral aggregate is not on this node: the node restarted since it was made, or the command reached another
  * node than the one holding it. Its state is gone from here. */
case class EphemeralAggregateLost(aggregateId: AggregateId) extends CustomCommandResponse[NoInfo]

/** D77: this node holds no ephemeral aggregate of that id: it was never made, was dropped while idle, or was
  * materialized (`Materialize`). */
case class EphemeralAggregateUnknown(aggregateId: AggregateId) extends CustomCommandResponse[NoInfo]
