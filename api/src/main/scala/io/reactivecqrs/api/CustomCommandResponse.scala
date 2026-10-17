package io.reactivecqrs.api

import io.reactivecqrs.api.id.{CommandId, AggregateId}


/** The information of a command result that carries none: the owner answers such a success with `SuccessResponse`.
  * Named `Nothing` before 1.0, which hid `scala.Nothing` after `import io.reactivecqrs.api._` (F165). */
object NoInfo {
  val empty: NoInfo = NoInfo()
}
case class NoInfo()

sealed abstract class CustomCommandResponse[INFO]

case class SuccessResponse(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResponse
case class CustomSuccessResponse[INFO](aggregateId: AggregateId, aggregateVersion: AggregateVersion, info: INFO) extends CustomCommandResponse[INFO]
case class FailureResponse(exceptions: List[String]) extends CustomCommandResponse[NoInfo]
case class AggregateConcurrentModificationError(aggregateId: AggregateId,
                                                aggregateType: AggregateType,
                                                expected: AggregateVersion,
                                                was: AggregateVersion) extends CustomCommandResponse[NoInfo]
case class CommandHandlingError(commandName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[NoInfo]
case class EventHandlingError(eventName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[NoInfo]

/** The command was not run: the aggregate's queue of waiting commands was full (serial command processing, R8).
  * Sending it again later may succeed. */
case class CommandRejected(aggregateId: AggregateId, reason: String) extends CustomCommandResponse[NoInfo]

/** D77: the ephemeral aggregate is not on this node: the node restarted since it was made, or the command reached another
  * node than the one holding it. Its state is gone from here. */
case class EphemeralAggregateLost(aggregateId: AggregateId) extends CustomCommandResponse[NoInfo]

/** D77: this node holds no ephemeral aggregate of that id: it was never made, was dropped while idle, or was
  * materialized (`Materialize`). */
case class EphemeralAggregateUnknown(aggregateId: AggregateId) extends CustomCommandResponse[NoInfo]
