package io.reactivecqrs.api

import io.reactivecqrs.api.id.{CommandId, AggregateId}


// Command handling default response

object Nothing {
  val empty = Nothing()
}
case class Nothing()

sealed abstract class CustomCommandResponse[INFO]

case class SuccessResponse(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResponse
case class CustomSuccessResponse[INFO](aggregateId: AggregateId, aggregateVersion: AggregateVersion, info: INFO) extends CustomCommandResponse[INFO]
case class FailureResponse(exceptions: List[String]) extends CustomCommandResponse[Nothing]
case class AggregateConcurrentModificationError(aggregateId: AggregateId,
                                                aggregateType: AggregateType,
                                                expected: AggregateVersion,
                                                was: AggregateVersion) extends CustomCommandResponse[Nothing]
case class CommandHandlingError(commandName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[Nothing]
case class EventHandlingError(eventName: String, errorId: String, commandId: CommandId) extends CustomCommandResponse[Nothing]

/** The command was not run: the aggregate's queue of waiting commands was full (serial command processing, R8).
  * Sending it again later may succeed. */
case class CommandRejected(aggregateId: AggregateId, reason: String) extends CustomCommandResponse[Nothing]

/** D77: the ephemeral aggregate is not on this node: the node restarted since it was made, or the command reached another
  * node than the one holding it. Its state is gone from here. */
case class EphemeralAggregateLost(aggregateId: AggregateId) extends CustomCommandResponse[Nothing]

/** D77: this node holds no ephemeral aggregate of that id: it was never made, was dropped while idle, or was
  * materialized (`Materialize`). */
case class EphemeralAggregateUnknown(aggregateId: AggregateId) extends CustomCommandResponse[Nothing]
