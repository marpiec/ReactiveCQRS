package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

// Command handling result

sealed abstract class CommandResult[+RESPONSE <: CommandResponse]


case class CommandSuccess[AGGREGATE_ROOT, RESPONSE <: CommandResponse](events: Seq[Event[AGGREGATE_ROOT]], response: (AggregateId, AggregateVersion) => RESPONSE)
  extends CommandResult[RESPONSE]

case class CommandFailure[AGGREGATE_ROOT, RESPONSE <: CommandResponse](response: RESPONSE)
  extends CommandResult[RESPONSE]



object CommandSuccess {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):CommandSuccess[AGGREGATE_ROOT, CommandResponse] =
    new CommandSuccess(List(event), (aggregateId, version) => SuccessResponse(aggregateId, version))

  def apply[AGGREGATE_ROOT, RESPONSE <: CommandResponse](event: Event[AGGREGATE_ROOT], response: (AggregateId, AggregateVersion) => RESPONSE) =
    new CommandSuccess(List(event), response)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):CommandSuccess[AGGREGATE_ROOT, CommandResponse] =
    new CommandSuccess(events, (aggregateId, version) => SuccessResponse(aggregateId, version))
}


object CommandFailure {
  def apply[AGGREGATE_ROOT](exceptions: List[String]) = new CommandFailure[AGGREGATE_ROOT, CommandResponse](FailureResponse(exceptions))
  def apply[AGGREGATE_ROOT](exception: String) = new CommandFailure[AGGREGATE_ROOT, CommandResponse](FailureResponse(List(exception)))
}