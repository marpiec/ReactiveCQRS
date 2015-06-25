package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

// Command handling result

sealed abstract class CommandResult[+RESPONSE]


case class Success[AGGREGATE_ROOT, RESPONSE](events: Seq[Event[AGGREGATE_ROOT]], response: (AggregateId, AggregateVersion) => RESPONSE)
  extends CommandResult[RESPONSE]

case class Failure[AGGREGATE_ROOT, RESPONSE](response: RESPONSE)
  extends CommandResult[RESPONSE]



object Success {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):Success[AGGREGATE_ROOT, CommandResponse] =
    new Success(List(event), (aggregateId, version) => SuccessResponse(aggregateId, version))

  def apply[AGGREGATE_ROOT, RESPONSE](event: Event[AGGREGATE_ROOT], response: (AggregateId, AggregateVersion) => RESPONSE) =
    new Success(List(event), response)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):Success[AGGREGATE_ROOT, CommandResponse] =
    new Success(events, (aggregateId, version) => SuccessResponse(aggregateId, version))
}


