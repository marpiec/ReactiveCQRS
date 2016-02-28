package io.reactivecqrs.api

import _root_.io.reactivecqrs.api.id.AggregateId


// Command handling default response

sealed abstract class CommandResponse

case class SuccessResponse(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResponse
case class FailureResponse(exceptions: List[String]) extends CommandResponse
case class AggregateConcurrentModificationError(aggregateType: AggregateType, aggregateId: AggregateId, expected: AggregateVersion, was: AggregateVersion) extends CommandResponse
