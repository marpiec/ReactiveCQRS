package io.reactivecqrs.api

import _root_.io.reactivecqrs.api.id.AggregateId


// Command handling default response

sealed abstract class CommandResponse(val success: Boolean)

case class SuccessResponse(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResponse(true)
case class FailureResponse(reason: String) extends CommandResponse(false)

