package io.reactivecqrs.api

import _root_.io.reactivecqrs.api.guid.AggregateId

import scala.reflect._


// Command handler

abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: AbstractCommand[AGGREGATE_ROOT, RESPONSE] : ClassTag, RESPONSE] {
  val commandClassName = classTag[COMMAND].runtimeClass.getName
  def handle(aggregateId: AggregateId, command: COMMAND): CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]
}

// Command handling default response

case class CommandResult(aggregateId: AggregateId, aggregateVersion: AggregateVersion)

// Command handling result

abstract class CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]


case class Success[AGGREGATE_ROOT, RESPONSE](events: Seq[Event[AGGREGATE_ROOT]], response: (AggregateId, AggregateVersion) => RESPONSE)
  extends CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]

case class Failure[AGGREGATE_ROOT, RESPONSE](response: RESPONSE)
  extends CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]

object Success {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):Success[AGGREGATE_ROOT, CommandResult] =
    new Success(List(event), (aggregateId, version) => CommandResult(aggregateId, version))

  def apply[AGGREGATE_ROOT, RESPONSE](event: Event[AGGREGATE_ROOT], response: (AggregateId, AggregateVersion) => RESPONSE) =
    new Success(List(event), response)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):Success[AGGREGATE_ROOT, CommandResult] =
    new Success(events, (aggregateId, version) => CommandResult(aggregateId, version))
}


