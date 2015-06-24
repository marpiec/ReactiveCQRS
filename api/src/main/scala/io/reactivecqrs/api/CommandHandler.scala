package io.reactivecqrs.api

import _root_.io.reactivecqrs.api.id.AggregateId

object CommandHandlerP {
  type CommandHandlerF[AGGREGATE_ROOT] = Command[AGGREGATE_ROOT, Any] => _ <: CommandHandlingResult[Any]

}


// Command handler


//abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: AbstractCommand[AGGREGATE_ROOT, RESPONSE] : ClassTag, RESPONSE] {
//  val commandClassName = classTag[COMMAND].runtimeClass.getName
//  def handle(aggregateId: AggregateId, command: COMMAND): CommandHandlingResult[RESPONSE]
//}

// Command handling default response

sealed abstract class CommandResult(val success: Boolean)

case class CommandSuccess(aggregateId: AggregateId, aggregateVersion: AggregateVersion) extends CommandResult(true)
case class CommandFailure(reason: String) extends CommandResult(false)

// Command handling result

abstract class CommandHandlingResult[+RESPONSE]


case class Success[AGGREGATE_ROOT, RESPONSE](events: Seq[Event[AGGREGATE_ROOT]], response: (AggregateId, AggregateVersion) => RESPONSE)
  extends CommandHandlingResult[RESPONSE]

case class Failure[AGGREGATE_ROOT, RESPONSE](response: RESPONSE)
  extends CommandHandlingResult[RESPONSE]

object Success {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):Success[AGGREGATE_ROOT, CommandResult] =
    new Success(List(event), (aggregateId, version) => CommandSuccess(aggregateId, version))

  def apply[AGGREGATE_ROOT, RESPONSE](event: Event[AGGREGATE_ROOT], response: (AggregateId, AggregateVersion) => RESPONSE) =
    new Success(List(event), response)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):Success[AGGREGATE_ROOT, CommandResult] =
    new Success(events, (aggregateId, version) => CommandSuccess(aggregateId, version))
}


