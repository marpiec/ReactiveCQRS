package io.reactivecqrs.api

import io.reactivecqrs.api.CommandHandlerP.CommandHandlerF
import io.reactivecqrs.api.id.AggregateId


case class GetAggregate(id: AggregateId)


abstract class AggregateCommandBus[AGGREGATE_ROOT] {
  var commandsHandlers: Vector[CommandHandlerF[AGGREGATE_ROOT]] = Vector()

  //protected def addCommandHandler(handler: AbstractCommand[AGGREGATE_ROOT, _ <: Any] => _ <: CommandHandlingResult[Any]) {
  protected def addCommandHandler[COMMAND <: AbstractCommand[_,_]](handler: COMMAND => _ <: CommandHandlingResult[Any]) {
    commandsHandlers = commandsHandlers :+ handler.asInstanceOf[CommandHandlerF[AGGREGATE_ROOT]]
  }

  val eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, _]]
}