package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId


case class GetAggregate(id: AggregateId)

abstract class AggregateCommandBus[AGGREGATE_ROOT] {
  val initialState: AGGREGATE_ROOT


  type SingleHandler = Function1[_ <: Command[AGGREGATE_ROOT, Any], CommandHandlingResult[Any]]
  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, CommandHandlingResult[Any]]
  type CommandHandlerWrapper = Function[CommandHandler, CommandHandler]
  

//  var commandsHandlers: Vector[CommandHandlerF[AGGREGATE_ROOT]] = Vector()

  //protected def addCommandHandler(handler: AbstractCommand[AGGREGATE_ROOT, _ <: Any] => _ <: CommandHandlingResult[Any]) {
//  protected def addCommandHandler[COMMAND <: AbstractCommand[_,_]](handler: COMMAND => _ <: CommandHandlingResult[Any]) {
//    commandsHandlers = commandsHandlers :+ handler.asInstanceOf[CommandHandlerF[AGGREGATE_ROOT]]
//  }

  val eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, _]]


  def commandHandlers: CommandHandler//PartialFunction[Any, _ >: AbstractCommand[AGGREGATE_ROOT] => CommandHandlingResult[Any]]
}