package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

import scala.reflect.runtime.universe.TypeTag

case class GetAggregate(id: AggregateId)
case class GetAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class GetEventsForAggregate(id: AggregateId)
case class GetEventsForAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class SimulateEvent[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

abstract class AggregateContext[AGGREGATE_ROOT: TypeTag] {
  def initialAggregateRoot: AGGREGATE_ROOT


  type HandlerWrapper = (=> CustomCommandResult[Any]) => CustomCommandResult[CustomCommandResponse[_]]

  type SingleHandler = (_ <: Command[AGGREGATE_ROOT, CustomCommandResponse[Any]]) => CustomCommandResult[Any]
  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, CustomCommandResult[Any]]
  type CommandHandlerWrapper = Function[CommandHandler, CommandHandler]


  type EventHandler = AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT]

//  var commandsHandlers: Vector[CommandHandlerF[AGGREGATE_ROOT]] = Vector()

  //protected def addCommandHandler(handler: AbstractCommand[AGGREGATE_ROOT, _ <: Any] => _ <: CommandHandlingResult[Any]) {
//  protected def addCommandHandler[COMMAND <: AbstractCommand[_,_]](handler: COMMAND => _ <: CommandHandlingResult[Any]) {
//    commandsHandlers = commandsHandlers :+ handler.asInstanceOf[CommandHandlerF[AGGREGATE_ROOT]]
//  }

//  val eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, _]]

  def eventHandlers: EventHandler

  def commandHandlers: CommandHandler//PartialFunction[Any, _ >: AbstractCommand[AGGREGATE_ROOT] => CommandHandlingResult[Any]]
}