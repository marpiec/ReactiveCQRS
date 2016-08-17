package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.reflect.runtime.universe.TypeTag

case class GetAggregate(id: AggregateId)
case class GetAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class GetEventsForAggregate(id: AggregateId)
case class GetEventsForAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class SimulateEvent[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

abstract class AggregateContext[AGGREGATE_ROOT] {
  def initialAggregateRoot: AGGREGATE_ROOT


  type HandlerWrapper = (=> CustomCommandResult[Any]) => CustomCommandResult[CustomCommandResponse[_]]

  type SingleHandler = (_ <: Command[AGGREGATE_ROOT, CustomCommandResponse[Any]]) => CustomCommandResult[Any]
  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, CustomCommandResult[Any]]
  type CommandHandlerWrapper = Function[CommandHandler, CommandHandler]

  type EventHandler = (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]

  def eventHandlers: EventHandler

  def commandHandlers: CommandHandler
}