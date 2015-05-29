package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId


case class GetAggregate(id: AggregateId)


abstract class AggregateCommandBus[AGGREGATE_ROOT] {
  val commandsHandlers: Seq[CommandHandler[AGGREGATE_ROOT,_,_]]
  val eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, _]]
}