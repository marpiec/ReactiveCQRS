package io.reactivecqrs.core

import io.reactivecqrs.api.guid.AggregateId


object AggregateVersion {
  val ZERO = AggregateVersion(0)
}


case class AggregateVersion(asInt: Int) {
  def < (other: AggregateVersion) = this.asInt < other.asInt
  def == (other: AggregateVersion) = this.asInt == other.asInt
  def > (other: AggregateVersion) = this.asInt > other.asInt

  def increment = AggregateVersion(asInt + 1)
  def incrementBy(count: Int) = AggregateVersion(asInt + count)

}


case class GetAggregate(id: AggregateId)


abstract class AggregateCommandBus[AGGREGATE_ROOT] {
  val commandsHandlers: Seq[CommandHandler[AGGREGATE_ROOT,_,_]]
  val eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, _]]
}