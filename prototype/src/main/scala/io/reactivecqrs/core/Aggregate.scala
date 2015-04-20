package io.reactivecqrs.core


/**
 * Globally unique id that identifies single aggregate in whole application.
 * @param asLong unique long identifier across aggregates.
 */
case class AggregateId(asLong: Long)


object AggregateVersion {
  val ZERO = AggregateVersion(0)
}


case class AggregateVersion(version: Int) {
  def < (other: AggregateVersion) = this.version < other.version
  def == (other: AggregateVersion) = this.version == other.version
  def > (other: AggregateVersion) = this.version > other.version

  def increment = AggregateVersion(version + 1)

}


case class GetAggregateRoot(id: AggregateId)


abstract class Aggregate[AGGREGATE_ROOT] {
  val commandsHandlers: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]]
  val eventsHandlers: Seq[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]]
}