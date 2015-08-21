package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

import scala.reflect.runtime.universe._

abstract class Event[AGGREGATE_ROOT: TypeTag] {
  def aggregateRootType = typeOf[AGGREGATE_ROOT]
}


/**
 * Special type of event, for removing effect of previous events.
 * @tparam AGGREGATE_ROOT type of aggregate this event is related to.
 */
abstract class UndoEvent[AGGREGATE_ROOT: TypeTag] extends Event[AGGREGATE_ROOT] {
  /** How many events should ba canceled. */
  val eventsCount: Int
}


abstract class DuplicationEvent[AGGREGATE_ROOT: TypeTag] extends Event[AGGREGATE_ROOT] {
  val baseAggregateId: AggregateId
  val baseAggregateVersion: AggregateVersion
}

