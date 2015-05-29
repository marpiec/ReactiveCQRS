package io.reactivecqrs.api

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


abstract class FirstEvent[AGGREGATE_ROOT: TypeTag] extends Event[AGGREGATE_ROOT]

abstract class DeleteEvent[AGGREGATE_ROOT: TypeTag] extends Event[AGGREGATE_ROOT]





