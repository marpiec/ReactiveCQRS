package io.reactivecqrs.api.event

import java.lang.reflect.Type

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl


/**
 * This trait marks class as a business event that occurred to aggregate.
 * @tparam AGGREGATE_ROOT type of aggregate this event is related to.
 */
abstract class Event[AGGREGATE_ROOT](implicit ev: Manifest[AGGREGATE_ROOT]) {
  def aggregateType = ev.runtimeClass.asInstanceOf[Class[AGGREGATE_ROOT]]
}

/**
 * Special type of event, for removing effect of previous events.
 * @tparam AGGREGATE_ROOT type of aggregate this event is related to.
 */
abstract class UndoEvent[AGGREGATE_ROOT](implicit ev: Manifest[AGGREGATE_ROOT]) extends Event[AGGREGATE_ROOT] {
  /** How many events should ba canceled. */
  val eventsCount: Int
}


abstract class DeleteEvent[AGGREGATE_ROOT](implicit ev: Manifest[AGGREGATE_ROOT]) extends Event[AGGREGATE_ROOT]
