package io.reactivecqrs.api.event

import java.lang.reflect.Type

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl


/**
 * This trait marks class as a business event that occurred to aggregate.
 * @tparam AGGREGATE type of aggregate this event is related to.
 */
abstract class Event[AGGREGATE](implicit ev: Manifest[AGGREGATE]) {
  def aggregateType = ev.runtimeClass.asInstanceOf[Class[AGGREGATE]]
}

/**
 * Special type of event, for removing effect of previous events.
 * @tparam AGGREGATE type of aggregate this event is related to.
 */
abstract class UndoEvent[AGGREGATE](implicit ev: Manifest[AGGREGATE]) extends Event[AGGREGATE] {
  /** How many events should ba canceled. */
  val eventsCount: Int
}


abstract class DeleteEvent[AGGREGATE](implicit ev: Manifest[AGGREGATE]) extends Event[AGGREGATE]
