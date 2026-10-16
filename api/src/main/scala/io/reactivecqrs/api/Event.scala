package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, SpaceId}

import scala.reflect.{ClassTag, classTag}

/** An event of an aggregate of type `AGGREGATE_ROOT`. It knows its root's class (a `ClassTag`, not reflected types: D75). */
abstract class Event[AGGREGATE_ROOT: ClassTag] extends Serializable {
  def aggregateRootClass: Class[_] = classTag[AGGREGATE_ROOT].runtimeClass
}


/**
 * Special type of event, for removing effect of previous events.
 * @tparam AGGREGATE_ROOT type of aggregate this event is related to.
 */
abstract class UndoEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT] {
  /** How many events should ba canceled. */
  val eventsCount: Int
}


abstract class DuplicationEvent[AGGREGATE_ROOT: ClassTag] extends FirstEvent[AGGREGATE_ROOT] {
  val baseAggregateId: AggregateId
  val baseAggregateVersion: AggregateVersion
  def spaceId: SpaceId
}


abstract class FirstEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT] {
  def spaceId: SpaceId
}

abstract class PermanentDeleteEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT]
