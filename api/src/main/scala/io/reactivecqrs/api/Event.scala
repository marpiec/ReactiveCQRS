package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, SpaceId}

import scala.reflect.{ClassTag, classTag}

/** An event of an aggregate of type `AGGREGATE_ROOT`: a fact the aggregate's command handlers decided, stored as the
  * JSON of its class under the class's name. A class whose shape must change is kept and a new class (`...V2`) takes
  * its place (D29). It knows its root's class (a `ClassTag`, not reflected types: D75). */
abstract class Event[AGGREGATE_ROOT: ClassTag] extends Serializable {
  /** The class of the aggregate root this event belongs to. */
  def aggregateRootClass: Class[_] = classTag[AGGREGATE_ROOT].runtimeClass
}

/** Cancels the aggregate's last `eventsCount` events: they stay stored, marked as no-ops, and a restore skips them.
  * Event handlers need no case for it (F148): the state after an undo is read from the store. */
abstract class UndoEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT] {
  /** How many of the events before this one it cancels. */
  val eventsCount: Int
}

/** The first event of an aggregate made as a copy of another, `baseAggregateId`, at `baseAggregateVersion`: the copy
  * shares the base's history up to that version, then has its own (a duplication chain). */
abstract class DuplicationEvent[AGGREGATE_ROOT: ClassTag] extends FirstEvent[AGGREGATE_ROOT] {
  /** The aggregate copied. */
  val baseAggregateId: AggregateId
  /** The version of the base the copy starts from. */
  val baseAggregateVersion: AggregateVersion
  def spaceId: SpaceId
}

/** The event that creates an aggregate: the first one of every aggregate. */
abstract class FirstEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT] {
  /** The organization the new aggregate belongs to (`SpaceId(0)` when the application has none). */
  def spaceId: SpaceId
}

/** Deletes the aggregate for good, with its events, its archive and its subscriptions, once the subscribers have
  * received this event; it waits while a subscription of the aggregate is failed (D20a). */
abstract class PermanentDeleteEvent[AGGREGATE_ROOT: ClassTag] extends Event[AGGREGATE_ROOT]
