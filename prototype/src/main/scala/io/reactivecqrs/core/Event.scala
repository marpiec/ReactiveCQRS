package io.reactivecqrs.core

import _root_.io.reactivecqrs.api.guid.{AggregateId, CommandId, UserId}
import akka.actor.ActorRef

import scala.reflect._
import scala.reflect.runtime.universe._

abstract class Event[AGGREGATE_ROOT: TypeTag] {
  def aggregateRootType = typeOf[AGGREGATE_ROOT]
}

case class EventsEnvelope[AGGREGATE_ROOT](respondTo: ActorRef,
                                         aggregateId: AggregateId,
                                         commandId: CommandId,
                                         userId: UserId,
                                         expectedVersion: AggregateVersion,
                                         events: Seq[Event[AGGREGATE_ROOT]])


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



sealed abstract class AbstractEventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT]) {
  val eventClassName = eventClassTag.runtimeClass.getName
}

abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT])
  extends AbstractEventHandler[AGGREGATE_ROOT, EVENT]{
  def handle(aggregateRoot: AGGREGATE_ROOT, event: EVENT): AGGREGATE_ROOT
}


abstract class FirstEventHandler[AGGREGATE_ROOT, EVENT <: FirstEvent[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT])
  extends AbstractEventHandler[AGGREGATE_ROOT, EVENT] {
  def handle(event: EVENT): AGGREGATE_ROOT
}


