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




abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT]) {
  val eventClassName = eventClassTag.runtimeClass.getName
  def handle(aggregateRoot: AGGREGATE_ROOT, event: EVENT): AGGREGATE_ROOT
}
