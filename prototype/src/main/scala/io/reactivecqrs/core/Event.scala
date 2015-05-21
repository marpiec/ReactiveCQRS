package io.reactivecqrs.core

import _root_.io.reactivecqrs.api.guid.{CommandId, UserId}
import akka.actor.ActorRef

import scala.reflect._
import scala.reflect.runtime.universe._

abstract class Event[AGGREGATE_ROOT: TypeTag] {
  def aggregateRootType = typeOf[AGGREGATE_ROOT]
}

case class EventEnvelope[AGGREGATE_ROOT](respondTo: ActorRef,
                                         commandId: CommandId,
                                         userId: UserId,
                                         expectedVersion: AggregateVersion,
                                         event: Event[AGGREGATE_ROOT])




abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT]) {
  val eventClassName = eventClassTag.runtimeClass.getName
  def handle(aggregateRoot: AGGREGATE_ROOT, event: EVENT): AGGREGATE_ROOT
}
