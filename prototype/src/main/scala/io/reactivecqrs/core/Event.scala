package io.reactivecqrs.core

import akka.actor.ActorRef

import scala.reflect._

abstract class Event[AGGREGATE_ROOT]

case class EventEnvelope[AGGREGATE_ROOT](respondTo: ActorRef,
                                         expectedVersion: AggregateVersion,
                                         event: Event[AGGREGATE_ROOT])




abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT]) {
  val eventClassName = eventClassTag.runtimeClass.getName
  def handle(aggregateRoot: AGGREGATE_ROOT, event: EVENT): Unit
}
