package io.reactivecqrs.api

import scala.reflect.ClassTag

sealed abstract class AbstractEventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit eventClassTag: ClassTag[EVENT]) {
  val eventClassName = eventClassTag.runtimeClass.getName
}

abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]: ClassTag] extends AbstractEventHandler[AGGREGATE_ROOT, EVENT]{
  def handle(aggregateRoot: AGGREGATE_ROOT, event: EVENT): AGGREGATE_ROOT
}
