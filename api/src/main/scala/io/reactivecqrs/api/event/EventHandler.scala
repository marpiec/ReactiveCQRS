package io.reactivecqrs.api.event

sealed abstract class EventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit ev: Manifest[EVENT]) {
  def eventClass = ev.runtimeClass.asInstanceOf[Class[EVENT]]
}

/**
 * Handler used for first event on Aggregate, thous event that creates aggregate and initiates it's state.
 * @tparam AGGREGATE_ROOT type of aggregate related to event handled by this handler.
 * @tparam EVENT type of event that can be handled by this handler.
 */
abstract class CreationEventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit ev: Manifest[EVENT]) extends EventHandler[AGGREGATE_ROOT, EVENT] {
  def handle(event: EVENT): AGGREGATE_ROOT
}

abstract class ModificationEventHandler[AGGREGATE_ROOT, EVENT <: Event[AGGREGATE_ROOT]](implicit ev: Manifest[EVENT]) extends EventHandler[AGGREGATE_ROOT, EVENT] {
  def handle(aggregate: AGGREGATE_ROOT, event: EVENT): AGGREGATE_ROOT
}
