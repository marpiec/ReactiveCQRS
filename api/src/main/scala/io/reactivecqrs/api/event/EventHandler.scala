package io.reactivecqrs.api.event

sealed abstract class EventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]](implicit ev: Manifest[EVENT]) {
  def eventClass = ev.runtimeClass.asInstanceOf[Class[EVENT]]
}

/**
 * Handler used for first event on Aggregate, thous event that creates aggregate and initiates it's state.
 * @tparam AGGREGATE type of aggregate related to event handled by this handler.
 * @tparam EVENT type of event that can be handled by this handler.
 */
abstract class CreationEventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]](implicit ev: Manifest[EVENT]) extends EventHandler[AGGREGATE, EVENT] {
  def handle(event: EVENT): AGGREGATE
}

abstract class ModificationEventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]](implicit ev: Manifest[EVENT]) extends EventHandler[AGGREGATE, EVENT] {
  def handle(aggregate: AGGREGATE, event: EVENT): AGGREGATE
}
