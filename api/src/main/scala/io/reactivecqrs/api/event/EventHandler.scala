package io.reactivecqrs.api.event

sealed trait EventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]] {
  def eventClass: Class[EVENT] // TODO extract from declaration
}

/**
 * Handler used for first event on Aggregate, thous event that creates aggregate and initiates it's state.
 * @tparam AGGREGATE type of aggregate related to event handled by this handler.
 * @tparam EVENT type of event that can be handled by this handler.
 */
trait CreationEventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]] extends EventHandler[AGGREGATE, EVENT] {
  def handle(event: EVENT): AGGREGATE
}


trait DeletionEventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]] extends EventHandler[AGGREGATE, EVENT]


trait ModificationEventHandler[AGGREGATE, EVENT <: Event[AGGREGATE]] extends EventHandler[AGGREGATE, EVENT] {
  def handle(aggregate: AGGREGATE, event: EVENT): AGGREGATE
}
