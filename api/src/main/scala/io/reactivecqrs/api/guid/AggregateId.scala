package io.reactivecqrs.api.guid

/**
 * Globally unique id that identifies single aggregate in whole application.
 * @param id unique long identifier across aggregates.
 */
case class AggregateId(id: Long)
