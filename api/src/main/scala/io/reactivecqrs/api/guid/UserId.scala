package io.reactivecqrs.api.guid

/**
 * Globally unique id that identifies user of the application, originator of command.
 * It might be the same as AggregateId, if user is represented as Aggregate in the system.
 * @param id unique long identifier across users.
 */
case class UserId(id: Long)
