package io.reactivecqrs.api.id

/**
 * Globally unique id that identifies command triggered by a user.
 * @param asLong unique long identifier across commands.
 */
case class CommandId(asLong: Long)