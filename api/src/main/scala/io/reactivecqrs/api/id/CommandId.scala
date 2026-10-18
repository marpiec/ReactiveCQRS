package io.reactivecqrs.api.id

/**
 * The id the command bus gives each command it receives, from its own id range: it names the command in errors
 * (`CommandHandlingError`, `EventHandlingError`) and in logs.
 *
 * @param asLong the command's id.
 */
case class CommandId(asLong: Long)
