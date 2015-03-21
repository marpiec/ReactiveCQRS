package io.reactivecqrs.api.command

/**
 * Trait that should be implemented by command class that will be instantiated by user.
 * @tparam RESPONSE Type of response that will be send to user after command handling.
 */
trait Command[AGGREGATE, RESPONSE]