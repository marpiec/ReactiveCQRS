package io.reactivecqrs.api

import io.reactivecqrs.api.guid.CommandId

/**
 * Responsible for generating globally unique identifiers for commands.
 */
trait CommandIdGenerator {
  def nextCommandId: CommandId
}
