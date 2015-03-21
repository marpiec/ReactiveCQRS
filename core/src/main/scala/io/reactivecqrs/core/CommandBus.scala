package io.reactivecqrs.core

import io.reactivecqrs.api.command.Command
import io.reactivecqrs.api.guid.UserId

trait CommandBus[AGGREGATE] {

  def submit[COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](userId: UserId, command: Command[AGGREGATE, RESPONSE]): RESPONSE

}
