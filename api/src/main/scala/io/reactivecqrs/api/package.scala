package io.reactivecqrs

package object api {
  type CommandResult = GenericCommandResult[Nothing]
  type CommandResponse = CustomCommandResponse[Nothing]


}
