package io.reactivecqrs

package object api {
  type CommandResult = CustomCommandResult[Nothing]
  type CommandResponse = CustomCommandResponse[Nothing]
}
