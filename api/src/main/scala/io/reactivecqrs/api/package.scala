package io.reactivecqrs

package object api {
  type CommandResult = GenericCommandResult[NoInfo]
  type CommandResponse = CustomCommandResponse[NoInfo]


}
