package io.reactivecqrs

/** The API a domain is written against: aggregate contexts (`AggregateContext`), their commands (`Command` and its
  * kinds), events (`Event` and its kinds), the results a command handler returns (`CommandSuccess`, `CommandFailure`)
  * and the responses a sender gets (`SuccessResponse`, `FailureResponse`, ...), the messages that read aggregates, and
  * the ids. It depends on Pekko only; the `core` module runs it. */
package object api {
  /** A command handler's result whose response carries no information of its own. */
  type CommandResult = GenericCommandResult[NoInfo]
  /** A response that carries no information of its own: `SuccessResponse`, `FailureResponse`, the errors. */
  type CommandResponse = CustomCommandResponse[NoInfo]
}
