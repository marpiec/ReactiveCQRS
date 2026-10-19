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

  /** `NoInfo` by its name before 1.0 (F165). Without it, code written for 0.12 that says `Nothing` after
    * `import io.reactivecqrs.api._` would silently get `scala.Nothing`, and a pattern `case _: Nothing` would never
    * match; with it, that code means what it meant, and the compiler names each use to rename. Say `NoInfo`, and
    * `scala.Nothing` where the bottom type is meant. */
  @deprecated("say NoInfo (F165); scala.Nothing for the bottom type", "1.0.1")
  type Nothing = NoInfo
  @deprecated("say NoInfo (F165)", "1.0.1")
  val Nothing: NoInfo.type = NoInfo
}
