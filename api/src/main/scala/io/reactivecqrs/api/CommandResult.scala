package io.reactivecqrs.api

// Command handling result

sealed abstract class CustomCommandResult[+RESPONSE_INFO]

case class CommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO](events: Seq[Event[AGGREGATE_ROOT]], responseInfo: RESPONSE_INFO)
  extends CustomCommandResult[RESPONSE_INFO]

case class CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](response: FailureResponse)
  extends CustomCommandResult[RESPONSE_INFO]



object CommandSuccess {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):CommandSuccess[AGGREGATE_ROOT, Nothing] =
    new CommandSuccess(List(event), Nothing.empty)

  def apply[AGGREGATE_ROOT, RESPONSE_INFO](event: Event[AGGREGATE_ROOT], responseInfo: RESPONSE_INFO) =
    new CommandSuccess(List(event), responseInfo)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):CommandSuccess[AGGREGATE_ROOT, Nothing] =
    new CommandSuccess(events, Nothing.empty)
}


object CommandFailure {
  def apply[AGGREGATE_ROOT](exceptions: List[String]) = new CommandFailure[AGGREGATE_ROOT, Nothing](FailureResponse(exceptions))
  def apply[AGGREGATE_ROOT](exception: String) = new CommandFailure[AGGREGATE_ROOT, Nothing](FailureResponse(List(exception)))
}

object CustomCommandFailure {
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exceptions: List[String]) = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(exceptions))
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exception: String) = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(List(exception)))
}