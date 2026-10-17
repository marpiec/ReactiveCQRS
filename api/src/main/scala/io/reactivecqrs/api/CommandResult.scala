package io.reactivecqrs.api

import scala.concurrent.Future

// Command handling result

sealed abstract class GenericCommandResult[+RESPONSE_INFO]

sealed abstract class CustomCommandResult[+RESPONSE_INFO] extends GenericCommandResult[RESPONSE_INFO]


case class RewriteCommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], events: Seq[Event[AGGREGATE_ROOT]], responseInfo: RESPONSE_INFO)
  extends CustomCommandResult[RESPONSE_INFO]

case class CommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO](events: Seq[Event[AGGREGATE_ROOT]], responseInfo: RESPONSE_INFO)
  extends CustomCommandResult[RESPONSE_INFO]

case class CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](response: FailureResponse)
  extends CustomCommandResult[RESPONSE_INFO]

case class AsyncCommandResult[RESPONSE_INFO](future: Future[CustomCommandResult[RESPONSE_INFO]]) extends GenericCommandResult[RESPONSE_INFO]


object RewriteCommandSuccess {
  def apply[AGGREGATE_ROOT](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], event: Event[AGGREGATE_ROOT]):RewriteCommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new RewriteCommandSuccess(eventsRewritten, List(event), NoInfo.empty)

  def apply[AGGREGATE_ROOT, RESPONSE_INFO](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], event: Event[AGGREGATE_ROOT], responseInfo: RESPONSE_INFO) =
    new RewriteCommandSuccess(eventsRewritten, List(event), responseInfo)

  def apply[AGGREGATE_ROOT](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], events: Seq[Event[AGGREGATE_ROOT]]):RewriteCommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new RewriteCommandSuccess(eventsRewritten, events, NoInfo.empty)
}

object CommandSuccess {
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]):CommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new CommandSuccess(List(event), NoInfo.empty)

  def apply[AGGREGATE_ROOT, RESPONSE_INFO](event: Event[AGGREGATE_ROOT], responseInfo: RESPONSE_INFO) =
    new CommandSuccess(List(event), responseInfo)

  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]):CommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new CommandSuccess(events, NoInfo.empty)
}


object CommandFailure {
  def apply[AGGREGATE_ROOT](exceptions: List[String]) = new CommandFailure[AGGREGATE_ROOT, NoInfo](FailureResponse(exceptions))
  def apply[AGGREGATE_ROOT](exception: String) = new CommandFailure[AGGREGATE_ROOT, NoInfo](FailureResponse(List(exception)))
}

object CustomCommandFailure {
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exceptions: List[String]) = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(exceptions))
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exception: String) = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(List(exception)))
}