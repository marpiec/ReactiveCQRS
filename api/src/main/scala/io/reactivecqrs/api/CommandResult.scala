package io.reactivecqrs.api

import scala.concurrent.Future

/** What a command handler returns: events to store, a refusal, or a `Future` of either (`AsyncCommandResult`).
  * `RESPONSE_INFO`: what the sender's success response carries besides the aggregate's id and version. */
sealed abstract class GenericCommandResult[+RESPONSE_INFO]

/** A command handler's decision: `CommandSuccess`, `CommandFailure` or `RewriteCommandSuccess`. */
sealed abstract class CustomCommandResult[+RESPONSE_INFO] extends GenericCommandResult[RESPONSE_INFO]

/** A history-rewriting command's success: `eventsRewritten` replace the stored events of their versions, and `events`
  * are stored after the last one; the sender gets `responseInfo`. */
case class RewriteCommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], events: Seq[Event[AGGREGATE_ROOT]], responseInfo: RESPONSE_INFO)
  extends CustomCommandResult[RESPONSE_INFO]

/** The command succeeded: `events` are stored, in one transaction, at the next versions; the sender gets a
  * `SuccessResponse` (a `CustomSuccessResponse` with `responseInfo` when that is not `NoInfo`). An empty `events` changes
  * nothing and answers the current version. */
case class CommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO](events: Seq[Event[AGGREGATE_ROOT]], responseInfo: RESPONSE_INFO)
  extends CustomCommandResult[RESPONSE_INFO]

/** The command was refused: nothing is stored, and the sender gets `response`. */
case class CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](response: FailureResponse)
  extends CustomCommandResult[RESPONSE_INFO]

/** A decision that a handler makes asynchronously (it asks another actor, say): the owner waits for `future`, at most
  * the context's `commandTimeout`. `AggregateContext` turns a `Future` returned by a handler into one. */
case class AsyncCommandResult[RESPONSE_INFO](future: Future[CustomCommandResult[RESPONSE_INFO]]) extends GenericCommandResult[RESPONSE_INFO]

object RewriteCommandSuccess {
  /** Rewrites `eventsRewritten` and stores `event`. */
  def apply[AGGREGATE_ROOT](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], event: Event[AGGREGATE_ROOT]): RewriteCommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new RewriteCommandSuccess(eventsRewritten, List(event), NoInfo.empty)

  /** Rewrites `eventsRewritten`, stores `event`, and answers with `responseInfo`. */
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], event: Event[AGGREGATE_ROOT], responseInfo: RESPONSE_INFO): RewriteCommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO] =
    new RewriteCommandSuccess(eventsRewritten, List(event), responseInfo)

  /** Rewrites `eventsRewritten` and stores `events`. */
  def apply[AGGREGATE_ROOT](eventsRewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], events: Seq[Event[AGGREGATE_ROOT]]): RewriteCommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new RewriteCommandSuccess(eventsRewritten, events, NoInfo.empty)
}

object CommandSuccess {
  /** Stores `event`. */
  def apply[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT]): CommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new CommandSuccess(List(event), NoInfo.empty)

  /** Stores `event` and answers with `responseInfo` (a `CustomSuccessResponse`). */
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](event: Event[AGGREGATE_ROOT], responseInfo: RESPONSE_INFO): CommandSuccess[AGGREGATE_ROOT, RESPONSE_INFO] =
    new CommandSuccess(List(event), responseInfo)

  /** Stores `events`, in this order, in one transaction. */
  def apply[AGGREGATE_ROOT](events: Seq[Event[AGGREGATE_ROOT]]): CommandSuccess[AGGREGATE_ROOT, NoInfo] =
    new CommandSuccess(events, NoInfo.empty)
}

object CommandFailure {
  /** Refuses the command with these reasons, which the sender gets in a `FailureResponse`. */
  def apply[AGGREGATE_ROOT](exceptions: List[String]): CommandFailure[AGGREGATE_ROOT, NoInfo] = new CommandFailure[AGGREGATE_ROOT, NoInfo](FailureResponse(exceptions))
  /** Refuses the command with this reason. */
  def apply[AGGREGATE_ROOT](exception: String): CommandFailure[AGGREGATE_ROOT, NoInfo] = new CommandFailure[AGGREGATE_ROOT, NoInfo](FailureResponse(List(exception)))
}

/** `CommandFailure` for a handler whose successes carry `RESPONSE_INFO`, so both branches have one type. */
object CustomCommandFailure {
  /** Refuses the command with these reasons. */
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exceptions: List[String]): CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO] = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(exceptions))
  /** Refuses the command with this reason. */
  def apply[AGGREGATE_ROOT, RESPONSE_INFO](exception: String): CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO] = new CommandFailure[AGGREGATE_ROOT, RESPONSE_INFO](FailureResponse(List(exception)))
}
