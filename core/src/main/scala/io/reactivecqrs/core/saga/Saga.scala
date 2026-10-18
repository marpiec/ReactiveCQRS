package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.UserId


object SagaPhase {
  /** The phase stored as `name`. */
  def byName(name: String): SagaPhase = name match {
    case CONTINUES.name => CONTINUES
    case REVERTING.name => REVERTING
    case ERROR.name => ERROR
    case FINISHED.name => FINISHED
    case other => throw new IllegalArgumentException("Unknown saga phase: " + other)
  }
}
/** Where a saga is (chapter 12), stored under `name`. */
abstract class SagaPhase(val name: String)
/** Working forward. */
case object CONTINUES extends SagaPhase("continues")
/** Undoing what it did, after a step failed. */
case object REVERTING extends SagaPhase("reverting")
/** Reserved: the runner never puts a saga there. */
case object ERROR extends SagaPhase("error")
/** F207: a saga of a keyed order that is done, kept with its answer for repeats of the order; never run. */
case object FINISHED extends SagaPhase("finished")


/** The message a saga hands itself for its next step (a `SagaOrder` is one too); stored with each step. */
trait SagaInternalOrder {
  /** Who started the saga; its commands are sent as this user. */
  val userId: UserId
}

/** The message a caller sends to start a saga. */
trait SagaOrder extends SagaInternalOrder

/** F205: what a step throws, or fails its future with, when its work could not be done now (an error, a rejection
  * during an upgrade): the runner runs the step again with the same key (`SagaDefinition.stepRetries`), and reverts only
  * once it keeps failing. `reason` reaches the logs: name the answer's class, not its values. */
class SagaStepNotDone(reason: String) extends RuntimeException(reason)

/** F207: an order a caller may send again (a retry after a timeout, through another node): a second order with the same
  * `orderKey` joins the saga the first started, or, once it is done, gets its answer (kept `answersKeptFor`), instead of
  * starting the saga twice. At most 256 characters. */
trait IdempotentSagaOrder extends SagaOrder {
  def orderKey: String
}

/** What the caller of a saga gets back. */
trait SagaResponse
/** A saga's failure, for `exceptions`. */
case class SagaFailureResponse(exceptions: List[String]) extends SagaResponse



/** What a step decided. */
sealed trait SagaHandlingStatus

/** The step is done; the next one runs with `order`. */
case class SagaContinues(order: SagaInternalOrder) extends SagaHandlingStatus

/** The whole job is done; the caller gets `response`. */
case class SagaSucceded(response: SagaResponse) extends SagaHandlingStatus

/** The job failed; the caller gets `response`, and the saga starts reverting. */
case class SagaFailed(response: SagaResponse) extends SagaHandlingStatus





/** What a revert step decided. */
sealed trait SagaRevertHandlingStatus

/** This much is undone; the next revert step runs with `order`. */
case class SagaRevertContinues(order: SagaInternalOrder) extends SagaRevertHandlingStatus

/** Everything is undone. */
case object SagaRevertSucceded extends SagaRevertHandlingStatus

/** Undoing failed, for `exceptions`. */
case class SagaRevertFailed(exceptions: List[String]) extends SagaRevertHandlingStatus

