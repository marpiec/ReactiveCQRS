package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.UserId


object SagaPhase {
  /** The phase stored as `name`. */
  def byName(name: String): SagaPhase = name match {
    case CONTINUES.name => CONTINUES
    case REVERTING.name => REVERTING
    case ERROR.name => ERROR
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


/** The message a saga hands itself for its next step (a `SagaOrder` is one too); stored with each step. */
trait SagaInternalOrder {
  /** Who started the saga; its commands are sent as this user. */
  val userId: UserId
}

/** The message a caller sends to start a saga. */
trait SagaOrder extends SagaInternalOrder

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

