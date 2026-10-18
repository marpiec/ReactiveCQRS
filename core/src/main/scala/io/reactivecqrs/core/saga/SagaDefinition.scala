package io.reactivecqrs.core.saga

import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.SpaceId
import io.reactivecqrs.core.util.Backoff

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * A process manager as data and functions (AR5, D86), as an `AggregateContext` is an aggregate: the library's
  * `SagaRunner` runs it, stores each saga's progress and resumes it after a restart. One instance serves every saga of
  * its name, and the futures of their steps run at once, so it keeps no mutable state. What its steps need (command
  * buses, an `ExecutionContext`, an ask `Timeout`) it takes in its constructor; it never sees the runner's actor.
  */
trait SagaDefinition {

  /** A step's handling of orders. */
  type ReceiveOrder = SagaDefinition.ReceiveOrder
  /** A revert step's handling of orders. */
  type ReceiveRevert = SagaDefinition.ReceiveRevert

  /** The key its sagas are stored under: the runner resumes the stored sagas of its name, so it never changes. */
  def name: String

  /** Starts the work of a step for the order and gives its outcome: continue with the next order, succeed or fail with
    * the caller's answer. A step runs again when the runner started again before its outcome was stored, so the commands
    * it sends carry `step` as their idempotency key (H3). A handler that throws, or does not take the order, fails the
    * step. */
  def handleOrder(step: SagaStep): ReceiveOrder

  /** The same for undoing what the steps before a failure did. */
  def handleRevert(step: SagaStep): ReceiveRevert

  /** The organization an order belongs to, stored with its saga so that a purge of it removes the saga (R17). None, the
    * default, leaves the saga out of purges. */
  def spaceOf(order: SagaInternalOrder): Option[SpaceId] = None

  /** Retries of a failed write of a saga's progress; after them the write is tried again at their longest delay until
    * it is stored, as the saga is never dropped while its runner runs (F35, F206). A new saga whose first write fails
    * them all is refused. */
  def persistRetries: Backoff = SagaDefinition.defaultPersistRetries

  /** F205: how a step whose future failed (an ask that timed out, an exception) runs again, with the same `SagaStep`,
    * before the saga reverts: a command applied late then answers with its stored response. Fail the future for an
    * answer that may change when sent again (`CommandRejected`, `CommandHandlingError`, `EventHandlingError`), and return
    * `SagaFailed` for a refusal. */
  def stepRetries: Backoff = SagaDefinition.defaultStepRetries

  /** F207: how long the answer of a saga started by an `IdempotentSagaOrder` is kept for repeats of its order. */
  def answersKeptFor: FiniteDuration = 30.days
}

object SagaDefinition {
  /** Takes an order and gives, in a future, what the step decided. */
  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  /** Takes an order and gives, in a future, what the revert step decided. */
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  /** From 100 ms, doubling, at most 30 s, 10 attempts. */
  val defaultPersistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
  /** From 1 s, doubling, at most 30 s, 5 attempts. */
  val defaultStepRetries: Backoff = Backoff(1.second, 30.seconds, maxAttempts = 5)
}
