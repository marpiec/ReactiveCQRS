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

  type ReceiveOrder = SagaDefinition.ReceiveOrder
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

  /** Retries of a failed write of a saga's progress; after them the saga waits at its last stored step until the runner
    * starts again (F35). */
  def persistRetries: Backoff = SagaDefinition.defaultPersistRetries
}

object SagaDefinition {
  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  /** From 100 ms, doubling, at most 30 s, 10 attempts. */
  val defaultPersistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
}
