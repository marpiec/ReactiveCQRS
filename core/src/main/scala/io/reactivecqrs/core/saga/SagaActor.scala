package io.reactivecqrs.core.saga

import io.reactivecqrs.core.util.Backoff
import org.apache.pekko.actor.ActorRef

/**
  * A saga written as an actor, as before 1.0: the subclass gives `uidGenerator`, `state`, `name`, `handleOrder` and
  * `handleRevert`, and is run by the same state machine as a `SagaDefinition` under `SagaRunner`. Kept so that sagas
  * written for 0.12 run unchanged on 1.0; move each to a `SagaDefinition` under the same `name` (its stored sagas then
  * resume under the runner), as the book's chapter 12 shows. Its code shares the runner's actor, which AR5 ends.
  */
@deprecated("write a SagaDefinition, run by SagaRunner, under the same name (AR5, the book's chapter 12)", "1.0.0")
abstract class SagaActor extends SagaRunning with SagaDefinition {

  /** Where saga ids come from. */
  val uidGenerator: ActorRef
  /** Where the sagas' progress is stored. */
  val state: SagaState

  final override protected def definition: SagaDefinition = this

  /** F285: a saga written for 0.12 sends its commands without an idempotency key, as 0.12 had none, so a step that
    * failed is not run again (a command applied late would be applied twice): the saga reverts, as in 0.12. Override it
    * with `SagaDefinition.defaultStepRetries` once every command of its steps carries the `SagaStep` as its key. */
  override def stepRetries: Backoff = SagaDefinition.noStepRetries
}
