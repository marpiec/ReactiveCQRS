package io.reactivecqrs.core.saga

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
}
