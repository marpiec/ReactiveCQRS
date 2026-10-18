package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}

import scala.concurrent.duration.FiniteDuration

/** A saga as stored: its id, user, where to answer, phase, step, order and order key (F207). */
case class StoredSaga(sagaId: SagaId, userId: UserId, respondTo: String, phase: SagaPhase, step: Int, order: SagaInternalOrder,
                      orderKey: Option[String] = None)

/** What storing a new saga did. */
sealed trait SagaCreation
/** The saga is stored (also when an earlier attempt of the same write was, F206). */
case object NewSaga extends SagaCreation
/** F207: another saga has the order's key: `sagaId`, with its caller's answer once it has one. */
case class SagaOfKey(sagaId: SagaId, answer: Option[SagaResponse]) extends SagaCreation

/** Where sagas' progress is stored, by saga name and id: `PostgresSagaState`, the table `sagas`. */
abstract class SagaState {

  /** Stores a new saga, with the organization its order belongs to, if any (R17: a purge of the space removes it), and
    * its order's key, if it has one (F207). */
  def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId] = None,
                 orderKey: Option[String] = None): SagaCreation

  /** Stores a saga's progress, and, for a keyed saga, the answer its caller was given (F207); false when the saga is not
    * stored, or (with `from`) not at that phase and step (F232). */
  def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int,
                 from: Option[(SagaPhase, Int)] = None, answer: Option[SagaResponse] = None): Boolean

  /** Removes a finished saga; false when it is not stored, or (with `from`) not at that phase and step. */
  def deleteSaga(name: String, sagaId: SagaId, from: Option[(SagaPhase, Int)] = None): Boolean

  /** F207: marks a keyed saga finished, keeping its row and the answer its caller was given (`answer`, or the one stored
    * before) for repeated orders, until `removeFinished`; false as `updateSaga`. */
  def finishSaga(name: String, sagaId: SagaId, answer: Option[SagaResponse], from: Option[(SagaPhase, Int)] = None): Boolean

  /** Removes the finished sagas of `name` older than `olderThan`; returns how many. */
  def removeFinished(name: String, olderThan: FiniteDuration): Int

  /** Calls `handler` with every stored unfinished saga of `name`. */
  def loadAllSagas(name: String, handler: StoredSaga => Unit): Unit
}
