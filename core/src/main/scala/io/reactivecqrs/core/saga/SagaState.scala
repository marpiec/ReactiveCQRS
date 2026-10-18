package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}


/** Where sagas' progress is stored, by saga name and id: `PostgresSagaState`, the table `sagas`. */
abstract class SagaState {

  /** Stores a new saga, with the organization its order belongs to, if any (R17: a purge of the space removes it). */
  def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId] = None): Unit

  /** Stores a saga's progress; false when the saga is not stored (anymore). */
  def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Boolean

  /** Removes a finished saga; false when it was not stored. */
  def deleteSaga(name: String, sagaId: SagaId): Boolean

  /** Calls `handler` with every stored saga of `name`: its id, user, where to answer, phase, step and order. */
  def loadAllSagas(name: String, handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit
}
