package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.{SagaId, UserId}


abstract class SagaState {

  def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder): Unit

  /** Stores a saga's progress; false when the saga is not stored (anymore). */
  def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Boolean

  /** Removes a finished saga; false when it was not stored. */
  def deleteSaga(name: String, sagaId: SagaId): Boolean

  def loadAllSagas(name: String, handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit
}
