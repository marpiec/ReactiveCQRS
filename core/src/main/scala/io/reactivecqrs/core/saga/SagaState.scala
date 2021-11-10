package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.{SagaId, UserId}


abstract class SagaState {

  def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder): Unit

  def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Unit

  def deleteSaga(name: String, sagaId: SagaId): Unit

  def loadAllSagas(name: String, handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit
}




