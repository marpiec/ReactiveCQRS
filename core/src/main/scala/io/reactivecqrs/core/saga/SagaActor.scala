package io.reactivecqrs.core.saga

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.core.saga.SagaActor.SagaPersisted
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

object SagaActor {

  case class SagaPersisted(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder, phase: SagaPhase)

}

abstract class SagaActor extends Actor with ActorLogging {

  import context.dispatcher

  val state: SagaState
  val name: String

  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  def handleOrder: ReceiveOrder
  def handleRevert: ReceiveRevert

  def nextSagaId = 0

  override def receive: Receive = {
    case m: SagaOrder => persistInitialSagaStatusAndSendConfirm(self, nextSagaId, sender, m)
    case m: SagaPersisted if m.phase == CONTINUES => internalHandleOrderContinue(self, m.sagaId, m.respondTo, m.order)
    case m: SagaPersisted if m.phase == REVERTING => internalHandleRevert(self, m.sagaId, m.respondTo, m.order)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName +"] "+m)
  }

  def persistInitialSagaStatusAndSendConfirm(me: ActorRef, sagaId: Int, respondTo: ActorRef, order: SagaOrder): Unit = {
    Future {
      state.createSaga(name, sagaId, respondTo, order)
      me ! SagaPersisted(sagaId, respondTo, order, CONTINUES)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def persistSagaStatusAndSendConfirm(me: ActorRef, sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder, phase: SagaPhase): Unit = {
    Future {
      state.updateSaga(name, sagaId, order, phase)
      me ! SagaPersisted(sagaId, respondTo, order, phase)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def deleteSagaStatus(sagaId: Int): Unit = {
    state.deleteSaga(name, sagaId)
  }

  private def internalHandleOrderContinue(me: ActorRef, sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder): Unit = {
    Future {
      handleOrder(order).onComplete {
        case Success(r) =>
          r match {
          case c: SagaContinues =>
            persistSagaStatusAndSendConfirm(me, sagaId, respondTo, c.order, CONTINUES)
          case c: SagaSucceded =>
            respondTo ! c.response
            deleteSagaStatus(sagaId)
          case c: SagaFailed =>
            respondTo ! c.response
            persistSagaStatusAndSendConfirm(me, sagaId, respondTo, order, REVERTING)
        }
        case Failure(ex) =>
          respondTo ! SagaFailureResponse(List(ex.getMessage))
          persistSagaStatusAndSendConfirm(me, sagaId, respondTo, order, REVERTING)
      }
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


  private def internalHandleRevert(me: ActorRef, sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder): Unit = {
    Future {
      handleRevert(order).onComplete {
        case Success(r) =>
          r match {
            case c: SagaRevertContinues =>
              persistSagaStatusAndSendConfirm(me, sagaId, respondTo, c.order, CONTINUES)
            case SagaRevertSucceded =>
              deleteSagaStatus(sagaId)
            case c: SagaRevertFailed =>
              log.error(s"Saga revert failed, sagaId = $sagaId, order = $order, "+ c.exceptions.mkString(", "))
              deleteSagaStatus(sagaId)
          }
        case Failure(ex) =>
          log.error(ex, s"Saga revert failed, sagaId = $sagaId, order = $order")
          deleteSagaStatus(sagaId)
      }
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

}

