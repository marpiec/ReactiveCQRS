package io.reactivecqrs.core.saga

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object SagaActor {

  case class SagaOrderPersisted(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder)
  case class SagaRevertPersisted(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder)


}

abstract class SagaActor extends Actor with ActorLogging {

  import context.dispatcher

  val state: SagaState
  val name: String

  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]

  def handleOrder: ReceiveOrder
  def handleRevert: ReceiveRevert

  def nextSagaId = 0

  override def receive: Receive = {
    case m: SagaOrder => persistInitialSagaStatusAndSendConfirm(nextSagaId, sender, m)
    case m: SagaPersisted =>
      internalHandleOrder(m.sagaId, m.respondTo, m.order, m.phase)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName +"]")
  }

  def persistInitialSagaStatusAndSendConfirm(sagaId: Int, respondTo: ActorRef, order: SagaOrder): Unit = {
    val me = self
    Future {
      state.createSaga(name, sagaId, respondTo, order)
      println("!1 " + SagaPersisted(sagaId, respondTo, order, CONTINUES))
      me ! SagaPersisted(sagaId, respondTo, order, CONTINUES)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def persistSagaStatusAndSendConfirm(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder, phase: SagaPhase): Unit = {
    val me = self
    Future {
      state.updateSaga(name, sagaId, order, phase)
      println("!2 " + SagaPersisted(sagaId, respondTo, order, phase))
      me ! SagaPersisted(sagaId, respondTo, order, phase)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def deleteSagaStatus(sagaId: Int): Unit = {
    state.deleteSaga(name, sagaId)
  }


  private def internalHandleOrder(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder, status: SagaPhase): Unit = {
    println("!internalHandleOrder " + order)
    val me = self
    Future {
      tryToHandleOrderOrRevert(order, status).onComplete {
        case Success(r) =>
          r match {
          case c: SagaContinues =>
            persistSagaStatusAndSendConfirm(sagaId, respondTo, c.order, CONTINUES)
          case c: SagaSucceded =>
            println("!5 respondTo !" + c.response)
            respondTo ! c.response
            deleteSagaStatus(sagaId)
          case c: SagaFailed =>
            println("!6 respondTo !" + c.response)
            respondTo ! c.response
            if(status == CONTINUES) {
              persistSagaStatusAndSendConfirm(sagaId, respondTo, order, REVERTING)
            } else {
              deleteSagaStatus(sagaId)
            }

        }
        case Failure(ex) =>
          log.error(ex, "Exception while handling saga " + order)
          val response = SagaFailureResponse(List(ex.getMessage))
          respondTo ! response
          if(status == CONTINUES) {
            persistSagaStatusAndSendConfirm(sagaId, respondTo, order, REVERTING)
          } else {
            deleteSagaStatus(sagaId)
          }
      }
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  private def tryToHandleOrderOrRevert(order: SagaInternalOrder, status: SagaPhase): Future[SagaHandlingStatus] = {
    status match {
      case CONTINUES => handleOrder(order)
      case REVERTING => handleRevert(order)
      case s => throw new IllegalStateException("Incorrect status value: " + s)
    }
  }

}

