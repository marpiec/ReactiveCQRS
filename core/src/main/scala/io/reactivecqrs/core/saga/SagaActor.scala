package io.reactivecqrs.core.saga

import akka.pattern.ask
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.{SagaId, UserId}
import io.reactivecqrs.core.saga.SagaActor.{LoadPendingSagas, SagaPersisted}
import io.reactivecqrs.core.uid.{NewSagasIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object SagaActor {


  case class SagaPersisted(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int)

  case object LoadPendingSagas
}

// Actor per saga -  to recieve messages from command bus?
// TODO manual reply of saga? as saga might not know that previous command succeded? or command idempotency?
abstract class SagaActor extends Actor with ActorLogging {

  implicit val timeout = Timeout(60.seconds)

  import context.dispatcher

  val uidGenerator: ActorRef
  val state: SagaState
  val name: String

  private var nextSagaId = 0L
  private var remainingSagasIds = 0L

  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  def handleOrder(sagaStep: SagaStep): ReceiveOrder
  def handleRevert(sagaStep: SagaStep): ReceiveRevert

  private def loadPendingSagas(): Unit = {
    state.loadAllSagas(name, (sagaId: SagaId, userId: UserId, respondTo: String, phase: SagaPhase, step: Int, order: SagaInternalOrder) => {
      println("Continuing saga " + name+" "+sagaId+" "+step)
      self ! SagaPersisted(sagaId, respondTo, order, phase, step)
    })
  }

  override def preStart(): Unit = {
    self ! LoadPendingSagas
  }

  override def receive: Receive = {
    case LoadPendingSagas => loadPendingSagas()
    case m: SagaOrder => persistInitialSagaStatusAndSendConfirm(self, takeNextSagaId, sender.path.toString, m)
    case m: SagaPersisted if m.phase == CONTINUES => internalHandleOrderContinue(self, m.sagaId, m.step, m.respondTo, m.order)
    case m: SagaPersisted if m.phase == REVERTING => internalHandleRevert(self, m.sagaId, m.step, m.respondTo, m.order)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName +"] "+m)
  }

  private def persistInitialSagaStatusAndSendConfirm(me: ActorRef, sagaId: SagaId, respondTo: String, order: SagaOrder): Unit = {
    Future {
      state.createSaga(name, sagaId, respondTo, order)
      me ! SagaPersisted(sagaId, respondTo, order, CONTINUES, 0)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  private def persistSagaStatusAndSendConfirm(me: ActorRef, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int): Unit = {
    Future {
      state.updateSaga(name, sagaId, order, phase, step)
      me ! SagaPersisted(sagaId, respondTo, order, phase, step)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  private def deleteSagaStatus(sagaId: SagaId): Unit = {
    state.deleteSaga(name, sagaId)
  }

  private def internalHandleOrderContinue(me: ActorRef, sagaId: SagaId, step: Int, respondTo: String, order: SagaInternalOrder): Unit = {
    Future {
      handleOrder(SagaStep(sagaId, step))(order).onComplete {
        case Success(r) =>
          r match {
          case c: SagaContinues =>
            persistSagaStatusAndSendConfirm(me, sagaId, respondTo, c.order, CONTINUES, step + 1)
          case c: SagaSucceded =>
            context.system.actorSelection(respondTo) ! c.response
            deleteSagaStatus(sagaId)
          case c: SagaFailed =>
            context.system.actorSelection(respondTo) ! c.response
            persistSagaStatusAndSendConfirm(me, sagaId, respondTo, order, REVERTING, step + 1)
        }
        case Failure(ex) =>
          context.system.actorSelection(respondTo) ! SagaFailureResponse(List(ex.getMessage))
          persistSagaStatusAndSendConfirm(me, sagaId, respondTo, order, REVERTING, step + 1)
      }
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


  private def internalHandleRevert(me: ActorRef, sagaId: SagaId, step: Int, respondTo: String, order: SagaInternalOrder): Unit = {
    Future {
      handleRevert(SagaStep(sagaId, step))(order).onComplete {
        case Success(r) =>
          r match {
            case c: SagaRevertContinues =>
              persistSagaStatusAndSendConfirm(me, sagaId, respondTo, c.order, REVERTING, step + 1)
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

  private def takeNextSagaId: SagaId = {
    if(remainingSagasIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout = Timeout(60 seconds)
      val pool: Future[NewSagasIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewSagasIdsPool).mapTo[NewSagasIdsPool]
      val newSagasIdsPool: NewSagasIdsPool = Await.result(pool, 60 seconds)
      remainingSagasIds = newSagasIdsPool.size
      nextSagaId = newSagasIdsPool.from
    }

    remainingSagasIds -= 1
    val sagaId = nextSagaId
    nextSagaId += 1
    SagaId(sagaId)


  }

}

