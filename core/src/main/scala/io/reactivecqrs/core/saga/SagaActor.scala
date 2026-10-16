package io.reactivecqrs.core.saga

import org.apache.pekko.pattern.ask
import org.apache.pekko.actor.{Actor, ActorRef}
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.{SagaId, UserId}
import io.reactivecqrs.core.saga.SagaActor._
import io.reactivecqrs.core.uid.{NewSagasIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{Backoff, MyActorLogging}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

object SagaActor {

  /** A saga to run from its stored step; sent by the actor to itself for every saga loaded at start. */
  case class SagaPersisted(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int)

  case object LoadPendingSagas
  private case class LoadPendingSagasAttempt(attempt: Int)

  private case class RunningSaga(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int)

  /** A write of saga progress; after it is stored the saga goes on (create, store) or is done (delete). */
  private sealed trait SagaWrite { val saga: RunningSaga }
  private case class CreateSaga(saga: RunningSaga) extends SagaWrite
  private case class StoreProgress(saga: RunningSaga) extends SagaWrite
  private case class DeleteSaga(saga: RunningSaga) extends SagaWrite

  private case class WriteCompleted(write: SagaWrite, attempt: Int, result: Try[Boolean])
  private case class RetryWrite(write: SagaWrite, attempt: Int)
  private case class StepCompleted(saga: RunningSaga, result: Try[SagaHandlingStatus])
  private case class RevertStepCompleted(saga: RunningSaga, result: Try[SagaRevertHandlingStatus])
}

/**
  * Runs process managers. Each step's result and each write of the saga's progress come back to the actor as a
  * message, so all decisions are taken on the actor's thread (F17). A failed write is retried with back-off; after
  * `persistRetries` runs out the saga stays at its last stored step and resumes from there after a restart (F35), which
  * repeats that step, so the commands a step sends must be idempotent (use the `SagaStep` key, H3).
  *
  * `handleOrder` and `handleRevert` run on the actor's thread and must only start the work, returning a Future.
  */
abstract class SagaActor extends Actor with MyActorLogging {

  implicit val timeout: Timeout = Timeout(60.seconds)

  import context.dispatcher

  val uidGenerator: ActorRef
  val state: SagaState
  val name: String

  /** Retries of a failed write of saga progress: from 100 ms, doubling, at most 30 s, 10 attempts; then the saga is
    * left for the next restart. */
  protected val persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)

  private var nextSagaId = 0L
  private var remainingSagasIds = 0L

  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  def handleOrder(sagaStep: SagaStep): ReceiveOrder
  def handleRevert(sagaStep: SagaStep): ReceiveRevert

  // All rows are read before any saga starts, so a load that fails part-way and is retried starts no saga twice.
  private def loadPendingSagas(attempt: Int): Unit = {
    Try {
      var loaded = Vector[SagaPersisted]()
      state.loadAllSagas(name, (sagaId: SagaId, userId: UserId, respondTo: String, phase: SagaPhase, step: Int, order: SagaInternalOrder) => {
        loaded :+= SagaPersisted(sagaId, respondTo, order, phase, step)
      })
      loaded
    } match {
      case Success(loaded) =>
        loaded.foreach { saga =>
          log.info("Continuing saga " + name + " " + saga.sagaId.asLong + " at step " + saga.step)
          self ! saga
        }
      case Failure(e) =>
        log.error(e, "Loading pending sagas " + name + " failed, attempt " + attempt + ", retrying")
        context.system.scheduler.scheduleOnce(persistRetries.delay(attempt + 1), self, LoadPendingSagasAttempt(attempt + 1))
    }
  }

  override def preStart(): Unit = {
    self ! LoadPendingSagas
  }

  // After a restart the running sagas still send their results to this actor; loading the stored ones again would
  // start them a second time (F75).
  override def postRestart(reason: Throwable): Unit = {
    log.warning("Saga actor " + name + " restarted after: " + reason.getMessage)
  }

  override def receive: Receive = {
    case LoadPendingSagas => loadPendingSagas(1)
    case LoadPendingSagasAttempt(attempt) => loadPendingSagas(attempt)
    case m: SagaOrder => startSaga(m, sender())
    case m: SagaPersisted => runStep(RunningSaga(m.sagaId, m.respondTo, m.order, m.phase, m.step))
    case StepCompleted(saga, result) => handleStepResult(saga, result)
    case RevertStepCompleted(saga, result) => handleRevertStepResult(saga, result)
    case WriteCompleted(pending, attempt, result) => handleWriteResult(pending, attempt, result)
    case RetryWrite(pending, attempt) => write(pending, attempt)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName +"] "+m)
  }

  // Without a saga id the order is refused, not thrown: a throw would restart this actor (F75).
  private def startSaga(order: SagaOrder, respondTo: ActorRef): Unit = {
    Try(takeNextSagaId) match {
      case Success(sagaId) => write(CreateSaga(RunningSaga(sagaId, respondTo.path.toString, order, CONTINUES, 0)))
      case Failure(e) =>
        log.error(e, "No saga id for an order of saga " + name + ", the order is refused")
        respondTo ! SagaFailureResponse(List("No saga id: " + e.getMessage))
    }
  }

  private def runStep(saga: RunningSaga): Unit = saga.phase match {
    case CONTINUES =>
      startStep(handleOrder(SagaStep(saga.sagaId, saga.step))(saga.order))(result => StepCompleted(saga, result))
    case REVERTING =>
      startStep(handleRevert(SagaStep(saga.sagaId, saga.step))(saga.order))(result => RevertStepCompleted(saga, result))
    case phase =>
      log.warning("Saga " + name + " " + saga.sagaId.asLong + " is in phase " + phase.name + " and is not run")
  }

  // A handler that throws, or does not handle the order, fails the step like a failed Future.
  private def startStep[S](step: => Future[S])(completed: Try[S] => Any): Unit = {
    Try(step) match {
      case Success(future) => future.onComplete(result => self ! completed(result))
      case Failure(e) => self ! completed(Failure(e))
    }
  }

  private def handleStepResult(saga: RunningSaga, result: Try[SagaHandlingStatus]): Unit = result match {
    case Success(SagaContinues(order)) =>
      write(StoreProgress(saga.copy(order = order, step = saga.step + 1)))
    case Success(SagaSucceded(response)) =>
      respond(saga, response)
      write(DeleteSaga(saga))
    case Success(SagaFailed(response)) =>
      respond(saga, response)
      write(StoreProgress(saga.copy(phase = REVERTING, step = saga.step + 1)))
    case Failure(e) =>
      log.error(e, "Saga " + name + " " + saga.sagaId.asLong + " step " + saga.step + " failed, reverting")
      respond(saga, SagaFailureResponse(List(e.getMessage)))
      write(StoreProgress(saga.copy(phase = REVERTING, step = saga.step + 1)))
  }

  private def handleRevertStepResult(saga: RunningSaga, result: Try[SagaRevertHandlingStatus]): Unit = result match {
    case Success(SagaRevertContinues(order)) =>
      write(StoreProgress(saga.copy(order = order, phase = REVERTING, step = saga.step + 1)))
    case Success(SagaRevertSucceded) =>
      write(DeleteSaga(saga))
    case Success(SagaRevertFailed(exceptions)) =>
      log.error(s"Saga revert failed, sagaId = ${saga.sagaId.asLong}, order = ${saga.order}, " + exceptions.mkString(", "))
      write(DeleteSaga(saga))
    case Failure(e) =>
      log.error(e, s"Saga revert failed, sagaId = ${saga.sagaId.asLong}, order = ${saga.order}")
      write(DeleteSaga(saga))
  }

  private def respond(saga: RunningSaga, response: SagaResponse): Unit = {
    context.system.actorSelection(saga.respondTo) ! response
  }

  // The write runs off the actor's thread and touches no actor state; its result comes back as a message.
  private def write(pending: SagaWrite, attempt: Int = 1): Unit = {
    val saga = pending.saga
    Future {
      pending match {
        case _: CreateSaga => state.createSaga(name, saga.sagaId, saga.respondTo, saga.order); true
        case _: StoreProgress => state.updateSaga(name, saga.sagaId, saga.order, saga.phase, saga.step)
        case _: DeleteSaga => state.deleteSaga(name, saga.sagaId)
      }
    }.onComplete(result => self ! WriteCompleted(pending, attempt, result))
  }

  private def handleWriteResult(pending: SagaWrite, attempt: Int, result: Try[Boolean]): Unit = {
    val saga = pending.saga
    result match {
      case Success(true) => pending match {
        case _: CreateSaga | _: StoreProgress => runStep(saga)
        case _: DeleteSaga => ()
      }
      case Success(false) => pending match {
        case _: DeleteSaga => log.warning("Saga " + name + " " + saga.sagaId.asLong + " was already removed")
        case _ => log.error("Saga " + name + " " + saga.sagaId.asLong + " is no longer stored (removed elsewhere?), stopping it at step " + saga.step)
      }
      case Failure(e) if !persistRetries.exhausted(attempt) =>
        log.warning("Storing saga " + name + " " + saga.sagaId.asLong + " failed (attempt " + attempt + " of " + persistRetries.maxAttempts + "): " + e.getMessage)
        context.system.scheduler.scheduleOnce(persistRetries.delay(attempt + 1), self, RetryWrite(pending, attempt + 1))
      case Failure(e) => pending match {
        case _: CreateSaga =>
          log.error(e, "Saga " + name + " could not be stored, its order is refused")
          respond(saga, SagaFailureResponse(List("The saga could not be stored: " + e.getMessage)))
        case _ =>
          log.error(e, "Storing saga " + name + " " + saga.sagaId.asLong + " failed " + attempt + " times; it resumes from its last stored step after a restart")
      }
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
