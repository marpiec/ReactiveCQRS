package io.reactivecqrs.core.saga

import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.Serialization
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}
import io.reactivecqrs.core.saga.SagaActor._
import io.reactivecqrs.core.uid.{IdPool, NewSagasIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{Backoff, BlockingDispatcher, MyActorLogging}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

object SagaActor {

  /** A saga to run from its stored step; sent by the actor to itself for every saga loaded at start. */
  case class SagaPersisted(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int)

  case object LoadPendingSagas
  private case class LoadPendingSagasAttempt(attempt: Int)
  private case class LoadFailed(attempt: Int)

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
  private case class SagaIdsNotReceived(error: Throwable)

  /** The caller's path with its node's address: the runner may answer from another node after it moved (D46, M5).
    * Pekko adds the address of an ask's temporary actor only while it writes a message, so outside one the path would
    * name an actor on whichever node reads it (F137). */
  private[saga] def callerPath(system: ActorSystem, caller: ActorRef): String =
    Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem])(() => Serialization.serializedActorPath(caller))
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
  // F27: the saga state's JDBC calls run on the blocking dispatcher.
  private val blocking: ExecutionContext = BlockingDispatcher(context.system)

  val uidGenerator: ActorRef
  val state: SagaState
  val name: String

  /** Retries of a failed write of saga progress: from 100 ms, doubling, at most 30 s, 10 attempts; then the saga is
    * left for the next restart. */
  protected val persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)

  // F23: saga ids come from pools fetched ahead; an order that arrives while none is at hand waits for the next pool,
  // with the orders after it.
  private val sagaIds = new IdPool
  private var waitingForIds = Vector[(SagaOrder, ActorRef)]()

  type ReceiveOrder = PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]
  type ReceiveRevert = PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]

  def handleOrder(sagaStep: SagaStep): ReceiveOrder
  def handleRevert(sagaStep: SagaStep): ReceiveRevert

  /** The organization an order belongs to, stored with its saga so that a purge of that organization removes the saga
    * (R17). None, the default, leaves the saga out of purges. */
  protected def spaceOf(order: SagaInternalOrder): Option[SpaceId] = None

  // All rows are read before any saga starts, so a load that fails part-way and is retried starts no saga twice. The
  // read runs off the actor's thread and touches no actor state.
  private def loadPendingSagas(attempt: Int): Unit = {
    Future {
      var loaded = Vector[SagaPersisted]()
      state.loadAllSagas(name, (sagaId: SagaId, userId: UserId, respondTo: String, phase: SagaPhase, step: Int, order: SagaInternalOrder) => {
        loaded :+= SagaPersisted(sagaId, respondTo, order, phase, step)
      })
      loaded
    }(blocking).onComplete {
      case Success(loaded) =>
        loaded.foreach { saga =>
          log.info("Continuing saga " + name + " " + saga.sagaId.asLong + " at step " + saga.step)
          self ! saga
        }
      case Failure(e) =>
        log.error(e, "Loading pending sagas " + name + " failed, attempt " + attempt + ", retrying")
        self ! LoadFailed(attempt)
    }
  }

  override def preStart(): Unit = {
    self ! LoadPendingSagas
    askForIds()
  }

  // After a restart the running sagas still send their results to this actor; loading the stored ones again would
  // start them a second time (F75).
  override def postRestart(reason: Throwable): Unit = {
    log.warning("Saga actor " + name + " restarted after: " + reason.getMessage)
  }

  override def receive: Receive = {
    case LoadPendingSagas => loadPendingSagas(1)
    case LoadPendingSagasAttempt(attempt) => loadPendingSagas(attempt)
    case LoadFailed(attempt) => context.system.scheduler.scheduleOnce(persistRetries.delay(attempt + 1), self, LoadPendingSagasAttempt(attempt + 1))
    case m: SagaOrder =>
      waitingForIds :+= (m -> sender())
      startWaiting()
      askForIds()
    case NewSagasIdsPool(from, size) =>
      sagaIds.received(from, size)
      startWaiting()
      askForIds()
    case SagaIdsNotReceived(e) => sagaIdsNotReceived(e)
    case m: SagaPersisted => runStep(RunningSaga(m.sagaId, m.respondTo, m.order, m.phase, m.step))
    case StepCompleted(saga, result) => handleStepResult(saga, result)
    case RevertStepCompleted(saga, result) => handleRevertStepResult(saga, result)
    case WriteCompleted(pending, attempt, result) => handleWriteResult(pending, attempt, result)
    case RetryWrite(pending, attempt) => write(pending, attempt)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName +"] "+m)
  }

  private def startWaiting(): Unit = while (waitingForIds.nonEmpty && sagaIds.available) {
    val (order, respondTo) = waitingForIds.head
    waitingForIds = waitingForIds.tail
    write(CreateSaga(RunningSaga(SagaId(sagaIds.take()), callerPath(context.system, respondTo), order, CONTINUES, 0)))
  }

  private def askForIds(): Unit =
    if (sagaIds.shouldAsk) (uidGenerator ? UidGeneratorActor.GetNewSagasIdsPool).recover { case e => SagaIdsNotReceived(e) }.pipeTo(self)

  // Without a saga id the waiting orders are refused, not thrown: a throw would restart this actor (F75). The pool is
  // asked for again when the next order arrives.
  private def sagaIdsNotReceived(e: Throwable): Unit = {
    sagaIds.failed()
    log.error(e, "No pool of saga ids for saga " + name + " came; " + waitingForIds.size + " orders waiting for one are refused")
    waitingForIds.foreach { case (_, respondTo) => respondTo ! SagaFailureResponse(List("No saga id: " + e.getMessage)) }
    waitingForIds = Vector.empty
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

  // A caller stored before 1.0 has a path without an address, which resolves on this node as it did.
  private def respond(saga: RunningSaga, response: SagaResponse): Unit =
    context.system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(saga.respondTo) ! response

  // The write runs off the actor's thread and touches no actor state; its result comes back as a message.
  private def write(pending: SagaWrite, attempt: Int = 1): Unit = {
    val saga = pending.saga
    val space = pending match {
      case _: CreateSaga => spaceOf(saga.order)
      case _ => None
    }
    Future {
      pending match {
        case _: CreateSaga => state.createSaga(name, saga.sagaId, saga.respondTo, saga.order, space); true
        case _: StoreProgress => state.updateSaga(name, saga.sagaId, saga.order, saga.phase, saga.step)
        case _: DeleteSaga => state.deleteSaga(name, saga.sagaId)
      }
    }(blocking).onComplete(result => self ! WriteCompleted(pending, attempt, result))
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


}
