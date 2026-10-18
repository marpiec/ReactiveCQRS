package io.reactivecqrs.core.saga

import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, ExtendedActorSystem, Props}
import org.apache.pekko.serialization.Serialization
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.SagaId
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.metrics.ReactiveMetrics
import io.reactivecqrs.core.saga.SagaRunner._
import io.reactivecqrs.core.uid.{IdPool, NewSagasIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import io.reactivecqrs.core.util.Errors

/** Runs a `SagaDefinition` (chapter 12). */
object SagaRunner {

  /** The runner as an actor of this node alone; `start` runs it once in the cluster. */
  def props(definition: SagaDefinition, uidGenerator: ActorRef, state: SagaState): Props =
    Props(new SagaRunner(definition, uidGenerator, state))

  /** Starts the runner of `definition` once in the cluster: a singleton (D46) named `actorName`, by default the
    * definition's name, which must then be a valid actor name; on an actor system that is not a cluster, the actor
    * itself. Returns where its orders go. */
  def start(system: ActorSystem, definition: SagaDefinition, uidGenerator: ActorRef, state: SagaState,
            actorName: Option[String] = None): ActorRef =
    Singletons.start(system, actorName.getOrElse(definition.name), props(definition, uidGenerator, state))

  /** A saga to run from its stored step, as the runner loads it. */
  case class SagaPersisted(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int,
                           orderKey: Option[String] = None)

  /** Loads the sagas stored unfinished and resumes those this runner does not run already (F232); the runner sends it
    * to itself at start, and orders wait until that first load is done. */
  case object LoadPendingSagas

  /** Asks the runner for its `SagaStatus` (F206). */
  case object GetSagaStatus
  /** `running`: sagas under way here; `waiting`: orders waiting for the first load or for saga ids; `failingWrites`:
    * sagas whose progress cannot be stored now (they are tried again until it can). */
  case class SagaStatus(name: String, running: Int, waiting: Int, failingWrites: Int)

  private[saga] case class LoadPendingSagasAttempt(attempt: Int)
  private[saga] case class LoadFailed(attempt: Int)
  private[saga] case class Loaded(sagas: Vector[SagaPersisted])
  private[saga] case object RemoveFinished

  private[saga] case class RunningSaga(sagaId: SagaId, respondTo: String, order: SagaInternalOrder, phase: SagaPhase, step: Int,
                                       orderKey: Option[String] = None, attempt: Int = 1) {
    def at: (SagaPhase, Int) = (phase, step)
  }

  /** A write of saga progress; after it is stored the saga goes on (create, store) or is done (finish). `from`: the phase
    * and step the stored saga must be at, so a copy that is behind writes nothing (F232); `answer`: what the caller is
    * told once the write is stored (F206). */
  private[saga] sealed trait SagaWrite { val saga: RunningSaga; val answer: Option[SagaResponse] }
  private[saga] case class CreateSaga(saga: RunningSaga, answer: Option[SagaResponse] = None) extends SagaWrite
  private[saga] case class StoreProgress(saga: RunningSaga, from: (SagaPhase, Int), answer: Option[SagaResponse] = None) extends SagaWrite
  private[saga] case class FinishSaga(saga: RunningSaga, from: (SagaPhase, Int), answer: Option[SagaResponse] = None) extends SagaWrite

  private[saga] case class WriteCompleted(write: SagaWrite, attempt: Int, result: Try[Any])
  private[saga] case class RetryWrite(write: SagaWrite, attempt: Int)
  private[saga] case class StepCompleted(saga: RunningSaga, result: Try[SagaHandlingStatus])
  private[saga] case class RevertStepCompleted(saga: RunningSaga, result: Try[SagaRevertHandlingStatus])
  private[saga] case class RunAgain(saga: RunningSaga)
  private[saga] case class SagaIdsNotReceived(error: Throwable)

  /** The caller's path with its node's address: the runner may answer from another node after it moved (D46, M5).
    * Pekko adds the address of an ask's temporary actor only while it writes a message, so outside one the path would
    * name an actor on whichever node reads it (F137). */
  private[saga] def callerPath(system: ActorSystem, caller: ActorRef): String =
    Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem])(() => Serialization.serializedActorPath(caller))

  /** The longest order key stored (`sagas.order_key`). */
  val MaxOrderKeyLength = 256
}

/** The library's runner of the sagas of one `SagaDefinition` (AR5, D86). */
final class SagaRunner(protected val definition: SagaDefinition, val uidGenerator: ActorRef, val state: SagaState) extends SagaRunning

/**
  * The state machine of sagas, which `SagaRunner` runs for a definition (and the deprecated `SagaActor` for itself).
  * Each step's result and each write of a saga's progress come back to the actor as a message, so all decisions are
  * taken on the actor's thread (F17). A step that fails (its future fails, or it throws) runs again, with the same
  * `SagaStep` key, `stepRetries` times, before the saga reverts (F205); a write that fails is retried with back-off, and
  * a saga is never dropped while its runner lives: once `persistRetries` ran out it is retried at their longest delay
  * until it is stored (F206). Writes compare the stored step and phase, so a copy of a saga that is behind (a load
  * meanwhile, an old runner's late write) stops (F232). A caller hears a failure only once the reverting saga is stored.
  * Steps only start their work and return a future.
  */
abstract class SagaRunning extends Actor with MyActorLogging {

  /** How long the runner waits for an answer it asks for. */
  implicit val timeout: Timeout = Timeout(60.seconds)

  import context.dispatcher
  // F27: the saga state's JDBC calls run on the blocking dispatcher.
  private val blocking: ExecutionContext = BlockingDispatcher(context.system)
  private lazy val metrics = ReactiveMetrics(context.system).metrics

  // Read after construction only: the deprecated SagaActor's subclasses set them in their own constructors.
  protected def definition: SagaDefinition
  /** Where saga ids come from. */
  def uidGenerator: ActorRef
  /** Where the sagas' progress is stored. */
  def state: SagaState

  private def sagaName: String = definition.name
  private def tags: Map[String, String] = Map("saga" -> sagaName)

  // F23: saga ids come from pools fetched ahead; an order that arrives while none is at hand waits for the next pool,
  // with the orders after it.
  private val sagaIds = new IdPool
  private var waitingForIds = Vector[(SagaOrder, ActorRef)]()

  // F232: orders wait for the first load, and a saga under way here is not started again by a load.
  private var loaded = false
  private var beforeLoad = Vector[(SagaOrder, ActorRef)]()
  private val running = mutable.Set[SagaId]()
  // F207: callers of a keyed order that joined a saga under way, answered with its caller.
  private val joined = mutable.Map[SagaId, Vector[String]]()
  // F206: sagas whose last write failed, for the status.
  private val failingWrites = mutable.Set[SagaId]()

  // All rows are read before any saga starts, so a load that fails part-way and is retried starts no saga twice. The
  // read runs off the actor's thread and touches no actor state.
  private def loadPendingSagas(attempt: Int): Unit = {
    val name = sagaName
    Future {
      var stored = Vector[SagaPersisted]()
      state.loadAllSagas(name, saga => stored :+= SagaPersisted(saga.sagaId, saga.respondTo, saga.order, saga.phase, saga.step, saga.orderKey))
      stored
    }(blocking).onComplete {
      case Success(stored) => self ! Loaded(stored)
      case Failure(e) =>
        log.error(e, "Loading pending sagas " + name + " failed, attempt " + attempt + ", retrying")
        self ! LoadFailed(attempt)
    }
  }

  override def preStart(): Unit = {
    self ! LoadPendingSagas
    askForIds()
    context.system.scheduler.scheduleWithFixedDelay(1.minute, 1.day, self, RemoveFinished)
  }

  // After a restart the running sagas still send their results to this actor; loading the stored ones again would
  // start them a second time (F75).
  override def postRestart(reason: Throwable): Unit = {
    log.warning("Saga actor " + sagaName + " restarted after: " + Errors.describe(reason))
  }

  override def receive: Receive = {
    case LoadPendingSagas => loadPendingSagas(1)
    case LoadPendingSagasAttempt(attempt) => loadPendingSagas(attempt)
    case LoadFailed(attempt) =>
      // The first load is tried until it succeeds: orders wait for it.
      context.system.scheduler.scheduleOnce(definition.persistRetries.delay(math.min(attempt + 1, definition.persistRetries.maxAttempts)), self,
        LoadPendingSagasAttempt(attempt + 1))
    case Loaded(stored) => sagasLoaded(stored)
    case m: SagaOrder =>
      if (loaded) {
        waitingForIds :+= (m -> sender())
        startWaiting()
        askForIds()
      } else beforeLoad :+= (m -> sender())
    case NewSagasIdsPool(from, size) =>
      sagaIds.received(from, size)
      startWaiting()
      askForIds()
    case SagaIdsNotReceived(e) => sagaIdsNotReceived(e)
    case m: SagaPersisted => resume(m)
    case StepCompleted(saga, result) => handleStepResult(saga, result)
    case RevertStepCompleted(saga, result) => handleRevertStepResult(saga, result)
    case RunAgain(saga) => if (running.contains(saga.sagaId)) runStep(saga)
    case WriteCompleted(pending, attempt, result) => handleWriteResult(pending, attempt, result)
    case RetryWrite(pending, attempt) => write(pending, attempt)
    case RemoveFinished => removeFinished()
    case GetSagaStatus => sender() ! SagaStatus(sagaName, running.size, beforeLoad.size + waitingForIds.size, failingWrites.size)
    case m => log.warning("Received incorrect message of type: [" + m.getClass.getName + "]")
  }

  private def sagasLoaded(stored: Vector[SagaPersisted]): Unit = {
    stored.foreach(resume)
    if (!loaded) {
      loaded = true
      waitingForIds ++= beforeLoad
      beforeLoad = Vector.empty
      startWaiting()
      askForIds()
    }
  }

  private def resume(saga: SagaPersisted): Unit =
    if (running.contains(saga.sagaId)) log.info("Saga " + sagaName + " " + saga.sagaId.asLong + " runs here already; it is not loaded again")
    else {
      log.info("Continuing saga " + sagaName + " " + saga.sagaId.asLong + " at step " + saga.step)
      running += saga.sagaId
      runStep(RunningSaga(saga.sagaId, saga.respondTo, saga.order, saga.phase, saga.step, saga.orderKey))
    }

  private def startWaiting(): Unit = while (waitingForIds.nonEmpty && sagaIds.available) {
    val (order, respondTo) = waitingForIds.head
    waitingForIds = waitingForIds.tail
    val key = order match {
      case keyed: IdempotentSagaOrder => Some(keyed.orderKey)
      case _ => None
    }
    if (key.exists(_.length > MaxOrderKeyLength)) {
      respondTo ! SagaFailureResponse(List("The order key has " + key.get.length + " characters; at most " + MaxOrderKeyLength + " are stored"))
    } else {
      write(CreateSaga(RunningSaga(SagaId(sagaIds.take()), callerPath(context.system, respondTo), order, CONTINUES, 0, key)))
    }
  }

  private def askForIds(): Unit =
    if (sagaIds.shouldAsk) (uidGenerator ? UidGeneratorActor.GetNewSagasIdsPool).recover { case e => SagaIdsNotReceived(e) }.pipeTo(self)

  // Without a saga id the waiting orders are refused, not thrown: a throw would restart this actor (F75). The pool is
  // asked for again when the next order arrives.
  private def sagaIdsNotReceived(e: Throwable): Unit = {
    sagaIds.failed()
    log.error(e, "No pool of saga ids for saga " + sagaName + " came; " + waitingForIds.size + " orders waiting for one are refused")
    waitingForIds.foreach { case (_, respondTo) => respondTo ! SagaFailureResponse(List("No saga id: " + Errors.describe(e))) }
    waitingForIds = Vector.empty
  }

  private def runStep(saga: RunningSaga): Unit = saga.phase match {
    case CONTINUES =>
      startStep(definition.handleOrder(SagaStep(saga.sagaId, saga.step))(saga.order))(result => StepCompleted(saga, result))
    case REVERTING =>
      startStep(definition.handleRevert(SagaStep(saga.sagaId, saga.step))(saga.order))(result => RevertStepCompleted(saga, result))
    case phase =>
      running -= saga.sagaId
      log.warning("Saga " + sagaName + " " + saga.sagaId.asLong + " is in phase " + phase.name + " and is not run")
  }

  // A handler that throws, or does not handle the order, fails the step like a failed Future.
  private def startStep[S](step: => Future[S])(completed: Try[S] => Any): Unit = {
    Try(step) match {
      case Success(future) => future.onComplete(result => self ! completed(result))
      case Failure(e) => self ! completed(Failure(e))
    }
  }

  // F205: a step that failed runs again with the same key, so a command that was applied late answers with its stored
  // response; only after `stepRetries` does the saga give up on it.
  private def retried(saga: RunningSaga, e: Throwable, what: String): Boolean = {
    val retries = definition.stepRetries
    if (retries.exhausted(saga.attempt)) false else {
      log.warning("Saga " + sagaName + " " + saga.sagaId.asLong + " " + what + " " + saga.step + " failed (attempt " + saga.attempt + " of " +
        retries.maxAttempts + "), runs again: " + Errors.describe(e))
      metrics.count("reactivecqrs.saga.step.retried", tags, 1L)
      context.system.scheduler.scheduleOnce(retries.delay(saga.attempt + 1), self, RunAgain(saga.copy(attempt = saga.attempt + 1)))
      true
    }
  }

  private def handleStepResult(saga: RunningSaga, result: Try[SagaHandlingStatus]): Unit = result match {
    case Success(SagaContinues(order)) =>
      write(StoreProgress(saga.copy(order = order, step = saga.step + 1, attempt = 1), saga.at))
    case Success(SagaSucceded(response)) =>
      write(FinishSaga(saga, saga.at, Some(response)))
    case Success(SagaFailed(response)) =>
      write(StoreProgress(saga.copy(phase = REVERTING, step = saga.step + 1, attempt = 1), saga.at, Some(response)))
    case Failure(e) if retried(saga, e, "step") => ()
    case Failure(e) =>
      log.error(e, "Saga " + sagaName + " " + saga.sagaId.asLong + " step " + saga.step + " failed, reverting")
      write(StoreProgress(saga.copy(phase = REVERTING, step = saga.step + 1, attempt = 1), saga.at, Some(SagaFailureResponse(List(Errors.describe(e))))))
  }

  private def handleRevertStepResult(saga: RunningSaga, result: Try[SagaRevertHandlingStatus]): Unit = result match {
    case Success(SagaRevertContinues(order)) =>
      write(StoreProgress(saga.copy(order = order, phase = REVERTING, step = saga.step + 1, attempt = 1), saga.at))
    case Success(SagaRevertSucceded) =>
      write(FinishSaga(saga, saga.at))
    case Success(SagaRevertFailed(exceptions)) =>
      log.error(s"Saga revert failed, sagaId = ${saga.sagaId.asLong}, order of class ${saga.order.getClass.getName}: " + exceptions.mkString(", "))
      write(FinishSaga(saga, saga.at))
    case Failure(e) if retried(saga, e, "revert step") => ()
    case Failure(e) =>
      log.error(e, s"Saga revert failed, sagaId = ${saga.sagaId.asLong}, order of class ${saga.order.getClass.getName}")
      write(FinishSaga(saga, saga.at))
  }

  // A caller stored before 1.0 has a path without an address, which resolves on this node as it did. Callers that joined
  // the saga with the same order key are answered too (F207).
  private def respond(saga: RunningSaga, response: SagaResponse): Unit =
    (saga.respondTo +: joined.remove(saga.sagaId).getOrElse(Vector.empty)).foreach(respondTo(_) ! response)

  private def respondTo(path: String): ActorRef = context.system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(path)

  // The write runs off the actor's thread and touches no actor state; its result comes back as a message.
  private def write(pending: SagaWrite, attempt: Int = 1): Unit = {
    val (name, saga) = (sagaName, pending.saga)
    val space = pending match {
      case _: CreateSaga => definition.spaceOf(saga.order)
      case _ => None
    }
    Future[Any] {
      pending match {
        case _: CreateSaga => state.createSaga(name, saga.sagaId, saga.respondTo, saga.order, space, saga.orderKey)
        case p: StoreProgress => state.updateSaga(name, saga.sagaId, saga.order, saga.phase, saga.step, Some(p.from),
          if (saga.orderKey.isDefined) p.answer else None)
        case p: FinishSaga =>
          if (saga.orderKey.isDefined) state.finishSaga(name, saga.sagaId, p.answer, Some(p.from)) // F207: kept, with its answer
          else state.deleteSaga(name, saga.sagaId, Some(p.from))
      }
    }(blocking).onComplete(result => self ! WriteCompleted(pending, attempt, result))
  }

  private def handleWriteResult(pending: SagaWrite, attempt: Int, result: Try[Any]): Unit = {
    val saga = pending.saga
    val retries = definition.persistRetries
    if (result.isSuccess) failingWrites -= saga.sagaId
    result match {
      case Success(NewSaga) =>
        running += saga.sagaId
        runStep(saga)
      case Success(SagaOfKey(_, Some(answer))) => // F207: a repeated order of a saga done
        respondTo(saga.respondTo) ! answer
      case Success(SagaOfKey(other, None)) if running.contains(other) => // F207: a repeated order joins the saga under way
        joined(other) = joined.getOrElse(other, Vector.empty) :+ saga.respondTo
        log.info("An order of saga " + sagaName + " repeats the key of saga " + other.asLong + ", and joins it")
      case Success(SagaOfKey(other, None)) if !retries.exhausted(attempt) =>
        // Read before that saga stored its answer, which came after: read again (or it runs in another runner).
        context.system.scheduler.scheduleOnce(retries.delay(attempt + 1), self, RetryWrite(pending, attempt + 1))
      case Success(SagaOfKey(other, None)) =>
        respondTo(saga.respondTo) ! SagaFailureResponse(List("The order key belongs to saga " + other.asLong + ", which does not run here"))
      case Success(true) =>
        pending.answer.foreach(respond(saga, _)) // F206: once the saga's outcome is stored
        pending match {
          case _: StoreProgress => runStep(saga)
          case _ => running -= saga.sagaId
        }
      case Success(_) =>
        // F232: the stored saga is not where this copy is: another copy moved it on (a load, an old runner, a split).
        running -= saga.sagaId
        val from = pending match {
          case p: StoreProgress => p.from
          case p: FinishSaga => p.from
          case _ => saga.at
        }
        log.warning("Saga " + sagaName + " " + saga.sagaId.asLong + " is not stored at " + from._1.name + " step " + from._2 +
          " any more (moved on by another copy, or removed); this copy stops")
      case Failure(e) if !retries.exhausted(attempt) =>
        log.warning("Storing saga " + sagaName + " " + saga.sagaId.asLong + " failed (attempt " + attempt + " of " + retries.maxAttempts + "): " + Errors.describe(e))
        context.system.scheduler.scheduleOnce(retries.delay(attempt + 1), self, RetryWrite(pending, attempt + 1))
      case Failure(e) => pending match {
        case _: CreateSaga =>
          log.error(e, "Saga " + sagaName + " could not be stored, its order is refused")
          respondTo(saga.respondTo) ! SagaFailureResponse(List("The saga could not be stored: " + Errors.describe(e)))
        case _ =>
          // F206: never dropped while this runner lives: tried again at the longest delay until it is stored.
          if (!failingWrites.contains(saga.sagaId) || attempt % 10 == 0)
            log.error(e, "Storing saga " + sagaName + " " + saga.sagaId.asLong + " failed " + attempt + " times; it is tried again every " + retries.max)
          failingWrites += saga.sagaId
          metrics.count("reactivecqrs.saga.write.failed", tags, 1L)
          context.system.scheduler.scheduleOnce(retries.max, self, RetryWrite(pending, attempt + 1))
      }
    }
  }

  // F207: the stored answers of keyed sagas go after `answersKeptFor`.
  private def removeFinished(): Unit = {
    val (name, kept) = (sagaName, definition.answersKeptFor)
    Future(state.removeFinished(name, kept))(blocking).failed.foreach(e => log.warning("Removing the finished sagas " + name + " failed: " + Errors.describe(e)))
  }
}
