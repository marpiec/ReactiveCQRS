package io.reactivecqrs.core.aggregate

import java.time.Instant
import java.time.temporal.ChronoUnit

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.commandhandler.{Idempotency, IdempotencyKeyTakenException}
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}
import io.reactivecqrs.core.eventstore.{EventStoreState, NewEvents}
import io.reactivecqrs.core.util.{Backoff, MyActorLogging, RandomUtil}
import org.apache.pekko.actor.{Actor, ActorRef, Cancellable}
import org.postgresql.util.PSQLException

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object AggregateOwner {

  /** A command for the owner's aggregate, with the id the command bus gave it and the caller to answer. */
  case class CommandEnvelope(respondTo: ActorRef, commandId: CommandId, command: Any)

  /** The aggregate at its current version: `Success(Aggregate)`, or `Failure` when it cannot be restored. */
  case class GetState(respondTo: ActorRef)
  /** Every event of the aggregate, those of the base it was duplicated from included: `Success(List[EventInfo])`. */
  case class GetEvents(respondTo: ActorRef)
  /** The aggregate as soon as it reaches `version`; `Failure(AggregateInIncorrectVersionException)` when it does not
    * within `maxMillis` (F81). */
  case class GetStateMinVersion(respondTo: ActorRef, version: AggregateVersion, maxMillis: Int)

  /** From the command bus, which routes nothing more to this owner: it finishes the commands it started and stops
    * (Cluster Sharding's passivation, D43, D72). */
  case object StopOwner
  /** To the command bus: this owner asks to be stopped, after a failed restore (D11). */
  case object OwnerPassivate

  /** A squash request (D47) and the caller to answer with a `SquashResponse`. */
  case class Squash(respondTo: ActorRef, request: SquashAggregate)

  /** D37: a concurrent command is run again on the latest state after a conflict, at most 10 times, waiting 10 ms
    * doubling to 500 ms, jittered so that competing commands spread out. */
  val defaultConcurrentRetries: Backoff = Backoff(10.millis, 500.millis, maxAttempts = 10, jitter = true)
}

/** What every owner of one aggregate type shares; the command bus builds it once from the type's context. Acknowledgements
  * of publications go to `publicationAcks`, the command bus, which outlives the owners (F91). */
class OwnerSettings[AGGREGATE_ROOT: ClassTag](context: AggregateContext[AGGREGATE_ROOT],
                                              val eventStore: EventStoreState,
                                              val idempotency: Idempotency,
                                              val eventsBus: ActorRef,
                                              val publicationAcks: ActorRef,
                                              val failedRestoreRetryAfter: FiniteDuration,
                                              val maxRepublishEvents: Int = 1000,
                                              val concurrentRetries: Backoff = AggregateOwner.defaultConcurrentRetries) {
  val aggregateType: AggregateType = context.aggregateType
  val commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]] = context.commandHandlers
  val rewriteHistoryCommandHandlers: (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]] =
    context.rewriteHistoryCommandHandlers
  val eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT] = context.eventHandlers
  val initialState: () => AGGREGATE_ROOT = () => context.initialAggregateRoot
  val commandTimeout: FiniteDuration = context.commandTimeout
  val snapshots: Option[SnapshotPolicy] = context.snapshots
  val commandProcessing: CommandProcessing = context.commandProcessing
  val squash: Option[SquashPolicy[AGGREGATE_ROOT]] = context.squash
  val outboxWriter: Option[OutboxWriter[AGGREGATE_ROOT]] = context match {
    case withWriter: WithOutboxWriter[AGGREGATE_ROOT @unchecked] => Some(withWriter.outboxWriter)
    case _ => None
  }
  val restorer = new AggregateRestorer[AGGREGATE_ROOT](eventStore, eventHandlers, initialState, snapshots, context.version,
    classTag[AGGREGATE_ROOT].runtimeClass.getName)
}

/**
  * The one actor of an aggregate (AR1): it restores the aggregate, runs its commands, persists and publishes their
  * events, re-publishes on request, and answers queries. A command handler runs as a `Future` on the state the owner
  * has when the command arrives; its events are persisted only if no other command changed the aggregate meanwhile,
  * otherwise a `ConcurrentCommand` runs again on the new state and any other command gets a conflict (D37). A command
  * that does not finish within the context's timeout is answered with an error and never persisted afterwards (R5).
  *
  * `fresh`: the aggregate id was just taken from the id pool, so there is nothing to restore.
  * See docs/design/M3-aggregates.md.
  */
class AggregateOwner[AGGREGATE_ROOT: TypeTag](aggregateId: AggregateId, settings: OwnerSettings[AGGREGATE_ROOT], fresh: Boolean = false)
  extends Actor with MyActorLogging {

  import AggregateOwner._
  import settings._
  import context.dispatcher

  private var version: AggregateVersion = AggregateVersion.ZERO
  private var root: AGGREGATE_ROOT = initialState()
  // The version of the aggregate's stored snapshot, as far as this owner knows (D30).
  private var snapshotVersion: AggregateVersion = AggregateVersion.ZERO

  /** A command of this owner, from its arrival until it is answered. `started`: it is under way (checked, its handler
    * running, or waiting to run again); in serial mode a command waits in `queued` until it may start. */
  private case class Pending(envelope: CommandEnvelope, timeout: Cancellable, attempt: Int, baseVersion: AggregateVersion, running: Boolean, started: Boolean)
  private val pending = mutable.LinkedHashMap[Long, Pending]() // by command id, oldest first
  private val queued = mutable.Queue[Long]() // serial mode (R8): commands that have not started, oldest first
  private var startingQueued = false
  private var restoreFailure: Option[AggregateRestoreFailedException] = None

  private case class HandlerFinished(commandId: CommandId, result: Try[CustomCommandResult[Any]])
  private case class CommandTimedOut(commandId: CommandId)
  private case class RunAgain(commandId: CommandId)
  private case object MinVersionDeadline
  private case object AskToStop

  private case class MinVersionQuery(respondTo: ActorRef, version: AggregateVersion, deadline: Long)
  private var minVersionQueries = Vector[MinVersionQuery]()
  private var minVersionTimer: Option[Cancellable] = None

  // The last version of the outbox part being re-published (RepublishOutbox): the next part follows its ack.
  private var outboxRepublishedUpTo: Option[AggregateVersion] = None

  private var stopping = false

  override def postStop(): Unit = minVersionTimer.foreach(_.cancel())

  override def receive: Receive = if (fresh) restored else unrestored

  // State is restored with the first message, so a failing restore can be answered (F10). An unexpected failure later
  // forgets the state, which the next message restores again.
  private def unrestored: Receive = {
    case message =>
      restore() match {
        case None =>
          context.become(restored)
          restored.applyOrElse(message, unhandled)
        case Some(failure) =>
          failedRestore(failure).applyOrElse(message, unhandled)
      }
  }

  /** Reads the aggregate from the store, first or again. On failure the owner answers every request with it and asks
    * the command bus to stop it after `failedRestoreRetryAfter`, so a later request restores in a new owner (D11). */
  private def restore(): Option[AggregateRestoreFailedException] = {
    Try(restorer.restore(aggregateId)) match {
      case Success(restored) =>
        version = restored.version
        root = restored.root
        snapshotVersion = restored.fromSnapshot.getOrElse(AggregateVersion.ZERO)
        restoreFailure = None
        None
      case Failure(e) =>
        log.error(e, "Restoring aggregate " + aggregateId.asLong + " of type " + aggregateType.simpleName + " failed, retry after " + failedRestoreRetryAfter)
        val failure = new AggregateRestoreFailedException(aggregateId, aggregateType, e)
        restoreFailure = Some(failure)
        context.become(failedRestore(failure))
        context.system.scheduler.scheduleOnce(failedRestoreRetryAfter, self, AskToStop)
        Some(failure)
    }
  }

  private def failedRestore(failure: AggregateRestoreFailedException): Receive = {
    case envelope: CommandEnvelope => finishWithError(envelope, failure)
    case HandlerFinished(commandId, _) => pendingFailed(commandId, failure)
    case RunAgain(commandId) => pendingFailed(commandId, failure)
    case CommandTimedOut(commandId) => timedOut(commandId)
    case GetState(respondTo) => respondTo ! Failure(failure)
    case GetEvents(respondTo) => respondTo ! Failure(failure)
    case GetStateMinVersion(respondTo, _, _) => respondTo ! Failure(failure)
    case Squash(respondTo, request) => respondTo ! SquashRefused(request.id, "the aggregate cannot be restored: " + failure.getMessage)
    case MinVersionDeadline => answerExpiredMinVersionQueries()
    case AskToStop => context.parent ! OwnerPassivate
    case StopOwner => stopNow(withSnapshot = false)
    case _ => () // nothing to publish without a state
  }

  private lazy val restored: Receive = guarded {
    case envelope: CommandEnvelope => start(envelope)
    case HandlerFinished(commandId, result) => handlerFinished(commandId, result)
    case RunAgain(commandId) => pending.get(commandId.asLong).foreach(attempt)
    case CommandTimedOut(commandId) => timedOut(commandId)
    case GetState(respondTo) => respondTo ! Success(aggregate)
    case GetEvents(respondTo) => respondTo ! (if (version == AggregateVersion.ZERO) Success(Nil) else Try(restorer.events(aggregateId)))
    case GetStateMinVersion(respondTo, minVersion, maxMillis) => minVersionQuery(respondTo, minVersion, maxMillis)
    case MinVersionDeadline => answerExpiredMinVersionQueries()
    case PublishEventsAck(_, versions, _) => delivered(versions)
    case RepublishOutbox(_) => eventStore.oldestVersionToPublish(aggregateId).foreach(from => outboxRepublishedUpTo = republish(from, None))
    case RepublishRange(_, from, subscriber) => republish(from, Some(subscriber))
    case Squash(respondTo, request) => respondTo ! squashHistory(request)
    case StopOwner =>
      stopping = true
      stopIfDone()
    case AskToStop => () // left from a failed restore that was followed by a successful one
  }

  // AR7: a message the owner fails on does not restart it, which would lose its pending callers. The state may be
  // inconsistent after an unexpected failure, so it is restored again from the store with the next message.
  private def guarded(handler: Receive): Receive = {
    case message if handler.isDefinedAt(message) =>
      try handler(message) catch {
        case NonFatal(e) =>
          log.error(e, "Owner of aggregate " + aggregateId.asLong + " of type " + aggregateType.simpleName + " failed on " +
            message.getClass.getSimpleName + "; its state is read again from the store")
          context.become(unrestored)
      }
  }

  private def aggregate: Aggregate[AGGREGATE_ROOT] =
    Aggregate[AGGREGATE_ROOT](aggregateId, version, if (version == AggregateVersion.ZERO) None else Option(root))

  // ************** Commands

  // The timeout counts from the command's arrival, also while it waits in the serial queue (R5).
  private def start(envelope: CommandEnvelope): Unit = commandProcessing match {
    case CommandProcessing.Serial(maxQueue, OverflowPolicy.Reject) if queued.size >= maxQueue =>
      log.warning("Command " + envelope.command.getClass.getSimpleName + " for aggregate " + aggregateId.asLong + " of type " +
        aggregateType.simpleName + " rejected: " + maxQueue + " commands wait already")
      envelope.respondTo ! CommandRejected(aggregateId, s"$maxQueue commands of the aggregate wait already")
    case processing =>
      val timeout = context.system.scheduler.scheduleOnce(commandTimeout, self, CommandTimedOut(envelope.commandId))
      val command = Pending(envelope, timeout, attempt = 1, version, running = false, started = false)
      pending += envelope.commandId.asLong -> command
      processing match {
        case CommandProcessing.Concurrent => begin(command)
        case _: CommandProcessing.Serial =>
          queued.enqueue(envelope.commandId.asLong)
          startQueued()
      }
  }

  // Serial mode: the oldest queued command starts once no command is under way. A command answered at once (a stored
  // response, a version mismatch) lets the next one start in the same loop, without recursion.
  private def startQueued(): Unit = if (!startingQueued) {
    startingQueued = true
    try {
      while (queued.nonEmpty && !pending.valuesIterator.exists(_.started)) {
        pending.get(queued.dequeue()).foreach(begin) // one that timed out while waiting is gone
      }
    } finally startingQueued = false
  }

  private def begin(command: Pending): Unit = {
    val started = command.copy(started = true)
    pending += command.envelope.commandId.asLong -> started
    restoreFailure match {
      case Some(failure) => finishWithError(command.envelope, failure)
      case None => attempt(started) // the idempotency check runs now, also for a command that waited (D31)
    }
  }

  // A copy of an idempotent command that was applied already is answered with the stored response (D12). A command
  // expecting a version above the owner's means another writer moved the aggregate on: it is reloaded first (F19).
  private def attempt(command: Pending): Unit = {
    val envelope = command.envelope
    Try(idempotency.storedResponse(envelope.command)) match {
      case Failure(e) => finishWithError(envelope, e)
      case Success(Some(stored)) => finish(envelope.commandId, stored)
      case Success(None) =>
        val expected = expectedVersion(envelope.command)
        val reloadFailure = if (expected.exists(_ > version)) restore() else None
        reloadFailure match {
          case Some(failure) => pendingFailed(envelope.commandId, failure)
          case None => expected match {
            case Some(e) if e != version =>
              conflict(command, AggregateConcurrentModificationError(aggregateId, aggregateType, e, version))
            case _ => runHandler(command)
          }
        }
    }
  }

  private def runHandler(command: Pending): Unit = {
    val envelope = command.envelope
    pending += envelope.commandId.asLong -> command.copy(baseVersion = version, running = true)
    // The handler runs on another thread: it gets immutable values only, never this actor's fields.
    val state: Either[String, AGGREGATE_ROOT] =
      if (envelope.command.isInstanceOf[FirstCommand[_, _]]) Right(initialState())
      else if (version == AggregateVersion.ZERO) Left("Aggregate " + aggregateId.asLong + " does not exist")
      else Option(root).toRight("Aggregate " + aggregateId.asLong + " is already deleted")
    // A rewrite gets the events it rewrites, read now, on the state the handler sees.
    val rewritten = rewrittenClasses(envelope.command).map(classes => restorer.eventsOfClasses(aggregateId, classes))
    Future {
      val current = state.fold(missing => throw new IllegalStateException(missing), identity)
      val result = rewritten match {
        case Some(events) => rewriteHistoryCommandHandlers(events, current)(envelope.command)
        case None => commandHandlers(current)(envelope.command)
      }
      result match {
        case done: CustomCommandResult[_] => Future.successful(done)
        case AsyncCommandResult(future) => future
      }
    }.flatten.onComplete(result => self ! HandlerFinished(envelope.commandId, result))
  }

  private def handlerFinished(commandId: CommandId, result: Try[CustomCommandResult[Any]]): Unit = {
    pending.get(commandId.asLong).filter(_.running).foreach { command => // a command that timed out is gone
      result match {
        case Success(failure: CommandFailure[_, _]) => finish(commandId, failure.response)
        case Success(success: CommandSuccess[_, _]) =>
          val s = success.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
          persistOrAnswer(command, s.events, Nil, s.responseInfo)
        case Success(rewrite: RewriteCommandSuccess[_, _]) =>
          val s = rewrite.asInstanceOf[RewriteCommandSuccess[AGGREGATE_ROOT, AnyRef]]
          persistOrAnswer(command, s.events, s.eventsRewritten, s.responseInfo)
        case Failure(e: AggregateInIncorrectVersionException) =>
          conflict(command, AggregateConcurrentModificationError(aggregateId, e.aggregateType, e.requestedVersion, e.currentVersion))
        case Failure(e) => finishWithError(command.envelope, e)
      }
    }
  }

  // R9: a command without events writes nothing and changes nothing.
  private def persistOrAnswer(command: Pending, events: Seq[Event[AGGREGATE_ROOT]], rewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], info: AnyRef): Unit = {
    if (events.isEmpty) {
      finish(command.envelope.commandId, successResponse(command.baseVersion, info))
    } else if (command.baseVersion != version) {
      conflict(command, AggregateConcurrentModificationError(aggregateId, aggregateType, command.baseVersion, version))
    } else {
      persist(command, events, rewritten, info)
    }
  }

  // The event handlers run once, on a copy, before the commit (F25); undo and duplication events, and rewrites, read
  // the state from the store after it instead. Every failure is thrown inside the transaction, so nothing of the
  // command is stored (F68), and every outcome answers the command (AR3).
  private def persist(command: Pending, events: Seq[Event[AGGREGATE_ROOT]], rewritten: Iterable[EventWithVersion[AGGREGATE_ROOT]], info: AnyRef): Unit = {
    val envelope = command.envelope
    val userId = userIdOf(envelope.command)
    val timestamp = Instant.now.truncatedTo(ChronoUnit.MICROS) // what PostgreSQL keeps, so a restore reads the same (F7)
    val readAfterCommit = rewritten.nonEmpty || events.exists(e => e.isInstanceOf[UndoEvent[_]] || e.isInstanceOf[DuplicationEvent[_]])
    nextRoot(userId, timestamp, events, readAfterCommit) match {
      case Left((e, event)) =>
        val error = EventHandlingError(event.getClass.getSimpleName, RandomUtil.generateRandomString(16), envelope.commandId)
        log.error(e, "Error handling event " + event.getClass.getSimpleName + ", errorId: [" + error.errorId + "]")
        finish(envelope.commandId, error)
      case Right(newRoot) =>
        val response = successResponse(version.incrementBy(events.size), info)
        val metadata = metadataOf(envelope.command)
        val outcome = Try {
          eventStore.localTx { implicit session =>
            if (rewritten.nonEmpty) eventStore.overwriteEvents(aggregateId, rewritten)
            val stored = eventStore.persistEvents(aggregateId, NewEvents(userId, version, timestamp, events, metadata)).get
              .map { case (event, eventVersion) => EventInfo(eventVersion, event, userId, timestamp, metadata) }
            if (idempotency.isIdempotent(envelope.command)) idempotency.record(envelope.command, response)
            // R12, D33: after the idempotency record, so a copy whose key is taken rolls back before writing anything.
            outboxWriter.foreach(_.write(aggregateId, stored.last.version, stored))
            stored
          }
        }
        outcome match {
          case Success(published) =>
            if (readAfterCommit) {
              // If the state cannot be read, nothing is published now: the outbox poller has it re-published later.
              if (restore().isEmpty) {
                publish(published)
                snapshotIfDue(command.baseVersion)
              }
            } else {
              root = newRoot
              version = published.last.version
              publish(published)
              snapshotIfDue(command.baseVersion)
            }
            finish(envelope.commandId, response)
          case Failure(_: IdempotencyKeyTakenException) =>
            // A copy of this idempotent command was applied first: its stored response is the answer.
            Try(idempotency.storedResponse(envelope.command)).toOption.flatten match {
              case Some(stored) => finish(envelope.commandId, stored)
              case None => finishWithError(envelope, new IllegalStateException("Idempotency key taken but no stored response found"))
            }
          case Failure(e: PSQLException) if e.getLocalizedMessage.contains("Concurrent aggregate modification exception") =>
            // The store moved on without this owner (another writer): reload, so a retry sees the real state (F19).
            restore() match {
              case None => conflict(command, AggregateConcurrentModificationError(aggregateId, aggregateType, command.baseVersion, version))
              case Some(failure) => pendingFailed(envelope.commandId, failure)
            }
          case Failure(e) =>
            val error = EventHandlingError(events.head.getClass.getSimpleName, RandomUtil.generateRandomString(16), envelope.commandId)
            log.error(e, "Error persisting events, errorId: [" + error.errorId + "]")
            finish(envelope.commandId, error)
        }
    }
  }

  private def nextRoot(userId: UserId, timestamp: Instant, events: Seq[Event[AGGREGATE_ROOT]], readAfterCommit: Boolean): Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT] = {
    if (events.exists(_.isInstanceOf[DuplicationEvent[_]])) {
      Right(root) // a duplicate has no state of its own before its first event; it is read after the commit
    } else {
      events.foldLeft[Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT]](Right(root)) {
        case (Right(state), event) =>
          try Right(eventHandlers(userId, timestamp, state)(event)) catch {
            case e: Exception => Left((e, event))
          }
        case (failed, _) => failed
      }
    }
  }

  // A conflict for an idempotent command usually means a copy of it won the race: that copy's stored response is
  // the answer (D12). Otherwise a concurrent command runs again on the latest state (D37), any other gets the error.
  private def conflict(command: Pending, error: AggregateConcurrentModificationError): Unit = {
    val envelope = command.envelope
    Try(idempotency.storedResponse(envelope.command)).toOption.flatten match {
      case Some(stored) => finish(envelope.commandId, stored)
      case None if runsAgainOnConflict(envelope.command) && !concurrentRetries.exhausted(command.attempt) =>
        pending += envelope.commandId.asLong -> command.copy(attempt = command.attempt + 1, running = false)
        context.system.scheduler.scheduleOnce(concurrentRetries.delay(command.attempt + 1), self, RunAgain(envelope.commandId))
      case None => finish(envelope.commandId, error)
    }
  }

  private def timedOut(commandId: CommandId): Unit = pending.remove(commandId.asLong).foreach { command =>
    queued.dequeueFirst(_ == commandId.asLong)
    val error = commandHandlingError(command.envelope)
    log.warning("Command " + command.envelope.command.getClass.getSimpleName + " timed out after " + commandTimeout +
      " and is not applied, errorId: [" + error.errorId + "]")
    command.envelope.respondTo ! error
    startQueued()
    stopIfDone()
  }

  private def finish(commandId: CommandId, response: CustomCommandResponse[_]): Unit = pending.remove(commandId.asLong).foreach { command =>
    command.timeout.cancel()
    command.envelope.respondTo ! response
    startQueued()
    stopIfDone()
  }

  /** An error for the caller; its id is written to the log with the cause, so both can be matched. */
  private def finishWithError(envelope: CommandEnvelope, cause: Throwable): Unit = {
    val error = commandHandlingError(envelope)
    log.error(cause, "Error handling command " + envelope.command.getClass.getSimpleName + ", errorId: [" + error.errorId + "]")
    if (pending.contains(envelope.commandId.asLong)) finish(envelope.commandId, error) else envelope.respondTo ! error
  }

  private def pendingFailed(commandId: CommandId, failure: Throwable): Unit =
    pending.get(commandId.asLong).foreach(command => finishWithError(command.envelope, failure))

  private def commandHandlingError(envelope: CommandEnvelope): CommandHandlingError =
    CommandHandlingError(envelope.command.getClass.getSimpleName, RandomUtil.generateRandomString(16), envelope.commandId)

  private def successResponse(newVersion: AggregateVersion, info: AnyRef): CustomCommandResponse[_] = info match {
    case _: io.reactivecqrs.api.Nothing => SuccessResponse(aggregateId, newVersion)
    case _ => CustomSuccessResponse(aggregateId, newVersion, info)
  }

  private def expectedVersion(command: Any): Option[AggregateVersion] = command match {
    case _: FirstCommand[_, _] => Some(AggregateVersion.ZERO)
    case c: Command[_, _] => Some(c.expectedVersion)
    case c: RewriteHistoryCommand[_, _] => Some(c.expectedVersion)
    case _ => None // concurrent kinds run on the current version
  }

  private def runsAgainOnConflict(command: Any): Boolean =
    command.isInstanceOf[ConcurrentCommand[_, _]] || command.isInstanceOf[RewriteHistoryConcurrentCommand[_, _]]

  private def rewrittenClasses(command: Any): Option[Set[String]] = command match {
    case c: RewriteHistoryCommand[_, _] => Some(c.eventsTypes.map(_.getName))
    case c: RewriteHistoryConcurrentCommand[_, _] => Some(c.eventsTypes.map(_.getName))
    case _ => None
  }

  // D32: the envelope a command gives its events, if it gives one.
  private def metadataOf(command: Any): Option[EventMetadata] = command match {
    case withMetadata: WithMetadata => Some(withMetadata.metadata)
    case _ => None
  }

  private def userIdOf(command: Any): UserId = command match {
    case c: FirstCommand[_, _] => c.userId
    case c: Command[_, _] => c.userId
    case c: ConcurrentCommand[_, _] => c.userId
    case c: RewriteHistoryCommand[_, _] => c.userId
    case c: RewriteHistoryConcurrentCommand[_, _] => c.userId
    case other => throw new IllegalArgumentException("Not a command: " + other.getClass.getName) // the bus sends only commands
  }

  // ************** Stopping

  private def stopIfDone(): Unit = if (stopping && pending.isEmpty) stopNow()

  private def stopNow(withSnapshot: Boolean = true): Unit = {
    if (withSnapshot && snapshots.exists(_.onPassivation) && version > snapshotVersion) writeSnapshot()
    queued.clear()
    pending.values.toList.foreach(command => finishWithError(command.envelope, new IllegalStateException("The aggregate's owner stopped")))
    minVersionQueries.foreach(q => q.respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, q.version)))
    minVersionQueries = Vector.empty
    context.stop(self)
  }

  // ************** Squash (D47–D52)

  // On this owner's thread, between commands: the version and the state stay as they are, so commands in flight are not
  // affected. The squash event is the context's, made from the state at `upTo`; the duplicates that read this
  // aggregate below `upTo` are detached first, each with a squash event of its own state at version 1 (D50).
  private def squashHistory(request: SquashAggregate): SquashResponse = squash match {
    case None => SquashRefused(aggregateId, "squash is not enabled for " + aggregateType.simpleName)
    case Some(_) if request.upTo <= AggregateVersion.ZERO || request.upTo > version =>
      SquashRefused(aggregateId, "version " + request.upTo.asInt + " is not between 1 and " + version.asInt)
    case Some(policy) =>
      val upTo = request.upTo
      // The blocker is checked first, before the states to squash are restored; the store checks it again.
      Try(for {
        _ <- eventStore.squashBlocker(aggregateId, upTo).toLeft(())
        state <- Option(if (upTo == version) root else restorer.restore(aggregateId, Some(Left(upTo))).root)
          .toRight("the aggregate is deleted at version " + upTo.asInt)
        detach = eventStore.duplicatesReading(aggregateId, Some(upTo)).map(duplicate =>
          duplicate -> policy.toEvent(restorer.restore(duplicate, Some(Left(AggregateVersion(1)))).root))
        _ <- eventStore.squash(aggregateId, upTo, policy.toEvent(state), request.userId, request.metadata, detach)
      } yield detach) match {
        case Success(Right(detach)) =>
          if (snapshotVersion <= upTo) snapshotVersion = AggregateVersion.ZERO
          log.info("Aggregate " + aggregateId.asLong + " of type " + aggregateType.simpleName + " squashed up to version " + upTo.asInt +
            (if (detach.isEmpty) "" else ", detaching " + detach.map(_._1.asLong).mkString(", ")))
          AggregateSquashed(aggregateId, upTo)
        case Success(Left(reason)) => SquashRefused(aggregateId, reason)
        case Failure(e) =>
          log.error(e, "Squashing aggregate " + aggregateId.asLong + " up to version " + upTo.asInt + " failed")
          SquashRefused(aggregateId, "the squash failed: " + e.getMessage)
      }
  }

  // ************** Snapshots (D30)

  // Every `every` events, written right after the commit that reaches the next multiple, on this owner's thread: the
  // owner also runs every undo and rewrite of its aggregate, so a write cannot race the deletes they make.
  private def snapshotIfDue(previous: AggregateVersion): Unit = snapshots.foreach { policy =>
    if (version.asInt / policy.every > previous.asInt / policy.every) writeSnapshot()
  }

  private def writeSnapshot(): Unit =
    if (version > AggregateVersion.ZERO && root != null && restorer.writeSnapshot(aggregateId, version, root)) snapshotVersion = version

  // ************** Queries for a version not reached yet (F81)

  private def minVersionQuery(respondTo: ActorRef, minVersion: AggregateVersion, maxMillis: Int): Unit = {
    if (version >= minVersion) {
      respondTo ! Success(aggregate)
    } else {
      minVersionQueries :+= MinVersionQuery(respondTo, minVersion, System.nanoTime() + maxMillis * 1000000L)
      scheduleMinVersionDeadline()
    }
  }

  private def answerReachedMinVersionQueries(): Unit = if (minVersionQueries.nonEmpty) {
    val (reached, waiting) = minVersionQueries.partition(_.version <= version)
    reached.foreach(_.respondTo ! Success(aggregate))
    minVersionQueries = waiting
    scheduleMinVersionDeadline()
  }

  private def answerExpiredMinVersionQueries(): Unit = {
    val now = System.nanoTime()
    val (expired, waiting) = minVersionQueries.partition(_.deadline <= now)
    expired.foreach(q => q.respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, q.version)))
    minVersionQueries = waiting
    scheduleMinVersionDeadline()
  }

  private def scheduleMinVersionDeadline(): Unit = {
    minVersionTimer.foreach(_.cancel())
    minVersionTimer = minVersionQueries.map(_.deadline).minOption.map { deadline =>
      context.system.scheduler.scheduleOnce(math.max(deadline - System.nanoTime(), 0L).nanos, self, MinVersionDeadline)
    }
  }

  // ************** Publication (D8, D20c)

  // The acknowledgement goes to `publicationAcks` (the command bus), which outlives this owner: delivered rows are
  // deleted even when the owner is stopped (or, later, passivated) before the subscribers caught up (F91).
  private def publish(events: Seq[EventInfo[AGGREGATE_ROOT]]): Unit = {
    eventsBus.tell(PublishEvents(aggregateType, events, aggregateId, Option(root)), publicationAcks)
    answerReachedMinVersionQueries()
  }

  // Re-publishes `from` up to the current version with the current state, so a subscriber that receives state ends with
  // a consistent range; subscribers drop what they applied before (D8, D20c). A long range goes in parts of at most
  // `maxRepublishEvents`, so a subscriber far behind never gets the whole history in one message: one to many
  // subscribers continues when the bus acks the part, a subscriber filling its gap asks for the next part. The parts
  // before the last carry the current state too. Returns the last version sent.
  private def republish(from: AggregateVersion, onlyTo: Option[ActorRef]): Option[AggregateVersion] = {
    // D51: nothing below the squash point exists any more; a range from below it starts at the squash event.
    val start = if (from <= version) eventStore.squashedVersion(aggregateId).filter(_ > from).getOrElse(from) else from
    if (start <= version) {
      val to = AggregateVersion(math.min(version.asInt.toLong, start.asInt.toLong + maxRepublishEvents - 1).toInt)
      val events = eventStore.readEventsRange[AGGREGATE_ROOT](aggregateId, start, to)
      if (events.nonEmpty) {
        eventsBus.tell(PublishEvents(aggregateType, events, aggregateId, Option(root), onlyTo), publicationAcks)
        Some(events.last.version)
      } else None
    } else None
  }

  // The command bus deleted the rows of these acknowledged versions and passes the ack on: when it ends the outbox part
  // being re-published, the next part follows.
  private def delivered(versions: Seq[AggregateVersion]): Unit = {
    outboxRepublishedUpTo.filter(versions.contains).foreach { end =>
      outboxRepublishedUpTo = if (end < version) republish(end.increment, None) else None
    }
  }
}
