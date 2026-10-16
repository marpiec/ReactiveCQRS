package io.reactivecqrs.core.aggregaterepository

import java.time.Instant

import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.util.MyActorLogging
import io.reactivecqrs.api._
import org.apache.pekko.actor.{Actor, ActorRef, PoisonPill}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}
import io.reactivecqrs.core.commandhandler.{CommandExecutorActor, Idempotency, IdempotencyKeyTakenException}
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.util.RandomUtil
import org.postgresql.util.PSQLException

import scala.concurrent.duration._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object AggregateRepositoryActor {
  case class GetEventsCurrentVersion(respondTo: ActorRef)
  case class GetAggregateRootCurrentVersion(respondTo: ActorRef)
  case class GetAggregateRootCurrentMinVersion(respondTo: ActorRef, version: AggregateVersion, durationMillis: Int)
  case class GetAggregateRootExactVersion(respondTo: ActorRef, version: AggregateVersion)
  case class GetAggregateRootWithEventsCurrentVersion(respondTo: ActorRef, eventTypes: Set[String])
  case class GetAggregateRootWithEventsExactVersion(respondTo: ActorRef, version: AggregateVersion, eventTypes: Set[String])

  case class IdempotentCommandInfo(command: Any, response: CustomCommandResponse[_])

  case class PersistEvents[AGGREGATE_ROOT](respondTo: ActorRef,
                                            commandId: CommandId,
                                            userId: UserId,
                                            expectedVersion: AggregateVersion,
                                            timestamp: Instant,
                                            events: Seq[Event[AGGREGATE_ROOT]],
                                            commandInfo: Option[IdempotentCommandInfo])

  case class OverrideAndPersistEvents[AGGREGATE_ROOT](rewriteEvents: Iterable[EventWithVersion[AGGREGATE_ROOT]],
                                                      persist: PersistEvents[AGGREGATE_ROOT])



  case class AggregateWithSelectedEvents[AGGREGATE_ROOT](aggregate: Aggregate[AGGREGATE_ROOT], events: Iterable[EventWithVersion[AGGREGATE_ROOT]])
}

case class DelayedMinVersionQuery(respondTo: ActorRef, requestedVersion: AggregateVersion, untilNanoTimestamp: Long)

class AggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                eventStore: EventStoreState,
                                                                idempotency: Idempotency,
                                                                eventsBus: ActorRef,
                                                                eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                                initialState: () => AGGREGATE_ROOT,
                                                                singleReadForVersionOnly: Option[Either[AggregateVersion, Instant]],
                                                                eventsVersionsMap: Map[EventTypeVersion, String],
                                                                eventsVersionsMapReverse: Map[String, EventTypeVersion],
                                                                failedRestoreRetryAfter: FiniteDuration,
                                                                maxRepublishEvents: Int = 1000) extends Actor with MyActorLogging {

  import AggregateRepositoryActor._


  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

  // The version of a PermanentDeleteEvent: once the bus acks it, the aggregate is deleted for good.
  private var permanentDeleteAt: Option[AggregateVersion] = None

  private var delayedQueries = List[DelayedMinVersionQuery]()

  // The last version of the outbox part being re-published (RepublishOutbox): the next part follows its ack.
  private var outboxRepublishedUpTo: Option[AggregateVersion] = None

  // Restoring only reads the state; what waits in the outbox is re-published when the outbox poller asks (D8, F72).
  private def assureRestoredState(): Unit = {
    //TODO make it future
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()
    permanentDeleteAt = None
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, singleReadForVersionOnly)(handleEvent)
  }

  override def preStart(): Unit = {
    // empty
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  // State is restored on the first message, not in the constructor, so a failing restore can be answered
  // instead of stopping the actor silently (F10).
  override def receive: Receive = {
    case message =>
      restore() match {
        case None =>
          context.become(restored)
          restored.applyOrElse(message, unhandled)
        case Some(failure) =>
          failedRestore(failure)(message)
      }
  }

  /** Reads the aggregate from the store, first or again. If that fails, the actor answers every request with the
    * failure and stops after `failedRestoreRetryAfter`, so the next request restores in a new actor (F10). */
  private def restore(): Option[AggregateRestoreFailedException] = {
    try {
      assureRestoredState()
      None
    } catch {
      case e: Exception =>
        log.error(e, "Restoring aggregate " + aggregateId.asLong + " of type " + aggregateType.simpleName + " failed, retry after " + failedRestoreRetryAfter)
        if (singleReadForVersionOnly.isEmpty) {
          context.system.scheduler.scheduleOnce(failedRestoreRetryAfter, self, PoisonPill)(context.dispatcher)
        }
        val failure = new AggregateRestoreFailedException(aggregateId, aggregateType, e)
        context.become(failedRestore(failure))
        Some(failure)
    }
  }

  // Until this actor stops, every request gets the restore failure at once; the command bus then creates a
  // new actor for the next request, which tries the restore again.
  private def failedRestore(failure: AggregateRestoreFailedException): Receive = {
    case GetAggregateRootCurrentVersion(respondTo) => answerRestoreFailed(respondTo, failure)
    case GetEventsCurrentVersion(respondTo) => answerRestoreFailed(respondTo, failure)
    case GetAggregateRootCurrentMinVersion(respondTo, _, _) => answerRestoreFailed(respondTo, failure)
    case GetAggregateRootExactVersion(respondTo, _) => answerRestoreFailed(respondTo, failure)
    case GetAggregateRootWithEventsCurrentVersion(respondTo, _) => answerRestoreFailed(respondTo, failure)
    case GetAggregateRootWithEventsExactVersion(respondTo, _, _) => answerRestoreFailed(respondTo, failure)
    case ee: PersistEvents[_] => answerEventHandlingError(ee, ee.events.head, failure)
    case ee: OverrideAndPersistEvents[_] => answerEventHandlingError(ee.persist, ee.persist.events.head, failure)
    case _ => () // nothing to publish without a restored state
  }

  private def answerEventHandlingError(persist: PersistEvents[_], event: Any, cause: Throwable): Unit = {
    val errorId = RandomUtil.generateRandomString(16)
    persist.respondTo ! EventHandlingError(event.getClass.getSimpleName, errorId, persist.commandId)
    log.error(cause, "Error handling event, errorId: [" + errorId+"]")
  }

  private def answerRestoreFailed(respondTo: ActorRef, failure: AggregateRestoreFailedException): Unit = {
    respondTo ! Failure(failure)
    stopIfSingleRead()
  }

  private lazy val restored: Receive = logReceive {
    case ee: PersistEvents[_] => handlePersistEvents(ee)
    case ee: OverrideAndPersistEvents[_] => handleOverrideAndPersistEvents(ee)
    case GetAggregateRootCurrentVersion(respondTo) => receiveReturnAggregateRoot(respondTo, None)
    case GetEventsCurrentVersion(respondTo) => receiveReturnEvents(respondTo, None)
    case GetAggregateRootCurrentMinVersion(respondTo, version, durationMillis) => receiveReturnAggregateRootMinVersion(respondTo, version, System.nanoTime() + durationMillis * 1_000_000L)
    case GetAggregateRootExactVersion(respondTo, version) => // for following command
      whenUpToDate(version, respondTo)(receiveReturnAggregateRoot(respondTo, Some(version)))
    case GetAggregateRootWithEventsCurrentVersion(respondTo, eventTypes) => receiveReturnAggregateRootWithEvents(respondTo, None, eventTypes) // for following command
    case GetAggregateRootWithEventsExactVersion(respondTo, version, eventTypes) => // for following command
      whenUpToDate(version, respondTo)(receiveReturnAggregateRootWithEvents(respondTo, Some(version), eventTypes))
    case PublishEventsAck(_, versions) => delivered(versions)
    case RepublishOutbox(_) => eventStore.oldestVersionToPublish(aggregateId).foreach(from => outboxRepublishedUpTo = republish(from, None))
    case RepublishRange(_, from, subscriber) => republish(from, Some(subscriber))
  }


  // Runs right after the commit, before the next message: a reload triggered by a later message then already
  // includes these events, instead of having them applied a second time (F69).
  private def applyPersisted(events: Seq[IdentifiableEvent[AGGREGATE_ROOT]]): Unit = {
    if (events.exists(e => e.event.isInstanceOf[UndoEvent[_]] || e.event.isInstanceOf[DuplicationEvent[_]])) {
      // In case of those events it's easier to re read past events. If that fails the state is unknown, so nothing
      // is published now: the outbox poller has the owner re-publish what waits in the outbox.
      if (restore().isEmpty) publish(events)
    } else {
      events.foreach(e => handleEvent(e.userId, e.timestamp, e.event, aggregateId, e.version.asInt, noopEvent = false))
      publish(events)
    }
  }

  // Re-publishes `from` up to the current version with the current state, so a subscriber that receives state ends with
  // a consistent range; subscribers drop what they applied before (D8, D20c). A long range goes in parts of at most
  // `maxRepublishEvents`, so a subscriber far behind never gets the whole history in one message: one to many
  // subscribers continues when the bus acks the part, a subscriber filling its gap asks for the next part. The parts
  // before the last carry the current state too. Returns the last version sent.
  private def republish(from: AggregateVersion, onlyTo: Option[ActorRef]): Option[AggregateVersion] = {
    if (from <= version) {
      val to = AggregateVersion(math.min(version.asInt.toLong, from.asInt.toLong + maxRepublishEvents - 1).toInt)
      val events = eventStore.readEventsRange[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, from, to)
      if (events.nonEmpty) {
        eventsBus ! PublishEvents(aggregateType, events, aggregateId, Option(aggregateRoot), onlyTo)
        Some(events.last.version)
      } else None
    } else None
  }

  private def publish(events: Seq[IdentifiableEvent[AGGREGATE_ROOT]]): Unit = {
    eventsBus ! PublishEvents(aggregateType, events.map(e => EventInfo(e.version, e.event, e.userId, e.timestamp)), aggregateId, Option(aggregateRoot))
    replayDelayedQueries()
  }

  private def handleOverrideAndPersistEvents(ee: OverrideAndPersistEvents[_]): Unit = {
    tryToApplyEvents(ee.persist) match {
      case s: Right[_, _] => overrideAndPersistEvents(ee.asInstanceOf[OverrideAndPersistEvents[AGGREGATE_ROOT]])
      case Left((exception, event)) => answerEventHandlingError(ee.persist, event, exception)
    }
  }

  private def handlePersistEvents(ee: PersistEvents[_]): Unit = {
    tryToApplyEvents(ee) match {
      case s: Right[_, _] => persistEvents(ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]])
      case Left((exception, event)) => answerEventHandlingError(ee, event, exception)
    }
  }

  private def tryToApplyEvents(ee: PersistEvents[_]) = {
    if(ee.events.exists(_.isInstanceOf[DuplicationEvent[_]])) {
      // e don't have previous aggregate state so we can't try events
      Right(aggregateRoot)
    } else {
      ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]].events.foldLeft(Right(aggregateRoot).asInstanceOf[Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT]])((aggEither, event) => {
        aggEither match {
          case Right(agg) => tryToHandleEvent(ee.userId, ee.timestamp, event, noopEvent = false, agg)
          case f: Left[_, _] => f
        }
      })
    }
  }

  private def overrideAndPersistEvents(eventsEnvelope: OverrideAndPersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.persist.expectedVersion == version) {
      persist(eventsEnvelope.persist, eventsEnvelope.rewriteEvents)
      //      println("AggregateRepository persisted events for expected version " + eventsEnvelope.expectedVersion)
    } else {
      eventsEnvelope.persist.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.persist.expectedVersion, version)
      //      println("AggregateRepository AggregateConcurrentModificationError expected " + eventsEnvelope.expectedVersion.asInt + " but i have " + version.asInt)
    }
  }

  private def persistEvents(eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope, Seq.empty)
//      println("AggregateRepository persisted events for expected version " + eventsEnvelope.expectedVersion)
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.expectedVersion, version)
//      println("AggregateRepository AggregateConcurrentModificationError expected " + eventsEnvelope.expectedVersion.asInt + " but i have " + version.asInt)
    }

  }



  // A request for a version this actor has not seen means someone else wrote the aggregate
  // (another node, a history rewrite, a repair): reload before answering instead of rejecting the command (F19).
  private def whenUpToDate(requestedVersion: AggregateVersion, respondTo: ActorRef)(answer: => Unit): Unit = {
    val reloadFailure = if (singleReadForVersionOnly.isEmpty && requestedVersion > version) restore() else None
    reloadFailure match {
      case None => answer
      case Some(failure) => respondTo ! Failure(failure)
    }
  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef, requestedVersion: Option[AggregateVersion]): Unit = {
    if(version == AggregateVersion.ZERO) {
      respondTo ! Success(Aggregate[AGGREGATE_ROOT](aggregateId, version, None))
    } else {
//      println("RepositoryActor "+this.toString+" Someone requested aggregate " + aggregateId.asLong + " of version " + requestedVersion.map(_.asInt.toString).getOrElse("None") + " and now I have version " + version.asInt)
      requestedVersion match {
        case Some(v) if v != version => respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, v))
        case _ => respondTo ! Success(Aggregate[AGGREGATE_ROOT](aggregateId, version, Option(aggregateRoot)))
      }

    }

    stopIfSingleRead()

  }

  private def receiveReturnEvents(respondTo: ActorRef, requestedVersion: Option[AggregateVersion]): Unit = {
    if(version == AggregateVersion.ZERO) {
      val events: List[EventInfo[AGGREGATE_ROOT]] = List.empty
      respondTo ! Success(events)
    } else {

      //      println("RepositoryActor "+this.toString+" Someone requested aggregate " + aggregateId.asLong + " of version " + requestedVersion.map(_.asInt.toString).getOrElse("None") + " and now I have version " + version.asInt)
      requestedVersion match {
        case Some(v) if v != version => respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, v))
        case _ => {
          var events: List[EventInfo[AGGREGATE_ROOT]] = List.empty
          eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, singleReadForVersionOnly)((userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], aggId: AggregateId, eventVersion: Int, noopEvent: Boolean) => {
            events ::= EventInfo(AggregateVersion(eventVersion), event, userId, timestamp)
          })
          respondTo ! Success(events.reverse)
        }
      }
    }
    stopIfSingleRead()
  }

  // An actor made for one historical read answers once and stops (the events read did not, F74).
  private def stopIfSingleRead(): Unit = if (singleReadForVersionOnly.isDefined) self ! PoisonPill

  private def replayDelayedQueries(): Unit = {
    if(delayedQueries.nonEmpty) {
      val now = System.nanoTime()
      val queries = delayedQueries
      delayedQueries = List.empty
      queries.foreach(q =>
        if(q.untilNanoTimestamp > now) {
          receiveReturnAggregateRootMinVersion(q.respondTo, q.requestedVersion, q.untilNanoTimestamp)
        })
    }
  }

  private def receiveReturnAggregateRootMinVersion(respondTo: ActorRef, requestedVersion: AggregateVersion, timeoutNanoTimestamp: Long): Unit = {
    if(version >= requestedVersion) {
      respondTo ! Success(Aggregate[AGGREGATE_ROOT](aggregateId, version, Option(aggregateRoot)))
    } else {
      val now = System.nanoTime()
      delayedQueries = DelayedMinVersionQuery(respondTo, requestedVersion, timeoutNanoTimestamp) :: delayedQueries.filter(_.untilNanoTimestamp > now)
    }
  }


  private def receiveReturnAggregateRootWithEvents(respondTo: ActorRef, requestedVersion: Option[AggregateVersion], eventTypes: Set[String]): Unit = {
    if(version == AggregateVersion.ZERO) {
      val events: List[EventWithVersion[AGGREGATE_ROOT]] = List.empty
      respondTo ! Success(AggregateWithSelectedEvents[AGGREGATE_ROOT](Aggregate[AGGREGATE_ROOT](aggregateId, version, None), events))
    } else {

      //      println("RepositoryActor "+this.toString+" Someone requested aggregate " + aggregateId.asLong + " of version " + requestedVersion.map(_.asInt.toString).getOrElse("None") + " and now I have version " + version.asInt)
      requestedVersion match {
        case Some(v) if v != version => respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, v))
        case _ => {
          var events: List[EventWithVersion[AGGREGATE_ROOT]] = List.empty
          eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, singleReadForVersionOnly)((userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], aggId: AggregateId, eventVersion: Int, noopEvent: Boolean) => {
            if(eventTypes.contains(event.getClass.getName)) {
              events ::= EventWithVersion(AggregateVersion(eventVersion), event)
            }
          })
          respondTo ! Success(AggregateWithSelectedEvents[AGGREGATE_ROOT](Aggregate[AGGREGATE_ROOT](aggregateId, version, Option(aggregateRoot)), events.reverse))
        }
      }

    }

    stopIfSingleRead()

  }
  private def persist(eventsEnvelope: PersistEvents[AGGREGATE_ROOT], overwrite: Iterable[EventWithVersion[AGGREGATE_ROOT]]): Unit = {
    // Every failure is thrown inside the transaction, so nothing of the command is stored (F68), and every
    // outcome becomes exactly one reply to the executor (AR3).
    val outcome = Try {
      eventStore.localTx { implicit session =>
        if (overwrite.nonEmpty) {
          eventStore.overwriteEvents(aggregateId, overwrite)
        }
        val eventsWithVersions = eventStore.persistEvents(eventsVersionsMapReverse, aggregateId, eventsEnvelope.asInstanceOf[PersistEvents[AnyRef]]).get
        eventsEnvelope.commandInfo.foreach(info => idempotency.record(info.command, info.response))
        eventsWithVersions
      }
    }

    outcome match {
      case Success(eventsWithVersions) =>
        applyPersisted(eventsWithVersions.map { case (event, eventVersion) =>
          IdentifiableEvent(AggregateType(event.aggregateRootType.toString), aggregateId, eventVersion, event.asInstanceOf[Event[AGGREGATE_ROOT]], eventsEnvelope.userId, eventsEnvelope.timestamp)
        })
        eventsEnvelope.respondTo ! CommandExecutorActor.AggregateModified
      case Failure(_: IdempotencyKeyTakenException) =>
        // A copy of this idempotent command was applied first; the executor answers with its stored response.
        eventsEnvelope.respondTo ! CommandExecutorActor.IdempotencyKeyTaken
      case Failure(e: PSQLException) if e.getLocalizedMessage.contains("Concurrent aggregate modification exception") =>
        // The store moved on without this actor (another writer): reload, so a retry sees the real state (F19).
        restore() match {
          case None => eventsEnvelope.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.expectedVersion, version)
          case Some(failure) => answerEventHandlingError(eventsEnvelope, eventsEnvelope.events.head, failure)
        }
      case Failure(exception) =>
        answerEventHandlingError(eventsEnvelope, eventsEnvelope.events.head, exception)
    }
  }

  private def tryToHandleEvent(userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], noopEvent: Boolean, tmpAggregateRoot: AGGREGATE_ROOT): Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT] = {
    if(!noopEvent) {
      try {
        Right(eventHandlers(userId, timestamp, tmpAggregateRoot)(event))
      } catch {
        case e: Exception =>
          log.error(e, "Error while handling event "+event.getClass.getSimpleName+" tryout")
          Left((e, event))
      }
    } else {
      Right(tmpAggregateRoot)
    }
  }

  private def handleEvent(userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], aggId: AggregateId, eventVersion: Int, noopEvent: Boolean): Unit = {
    if(!noopEvent) {
      try {
        aggregateRoot = eventHandlers(userId, timestamp, aggregateRoot)(event)
      } catch {
        case e: Exception =>
          log.error(e, "Error while handling event " + event.getClass.getSimpleName)
          throw e;
      }
    }

    if(aggregateId == aggId) { // otherwise it's event from base aggregate we don't want to count
      version = version.increment
      if (!noopEvent && event.isInstanceOf[PermanentDeleteEvent[_]]) permanentDeleteAt = Some(version)
    }
  }

  // The bus acked these versions: every subscriber took or failed them, so their outbox rows go. A failed delete only
  // leaves rows that the outbox poller re-publishes later. A permanent delete waits while a subscription of the
  // aggregate is failed: its event stays in the outbox and is tried again on the next re-publication's ack.
  private def delivered(versions: Seq[AggregateVersion]): Unit = {
    Try {
      permanentDeleteAt.filter(versions.contains) match {
        case Some(at) if !eventStore.deleteAggregatePermanently(aggregateId) =>
          log.warning("Aggregate " + aggregateId.asLong + " of type " + aggregateType.simpleName + " is not deleted yet: a subscription of it is failed")
          eventStore.deletePublished(aggregateId, versions.filterNot(_ == at))
        case Some(_) => ()
        case None => eventStore.deletePublished(aggregateId, versions)
      }
    }.failed.foreach(e => log.error(e, "Removing delivered events of aggregate " + aggregateId.asLong + " from the outbox failed"))
    outboxRepublishedUpTo.filter(versions.contains).foreach { end =>
      outboxRepublishedUpTo = if (end < version) republish(end.increment, None) else None
    }
  }


}
