package io.reactivecqrs.core.aggregaterepository

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.util.ActorLogging
import io.reactivecqrs.api._
import akka.actor.{Actor, ActorRef, PoisonPill}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}
import io.reactivecqrs.core.commandhandler.{CommandExecutorActor, CommandResponseState}
import org.postgresql.util.PSQLException
import scalikejdbc.DBSession

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

object AggregateRepositoryActor {
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


  case class EventsPersisted[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])

  case object ResendPersistedMessages


  case class AggregateWithSelectedEvents[AGGREGATE_ROOT](aggregate: Aggregate[AGGREGATE_ROOT], events: Iterable[EventWithVersion[AGGREGATE_ROOT]])
}

case class DelayedMinVersionQuery(respondTo: ActorRef, requestedVersion: AggregateVersion, untilTimestamp: Long)

class AggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                eventStore: EventStoreState,
                                                                commandResponseState: CommandResponseState,
                                                                eventsBus: ActorRef,
                                                                eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                                initialState: () => AGGREGATE_ROOT,
                                                                singleReadForVersionOnly: Option[AggregateVersion],
                                                                eventsVersionsMap: Map[EventTypeVersion, String],
                                                                eventsVersionsMapReverse: Map[String, EventTypeVersion]) extends Actor with ActorLogging {

  import AggregateRepositoryActor._


  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

  private var eventsToPublish = List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]()

  private var pendingPublish = List[EventWithIdentifier[AGGREGATE_ROOT]]()

  private var delayedQueries = List[DelayedMinVersionQuery]()

  private def assureRestoredState(): Unit = {
    //TODO make it future
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, singleReadForVersionOnly)(handleEvent)

    if(singleReadForVersionOnly.isEmpty) {
      eventsToPublish = eventStore.readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap, aggregateId)

      context.system.scheduler.scheduleOnce(10.seconds, self, ResendPersistedMessages)(context.dispatcher)
    }
  }

  private def resendEventsToPublish(): Unit = {
    if(eventsToPublish.nonEmpty) {
      log.info("Resending messages for " + aggregateType+" "+aggregateId+" " + eventsToPublish.map(e => e.event.getClass.getSimpleName+" "+e.version))
      eventsBus ! PublishEvents(aggregateType, eventsToPublish.map(e => EventInfo(e.version, e.event, e.userId, e.timestamp)), aggregateId, Option(aggregateRoot))

      pendingPublish = (eventsToPublish.map(e => EventWithIdentifier[AGGREGATE_ROOT](e.aggregateId, e.version, e.event)) ::: pendingPublish).distinct

      context.system.scheduler.scheduleOnce(60.seconds, self, ResendPersistedMessages)(context.dispatcher)
    }
  }

  assureRestoredState()

  override def preStart(): Unit = {
    // empty
  }

  override def postRestart(reason: Throwable) {
    // do not call preStart
  }

  private def stackTraceToString(e: Throwable) = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  override def receive = logReceive {
    case ee: PersistEvents[_] => handlePersistEvents(ee)
    case ee: OverrideAndPersistEvents[_] => handleOverrideAndPersistEvents(ee)
    case ep: EventsPersisted[_] => handleEventsPersisted(ep)
    case GetAggregateRootCurrentVersion(respondTo) => receiveReturnAggregateRoot(respondTo, None)
    case GetAggregateRootCurrentMinVersion(respondTo, version, durationMillis) => receiveReturnAggregateRootMinVersion(respondTo, version, System.currentTimeMillis() + durationMillis)
    case GetAggregateRootExactVersion(respondTo, version) => receiveReturnAggregateRoot(respondTo, Some(version)) // for following command
    case GetAggregateRootWithEventsCurrentVersion(respondTo, eventTypes) => receiveReturnAggregateRootWithEvents(respondTo, None, eventTypes) // for following command
    case GetAggregateRootWithEventsExactVersion(respondTo, version, eventTypes) => receiveReturnAggregateRootWithEvents(respondTo, Some(version), eventTypes) // for following command
    case PublishEventsAck(aggId, versions) => markPublishedEvents(aggregateId, versions)
    case ResendPersistedMessages => resendEventsToPublish()
  }


  private def handleEventsPersisted(ep: EventsPersisted[_]): Unit = {
    val events = ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events
    if (events.exists(_.event.isInstanceOf[UndoEvent[_]]) ||
      events.exists(_.event.isInstanceOf[DuplicationEvent[_]])) {
      // In case of those events it's easier to re read past events
      assureRestoredState()
    } else {
      events.foreach(eventIdentifier => handleEvent(eventIdentifier.userId, eventIdentifier.timestamp, eventIdentifier.event, aggregateId, eventIdentifier.version.asInt, noopEvent = false))
    }
    eventsBus ! PublishEvents(aggregateType, events.map(e => EventInfo(e.version, e.event, e.userId, e.timestamp)), aggregateId, Option(aggregateRoot))
    pendingPublish = (events.map(e => EventWithIdentifier[AGGREGATE_ROOT](e.aggregateId, e.version, e.event)).toList ::: pendingPublish).distinct
    replayDelayedQueries()
  }

  private def handleOverrideAndPersistEvents(ee: OverrideAndPersistEvents[_]): Unit = {
    tryToApplyEvents(ee.persist) match {
      case s: Right[_, _] => overrideAndPersistEvents(ee.asInstanceOf[OverrideAndPersistEvents[AGGREGATE_ROOT]])
      case Left((exception, event)) =>
        ee.persist.respondTo ! EventHandlingError(event.getClass.getSimpleName, stackTraceToString(exception), ee.persist.commandId)
        log.error(exception, "Error handling event")
    }
  }

  private def handlePersistEvents(ee: PersistEvents[_]): Unit = {
    tryToApplyEvents(ee) match {
      case s: Right[_, _] => persistEvents(ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]])
      case Left((exception, event)) =>
        ee.respondTo ! EventHandlingError(event.getClass.getSimpleName, stackTraceToString(exception), ee.commandId)
        log.error(exception, "Error handling event")
    }
  }

  private def tryToApplyEvents(ee: PersistEvents[_]) = {
    ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]].events.foldLeft(Right(aggregateRoot).asInstanceOf[Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT]])((aggEither, event) => {
      aggEither match {
        case Right(agg) => tryToHandleEvent(ee.userId, ee.timestamp, event, noopEvent = false, agg)
        case f: Left[_, _] => f
      }
    })
  }

  private def overrideAndPersistEvents(eventsEnvelope: OverrideAndPersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.persist.expectedVersion == version) {
      persist(eventsEnvelope.persist, eventsEnvelope.rewriteEvents)(respond(eventsEnvelope.persist.respondTo))
      //      println("AggregateRepository persisted events for expected version " + eventsEnvelope.expectedVersion)
    } else {
      eventsEnvelope.persist.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.persist.expectedVersion, version)
      //      println("AggregateRepository AggregateConcurrentModificationError expected " + eventsEnvelope.expectedVersion.asInt + " but i have " + version.asInt)
    }
  }

  private def persistEvents(eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope, Seq.empty)(respond(eventsEnvelope.respondTo))
//      println("AggregateRepository persisted events for expected version " + eventsEnvelope.expectedVersion)
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.expectedVersion, version)
//      println("AggregateRepository AggregateConcurrentModificationError expected " + eventsEnvelope.expectedVersion.asInt + " but i have " + version.asInt)
    }

  }



  private def receiveReturnAggregateRoot(respondTo: ActorRef, requestedVersion: Option[AggregateVersion]): Unit = {
    if(version == AggregateVersion.ZERO) {
      respondTo ! Failure(new NoEventsForAggregateException(aggregateId, aggregateType))
    } else {
//      println("RepositoryActor "+this.toString+" Someone requested aggregate " + aggregateId.asLong + " of version " + requestedVersion.map(_.asInt.toString).getOrElse("None") + " and now I have version " + version.asInt)
      requestedVersion match {
        case Some(v) if v != version => respondTo ! Failure(new AggregateInIncorrectVersionException(aggregateId, aggregateType, version, v))
        case _ => respondTo ! Success(Aggregate[AGGREGATE_ROOT](aggregateId, version, Some(aggregateRoot)))
      }

    }

    if(singleReadForVersionOnly.isDefined) {
      self ! PoisonPill
    }

  }

  private def replayDelayedQueries(): Unit = {
    if(delayedQueries.nonEmpty) {
      val now = System.currentTimeMillis()
      val queries = delayedQueries
      delayedQueries = List.empty
      queries.foreach(q =>
        if(q.untilTimestamp > now) {
          receiveReturnAggregateRootMinVersion(q.respondTo, q.requestedVersion, q.untilTimestamp)
        })
    }
  }

  private def receiveReturnAggregateRootMinVersion(respondTo: ActorRef, requestedVersion: AggregateVersion, timeoutTimestamp: Long): Unit = {
    if(version >= requestedVersion) {
      respondTo ! Success(Aggregate[AGGREGATE_ROOT](aggregateId, version, Some(aggregateRoot)))
    } else {
      val now = System.currentTimeMillis()
      delayedQueries = DelayedMinVersionQuery(respondTo, requestedVersion, timeoutTimestamp) :: delayedQueries.filter(_.untilTimestamp > now)
    }
  }


  private def receiveReturnAggregateRootWithEvents(respondTo: ActorRef, requestedVersion: Option[AggregateVersion], eventTypes: Set[String]): Unit = {
    if(version == AggregateVersion.ZERO) {
      respondTo ! Failure(new NoEventsForAggregateException(aggregateId, aggregateType))
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
          respondTo ! Success(AggregateWithSelectedEvents[AGGREGATE_ROOT](Aggregate[AGGREGATE_ROOT](aggregateId, version, Some(aggregateRoot)), events.reverse))
        }
      }

    }

    if(singleReadForVersionOnly.isDefined) {
      self ! PoisonPill
    }

  }
  private def persist(eventsEnvelope: PersistEvents[AGGREGATE_ROOT], overwrite: Iterable[EventWithVersion[AGGREGATE_ROOT]])(afterPersist: Seq[Event[AGGREGATE_ROOT]] => Unit): Unit = {
    //Future { FIXME this future can broke order in which events are stored
    val eventsWithVersionsTry = eventStore.localTx {implicit session =>

      if(overwrite.nonEmpty) {
        eventStore.overwriteEvents(aggregateId, overwrite)
      }

      eventStore.persistEvents(eventsVersionsMapReverse, aggregateId, eventsEnvelope.asInstanceOf[PersistEvents[AnyRef]]) match {
        case Failure(exception) => Failure(exception)
        case Success(eventsWithVersions) =>
          persistIdempotentCommandResponse(eventsEnvelope.commandInfo)
          Success(eventsWithVersions)
      }
    }

    eventsWithVersionsTry match {
      case Failure(exception) => exception match {
        case e: PSQLException if e.getLocalizedMessage.contains("Concurrent aggregate modification exception") => eventsEnvelope.respondTo ! AggregateConcurrentModificationError(aggregateId, aggregateType, eventsEnvelope.expectedVersion, version)
        case e => eventsEnvelope.respondTo ! EventHandlingError(eventsEnvelope.events.head.getClass.getSimpleName, stackTraceToString(e), eventsEnvelope.commandId)
      }
      case Success(eventsWithVersions) =>
        var mappedEvents = 0
        self ! EventsPersisted(eventsWithVersions.map { case (event, eventVersion) =>
          mappedEvents += 1
          IdentifiableEvent(AggregateType(event.aggregateRootType.toString), aggregateId, eventVersion, event, eventsEnvelope.userId, eventsEnvelope.timestamp)
        })
        afterPersist(eventsEnvelope.events)
    }
//    } onFailure {
//      case e: Exception => throw new IllegalStateException(e)
//    }
  }

  private def persistIdempotentCommandResponse(commandInfo: Option[IdempotentCommandInfo])(implicit session: DBSession): Unit = {
    commandInfo match {
      case Some(ci) =>
        ci.command match {
          case idm: IdempotentCommand[_] if idm.idempotencyId.isDefined =>
            val key = idm.idempotencyId.get.asDbKey
            commandResponseState.storeResponse(key, ci.response)
          case _ => ()
        }
      case None => ()
    }

  }

  private def respond(respondTo: ActorRef)(events: Seq[Event[AGGREGATE_ROOT]]): Unit = {
    respondTo ! CommandExecutorActor.AggregateModified
  }

  private def tryToHandleEvent(userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], noopEvent: Boolean, tmpAggregateRoot: AGGREGATE_ROOT): Either[(Exception, Event[AGGREGATE_ROOT]), AGGREGATE_ROOT] = {
    if(!noopEvent) {
      try {
        Right(eventHandlers(userId, timestamp, tmpAggregateRoot)(event))
      } catch {
        case e: Exception =>
          log.error(e, "Error while handling event tryout : " + event +", aggregateRoot: " + aggregateRoot)
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
          log.error(e, "Error while handling event: " + event +", aggregateRoot: " + aggregateRoot)
          throw e;
      }
    }

    if(aggregateId == aggId) { // otherwise it's event from base aggregate we don't want to count
      version = version.increment
    }
  }

  def markPublishedEvents(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit = {
    import context.dispatcher
    eventsToPublish = eventsToPublish.filterNot(e => e.aggregateId == aggregateId && versions.contains(e.version))

    val (published, remaining) = pendingPublish.partition(e => e.aggregateId == aggregateId && versions.contains(e.version))

    pendingPublish = remaining

    Future { // Fire and forget
      eventStore.deletePublishedEventsToPublish(published.map(v => EventWithIdentifier(aggregateId, v.version, v.event)))

      if(published.exists(_.event.isInstanceOf[PermanentDeleteEvent[_]])) {
        self ! PoisonPill
      }
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


}
