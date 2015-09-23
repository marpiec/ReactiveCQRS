package io.reactivecqrs.core.aggregaterepository

import _root_.io.reactivecqrs.api._
import _root_.io.reactivecqrs.core.commandhandler.ResultAggregator
import _root_.io.reactivecqrs.core.errors.AggregateConcurrentModificationError
import _root_.io.reactivecqrs.core.eventstore.EventStoreState
import akka.actor.{PoisonPill, Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

object AggregateRepositoryActor {
  case class GetAggregateRoot(respondTo: ActorRef)
  case class GetAggregateRootForVersion(respondTo: ActorRef, version: AggregateVersion)

  case class PersistEvents[AGGREGATE_ROOT](respondTo: ActorRef,
                                            aggregateId: AggregateId,
                                            commandId: CommandId,
                                            userId: UserId,
                                            expectedVersion: AggregateVersion,
                                            events: Seq[Event[AGGREGATE_ROOT]])


  case class EventsPersisted[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])

  case object ResendPersistedMessages
}


class AggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](id: AggregateId,
                                                         eventStore: EventStoreState,
                                                         eventsBus: ActorRef,
                                                         eventHandlers: AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT],
                                                         initialState: () => AGGREGATE_ROOT,
                                                         singleReadForVersionOnly: Option[AggregateVersion]) extends Actor {

  import AggregateRepositoryActor._


  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

  private var eventsToPublish = List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]()


  private def assureRestoredState(): Unit = {
    //TODO make it future
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](id, singleReadForVersionOnly)(handleEvent)

    eventsToPublish = eventStore.readEventsToPublishForAggregate[AGGREGATE_ROOT](id)
    resendEventsToPublish()
  }

  private def resendEventsToPublish(): Unit = {
    if(eventsToPublish.nonEmpty) {
      eventsBus ! PublishEvents(aggregateType, eventsToPublish.map(e => IdentifiableEvent(aggregateType, id, e.version, e.event)), id, version, Option(aggregateRoot))
    }
  }

  assureRestoredState()

  context.system.scheduler.schedule(60.seconds, 60.seconds, self, ResendPersistedMessages)(context.dispatcher)



  override def receive = LoggingReceive {
    case ep: EventsPersisted[_] =>
      if(ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events.exists(_.event.isInstanceOf[UndoEvent[_]]) ||
        ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events.exists(_.event.isInstanceOf[DuplicationEvent[_]])) {
        // In case of those events it's easier to re read past events
        assureRestoredState()
      } else {
        ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events.foreach(eventIdentifier => handleEvent(eventIdentifier.event, id, false))
      }
      eventsBus ! PublishEvents(aggregateType, ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events, id, version, Option(aggregateRoot))
    case ee: PersistEvents[_] =>
      handlePersistEvents(ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]])
    case GetAggregateRoot(respondTo) =>
      receiveReturnAggregateRoot(respondTo)
    case PublishEventsAck(events) =>
      markPublishedEvents(events)
    case ResendPersistedMessages =>
      resendEventsToPublish()
  }


  private def handlePersistEvents(eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope)(respond(eventsEnvelope.respondTo))
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(eventsEnvelope.expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    if(version == AggregateVersion.ZERO) {
      respondTo ! Failure(new NoEventsForAggregateException(id))
    } else {
      respondTo ! Success(Aggregate[AGGREGATE_ROOT](id, version, Some(aggregateRoot)))
    }
    
    if(singleReadForVersionOnly.isDefined) {
      self ! PoisonPill
    }

  }


  private def persist(eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(afterPersist: Seq[Event[AGGREGATE_ROOT]] => Unit): Unit = {
    //Future { FIXME this future can broke order in which events are stored
      eventStore.persistEvents(id, eventsEnvelope.asInstanceOf[PersistEvents[AnyRef]])
      var mappedEvents = 0
      self ! EventsPersisted(eventsEnvelope.events.map { event =>
        val eventVersion = eventsEnvelope.expectedVersion.incrementBy(mappedEvents + 1)
        mappedEvents += 1
        IdentifiableEvent(AggregateType(event.aggregateRootType.toString), eventsEnvelope.aggregateId, eventVersion, event)
      })
      afterPersist(eventsEnvelope.events)
//    } onFailure {
//      case e: Exception => throw new IllegalStateException(e)
//    }
  }

  private def respond(respondTo: ActorRef)(events: Seq[Event[AGGREGATE_ROOT]]): Unit = {
    respondTo ! ResultAggregator.AggregateModified
  }

  private def handleEvent(event: Event[AGGREGATE_ROOT], aggregateId: AggregateId, noopEvent: Boolean): Unit = {
//    aggregateRoot = eventHandlers(event.getClass.getName) match {
//      case handler: FirstEventHandler[_, _] => handler.asInstanceOf[FirstEventHandler[AGGREGATE_ROOT, FirstEvent[AGGREGATE_ROOT]]].handle(event.asInstanceOf[FirstEvent[AGGREGATE_ROOT]])
//      case handler: EventHandler[_, _] => handler.asInstanceOf[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]].handle(aggregateRoot, event)
//    }
    if(!noopEvent) {
      aggregateRoot = eventHandlers(aggregateRoot)(event)
    }

    if(aggregateId == id) { // otherwise it's event from base aggregate we don't want to count
      version = version.increment
    }
  }

  def markPublishedEvents(events: Seq[EventIdentifier]): Unit = {
    import context.dispatcher
    eventsToPublish = eventsToPublish.filterNot(e => events.exists(ep => ep.version == e.version))
    Future { // Fire and forget
      eventStore.deletePublishedEventsToPublish(events)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


}
