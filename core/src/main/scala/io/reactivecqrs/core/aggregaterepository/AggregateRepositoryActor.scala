package io.reactivecqrs.core.aggregaterepository

import _root_.io.reactivecqrs.core.commandhandler.ResultAggregator
import _root_.io.reactivecqrs.core.eventstore.EventStoreState
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import _root_.io.reactivecqrs.core.errors.AggregateConcurrentModificationError
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}

import scala.concurrent.Future
import scala.reflect._
import scala.reflect.runtime.universe._

object AggregateRepositoryActor {
  case class GetAggregateRoot(respondTo: ActorRef)

  case class PersistEvents[AGGREGATE_ROOT](respondTo: ActorRef,
                                            aggregateId: AggregateId,
                                            commandId: CommandId,
                                            userId: UserId,
                                            expectedVersion: AggregateVersion,
                                            events: Seq[Event[AGGREGATE_ROOT]])


  case class EventsPersisted[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])



}


class AggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](id: AggregateId,
                                                         eventStore: EventStoreState,
                                                         eventsBus: ActorRef,
                                                         eventHandlers: AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT],
                                                         initialState: () => AGGREGATE_ROOT) extends Actor {

  import AggregateRepositoryActor._


  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private var notRestored = true


  private def assureRestoredState(): Unit = {
    if(notRestored) {
      //TODO make it future
      eventStore.readAllEvents[AGGREGATE_ROOT](id)(handleEvent)
      notRestored = false
    }
  }



  override def receive = LoggingReceive {
    case ep: EventsPersisted[_] =>
      ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events.foreach(eventIdentifier => handleEvent(eventIdentifier.event))
      eventsBus ! PublishEvents(AggregateType(classTag[AGGREGATE_ROOT].toString), ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events, id, version, Option(aggregateRoot))
    case ee: PersistEvents[_] =>
      assureRestoredState()
      handlePersistEvents(ee.asInstanceOf[PersistEvents[AGGREGATE_ROOT]])
    case GetAggregateRoot(respondTo) =>
      assureRestoredState()
      receiveReturnAggregateRoot(respondTo)
    case PublishEventsAck(events) =>
      markPublishedEvents(events)
  }


  private def handlePersistEvents(eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope)(respond(eventsEnvelope.respondTo))
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(eventsEnvelope.expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    respondTo ! Aggregate[AGGREGATE_ROOT](id, version, Some(aggregateRoot))
  }


  private def persist(eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(afterPersist: Seq[Event[AGGREGATE_ROOT]] => Unit): Unit = {
    import context.dispatcher
    Future {
      eventStore.persistEvents(id, eventsEnvelope.asInstanceOf[PersistEvents[AnyRef]])
      var mappedEvents = 0
      self ! EventsPersisted(eventsEnvelope.events.map { event =>
        val eventVersion = eventsEnvelope.expectedVersion.incrementBy(mappedEvents + 1)
        mappedEvents += 1
        IdentifiableEvent(AggregateType(event.aggregateRootType.toString), eventsEnvelope.aggregateId, eventVersion, event)
      })
      afterPersist(eventsEnvelope.events)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  private def respond(respondTo: ActorRef)(events: Seq[Event[AGGREGATE_ROOT]]): Unit = {
    respondTo ! ResultAggregator.AggregateModified
  }

  private def handleEvent(event: Event[AGGREGATE_ROOT]): Unit = {
//    aggregateRoot = eventHandlers(event.getClass.getName) match {
//      case handler: FirstEventHandler[_, _] => handler.asInstanceOf[FirstEventHandler[AGGREGATE_ROOT, FirstEvent[AGGREGATE_ROOT]]].handle(event.asInstanceOf[FirstEvent[AGGREGATE_ROOT]])
//      case handler: EventHandler[_, _] => handler.asInstanceOf[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]].handle(aggregateRoot, event)
//    }
    aggregateRoot = eventHandlers(aggregateRoot)(event)

    version = version.increment
  }

  def markPublishedEvents(events: Seq[EventIdentifier]): Unit = {
    import context.dispatcher
    Future { // Fire and forget
      eventStore.deletePublishedEvents(events)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


}
