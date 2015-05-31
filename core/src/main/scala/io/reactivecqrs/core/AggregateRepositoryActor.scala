package io.reactivecqrs.core

import io.reactivecqrs.core.api.{IdentifiableEvent, EventIdentifier}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId, CommandId}
import io.reactivecqrs.core.EventsBusActor.{EventsPublishAck, PublishEvents}
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive

import scala.concurrent.Future
import scala.reflect._





object AggregateRepositoryActor {
  case class ReturnAggregateRoot(respondTo: ActorRef)

  case class EventsEnvelope[AGGREGATE_ROOT](respondTo: ActorRef,
                                            aggregateId: AggregateId,
                                            commandId: CommandId,
                                            userId: UserId,
                                            expectedVersion: AggregateVersion,
                                            events: Seq[Event[AGGREGATE_ROOT]])


  case class EventsPersisted[AGGREGATE_ROOT](events: Seq[IdentifiableEvent[AGGREGATE_ROOT]])



}


class AggregateRepositoryActor[AGGREGATE_ROOT: ClassTag](id: AggregateId,
                                                         eventsBus: ActorRef,
                                                         eventHandlers: Map[String, AbstractEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]]) extends Actor {

  import AggregateRepositoryActor._


  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = _
  private var notRestored = true


  private val eventStore = new EventStore

  private def assureRestoredState(): Unit = {
    if(notRestored) {
      println("Restoring state")
      //TODO make it future
      eventStore.readAllEvents[AGGREGATE_ROOT](id)(handleEvent)
      notRestored = false
    }
  }



  override def receive = LoggingReceive {
    case ep: EventsPersisted[_] =>
      eventsBus ! PublishEvents(ep.events)
      ep.asInstanceOf[EventsPersisted[AGGREGATE_ROOT]].events.foreach(eventIdentifier => handleEvent(eventIdentifier.event))
    case ee: EventsEnvelope[_] =>
      assureRestoredState()
      receiveEvent(ee.asInstanceOf[EventsEnvelope[AGGREGATE_ROOT]])
    case ReturnAggregateRoot(respondTo) =>
      assureRestoredState()
      receiveReturnAggregateRoot(respondTo)
    case EventsPublishAck(events) =>
      markPublishedEvents(events)
  }


  private def receiveEvent(eventsEnvelope: EventsEnvelope[AGGREGATE_ROOT]): Unit = {
    println("Received event " + eventsEnvelope.events.head +" for version " + eventsEnvelope.expectedVersion +" when version was " + version)
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope)(respond(eventsEnvelope.respondTo))
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(eventsEnvelope.expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    println("ReturnAggregateRoot " + aggregateRoot)
    respondTo ! Aggregate(id, version, Some(aggregateRoot))
  }


  private def persist(eventsEnvelope: EventsEnvelope[AGGREGATE_ROOT])(afterPersist: Seq[Event[AGGREGATE_ROOT]] => Unit): Unit = {
    import context.dispatcher
    Future {
      eventStore.persistEvents(id, eventsEnvelope.asInstanceOf[EventsEnvelope[AnyRef]])
      var mappedEvents = 0
      self ! EventsPersisted(eventsEnvelope.events.map { event =>
        val eventVersion = eventsEnvelope.expectedVersion.incrementBy(mappedEvents + 1)
        mappedEvents += 1
        IdentifiableEvent(eventsEnvelope.aggregateId, eventVersion, event)
      })
      afterPersist(eventsEnvelope.events)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  private def respond(respondTo: ActorRef)(events: Seq[Event[AGGREGATE_ROOT]]): Unit = {
    println("Updating state and responding")
    respondTo ! ResultAggregator.AggregateAck
  }

  private def handleEvent(event: Event[AGGREGATE_ROOT]): Unit = {
    println("Updating state by handling " + event)
    aggregateRoot = eventHandlers(event.getClass.getName) match {
      case handler: FirstEventHandler[_, _] => handler.asInstanceOf[FirstEventHandler[AGGREGATE_ROOT, FirstEvent[AGGREGATE_ROOT]]].handle(event.asInstanceOf[FirstEvent[AGGREGATE_ROOT]])
      case handler: EventHandler[_, _] => handler.asInstanceOf[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]].handle(aggregateRoot, event)
    }
    version = version.increment
  }

  def markPublishedEvents(events: Seq[EventIdentifier]): Unit = {
    import context.dispatcher
    Future { // Fire and forget
      eventStore.clearEventsBroadcast(events)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }


}
