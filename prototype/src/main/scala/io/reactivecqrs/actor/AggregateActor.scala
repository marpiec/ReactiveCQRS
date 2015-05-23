package io.reactivecqrs.actor

import _root_.io.reactivecqrs.api.guid.AggregateId
import _root_.io.reactivecqrs.core._
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive

import scala.concurrent.Future
import scala.reflect._

case object AggregateAck
case class AggregateConcurrentModificationError(expected: AggregateVersion, was: AggregateVersion)

case class ReturnAggregateRoot(respondTo: ActorRef)

case class AggregateRoot[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])




class AggregateRepositoryPersistentActor[AGGREGATE_ROOT](val id: AggregateId,
                                                                   eventsHandlersSeq: Seq[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                                                  (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor {


  private val eventHandlers = eventsHandlersSeq.map(eh => (eh.eventClassName, eh)).toMap

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
    case ee: EventsEnvelope[_] =>
      assureRestoredState()
      receiveEvent(ee.asInstanceOf[EventsEnvelope[AGGREGATE_ROOT]])
    case ReturnAggregateRoot(respondTo) =>
      assureRestoredState()
      receiveReturnAggregateRoot(respondTo)
  }


  private def receiveEvent(eventsEnvelope: EventsEnvelope[AGGREGATE_ROOT]): Unit = {
    println("Received command " + eventsEnvelope.events.head +" for version " + eventsEnvelope.expectedVersion +" when version was " + version)
    if (eventsEnvelope.expectedVersion == version) {
      persist(eventsEnvelope)(handleEventAndRespond(eventsEnvelope.respondTo))
    } else {
      eventsEnvelope.respondTo ! AggregateConcurrentModificationError(eventsEnvelope.expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    println("ReturnAggregateRoot " + aggregateRoot)
    respondTo ! AggregateRoot(id, version, Some(aggregateRoot))
  }


  private def persist(eventsEnvelope: EventsEnvelope[AGGREGATE_ROOT])(afterPersist: Seq[Event[AGGREGATE_ROOT]] => Unit): Unit = {
    import context.dispatcher
    Future {
      eventStore.persistEvent(id, eventsEnvelope.asInstanceOf[EventsEnvelope[AnyRef]])
      afterPersist(eventsEnvelope.events)
    }
  }

  private def handleEventAndRespond(respondTo: ActorRef)(events: Seq[Event[AGGREGATE_ROOT]]): Unit = {
    println("Updating state and responding")
    events.foreach(handleEvent)
    respondTo ! AggregateAck
  }

  private def handleEvent(event: Event[AGGREGATE_ROOT]): Unit = {
    println("Updating state by handling " + event)
    aggregateRoot = eventHandlers(event.getClass.getName).handle(aggregateRoot, event)
    version = version.increment
  }


}