package io.reactivecqrs.actor

import _root_.io.reactivecqrs.api.guid.{AggregateId, UserId, CommandId}
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import akka.persistence.PersistentActor
import _root_.io.reactivecqrs.core._

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

  private val persistenceId: String = aggregateRootClassTag.getClass.getName + id.asLong


  private val eventStore = new EventStore

  override def receive = LoggingReceive {
    case ee: EventEnvelope[_] => receiveEvent(ee.asInstanceOf[EventEnvelope[AGGREGATE_ROOT]])
    case ReturnAggregateRoot(respondTo) => receiveReturnAggregateRoot(respondTo)
  }


  private def receiveEvent(eventEnvelope: EventEnvelope[AGGREGATE_ROOT]): Unit = {
    println("Received command " + eventEnvelope.event +" for version " + eventEnvelope.expectedVersion +" when version was " + version)
    if (eventEnvelope.expectedVersion == version) {
      persist(eventEnvelope)(handleEventAndRespond(eventEnvelope.respondTo))
    } else {
      eventEnvelope.respondTo ! AggregateConcurrentModificationError(eventEnvelope.expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    println("ReturnAggregateRoot " + aggregateRoot)
    respondTo ! AggregateRoot(id, version, Some(aggregateRoot))
  }


  private def persist(eventEnvelope: EventEnvelope[AGGREGATE_ROOT])(afterPersist: Event[AGGREGATE_ROOT] => Unit): Unit = {
    import context.dispatcher
    Future {
      eventStore.persistEvent(id, eventEnvelope.asInstanceOf[EventEnvelope[AnyRef]])
      afterPersist(eventEnvelope.event)
    }
  }

  private def handleEventAndRespond(respondTo: ActorRef)(event: Event[AGGREGATE_ROOT]): Unit = {
    println("Updating state and responding")
    handleEvent(event)
    respondTo ! AggregateAck
  }

  private def handleEvent(event: Event[AGGREGATE_ROOT]): Unit = {
    println("Updating state by handling " + event)
    aggregateRoot = eventHandlers(event.getClass.getName).handle(aggregateRoot, event)
    version = version.increment
  }


}
