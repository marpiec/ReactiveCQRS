package io.reactivecqrs.actor

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import akka.persistence.PersistentActor
import io.reactivecqrs.core._

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



  override def receive = LoggingReceive {
    case EventEnvelope(respondTo, expectedVersion, event) => receiveEvent(respondTo, expectedVersion, event.asInstanceOf[Event[AGGREGATE_ROOT]])
    case ReturnAggregateRoot(respondTo) => receiveReturnAggregateRoot(respondTo)
  }


  private def receiveEvent(respondTo: ActorRef, expectedVersion: AggregateVersion, event: Event[AGGREGATE_ROOT]): Unit = {
    println("Received command " + event +" for version " + expectedVersion +" when version was " + version)
    if (expectedVersion == version) {
      persist(event)(handleEventAndRespond(respondTo))
    } else {
      respondTo ! AggregateConcurrentModificationError(expectedVersion, version)
    }

  }

  private def receiveReturnAggregateRoot(respondTo: ActorRef): Unit = {
    println("ReturnAggregateRoot " + aggregateRoot)
    respondTo ! AggregateRoot(id, version, Some(aggregateRoot))
  }


  private def persist(event: Event[AGGREGATE_ROOT])(afterPersist: Event[AGGREGATE_ROOT] => Unit): Unit = {

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
