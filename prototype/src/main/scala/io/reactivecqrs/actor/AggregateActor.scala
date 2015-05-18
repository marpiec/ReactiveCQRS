package io.reactivecqrs.actor

import akka.actor.ActorRef
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
                                                                  (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends PersistentActor {


  println("AggregateRepositoryPersistentActor created")

  private val eventHandlers = eventsHandlersSeq.map(eh => (eh.eventClassName, eh)).toMap

  var version: AggregateVersion = AggregateVersion.ZERO
  var aggregateRoot: AGGREGATE_ROOT = _

  override val persistenceId: String = aggregateRootClassTag.getClass.getName + id.asLong



  override def receiveRecover: Receive = LoggingReceive {
    case event: Event[_] => println("ReceiveRecover"); handleEvent(event.asInstanceOf[Event[AGGREGATE_ROOT]])
  }

  // This receives cqrs Event, not command
  override def receiveCommand: Receive = LoggingReceive {
    case EventEnvelope(respondTo, expectedVersion, event) =>
      println("Received command " + event +" for version " + expectedVersion +" when version was " + version)
      if (expectedVersion == version) {
        persist(event.asInstanceOf[Event[AGGREGATE_ROOT]])(handleEventAndRespond(respondTo))
      } else {
        respondTo ! AggregateConcurrentModificationError(expectedVersion, version)
      }
    case ReturnAggregateRoot(respondTo) => {
      println("ReturnAggregateRoot " + aggregateRoot)
      respondTo ! AggregateRoot(id, version, Some(aggregateRoot))
    }
    case m => throw new IllegalArgumentException("Unsupported message " + m)
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
