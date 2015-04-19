package io.reactivecqrs.testdomain

import akka.actor.ActorRef
import akka.event.LoggingReceive
import akka.persistence.PersistentActor
import io.reactivecqrs.core.{EventEnvelope, AggregateVersion, AggregateId, Event}
import io.reactivecqrs.testdomain.api.{UserRegistered, User}

case object AggregateAck
case class AggregateConcurrentModificationError(expected: AggregateVersion, was: AggregateVersion)

case class ReturnAggregateRoot(respondTo: ActorRef)

case class Aggregate[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

class UserAggregate(val id: AggregateId) extends PersistentActor {

  var version: AggregateVersion = AggregateVersion.ZERO
  var aggregateRoot: User = null

  override val persistenceId: String = "User" + id.asLong

  override def receiveRecover: Receive = LoggingReceive {
    case event: Event[_] => println("ReceiveRecover"); handleEvent(event.asInstanceOf[Event[User]])
  }

  override def receiveCommand: Receive = LoggingReceive {
    case EventEnvelope(respondTo, expectedVersion, event) =>
      println("Received command " + event +" for version " + expectedVersion +" when version was " + version)
      if (expectedVersion == version) {
        persist(event.asInstanceOf[Event[User]])(handleEventAndRespond(respondTo))
      } else {
        respondTo ! AggregateConcurrentModificationError(expectedVersion, version)
      }
    case ReturnAggregateRoot(respondTo) => respondTo ! Aggregate(id, version, Some(aggregateRoot))
    case m => throw new IllegalArgumentException("Unsupported message " + m)
  }

  private def handleEventAndRespond(respondTo: ActorRef)(event: Event[User]): Unit = {
    println("Updating state and responding")
    handleEvent(event)
    respondTo ! AggregateAck
  }

  private def handleEvent(event: Event[User]): Unit = {
    println("Updating state by handling " + event)
    aggregateRoot = event match {
      case event: UserRegistered => handleUserRegistered(event)
    }
    version = version.increment
  }


  private def handleUserRegistered(event: UserRegistered): User ={
    User(event.name, None)
  }



}
