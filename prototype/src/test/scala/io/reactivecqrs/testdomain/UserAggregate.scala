package io.reactivecqrs.testdomain

import akka.actor.ActorRef
import akka.persistence.PersistentActor
import io.reactivecqrs.core.{EventEnvelope, AggregateVersion, AggregateId, Event}
import io.reactivecqrs.testdomain.api.{UserRegistered, User}

class UserAggregate(val id: AggregateId) extends PersistentActor {

  var version: AggregateVersion = AggregateVersion.ZERO
  var aggregateRoot: User = null

  override val persistenceId: String = "User" + id.asLong

  override def receiveRecover: Receive = {
    case event: Event[_] => println("ReceiveRecover"); handleEvent(event.asInstanceOf[Event[User]])
  }

  override def receiveCommand: Receive = {
    case EventEnvelope(respondTo, expectedVersion, event) =>
      println("Received command " + event +" for version " + expectedVersion +" when version was " + version)
      if (expectedVersion == version) {
        persist(event.asInstanceOf[Event[User]])(handleEventAndRespond(respondTo))
      } else {
        respondTo ! "Concurrent modification error " + expectedVersion+" "+version
      }
    case m => throw new IllegalArgumentException("Unsupported message " + m)
  }

  private def handleEventAndRespond(respondTo: ActorRef)(event: Event[User]): Unit = {
    println("Updating state and responding")
    handleEvent(event)
    respondTo ! "OK"
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
