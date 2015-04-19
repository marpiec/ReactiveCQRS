package io.reactivecqrs.testdomain

import akka.actor.{PoisonPill, Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.testdomain.api.RegisterUserResult

class UserCommandResultAggregator(private val respondTo: ActorRef,
                                  private val result: RegisterUserResult) extends Actor {

  override def receive: Receive = LoggingReceive {
    case AggregateAck =>
      respondTo ! result
      self ! PoisonPill
    case e: AggregateConcurrentModificationError =>
      respondTo ! e
      self ! PoisonPill
  }
}
