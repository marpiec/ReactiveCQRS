package io.reactivecqrs.testdomain

import akka.actor.{PoisonPill, Actor, ActorRef}
import io.reactivecqrs.testdomain.api.RegisterUserResult

class UserCommandResultAggregator(private val respondTo: ActorRef,
                                  private val result: RegisterUserResult) extends Actor {

  override def receive: Receive = {
    case AggregateAck =>
      respondTo ! result
      self ! PoisonPill
    case e: AggregateConcurrentModificationError =>
      respondTo ! e
      self ! PoisonPill
  }
}
