package io.reactivecqrs.actor

import akka.actor.{Actor, ActorRef, PoisonPill}
import akka.event.LoggingReceive

class ResultAggregator[RESULT](private val respondTo: ActorRef,
                                  private val result: RESULT) extends Actor {

  override def receive: Receive = LoggingReceive {
    case AggregateAck =>
      respondTo ! result
      self ! PoisonPill
    case e: AggregateConcurrentModificationError =>
      respondTo ! e
      self ! PoisonPill
  }
}
