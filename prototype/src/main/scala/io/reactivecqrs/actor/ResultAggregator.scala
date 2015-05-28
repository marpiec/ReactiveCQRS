package io.reactivecqrs.actor

import akka.actor.{Actor, ActorRef, PoisonPill}
import akka.event.LoggingReceive

object ResultAggregator {
  case object AggregateAck
}

class ResultAggregator[RESULT]  (private val respondTo: ActorRef,
                                  private val result: RESULT) extends Actor {

  import ResultAggregator._

  override def receive: Receive = LoggingReceive {
    case AggregateAck =>
      respondTo ! result
      self ! PoisonPill
    case e: AggregateConcurrentModificationError =>
      respondTo ! e
      self ! PoisonPill
  }
}
