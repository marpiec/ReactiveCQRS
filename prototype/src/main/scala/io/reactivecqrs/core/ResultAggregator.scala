package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef, PoisonPill}
import akka.event.LoggingReceive

import scala.concurrent.duration._

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

  // stop waiting after some time
  import context.dispatcher
  context.system.scheduler.scheduleOnce(5 seconds) {
    self ! PoisonPill
  }

}
