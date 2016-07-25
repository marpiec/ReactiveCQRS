package io.reactivecqrs.core.commandhandler

import akka.actor.{Actor, ActorRef, PoisonPill}
import io.reactivecqrs.api._
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.duration._

object ResultAggregator {

  case object AggregateModified

}

class ResultAggregator[RESPONSE <: CustomCommandResponse[_]](private val respondTo: ActorRef,
                                                             private val result: RESPONSE,
                                                             private val timeout: FiniteDuration) extends Actor with ActorLogging {

  import ResultAggregator._

  override def receive: Receive = logReceive {
    case AggregateModified =>
      respondTo ! result
      self ! PoisonPill
    case e: AggregateConcurrentModificationError =>
      respondTo ! e
      self ! PoisonPill
    case e: CommandHandlingError =>
      log.error("CommandHandlingError " + e.commandName +"\n" + e.stackTrace)
      respondTo ! e
      self ! PoisonPill
    case e: EventHandlingError =>
      log.error("EventHandlingError " + e.eventName +"\n" + e.stackTrace)
      respondTo ! e
      self ! PoisonPill
  }

  // stop waiting after some time

  import context.dispatcher

  context.system.scheduler.scheduleOnce(timeout) {
    self ! PoisonPill
  }

}
