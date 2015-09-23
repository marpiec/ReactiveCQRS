package io.reactivecqrs.core.util

import akka.actor.{Actor, ActorContext}
import akka.event.LoggingReceive

trait ActorLogging {this: Actor =>

  def logReceive(r: Receive)(implicit context: ActorContext): Receive = LoggingReceive.withLabel("(from " + sender.path.toString+")")(r)
  
}
