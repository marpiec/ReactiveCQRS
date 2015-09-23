package io.reactivecqrs.core.util

import akka.actor.{Actor, ActorContext}
import akka.event.{Logging, LoggingReceive}

trait ActorLogging {this: Actor =>

  val log = Logging(context.system, this)

  def logReceive(r: Receive)(implicit context: ActorContext): Receive = LoggingReceive.withLabel("(from " + sender.path.toString+")")(new Receive {
    override def isDefinedAt(o: Any): Boolean = r.isDefinedAt(o)
    override def apply(o: Any): Unit = {
      try {
        r(o)
      } catch {
        case e: Exception =>
          log.error(e, s"Error while handling $o from  ${sender()}")
          throw e;
      }
    }
  })

}
