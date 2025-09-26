package io.reactivecqrs.core.util

import org.apache.pekko.actor.{Actor, ActorContext}
import org.apache.pekko.event.{Logging, LoggingReceive}

class ActorException(message: String, cause: Throwable) extends Exception(message, cause) {
  override def fillInStackTrace(): Throwable = this
}

trait MyActorLogging {this: Actor =>

  val log = Logging(context.system, this)

  def logReceive(r: Receive)(implicit context: ActorContext): Receive = LoggingReceive.withLabel("(from " + sender().path.toString+")")(new Receive {
    override def isDefinedAt(o: Any): Boolean = r.isDefinedAt(o)
    override def apply(o: Any): Unit = {
      try {
        r(o)
      } catch {
        case e: Exception => throw new ActorException(s"Error while handling $o from  ${sender()}", e)
      }
    }
  })
}

