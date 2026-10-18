package io.reactivecqrs.core.util

import org.apache.pekko.actor.{Actor, ActorContext}
import org.apache.pekko.event.{Logging, LoggingReceive}

/** A message an actor could not handle, naming the message and its sender. */
class ActorException(message: String, cause: Throwable) extends Exception(message, cause) {
  override def fillInStackTrace(): Throwable = this
}

/** The library's actors' log, and their `receive` wrapped to name the message an error came from. */
trait MyActorLogging {this: Actor =>

  /** The actor's log. */
  val log = Logging(context.system, this)

  /** `r`, with each message logged at debug level (`pekko.actor.debug.receive`) and an error wrapped in an
    * `ActorException` that names the message and its sender. */
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

