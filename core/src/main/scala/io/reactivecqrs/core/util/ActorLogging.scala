package io.reactivecqrs.core.util

import org.apache.pekko.actor.{Actor, ActorContext}
import org.apache.pekko.event.{Logging, LoggingAdapter, LoggingReceive}

/** A message an actor could not handle, naming the message's class and its sender. */
class ActorException(message: String, cause: Throwable) extends Exception(message, cause) {
  override def fillInStackTrace(): Throwable = this
}

/** The library's actors' log, and their `receive` wrapped to name the message an error came from. */
trait MyActorLogging {this: Actor =>

  /** The actor's log; the causes of its errors and warnings are logged without the values they carry (F191). */
  val log: LoggingAdapter = new RedactingLog(Logging(context.system, this))

  /** `r`, with each message logged at debug level (`pekko.actor.debug.receive`) and an error wrapped in an
    * `ActorException` that names the message's class and its sender: the message itself may carry personal data. */
  def logReceive(r: Receive)(implicit context: ActorContext): Receive = LoggingReceive.withLabel("(from " + sender().path.toString+")")(new Receive {
    override def isDefinedAt(o: Any): Boolean = r.isDefinedAt(o)
    override def apply(o: Any): Unit = {
      try {
        r(o)
      } catch {
        case e: Exception => throw new ActorException("Error while handling a " + o.getClass.getName + " from " + sender(), e)
      }
    }
  })
}

/** F191: a log whose errors and warnings carry their causes as `Errors.redacted` gives them. */
private final class RedactingLog(underlying: LoggingAdapter) extends LoggingAdapter {
  override def isErrorEnabled: Boolean = underlying.isErrorEnabled
  override def isWarningEnabled: Boolean = underlying.isWarningEnabled
  override def isInfoEnabled: Boolean = underlying.isInfoEnabled
  override def isDebugEnabled: Boolean = underlying.isDebugEnabled
  override protected def notifyError(message: String): Unit = underlying.error(message)
  override protected def notifyError(cause: Throwable, message: String): Unit = underlying.error(Errors.redacted(cause), message)
  override protected def notifyWarning(message: String): Unit = underlying.warning(message)
  override protected def notifyWarning(cause: Throwable, message: String): Unit = underlying.warning(Errors.redacted(cause), message)
  override protected def notifyInfo(message: String): Unit = underlying.info(message)
  override protected def notifyDebug(message: String): Unit = underlying.debug(message)
}
