package io.reactivecqrs.testkit

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.ConcurrentLinkedQueue

import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy}
import ch.qos.logback.classic.{Logger => LogbackLogger}
import ch.qos.logback.core.AppenderBase
import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.event.Logging
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** Every line logged while it is open, by the actor system's loggers and by the slf4j ones (logback), each with the
  * stack trace of its cause as a log would print it: what a spec searches for values that must not reach the logs
  * (F190, F191). Pekko's lines arrive asynchronously; wait for an expected line before asserting an absence. */
final class LogCapture(system: ActorSystem) extends AutoCloseable {

  private val lines = new ConcurrentLinkedQueue[String]()

  private val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[LogbackLogger]
  private val appender = new AppenderBase[ILoggingEvent] {
    override def append(event: ILoggingEvent): Unit = lines.add(event.getFormattedMessage + proxyText(event.getThrowableProxy))
  }
  appender.setContext(root.getLoggerContext)
  appender.start()
  root.addAppender(appender)

  private val listener = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case event: Logging.LogEvent => event match {
        case withCause: Logging.LogEventWithCause if withCause.cause != Logging.Error.NoCause =>
          lines.add(event.message.toString + " " + stackTrace(withCause.cause))
        case _ => lines.add(event.message.toString)
      }
    }
  }))
  system.eventStream.subscribe(listener, classOf[Logging.LogEvent])

  /** The lines logged so far. */
  def all: Seq[String] = lines.asScala.toSeq

  /** The lines logged so far that contain `text`. */
  def containing(text: String): Seq[String] = all.filter(_.contains(text))

  override def close(): Unit = {
    root.detachAppender(appender)
    system.eventStream.unsubscribe(listener)
    system.stop(listener)
  }

  private def stackTrace(cause: Throwable): String = {
    val text = new StringWriter()
    cause.printStackTrace(new PrintWriter(text))
    text.toString
  }

  private def proxyText(proxy: IThrowableProxy): String =
    if (proxy == null) "" else " " + proxy.getClassName + ": " + proxy.getMessage + proxyText(proxy.getCause)
}
