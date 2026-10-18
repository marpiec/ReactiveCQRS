package io.reactivecqrs.testkit

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.pekko.actor.Actor
import org.apache.pekko.event.Logging

/** Keeps the errors logged in its actor system once subscribed to them (`eventStream.subscribe(recorder,
  * classOf[Logging.Error])`); answers "ping" once it has kept those logged before. */
class ErrorRecorder(errors: ConcurrentLinkedQueue[String]) extends Actor {
  override def receive: Receive = {
    case e: Logging.Error => errors.add(e.message.toString)
    case "ping" => sender() ! "pong"
  }
}
