package io.reactivecqrs.core.util

import scala.concurrent.duration.FiniteDuration

/**
  * F186: one read at a time, for an actor whose reads run in futures (the outbox poller, the squash scheduler). A read
  * that has run longer than `timeout` is given up: a connection that no longer answers would keep it waiting for ever,
  * and the actor with it. The next `start` begins a new read, and the result of the one given up is ignored when it
  * comes. Not thread-safe: part of the actor's state.
  */
final class ReadWatch(timeout: FiniteDuration) {

  private var current: Option[(Int, Long)] = None // the read under way: its number, and when it started (nanoTime)
  private var started = 0

  /** The number of a new read, and whether one under way was given up for it; `None` while one runs within the timeout. */
  def start(): Option[(Int, Boolean)] = current match {
    case Some((_, since)) if System.nanoTime() - since < timeout.toNanos => None
    case running =>
      started += 1
      current = Some(started -> System.nanoTime())
      Some(started -> running.isDefined)
  }

  /** Whether `number` is the read under way, which ends; false for one given up. */
  def ended(number: Int): Boolean = {
    val isCurrent = current.exists(_._1 == number)
    if (isCurrent) current = None
    isCurrent
  }

  /** The line to log when a read was given up. */
  def givenUp(what: String): String = what + " did not end in " + timeout + "; it is given up and done again. A connection that no " +
    "longer answers can hold a read for ever: set socketTimeout in the JDBC URL (F186)"
}
