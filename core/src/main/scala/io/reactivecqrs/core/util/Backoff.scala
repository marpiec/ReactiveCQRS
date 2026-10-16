package io.reactivecqrs.core.util

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

/**
  * The waiting rule of one kind of retry (AR6): before attempt `n` (counted from 2, the first attempt does not wait)
  * wait `first` doubled `n - 2` times, at most `max`; with `jitter` a random part of up to half of that, so competing
  * retries spread out. `maxAttempts` counts every attempt, the first one included.
  */
case class Backoff(first: FiniteDuration, max: FiniteDuration, maxAttempts: Int, jitter: Boolean = false) {

  /** The wait before attempt `attempt` (2, 3, ...). */
  def delay(attempt: Int): FiniteDuration = {
    val ceilingMillis = math.min(max.toMillis, first.toMillis << math.min(math.max(attempt - 2, 0), 30))
    if (jitter) (ceilingMillis / 2 + ThreadLocalRandom.current().nextLong(ceilingMillis / 2 + 1)).millis
    else ceilingMillis.millis
  }

  /** True when `attempt` attempts have been made and no further one is allowed. */
  def exhausted(attempt: Int): Boolean = attempt >= maxAttempts
}
