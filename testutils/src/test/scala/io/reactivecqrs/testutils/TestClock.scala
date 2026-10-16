package io.reactivecqrs.testutils

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

import scala.concurrent.duration.FiniteDuration

/** A clock that moves only when a test moves it, for timings measured in minutes. */
class TestClock(start: Instant = Instant.now()) extends Clock {

  @volatile private var now: Instant = start

  def advance(by: FiniteDuration): Unit = now = now.plusMillis(by.toMillis)

  override def instant(): Instant = now
  override def getZone: ZoneId = ZoneOffset.UTC
  override def withZone(zone: ZoneId): Clock = this
}
