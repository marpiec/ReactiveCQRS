package io.reactivecqrs.testutils

import java.util.concurrent.atomic.AtomicLong

/** Aggregate ids a test chooses: unique in a test run (its schema), and in the legacy range below 10^12 that installation
  * 0, the tests' installation, may choose from (D39); far above the ids the pools hand out. */
object TestIds {
  private val next = new AtomicLong(500000000000L)

  def aggregate(): Long = next.incrementAndGet()
}
