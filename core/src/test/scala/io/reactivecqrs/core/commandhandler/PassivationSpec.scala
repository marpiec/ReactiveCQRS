package io.reactivecqrs.core.commandhandler

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable

// F279: the sweep of idle Local owners over time, under sustained traffic, as the bus runs it (a send sweeps when due).
class PassivationSpec extends AnyFeatureSpec with Matchers {

  /** Runs `minutes` of `perSecond` new aggregates, each used once; returns the owners alive at each second. */
  private def simulate(perSecond: Int, minutes: Int, keepAliveLimit: Int = 200): Vector[Int] = {
    val activity = mutable.HashMap[Long, Long]()
    var next = 0L
    var nextSweep = 0L
    (0 until minutes * 60).map { second =>
      (0 until 10).foreach { tenth => // a send every 100 ms, each bringing a tenth of the second's aggregates
        val now = second * 1000L + tenth * 100L
        (0 until perSecond / 10).foreach { _ => activity += next -> now; next += 1 }
        if (now >= nextSweep) {
          val sweep = Passivation.sweep(activity, now, keepAliveLimit, maxInactivityMillis = 43200000L)
          sweep.stop.foreach(activity -= _)
          nextSweep = now + sweep.nextInMillis
        }
      }
      activity.size
    }.toVector
  }

  Feature("Stopping idle owners under sustained traffic (F218, F279)") {

    Scenario("F279 with 1 000 new aggregates a second the owners stay bounded: those of the last 2 minutes and a short backlog") {
      val alive = simulate(perSecond = 1000, minutes = 10)
      // Each owner lives 2 minutes idle before it may go; a backlog of seconds beyond that is the pace's.
      alive.drop(7 * 60).max must be <= (200 + 140 * 1000)
      alive.last must be <= alive(7 * 60) + 1000
    }

    Scenario("light traffic is swept every 5 minutes, at most 200 at once") {
      val activity = mutable.HashMap[Long, Long]((0L until 300L).map(id => id -> 0L): _*)
      val sweep = Passivation.sweep(activity, 180000L, keepAliveLimit = 200, maxInactivityMillis = 43200000L)
      sweep.stop.size mustBe 100
      sweep.nextInMillis mustBe Passivation.SweepEveryMillis
    }
  }
}
