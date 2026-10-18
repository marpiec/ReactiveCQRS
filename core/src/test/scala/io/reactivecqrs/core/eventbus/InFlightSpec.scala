package io.reactivecqrs.core.eventbus

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// F212: a bus tick visits what expires or stalls, not every delivery in flight.
class InFlightSpec extends AnyFeatureSpec with Matchers {

  private case class D(first: Long, last: Long, watched: Boolean = true, held: Boolean = false)

  private def inFlight(visits: AtomicInteger = new AtomicInteger) =
    new InFlight[Int, D](d => { visits.incrementAndGet(); d.first }, d => { visits.incrementAndGet(); d.last }, _.watched, _.held)

  Feature("Deliveries in flight") {

    Scenario("F212 a deadline and a stall check visit only the deliveries that old, each stall once") {
      val visits = new AtomicInteger
      val f = inFlight(visits)
      (1 to 100000).foreach(i => f.put(i, D(i.toLong, i.toLong)))

      visits.set(0)
      f.firstSentBy(10).map(_._1) mustBe (1 to 10)
      visits.get must be <= 11
      visits.set(0)
      f.lastSentBy(10).map(_._1) mustBe (1 to 10)
      visits.get must be <= 20
      f.lastSentBy(10) mustBe empty
    }

    Scenario("a delivery sent again stalls from its new send, one first sent anew goes last, and those held are counted") {
      val f = inFlight()
      f.put(1, D(1, 1))
      f.put(2, D(2, 2))
      f.put(1, D(1, 5))
      f.lastSentBy(3).map(_._1) mustBe Seq(2)
      f.lastSentBy(5).map(_._1) mustBe Seq(1)

      f.put(1, D(6, 6)) // adopted: first sent anew
      f.firstSentBy(5).map(_._1) mustBe Seq(2)
      f.firstSentBy(6).map(_._1) mustBe Seq(2, 1)

      f.put(3, D(7, 7, held = true))
      f.heldSize mustBe 1
      f.put(3, D(7, 7))
      f.heldSize mustBe 0
      f.put(4, D(8, 8, held = true))
      f.remove(4)
      (f.heldSize, f.size) mustBe ((0, 3))
      f.keysWhere(_.first >= 6).toSet mustBe Set(1, 3)
    }
  }
}
