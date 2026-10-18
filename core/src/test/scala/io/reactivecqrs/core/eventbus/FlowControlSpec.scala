package io.reactivecqrs.core.eventbus

import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.FlowControl.{Pull, SentKey}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// F93, F126: a subscriber's window of events and bytes sent and not answered; what does not fit is held and pulled.
class FlowControlSpec extends AnyFeatureSpec with Matchers {

  private val cart = AggregateType("Cart")
  private val second = 1000000000L

  // A window of 4 events and 4 000 bytes; at most 2 pulls at once; pulls answered within 60 s; the deadline 600 s.
  private def flow = new FlowControl[String](window = 4, windowBytes = 4000, maxPulls = 2, pullTimeoutNanos = 60 * second, deadlineNanos = 600 * second)
  private def id(n: Long) = AggregateId(n)
  private def key(n: Long, first: Int) = SentKey(id(n), first, "events")

  Feature("The window (F93)") {

    Scenario("a subscriber takes publications while they fit its window of events, and of bytes") {
      val f = flow
      f.admits("s", id(1), 3, 100) mustBe true
      f.sent("s", key(1, 1), 3, 100, 0)
      f.admits("s", id(2), 1, 100) mustBe true
      f.admits("s", id(2), 2, 100) mustBe false // 5 events
      f.admits("s", id(2), 1, 3901) mustBe false // 4 001 bytes
      f.admits("other", id(2), 2, 100) mustBe true // a window per subscriber
    }

    Scenario("with nothing in flight, one publication goes whatever its size") {
      val f = flow
      f.admits("s", id(1), 20, 50000) mustBe true
      f.sent("s", key(1, 1), 20, 50000, 0)
      f.admits("s", id(2), 1, 1) mustBe false
      f.answered("s", key(1, 1))
      f.admits("s", id(2), 1, 1) mustBe true
    }

    Scenario("an aggregate behind for a subscriber stays held, so the subscriber never sees a gap of the bus's making") {
      val f = flow
      f.sent("s", key(1, 1), 4, 100, 0)
      f.admits("s", id(2), 1, 10) mustBe false
      f.hold("s", cart, id(2), 1, 1, 10, 0)
      f.answered("s", key(1, 1))
      f.admits("s", id(2), 1, 10) mustBe false // the window has room, but version 1 is held
      f.admits("s", id(3), 1, 10) mustBe true
    }

    Scenario("an answer frees what its message took; an answer the bus does not know changes nothing") {
      val f = flow
      f.sent("s", key(1, 1), 2, 100, 0)
      f.sent("s", key(2, 1), 2, 100, 0)
      f.answered("s", key(9, 1))
      f.admits("s", id(3), 1, 1) mustBe false
      f.answered("s", key(1, 1))
      f.admits("s", id(3), 2, 1) mustBe true
      f.status("s").events mustBe 2
    }

    Scenario("what pulls asked for and have not got yet counts as in flight: it is on its way") {
      val f = flow
      f.hold("s", cart, id(1), 1, 3, 30, 0)
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1))
      f.sent("s", key(9, 1), 1, 10, 0)
      f.admits("s", id(2), 1, 10) mustBe false // 1 in flight and 3 on their way
    }

    Scenario("while aggregates are behind, a live publication takes at most half the window: the pulls take the rest, so the oldest held move") {
      val f = flow
      f.sent("s", key(8, 1), 1, 10, 0)
      f.sent("s", key(9, 1), 3, 10, 0)
      f.hold("s", cart, id(1), 1, 1, 10, 0)
      f.answered("s", key(9, 1))
      f.admits("s", id(2), 1, 10) mustBe true
      f.admits("s", id(2), 2, 10) mustBe false // 3 of 4 in flight
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1))
      f.admits("s", id(2), 2, 10) mustBe true // nothing behind: the whole window, less what is on its way
    }

    Scenario("a message sent again under the same key counts once") {
      val f = flow
      f.sent("s", key(1, 1), 3, 100, 0)
      f.sent("s", key(1, 1), 3, 100, 1)
      f.status("s").events mustBe 3
      f.answered("s", key(1, 1))
      f.status.get("s") mustBe None // forgotten when idle
    }
  }

  Feature("Pulling what is held (F93)") {

    Scenario("nothing is pulled while more than half the window is in flight; then the oldest held first, as many as fit") {
      val f = flow
      f.sent("s", key(9, 1), 4, 100, 0)
      f.hold("s", cart, id(1), 1, 1, 10, 0)
      f.hold("s", cart, id(2), 1, 1, 10, 0)
      f.hold("s", cart, id(3), 1, 1, 10, 0)
      f.pulls("s", 1000, 1000000, 0) mustBe empty
      f.answered("s", key(9, 1))
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1), Pull(cart, id(2), 1)) // at most 2 at once
      f.pulls("s", 1000, 1000000, 0) mustBe empty
      f.republished("s", id(1), 1)
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(3), 1))
    }

    Scenario("pulls stop where the window would be full, counting each as a part of at most the owners' size") {
      val f = new FlowControl[String](window = 2000, windowBytes = 4000, maxPulls = 10, pullTimeoutNanos = 60 * second, deadlineNanos = 600 * second)
      f.hold("s", cart, id(1), 1, 5000, 15000, 0) // 5 000 events held, counted as one part of 1 000 events and 2 500 bytes
      f.hold("s", cart, id(2), 1, 10, 3000, 0)
      f.hold("s", cart, id(3), 1, 10, 10, 0)
      f.pulls("s", 1000, 2500, 0).map(_.aggregateId) mustBe Seq(id(1), id(2)) // 5 000 bytes counted after the second: no third
    }

    Scenario("a part that ends before the last held version leaves the rest behind, from the next version") {
      val f = flow
      f.hold("s", cart, id(1), 5, 3000, 3000, 0)
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 5))
      f.hold("s", cart, id(1), 3001, 3001, 1, 0) // a later event while it was pulled
      f.republished("s", id(1), 1004)
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1005))
      f.republished("s", id(1), 3001)
      f.status.get("s") mustBe None
    }

    Scenario("an event held again counts its bytes once") {
      val f = new FlowControl[String](window = 100, windowBytes = 4000, maxPulls = 10, pullTimeoutNanos = 60 * second, deadlineNanos = 600 * second)
      f.hold("s", cart, id(1), 1, 10, 1000, 0)
      f.hold("s", cart, id(1), 1, 10, 1000, 0) // re-published from the outbox while held: the same events
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1)) // 1 000 bytes on their way
      f.sent("s", key(9, 1), 1, 2000, 0)
      f.admits("s", id(2), 1, 1000) mustBe true
      f.admits("s", id(2), 1, 1001) mustBe false
    }

    Scenario("a part and what it leaves behind each count their share of the bytes held") {
      val f = new FlowControl[String](window = 100, windowBytes = 4000, maxPulls = 10, pullTimeoutNanos = 60 * second, deadlineNanos = 600 * second)
      f.hold("s", cart, id(1), 1, 20, 2000, 0)
      f.pulls("s", 5, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1)) // a part of 5 events: 500 bytes on their way
      f.sent("s", key(9, 1), 1, 3000, 0)
      f.admits("s", id(2), 1, 500) mustBe true
      f.admits("s", id(2), 1, 501) mustBe false
      f.republished("s", id(1), 5)
      f.answered("s", key(9, 1))
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 6)) // the other 15 events: 1 500 bytes
      f.sent("s", key(9, 1), 1, 2500, 0)
      f.admits("s", id(2), 1, 0) mustBe true
      f.admits("s", id(2), 1, 1) mustBe false
    }
  }

  Feature("What goes unanswered") {

    Scenario("a message not answered by the deadline lets one message through, so a lost one cannot keep the window shut") {
      val f = flow
      f.sent("s", key(1, 1), 4, 100, 0)
      f.admits("s", id(2), 1, 1) mustBe false
      f.expire(599 * second)
      f.admits("s", id(2), 1, 1) mustBe false
      f.expire(600 * second)
      f.admits("s", id(2), 1, 1) mustBe true
    }

    Scenario("F221 a message past the deadline still counts, until the subscriber answers one sent after it") {
      val f = flow
      f.sent("s", key(1, 1), 4, 100, 0)
      f.expire(600 * second)
      f.sent("s", key(2, 1), 1, 1, 600 * second) // the one message that goes with nothing in flight
      f.admits("s", id(3), 1, 1) mustBe false // the first is presumed in the mailbox
      f.answered("s", key(2, 1)) // so it has passed the first
      f.admits("s", id(3), 1, 1) mustBe true
      f.status.get("s") mustBe None
    }

    Scenario("a pull not answered in time is behind again") {
      val f = flow
      f.hold("s", cart, id(1), 1, 1, 10, 0)
      f.pulls("s", 1000, 1000000, 0) mustBe Seq(Pull(cart, id(1), 1))
      f.expire(59 * second)
      f.pulls("s", 1000, 1000000, 59 * second) mustBe empty
      f.expire(60 * second)
      f.pulls("s", 1000, 1000000, 60 * second) mustBe Seq(Pull(cart, id(1), 1))
    }

    Scenario("an aggregate held since the deadline is dropped, pulled or not: its pulls may be answered on another node") {
      val f = flow
      f.hold("s", cart, id(1), 1, 1, 10, 0)
      f.hold("s", cart, id(2), 1, 1, 10, 0)
      f.pulls("s", 1000, 1000000, 0).map(_.aggregateId) mustBe Seq(id(1), id(2))
      f.hold("s", cart, id(3), 1, 1, 10, 500 * second)
      f.expire(600 * second) // the pulls of 1 and 2 time out, too old to be behind again
      f.pulls("s", 1000, 1000000, 600 * second) mustBe Seq(Pull(cart, id(3), 1))
      f.expire(1100 * second)
      f.withBehind mustBe empty
    }

    Scenario("an aggregate held again stays behind a deadline after its latest hold: what was held then still waits") {
      val f = flow
      f.sent("s", key(9, 1), 4, 100, 500 * second) // the window is full: nothing is pulled
      f.hold("s", cart, id(1), 1, 1, 10, 0)
      f.hold("s", cart, id(1), 2, 2, 10, 500 * second)
      f.expire(600 * second)
      f.status("s").aggregatesBehind mustBe 1
      f.expire(1100 * second)
      f.status("s").aggregatesBehind mustBe 0 // the message past the deadline still counts (F221)
    }

    Scenario("a subscriber removed leaves nothing behind") {
      val f = flow
      f.sent("s", key(1, 1), 4, 100, 0)
      f.hold("s", cart, id(2), 1, 1, 10, 0)
      f.remove("s")
      f.status mustBe empty
      f.withBehind mustBe empty
    }
  }
}
