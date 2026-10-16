package io.reactivecqrs.core.delivery

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.core.delivery.SequenceGate._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable
import scala.util.Random

// D4, F5: one gate decides for every delivery whether to ack only, apply a range, or ask for a gap.
class SequenceGateSpec extends AnyFeatureSpec with Matchers {

  private def v(n: Int) = AggregateVersion(n)

  Feature("Single deliveries") {

    Scenario("a delivery already applied is only acknowledged") {
      decide(v(5), v(3), v(5)) mustBe AlreadyApplied
    }

    Scenario("a partly applied delivery applies only its new part (F5)") {
      decide(v(3), v(2), v(5)) mustBe Apply(v(4), v(5))
    }

    Scenario("a delivery right after the applied version is applied whole") {
      decide(v(3), v(4), v(6)) mustBe Apply(v(4), v(6))
    }

    Scenario("a delivery after missing versions asks for them") {
      decide(v(3), v(6), v(7)) mustBe Gap(v(4))
    }

    Scenario("the first delivery of a new aggregate starts at version 1") {
      decide(AggregateVersion.ZERO, v(1), v(2)) mustBe Apply(v(1), v(2))
      decide(AggregateVersion.ZERO, v(2), v(2)) mustBe Gap(v(1))
    }
  }

  Feature("Any order of deliveries, with duplicates, overlaps and losses") {

    // A subscriber driven only by the gate. Events are committed over time; each commit is delivered once (and may be
    // lost); re-deliveries repeat random earlier ranges; a gap asks for a re-publication of everything from the
    // missing version up to what is committed by then, which arrives later in random order. Nothing else is
    // buffered. After the last commit, an outbox re-publication of everything not yet applied arrives (D8).
    Scenario("the subscriber applies every version exactly once, in order (10 000 generated cases)") {
      for (seed <- 1 to 10000) {
        val random = new Random(seed)
        val events = 1 + random.nextInt(30)
        var committed = 0
        val inFlight = mutable.ArrayBuffer[(Int, Int)]()
        var applied = 0
        val appliedLog = mutable.ArrayBuffer[Int]()

        def deliverOne(): Unit = {
          val (first, last) = inFlight.remove(random.nextInt(inFlight.size))
          decide(v(applied), v(first), v(last)) match {
            case AlreadyApplied => ()
            case Apply(from, to) =>
              (from.asInt to to.asInt).foreach(appliedLog += _)
              applied = to.asInt
            case Gap(from) =>
              if (from.asInt <= committed) inFlight += ((from.asInt, committed))
          }
        }

        while (committed < events || inFlight.nonEmpty) {
          if (committed < events && (inFlight.isEmpty || random.nextInt(3) == 0)) {
            val batch = 1 + random.nextInt(math.min(3, events - committed))
            val range = (committed + 1, committed + batch)
            committed += batch
            if (random.nextInt(5) != 0) inFlight += range // one in five live deliveries is lost
            if (random.nextInt(4) == 0 && committed > 1) { // a re-delivery of an earlier range
              val first = 1 + random.nextInt(committed)
              inFlight += ((first, first + random.nextInt(committed - first + 1)))
            }
          } else {
            deliverOne()
          }
          if (committed == events && inFlight.isEmpty && applied < events) {
            inFlight += ((applied + 1, events)) // the outbox re-publication from the oldest unacked version
          }
        }

        withClue(s"seed $seed, $events events: ") {
          applied mustBe events
          appliedLog.toSeq mustBe (1 to events)
        }
      }
    }
  }
}
