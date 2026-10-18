package io.reactivecqrs.core.delivery

import java.time.Instant

import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.delivery.RedeliveryPolicy._
import io.reactivecqrs.core.eventstore.WaitingInOutbox
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._

// D8, D20b: re-publications at 3 and 6 minutes after the first attempt, then "undelivered" once at 10 minutes; rows
// from before the start get a fresh set of attempts, the first one at once.
class RedeliveryPolicySpec extends AnyFeatureSpec with Matchers {

  private val options = Options()
  private val startedAt = Instant.parse("2026-10-16T10:00:00Z")
  private val cartType = AggregateType("Cart")

  private def waiting(id: Long, version: Int, since: Instant) = WaitingInOutbox(cartType, AggregateId(id), AggregateVersion(version), since)

  private def at(after: FiniteDuration): Instant = startedAt.plusMillis(after.toMillis)

  /** Polls every 30 seconds from `from` to `to` with the same waiting rows; returns when each re-publication and
    * each give-up happened. */
  private def pollEvery30Seconds(rows: Seq[WaitingInOutbox], from: FiniteDuration, to: FiniteDuration,
                                 memory: Map[AggregateId, Tracked] = Map.empty): (Seq[(FiniteDuration, Long)], Seq[(FiniteDuration, Long)], Map[AggregateId, Tracked]) = {
    var current = memory
    var republished = Vector[(FiniteDuration, Long)]()
    var gaveUp = Vector[(FiniteDuration, Long)]()
    var time = from
    while (time <= to) {
      val decision = decide(rows, current, startedAt, at(time), options)
      republished ++= decision.republish.map(w => time -> w.aggregateId.asLong)
      gaveUp ++= decision.gaveUp.map(w => time -> w.aggregateId.asLong)
      current = decision.memory
      time += 30.seconds
    }
    (republished, gaveUp, current)
  }

  Feature("Rows written while the process runs") {

    Scenario("F268 one first seen late is re-published once, and given up only as long after it as the last attempt gets") {
      val row = waiting(1, 4, at(1.minute))

      val (republished, gaveUp, _) = pollEvery30Seconds(Seq(row), 12.minutes, 20.minutes) // first seen 11 minutes old

      republished mustBe Seq(12.minutes -> 1L)
      gaveUp mustBe Seq(16.minutes -> 1L)
    }

    Scenario("are re-published at 3 and 6 minutes, and given up once at 10 minutes") {
      val row = waiting(1, 4, at(1.minute))

      val (republished, gaveUp, memory) = pollEvery30Seconds(Seq(row), 1.minute, 20.minutes)

      republished mustBe Seq(4.minutes -> 1L, 7.minutes -> 1L)
      gaveUp mustBe Seq(11.minutes -> 1L)
      memory(AggregateId(1)).gaveUp mustBe true
    }

    // F123: a subscriber may be back without a restart of the poller.
    Scenario("once given up, are reported once and re-published again every 30 minutes") {
      val row = waiting(1, 4, at(1.minute))

      val (republished, gaveUp, _) = pollEvery30Seconds(Seq(row), 1.minute, 75.minutes)

      republished mustBe Seq(4.minutes -> 1L, 7.minutes -> 1L, 41.minutes -> 1L, 71.minutes -> 1L)
      gaveUp mustBe Seq(11.minutes -> 1L)
    }

    Scenario("seen late, after a long pause, get one re-publication, not one per missed attempt") {
      val row = waiting(1, 4, at(0.seconds))

      val decision = decide(Seq(row), Map.empty, startedAt.minusSeconds(60), at(8.minutes), options)

      decision.republish mustBe Seq(row)
      decide(Seq(row), decision.memory, startedAt.minusSeconds(60), at(8.minutes + 30.seconds), options).republish mustBe empty
    }
  }

  Feature("Rows written before the process started") {

    Scenario("are re-published at the first poll, then 3 and 6 minutes later, and given up at 10 minutes") {
      val row = waiting(2, 1, startedAt.minusSeconds(3600))

      val (republished, gaveUp, _) = pollEvery30Seconds(Seq(row), 10.seconds, 20.minutes)

      republished.map(_._1) mustBe Seq(10.seconds, 3.minutes + 10.seconds, 6.minutes + 10.seconds)
      gaveUp.map(_._1) mustBe Seq(10.minutes + 10.seconds)
    }
  }

  Feature("Load") {

    Scenario("at most the configured number of aggregates is re-published per poll, the longest waiting first") {
      val rows = (1 to 5).map(n => waiting(n, 1, startedAt.minusSeconds(100 - n))) // aggregate 1 waits longest
      val limited = options.copy(maxRepublishPerPoll = 2)

      val first = decide(rows, Map.empty, startedAt, at(10.seconds), limited)
      val second = decide(rows, first.memory, startedAt, at(40.seconds), limited)
      val third = decide(rows, second.memory, startedAt, at(70.seconds), limited)

      first.republish.map(_.aggregateId.asLong) mustBe Seq(1, 2)
      second.republish.map(_.aggregateId.asLong) mustBe Seq(3, 4)
      third.republish.map(_.aggregateId.asLong) mustBe Seq(5)
      decide(rows, third.memory, startedAt, at(3.minutes + 10.seconds), limited).republish.map(_.aggregateId.asLong) mustBe Seq(1, 2)
    }

    Scenario("retries of given-up aggregates take at most half the budget while newer rows are due, so those still get their attempts (F141)") {
      val limited = options.copy(maxRepublishPerPoll = 4)
      val now = at(2.hours)
      val givenUp = (1 to 6).map(n => waiting(n, 1, at(1.minute))) // waiting longest, their retries due
      val memory = givenUp.map(w => w.aggregateId -> Tracked(w, at(1.minute), attemptsMade = 2, gaveUp = true, lastTry = Some(at(1.hour)))).toMap
      val fresh = (7 to 9).map(n => waiting(n, 1, now.minusSeconds(200))) // their first re-publication due

      val first = decide(givenUp ++ fresh, memory, startedAt, now, limited)
      val second = decide(givenUp ++ fresh, first.memory, startedAt, now.plusSeconds(30), limited)

      first.republish.map(_.aggregateId.asLong) mustBe Seq(1, 2, 7, 8)
      second.republish.map(_.aggregateId.asLong) mustBe Seq(3, 4, 5, 9) // what attempts leave goes to retries
    }
  }

  Feature("Memory") {

    Scenario("a newer oldest version, after the older rows were delivered, gets a fresh set of attempts") {
      val (_, _, memory) = pollEvery30Seconds(Seq(waiting(1, 4, at(0.seconds))), 0.seconds, 12.minutes)
      memory(AggregateId(1)).gaveUp mustBe true

      val (republished, _, _) = pollEvery30Seconds(Seq(waiting(1, 6, at(12.minutes))), 12.minutes, 16.minutes, memory)

      republished mustBe Seq(15.minutes -> 1L)
    }

    Scenario("forgets an aggregate once its rows are gone") {
      val (_, _, memory) = pollEvery30Seconds(Seq(waiting(1, 4, at(0.seconds)), waiting(2, 1, at(0.seconds))), 0.seconds, 1.minute)
      memory.keySet mustBe Set(AggregateId(1), AggregateId(2))

      decide(Seq(waiting(2, 1, at(0.seconds))), memory, startedAt, at(90.seconds), options).memory.keySet mustBe Set(AggregateId(2))
      decide(Seq.empty, memory, startedAt, at(90.seconds), options).memory mustBe empty
    }
  }
}
