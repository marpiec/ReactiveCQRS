package io.reactivecqrs.core.aggregate

import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregate.SquashScheduler._
import io.reactivecqrs.core.eventstore.{EventA, EventB, MemoryEventStoreState, NewEvents, SomeAggregate}
import io.reactivecqrs.testkit.{CommonSpec, LogCapture}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

// D52: the scheduler, against the in-memory store, with a probe in place of the command bus.
class SquashSchedulerSpec extends CommonSpec {

  private val aggregateType = AggregateType.of[SomeAggregate]
  private val rule = AutomaticSquash(afterEvents = 3, keepLast = 1)

  /** Three aggregates in id order: one of 6 events (squashed up to 5), one of 2 (too short), one of 8 (up to 7). */
  private class Fixture(perRun: Int = 100, answerTimeout: FiniteDuration = 1.minute) {
    val store = new MemoryEventStoreState
    val Seq(first, short, last) = Seq(6, 2, 8).zipWithIndex.map { case (events, i) =>
      val id = AggregateId(1000 + i)
      store.localTx(session => store.persistEvents(id, NewEvents[SomeAggregate](UserId(1), AggregateVersion.ZERO, Instant.now,
        EventA("first") +: (2 to events).map(EventB)))(session)).get
      id
    }: @unchecked
    val bus = TestProbe()
    val scheduler: ActorRef = system.actorOf(Props(new SquashScheduler(store, Seq(SquashTarget(aggregateType, bus.ref, rule)),
      SquashSchedulerOptions(firstRunAfter = 1.hour, perRun = perRun, answerTimeout = answerTimeout))))

    def asked(id: AggregateId, upTo: Int): Unit = bus.expectMsg(SquashAggregate(id, AggregateVersion(upTo), metadata = Some(SquashScheduler.metadata)))
    def status: SquashStatus = scheduler.askFor[SquashStatus](GetSquashStatus)
  }

  Feature("The squash scheduler") {

    Scenario("asks for a squash of each aggregate its rule selects, one at a time, and counts the answers") {
      val f = new Fixture
      f.scheduler ! Run

      f.asked(f.first, 5)
      f.bus.expectNoMessage(200.millis)
      f.bus.reply(AggregateSquashed(f.first, AggregateVersion(5)))
      f.asked(f.last, 7)
      f.bus.reply(SquashRefused(f.last, "a subscription of the aggregate is failed"))

      awaitAssert(f.status mustBe SquashStatus(squashed = 1, refused = 1, waiting = 0), 3.seconds, 20.millis)
    }

    Scenario("goes on after the last aggregate of a run that reached its limit, and starts over after one that did not") {
      val f = new Fixture(perRun = 1)
      def runAnswered(id: AggregateId, upTo: Int): Unit = {
        f.scheduler ! Run
        f.asked(id, upTo)
        f.bus.reply(SquashRefused(id, "refused"))
      }
      runAnswered(f.first, 5)
      runAnswered(f.last, 7)
      f.scheduler ! Run // nothing after the last one: the next run starts over
      f.bus.expectNoMessage(300.millis)
      runAnswered(f.first, 5)
    }

    Scenario("goes on with the next aggregate when a squash is not answered in time, and skips a run while one is going") {
      val f = new Fixture(answerTimeout = 300.millis)
      f.scheduler ! Run
      f.asked(f.first, 5)
      f.scheduler ! Run
      f.asked(f.last, 7) // after the timeout, not from the second run
      f.bus.reply(AggregateSquashed(f.last, AggregateVersion(7)))

      awaitAssert(f.status mustBe SquashStatus(squashed = 1, refused = 0, waiting = 0), 3.seconds, 20.millis)
      f.bus.expectNoMessage(200.millis)
    }

    Scenario("F186 gives up a read that does not end after the read timeout, logs it, and reads again at the next run") {
      val reads = new AtomicInteger()
      val hanging = new CountDownLatch(1)
      val store = new MemoryEventStoreState {
        override def squashCandidates(aggregateType: AggregateType, policy: AutomaticSquash, after: AggregateId, limit: Int,
                                      now: Instant): Seq[(AggregateId, AggregateVersion)] = {
          if (reads.incrementAndGet() == 1) hanging.await() // a connection that no longer answers
          Nil
        }
      }
      val capture = new LogCapture(system)
      try {
        val scheduler = system.actorOf(Props(new SquashScheduler(store, Seq(SquashTarget(aggregateType, TestProbe().ref, rule)),
          SquashSchedulerOptions(firstRunAfter = 1.hour, readTimeout = 300.millis))))
        scheduler ! Run
        awaitAssert(reads.get mustBe 1, 3.seconds, 20.millis)
        Thread.sleep(400)
        scheduler ! Run
        awaitAssert(reads.get mustBe 2, 3.seconds, 20.millis)
        awaitAssert(capture.containing("did not end in") must not be empty, 3.seconds, 50.millis)
      } finally {
        hanging.countDown()
        capture.close()
      }
    }
  }
}
