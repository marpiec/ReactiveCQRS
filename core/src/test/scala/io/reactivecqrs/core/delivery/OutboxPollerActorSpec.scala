package io.reactivecqrs.core.delivery

import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus, Poll}
import io.reactivecqrs.core.eventbus.EventsBusActor.Republish
import io.reactivecqrs.core.eventstore.{MemoryEventStoreState, WaitingInOutbox}
import io.reactivecqrs.testkit.{CommonSpec, LogCapture, TestIds}
import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case class PolledNote(text: String)

// The poller's own bookkeeping, against an outbox that holds what the test says; its timing is RedeliveryPolicySpec's.
class OutboxPollerActorSpec extends CommonSpec {

  private val noteType = AggregateType.of[PolledNote]

  Feature("The outbox status") {

    Scenario("counts every aggregate waiting at the last poll, also those postponed beyond a poll's re-publications (F169)") {
      val before = Instant.now.minusSeconds(3600) // written before the poller started: each is due at once
      val waiting = (1 to 5).map(_ => WaitingInOutbox(noteType, AggregateId(TestIds.aggregate()), AggregateVersion(1), before))
      val outbox = new MemoryEventStoreState {
        override def readWaitingInOutbox(limit: Int): Seq[WaitingInOutbox] = waiting.take(limit)
      }
      val bus = TestProbe()
      val poller = system.actorOf(Props(new OutboxPollerActor(outbox, bus.ref, OutboxPollerOptions(firstPollAfter = 1.hour,
        pollInterval = 1.hour, redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 2)))))

      poller ! Poll
      bus.receiveN(2, 3.seconds).collect { case r: Republish => r.aggregateId } must have size 2
      bus.expectNoMessage(200.millis)

      poller.askFor[OutboxStatus](GetOutboxStatus, 3.seconds).waiting mustBe 5
    }
  }

  Feature("A read that does not end") {

    Scenario("F186 is given up after the read timeout: the poller logs it and reads again, and a late result is ignored") {
      val reads = new AtomicInteger()
      val hanging = new CountDownLatch(1)
      val outbox = new MemoryEventStoreState {
        override def readWaitingInOutbox(limit: Int): Seq[WaitingInOutbox] = {
          if (reads.incrementAndGet() == 1) hanging.await() // a connection that no longer answers
          Nil
        }
      }
      val capture = new LogCapture(system)
      try {
        val poller = system.actorOf(Props(new OutboxPollerActor(outbox, TestProbe().ref,
          OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour, readTimeout = 300.millis))))
        poller ! Poll
        awaitAssert(reads.get mustBe 1, 3.seconds, 20.millis)
        poller ! Poll
        reads.get mustBe 1 // not yet given up
        Thread.sleep(400)
        poller ! Poll
        awaitAssert(reads.get mustBe 2, 3.seconds, 20.millis)
        awaitAssert(capture.containing("did not end in") must not be empty, 3.seconds, 50.millis)
      } finally {
        hanging.countDown()
        capture.close()
      }
    }
  }
}
