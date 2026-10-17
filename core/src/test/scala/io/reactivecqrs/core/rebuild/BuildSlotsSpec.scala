package io.reactivecqrs.core.rebuild

import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{BuildSlotGranted, ReleaseBuildSlot, RequestBuildSlot}
import io.reactivecqrs.testkit.CommonSpec
import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

// F110: builds take turns; a supervisor that asks again while it holds or waits for a slot is not granted twice, and one
// that gives up while waiting leaves the queue.
class BuildSlotsSpec extends CommonSpec {

  Feature("Build slots") {

    Scenario("a supervisor that asks twice holds one slot, and the next one waits for it") {
      val slots = system.actorOf(Props(new BuildSlots(1)))
      val (a, b) = (TestProbe(), TestProbe())

      a.send(slots, RequestBuildSlot)
      a.send(slots, RequestBuildSlot)
      b.send(slots, RequestBuildSlot)

      a.expectMsg(3.seconds, BuildSlotGranted)
      a.expectNoMessage(200.millis)
      b.expectNoMessage(200.millis)
      a.send(slots, ReleaseBuildSlot)
      b.expectMsg(3.seconds, BuildSlotGranted)
    }

    Scenario("a supervisor that gives up while it waits is not granted a slot later") {
      val slots = system.actorOf(Props(new BuildSlots(1)))
      val (a, b, c) = (TestProbe(), TestProbe(), TestProbe())
      a.send(slots, RequestBuildSlot)
      a.expectMsg(3.seconds, BuildSlotGranted)
      b.send(slots, RequestBuildSlot)
      c.send(slots, RequestBuildSlot)

      b.send(slots, ReleaseBuildSlot)
      a.send(slots, ReleaseBuildSlot)

      c.expectMsg(3.seconds, BuildSlotGranted)
      b.expectNoMessage(200.millis)
    }
  }
}
