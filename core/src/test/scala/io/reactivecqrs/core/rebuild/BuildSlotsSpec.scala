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

      a.send(slots, RequestBuildSlot())
      a.send(slots, RequestBuildSlot())
      b.send(slots, RequestBuildSlot())

      a.expectMsg(3.seconds, BuildSlotGranted)
      a.expectNoMessage(200.millis)
      b.expectNoMessage(200.millis)
      a.send(slots, ReleaseBuildSlot)
      b.expectMsg(3.seconds, BuildSlotGranted)
    }

    Scenario("a supervisor that gives up while it waits is not granted a slot later") {
      val slots = system.actorOf(Props(new BuildSlots(1)))
      val (a, b, c) = (TestProbe(), TestProbe(), TestProbe())
      a.send(slots, RequestBuildSlot())
      a.expectMsg(3.seconds, BuildSlotGranted)
      b.send(slots, RequestBuildSlot())
      c.send(slots, RequestBuildSlot())

      b.send(slots, ReleaseBuildSlot)
      a.send(slots, ReleaseBuildSlot)

      c.expectMsg(3.seconds, BuildSlotGranted)
      b.expectNoMessage(200.millis)
    }
    // D99: among the builds that wait, the one with the highest priority gets the next turn; equal ones in the order they asked.
    Scenario("the waiting build with the highest priority gets the next turn, equal ones in the order they asked") {
      val slots = system.actorOf(Props(new BuildSlots(1)))
      val (running, first, urgent, last, mostUrgent) = (TestProbe(), TestProbe(), TestProbe(), TestProbe(), TestProbe())
      running.send(slots, RequestBuildSlot())
      running.expectMsg(3.seconds, BuildSlotGranted)
      first.send(slots, RequestBuildSlot())
      urgent.send(slots, RequestBuildSlot(priority = 1))
      last.send(slots, RequestBuildSlot())
      mostUrgent.send(slots, RequestBuildSlot(priority = 2))

      // Each release gives the turn to the next one, and to nobody else
      val turns = Seq(running, mostUrgent, urgent, first, last)
      for ((holder, next) <- turns.zip(turns.tail)) {
        holder.send(slots, ReleaseBuildSlot)
        next.expectMsg(3.seconds, BuildSlotGranted)
        turns.filter(_ != next).foreach(_.expectNoMessage(50.millis))
      }
    }

    Scenario("a build held with the others starts first once the hold is over, whenever it asked") {
      var held: Option[String] = Some("the store is not ready")
      val slots = system.actorOf(Props(new BuildSlots(1, Some(BuildHold(() => held, 100.millis)))))
      val (ordinary, access) = (TestProbe(), TestProbe())
      ordinary.send(slots, RequestBuildSlot())
      access.send(slots, RequestBuildSlot(priority = 1))
      ordinary.expectMsgType[ProjectionSupervisor.BuildSlotHeld](3.seconds)

      held = None

      access.fishForMessage(3.seconds) { case BuildSlotGranted => true; case _ => false }
      ordinary.fishForMessage(1.second) { case ProjectionSupervisor.BuildSlotHeld(None) => true; case _ => false }
      ordinary.expectNoMessage(200.millis)
      access.send(slots, ReleaseBuildSlot)
      ordinary.expectMsg(3.seconds, BuildSlotGranted)
    }
  }
}
