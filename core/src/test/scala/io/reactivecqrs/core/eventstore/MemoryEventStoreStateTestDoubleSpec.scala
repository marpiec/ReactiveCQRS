package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateVersion, Event, EventWithVersion}
import io.reactivecqrs.core.eventstore.NewEvents
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc.NoSession

case class OtherAggregate()

case class OtherEvent(text: String) extends Event[OtherAggregate]

// F14: the in-memory event store must behave like PostgresEventStoreState for the outbox and rewrites
class MemoryEventStoreStateTestDoubleSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  private val aggregateA = AggregateId(1)
  private val aggregateC = AggregateId(3)

  // Long before the test, like rows a restart finds in the outbox.
  private val longAgo = Instant.now.minusSeconds(120)

  private def persist[R](store: MemoryEventStoreState, aggregateId: AggregateId, expectedVersion: Int, events: Event[R]*): Unit = {
    store.persistEvents(aggregateId,
      NewEvents(UserId(1), AggregateVersion(expectedVersion), longAgo, events))(NoSession).get
  }

  private def eventsOf(store: MemoryEventStoreState, aggregateId: AggregateId): Vector[Event[SomeAggregate]] = {
    var events = Vector[Event[SomeAggregate]]()
    store.readAndProcessEvents[SomeAggregate](aggregateId, None)((_, _, event, _, _, _) => events :+= event)
    events
  }

  Feature("Events waiting for publication") {
    Scenario("are listed per aggregate with the oldest waiting version and the aggregate type") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"), EventB(2))
      persist(store, aggregateC, 0, OtherEvent("c1"))
      persist(store, aggregateA, 2, EventC(predicate = true))

      store.readWaitingInOutbox().map(w => (w.aggregateId, w.aggregateType.typeName, w.oldestVersion.asInt)).toSet mustBe
        Set((aggregateA, classOf[SomeAggregate].getName, 1), (aggregateC, classOf[OtherAggregate].getName, 1))
    }

    Scenario("deleting published events removes only those and tolerates a repeated delete") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"), EventB(2), EventC(predicate = true))
      val published = Seq(AggregateVersion(1), AggregateVersion(2))

      store.deletePublished(aggregateA, published)
      store.deletePublished(aggregateA, published)

      store.readWaitingInOutbox().map(w => (w.aggregateId, w.oldestVersion.asInt)) mustBe Seq((aggregateA, 3))
    }
  }

  Feature("Overwriting events") {
    Scenario("readers and re-publication see the rewritten event") {
      Given("an aggregate with two events")
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("original"), EventB(2))

      When("the first event is rewritten")
      store.overwriteEvents(aggregateA, Seq(EventWithVersion[SomeAggregate](AggregateVersion(1), EventA("rewritten"))))(NoSession)

      Then("restore and re-publication both read the rewritten event")
      eventsOf(store, aggregateA) mustBe Vector(EventA("rewritten"), EventB(2))
      store.readEventsRange[SomeAggregate](aggregateA, AggregateVersion(1), AggregateVersion(2)).map(_.event) mustBe Seq(EventA("rewritten"), EventB(2))
    }
  }
}
