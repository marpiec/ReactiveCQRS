package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.api.{AggregateVersion, Event, EventWithIdentifier, EventWithVersion}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import org.apache.pekko.actor.ActorRef
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc.NoSession

case class OtherAggregate()

case class OtherEvent(text: String) extends Event[OtherAggregate]

// F14: the in-memory event store must behave like PostgresEventStoreState for the outbox and rewrites
class MemoryEventStoreStateTestDoubleSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  private val aggregateA = AggregateId(1)
  private val aggregateB = AggregateId(2)
  private val aggregateC = AggregateId(3)

  // Older than the 10 s / 60 s thresholds used when looking for events to re-publish.
  private val longAgo = Instant.now.minusSeconds(120)

  private def persist[R](store: MemoryEventStoreState, aggregateId: AggregateId, expectedVersion: Int, events: Event[R]*): Unit = {
    store.persistEvents(Map.empty, aggregateId,
      PersistEvents(ActorRef.noSender, CommandId(1), UserId(1), AggregateVersion(expectedVersion), longAgo, events, None))(NoSession).get
  }

  private def eventsOf(store: MemoryEventStoreState, aggregateId: AggregateId): Vector[Event[SomeAggregate]] = {
    var events = Vector[Event[SomeAggregate]]()
    store.readAndProcessEvents[SomeAggregate](Map.empty, aggregateId, None)((_, _, event, _, _, _) => events :+= event)
    events
  }

  Feature("Events waiting for publication") {
    Scenario("are read per aggregate, in version order") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"), EventB(2))
      persist(store, aggregateB, 0, EventA("b1"))
      persist(store, aggregateA, 2, EventC(predicate = true))

      store.readEventsToPublishForAggregate[SomeAggregate](Map.empty, aggregateA).map(e => (e.aggregateId, e.version.asInt)) mustBe List((aggregateA, 1), (aggregateA, 2), (aggregateA, 3))
      store.readEventsToPublishForAggregate[SomeAggregate](Map.empty, aggregateB).map(e => (e.aggregateId, e.version.asInt)) mustBe List((aggregateB, 1))
    }

    Scenario("deleting published events removes only those and tolerates a repeated delete") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"), EventB(2), EventC(predicate = true))
      val published = Seq(EventWithIdentifier[SomeAggregate](aggregateA, AggregateVersion(1), EventA("a1")), EventWithIdentifier[SomeAggregate](aggregateA, AggregateVersion(2), EventB(2)))

      store.deletePublishedEventsToPublish(aggregateA, published)
      store.deletePublishedEventsToPublish(aggregateA, published)

      store.readEventsToPublishForAggregate[SomeAggregate](Map.empty, aggregateA).map(_.version.asInt) mustBe List(3)
    }

    Scenario("aggregates with events to publish are filtered by aggregate type") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"))
      persist(store, aggregateC, 0, OtherEvent("c1"))

      var found = List[AggregateId]()
      store.readAggregatesWithEventsToPublish(classOf[SomeAggregate].getName, oldOnly = true)(id => found ::= id)

      found mustBe List(aggregateA)
    }
  }

  Feature("Overwriting events") {
    Scenario("readers and re-publication see the rewritten event") {
      Given("an aggregate with two events")
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("original"), EventB(2))

      When("the first event is rewritten")
      store.overwriteEvents(aggregateA, Seq(EventWithVersion[SomeAggregate](AggregateVersion(1), EventA("rewritten"))))(NoSession)

      Then("restore and the outbox both return the rewritten event")
      eventsOf(store, aggregateA) mustBe Vector(EventA("rewritten"), EventB(2))
      store.readEventsToPublishForAggregate[SomeAggregate](Map.empty, aggregateA).map(_.event) mustBe List(EventA("rewritten"), EventB(2))
    }
  }

  Feature("Counting events") {
    Scenario("events are counted for the given aggregate types only") {
      val store = new MemoryEventStoreState
      persist(store, aggregateA, 0, EventA("a1"), EventB(2))
      persist(store, aggregateC, 0, OtherEvent("c1"))

      store.countEventsForAggregateTypes(Seq(classOf[SomeAggregate].getName)) mustBe 2
      store.countEventsForAggregateTypes(Seq(classOf[SomeAggregate].getName, classOf[OtherAggregate].getName)) mustBe 3
      store.countEventsForAggregateTypes(Seq.empty) mustBe 0
    }
  }
}
