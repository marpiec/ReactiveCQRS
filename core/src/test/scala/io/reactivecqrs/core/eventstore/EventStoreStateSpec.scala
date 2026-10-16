package io.reactivecqrs.core.eventstore

import java.time.Instant

import org.apache.pekko.actor.ActorRef
import io.reactivecqrs.api.{AggregateVersion, DuplicationEvent, Event, FirstEvent, UndoEvent}
import io.reactivecqrs.api.id.{AggregateId, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.testutils.CommonSpec

import scala.concurrent.duration._

case class SomeAggregate()

// Every scenario starts an aggregate with EventA, and PostgreSQL requires a FirstEvent (with a space) to create one.
case class EventA(text: String) extends FirstEvent[SomeAggregate] {
  def spaceId: SpaceId = SpaceId(0)
}
case class EventB(number: Int) extends Event[SomeAggregate]
case class EventC(predicate: Boolean) extends Event[SomeAggregate]

case class Undo(eventsCount: Int) extends UndoEvent[SomeAggregate]

case class Copy(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[SomeAggregate]


class TestFixture(val eventStoreState: EventStoreState) {

  val aggregateId = AggregateId(System.nanoTime() + Math.random().toLong)
  val commandId = CommandId(101)
  val userId = UserId(201)
  var expectedVersion = AggregateVersion(0)

  def storeEvents(events: Seq[Event[SomeAggregate]], id: AggregateId = aggregateId, exVersion: AggregateVersion = expectedVersion): Unit = {
    eventStoreState.localTx { session =>
      eventStoreState.persistEvents(id,
        PersistEvents(ActorRef.noSender, commandId, userId, exVersion, Instant.now, events, None))(session)
    }.get
    if(exVersion == expectedVersion) {
      expectedVersion = expectedVersion.incrementBy(events.length)
    }
  }

  def getEvents(version: Option[AggregateVersion] = None, id: AggregateId = aggregateId): Vector[Event[SomeAggregate]] = {
    var events = Vector[Event[SomeAggregate]]()
    eventStoreState.readAndProcessEvents[SomeAggregate](id, version.map(Left(_)))((userId: UserId, timestamp: Instant, event: Event[SomeAggregate], id: AggregateId, eventVersion: Int, noop: Boolean) => {
        if(!noop) {
          events :+= event
        }
      })
    events
  }
}


abstract class EventStoreStateSpec(val eventStoreState: EventStoreState) extends CommonSpec {

  feature("Can store and retrieve correct events for aggregate") {

    scenario("Simple adding and reading events") {

      Given("Event store state")

      val f = new TestFixture(eventStoreState)
      import f._


      When("Adding multiple events")

      storeEvents(List(EventA("one")))
      storeEvents(List(EventB(2)))
      storeEvents(List(EventC(false)))
      storeEvents(List(EventA("four")))
      storeEvents(List(EventB(5)))
      storeEvents(List(EventC(true)))
      storeEvents(List(EventA("seven")))

      Then("We can get all events in correct order")


      getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"), EventB(5), EventC(true), EventA("seven"))

    }


    scenario("Adding and reading events in batches") {

      Given("Event store state")

      val f = new TestFixture(eventStoreState)
      import f._

      When("Adding multiple events")

      storeEvents(List(EventA("one"), EventB(2), EventC(false), EventA("four")))
      storeEvents(List(EventB(5), EventC(true), EventA("seven")))

      Then("We can get all events in correct order")

      getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"), EventB(5), EventC(true), EventA("seven"))

    }
  }

  scenario("Adding and reading undo events") {

    Given("Event store state")
    val f = new TestFixture(eventStoreState)
    import f._

    When("Adding events and undoing some")

    storeEvents(List(EventA("one"))) //1
    storeEvents(List(EventB(2))) //2
    storeEvents(List(EventC(false)))//3
    storeEvents(List(EventA("four")))//4
    storeEvents(List(Undo(1)))//5
    storeEvents(List(EventB(5)))//6
    storeEvents(List(EventC(true)))//7
    storeEvents(List(Undo(2)))//8
    storeEvents(List(EventA("seven")))//9

    Then("We can get all events in correct order")

    getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("seven"))

    Then("We can get correct events for different versions")
    getEvents(Some(AggregateVersion(8))) mustBe List(EventA("one"), EventB(2), EventC(false))

    getEvents(Some(AggregateVersion(7))) mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), EventC(true))

    getEvents(Some(AggregateVersion(4))) mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"))

  }

  scenario("Adding and reading events for duplicated aggregates") {

    Given("Event store state")
    val f = new TestFixture(eventStoreState)
    import f._

    val aggregateBId = AggregateId(System.nanoTime() + Math.random().toLong)

    When("Adding events and undoing some")

    storeEvents(List(EventA("one"))) //1
    storeEvents(List(EventB(2))) //2
    storeEvents(List(EventC(false)))//3
    storeEvents(List(EventA("four")))//4
    storeEvents(List(Undo(1)))//5
    storeEvents(List(EventB(5)))//6

    storeEvents(List(Copy(SpaceId(1), aggregateId, AggregateVersion(6))), aggregateBId, AggregateVersion(0))
    storeEvents(List(EventA("newSix")), aggregateBId, AggregateVersion(1))

    storeEvents(List(EventA("oldSix")))

    Then("We can get all events in correct order")

    getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), EventA("oldSix"))
    getEvents(None, aggregateBId) mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), Copy(SpaceId(1), aggregateId, AggregateVersion(6)), EventA("newSix"))

  }

  //TODO test for undo and duplication

  // F12: ids beyond 32 bits (installation id ranges, D39) must survive the search for events to re-publish.
  Scenario("Aggregates with events waiting for publication are found, also with ids beyond 32 bits") {
    val aggregateId = AggregateId(5000000000L + System.nanoTime() % 1000000L)
    eventStoreState.localTx { session =>
      eventStoreState.persistEvents(aggregateId,
        PersistEvents(ActorRef.noSender, CommandId(1), UserId(1), AggregateVersion.ZERO, Instant.now.minusSeconds(120), Seq(EventA("one")), None))(session)
    }.get

    eventStoreState.readWaitingInOutbox().filter(_.aggregateId == aggregateId).map(w => (w.aggregateType.typeName, w.oldestVersion)) mustBe
      Seq((classOf[SomeAggregate].getName, AggregateVersion(1)))
  }

  // D8, F37: the outbox poller reads every waiting aggregate of every type in one query.
  Scenario("Every aggregate waiting in the outbox is listed once, with its oldest waiting version") {
    val a = new TestFixture(eventStoreState)
    val b = new TestFixture(eventStoreState)
    a.storeEvents(Seq(EventA("one"), EventB(2)))
    a.storeEvents(Seq(EventC(true)))
    b.storeEvents(Seq(EventA("other")))

    eventStoreState.deletePublished(a.aggregateId, Seq(AggregateVersion(1)))

    awaitAssert({
      val waiting = eventStoreState.readWaitingInOutbox().filter(w => w.aggregateId == a.aggregateId || w.aggregateId == b.aggregateId)
      waiting.map(w => (w.aggregateId, w.oldestVersion)).toSet mustBe Set((a.aggregateId, AggregateVersion(2)), (b.aggregateId, AggregateVersion(1)))
    }, 3.seconds, 20.millis)
  }

  // F26: deletes from many owners are batched; each one still removes exactly its own rows.
  Scenario("Deleting delivered versions of many aggregates removes exactly those rows") {
    val fixtures = (1 to 20).map(_ => new TestFixture(eventStoreState))
    fixtures.foreach(_.storeEvents(Seq(EventA("one"), EventB(2), EventC(true))))

    fixtures.foreach(f => eventStoreState.deletePublished(f.aggregateId, Seq(AggregateVersion(1), AggregateVersion(2))))

    awaitAssert(fixtures.foreach(f => eventStoreState.oldestVersionToPublish(f.aggregateId) mustBe Some(AggregateVersion(3))), 3.seconds, 20.millis)
  }

  Scenario("A permanent delete removes the aggregate's events and outbox rows") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))

    eventStoreState.deleteAggregatePermanently(f.aggregateId)

    f.getEvents() mustBe empty
    eventStoreState.oldestVersionToPublish(f.aggregateId) mustBe None
    eventStoreState.readWaitingInOutbox().map(_.aggregateId) must not contain f.aggregateId
  }

  // F7: restore must see the timestamp the live path used, which is the one given with the events.
  Scenario("Events keep the timestamp they were persisted with") {
    val f = new TestFixture(eventStoreState)
    import f.{aggregateId, commandId, userId}

    def persistAt(timestamp: Instant, expectedVersion: Int, events: Event[SomeAggregate]*): Unit =
      eventStoreState.localTx { session =>
        eventStoreState.persistEvents(aggregateId,
          PersistEvents(ActorRef.noSender, commandId, userId, AggregateVersion(expectedVersion), timestamp, events, None))(session)
      }.get

    val batchTime = Instant.parse("2020-01-02T03:04:05.123456Z")
    val singleTime = Instant.parse("2020-01-02T03:04:06.654321Z")
    persistAt(batchTime, 0, EventA("one"), EventB(2))
    persistAt(singleTime, 2, EventC(true))

    var timestamps = Vector[Instant]()
    eventStoreState.readAndProcessEvents[SomeAggregate](aggregateId, None)((_, eventTime, _, _, _, _) => timestamps :+= eventTime)
    timestamps mustBe Vector(batchTime, batchTime, singleTime)
  }

  // D60, D8: a re-publication sends a range of the aggregate's own events exactly as live delivery sent them.
  Scenario("A range of an aggregate's own events is read with the undone ones") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one")))
    f.storeEvents(Seq(EventB(2)))
    f.storeEvents(Seq(EventC(true)))
    f.storeEvents(Seq(Undo(1))) // undoes EventC
    f.storeEvents(Seq(EventB(5)))

    val range = eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(2), AggregateVersion(4))

    range.map(_.version.asInt) mustBe Seq(2, 3, 4)
    range.map(_.event) mustBe Seq(EventB(2), EventC(true), Undo(1))
    range.map(_.userId).distinct mustBe Seq(f.userId)
  }

  Scenario("The oldest version waiting in the outbox is found") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(false)))

    eventStoreState.oldestVersionToPublish(f.aggregateId) mustBe Some(AggregateVersion(1))
    eventStoreState.deletePublished(f.aggregateId, Seq(AggregateVersion(1)))
    awaitAssert(eventStoreState.oldestVersionToPublish(f.aggregateId) mustBe Some(AggregateVersion(2)), 3.seconds, 20.millis)
    eventStoreState.oldestVersionToPublish(AggregateId(f.aggregateId.asLong + 1)) mustBe None
  }

}
