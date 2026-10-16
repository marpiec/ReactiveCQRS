package io.reactivecqrs.core.eventstore

import java.time.Instant

import org.apache.pekko.actor.ActorRef
import io.reactivecqrs.api.{AggregateVersion, DuplicationEvent, Event, EventMetadata, EventWithVersion, FirstEvent, UndoEvent}
import io.reactivecqrs.api.id.{AggregateId, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.testutils.CommonSpec

import scala.concurrent.duration._
import scala.util.Try

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
        NewEvents(userId, exVersion, Instant.now, events))(session)
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
        NewEvents(UserId(1), AggregateVersion.ZERO, Instant.now.minusSeconds(120), Seq(EventA("one"))))(session)
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
          NewEvents(userId, AggregateVersion(expectedVersion), timestamp, events))(session)
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

  // D30: the tail after a snapshot.
  Scenario("The aggregate's own events after a version are read with whether they were undone at the version read") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one")))
    f.storeEvents(Seq(EventB(2)))
    f.storeEvents(Seq(EventC(true)))
    f.storeEvents(Seq(Undo(1))) // undoes EventC
    f.storeEvents(Seq(EventB(5)))

    def tail(upTo: Option[Int]): Seq[(Int, Boolean)] = {
      var read = Vector[(Int, Boolean)]()
      eventStoreState.readAndProcessOwnEvents[SomeAggregate](f.aggregateId, AggregateVersion(2), upTo.map(AggregateVersion(_)))((_, _, _, version, noop) => read :+= (version, noop))
      read
    }
    tail(None) mustBe Seq((3, true), (4, true), (5, false))
    tail(Some(3)) mustBe Seq((3, false))
  }

  // D30: snapshots are disposable; the changes that would make them wrong delete them.
  Scenario("A snapshot is read back as written, and an undo or a rewrite deletes it, also a duplicate's") {
    val f = new TestFixture(eventStoreState)
    val stateClass = classOf[SomeAggregate].getName
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    val snapshot = Snapshot(AggregateVersion(3), 7, 2, SomeAggregate())

    eventStoreState.writeSnapshot(f.aggregateId, snapshot, stateClass)
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass).map(_.get) mustBe Some(snapshot)

    f.storeEvents(Seq(Undo(1)))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None

    val copyId = AggregateId(System.nanoTime() + 1)
    f.storeEvents(Seq(Copy(SpaceId(1), f.aggregateId, AggregateVersion(4))), copyId, AggregateVersion(0))
    eventStoreState.writeSnapshot(f.aggregateId, snapshot.copy(version = AggregateVersion(4)), stateClass)
    eventStoreState.writeSnapshot(copyId, snapshot.copy(version = AggregateVersion(1)), stateClass)
    eventStoreState.localTx(session => eventStoreState.overwriteEvents(f.aggregateId, Seq(EventWithVersion(AggregateVersion(1), EventA("rewritten"))))(session))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None
    eventStoreState.readSnapshot[SomeAggregate](copyId, stateClass) mustBe None
  }

  // D32, R13: the metadata envelope travels with the events; events stored without one get their user as the actor.
  Scenario("A command's metadata envelope is read back with each of its events, and a default one for events without") {
    val f = new TestFixture(eventStoreState)
    val metadata = EventMetadata("agent:7", correlationId = Some("corr"), causationId = Some("cmd 1"), traceContext = Some("00-ab-cd-01"),
      spaceId = Some(SpaceId(3)), custom = Map("channel" -> "api"))
    eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId,
      NewEvents(f.userId, AggregateVersion.ZERO, Instant.now, Seq(EventA("one"), EventB(2)), Some(metadata)))(session)).get
    eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId,
      NewEvents(f.userId, AggregateVersion(2), Instant.now, Seq(EventC(true))))(session)).get

    val range = eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(3))
    range.map(_.storedMetadata) mustBe Seq(Some(metadata), Some(metadata), None)
    range.last.metadata mustBe EventMetadata.ofUser(f.userId)

    var read = Vector[Option[EventMetadata]]()
    eventStoreState.readAndProcessEventsWithMetadata[SomeAggregate](f.aggregateId, None)((event, _, _) => read :+= event.storedMetadata)
    read mustBe Vector(Some(metadata), Some(metadata), None)

    var replayed = Vector[Option[EventMetadata]]()
    eventStoreState.readAndProcessAllEvents(classOf[SomeAggregate].getName, batchPerAggregate = false, (events, aggregateId, _) =>
      if (aggregateId == f.aggregateId) replayed ++= events.map(_.storedMetadata))
    replayed mustBe Vector(Some(metadata), Some(metadata), None)
  }

  // D17, F8: a rewrite may change an event's values, never its class.
  Scenario("A rewrite that would change an event's class is refused, and nothing is rewritten") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))

    val refused = Try(eventStoreState.localTx(session => eventStoreState.overwriteEvents(f.aggregateId,
      Seq(EventWithVersion(AggregateVersion(1), EventA("rewritten")), EventWithVersion(AggregateVersion(2), EventC(true))))(session)))
    refused.failed.get mustBe a[RewriteChangesClassException]
    f.getEvents() mustBe Seq(EventA("one"), EventB(2))

    eventStoreState.localTx(session => eventStoreState.overwriteEvents(f.aggregateId, Seq(EventWithVersion(AggregateVersion(2), EventB(20))))(session))
    f.getEvents() mustBe Seq(EventA("one"), EventB(20))
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
