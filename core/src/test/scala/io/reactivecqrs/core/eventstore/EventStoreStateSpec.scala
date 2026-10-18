package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.{AggregateSquashedException, AggregateType, AggregateVersion, AutomaticSquash, DuplicationEvent, Event, EventMetadata, EventWithVersion, FirstEvent, HistoryChange, UndoEvent}
import io.reactivecqrs.api.id.{AggregateId, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.testkit.{CommonSpec, TestIds}

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

// D76: a request to erase a number from a history.
case class EraseNumber(number: Int) extends HistoryChange

// F50: an aggregate root class nested in an object.
object Nesting {
  case class NestedRoot()
  case class NestedCreated() extends FirstEvent[NestedRoot] {
    def spaceId: SpaceId = SpaceId(0)
  }
}


class TestFixture(val eventStoreState: EventStoreState) {

  val aggregateId = AggregateId(TestIds.aggregate()) // unique in the run: consecutive nanoTime readings can be equal
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

  Feature("Can store and retrieve correct events for aggregate") {

    Scenario("Simple adding and reading events") {

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


    Scenario("Adding and reading events in batches") {

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

  Scenario("Adding and reading undo events") {

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

  Scenario("Adding and reading events for duplicated aggregates") {

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

    eventStoreState.deleteAggregatePermanently(f.aggregateId) mustBe Right(())

    f.getEvents() mustBe empty
    eventStoreState.oldestVersionToPublish(f.aggregateId) mustBe None
    eventStoreState.readWaitingInOutbox().map(_.aggregateId) must not contain f.aggregateId
  }

  // F7: restore must see the timestamp the live path used, which is the one given with the events.
  Scenario("Events keep the timestamp they were persisted with") {
    val f = new TestFixture(eventStoreState)
    import f.{aggregateId, userId}

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
      eventStoreState.readAndProcessOwnEvents[SomeAggregate](f.aggregateId, AggregateVersion(2), upTo.map(AggregateVersion(_)))((_, _, _, version, noop) => read :+= ((version, noop)))
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

    eventStoreState.writeSnapshot(f.aggregateId, snapshot, stateClass, eventStoreState.historyGeneration(f.aggregateId))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass).map(_.get) mustBe Some(snapshot)

    f.storeEvents(Seq(Undo(1)))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None

    val copyId = AggregateId(System.nanoTime() + 1)
    f.storeEvents(Seq(Copy(SpaceId(1), f.aggregateId, AggregateVersion(4))), copyId, AggregateVersion(0))
    eventStoreState.writeSnapshot(f.aggregateId, snapshot.copy(version = AggregateVersion(4)), stateClass, eventStoreState.historyGeneration(f.aggregateId))
    eventStoreState.writeSnapshot(copyId, snapshot.copy(version = AggregateVersion(1)), stateClass, eventStoreState.historyGeneration(copyId))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass).isDefined mustBe true
    eventStoreState.readSnapshot[SomeAggregate](copyId, stateClass).isDefined mustBe true
    eventStoreState.localTx(session => eventStoreState.overwriteEvents(f.aggregateId, Seq(EventWithVersion(AggregateVersion(1), EventA("rewritten"))))(session))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None
    eventStoreState.readSnapshot[SomeAggregate](copyId, stateClass) mustBe None
  }

  // F119: an owner that read its state before such a change, and stops after it, must not write that state back.
  Scenario("A snapshot of a state read before an undo or a rewrite is not written, also a duplicate's") {
    val f = new TestFixture(eventStoreState)
    val stateClass = classOf[SomeAggregate].getName
    val snapshot = Snapshot(AggregateVersion(2), 7, 2, SomeAggregate())
    f.storeEvents(Seq(EventA("one"), EventB(2)))
    val copyId = AggregateId(System.nanoTime() + 1)
    f.storeEvents(Seq(Copy(SpaceId(1), f.aggregateId, AggregateVersion(2))), copyId, AggregateVersion(0))
    val readAt = eventStoreState.historyGeneration(f.aggregateId)
    val copyReadAt = eventStoreState.historyGeneration(copyId)

    eventStoreState.localTx(session => eventStoreState.overwriteEvents(f.aggregateId, Seq(EventWithVersion(AggregateVersion(1), EventA("rewritten"))))(session))
    eventStoreState.writeSnapshot(f.aggregateId, snapshot, stateClass, readAt)
    eventStoreState.writeSnapshot(copyId, snapshot.copy(version = AggregateVersion(1)), stateClass, copyReadAt)
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None
    eventStoreState.readSnapshot[SomeAggregate](copyId, stateClass) mustBe None

    val rewrittenAt = eventStoreState.historyGeneration(f.aggregateId)
    f.storeEvents(Seq(Undo(1)))
    eventStoreState.writeSnapshot(f.aggregateId, snapshot, stateClass, rewrittenAt)
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass) mustBe None

    eventStoreState.writeSnapshot(f.aggregateId, snapshot.copy(version = AggregateVersion(3)), stateClass, eventStoreState.historyGeneration(f.aggregateId))
    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, stateClass).map(_.get.version) mustBe Some(AggregateVersion(3))
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

    eventStoreState.readCommitted(Seq(AggregateType.of[SomeAggregate]), 0L, Int.MaxValue, AggregateScope.Only(Set(f.aggregateId)))
      .map(_.event.toOption.get.storedMetadata) mustBe Vector(Some(metadata), Some(metadata), None)
  }

  // D59, D60: what a rebuild reads.
  Scenario("Events are read in commit order, each under its own aggregate, undone ones and undos too, within a scope") {
    val (a, b) = (new TestFixture(eventStoreState), new TestFixture(eventStoreState))
    a.storeEvents(Seq(EventA("a"), EventB(2)))
    b.storeEvents(Seq(EventA("b")))
    a.storeEvents(Seq(Undo(1)))
    val copy = new TestFixture(eventStoreState)
    copy.storeEvents(Seq(Copy(SpaceId(0), a.aggregateId, AggregateVersion(2)), EventC(true)))
    val ids = Set(a.aggregateId, b.aggregateId, copy.aggregateId)
    val someType = AggregateType.of[SomeAggregate]

    val rows = eventStoreState.readCommitted(Seq(someType), 0L, Int.MaxValue, AggregateScope.Only(ids))
    rows.map(_.id) mustBe rows.map(_.id).sorted
    rows.map(row => (row.aggregateId, row.version.asInt, row.event.toOption.get.event.getClass.getSimpleName)) mustBe Seq(
      (a.aggregateId, 1, "EventA"), (a.aggregateId, 2, "EventB"), (b.aggregateId, 1, "EventA"), (a.aggregateId, 3, "Undo"),
      (copy.aggregateId, 1, "Copy"), (copy.aggregateId, 2, "EventC")) // the copy's own events only
    eventStoreState.readCommitted(Seq(someType), rows(2).id, 2, AggregateScope.Only(ids)).map(_.version.asInt) mustBe Seq(3, 1)

    eventStoreState.aggregatesOf(Seq(someType), AggregateId(Long.MinValue), 10, AggregateScope.Only(ids))
      .map(s => (s.aggregateId, s.version.asInt)) mustBe Seq(a, b, copy).map(_.aggregateId).sortBy(_.asLong).map(id =>
      id -> (if (id == a.aggregateId) 3 else if (id == b.aggregateId) 1 else 2))
    eventStoreState.aggregatesOf(Seq(someType), AggregateId(Long.MinValue), 10, AggregateScope.InSpace(SpaceId(77))) mustBe empty
  }

  // F173: a builder walks the aggregates of a projection's types in id order, a page at a time.
  Scenario("A page of several types' aggregates is in id order across the types, after an id, at most a limit long") {
    val a = new TestFixture(eventStoreState)
    val nested = AggregateId(TestIds.aggregate()) // between the two of the other type
    val b = new TestFixture(eventStoreState)
    a.storeEvents(Seq(EventA("a")))
    eventStoreState.localTx(session => eventStoreState.persistEvents(nested,
      NewEvents[Nesting.NestedRoot](UserId(1), AggregateVersion.ZERO, Instant.now, Seq(Nesting.NestedCreated())))(session)).get
    b.storeEvents(Seq(EventA("b")))
    val ids = Seq(a.aggregateId, nested, b.aggregateId)
    val types = Seq(AggregateType.of[SomeAggregate], AggregateType.of[Nesting.NestedRoot])
    def page(after: AggregateId, limit: Int): Seq[AggregateId] =
      eventStoreState.aggregatesOf(types, after, limit, AggregateScope.Only(ids.toSet)).map(_.aggregateId)

    ids mustBe ids.sortBy(_.asLong)
    page(AggregateId(Long.MinValue), 10) mustBe ids
    page(AggregateId(Long.MinValue), 2) mustBe ids.take(2)
    page(ids.head, 10) mustBe ids.tail
    eventStoreState.aggregatesOf(types.take(1), AggregateId(Long.MinValue), 10, AggregateScope.Only(ids.toSet)).map(_.aggregateId) mustBe
      Seq(a.aggregateId, b.aggregateId)
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

  // As add_undo_event: an undo counts back over the events not undone yet, so it never undoes another undo.
  Scenario("An undo right after another undoes the newest event that is not undone yet") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one")))
    f.storeEvents(Seq(EventB(2)))
    f.storeEvents(Seq(EventC(true)))
    f.storeEvents(Seq(Undo(1))) // undoes EventC
    f.storeEvents(Seq(Undo(1))) // undoes EventB

    f.getEvents() mustBe Seq(EventA("one"))
    f.getEvents(Some(AggregateVersion(4))) mustBe Seq(EventA("one"), EventB(2))
  }

  protected def newId(): AggregateId = AggregateId(TestIds.aggregate())

  protected def published(ids: AggregateId*): Unit = {
    ids.foreach(id => eventStoreState.deletePublished(id, (1 to 20).map(AggregateVersion(_))))
    awaitAssert(ids.foreach(id => eventStoreState.oldestVersionToPublish(id) mustBe None), 3.seconds, 20.millis)
  }

  protected def squash(f: TestFixture, upTo: Int, detach: Seq[(AggregateId, FirstEvent[_])] = Nil, metadata: Option[EventMetadata] = None,
                       archive: Set[String] = Set.empty) =
    eventStoreState.squash(f.aggregateId, AggregateVersion(upTo), EventA("squashed"), UserId(9), metadata, detach, archive)

  // D47, D49, D51.
  Scenario("A squash keeps the version and the later events; reading, undoing and duplicating below the point are refused") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    f.storeEvents(Seq(EventB(4)))
    published(f.aggregateId)
    val metadata = EventMetadata("operator")

    squash(f, 3, metadata = Some(metadata)) mustBe Right(())

    f.getEvents() mustBe Seq(EventA("squashed"), EventB(4))
    eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(4)).map(e => (e.version.asInt, e.userId, e.storedMetadata)) mustBe
      Seq((3, UserId(9), Some(metadata)), (4, f.userId, None))
    eventStoreState.squashedVersion(f.aggregateId) mustBe Some(AggregateVersion(3))
    f.getEvents(Some(AggregateVersion(3))) mustBe Seq(EventA("squashed"))
    the[AggregateSquashedException] thrownBy f.getEvents(Some(AggregateVersion(2)))

    f.storeEvents(Seq(EventC(false))) // version 5
    Try(f.storeEvents(Seq(Undo(3)))).isFailure mustBe true // would undo the squash event
    f.storeEvents(Seq(Undo(1)))
    f.getEvents() mustBe Seq(EventA("squashed"), EventB(4))

    Try(f.storeEvents(Seq(Copy(SpaceId(1), f.aggregateId, AggregateVersion(2))), newId(), AggregateVersion.ZERO)).isFailure mustBe true
    val copyId = newId()
    f.storeEvents(Seq(Copy(SpaceId(1), f.aggregateId, AggregateVersion(3))), copyId, AggregateVersion.ZERO)
    f.getEvents(None, copyId) mustBe Seq(EventA("squashed"), Copy(SpaceId(1), f.aggregateId, AggregateVersion(3)))

    squash(f, 3).left.toOption.get must include("already squashed up to version 3")
  }

  Scenario("A squash is refused, changing nothing, while events at or below the point wait in the outbox, or after an undo that reached them") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    eventStoreState.squashBlocker(f.aggregateId, AggregateVersion(2)).get must include("outbox")
    squash(f, 2).left.toOption.get must include("outbox")

    f.storeEvents(Seq(Undo(2))) // version 4 undoes versions 3 and 2
    published(f.aggregateId)
    squash(f, 3).left.toOption.get must include("undo")
    f.getEvents() mustBe Seq(EventA("one"))
    eventStoreState.squashBlocker(f.aggregateId, AggregateVersion(4)) mustBe None
  }

  // D50, F76.
  Scenario("A squash or a permanent delete detaches the duplicates first, and changes nothing when given other duplicates than there are") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2), EventC(true), EventB(4)))
    val (early, late) = (newId(), newId())
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), early, AggregateVersion.ZERO)
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(4))), late, AggregateVersion.ZERO)

    eventStoreState.duplicatesReading(base.aggregateId, Some(AggregateVersion(3))) mustBe Seq(early)
    published(base.aggregateId)
    eventStoreState.squashBlocker(base.aggregateId, AggregateVersion(3)).get must include("first event of duplicate " + early.asLong)
    published(early, late)
    squash(base, 3).left.toOption.get must include("changed meanwhile")

    squash(base, 3, Seq(early -> EventA("early copy"))) mustBe Right(())
    base.getEvents(None, early) mustBe Seq(EventA("early copy"))
    base.getEvents(None, late) mustBe Seq(EventA("squashed"), EventB(4), Copy(SpaceId(1), base.aggregateId, AggregateVersion(4)))

    eventStoreState.deleteAggregatePermanently(base.aggregateId).left.toOption.get must include("changed meanwhile")
    base.getEvents() mustBe Seq(EventA("squashed"), EventB(4))
    eventStoreState.deleteAggregatePermanently(base.aggregateId, Seq(late -> EventA("late copy"))) mustBe Right(())
    base.getEvents() mustBe empty
    base.getEvents(None, late) mustBe Seq(EventA("late copy"))
    base.getEvents(None, early) mustBe Seq(EventA("early copy"))
  }

  // F50, D75: one key per aggregate type, the one 0.x stored (Scala's full name), also for a nested root class, so the
  // outbox names it as its command bus and its projections do.
  Scenario("The outbox names a nested aggregate root class by the key its command bus and projections use") {
    val id = newId()
    eventStoreState.localTx(session => eventStoreState.persistEvents(id,
      NewEvents(UserId(1), AggregateVersion.ZERO, Instant.now, Seq(Nesting.NestedCreated())))(session)).get

    eventStoreState.readWaitingInOutbox().filter(_.aggregateId == id).map(_.aggregateType) mustBe Seq(AggregateType.of[Nesting.NestedRoot])
    AggregateType.of[Nesting.NestedRoot].typeName mustBe "io.reactivecqrs.core.eventstore.Nesting.NestedRoot"
  }

  // D52: what the squash scheduler reads.
  Scenario("The squash candidates are the aggregates of the type with enough events older than the newest kept ones, in id order") {
    val Seq(a, b, c) = Seq(6, 2, 8).map { events =>
      val f = new TestFixture(eventStoreState)
      f.storeEvents(EventA("first") +: (2 to events).map(EventB))
      f
    }: @unchecked
    published(c.aggregateId)
    squash(c, 4) mustBe Right(())
    val rule = AutomaticSquash(afterEvents = 3, keepLast = 1)
    val someAggregate = AggregateType.of[SomeAggregate]
    val before = AggregateId(a.aggregateId.asLong - 1) // aggregates of other scenarios have lower ids

    eventStoreState.squashCandidates(someAggregate, rule, before, 10) mustBe Seq(a.aggregateId -> AggregateVersion(5), c.aggregateId -> AggregateVersion(7))
    eventStoreState.squashCandidates(someAggregate, rule, before, 1) mustBe Seq(a.aggregateId -> AggregateVersion(5))
    eventStoreState.squashCandidates(someAggregate, rule, a.aggregateId, 10) mustBe Seq(c.aggregateId -> AggregateVersion(7))
    eventStoreState.squashCandidates(AggregateType("other.Type"), rule, before, 10) mustBe empty
  }

  // A detach replaces the duplicate's first event; if an undo marked it, the undo would then take the base's state too.
  Scenario("A squash or a permanent delete is refused while a duplicate it would detach has its first event undone") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    val copy = newId()
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), copy, AggregateVersion.ZERO)
    base.storeEvents(Seq(EventB(20)), copy, AggregateVersion(1))
    base.storeEvents(Seq(Undo(2)), copy, AggregateVersion(2)) // undoes EventB(20) and the copy's first event
    published(base.aggregateId, copy)

    eventStoreState.squashBlocker(base.aggregateId, AggregateVersion(3)).get must include("first event of duplicate " + copy.asLong + ", which the squash detaches, was undone")
    squash(base, 3, Seq(copy -> EventA("copy"))).left.toOption.get must include("was undone")
    eventStoreState.deleteAggregatePermanently(base.aggregateId, Seq(copy -> EventA("copy"))).left.toOption.get must include("was undone")
    base.getEvents() mustBe Seq(EventA("one"), EventB(2), EventC(true))
  }

  // R11: what a squash keeps of the history it replaces.
  Scenario("A squash moves the events of the kept classes to the archive, with their undo marks; a permanent delete takes the archive too") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    f.storeEvents(Seq(Undo(1))) // undoes EventC(true) at version 3
    f.storeEvents(Seq(EventB(5)))
    published(f.aggregateId)

    squash(f, 4, archive = Set(classOf[EventB].getName, classOf[EventC].getName)) mustBe Right(())

    eventStoreState.readArchive[SomeAggregate](f.aggregateId).map(a => (a.info.version.asInt, a.info.event, a.undoneFrom.map(_.asInt))) mustBe
      Seq((2, EventB(2), None), (3, EventC(true), Some(4)))
    f.getEvents() mustBe Seq(EventA("squashed"), EventB(5))

    eventStoreState.deleteAggregatePermanently(f.aggregateId) mustBe Right(())
    eventStoreState.readArchive[SomeAggregate](f.aggregateId) mustBe empty
  }

  Scenario("Automatic squash keeps the events younger than its age too, and squashes only once enough older ones wait") {
    val f = new TestFixture(eventStoreState)
    // Later than every event stored before, as the times of a real history grow with the ids.
    val now = Instant.now.plusMillis(4.hours.toMillis)
    Seq(3.hours, 2.hours, 30.minutes, 1.minute).zipWithIndex.foreach { case (ago, i) =>
      eventStoreState.localTx { session =>
        eventStoreState.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion(i), now.minusMillis(ago.toMillis),
          Seq(if (i == 0) EventA("one") else EventB(i))))(session)
      }.get
    }
    val before = AggregateId(f.aggregateId.asLong - 1)
    val someAggregate = AggregateType.of[SomeAggregate]

    eventStoreState.squashCandidates(someAggregate, AutomaticSquash(2, keepNewerThan = Some(1.hour)), before, 1, now) mustBe Seq(f.aggregateId -> AggregateVersion(2))
    eventStoreState.squashCandidates(someAggregate, AutomaticSquash(3, keepNewerThan = Some(1.hour)), before, 1, now).filter(_._1 == f.aggregateId) mustBe empty
    eventStoreState.squashCandidates(someAggregate, AutomaticSquash(1, keepLast = 3, keepNewerThan = Some(1.hour)), before, 1, now) mustBe Seq(f.aggregateId -> AggregateVersion(1))
    eventStoreState.squashCandidates(someAggregate, AutomaticSquash(1, keepNewerThan = Some(5.hours)), before, 1, now).filter(_._1 == f.aggregateId) mustBe empty
  }

  // F143: as for a squash, subscribers must have a duplicate's original first event before a detach replaces it.
  Scenario("A permanent delete waits while a duplicate it would detach still has its first event in the outbox") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2)))
    val copy = newId()
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), copy, AggregateVersion.ZERO)
    published(base.aggregateId)

    eventStoreState.deleteAggregatePermanently(base.aggregateId, Seq(copy -> EventA("copy"))).left.toOption.get must
      include("first event of duplicate " + copy.asLong + ", which the delete detaches, still waits in the outbox")
    base.getEvents() mustBe Seq(EventA("one"), EventB(2))

    published(copy)
    eventStoreState.deleteAggregatePermanently(base.aggregateId, Seq(copy -> EventA("copy"))) mustBe Right(())
    base.getEvents(id = copy) mustBe Seq(EventA("copy"))
  }

  // R17: a purge detaches the copies of other organizations, and only those, each once subscribers have its first event.
  Scenario("Detaching some duplicates leaves the others reading the aggregate, and waits only for the ones it detaches") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2)))
    val (detached, kept) = (newId(), newId())
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), detached, AggregateVersion.ZERO)
    base.storeEvents(Seq(Copy(SpaceId(2), base.aggregateId, AggregateVersion(1))), kept, AggregateVersion.ZERO)
    published(base.aggregateId)

    eventStoreState.detachDuplicates(base.aggregateId, Seq(detached -> EventA("copy"))).left.toOption.get must
      include("first event of duplicate " + detached.asLong + ", which the purge detaches, still waits in the outbox")
    eventStoreState.detachDuplicates(base.aggregateId, Nil) mustBe Right(()) // none named, none checked
    published(detached) // the other duplicate's first event still waits: not this detach's concern
    eventStoreState.detachDuplicates(base.aggregateId, Seq(detached -> EventA("copy"))) mustBe Right(())

    base.getEvents(id = detached) mustBe Seq(EventA("copy"))
    eventStoreState.duplicatesReading(base.aggregateId, None) mustBe Seq(kept)
    eventStoreState.detachDuplicates(base.aggregateId, Seq(detached -> EventA("copy"))) mustBe Left("the duplicates of the aggregate changed meanwhile")
    eventStoreState.detachDuplicates(base.aggregateId, Seq(kept -> EventA("copy"))).left.toOption.get must include("still waits in the outbox")
  }

  // F147: a copy reads its base up to the version it was made at, so the base must have reached it.
  Scenario("F147 a duplication at a version its base has not reached is refused") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2)))
    val (early, copy) = (newId(), newId())

    Try(base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(3))), early, AggregateVersion.ZERO)).isFailure mustBe true
    base.getEvents(id = early) mustBe empty

    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), copy, AggregateVersion.ZERO)
    base.getEvents(id = copy).take(2) mustBe Seq(EventA("one"), EventB(2))
  }

  // As the procedures store a command's events one after another, each undo is checked against the events before it.
  Scenario("The undos of one command are checked one after another against the squash point") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    published(f.aggregateId)
    squash(f, 3) mustBe Right(())

    f.storeEvents(Seq(EventB(4), Undo(1))) // the undo reaches the event stored just before it
    f.getEvents() mustBe Seq(EventA("squashed"))
    Try(f.storeEvents(Seq(EventB(6), Undo(1), Undo(1)))).isFailure mustBe true // the second undo would reach the squash event
    f.getEvents() mustBe Seq(EventA("squashed"))
    eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(10)).map(_.version.asInt) mustBe Seq(3, 4, 5)
  }


  // ************** Complete rewrite (D76)

  private def rewrite(id: AggregateId, alter: (Event[SomeAggregate], Option[EventMetadata]) => (Event[SomeAggregate], Option[EventMetadata])) =
    eventStoreState.localTx(session => eventStoreState.rewriteEverywhere[SomeAggregate](id, UserId(7), EraseNumber(2), alter)(session))

  Scenario("D76 a complete rewrite writes again the stored and archived events a change alters, with their envelopes, and keeps the version") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId,
      NewEvents(f.userId, AggregateVersion(3), Instant.now, Seq(EventB(2)), Some(EventMetadata("alice"))))(session)).get
    published(f.aggregateId)
    squash(f, 3, archive = Set(classOf[EventB].getName)) mustBe Right(()) // EventB(2) at version 2 goes to the archive
    val generation = eventStoreState.historyGeneration(f.aggregateId)
    val erase: (Event[SomeAggregate], Option[EventMetadata]) => (Event[SomeAggregate], Option[EventMetadata]) = {
      case (EventB(2), metadata) => (EventB(0), metadata.map(_.copy(actor = "erased")))
      case other => other
    }

    rewrite(f.aggregateId, erase) mustBe Right(RewriteCount(1, 1, Set(classOf[EventB].getName), Set("number", "metadata.actor")))

    eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(4)).map(e => (e.version.asInt, e.event, e.storedMetadata)) mustBe
      Seq((3, EventA("squashed"), None), (4, EventB(0), Some(EventMetadata("erased"))))
    eventStoreState.readArchive[SomeAggregate](f.aggregateId).map(a => (a.info.version.asInt, a.info.event)) mustBe Seq(2 -> EventB(0))
    eventStoreState.storedVersion(f.aggregateId).map(_._1) mustBe Some(AggregateVersion(4))
    eventStoreState.historyGeneration(f.aggregateId) must be > generation // the snapshots held the value

    // Applied again, the change finds nothing to change.
    val after = eventStoreState.historyGeneration(f.aggregateId)
    rewrite(f.aggregateId, erase) mustBe Right(RewriteCount(0, 0, Set.empty, Set.empty))
    eventStoreState.historyGeneration(f.aggregateId) mustBe after
  }

  Scenario("D76 a complete rewrite that alters an event's class, or an undo, is refused and writes nothing") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), Undo(1)))
    def stored = eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(3)).map(_.event)

    rewrite(f.aggregateId, { case (EventB(2), m) => (EventC(true), m); case other => other }).left.toOption.get must include("D17")
    rewrite(f.aggregateId, { case (EventB(2), m) => (EventB(0), m); case (Undo(1), m) => (Undo(2), m); case other => other })
      .left.toOption.get must include("an undo or a duplication")
    stored mustBe Seq(EventA("one"), EventB(2), Undo(1))
  }

  Scenario("D76 an aggregate's copies are found, those reading it and those detached from it, and so are the bases a copy reads") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    val (attached, detached, copyOfDetached) = (newId(), newId(), newId())
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(3))), attached, AggregateVersion.ZERO)
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), detached, AggregateVersion.ZERO)
    published(base.aggregateId, attached, detached)
    eventStoreState.detachDuplicates(base.aggregateId, Seq(detached -> EventA("copy"))) mustBe Right(())
    base.storeEvents(Seq(Copy(SpaceId(1), detached, AggregateVersion(1))), copyOfDetached, AggregateVersion.ZERO)

    eventStoreState.copiesOf(base.aggregateId) mustBe Seq(attached, detached).sortBy(_.asLong)
    eventStoreState.copiesOf(detached) mustBe Seq(copyOfDetached)
    eventStoreState.basesOf(attached) mustBe Seq(base.aggregateId)
    eventStoreState.basesOf(copyOfDetached) mustBe Seq(detached)
    eventStoreState.basesOf(detached) mustBe empty

    // A permanent delete takes what was copied from it along: nothing is left to follow from it.
    published(copyOfDetached)
    eventStoreState.deleteAggregatePermanently(base.aggregateId, Seq(attached -> EventA("kept"))) mustBe Right(())
    eventStoreState.copiesOf(base.aggregateId) mustBe empty
    eventStoreState.copiesOf(detached) mustBe Seq(copyOfDetached)
  }

  // F193: a squash detaches a copy from its bases (D50), and records what it read of them like any detach (D76), so an
  // erasure of a base still finds the values the squash event holds.
  Scenario("F193 a copy squashed on its own is still found among its base's copies") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2)))
    val copy = new TestFixture(eventStoreState)
    copy.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))))
    copy.storeEvents(Seq(EventB(3)))
    published(base.aggregateId, copy.aggregateId)

    squash(copy, 2) mustBe Right(())

    eventStoreState.basesOf(copy.aggregateId) mustBe empty
    eventStoreState.copiesOf(base.aggregateId) mustBe Seq(copy.aggregateId)
  }

}
