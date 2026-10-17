package io.reactivecqrs.core.eventstore

import java.time.Instant
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateSquashedException, AggregateVersion, Event, TooManyEventsException}
import io.reactivecqrs.api.id.SpaceId
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

object PostgresEventStoreStateSpec {
  def storeState = {
    TestDatabase.init()

    val typesNamesState = new PostgresTypesNamesState().initSchema()

    new PostgresEventStoreState(new MPJsons, typesNamesState).initSchema()
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState) {

  // F39: a read at an instant reads the aggregate as it was then, undone events included as they were then.
  Scenario("a read at an instant gives the events up to it, an event undone later not yet undone") {
    val f = new TestFixture(eventStoreState)
    def storeAt(time: String, expected: Int, event: Event[SomeAggregate]): Unit = eventStoreState.localTx { session =>
      eventStoreState.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion(expected), Instant.parse(time), Seq(event)))(session)
    }.get
    storeAt("2026-01-01T10:00:00Z", 0, EventA("one"))
    storeAt("2026-01-01T11:00:00Z", 1, EventC(true))
    storeAt("2026-01-01T12:00:00Z", 2, Undo(1)) // undoes EventC

    def at(time: String): Seq[(Int, Boolean)] = {
      var read = Vector[(Int, Boolean)]()
      eventStoreState.readAndProcessEvents[SomeAggregate](f.aggregateId, Some(Right(Instant.parse(time))))((_, _, _, _, version, noop) => read :+= (version, noop))
      read
    }
    at("2026-01-01T11:30:00Z") mustBe Seq((1, false), (2, false))
    at("2026-01-01T12:30:00Z") mustBe Seq((1, false), (2, true), (3, true))
    at("2026-01-01T09:00:00Z") mustBe empty
  }

  // F65: the stored time is the UTC wall time whatever the JVM's zone (this machine's is not UTC around this instant).
  Scenario("event times are stored as UTC and read back unchanged, also across a daylight-saving change") {
    val f = new TestFixture(eventStoreState)
    val time = Instant.parse("2026-03-29T01:30:00Z") // the hour Central European clocks skip
    eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion.ZERO, time, Seq(EventA("one"))))(session)).get

    DB.readOnly { implicit session =>
      sql"SELECT event_time::text FROM events WHERE aggregate_id = ${f.aggregateId.asLong}".map(_.string(1)).single()
    } mustBe Some("2026-03-29 01:30:00")
    eventStoreState.readEventsRange[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(1)).map(_.timestamp) mustBe Seq(time)
  }

  // D49: an undo may not reach the squashed history.
  Scenario("an undo that would reach the squashed history is refused") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    DB.autoCommit { implicit session => sql"UPDATE aggregates SET squashed_version = 2 WHERE id = ${f.aggregateId.asLong}".update() }

    val reaching = Try(eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId,
      NewEvents(f.userId, AggregateVersion(3), Instant.now, Seq(Undo(2))))(session).get))
    reaching.failed.get.getMessage must include("Undo cannot reach the squashed history")

    f.storeEvents(Seq(Undo(1))) // undoes version 3, after the squash point
    f.getEvents() mustBe Seq(EventA("one"), EventB(2))
  }

  // F125: the limit is the highest version an aggregate reaches, however its events are stored.
  Scenario("an aggregate takes no event past aggregateVersionLimit, stored together, one by one or as an undo") {
    val limited = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState(), aggregateVersionLimit = 3)
    val f = new TestFixture(limited)
    def store(expected: Int, events: Event[SomeAggregate]*): Try[Seq[Int]] =
      Try(limited.localTx(session => limited.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion(expected), Instant.now, events))(session).get))
        .map(_.map(_._2.asInt))

    store(0, EventA("one"), EventB(2)) mustBe Success(Seq(1, 2))
    store(2, EventC(true), EventB(4)).failed.get mustBe a[TooManyEventsException]
    store(2, EventC(true)) mustBe Success(Seq(3))
    store(3, EventB(4)).failed.get mustBe a[TooManyEventsException]
    store(3, Undo(1)).failed.get mustBe a[TooManyEventsException]
  }

  // F38, D69: the restore index is built online, once; running it again does nothing.
  Scenario("the index of events by aggregate and version is built without blocking writes, and only once") {
    val store = eventStoreState.asInstanceOf[PostgresEventStoreState]
    store.buildIndexesConcurrently()
    store.buildIndexesConcurrently()
    DB.readOnly { implicit session =>
      sql"""SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           | JOIN pg_namespace n ON n.oid = c.relnamespace
           | WHERE c.relname = 'events_aggregate_version_idx' AND n.nspname = current_schema()""".stripMargin.map(_.boolean(1)).list()
    } mustBe List(true)
  }

  // D29: event versioning is gone. Rows of version 0 read as before; a row written with a type version names a class
  // that cannot be known any more, so reading it fails with a message naming the row.
  Scenario("an event stored with a non-zero event type version fails to read, with a message naming it") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))
    f.getEvents() mustBe Seq(EventA("one"), EventB(2))

    def setTypeVersion(typeVersion: Int): Unit = DB.autoCommit { implicit session =>
      sql"UPDATE events SET event_type_version = $typeVersion WHERE aggregate_id = ${f.aggregateId.asLong} AND version = 2".update()
    }
    setTypeVersion(3)
    try {
      val failure = the[UnsupportedEventVersionException] thrownBy f.getEvents()
      failure.getMessage must include("event_type_version 3")
      failure.getMessage must include(classOf[EventB].getName)
      failure.getMessage must include("aggregate " + f.aggregateId.asLong + " at version 2")
    } finally setTypeVersion(0) // later specs in this schema read every stored event
  }

  private def waitingForLock(query: String): Boolean = DB.readOnly { implicit session =>
    sql"""SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $query)"""
      .map(_.boolean(1)).single().get
  }

  /** Runs `block` in a transaction that stays open until the returned latch is counted down. */
  private def heldOpen(block: DBSession => Unit): (Future[Unit], CountDownLatch) = {
    val holding, release = new CountDownLatch(1)
    val transaction = Future(DB.localTx { session =>
      block(session)
      holding.countDown()
      release.await(10, TimeUnit.SECONDS)
    })
    holding.await(10, TimeUnit.SECONDS) mustBe true
    (transaction.map(_ => ()), release)
  }

  // D50: the duplication procedure waits while a squash of its base holds the base's chain, then sees the squash point.
  Scenario("a duplication made while a squash of its base runs waits for the squash, and is then refused below the squash point") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    published(base.aggregateId)
    val id = base.aggregateId.asLong
    // The squash stops at the event it rewrites, with the chain locked.
    val (holding, release) = heldOpen(implicit session => sql"SELECT id FROM events WHERE aggregate_id = $id AND version = 3 FOR UPDATE".map(_.long(1)).single())
    val squashing = Future(eventStoreState.squash(base.aggregateId, AggregateVersion(3), EventA("squashed"), base.userId, None, Nil))
    awaitAssert(waitingForLock("UPDATE events SET event%") mustBe true, 5.seconds, 20.millis)

    val duplication = Future(Try(base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), newId(), AggregateVersion.ZERO)))
    awaitAssert(waitingForLock("%add_duplication_event%") mustBe true, 5.seconds, 20.millis)
    release.countDown()

    Await.result(holding, 10.seconds)
    Await.result(squashing, 10.seconds) mustBe Right(())
    Await.result(duplication, 10.seconds).failed.get.getMessage must include("Duplication cannot read the squashed history")
  }

  // D51: an instant before the squash point cannot be answered, not even as "the aggregate did not exist yet".
  Scenario("a read at an instant before the squash point fails, also before the oldest event left") {
    val f = new TestFixture(eventStoreState)
    def storeAt(time: String, expected: Int, event: Event[SomeAggregate]): Unit = eventStoreState.localTx { session =>
      eventStoreState.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion(expected), Instant.parse(time), Seq(event)))(session)
    }.get
    storeAt("2026-02-01T10:00:00Z", 0, EventA("one"))
    storeAt("2026-02-01T11:00:00Z", 1, EventB(2))
    storeAt("2026-02-01T12:00:00Z", 2, EventC(true))
    published(f.aggregateId)
    squash(f, 2) mustBe Right(()) // the squash event keeps version 2's time, 11:00

    def at(time: String): Try[Seq[Int]] = Try {
      var read = Vector[Int]()
      eventStoreState.readAndProcessEvents[SomeAggregate](f.aggregateId, Some(Right(Instant.parse(time))))((_, _, _, _, version, _) => read :+= version)
      read
    }
    at("2026-02-01T11:30:00Z") mustBe Success(Seq(2))
    at("2026-02-01T10:30:00Z").failed.get mustBe an[AggregateSquashedException]
    at("2026-02-01T09:00:00Z").failed.get mustBe an[AggregateSquashedException]
  }

  // F96, review 5: every row a detach changes is locked in the one key order, so a duplication of a duplicate waits.
  Scenario("a squash that detaches a duplicate of a duplicate does not deadlock with a duplication of it in progress") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2)))
    val (middle, last) = (newId(), newId())
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(2))), middle, AggregateVersion.ZERO)
    base.storeEvents(Seq(EventB(3)), middle, AggregateVersion(1))
    base.storeEvents(Seq(Copy(SpaceId(1), middle, AggregateVersion(1))), last, AggregateVersion.ZERO) // reads middle below 2
    published(base.aggregateId, middle, last)
    val (baseId, middleId, lastId) = (base.aggregateId.asLong, middle.asLong, last.asLong)
    // A duplication of `last` in progress: it holds the first row of last's chain, and asks for the next one later.
    val holding, goOn = new CountDownLatch(1)
    val duplicating = Future(DB.localTx { implicit session =>
      sql"SELECT 1 FROM aggregates WHERE id = $lastId AND base_id = $baseId FOR KEY SHARE".map(_.int(1)).single()
      holding.countDown()
      goOn.await(10, TimeUnit.SECONDS)
      sql"SELECT 1 FROM aggregates WHERE id = $lastId AND base_id = $middleId FOR KEY SHARE".map(_.int(1)).single()
    })
    holding.await(10, TimeUnit.SECONDS) mustBe true
    val squashing = Future(eventStoreState.squash(middle, AggregateVersion(2), EventA("squashed"), base.userId, None, Seq(last -> EventA("copy"))))
    awaitAssert(waitingForLock("%") mustBe true, 5.seconds, 20.millis)

    goOn.countDown()

    Await.result(duplicating, 10.seconds) mustBe Some(1)
    Await.result(squashing, 10.seconds) mustBe Right(())
  }

  // D50: a squash waits while a duplication of its base is in progress, then sees the new duplicate.
  Scenario("a squash that waits for a duplication of its base in progress sees the new duplicate, and changes nothing") {
    val base = new TestFixture(eventStoreState)
    base.storeEvents(Seq(EventA("one"), EventB(2), EventC(true)))
    published(base.aggregateId)
    val id = base.aggregateId.asLong
    val copyId = newId().asLong
    // What add_duplication_event does first: lock the base's chain, then add the duplicate's chain row.
    val (holding, release) = heldOpen { implicit session =>
      sql"SELECT id FROM aggregates WHERE id = $id ORDER BY id, base_id FOR KEY SHARE".map(_.long(1)).list()
      sql"""INSERT INTO aggregates (space_id, id, creation_time, type_id, base_order, base_id, base_version)
           | VALUES (1, $copyId, current_timestamp, 1, 1, $id, 2)""".stripMargin.update()
    }
    val squashing = Future(eventStoreState.squash(base.aggregateId, AggregateVersion(3), EventA("squashed"), base.userId, None, Nil))
    awaitAssert(waitingForLock("%ORDER BY id, base_id FOR UPDATE") mustBe true, 5.seconds, 20.millis)
    release.countDown()

    Await.result(holding, 10.seconds)
    try {
      Await.result(squashing, 10.seconds).left.toOption.get must include("the duplicates of the aggregate changed meanwhile")
      base.getEvents() mustBe Seq(EventA("one"), EventB(2), EventC(true))
    } finally DB.autoCommit(implicit session => sql"DELETE FROM aggregates WHERE id = $copyId".update())
  }
}
