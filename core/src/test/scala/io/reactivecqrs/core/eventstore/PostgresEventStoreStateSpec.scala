package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateVersion, Event}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import scalikejdbc._

import scala.util.Try

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
}
