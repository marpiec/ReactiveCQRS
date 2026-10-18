package io.reactivecqrs.core.eventstore

import java.time.Instant
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateSquashedException, AggregateType, AggregateVersion, CustomSuccessResponse, Event, EventMetadata, EventTooLargeException, EventWithVersion, FirstEvent, TooManyEventsException}
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.TestDatabase
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

/** A squash or first event that a store limits to a few characters (F154). */
case class Summary(text: String) extends FirstEvent[SomeAggregate] {
  def spaceId: SpaceId = SpaceId(0)
}

/** The end of an aggregate (F244). */
case class Gone() extends io.reactivecqrs.api.PermanentDeleteEvent[SomeAggregate]

/** Erases a number, also from an event that cannot be read (F245). */
case class EraseNumberAnywhere(number: Int) extends io.reactivecqrs.api.HistoryChange {
  override def inUnreadableEvent(json: String): Option[String] = Some(json.replace(":" + number, ":0"))
}

/** The first event of an aggregate of a given organization. */
case class EventInSpace(space: Long) extends FirstEvent[SomeAggregate] {
  def spaceId: SpaceId = SpaceId(space)
}

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
      eventStoreState.readAndProcessEvents[SomeAggregate](f.aggregateId, Some(Right(Instant.parse(time))))((_, _, _, _, version, noop) => read :+= ((version, noop)))
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

  // F244: the limit guards growth; erasure, squash and a permanent delete must still reach an aggregate at it.
  Scenario("F244 an aggregate at the version limit can be rewritten, squashed and deleted, but takes no new event") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventB(3), EventB(4), EventB(5)))
    published(f.aggregateId)
    val limited = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState(), aggregateVersionLimit = 3)
    val (id, user) = (f.aggregateId, f.userId)
    val erase: (Event[SomeAggregate], Option[EventMetadata]) => (Event[SomeAggregate], Option[EventMetadata]) =
      (e, m) => (if (e == EventB(50)) EventB(0) else e, m)
    val newEvent = Try(limited.localTx(session => limited.persistEvents(id, NewEvents(user, AggregateVersion(5), Instant.now, Seq(EventB(6))))(session).get))
    newEvent.failed.get mustBe a[TooManyEventsException]

    Try(limited.localTx(session => limited.overwriteEvents(id, Seq(EventWithVersion(AggregateVersion(5), EventB(50))), None)(session))) mustBe Success(())
    limited.localTx(session => limited.rewriteEverywhere[SomeAggregate](id, user, EraseNumber(50), erase)(session)).map(_.events) mustBe Right(1)
    limited.squash(id, AggregateVersion(5), EventA("squashed"), user, None, Nil) mustBe Right(())
    Try(limited.localTx(session => limited.persistEvents(id, NewEvents(user, AggregateVersion(5), Instant.now, Seq(Gone())))(session).get)).isSuccess mustBe true
  }

  // F237: a snapshot write that read the generation before a change in progress must not land after it.
  Scenario("F237 a snapshot written while a history change is under way waits for it, and is not written") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))
    val generation = eventStoreState.historyGeneration(f.aggregateId)
    val (changing, release) = heldOpen(session => eventStoreState.overwriteEvents(f.aggregateId, Seq(EventWithVersion(AggregateVersion(2), EventB(20))), None)(session))
    val writing = Future(eventStoreState.writeSnapshot(f.aggregateId, Snapshot(AggregateVersion(2), 1, 1, SomeAggregate()), classOf[SomeAggregate].getName, generation))
    Thread.sleep(300) // the write either waits for the change's lock or, before the fix, is done
    release.countDown()
    Await.result(changing, 10.seconds)
    Await.result(writing, 10.seconds)

    eventStoreState.readSnapshot[SomeAggregate](f.aggregateId, classOf[SomeAggregate].getName) mustBe None
  }

  // F195: a command's events, or its rewrite, are refused when the history changed since its state was read.
  Scenario("F195 events and rewrites for a history generation that changed are refused, as for a changed version") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))
    val (id, user) = (f.aggregateId, f.userId)
    val before = eventStoreState.historyGeneration(id)
    eventStoreState.localTx(session => eventStoreState.overwriteEvents(id, Seq(EventWithVersion(AggregateVersion(2), EventB(20))), None)(session))
    val now = eventStoreState.historyGeneration(id)
    now mustBe before + 1

    def events(generation: Int): Try[Seq[(Event[SomeAggregate], AggregateVersion)]] = Try(eventStoreState.localTx(session =>
      eventStoreState.persistEvents(id, NewEvents(user, AggregateVersion(2), Instant.now, Seq[Event[SomeAggregate]](EventB(3)), None, Some(generation)))(session).get))
    val rewritten = Try(eventStoreState.localTx(session =>
      eventStoreState.overwriteEvents(id, Seq(EventWithVersion(AggregateVersion(2), EventB(21))), Some((AggregateVersion(2), before)))(session)))

    events(before).failed.get.getMessage must include("its history changed")
    rewritten.failed.get mustBe a[HistoryChangedException]
    events(now).isSuccess mustBe true
  }

  // F245: erasure reaches an event that cannot be read through the change's text form; without one it is refused.
  Scenario("F245 a rewrite changes an event that cannot be read as text, or is refused when the change has no text form") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(77), EventC(true)))
    def setEvent(json: String): Unit = DB.autoCommit { implicit session =>
      sql"UPDATE events SET event = $json WHERE aggregate_id = ${f.aggregateId.asLong} AND version = 2".update()
    }
    val (id, user) = (f.aggregateId, f.userId)
    val unchanged: (Event[SomeAggregate], Option[EventMetadata]) => (Event[SomeAggregate], Option[EventMetadata]) = (e, m) => (e, m)
    def rewrite(change: io.reactivecqrs.api.HistoryChange) =
      eventStoreState.localTx(session => eventStoreState.rewriteEverywhere[SomeAggregate](id, user, change, unchanged)(session))
    setEvent("{\"number\":77") // cut short
    try {
      rewrite(EraseNumber(77)).left.toOption.get must include("event 2 of aggregate " + f.aggregateId.asLong + " cannot be read")
      rewrite(EraseNumberAnywhere(77)).map(c => (c.events, c.unreadable)) mustBe Right((1, 1))
      DB.readOnly(implicit session => sql"SELECT event FROM events WHERE aggregate_id = ${f.aggregateId.asLong} AND version = 2"
        .map(_.string(1)).single()) mustBe Some("{\"number\":0")
    } finally setEvent("{\"number\":0}") // later specs in this schema read every stored event
  }

  // F126: the store reports each event's stored size, in bytes as PostgreSQL keeps it (non-ASCII text too, F142), and cuts
  // parts and pages by them, keeping at least one event.
  Scenario("events come with their stored sizes; parts and pages are cut by bytes, at least one event each") {
    val f = new TestFixture(eventStoreState)
    val stored = eventStoreState.localTx(session => eventStoreState.persist(f.aggregateId,
      NewEvents(f.userId, AggregateVersion(0), Instant.now, Seq(EventA("x" * 50 + "ł" * 50), EventB(1), EventC(true))))(session)).get
    stored.map(_.version.asInt) mustBe Seq(1, 2, 3)
    val sizes = stored.map(_.size)
    sizes.head must be > 100

    def part(maxBytes: Long): Seq[(Int, Int)] =
      eventStoreState.readPart[SomeAggregate](f.aggregateId, AggregateVersion(1), AggregateVersion(3), maxBytes).map { case (e, size) => (e.version.asInt, size) }
    part(Long.MaxValue) mustBe Seq(1, 2, 3).zip(sizes)
    part(sizes(0) + sizes(1)).map(_._1) mustBe Seq(1, 2)
    part(1).map(_._1) mustBe Seq(1)

    val types = Seq(AggregateType.of[SomeAggregate])
    val cut = eventStoreState.readCommittedPage(types, 0L, 10, AggregateScope.Only(Set(f.aggregateId)), sizes(0) + 1)
    (cut.events.map(e => (e.version.asInt, e.size)), cut.cut) mustBe ((Seq(1 -> sizes(0)), true))
    val whole = eventStoreState.readCommittedPage(types, 0L, 10, AggregateScope.Only(Set(f.aggregateId)), Long.MaxValue)
    (whole.events.map(_.version.asInt), whole.cut) mustBe ((Seq(1, 2, 3), false))
    eventStoreState.readCommittedPage(types, 0L, 2, AggregateScope.Only(Set(f.aggregateId)), Long.MaxValue).cut mustBe false // its limit, not its bytes
  }

  Scenario("a size is counted in UTF-8 bytes, as PostgreSQL keeps text (F142)") {
    Seq("", "plain", "zażółć gęślą jaźń", "ࠀ￿", "𝄞 and 😀").foreach { s =>
      PostgresEventStoreState.utf8Length(s) mustBe s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    }
  }

  // F122: the poller holds at most a limit of waiting aggregates, those waiting longest first.
  Scenario("the aggregates waiting in the outbox are read at most a limit at a time, those waiting longest first") {
    Seq("2026-02-01T10:00:02Z", "2026-02-01T10:00:00Z", "2026-02-01T10:00:01Z").foreach { time =>
      val f = new TestFixture(eventStoreState)
      eventStoreState.localTx(session => eventStoreState.persistEvents(f.aggregateId, NewEvents(f.userId, AggregateVersion.ZERO, Instant.parse(time),
        Seq(EventA("waiting"))))(session)).get
    }
    val all = eventStoreState.readWaitingInOutbox()
    all mustBe all.sortBy(w => (w.since, w.aggregateId.asLong))
    eventStoreState.readWaitingInOutbox(2) mustBe all.take(2)
    eventStoreState.readNewestWaitingInOutbox(2) mustBe all.reverse.take(2) // F260
  }

  Scenario("F252 a negative version, and store limits that are not positive, are refused where they are made") {
    an[IllegalArgumentException] must be thrownBy AggregateVersion(-1)
    an[IllegalArgumentException] must be thrownBy new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState(), fetchSize = 0)
  }

  Scenario("F269 a node of an older version leaves the procedures of a newer one as they are") {
    val initializer = new PostgresEventStoreSchemaInitializer()
    DB.autoCommit(implicit session => sql"INSERT INTO schema_steps (name, completed) VALUES ('procedures:999', current_timestamp)".update())
    try initializer.installProcedures() mustBe false
    finally DB.autoCommit(implicit session => sql"DELETE FROM schema_steps WHERE name = 'procedures:999'".update())
    initializer.installProcedures() mustBe true
  }

  // F38, D69: the restore index is built online, once; running it again does nothing. F198: it is unique, replacing the
  // plain one, so no aggregate gets two events of one version.
  Scenario("the index of events by aggregate and version is built without blocking writes, only once, and unique (F198)") {
    def indexes: Map[String, (Boolean, Boolean)] = DB.readOnly { implicit session =>
      sql"""SELECT c.relname, i.indisvalid, i.indisunique FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           | JOIN pg_namespace n ON n.oid = c.relnamespace
           | WHERE c.relname LIKE 'events_aggregate_version%' AND n.nspname = current_schema()""".stripMargin
        .map(rs => rs.string(1) -> (rs.boolean(2), rs.boolean(3))).list().toMap
    }
    indexes mustBe Map("events_aggregate_version_uidx" -> (true, true)) // a small store, at start

    DB.autoCommit { implicit session =>
      sql"DROP INDEX events_aggregate_version_uidx".execute()
      sql"CREATE INDEX events_aggregate_version_idx ON events (aggregate_id, version)".execute() // as 1.0.0 left it
    }
    val store = eventStoreState.asInstanceOf[PostgresEventStoreState]
    store.buildIndexesConcurrently()
    store.buildIndexesConcurrently()
    indexes mustBe Map("events_aggregate_version_uidx" -> (true, true))

    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one")))
    Try(DB.autoCommit { implicit session =>
      sql"""INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event)
           | SELECT nextval('events_seq'), user_id, aggregate_id, event_time, version, event_type_id, 0, event FROM events
           | WHERE aggregate_id = ${f.aggregateId.asLong}""".stripMargin.update()
    }).isFailure mustBe true
  }

  // F173: a page of a type's aggregates reads their own rows through an index of its own: at a million aggregates of a
  // type, the planner took aggregates (base_id) and read about 80 ms a page. A small store gets it at start, a large one
  // from buildIndexesConcurrently.
  Scenario("the aggregates' own rows are indexed by type and id, at start or online (F173)") {
    def definition(): Option[String] = DB.readOnly { implicit session =>
      sql"""SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'aggregates_own_type_idx'""".map(_.string(1)).single()
    }
    definition().getOrElse("") must include ("(type_id, id) WHERE (id = base_id)")

    DB.autoCommit(implicit session => sql"""DROP INDEX aggregates_own_type_idx""".execute())
    eventStoreState.asInstanceOf[PostgresEventStoreState].buildIndexesConcurrently()
    definition().getOrElse("") must include ("(type_id, id) WHERE (id = base_id)")
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

  // F124: the keys of the commit-order pass by type and space.
  private def keysOf(id: AggregateId): Seq[(Int, Long, Boolean)] = DB.readOnly { implicit session =>
    sql"""SELECT e.version, k.space_id, k.type_id = a.type_id FROM events e JOIN event_keys k ON k.id = e.id
         | JOIN aggregates a ON a.id = e.aggregate_id AND a.base_id = e.aggregate_id
         | WHERE e.aggregate_id = ${id.asLong} ORDER BY e.version""".stripMargin.map(rs => (rs.int(1), rs.long(2), rs.boolean(3))).list()
  }

  private def orphanKeys(): Long = DB.readOnly { implicit session =>
    sql"""SELECT count(*) FROM event_keys k WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = k.id)""".map(_.long(1)).single().get
  }

  // F167: a permanent delete takes the values of the aggregate's stored command responses with it. F261: their keys stay,
  // so a retried keyed command is answered as it was, without its info, instead of making the aggregate again.
  Scenario("F167, F261 a permanent delete clears the aggregate's stored responses' info and keeps their keys") {
    val responses = new PostgresCommandResponseState(new MPJsons, new PostgresTypesNamesState).initSchema()
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one")))
    val key = "F167-" + f.aggregateId.asLong
    DB.localTx { implicit session =>
      responses.storeResponse(key, CustomSuccessResponse(f.aggregateId, AggregateVersion(1), "personal"), Some(f.aggregateId))
    }
    published(f.aggregateId)

    eventStoreState.deleteAggregatePermanently(f.aggregateId) mustBe Right(())
    DB.readOnly { implicit session =>
      sql"SELECT count(*) FROM commands_responses WHERE aggregate_id = ${f.aggregateId.asLong} AND info IS NOT NULL".map(_.long(1)).single().get
    } mustBe 0L
    responses.responseByKey(key) mustBe Some(CustomSuccessResponse(f.aggregateId, AggregateVersion(1), null))
  }

  private def rewrite(store: PostgresEventStoreState, id: AggregateId, event: Event[SomeAggregate]): Unit =
    store.localTx(session => store.overwriteEvents(id, Seq(EventWithVersion(AggregateVersion(1), event)))(session))

  // F154: squash, detach and rewritten events meet the size limits like every event.
  Scenario("F154 a squash, detach or rewritten event above the size limit fails, and nothing changes") {
    val small = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState, eventSizeLimitPerClassName = Map(classOf[Summary].getName -> 30))
    val large = Summary("x" * 100)

    val squashed = new TestFixture(small)
    squashed.storeEvents(Seq(EventA("one"), EventB(2)))
    published(squashed.aggregateId)
    Try(small.squash(squashed.aggregateId, AggregateVersion(2), large, UserId(9), None, Nil)).failed.get mustBe an[EventTooLargeException]
    squashed.getEvents() mustBe Seq(EventA("one"), EventB(2))
    small.squash(squashed.aggregateId, AggregateVersion(2), Summary("ok"), UserId(9), None, Nil) mustBe Right(())

    val base = new TestFixture(small)
    base.storeEvents(Seq(EventA("one")))
    val copy = newId()
    base.storeEvents(Seq(Copy(SpaceId(1), base.aggregateId, AggregateVersion(1))), copy, AggregateVersion.ZERO)
    published(base.aggregateId, copy)
    Try(small.detachDuplicates(base.aggregateId, Seq(copy -> large))).failed.get mustBe an[EventTooLargeException]
    small.duplicatesReading(base.aggregateId, None) mustBe Seq(copy)

    val rewritten = new TestFixture(small)
    rewritten.storeEvents(Seq(Summary("ok")))
    Try(rewrite(small, rewritten.aggregateId, large)).failed.get mustBe an[EventTooLargeException]
    rewritten.getEvents() mustBe Seq(Summary("ok"))
  }

  // R17: one batch of a purge, in its transaction.
  Scenario("a purge batch deletes every row of its aggregates, and leaves out one an aggregate of another space reads") {
    val store = eventStoreState.asInstanceOf[PostgresEventStoreState]
    val space = SpaceId(9000000000L + System.nanoTime() % 1000000L)
    val f = new TestFixture(eventStoreState)
    val (purged, read, theirCopy) = (newId(), newId(), newId())
    f.storeEvents(Seq(EventInSpace(space.asLong), EventB(2), EventB(3)), purged, AggregateVersion.ZERO)
    f.storeEvents(Seq(Undo(1)), purged, AggregateVersion(3))
    f.storeEvents(Seq(EventInSpace(space.asLong), EventB(2)), read, AggregateVersion.ZERO)
    f.storeEvents(Seq(Copy(SpaceId(space.asLong + 1), read, AggregateVersion(2))), theirCopy, AggregateVersion.ZERO)
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO events_archive (id, aggregate_id, version, event_type_id, event_type_version, event, user_id, event_time, archived)
           | SELECT id, aggregate_id, version, event_type_id, event_type_version, event, user_id, event_time, current_timestamp FROM events
           | WHERE aggregate_id = ${purged.asLong} AND version = 2""".stripMargin.update()
      sql"""INSERT INTO snapshots (aggregate_id, version, context_version, schema_version, state, created)
           | VALUES (${purged.asLong}, 3, 1, 1, '{}', current_timestamp)""".stripMargin.update()
    }
    def rows(id: AggregateId): Seq[Long] = DB.readOnly { implicit session =>
      Seq("events", "events_archive", "events_to_publish", "snapshots").map(table =>
        SQL(s"SELECT count(*) FROM $table WHERE aggregate_id = ?").bind(id.asLong).map(_.long(1)).single().get) ++
        Seq(sql"SELECT count(*) FROM aggregates WHERE id = ${id.asLong}".map(_.long(1)).single().get,
          sql"SELECT count(*) FROM noop_events n JOIN events e ON e.id = n.id WHERE e.aggregate_id = ${id.asLong}".map(_.long(1)).single().get)
    }

    store.spaceAggregates(space, AggregateId(Long.MaxValue), 10).map(_.aggregateId) mustBe Seq(purged, read).sortBy(-_.asLong)
    store.spaceAggregates(space, read, 10).map(_.aggregateId) mustBe Seq(purged).filter(_.asLong < read.asLong)
    store.spaceTypes(space) mustBe Set(AggregateType.of[SomeAggregate])
    store.duplicatesInOtherSpaces(space) mustBe Seq((AggregateType.of[SomeAggregate], read, theirCopy))
    rows(purged) mustBe Seq(4L, 1L, 4L, 1L, 1L, 2L)

    val deleted = DB.localTx { implicit session =>
      val ids = store.lockForPurge(space, Seq(purged, read))
      ids mustBe Seq(purged)
      store.purgeAggregates(ids)
    }

    (deleted("events"), deleted("noop_events"), deleted("events_archive"), deleted("snapshots"), deleted("aggregates")) mustBe ((4L, 2L, 1L, 1L, 1L))
    rows(purged) mustBe Seq(0L, 0L, 0L, 0L, 0L, 0L)
    keysOf(purged) mustBe empty
    rows(read).take(1) mustBe Seq(2L)
    store.spaceSize(space) mustBe 1
  }

  Scenario("each event gets a key of its aggregate's type and space from every procedure, and the key goes with its event (F124)") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventB(3)))
    f.storeEvents(Seq(Undo(1)))
    val copy = newId()
    f.storeEvents(Seq(Copy(SpaceId(7), f.aggregateId, AggregateVersion(2))), copy, AggregateVersion.ZERO)

    keysOf(f.aggregateId) mustBe Seq((1, 0L, true), (2, 0L, true), (3, 0L, true), (4, 0L, true))
    keysOf(copy) mustBe Seq((1, 7L, true))

    published(f.aggregateId, copy)
    eventStoreState.deleteAggregatePermanently(copy, Nil) mustBe Right(())
    squash(f, 2) mustBe Right(())
    keysOf(f.aggregateId).map(_._1) mustBe Seq(2, 3, 4)
    orphanKeys() mustBe 0L
  }

  Scenario("the keys of events from before are backfilled; a store reads through them only once every event has one (F124)") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2), EventB(3)))
    val types = Seq(AggregateType.of[SomeAggregate])
    val platform = AggregateScope.InSpace(SpaceId(0))
    def store = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState) // reads the step afresh
    def page(on: PostgresEventStoreState): Seq[Long] = on.readCommittedPage(types, 0L, 1000000, platform, Long.MaxValue).events.map(_.id)

    Given("events written before the keys: theirs missing, the step not recorded")
    DB.autoCommit { implicit session =>
      sql"DELETE FROM event_keys WHERE id IN (SELECT id FROM events WHERE aggregate_id = ${f.aggregateId.asLong})".update()
      sql"DELETE FROM schema_steps WHERE name = 'event_keys'".update()
    }
    val walked = page(store)
    walked must contain allElementsOf DB.readOnly { implicit session =>
      sql"SELECT id FROM events WHERE aggregate_id = ${f.aggregateId.asLong}".map(_.long(1)).list()
    }

    When("the backfill runs, a few ids at a time")
    store.backfillEventKeys(batchSize = 2) mustBe EventKeysBackfill(3, 0)
    keysOf(f.aggregateId).map(_._2) mustBe Seq(0L, 0L, 0L)

    Then("a store reads through the keys, with the same pages")
    val keyed = store
    page(keyed) mustBe walked
    DB.autoCommit { implicit session => // a key that says another space shows which way a store reads
      sql"UPDATE event_keys SET space_id = 99 WHERE id = (SELECT id FROM events WHERE aggregate_id = ${f.aggregateId.asLong} AND version = 3)".update()
    }
    page(keyed).size mustBe walked.size - 1
    DB.autoCommit { implicit session =>
      sql"UPDATE event_keys SET space_id = 0 WHERE id = (SELECT id FROM events WHERE aggregate_id = ${f.aggregateId.asLong} AND version = 3)".update()
    }
  }

  Scenario("the space backfill gives the keys of an aggregate's events its space too (F124)") {
    val f = new TestFixture(eventStoreState)
    f.storeEvents(Seq(EventA("one"), EventB(2)))
    DB.autoCommit { implicit session =>
      sql"UPDATE aggregates SET space_id = -1 WHERE id = ${f.aggregateId.asLong}".update()
      sql"UPDATE event_keys SET space_id = -1 WHERE id IN (SELECT id FROM events WHERE aggregate_id = ${f.aggregateId.asLong})".update()
    }

    eventStoreState.asInstanceOf[PostgresEventStoreState].backfillSpaces()

    keysOf(f.aggregateId).map(_._2) mustBe Seq(0L, 0L)
  }
}
