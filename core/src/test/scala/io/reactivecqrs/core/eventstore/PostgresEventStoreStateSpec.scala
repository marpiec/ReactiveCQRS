package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import scalikejdbc._

object PostgresEventStoreStateSpec {
  def storeState = {
    TestDatabase.init()

    val typesNamesState = new PostgresTypesNamesState().initSchema()

    new PostgresEventStoreState(new MPJsons, typesNamesState).initSchema()
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState) {

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
