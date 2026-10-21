package io.reactivecqrs.examples.e1

import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

// Chapter 2's first program in an empty schema of the test database: the tables it makes, twice.
class SetupCheckSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private def tables(): Set[String] = DB.readOnly { implicit session =>
    sql"SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()".map(_.string(1)).list().toSet
  }

  Feature("E1's setup check") {

    Scenario("makes the tables chapter 2 lists, and changes nothing when run again") {
      val (first, second) = TestDatabase.inSchema("setup_check_" + System.nanoTime() % 1000000) {
        SetupCheck.initTables()
        val first = tables()
        SetupCheck.initTables()
        (first, tables())
      }
      first mustBe Set("aggregate_lineage", "aggregates", "commands_responses", "event_keys", "events", "events_archive",
        "events_to_publish", "history_rewrites", "noop_events", "online_jobs", "restore_failures", "sagas", "schema_steps", "snapshots", "subscription_failures",
        "subscriptions", "types_names")
      second mustBe first
    }
  }
}
