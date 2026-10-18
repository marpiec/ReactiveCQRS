package io.reactivecqrs.core.util

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.PostgresProjectionRegistry
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

case class StartupNote(text: String)

// F185: the schema steps every start runs take no lock on a table that has what they would add, so a long transaction on
// a busy table (a backup, an organization's export) does not queue every other statement on it behind a node's start.
class StartupLocksSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val types = new PostgresTypesNamesState().initSchema()

  // Each table, and the start that would touch it.
  private val starts: Seq[(String, () => Any)] = Seq(
    "events" -> (() => new PostgresEventStoreState(mpjsons, types).initSchema()),
    "aggregates" -> (() => new PostgresEventStoreState(mpjsons, types).initSchema()),
    "subscriptions" -> (() => new PostgresSubscriptionsState(types).initSchema()),
    "commands_responses" -> (() => new PostgresCommandResponseState(mpjsons, types).initSchema()),
    "sagas" -> (() => new PostgresSagaState(mpjsons, types).initSchema()),
    "projections" -> (() => new PostgresProjectionRegistry().initSchema()),
    "types_names" -> (() => new PostgresTypesNamesState().initSchema()),
    "projection_startup_notes" -> (() => new PostgresDocumentStore[StartupNote]("startup_notes", mpjsons, new NoopDocumentStoreCache,
      Seq(MultipleTextIndex(1, Seq("text"))))))

  starts.foreach(_._2()) // every table has everything already

  // Whether a writer gets the table within `timeout`: it does unless a start's lock request waits ahead of it.
  private def writableWithin(table: String, timeout: FiniteDuration): Boolean = Try(DB.localTx { implicit session =>
    SQL(s"SET LOCAL lock_timeout = '${timeout.toMillis}ms'").execute.apply()
    SQL(s"LOCK TABLE $table IN ROW EXCLUSIVE MODE").execute.apply()
  }).isSuccess

  Feature("F185 schema steps at start") {
    for ((table, start) <- starts; (holding, lock) <- Seq("reads" -> "ACCESS SHARE", "writes" -> "ROW EXCLUSIVE")) {
      Scenario(s"a start while a long transaction $holding $table leaves $table writable, and ends with the transaction") {
        val holder = ConnectionPool.borrow()
        holder.setAutoCommit(false)
        holder.createStatement().execute(s"LOCK TABLE $table IN $lock MODE")
        val started = Future(start())(ExecutionContext.global)
        try {
          Thread.sleep(500) // the start reaches the table
          writableWithin(table, 3.seconds) mustBe true
        } finally {
          holder.rollback()
          holder.close()
        }
        Await.result(started, 30.seconds)
      }
    }

    Scenario("a change that is needed waits a bounded time for its lock, keeping the table writable, then fails the start with why") {
      DB.autoCommit(implicit session => sql"CREATE TABLE IF NOT EXISTS startup_locks_busy (id BIGINT)".execute())
      val holder = ConnectionPool.borrow()
      holder.setAutoCommit(false)
      holder.createStatement().execute("LOCK TABLE startup_locks_busy IN ACCESS SHARE MODE")
      try {
        val started = Future(Ddl.change("adding a column", Ddl.hasColumn("startup_locks_busy", "added")(_), lockTimeout = 300.millis, attempts = 3) {
          implicit session => sql"ALTER TABLE startup_locks_busy ADD COLUMN added INT".execute()
        })(ExecutionContext.global)
        Thread.sleep(200)
        writableWithin("startup_locks_busy", 3.seconds) mustBe true
        val failure = Try(Await.result(started, 30.seconds)).failed.get
        failure.getMessage must (include("adding a column") and include("pg_stat_activity"))
      } finally {
        holder.rollback()
        holder.close()
      }
    }
  }
}
