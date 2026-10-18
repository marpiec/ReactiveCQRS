package io.reactivecqrs.core.util

import io.reactivecqrs.core.util.SqlLogging.withoutValues
import org.postgresql.util.PSQLException
import scalikejdbc._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * F185: the schema steps every start runs. A statement that changes a table locks it before it looks whether there is
  * anything to change: `ALTER TABLE … IF NOT EXISTS` takes ACCESS EXCLUSIVE, `CREATE INDEX IF NOT EXISTS` SHARE. Behind a
  * long transaction on the table (a backup, an organization's export) that lock waits, and every other statement on the
  * table queues behind it, on every node. So each step first reads the catalog, which locks no table, and changes the
  * table only when something is missing; and then its lock waits at most `LockTimeout`, `Attempts` times, before the
  * start fails with what to look at. Statements are the library's own: tables and indexes named in code, or document
  * stores' checked names.
  */
object Ddl {

  /** How long a step waits for its table's lock, each time it tries. */
  val LockTimeout: FiniteDuration = 2.seconds
  /** How many times a step tries, a second apart. */
  val Attempts = 30

  /** Whether the table, in the current schema, has the column. */
  def hasColumn(table: String, column: String)(implicit session: DBSession): Boolean =
    sql"""SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema()
         | AND table_name = ${table.toLowerCase} AND column_name = ${column.toLowerCase}""".stripMargin.map(_ => ()).single().isDefined

  /** Whether a table, an index or a sequence of the name is found on the search path. */
  def has(name: String)(implicit session: DBSession): Boolean = sql"SELECT to_regclass($name) IS NOT NULL".map(_.boolean(1)).single().contains(true)

  /** Adds the column, of `definition`, unless the table has it. */
  def addColumn(table: String, column: String, definition: String): Unit =
    change("adding the column " + column + " to " + table, hasColumn(table, column)(_))(run(s"ALTER TABLE $table ADD COLUMN IF NOT EXISTS $column $definition"))

  /** Drops the column, if the table has it. */
  def dropColumn(table: String, column: String): Unit =
    change("dropping the column " + column + " of " + table, !hasColumn(table, column)(_))(run(s"ALTER TABLE $table DROP COLUMN IF EXISTS $column"))

  /** Runs `statement`, which creates the index `name`, unless the index is there. */
  def createIndex(name: String, statement: String): Unit = change("creating the index " + name, has(name)(_))(run(statement))

  /** Drops the index, if it is there. */
  def dropIndex(name: String): Unit = change("dropping the index " + name, !has(name)(_))(run(s"DROP INDEX IF EXISTS $name"))

  /** Makes the change, unless `done` says it is in place: in a transaction whose locks wait at most `lockTimeout`,
    * tried `attempts` times. */
  def change(what: String, done: DBSession => Boolean, lockTimeout: FiniteDuration = LockTimeout, attempts: Int = Attempts)
            (make: DBSession => Unit): Unit =
    if (!DB.readOnly(done)) attempt(what, done, make, lockTimeout, attempts, 1)

  private def run(statement: String)(session: DBSession): Unit = SQL(statement).execute.apply()(session)

  @tailrec
  private def attempt(what: String, done: DBSession => Boolean, make: DBSession => Unit, lockTimeout: FiniteDuration, attempts: Int,
                      number: Int): Unit = Try(DB.localTx { implicit session =>
    if (!done(session)) {
      SQL("SET LOCAL lock_timeout = '" + lockTimeout.toMillis + "ms'").execute.apply()
      make(session)
    }
  }) match {
    case Success(_) => ()
    case Failure(e: PSQLException) if e.getSQLState == LockNotAvailable && number < attempts =>
      Thread.sleep(1000)
      attempt(what, done, make, lockTimeout, attempts, number + 1)
    case Failure(e: PSQLException) if e.getSQLState == LockNotAvailable =>
      throw new IllegalStateException("The start could not finish " + what + ": it waited " + attempts + " times " + lockTimeout +
        " for the table's lock, which a long transaction holds (pg_stat_activity shows it); start again once it has ended", e)
    case Failure(e) => throw e
  }

  private val LockNotAvailable = "55P03"
}
