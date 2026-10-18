package io.reactivecqrs.core.uid

import io.reactivecqrs.core.util.SchemaLock
import org.postgresql.util.PSQLException
import scalikejdbc._
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/** The ids `from` to `from + size - 1`. */
case class IdsPool(from: Long, size: Long)

/** A source of pools of ids. */
abstract class UidGenerator {
  /** The next pool; no id is given twice. */
  def nextIdsPool: IdsPool
}

/**
  * Pools of ids from a sequence that steps by the pool size. `range`, for aggregate ids (D39): the sequence is moved to
  * the range's start when it is behind (never back), a start with the sequence already past the range's end is refused
  * (a wrong installation number, or two on one database), and so is a pool past that end. Without one, pools stop at
  * 2^53.
  */
class PostgresUidGenerator(sequenceName: String, range: Option[IdRange] = None) extends UidGenerator {

  new UidGeneratorSchemaInitializer().initSchema()

  /** The size of every pool: the sequence's step. */
  val poolSize: Long = readSequenceStep

  range.foreach(moveInto)

  private val until = range.map(_.until).getOrElse(IdRange.Limit)

  /** The next pool; throws when it would pass the range's end. */
  def nextIdsPool: IdsPool = {
    val poolFrom = DB.autoCommit { implicit session =>
      SQL(s"SELECT NEXTVAL('$sequenceName')").map(rs => rs.long(1)).single().getOrElse {
        throw new IllegalStateException("Query returned no values, that should not happen.")
      }
    }
    if (poolFrom + poolSize > until) throw new IllegalStateException(s"No ids left in $sequenceName: the pool from $poolFrom would pass $until" +
      range.map(r => s", the end of installation ${r.installation}'s range").getOrElse(", 2^53"))
    IdsPool(poolFrom, poolSize)
  }

  // Under the schema lock, as the nodes of an installation start together.
  private def moveInto(r: IdRange): Unit = SchemaLock {
    DB.autoCommit { implicit session =>
      val (last, called) = SQL(s"SELECT last_value, is_called FROM $sequenceName").map(rs => (rs.long(1), rs.boolean(2))).single().get
      val next = if (called) last + poolSize else last
      if (next + poolSize > r.until) throw new IllegalStateException(s"The ids of $sequenceName are past installation ${r.installation}'s range " +
        s"(the next pool starts at $next, the range ends before ${r.until}): is the installation number right?")
      if (next < r.from) SQL(s"SELECT setval('$sequenceName', ${r.from}, false)").map(_.long(1)).single()
    }
  }

  private def readSequenceStep: Long = {
    try {
      DB.readOnly { implicit session =>
        SQL(s"SELECT increment_by FROM pg_sequences WHERE sequencename = '$sequenceName' AND schemaname = current_schema()").map(rs => rs.long(1)).single().get
      }
    } catch {
      case e: PSQLException =>
        // fallback for 9.6 and older
        DB.readOnly { implicit session =>
          SQL(s"SELECT increment_by FROM $sequenceName").map(rs => rs.long(1)).single().get
        }
    }
  }

}

/** Pools in memory, for tests; like the sequences, from 1001: the ids below are system ids, and 0 is no id (D39). */
class MemoryUidGenerator extends UidGenerator {

  /** The size of every pool. */
  val poolSize = 100
  /** The first id of the next pool. */
  var lastValue = 1001L

  /** The next pool. */
  def nextIdsPool: IdsPool = synchronized {
    val pool = IdsPool(lastValue, poolSize)
    lastValue += poolSize
    pool
  }

}
