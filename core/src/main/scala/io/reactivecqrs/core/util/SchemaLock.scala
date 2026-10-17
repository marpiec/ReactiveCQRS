package io.reactivecqrs.core.util

import java.sql.Connection

import scalikejdbc.ConnectionPool

import scala.concurrent.duration._

/**
  * D69, F63: changes to the schema run one process at a time across every node and every test run that uses the
  * database: under a PostgreSQL advisory lock, held on a connection of its own while `work` runs. Reentrant on a
  * thread, as one initialization may start another.
  */
object SchemaLock {

  /** The lock's number; advisory locks are per database, so it is this library's own across schemas. */
  val Key: Long = 0x52435152L // "RCQR"

  /** How long a node waits for another one's initialization before it gives up. */
  val MaxWait: FiniteDuration = 5.minutes

  private val depth = ThreadLocal.withInitial[Int](() => 0)

  def apply[T](work: => T): T = if (depth.get() > 0) nested(work) else {
    val connection = ConnectionPool.borrow()
    try {
      connection.setAutoCommit(true)
      acquire(connection)
      try nested(work) finally call(connection, "SELECT pg_advisory_unlock(?)")
    } finally connection.close()
  }

  private def nested[T](work: => T): T = {
    depth.set(depth.get() + 1)
    try work finally depth.set(depth.get() - 1)
  }

  // A waiting try, not a blocking lock: a session setting such as lock_timeout would stay on the pooled connection.
  private def acquire(connection: Connection): Unit = {
    val deadline = System.nanoTime() + MaxWait.toNanos
    while (!call(connection, "SELECT pg_try_advisory_lock(?)")) {
      if (System.nanoTime() > deadline) throw new IllegalStateException("The schema lock was not free for " + MaxWait + ": another process initializes the schema")
      Thread.sleep(50)
    }
  }

  private def call(connection: Connection, query: String): Boolean = {
    val statement = connection.prepareStatement(query)
    try {
      statement.setLong(1, Key)
      val result = statement.executeQuery()
      try result.next() && result.getBoolean(1) finally result.close()
    } finally statement.close()
  }
}
