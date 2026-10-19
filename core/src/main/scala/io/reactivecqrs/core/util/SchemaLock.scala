package io.reactivecqrs.core.util

import java.sql.Connection
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scalikejdbc.ConnectionPool

import scala.concurrent.duration._

/**
  * D69, F63: changes to the schema run one process at a time across every node and every test run that uses the
  * database: under a PostgreSQL advisory lock, held on a connection of its own while `work` runs. Reentrant on a
  * thread, as one initialization may start another. F278: the threads of one process wait for it in the process, in turn,
  * holding no connection, so the pool always has one left for the work of the thread that holds the lock; only one thread
  * per process waits on the database.
  */
object SchemaLock {

  /** The lock's number; advisory locks are per database, so it is this library's own across schemas. */
  val Key: Long = 0x52435152L // "RCQR"

  /** How long a node waits for another one's initialization before it gives up. */
  val MaxWait: FiniteDuration = 5.minutes

  private val depth = ThreadLocal.withInitial[Int](() => 0)
  private val inThisProcess = new ReentrantLock(true)

  /** Runs `work` holding the lock: one process at a time changes the schema. */
  def apply[T](work: => T): T = if (depth.get() > 0) nested(work) else {
    val deadline = System.nanoTime() + MaxWait.toNanos
    if (!inThisProcess.tryLock(MaxWait.toNanos, TimeUnit.NANOSECONDS)) throw notFree()
    try {
      val connection = ConnectionPool.borrow()
      try {
        connection.setAutoCommit(true)
        acquire(connection, deadline)
        try nested(work) finally call(connection, "SELECT pg_advisory_unlock(?)")
      } finally connection.close()
    } finally inThisProcess.unlock()
  }

  private def nested[T](work: => T): T = {
    depth.set(depth.get() + 1)
    try work finally depth.set(depth.get() - 1)
  }

  // A waiting try, not a blocking lock: a session setting such as lock_timeout would stay on the pooled connection.
  private def acquire(connection: Connection, deadline: Long): Unit =
    while (!call(connection, "SELECT pg_try_advisory_lock(?)")) {
      if (System.nanoTime() > deadline) throw notFree()
      Thread.sleep(50)
    }

  private def notFree() = new IllegalStateException("The schema lock was not free for " + MaxWait + ": another process initializes the schema")

  private def call(connection: Connection, query: String): Boolean = {
    val statement = connection.prepareStatement(query)
    try {
      statement.setLong(1, Key)
      val result = statement.executeQuery()
      try result.next() && result.getBoolean(1) finally result.close()
    } finally statement.close()
  }
}
