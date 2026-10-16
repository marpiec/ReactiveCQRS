package io.reactivecqrs.bench

import java.lang.management.{ManagementFactory, MemoryType}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import scalikejdbc._

import scala.jdk.CollectionConverters._

/** Latencies of one measured load, in nanoseconds. */
class Latencies {
  private val samples = new ConcurrentLinkedQueue[Long]()

  def record(nanos: Long): Unit = samples.add(nanos)

  def count: Int = samples.size

  /** The value at a percentile (0–100), in milliseconds. */
  def percentileMillis(percentile: Double): Double = {
    val sorted = samples.asScala.toVector.sorted
    if (sorted.isEmpty) 0.0
    else sorted(math.min(sorted.size - 1, math.ceil(percentile / 100.0 * sorted.size).toInt - 1).max(0)) / 1e6
  }
}

/** Counts every SQL statement ScalikeJDBC runs, through its completion and failure listeners. */
object StatementCounter {
  private val statements = new AtomicLong()

  def install(): Unit = {
    GlobalSettings.queryCompletionListener = (_, _, _) => statements.incrementAndGet()
    GlobalSettings.queryFailureListener = (_, _, _) => statements.incrementAndGet()
  }

  def current: Long = statements.get
}

/** Transactions committed in the database, from `pg_stat_database` (read-only autocommit statements count too). */
object DatabaseCounters {

  def committedTransactions(): Long = DB.readOnly { implicit session =>
    scala.util.Try(sql"SELECT pg_stat_force_next_flush()".execute()) // PostgreSQL 15+
    sql"SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()".map(_.long(1)).single().get
  }

  /** Other connections report their counters at most once a second, or about 10 s after going idle; wait for that. */
  def settle(): Unit = Thread.sleep(11000)

  def serverVersion(): String = DB.readOnly { implicit session => sql"SHOW server_version".map(_.string(1)).single().get }
}

object Heap {

  def usedAfterGc(): Long = {
    System.gc()
    Thread.sleep(200)
    System.gc()
    Thread.sleep(200)
    val runtime = Runtime.getRuntime
    runtime.totalMemory - runtime.freeMemory
  }

  private def heapPools = ManagementFactory.getMemoryPoolMXBeans.asScala.filter(_.getType == MemoryType.HEAP)

  def resetPeak(): Unit = heapPools.foreach(_.resetPeakUsage())

  /** The sum of the heap pools' peaks since the last reset: an upper bound of the peak heap use. */
  def peak(): Long = heapPools.map(_.getPeakUsage.getUsed).sum

  def mb(bytes: Long): String = f"${bytes / 1024.0 / 1024.0}%.0f MB"
}
