package io.reactivecqrs.core.eventstore

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadFactory, TimeUnit}

import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.control.NonFatal

private object OutboxDeletes {
  private val log = LoggerFactory.getLogger(classOf[OutboxDeletes])

  // One thread for every store in the JVM, so stores created again (tests, restarts) leave no threads behind (R4).
  private val executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(task: Runnable): Thread = {
      val thread = new Thread(task, "reactivecqrs-outbox-deletes")
      thread.setDaemon(true)
      thread
    }
  })
}

/**
  * Collects the outbox rows of delivered events from every aggregate owner and deletes them in batches on one
  * background thread, so under load many acknowledgements cost one statement (F26). A row waits at most `maxDelay`.
  * A batch that fails is logged and dropped, as are rows still waiting when the JVM stops: they stay in the outbox,
  * and the outbox poller re-publishes them later, which subscribers drop as duplicates.
  */
private[eventstore] class OutboxDeletes(maxDelay: FiniteDuration, maxRows: Int)(delete: Seq[(Long, Int)] => Unit) {

  import OutboxDeletes._

  private val waiting = new ConcurrentLinkedQueue[(Long, Int)]()
  private val flushScheduled = new AtomicBoolean(false)
  private val flushTask = new Runnable {
    override def run(): Unit = flush()
  }

  def add(aggregateId: Long, versions: Seq[Int]): Unit = {
    versions.foreach(version => waiting.add((aggregateId, version)))
    scheduleFlush(maxDelay)
  }

  private def scheduleFlush(after: FiniteDuration): Unit =
    if (flushScheduled.compareAndSet(false, true)) executor.schedule(flushTask, after.toMillis, TimeUnit.MILLISECONDS)

  /** Deletes the rows that wait, on the caller's thread, after a flush already under way: a check of the outbox right
    * after an acknowledgement then sees what the acknowledgement deleted (F143). */
  def flushNow(): Unit = flush()

  private def flush(): Unit = synchronized {
    flushScheduled.set(false) // rows added from here on schedule the next flush
    var batch = Vector[(Long, Int)]()
    var row = waiting.poll()
    while (row != null) {
      batch :+= row
      row = if (batch.size < maxRows) waiting.poll() else null
    }
    if (!waiting.isEmpty) scheduleFlush(Duration.Zero)
    if (batch.nonEmpty) {
      try delete(batch) catch {
        case NonFatal(e) => log.warn("Deleting " + batch.size + " delivered rows from the outbox failed; the outbox poller re-publishes them later", e)
      }
    }
  }
}
