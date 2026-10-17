package io.reactivecqrs.core.metrics

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters._

/** R20: metrics kept in memory, for tests: counts summed, times and gauges kept, by name and tags. A query names the tags
  * it cares about; measures with more tags match too. */
final class RecordingMetrics extends Metrics {

  private val counts = new ConcurrentHashMap[(String, Map[String, String]), AtomicLong]()
  private val times = new ConcurrentLinkedQueue[(String, Map[String, String], Long)]()
  private val gauges = new ConcurrentHashMap[(String, Map[String, String]), java.lang.Long]()

  override def count(name: String, tags: Map[String, String], amount: Long): Unit =
    counts.computeIfAbsent((name, tags), _ => new AtomicLong()).addAndGet(amount)

  override def time(name: String, tags: Map[String, String], nanos: Long): Unit = times.add((name, tags, nanos))

  override def gauge(name: String, tags: Map[String, String], value: Long): Unit = gauges.put((name, tags), value)

  /** The sum of the counts of `name` with these tags. */
  def counted(name: String, tags: Map[String, String] = Map.empty): Long =
    counts.asScala.collect { case ((n, t), count) if n == name && matches(t, tags) => count.get }.sum

  /** The times of `name` with these tags, in the order they were taken. */
  def timed(name: String, tags: Map[String, String] = Map.empty): Seq[Long] =
    times.asScala.collect { case (n, t, nanos) if n == name && matches(t, tags) => nanos }.toVector

  /** The last values of the gauge `name` with these tags, one per set of tags. */
  def gauged(name: String, tags: Map[String, String] = Map.empty): Seq[Long] =
    gauges.asScala.collect { case ((n, t), value) if n == name && matches(t, tags) => value.longValue }.toVector

  private def matches(measured: Map[String, String], asked: Map[String, String]): Boolean = asked.forall { case (k, v) => measured.get(k).contains(v) }
}
