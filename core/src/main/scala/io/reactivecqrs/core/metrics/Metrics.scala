package io.reactivecqrs.core.metrics

import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
  * R19, D56: what the library measures, for the host to send on to its metrics system (Micrometer, Kamon, Prometheus).
  * The library calls it on its own threads and often, so an implementation must be quick and thread-safe and must not
  * throw. Names are dot-separated; tags are few and of low cardinality (aggregate types, command and response classes,
  * projection names), never ids. What is measured is listed in docs/ARCHITECTURE.md (§14). The default measures nothing.
  */
trait Metrics {
  /** Something happened `amount` times. */
  def count(name: String, tags: Map[String, String], amount: Long): Unit
  /** Something took `nanos`. */
  def time(name: String, tags: Map[String, String], nanos: Long): Unit
  /** A level, sampled now. */
  def gauge(name: String, tags: Map[String, String], value: Long): Unit
}

object Metrics {
  /** Measures nothing: the default. */
  object NoOp extends Metrics {
    override def count(name: String, tags: Map[String, String], amount: Long): Unit = ()
    override def time(name: String, tags: Map[String, String], nanos: Long): Unit = ()
    override def gauge(name: String, tags: Map[String, String], value: Long): Unit = ()
  }
}

/** The metrics of one actor system. The host sets its own with `use`, before or after the library's actors start: each
  * measure goes to the one set when it is taken. */
class ReactiveMetrics extends Extension {
  @volatile private var current: Metrics = Metrics.NoOp
  /** From now on, measures go to `metrics`. */
  def use(metrics: Metrics): Unit = current = metrics
  /** Where measures go now. */
  def metrics: Metrics = current
}

/** `ReactiveMetrics(system)`: the metrics of the actor system. */
object ReactiveMetrics extends ExtensionId[ReactiveMetrics] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): ReactiveMetrics = new ReactiveMetrics
  override def lookup: ReactiveMetrics.type = ReactiveMetrics
  override def get(system: ActorSystem): ReactiveMetrics = super.get(system)
}
