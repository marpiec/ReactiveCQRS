package io.reactivecqrs.core.util

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.ExecutionContext

/** F27: the dispatcher of the library's synchronous JDBC work, defined in its reference.conf. Host actors that do such
  * work, projections first, should run on it too: `Props(...).withDispatcher(BlockingDispatcher.id)`. */
object BlockingDispatcher {
  /** The dispatcher's id in the configuration. */
  val id = "reactivecqrs.blocking-dispatcher"

  /** The dispatcher, as an `ExecutionContext` for futures that do JDBC work. */
  def apply(system: ActorSystem): ExecutionContext = system.dispatchers.lookup(id)
}
