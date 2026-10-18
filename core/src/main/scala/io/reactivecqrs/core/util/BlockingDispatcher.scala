package io.reactivecqrs.core.util

import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import org.slf4j.LoggerFactory
import scalikejdbc.ConnectionPool

import scala.concurrent.ExecutionContext
import scala.util.Try

/** F27: the dispatcher of the library's synchronous JDBC work, defined in its reference.conf. Host actors that do such
  * work, projections first, should run on it too: `Props(...).withDispatcher(BlockingDispatcher.id)`. */
object BlockingDispatcher {
  /** The dispatcher's id in the configuration. */
  val id = "reactivecqrs.blocking-dispatcher"

  /** The dispatcher, as an `ExecutionContext` for futures that do JDBC work. */
  def apply(system: ActorSystem): ExecutionContext = {
    PoolCheck(system)
    system.dispatchers.lookup(id)
  }
}

/** F272: checks once per actor system, when the library first runs JDBC work, that ScalikeJDBC's default pool has as
  * many connections as the blocking dispatcher has threads; with fewer, work waits for a connection under load and
  * fails after the pool's wait. ScalikeJDBC's own default is 8. */
private[util] final class PoolCheck(system: ExtendedActorSystem) extends Extension {
  for {
    threads <- Try(system.settings.config.getInt(BlockingDispatcher.id + ".thread-pool-executor.fixed-pool-size"))
    connections <- Try(ConnectionPool.get().settings.maxSize)
  } if (connections < threads) LoggerFactory.getLogger(classOf[PoolCheck]).warn("The JDBC pool has " + connections +
    " connections and the blocking dispatcher " + threads + " threads: under load, work waits for a connection and fails " +
    "after the pool's wait. Make the pool at least as large (docs/book/18-production.md, step 1)")
}

private[util] object PoolCheck extends ExtensionId[PoolCheck] with ExtensionIdProvider {
  override def lookup: ExtensionId[PoolCheck] = PoolCheck
  override def createExtension(system: ExtendedActorSystem): PoolCheck = new PoolCheck(system)
}
