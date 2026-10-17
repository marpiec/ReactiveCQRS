package io.reactivecqrs.testkit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}

/** Actor systems for tests. With `-Dreactivecqrs.test.clusterOfOne=true` each is a cluster of one node, which must behave
  * like a single node (M5, G7): the library runs its specs both ways, and an application can too. */
object TestSystems {

  val clusterOfOne: Boolean = sys.props.get("reactivecqrs.test.clusterOfOne").contains("true")

  private val quiet = ConfigFactory.parseString("""
          pekko.loglevel = "WARNING"
          pekko.actor.debug.receive = off
          pekko.actor.debug.fsm = off
          pekko.actor.debug.lifecycle = off""")

  /** What several nodes in one JVM need: local ports chosen by the system, JMX beans of their own, and no JVM exit or
    * shutdown hook when one stops. */
  val clusterConfig: Config = ConfigFactory.parseString("""
          pekko.actor.provider = cluster
          pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
          pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
          pekko.coordinated-shutdown.exit-jvm = off
          pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off""")

  /** A system with `config` on top of quiet logging; a cluster of one, up, when the tests run as one. */
  def newSystem(name: String = "testsystem", config: Config = ConfigFactory.empty()): ActorSystem =
    if (!clusterOfOne) ActorSystem(name, config.withFallback(quiet)) else {
      val system = ActorSystem(name, config.withFallback(clusterConfig).withFallback(quiet))
      val cluster = Cluster(system)
      cluster.join(cluster.selfAddress)
      val deadline = System.nanoTime() + 30000000000L
      while (!cluster.state.members.exists(m => m.address == cluster.selfAddress && m.status == MemberStatus.Up)) {
        if (System.nanoTime() > deadline) throw new IllegalStateException("The cluster of one did not come up")
        Thread.sleep(50)
      }
      system
    }
}
