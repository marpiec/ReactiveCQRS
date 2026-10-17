package io.reactivecqrs.testutils

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

object CommonSpec {

  /** M5, G7: with `-Dreactivecqrs.test.clusterOfOne=true` every spec runs on a cluster of one node, which must behave
    * like a single node. */
  val clusterOfOne: Boolean = sys.props.get("reactivecqrs.test.clusterOfOne").contains("true")

  private val config = ConfigFactory.parseString("""
          pekko.loglevel = "WARNING"
          pekko.actor.debug.receive = off
          pekko.actor.debug.fsm = off
          pekko.actor.debug.lifecycle = off""")

  private val clusterConfig = ConfigFactory.parseString("""
          pekko.actor.provider = cluster
          pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
          pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
          pekko.coordinated-shutdown.exit-jvm = off
          pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off""")

  def newSystem(): ActorSystem =
    if (!clusterOfOne) ActorSystem("testsystem", config) else {
      val system = ActorSystem("testsystem", clusterConfig.withFallback(config))
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

abstract class CommonSpec extends TestKit(CommonSpec.newSystem()) with AnyFeatureSpecLike with GivenWhenThen with ActorAskSupport with Matchers {

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = false,
    singleLineMode = true,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 1,
    logLevel = "debug",
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = "warn"
  )

  def step(description: String): Unit = {
    // do nothing
  }

}
