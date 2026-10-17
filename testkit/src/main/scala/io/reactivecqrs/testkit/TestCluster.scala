package io.reactivecqrs.testkit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * Several nodes of a cluster in one JVM, for an application's tests of what chapter 17 of the book describes (R20,
  * M5): each node is an actor system on local ports, joined to the first, with the split brain resolver. The
  * application starts on each what its host starts on a node; all of them share the default connection pool, as the
  * processes of one installation share a database.
  */
object TestCluster {

  /** A node's configuration: a cluster on local ports with the split brain resolver, and `extra` on top. */
  def config(extra: String = ""): Config = ConfigFactory.parseString(
    s"""pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
       |pekko.cluster.split-brain-resolver.stable-after = 2s
       |pekko.loglevel = WARNING
       |$extra""".stripMargin).withFallback(TestSystems.clusterConfig).withFallback(ConfigFactory.load())

  /** `count` nodes joined into one cluster, every one up; the first is the oldest. `appVersion` gives a node its
    * `pekko.cluster.app-version` (empty: Pekko's 0.0.0), for rolling upgrades (D65). */
  def start(count: Int, extra: String = "", appVersion: Int => String = _ => "", name: String = "cluster"): Vector[ActorSystem] = {
    val systems = (0 until count).toVector.map(i => ActorSystem(name, config(extra + versionLine(appVersion(i)))))
    val seed = Cluster(systems.head).selfAddress
    Cluster(systems.head).join(seed)
    awaitUp(systems.take(1), 1)
    systems.tail.foreach(system => Cluster(system).join(seed))
    awaitUp(systems, count)
    systems
  }

  /** `count` more nodes joined to the cluster of `to`, once every one is up. */
  def join(to: ActorSystem, count: Int, extra: String = "", appVersion: Int => String = _ => "", name: String = "cluster"): Vector[ActorSystem] = {
    val systems = (0 until count).toVector.map(i => ActorSystem(name, config(extra + versionLine(appVersion(i)))))
    systems.foreach(system => Cluster(system).join(Cluster(to).selfAddress))
    awaitUp(systems :+ to, Cluster(to).state.members.size + count)
    systems
  }

  private def versionLine(version: String): String = if (version.isEmpty) "" else "\npekko.cluster.app-version = \"" + version + "\""

  /** Waits until each of `systems` sees `count` members up; throws after `within`. */
  def awaitUp(systems: Seq[ActorSystem], count: Int, within: FiniteDuration = 30.seconds): Unit = {
    val deadline = System.nanoTime() + within.toNanos
    while (!systems.forall(system => Cluster(system).state.members.count(_.status == MemberStatus.Up) == count)) {
      if (System.nanoTime() > deadline) throw new IllegalStateException(s"$count nodes did not come up in $within")
      Thread.sleep(100)
    }
  }

  /** Stops the nodes, each waited for up to `within`; one that does not stop in time is left behind. */
  def stop(systems: Seq[ActorSystem], within: FiniteDuration = 30.seconds): Unit =
    systems.foreach(system => Try(Await.ready(system.terminate(), within)))
}
