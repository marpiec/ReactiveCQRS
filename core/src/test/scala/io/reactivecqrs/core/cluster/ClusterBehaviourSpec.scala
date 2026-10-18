package io.reactivecqrs.core.cluster

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Address, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.coordination.lease.LeaseSettings
import org.apache.pekko.coordination.lease.scaladsl.Lease
import org.apache.pekko.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/** Answers where it runs. */
class WhereAmI extends Actor {
  override def receive: Receive = { case _: String => sender() ! Cluster(context.system).selfAddress }
}

/** A lease in this JVM, as a Kubernetes lease is one in the cluster: one owner at a time. */
object InJvmLease {
  val owner = new java.util.concurrent.atomic.AtomicReference[Option[String]](None)
}
class InJvmLease(settings: LeaseSettings, system: ExtendedActorSystem) extends Lease(settings) {
  private def take(): Boolean = InJvmLease.owner.compareAndSet(None, Some(settings.ownerName)) || checkLease()
  override def acquire(): Future[Boolean] = Future.successful(take())
  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] = acquire()
  override def release(): Future[Boolean] = Future.successful(InJvmLease.owner.compareAndSet(Some(settings.ownerName), None))
  override def checkLease(): Boolean = InJvmLease.owner.get().contains(settings.ownerName)
}

// M5 spike (D66, D46): what the cluster design relies on in Pekko 1.7, kept as a test so an upgrade that changes it
// is noticed. Nodes are ActorSystems in this JVM on local ports.
class ClusterBehaviourSpec extends AnyFeatureSpec with Matchers with Eventually with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Millis))
  implicit private val timeout: Timeout = Timeout(5.seconds)

  private var systems = Vector[ActorSystem]()

  private def node(appVersion: String, extra: String = ""): ActorSystem = {
    val config = ConfigFactory.parseString(
      s"""pekko.actor.provider = cluster
         |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
         |pekko.cluster.app-version = "$appVersion"
         |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
         |pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
         |pekko.cluster.split-brain-resolver.stable-after = 2s
         |pekko.coordinated-shutdown.exit-jvm = off
         |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
         |pekko.loglevel = WARNING
         |$extra""".stripMargin).withFallback(ConfigFactory.load())
    val system = ActorSystem("spike", config)
    systems :+= system
    system
  }

  private def join(system: ActorSystem, seed: ActorSystem): Unit = {
    Cluster(system).join(Cluster(seed).selfAddress)
    eventually(Cluster(system).selfMember.status mustBe MemberStatus.Up)
  }

  private def members(system: ActorSystem): Set[Address] = Cluster(system).state.members.filter(_.status == MemberStatus.Up).map(_.address)

  override def afterAll(): Unit = systems.foreach(system => TestKit.shutdownActorSystem(system, 20.seconds, verifySystemShutdown = false))

  // Strings cross nodes with Pekko's own serializer; the library's messages get theirs in M5 (D45).
  private val extractEntityId: ShardRegion.ExtractEntityId = { case id: String => (id, id) }
  private val extractShardId: ShardRegion.ExtractShardId = {
    case id: String => (math.abs(id.hashCode) % 20).toString
    case ShardRegion.StartEntity(id) => (math.abs(id.hashCode) % 20).toString
    case other => throw new IllegalArgumentException("Not a message of this spec: " + other)
  }

  private def startSharding(system: ActorSystem): ActorRef =
    ClusterSharding(system).start("where", Props(new WhereAmI), ClusterShardingSettings(system), extractEntityId, extractShardId)

  Feature("Shard allocation during a rolling upgrade (D66)") {

    Scenario("new shards go to the nodes with the newest app-version, none to an older one") {
      val (old, newer) = (node("1.0.0"), node("1.0.1"))
      join(old, old)
      join(newer, old)
      val regions = Seq(startSharding(old), startSharding(newer))

      val placed = (1 to 60).map(i => Await.result((regions(i % 2) ? ("entity-" + i)).mapTo[Address], 10.seconds)).toSet

      placed mustBe Set(Cluster(newer).selfAddress)
    }
  }

  Feature("Singletons (D46, D66)") {

    Scenario("a singleton runs on the oldest node and moves to the next oldest when that one leaves") {
      val (first, second) = (node("2.0.0"), node("2.0.0"))
      join(first, first)
      join(second, first)
      Seq(first, second).foreach { system =>
        system.actorOf(ClusterSingletonManager.props(Props(new WhereAmI), PoisonPill, ClusterSingletonManagerSettings(system)), "singleton")
      }
      val proxy = second.actorOf(ClusterSingletonProxy.props("/user/singleton", ClusterSingletonProxySettings(second)))
      def where = Await.result((proxy ? "where").mapTo[Address], 10.seconds)

      eventually(where mustBe Cluster(first).selfAddress)
      Cluster(first).leave(Cluster(first).selfAddress)
      eventually(where mustBe Cluster(second).selfAddress)
    }
  }

  Feature("The split brain resolver (D46)") {

    Scenario("a node that is lost without leaving is downed, and the rest go on") {
      val (a, b, c) = (node("3.0.0", "pekko.cluster.split-brain-resolver.active-strategy = keep-majority"),
        node("3.0.0", "pekko.cluster.split-brain-resolver.active-strategy = keep-majority"),
        node("3.0.0", "pekko.cluster.split-brain-resolver.active-strategy = keep-majority"))
      join(a, a)
      join(b, a)
      join(c, a)
      eventually(members(a).size mustBe 3)

      Await.result(c.terminate(), 20.seconds) // as a crash: no leave

      eventually(members(a) mustBe Set(Cluster(a).selfAddress, Cluster(b).selfAddress))
      Cluster(a).state.unreachable mustBe empty
    }

    Scenario("with a lease, the side that holds it stays when the other is lost, also without a majority") {
      val withLease =
        """pekko.cluster.split-brain-resolver.active-strategy = lease-majority
          |pekko.cluster.split-brain-resolver.lease-majority.lease-implementation = "in-jvm-lease"
          |pekko.cluster.split-brain-resolver.lease-majority.acquire-lease-delay-for-minority = 1s
          |in-jvm-lease {
          |  lease-class = "io.reactivecqrs.core.cluster.InJvmLease"
          |  heartbeat-interval = 200ms
          |  heartbeat-timeout = 10s
          |  lease-operation-timeout = 3s
          |}""".stripMargin
      val (a, b) = (node("4.0.0", withLease), node("4.0.0", withLease))
      join(a, a)
      join(b, a)
      eventually(members(a).size mustBe 2)

      Await.result(b.terminate(), 20.seconds)

      eventually(members(a) mustBe Set(Cluster(a).selfAddress))
      Cluster(a).selfMember.status mustBe MemberStatus.Up
    }
  }
}
