package io.reactivecqrs.examples.e2

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{ServerSocket, URI}

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap
import org.apache.pekko.management.scaladsl.PekkoManagement
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

// What E2's main and its deployments rely on, without Docker or Kubernetes: the configurations made from the
// environment, Cluster Bootstrap, and Pekko Management's health checks with the library's readiness check.
class NodeSpec extends AnyFeatureSpec with Matchers with Eventually {

  TestDatabase.init()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(200, Millis))

  Feature("E2's node configuration") {

    Scenario("on Kubernetes: the pod's IP, contact points from the Kubernetes API, no seed nodes, and the lease (F180)") {
      val config = ClusterSetup.nodeConfig(Map("NODE_HOST" -> "10.244.0.6", "REQUIRED_CONTACT_POINTS" -> "3",
        "APP_VERSION" -> "2.0.0", "SHOP_CONFIG" -> "e2-kubernetes").get)

      config.getString("pekko.remote.artery.canonical.hostname") mustBe "10.244.0.6"
      config.getString("pekko.management.http.hostname") mustBe "10.244.0.6"
      config.getStringList("pekko.cluster.seed-nodes").asScala mustBe empty
      config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.discovery-method") mustBe "kubernetes-api"
      config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.service-name") mustBe "shop"
      config.getInt("pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr") mustBe 3
      config.getString("pekko.cluster.app-version") mustBe "2.0.0"
      config.getString("pekko.cluster.split-brain-resolver.active-strategy") mustBe "lease-majority"
      val lease = config.getString("pekko.cluster.split-brain-resolver.lease-majority.lease-implementation")
      config.getString(lease + ".lease-class") mustBe "org.apache.pekko.coordination.lease.kubernetes.NativeKubernetesLease"
      config.getBoolean("pekko.coordinated-shutdown.exit-jvm") mustBe true
      config.getInt("pekko.management.http.port") mustBe 8558
    }

    Scenario("with Docker Compose: the three nodes as contact points, all three needed for a new cluster") {
      val config = ClusterSetup.nodeConfig(Map("NODE_HOST" -> "node2", "CONTACT_POINTS" -> "node1, node2, node3").get)

      config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.discovery-method") mustBe "config"
      config.getConfigList("pekko.discovery.config.services.shop.endpoints").asScala.map(e => (e.getString("host"), e.getInt("port"))) mustBe
        Seq(("node1", 8558), ("node2", 8558), ("node3", 8558))
      config.getInt("pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr") mustBe 3
      config.getString("pekko.cluster.split-brain-resolver.active-strategy") mustBe "keep-majority"
    }

    Scenario("binds the library's serializer itself, whatever the order of the classpath's reference.conf files (F178)") {
      ConfigFactory.parseResources("e2.conf").getString("pekko.actor.serialization-bindings.\"java.io.Serializable\"") mustBe
        "reactivecqrs"
    }

    Scenario("a node alone, as main starts it: Bootstrap forms a cluster of one, ready once its bus knows the carts list") {
      val port = { val socket = new ServerSocket(0); try socket.getLocalPort finally socket.close() }
      // As main makes it, but on free ports, and a test's stop must not end the JVM.
      val system = ActorSystem("shop", ConfigFactory.parseString(
        s"""pekko.remote.artery.canonical.port = 0
           |pekko.management.http.port = $port
           |pekko.discovery.config.services.shop.endpoints = [{host = "127.0.0.1", port = $port}]
           |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
           |pekko.coordinated-shutdown.exit-jvm = off
           |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
           |""".stripMargin).withFallback(ClusterSetup.nodeConfig(Map("NODE_HOST" -> "127.0.0.1").get)))
      try {
        Await.result(PekkoManagement(system).start(), 30.seconds)
        ClusterBootstrap(system).start()
        ClusterSetup.startNode(system)

        eventually(Cluster(system).selfMember.status mustBe MemberStatus.Up)
        val http = HttpClient.newHttpClient()
        def status(path: String): Int =
          http.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).build(), HttpResponse.BodyHandlers.discarding()).statusCode()
        status("/alive") mustBe 200
        eventually(status("/ready") mustBe 200)
      } finally Await.ready(system.terminate(), 30.seconds)
    }
  }
}
