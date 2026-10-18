package io.reactivecqrs.examples.e2

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{ServerSocket, URI}

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.management.scaladsl.PekkoManagement
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

// What E2's main and its deployments rely on, without Docker or Kubernetes: the configurations made from the
// environment, and Pekko Management's health checks with the library's readiness check.
class NodeSpec extends AnyFeatureSpec with Matchers with Eventually {

  TestDatabase.init()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(200, Millis))

  Feature("E2's node configuration") {

    Scenario("on Kubernetes: the pod's address and the seeds from the environment, and the split brain resolver's lease") {
      val config = ClusterSetup.nodeConfig(Map("NODE_HOST" -> "shop-1.shop", "APP_VERSION" -> "2.0.0", "SHOP_CONFIG" -> "e2-kubernetes",
        "SEED_NODES" -> "pekko://shop@shop-0.shop:17355, pekko://shop@shop-1.shop:17355").get)

      config.getString("pekko.remote.artery.canonical.hostname") mustBe "shop-1.shop"
      config.getInt("pekko.remote.artery.canonical.port") mustBe 17355
      config.getStringList("pekko.cluster.seed-nodes").asScala mustBe Seq("pekko://shop@shop-0.shop:17355", "pekko://shop@shop-1.shop:17355")
      config.getString("pekko.cluster.app-version") mustBe "2.0.0"
      config.getString("pekko.cluster.split-brain-resolver.active-strategy") mustBe "lease-majority"
      val lease = config.getString("pekko.cluster.split-brain-resolver.lease-majority.lease-implementation")
      config.getString(lease + ".lease-class") mustBe "org.apache.pekko.coordination.lease.kubernetes.NativeKubernetesLease"
      config.getBoolean("pekko.coordinated-shutdown.exit-jvm") mustBe true
      config.getInt("pekko.management.http.port") mustBe 8558
    }

    Scenario("with Docker Compose, a node alone: a cluster of one, keep-majority, ready once its bus knows the carts list") {
      val port = { val socket = new ServerSocket(0); try socket.getLocalPort finally socket.close() }
      // As main makes it, but on free ports, and a test's stop must not end the JVM.
      val system = ActorSystem("shop", ConfigFactory.parseString(
        s"""pekko.remote.artery.canonical.port = 0
           |pekko.cluster.seed-nodes = []
           |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
           |pekko.coordinated-shutdown.exit-jvm = off
           |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
           |pekko.management.http.port = $port
           |""".stripMargin).withFallback(ClusterSetup.nodeConfig(Map("NODE_HOST" -> "127.0.0.1").get)))
      try {
        system.settings.config.getString("pekko.cluster.split-brain-resolver.active-strategy") mustBe "keep-majority"
        Await.result(PekkoManagement(system).start(), 30.seconds)
        Cluster(system).join(Cluster(system).selfAddress)
        ClusterSetup.startNode(system)

        val http = HttpClient.newHttpClient()
        def status(path: String): Int =
          http.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).build(), HttpResponse.BodyHandlers.discarding()).statusCode()
        status("/alive") mustBe 200
        eventually(status("/ready") mustBe 200)
      } finally Await.ready(system.terminate(), 30.seconds)
    }
  }
}
