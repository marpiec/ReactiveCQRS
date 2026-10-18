package io.reactivecqrs.testdomain.spec

import java.util.concurrent.ConcurrentLinkedQueue

import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.core.cluster.{Readiness, Singletons}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart
import io.reactivecqrs.testkit.{ErrorRecorder, TestCluster}
import org.apache.pekko.actor.{Actor, CoordinatedShutdown, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.event.Logging
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** A subscriber of carts' events that runs once in the cluster, as a managed projection's supervisor does. */
class CartsSubscriber extends Actor {
  override def receive: Receive = {
    case GetSubscriptionRequests(registered) =>
      sender() ! SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], registered)), registered)
  }
}

// A node that the split brain resolver downs ends: its cluster shuts down, which stops the singletons' proxies, and its
// coordinated shutdown starts a moment later. Found by E2's fault tests in Docker, where the proxy stopped in that
// moment and the bus logged an error (F182).
class DownedNodeSpec extends AnyFeatureSpec with Matchers with Eventually with GivenWhenThen {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Millis))
  private implicit val timeout: Timeout = Timeout(5.seconds)

  Feature("A node that is downed") {

    Scenario("its bus logs no error for the singleton's proxy that stops with its cluster (F182)") {
      Given("two nodes whose coordinated shutdown waits after a downing, so the moment before it starts lasts")
      val nodes = TestCluster.start(2, extra = "pekko.cluster.run-coordinated-shutdown-when-down = off", name = "downed")
      val Vector(nodeA, nodeB) = nodes: @unchecked
      try {
        And("on the second, a bus whose subscriber is a singleton's proxy, and a record of the errors it logs")
        Singletons.start(nodeA, "cartsSubscriber", Props(new CartsSubscriber)) // runs on the oldest, the first node
        val proxy = Singletons.start(nodeB, "cartsSubscriber", Props(new CartsSubscriber))
        val errors = new ConcurrentLinkedQueue[String]()
        val recorder = nodeB.actorOf(Props(new ErrorRecorder(errors)))
        nodeB.eventStream.subscribe(recorder, classOf[Logging.Error])
        val bus = nodeB.actorOf(Props(new EventsBusActor()), "eventBus")
        bus ! RegisterSubscribers(Seq(proxy))
        eventually(Readiness(nodeB).ready mustBe true)

        When("the second node downs itself, as the resolver does on the smaller side of a split")
        Cluster(nodeB).down(Cluster(nodeB).selfAddress)

        Then("its cluster ends, and the proxy with it, before any coordinated shutdown")
        eventually(Cluster(nodeB).isTerminated mustBe true)
        eventually(Await.result((bus ? GetDeliveryStatus).mapTo[DeliveryStatus], 5.seconds).stopped mustBe Seq(proxy.path.name))
        CoordinatedShutdown(nodeB).shutdownReason() mustBe None

        And("the bus logged no error about the proxy: it stopped as its node left the cluster")
        Await.result(recorder ? "ping", 5.seconds) // what was logged before is kept
        errors.asScala.filter(_.startsWith("Subscriber ")) mustBe empty
      } finally TestCluster.stop(nodes)
    }
  }
}
