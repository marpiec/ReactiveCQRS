package io.reactivecqrs.examples.e2

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateVersion, CustomCommandResponse, SuccessResponse}
import io.reactivecqrs.examples.e1.{AddItem, CartSummary}
import io.reactivecqrs.testkit.{ActorAskSupport, TestCluster, TestDatabase}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

// E2's node, three times in one JVM (the test kit's TestCluster), against the test database.
class ClusterSpec extends AnyFeatureSpec with Matchers with Eventually with ActorAskSupport with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(200, Millis))
  private implicit val timeout: Timeout = Timeout(70.seconds)

  // #threeNodes
  TestDatabase.init()
  private val systems = TestCluster.start(3, name = "shop")
  private val nodes = systems.map(system => ClusterSetup.startNode(system)) // what the host starts on a node

  override def afterAll(): Unit = TestCluster.stop(systems)
  // #threeNodes
  private val Vector(nodeA, nodeB, nodeC) = nodes: @unchecked

  private def addItem(node: ClusterSetup.Node, response: SuccessResponse, item: String): SuccessResponse =
    node.carts.askFor[CustomCommandResponse[_]](AddItem(UserId(1), response.aggregateId, response.aggregateVersion, item)) match {
      case added: SuccessResponse => added
      case refused => fail("Not added: " + refused)
    }

  Feature("E2: three nodes") {

    Scenario("a cart filled on every node has one owner, and the carts list, which runs once, answers on every node") {
      val (cartId, made) = ClusterSetup.fillCart(nodeA, UserId(1), "Shared") // made on the first node, with apples
      made mustBe Some(CartSummary("Shared", 1, 2))
      val third = addItem(nodeC, addItem(nodeB, SuccessResponse(cartId, AggregateVersion(2)), "pears"), "plums")

      third mustBe SuccessResponse(cartId, AggregateVersion(4)) // no version taken twice: one owner
      for (node <- Seq(nodeA, nodeB, nodeC)) ClusterSetup.summary(node, cartId, third.aggregateVersion) mustBe Some(CartSummary("Shared", 3, 4))
    }

    Scenario("the oldest node leaves: the carts list moves to another node, and gets the events published meanwhile") {
      val (cartId, _) = ClusterSetup.fillCart(nodeB, UserId(1), "Before the leave")
      val oldest = Cluster(systems.head)
      oldest.leave(oldest.selfAddress)
      val added = addItem(nodeC, SuccessResponse(cartId, AggregateVersion(2)), "pears") // while it leaves

      eventually(Cluster(systems(1)).state.members.map(_.address) must not contain oldest.selfAddress)
      eventually(ClusterSetup.summary(nodeB, cartId, added.aggregateVersion) mustBe Some(CartSummary("Before the leave", 2, 3)))
    }
  }
}
