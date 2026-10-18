package io.reactivecqrs.testdomain.spec

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregate.AggregateOwner.OwnerStarted
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishEventsAck
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.cluster.sharding.ShardRegion.{CurrentShardRegionState, GetShardRegionState}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// D43, D72, F46: owners of a sharded type, one per aggregate in the cluster.
class ShardedOwnersSpec extends AnyFeatureSpec with Matchers with Eventually with GivenWhenThen with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Millis))
  implicit private val timeout: Timeout = Timeout(30.seconds)

  // Three nodes; the last one leaves in the last scenario. Owners idle for 5 s are passivated by their shards.
  private val nodes = CartCluster.start(3, "pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = 5s")
  private val Vector(nodeA, nodeB, nodeC) = nodes: @unchecked

  override def afterAll(): Unit = CartCluster.stop(nodes)

  private def askFor[T](ref: ActorRef, message: Any): T = Await.result(ref ? message, 30.seconds).asInstanceOf[T]

  private def create(node: TestNode, name: String): AggregateId =
    askFor[CustomCommandResponse[_]](node.carts, CreateShoppingCart(None, UserId(1), name)) match {
      case SuccessResponse(id, _) => id
      case other => fail("Not created: " + other)
    }

  private def addItem(node: TestNode, cart: AggregateId, version: Int, name: String): CustomCommandResponse[_] =
    askFor[CustomCommandResponse[_]](node.carts, AddItem(UserId(1), cart, AggregateVersion(version), name))

  private def cartOn(node: TestNode, cart: AggregateId): Aggregate[ShoppingCart] =
    askFor[Try[Aggregate[ShoppingCart]]](node.carts, GetAggregate(cart)).get

  /** The aggregates whose owners run on the node. */
  private def ownersOn(node: TestNode): Set[Long] =
    askFor[CurrentShardRegionState](node.cartOwners, GetShardRegionState).shards.flatMap(_.entityIds).map(_.toLong)

  private def waitingInOutbox(carts: Seq[AggregateId]): Long = DB.readOnly { implicit session =>
    SQL("SELECT count(*) FROM events_to_publish WHERE aggregate_id IN (" + carts.map(_.asLong).mkString(",") + ")").map(_.long(1)).single().getOrElse(0L)
  }

  Feature("One owner per aggregate in the cluster (D43)") {

    Scenario("a command sent on any node reaches the aggregate's one owner, wherever it runs") {
      Given("twenty carts created through the first node")
      val carts = (1 to 20).map(i => create(nodeA, "cart " + i))

      Then("their owners run on every node, each on one node only")
      val owners = nodes.map(ownersOn)
      owners.foreach(_ must not be empty)
      owners.flatten.size mustBe owners.flatten.toSet.size
      carts.map(_.asLong).toSet.subsetOf(owners.flatten.toSet) mustBe true

      When("an item is added to each through the second node, and another through the third")
      carts.foreach(cart => addItem(nodeB, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2)))
      carts.foreach(cart => addItem(nodeC, cart, 2, "pear") mustBe SuccessResponse(cart, AggregateVersion(3)))

      Then("every node reads the same carts")
      carts.foreach { cart =>
        val read = nodes.map(cartOn(_, cart))
        read.map(_.version).distinct mustBe Seq(AggregateVersion(3))
        read.map(_.aggregateRoot.get.items.map(_.name)).distinct mustBe Seq(Vector("apple", "pear"))
      }
    }

    Scenario("concurrent commands from every node to one aggregate run in its one owner") {
      val cart = create(nodeA, "busy cart")
      import scala.concurrent.ExecutionContext.Implicits.global
      val sent = Future.sequence(for (node <- nodes; i <- 1 to 15) yield
        (node.carts ? AddItemConcurrently(UserId(1), cart, "item " + i)).mapTo[CustomCommandResponse[_]])
      Await.result(sent, 60.seconds).collect { case s: SuccessResponse => s }.size mustBe 45
      cartOn(nodeB, cart).version mustBe AggregateVersion(46)
      nodes.count(node => ownersOn(node).contains(cart.asLong)) mustBe 1
    }

    Scenario("an owner's outbox is emptied by its own node's command bus, wherever the command came from") {
      val carts = (1 to 10).map(i => create(nodeB, "delivered " + i))
      carts.foreach(cart => addItem(nodeC, cart, 1, "apple"))
      eventually(waitingInOutbox(carts) mustBe 0L)
    }

    Scenario("acknowledgements reach the owner that runs on the bus's node, and only while it runs") {
      val cart = AggregateId(Long.MaxValue / 2)
      val ack = PublishEventsAck(cart, Seq(AggregateVersion(1)), None)
      val first = TestProbe()(nodeA.system)
      first.send(nodeA.carts, OwnerStarted(cart))
      nodeA.carts ! ack
      first.expectMsg(ack)

      nodeA.system.stop(first.ref)
      val next = TestProbe()(nodeA.system)
      next.watch(first.ref)
      next.expectTerminated(first.ref)
      next.send(nodeA.carts, OwnerStarted(cart))
      nodeA.carts ! ack
      next.expectMsg(ack)
    }
  }

  Feature("Stopping owners (D72, F46)") {

    Scenario("a passivation asked on one node stops the owner on another, and the next command restores it") {
      val cart = Iterator.continually(create(nodeA, "passivated")).find(cart => !ownersOn(nodeA).contains(cart.asLong)).get
      val node = nodes.find(node => ownersOn(node).contains(cart.asLong)).get
      addItem(nodeA, cart, 1, "apple")

      nodeA.carts ! PassivateAggregate(cart)
      eventually(timeout(Span(2, Seconds)))(ownersOn(node) must not contain cart.asLong) // well before idle passivation

      addItem(nodeA, cart, 2, "pear") mustBe SuccessResponse(cart, AggregateVersion(3))
      cartOn(nodeA, cart).aggregateRoot.get.items.map(_.name) mustBe Vector("apple", "pear")
    }

    Scenario("a rewrite of a base stops its duplicate's owner wherever it runs, and the next one reads the new history") {
      val base = create(nodeA, "Groceries")
      addItem(nodeA, base, 1, "apple")
      val copy = askFor[CustomCommandResponse[_]](nodeB.carts, DuplicateShoppingCart(UserId(1), base, AggregateVersion(2))) match {
        case SuccessResponse(id, _) => id
        case other => fail("Not duplicated: " + other)
      }
      cartOn(nodeC, copy).aggregateRoot.get.name mustBe "Groceries" // its owner runs

      askFor[CustomCommandResponse[_]](nodeA.carts, RewriteCartName(UserId(1), base, AggregateVersion(2), "Renamed")) mustBe a[SuccessResponse]

      // Each read keeps the owner from idling: only the stop for the changed history replaces it.
      eventually(timeout(Span(3, Seconds)))(cartOn(nodeB, copy).aggregateRoot.get.name mustBe "Renamed")
    }

    Scenario("an idle owner is passivated by its shard, and comes back with its state") {
      val cart = create(nodeC, "idle")
      addItem(nodeC, cart, 1, "apple")
      eventually(nodes.exists(node => ownersOn(node).contains(cart.asLong)) mustBe false)
      cartOn(nodeA, cart).aggregateRoot.get.items.map(_.name) mustBe Vector("apple")
    }

    Scenario("a node that leaves hands its owners over, and every command sent meanwhile is applied once") {
      Given("carts whose owners run on the node that will leave, one of them at work on a command that takes twelve seconds")
      val carts = Iterator.continually(create(nodeA, "handed over")).filter(cart => ownersOn(nodeC).contains(cart.asLong)).take(10).toVector
      val slowCart = Iterator.continually(create(nodeA, "slow")).find(cart => ownersOn(nodeC).contains(cart.asLong)).get
      import scala.concurrent.ExecutionContext.Implicits.global
      val slow = (nodeB.carts ? AddItemConcurrently(UserId(1), slowCart, "slow item")).mapTo[CustomCommandResponse[_]]
      Thread.sleep(500)

      When("each gets five items, one every 150 ms (a cart takes five), while that node leaves")
      val added = Future.sequence(carts.map(cart => Future {
        (1 to 5).map { i =>
          Thread.sleep(150)
          addItem(if (i % 2 == 0) nodeA else nodeB, cart, i, "item " + i)
        }
      }))
      Thread.sleep(300)
      nodeC.cluster.leave(nodeC.cluster.selfAddress)

      Then("every command succeeds, and each cart has its five items, once each")
      Await.result(added, 120.seconds).flatten.filterNot(_.isInstanceOf[SuccessResponse]) mustBe empty

      And("the node waited for the slow command before it left (D72)")
      Await.result(slow, 30.seconds) mustBe SuccessResponse(slowCart, AggregateVersion(2))

      And("once the node is gone, the others' owners serve the carts")
      eventually(nodeA.cluster.state.members.map(_.address) must not contain nodeC.cluster.selfAddress)
      carts.foreach { cart =>
        val read = cartOn(nodeA, cart)
        read.version mustBe AggregateVersion(6)
        read.aggregateRoot.get.items.map(_.name) mustBe (1 to 5).map("item " + _).toVector
      }
      carts.map(_.asLong).toSet.subsetOf(Seq(nodeA, nodeB).flatMap(ownersOn).toSet) mustBe true
    }
  }
}
