package io.reactivecqrs.testdomain.spec

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.{Members, Singletons}
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.sharding.ShardRegion.{CurrentShardRegionState, GetShardRegionState}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.{Timeout, Version}
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

// D65–D67, D73: a rolling upgrade of the carts from 1.0.0 to 2.0.0, which adds the event class ItemAddedV2, under load.
class RollingUpgradeSpec extends AnyFeatureSpec with Matchers with Eventually with GivenWhenThen with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(90, Seconds), Span(200, Millis))
  implicit private val timeout: Timeout = Timeout(30.seconds)

  private val projectionOn = new ConcurrentHashMap[ActorSystem, ActorRef]()

  private def projection(node: TestNode): Seq[ActorRef] = {
    val subscriptions = new PostgresSubscriptionsState(node.typesNames).initSchema()
    val projections = new Projections(node.system, ProjectionsSettings(node.eventStore, subscriptions, new PostgresProjectionRegistry().initSchema(),
      Seq(new ShardedCartContext)))
    val carts = projections.start[ClusterCartsProjection]("rolling-carts", 1, "rollingCarts") { build =>
      new ClusterCartsProjection(subscriptions, new PostgresDocumentStore[ClusterCartItems](build.documentStore("rolling_carts"), node.mpjsons, new NoopDocumentStoreCache))
    }
    projectionOn.put(node.system, carts)
    // What waited in the outbox while the projection moved comes back after 2 or 4 seconds.
    Singletons.start(node.system, "outboxPoller", Props(new OutboxPollerActor(node.eventStore, node.eventBus, OutboxPollerOptions(firstPollAfter = 1.second,
      pollInterval = 1.second, redelivery = RedeliveryPolicy.Options(attemptsAfter = Seq(2.seconds, 4.seconds), giveUpAfter = 2.minutes)))))
    Seq(carts)
  }

  // Version 1.0.0 first: the oldest node runs the projection, and the carts made now have their owners on these nodes.
  private val olds = TestCluster.start(2, subscribers = projection, appVersion = _ => "1.0.0")
  private val Vector(oldest, olderToo) = olds
  private val oldCarts = (1 to 4).map(i => create(oldest, "old " + i))
  // Then version 2.0.0 joins: new shards go only to it (D66).
  private val news = TestCluster.join(oldest, 2, subscribers = projection, appVersion = _ => "2.0.0", carts = _ => new ShardedCartContextV2)
  private val Vector(newer, newest) = news

  override def afterAll(): Unit = TestCluster.stop(olds ++ news)

  private def askFor[T](ref: ActorRef, message: Any): T = Await.result(ref ? message, 30.seconds).asInstanceOf[T]

  private def create(node: TestNode, name: String): AggregateId =
    askFor[CustomCommandResponse[_]](node.carts, CreateShoppingCart(None, UserId(1), name)) match {
      case SuccessResponse(id, _) => id
      case other => throw new IllegalStateException("Not created: " + other)
    }

  private def add(node: TestNode, cart: AggregateId, name: String): CustomCommandResponse[_] =
    askFor[CustomCommandResponse[_]](node.carts, AddItemConcurrently(UserId(1), cart, name))

  // As a client would: a node may begin to leave right after it was chosen, so a command that gets no answer in time is
  // sent again through another node, with the same idempotency key: it is applied once either way.
  private def sendRetrying(cart: AggregateId, name: String, first: TestNode): CustomCommandResponse[_] = {
    val command = AddItemConcurrently(UserId(1), cart, name, Some(ClientKey(cart.asLong + " " + name)))
    Try(Await.result((first.carts ? command)(Timeout(10.seconds)), 10.seconds).asInstanceOf[CustomCommandResponse[_]])
      .getOrElse(askFor[CustomCommandResponse[_]](newest.carts, command))
  }

  private def up(node: TestNode): Boolean =
    newest.cluster.state.members.exists(m => m.address == node.cluster.selfAddress && m.status == MemberStatus.Up)

  private def itemsOf(cart: AggregateId): Vector[String] =
    askFor[Try[Aggregate[ShoppingCart]]](newest.carts, GetAggregate(cart)).get.aggregateRoot.get.items.map(_.name)

  private def storedV2(carts: Seq[AggregateId]): Long = DB.readOnly { implicit session =>
    SQL("SELECT count(*) FROM events e JOIN types_names t ON t.id = e.event_type_id WHERE t.name = ? AND e.aggregate_id IN (" +
      carts.map(_.asLong).mkString(",") + ")").bind(classOf[ItemAddedV2].getName).map(_.long(1)).single().getOrElse(0L)
  }

  private def ownersOn(node: TestNode): Set[Long] =
    askFor[CurrentShardRegionState](node.cartOwners, GetShardRegionState).shards.flatMap(_.entityIds).map(_.toLong)

  private def gone(node: TestNode): Boolean = !newest.cluster.state.members.exists(_.address == node.cluster.selfAddress)

  Feature("A rolling upgrade that adds an event class (D65–D67, D73)") {

    Scenario("the old nodes leave one by one under load; the new class is written only once they are gone, and no command is lost or applied twice") {
      Given("carts whose owners run on the old nodes and carts whose owners run on the new ones, under a steady load from every node")
      // Twelve, so their ids (consecutive in this node's pool) reach the shards that no old cart took: those go to the new nodes.
      val newCarts = (1 to 12).map(i => create(newer, "new " + i))
      val carts = oldCarts ++ newCarts
      val sent = new ConcurrentLinkedQueue[(AggregateId, String, CustomCommandResponse[_])]()
      @volatile var sending = true
      import scala.concurrent.ExecutionContext.Implicits.global
      val load = Future {
        var round = 0
        while (sending) {
          round += 1
          val through = Seq(oldest, olderToo, newer, newest)(round % 4)
          carts.foreach { cart =>
            val name = "item " + round
            sent.add((cart, name, sendRetrying(cart, name, if (up(through)) through else newest)))
          }
        }
      }

      Then("while an old node is a member, the new class is not written, and a command that would write it is rejected")
      Members(newest.system).oldestAppVersion mustBe Some(Version("1.0.0"))
      // An owner on a new node runs version 2.0.0; one on an old node does not know the class.
      val onNewNode = newCarts.find(cart => Seq(newer, newest).exists(node => ownersOn(node).contains(cart.asLong))).get
      add(newest, onNewNode, "v2 early") mustBe a[CommandRejected]
      eventually(sent.size must be > 40)
      storedV2(carts) mustBe 0L

      When("the old nodes leave, the oldest, which runs the projection, last")
      olderToo.cluster.leave(olderToo.cluster.selfAddress)
      eventually(gone(olderToo) mustBe true)
      storedV2(carts) mustBe 0L
      oldest.cluster.leave(oldest.cluster.selfAddress)
      eventually(gone(oldest) mustBe true)

      Then("the new class is written, by the owners that moved from the old nodes too")
      eventually(add(newest, oldCarts.head, "v2 late") mustBe a[SuccessResponse])
      eventually(storedV2(oldCarts) must be > 1L)
      sending = false
      Await.result(load, 60.seconds)

      And("every command succeeded, and each item is in its cart once, in the store and in the projection")
      val responses = sent.asScala.toVector
      responses.map(_._3).filterNot(_.isInstanceOf[SuccessResponse]) mustBe empty
      carts.foreach { cart =>
        val expected = (responses.filter(_._1 == cart).map(_._2) ++ (if (cart == oldCarts.head) Seq("v2 late") else Nil)).sorted
        itemsOf(cart).sorted mustBe expected
        eventually(askFor[Option[Vector[String]]](projectionOn.get(newest.system), GetClusterCartItems(cart)).map(_.sorted) mustBe Some(expected))
      }
    }
  }
}
