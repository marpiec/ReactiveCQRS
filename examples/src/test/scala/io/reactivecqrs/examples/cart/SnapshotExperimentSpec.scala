package io.reactivecqrs.examples.cart

// Chapter 13's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it does.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

class SnapshotExperimentSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)
  private val metrics = new RecordingMetrics
  ReactiveMetrics(system).use(metrics)

  private def succeeds(bus: org.apache.pekko.actor.ActorRef, command: AnyRef): SuccessResponse =
    Await.result(bus ? command, timeout.duration) match {
      case done: SuccessResponse => done
      case refused => fail(s"$command was answered $refused")
    }

  private def snapshotVersion(cartId: AggregateId): Option[Int] = DB.readOnly { implicit session =>
    sql"SELECT version FROM snapshots WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single()
  }

  private def within20Seconds(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) fail("Not within 20 seconds")
      Thread.sleep(100)
    }
  }

  Feature("Snapshots") {

    Scenario("the context's policy: one every 200 events, and one at passivation") {
      new SnapshottedCarts().snapshots mustBe Some(SnapshotPolicy(every = 200, onPassivation = true, schemaVersion = 1))
      new ReshapedCarts().snapshots.map(_.schemaVersion) mustBe Some(2)
    }

    Scenario("watching it work: a snapshot at version 3, and a restore that reads it and no event") {
      val carts = cartsOf(new ShoppingCartAggregateContext {
        override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = 3))
      }, "snapshotCarts")
      val cartId = succeeds(carts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(1), "apples"))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(2), "oranges"))
      within20Seconds(snapshotVersion(cartId).contains(3))

      // #passivate
      import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate

      carts ! PassivateAggregate(cartId)
      // #passivate
      val restoresFromSnapshots = metrics.timed("reactivecqrs.restore", Map("from" -> "snapshot")).size
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(3), "pears"))
      metrics.timed("reactivecqrs.restore", Map("from" -> "snapshot")).size mustBe restoresFromSnapshots + 1
      snapshotVersion(cartId) mustBe Some(3)
    }

    Scenario("an event store that allows longer histories") {
      // #limit
      val eventStore = new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000).initSchema()
      // #limit
      eventStore must not be null
    }
  }
}

// #policy
import io.reactivecqrs.api.SnapshotPolicy

class SnapshottedCarts extends ShoppingCartAggregateContext {
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy())
}
// #policy

class ReshapedCarts extends ShoppingCartAggregateContext {
  // #schemaVersion
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(schemaVersion = 2))
  // #schemaVersion
}
