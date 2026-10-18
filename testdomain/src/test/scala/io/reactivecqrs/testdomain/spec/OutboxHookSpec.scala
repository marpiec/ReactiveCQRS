package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregate.{OutboxWriter, WithOutboxWriter}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

/** Carts whose added items become rows of a job table, written with the events; an item named "refused" fails. */
class CartsWithJobs extends ShoppingCartAggregateContext with WithOutboxWriter[ShoppingCart] {
  override def outboxWriter: OutboxWriter[ShoppingCart] = new OutboxWriter[ShoppingCart] {
    override def write(aggregateId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[ShoppingCart]])(implicit session: DBSession): Unit =
      events.foreach(_.event match {
        case ItemAdded("refused") => throw new IllegalStateException("the job store refuses this item")
        case ItemAdded(name) => sql"INSERT INTO outbox_hook_jobs (aggregate_id, version, item) VALUES (${aggregateId.asLong}, ${version.asInt}, $name)".update()
        case _ => ()
      })
  }
}

// R12, D33: rows of the application's own, written in the transaction of the events.
class OutboxHookSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  DB.autoCommit { implicit session =>
    sql"CREATE TABLE IF NOT EXISTS outbox_hook_jobs (aggregate_id BIGINT NOT NULL, version INT NOT NULL, item TEXT NOT NULL)".execute()
  }
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new CartsWithJobs, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus))

  private val userId = UserId(1)

  private def storedCart(): AggregateId = {
    val cartId = AggregateId(TestIds.aggregate())
    eventStore.localTx(session => eventStore.persistEvents(cartId,
      NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries"))))(session)).get
    cartId
  }

  private def jobs(cartId: AggregateId): List[(Int, String)] = DB.readOnly { implicit session =>
    sql"SELECT version, item FROM outbox_hook_jobs WHERE aggregate_id = ${cartId.asLong} ORDER BY version".map(rs => (rs.int(1), rs.string(2))).list()
  }

  private def storedEvents(cartId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single().get
  }

  Feature("The outbox hook (R12)") {

    Scenario("writes its rows in the transaction of the events") {
      val cartId = storedCart()
      val added = commandBus.askFor[CustomCommandResponse[_]](AddItems(userId, cartId, AggregateVersion(1), Seq("apple", "pear")))
      added mustBe SuccessResponse(cartId, AggregateVersion(3))
      jobs(cartId) mustBe List((3, "apple"), (3, "pear"))
    }

    Scenario("a writer that fails rolls the events back, and the aggregate carries on") {
      val cartId = storedCart()
      val refused = commandBus.askFor[CustomCommandResponse[_]](AddItems(userId, cartId, AggregateVersion(1), Seq("apple", "refused")))
      refused mustBe an[EventHandlingError]
      storedEvents(cartId) mustBe 1
      jobs(cartId) mustBe empty

      val next = commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(1), "plum"))
      next mustBe SuccessResponse(cartId, AggregateVersion(2))
      jobs(cartId) mustBe List((2, "plum"))
    }

    Scenario("a copy of an idempotent command does not write the rows again") {
      val cartId = storedCart()
      val key = Some(ClientKey("outbox hook " + System.nanoTime()))
      val first = commandBus.askFor[CustomCommandResponse[_]](AddItemConcurrently(userId, cartId, "fig", key))
      val copy = commandBus.askFor[CustomCommandResponse[_]](AddItemConcurrently(userId, cartId, "fig", key))
      copy mustBe first
      jobs(cartId) mustBe List((2, "fig"))
      storedEvents(cartId) mustBe 2
    }
  }
}
