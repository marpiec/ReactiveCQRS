package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicInteger

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._
import scala.util.Try

/** Carts whose commands time out after 1 s; adding the item "plain" takes 200 ms. */
class OneSecondCarts extends ShoppingCartAggregateContext {
  override val commandTimeout: FiniteDuration = 1.second
  override def commandHandlers = cart => {
    case c: AddItemConcurrently if c.name == "plain" =>
      Thread.sleep(200)
      super.commandHandlers(cart)(c)
    case other => super.commandHandlers(cart)(other)
  }
}

/** Responses in memory, whose look-ups by key take 1.5 s: a database that is slow for a moment. */
class SlowResponses extends MemoryCommandResponseState() {
  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = {
    Thread.sleep(1500)
    super.responseByKey(key)
  }
}

/** The store, whose look-ups of an aggregate's version fail `failures` times: a connection reset. */
class FailingLookupStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {
  val failures = new AtomicInteger()
  override def storedVersion(aggregateId: AggregateId): Option[(AggregateVersion, Int)] =
    if (failures.getAndDecrement() > 0) throw new java.sql.SQLException("connection reset", "08006") else super.storedVersion(aggregateId)
}

// Step 4 of the risk assessment of 1.0.0: an owner under a slow or failing database (F229, F231).
class OwnerFaultsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new FailingLookupStore(mpjsons, typesNamesState).initSchema().asInstanceOf[FailingLookupStore]
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val user = UserId(1)

  private def carts(context: ShoppingCartAggregateContext, responses: MemoryCommandResponseState = new MemoryCommandResponseState()): ActorRef =
    system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responses, eventBus))

  private def created(bus: ActorRef): AggregateId = bus.askFor[CustomCommandResponse[_]](CreateShoppingCart(None, user, "cart")) match {
    case SuccessResponse(id, _) => id
    case other => fail("not created: " + other)
  }

  Feature("An owner under a slow or failing database") {

    Scenario("F229 a command whose handler finished before its deadline, but whose turn came after, is not persisted") {
      val bus = carts(new OneSecondCarts, new SlowResponses)
      val cart = created(bus)
      val (plain, keyed) = (TestProbe(), TestProbe())

      plain.send(bus, AddItemConcurrently(user, cart, "plain")) // its handler ends while the owner reads the key, its turn after
      keyed.send(bus, AddItemConcurrently(user, cart, "keyed", Some(ClientKey("k-" + System.nanoTime())))) // its key is read for 1.5 s

      plain.expectMsgType[CustomCommandResponse[_]](5.seconds) mustBe a[CommandHandlingError]
      keyed.expectMsgType[CustomCommandResponse[_]](5.seconds) mustBe a[CommandHandlingError]
      bus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cart)).get.version mustBe AggregateVersion(1)
    }

    Scenario("F231 a look-up of the stored version that fails answers its command at once, and the owner goes on") {
      val bus = carts(new ShoppingCartAggregateContext)
      val cart = created(bus)
      eventStore.failures.set(1)
      val asker = TestProbe()

      asker.send(bus, AddItem(user, cart, AggregateVersion(1000), "far ahead")) // a version above the owner's is looked up
      asker.receiveOne(10.seconds) mustBe a[CommandHandlingError] // at once, not left to the 60 s timeout
      bus.askFor[CustomCommandResponse[_]](AddItem(user, cart, AggregateVersion(1), "next")) mustBe SuccessResponse(cart, AggregateVersion(2))
    }
  }
}
