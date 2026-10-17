package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicInteger

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/** Carts that count how often their handler of `AddItemConcurrently` runs. */
class CountingCarts extends ShoppingCartAggregateContext {
  val handlerRuns = new AtomicInteger()
  override def commandHandlers = cart => {
    case c: AddItemConcurrently =>
      handlerRuns.incrementAndGet()
      CommandSuccess(ItemAdded(c.name))
    case other => super.commandHandlers(cart)(other)
  }
}

// F99: concurrent commands that conflict with commits of their own owner run again at once, one at a time.
class ContendedCommandsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(new PostgresUidGenerator("aggregates_uids_seq"),
    new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val carts = new CountingCarts
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(carts, uidGenerator, eventStore, responses, eventBus))
  private val userId = UserId(1)

  feature("Concurrent commands that conflict only with their own owner's commits (F99)") {

    scenario("forty sent to one aggregate at once all succeed, and each handler runs at most twice") {
      val created: CustomCommandResponse[_] = commandBus ?? CreateShoppingCart(None, userId, "contended")
      val cartId = created.asInstanceOf[SuccessResponse].aggregateId
      carts.handlerRuns.set(0)

      implicit val timeout: Timeout = Timeout(60.seconds)
      import scala.concurrent.ExecutionContext.Implicits.global
      val answers = Await.result(Future.sequence((1 to 40).map(i => commandBus ? AddItemConcurrently(userId, cartId, "item " + i))), 60.seconds)

      answers.filterNot(_.isInstanceOf[SuccessResponse]) mustBe empty
      val cart: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      cart.get.version mustBe AggregateVersion(41)
      carts.handlerRuns.get must be <= 80
    }
  }
}
