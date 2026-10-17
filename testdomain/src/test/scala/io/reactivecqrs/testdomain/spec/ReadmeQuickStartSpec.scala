package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/** The domain of README.md's quick start, as it is written there. Change both together. */
object ReadmeQuickStart {

  case class ShoppingCart(name: String, items: Vector[String])

  case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
    def spaceId: SpaceId = SpaceId(0) // the organization the cart belongs to; 0 is the platform
  }
  case class ItemAdded(name: String) extends Event[ShoppingCart]

  case class CreateShoppingCart(userId: UserId, name: String)
    extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]
  case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
    extends Command[ShoppingCart, CustomCommandResponse[_]]

  class ShoppingCartContext extends AggregateContext[ShoppingCart] {
    override val version: Int = 1
    override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

    override def commandHandlers = cart => {
      case c: CreateShoppingCart => CommandSuccess(ShoppingCartCreated(c.name))
      case c: AddItem =>
        if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
        else CommandSuccess(ItemAdded(c.name))
    }

    override def eventHandlers = (userId, timestamp, cart) => {
      case e: ShoppingCartCreated => ShoppingCart(e.name, Vector.empty)
      case e: ItemAdded => cart.copy(items = cart.items :+ e.name)
    }
  }
}

// README.md's quick start, compiled and run, so it cannot drift from the code. The spec's database and actor system
// stand in for the README's connection pool and `ActorSystem("shop")`.
class ReadmeQuickStartSpec extends CommonSpec {

  TestDatabase.init()

  Feature("The README's quick start") {

    Scenario("creates a cart and adds an item to it") {
      import ReadmeQuickStart._

      val mpjsons = new MPJsons
      val typesNames = new PostgresTypesNamesState().initSchema()
      val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
      val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

      val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system))
      val eventBus = system.actorOf(Props(new EventsBusActor()))
      val carts = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus))
      eventBus ! EventsBusActor.RegisterSubscribers(Nil) // the projections and sagas, once you have some
      val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)))

      implicit val timeout: Timeout = Timeout(70.seconds)
      val alice = UserId(1)

      def send(command: AnyRef): CustomCommandResponse[_] =
        Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)

      val added = send(CreateShoppingCart(alice, "Groceries")) match {
        case SuccessResponse(cartId, version) =>          // SuccessResponse(Id(1001),Version(1))
          send(AddItem(alice, cartId, version, "apples")) // SuccessResponse(Id(1001),Version(2))
        case refused => fail("Refused: " + refused)
      }

      added.asInstanceOf[SuccessResponse].aggregateVersion mustBe AggregateVersion(2)
      Seq(outboxPoller, carts, eventBus, uidGenerator).foreach(system.stop)
    }
  }
}
