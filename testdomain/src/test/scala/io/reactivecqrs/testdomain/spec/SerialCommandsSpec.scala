package io.reactivecqrs.testdomain.spec

import io.reactivecqrs.api.CommandProcessing.Serial
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.spec.AggregateOwnerSpec._
import io.reactivecqrs.testutils.CommonSpec
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// R8, D31: serial command processing.
class SerialCommandsSpec extends CommonSpec {

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)

  private def busOf(context: AggregateContext[_]): ActorRef = context match {
    case carts: ShoppingCartAggregateContext => system.actorOf(AggregateCommandBusActor(carts, uidGenerator, new MemoryEventStoreState,
      new MemoryCommandResponseState, eventBus))
    case tallies: TallyContext => system.actorOf(AggregateCommandBusActor(tallies, uidGenerator, new MemoryEventStoreState,
      new MemoryCommandResponseState, eventBus))
  }

  private class SerialCarts extends ShoppingCartAggregateContext {
    override val commandProcessing: CommandProcessing = Serial()
  }
  private class SerialTallies(maxQueue: Int) extends TallyContext {
    override val commandProcessing: CommandProcessing = Serial(maxQueuePerAggregate = maxQueue)
  }

  private val userId = UserId(1)
  implicit private val timeout: Timeout = Timeout(30.seconds)
  import system.dispatcher

  private def created(bus: ActorRef): AggregateId = {
    val response: CustomCommandResponse[_] = bus ?? CreateShoppingCartWithId(None, userId, Await.result((bus ? ReserveAggregateId).mapTo[AggregateId], 5.seconds), "Groceries")
    response.asInstanceOf[SuccessResponse].aggregateId
  }

  private def thousandAtOnce(bus: ActorRef, cartId: AggregateId): Seq[Any] =
    Await.result(Future.sequence((1 to 1000).map(i => bus ? AddItemConcurrently(userId, cartId, "item " + i))), 60.seconds)

  Feature("One command at a time per aggregate (R8)") {

    Scenario("1 000 commands sent at once to one aggregate give 1 000 versions in a row, with no conflict") {
      val bus = busOf(new SerialCarts)
      val cartId = created(bus)

      val responses = thousandAtOnce(bus, cartId)

      responses.collect { case e: AggregateConcurrentModificationError => e } mustBe empty
      responses.collect { case SuccessResponse(_, version) => version.asInt }.sorted mustBe (2 to 1001)
      val cart: Try[Aggregate[ShoppingCart]] = bus ?? GetAggregate(cartId)
      cart.get.version mustBe AggregateVersion(1001)
      cart.get.aggregateRoot.get.items.size mustBe 1000
    }

    Scenario("in concurrent mode the same load ends without conflicts too: the commands that conflict run again in turn (F99)") {
      val bus = busOf(new ShoppingCartAggregateContext)
      val cartId = created(bus)

      val responses = thousandAtOnce(bus, cartId)

      responses.collect { case e: AggregateConcurrentModificationError => e } mustBe empty
      responses.collect { case SuccessResponse(_, version) => version.asInt }.sorted mustBe (2 to 1001)
    }

    Scenario("a command beyond the queue limit is rejected at once") {
      val bus = busOf(new SerialTallies(maxQueue = 3))
      val started: CustomCommandResponse[_] = bus ?? StartTally(userId)
      val id = started.asInstanceOf[SuccessResponse].aggregateId

      val running = bus ? CountWhenReleased(userId, id, "queue limit")
      val waiting = (1 to 3).map(_ => bus ? CountWhenReleased(userId, id, "queue limit"))
      Thread.sleep(200)
      val beyond: CustomCommandResponse[_] = bus ?? CountWhenReleased(userId, id, "queue limit")
      beyond mustBe a[CommandRejected]

      release("queue limit")
      (running +: waiting).map(Await.result(_, 10.seconds)).collect { case SuccessResponse(_, version) => version.asInt }.sorted mustBe (2 to 5)
    }

    Scenario("a command's timeout counts from its arrival, also the time it waited in the queue") {
      val bus = busOf(new SerialTallies(maxQueue = 10)) // a timeout of 1 s
      val started: CustomCommandResponse[_] = bus ?? StartTally(userId)
      val id = started.asInstanceOf[SuccessResponse].aggregateId

      val first = bus ? CountWhenReleased(userId, id, "never released")
      Thread.sleep(50)
      val sentAt = System.nanoTime()
      val second = bus ? CountWhenReleased(userId, id, "never released") // starts only when the first timed out

      Await.result(second, 5.seconds) mustBe a[CommandHandlingError]
      (System.nanoTime() - sentAt).nanos must be < 1700.millis // not 1 s after it started
      Await.result(first, 5.seconds) mustBe a[CommandHandlingError]
    }

    Scenario("a copy of an idempotent command that waited behind it gets the stored response") {
      val bus = busOf(new SerialCarts)
      val cartId = created(bus)
      val key = Some(ClientKey("serial copy " + System.nanoTime()))

      val copies = Await.result(Future.sequence((1 to 2).map(_ => bus ? AddItemConcurrently(userId, cartId, "once", key))), 10.seconds)

      copies.distinct mustBe Seq(SuccessResponse(cartId, AggregateVersion(2)))
      val cart: Try[Aggregate[ShoppingCart]] = bus ?? GetAggregate(cartId)
      cart.get.version mustBe AggregateVersion(2)
    }
  }
}
