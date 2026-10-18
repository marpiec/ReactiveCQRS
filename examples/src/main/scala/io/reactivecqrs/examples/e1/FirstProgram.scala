package io.reactivecqrs.examples.e1

// E1, chapter 4's program: chapter 3's cart wired up with no projections; a cart created, filled and read back. Run it
// with `sbt "examples/runMain io.reactivecqrs.examples.e1.FirstProgram"` against the database of chapter 2;
// `FirstProgramSpec` runs `run` against the test database.

// #program
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object FirstProgram {

  def main(args: Array[String]): Unit = {
    // #pool
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    // #pool
    // #system
    val system = ActorSystem("shop")
    // #system
    try run(system)
    finally {
      Await.result(system.terminate(), 30.seconds)
      ConnectionPool.closeAll()
    }
  }

  def run(system: ActorSystem): Unit = {
    // 1. The tables.
    // #tables
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    // #tables

    // 2. The actors.
    // #uids
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    // #uids
    // #eventBus
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    // #eventBus
    // #carts
    val carts = system.actorOf(
      AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    // #carts
    // #poller
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    // #poller

    // 3. Talking to the carts.
    implicit val timeout: Timeout = Timeout(70.seconds)
    val alice = UserId(1)

    def send(command: AnyRef): SuccessResponse =
      Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
        case success: SuccessResponse => success
        case other => throw new IllegalStateException(command.getClass.getSimpleName + " failed: " + other)
      }

    val created = send(CreateShoppingCart(alice, "Groceries"))
    println("Created: " + created)

    val withApples = send(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
    val withOranges = send(AddItem(alice, created.aggregateId, withApples.aggregateVersion, "oranges"))
    println("Now at: " + withOranges.aggregateVersion)

    val cart = Await.result((carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]],
      timeout.duration).get
    println("The cart: " + cart)
  }
}
// #program
