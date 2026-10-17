package io.reactivecqrs.examples.e1

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{CustomCommandResponse, SuccessResponse}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.core.util.BlockingDispatcher
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * E1, a basic single-node setup: the connection pool, the tables, the actors of one aggregate type and one projection,
  * then a command and a query that reads its own write. Run it with `sbt "examples/runMain
  * io.reactivecqrs.examples.e1.BasicSetup"` against the database of chapter 2 (`-Dreactivecqrs.example.url`, `.user`
  * and `.password` name another); `BasicSetupSpec` runs the same code against the test database.
  */
object BasicSetup {

  /** What the host keeps of the shop: where its commands and its questions go. */
  final case class Shop(carts: ActorRef, cartsList: ActorRef)

  // #pool
  def connect(url: String, user: String, password: String): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(url, user, password,
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
  }
  // #pool

  def start(system: ActorSystem): Shop = {
    // #wiring
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val carts = system.actorOf(
      AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    val cartsStore = new PostgresDocumentStore[CartSummary]("e1_carts_list", mpjsons, new NoopDocumentStoreCache)
    val cartsList = system.actorOf(
      Props(new CartsListProjection(subscriptions, cartsStore)).withDispatcher(BlockingDispatcher.id), "cartsList")
    eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList)) // every projection, saga and listener
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    // #wiring
    Shop(carts, cartsList)
  }

  // #commands
  def fillCart(shop: Shop, user: UserId)(implicit timeout: Timeout): Option[CartSummary] = {
    def send(command: AnyRef): CustomCommandResponse[_] =
      Await.result((shop.carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)

    send(CreateShoppingCart(user, "Groceries")) match {
      case SuccessResponse(cartId, version) =>   // the new cart's id, at version 1
        send(AddItem(user, cartId, version, "apples")) match {
          case SuccessResponse(_, added) =>        // version 2
            Await.result((shop.cartsList ? GetCartSummaryAtLeast(cartId, added)).mapTo[Option[CartSummary]], timeout.duration)
          case refused => throw new IllegalStateException("Not added: " + refused)
        }
      case refused => throw new IllegalStateException("Not created: " + refused)
    }
  }
  // #commands

  def main(args: Array[String]): Unit = {
    connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e1")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      println("The read model has: " + fillCart(start(system), UserId(1))) // Some(CartSummary(Groceries,1,2))
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
