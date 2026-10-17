package io.reactivecqrs.examples.e4

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{Aggregate, CustomCommandResponse, GetAggregate, SuccessResponse}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.saga.{PostgresSagaState, SagaResponse, SagaRunner}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.BasicSetup
import io.reactivecqrs.examples.e4.CheckoutSaga.Checkout
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * E4, a saga: a checkout that reserves items in a stock, then charges a payment account, and gives the items back when
  * the charge is refused (chapter 12). Run it with `sbt "examples/runMain io.reactivecqrs.examples.e4.SagaSetup"`;
  * `CheckoutSagaSpec` runs the same code against the test database.
  */
object SagaSetup {

  final case class Checkouts(stock: ActorRef, payments: ActorRef, saga: ActorRef)

  def start(system: ActorSystem): Checkouts = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val stock = system.actorOf(AggregateCommandBusActor(new StockContext, uidGenerator, eventStore, responses, eventBus), "stock")
    val payments = system.actorOf(AggregateCommandBusActor(new PaymentContext, uidGenerator, eventStore, responses, eventBus), "payments")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    // #runner
    val sagaState = new PostgresSagaState(mpjsons, typesNames)
    sagaState.initSchema()
    val saga = SagaRunner.start(system, new CheckoutSaga(stock, payments)(system.dispatcher, Timeout(60.seconds)),
      uidGenerator, sagaState)
    // #runner
    Checkouts(stock, payments, saga)
  }

  /** Opens a stock and a payment account, and checks out: the saga's answer, and the items the stock has left once any
    * revert has run. */
  def checkout(shop: Checkouts, user: UserId, items: Int, limitCents: Long, amountCents: Long)(implicit timeout: Timeout): (SagaResponse, AggregateId) = {
    def created(bus: ActorRef, command: AnyRef): AggregateId = Await.result((bus ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case SuccessResponse(id, _) => id
      case refused => throw new IllegalStateException("Not created: " + refused)
    }
    val stockId = created(shop.stock, OpenStock(user, available = 10))
    val paymentId = created(shop.payments, OpenPayment(user, limitCents))
    // #order
    val answer = Await.result((shop.saga ? Checkout(user, stockId, paymentId, items, amountCents)).mapTo[SagaResponse], 2.minutes)
    // #order
    (answer, stockId)
  }

  def available(shop: Checkouts, stockId: AggregateId)(implicit timeout: Timeout): Int =
    Await.result((shop.stock ? GetAggregate(stockId)).mapTo[Try[Aggregate[Stock]]], timeout.duration).get.aggregateRoot.get.available

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e4")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val shop = start(system)
      val (paid, _) = checkout(shop, UserId(1), items = 3, limitCents = 10000, amountCents = 2500)
      println("Within the limit: " + paid) // CheckedOut(...)
      val (refused, stockId) = checkout(shop, UserId(1), items = 3, limitCents = 1000, amountCents = 2500)
      Thread.sleep(1000) // the revert runs after the answer
      println("Over the limit: " + refused + ", items left: " + available(shop, stockId)) // CheckoutRefused(...), 10
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
