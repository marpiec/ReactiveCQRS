package io.reactivecqrs.examples.e3

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateVersion, CustomCommandResponse, SuccessResponse}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{FailedSubscriptionsHandled, GetProjectionStatus, PostgresSubscriptionsState, ProjectionActorOptions, ProjectionStatus, SkipFailedVersion}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.core.util.{Backoff, BlockingDispatcher}
import io.reactivecqrs.examples.e1.{AddItem, BasicSetup, CreateShoppingCart, ShoppingCartContext}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * E3, projections (chapters 8 and 9): one read model filled by the three kinds of listener, a delayed query that reads
  * its own write, and a listener that keeps failing on one cart, which the projection records, shows in its status and
  * lets you skip. Run it with `sbt "examples/runMain io.reactivecqrs.examples.e3.ProjectionsSetup"`; `ProjectionsSpec`
  * runs the same code against the test database.
  */
object ProjectionsSetup {

  final case class Shop(carts: ActorRef, activity: ActorRef)

  def start(system: ActorSystem, retries: Backoff = ProjectionActorOptions.defaultRetries): Shop = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val carts = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    // #start
    val store = new PostgresDocumentStore[CartActivity]("e3_cart_activity", mpjsons, new NoopDocumentStoreCache)
    val activity = system.actorOf(Props(new CartActivityProjection(subscriptions, store,
      ProjectionActorOptions.DEFAULT.withRetries(retries))).withDispatcher(BlockingDispatcher.id), "cartActivity")
    eventBus ! EventsBusActor.RegisterSubscribers(List(activity))
    // #start
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Shop(carts, activity)
  }

  /** A cart with its items; its id and version. */
  def cart(shop: Shop, user: UserId, name: String, items: String*)(implicit timeout: Timeout): (AggregateId, AggregateVersion) = {
    def send(command: AnyRef): SuccessResponse = Await.result((shop.carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    val created = send(CreateShoppingCart(user, name))
    val added = items.foldLeft(created) { (last, item) => send(AddItem(user, created.aggregateId, last.aggregateVersion, item)) }
    (created.aggregateId, added.aggregateVersion)
  }

  // #queries
  def activityAtLeast(shop: Shop, cartId: AggregateId, version: AggregateVersion)(implicit timeout: Timeout): Option[CartActivity] =
    Await.result((shop.activity ? GetCartActivityAtLeast(cartId, version)).mapTo[Option[CartActivity]], timeout.duration)

  def status(shop: Shop)(implicit timeout: Timeout): ProjectionStatus =
    Await.result((shop.activity ? GetProjectionStatus).mapTo[ProjectionStatus], timeout.duration)

  def skip(shop: Shop, cartId: AggregateId)(implicit timeout: Timeout): FailedSubscriptionsHandled =
    Await.result((shop.activity ? SkipFailedVersion(cartId)).mapTo[FailedSubscriptionsHandled], timeout.duration)
  // #queries

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e3")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val shop = start(system, Backoff(100.millis, 200.millis, maxAttempts = 3)) // a failure is recorded within a second
      val (cartId, version) = cart(shop, UserId(1), "Groceries", "apples", "pears")
      println("The read model has: " + activityAtLeast(shop, cartId, version))
      val (broken, _) = cart(shop, UserId(1), "Broken")
      Thread.sleep(2000)
      println("Failures: " + status(shop).failures.map(f => s"${f.aggregateId.asLong} at ${f.version.asInt}: ${f.error}"))
      println("Skipped: " + skip(shop, broken).count + "; failures left: " + status(shop).failures.size)
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
