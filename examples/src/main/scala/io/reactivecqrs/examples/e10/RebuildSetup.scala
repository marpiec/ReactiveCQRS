package io.reactivecqrs.examples.e10

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{CustomCommandResponse, SuccessResponse}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, RebuildTarget}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{AggregatesRebuilt, Builds, GetBuilds, RebuildAggregates}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.{BasicSetup, CreateShoppingCart, ShoppingCartContext}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * E10, rebuilding projections (chapter 10): a managed projection of E1's carts' names, released at version 1, then at
  * version 2, which writes the names in capitals. Version 2 is built from the event store, in tables of its own, while
  * version 1 serves, and takes over in one update; then a cart's damaged document is repaired by a targeted rebuild. Each
  * release runs in an actor system of its own, as a redeploy starts the application again. Run it with `sbt
  * "examples/runMain io.reactivecqrs.examples.e10.RebuildSetup"`; `RebuildSpec` runs the same code against the test
  * database.
  */
object RebuildSetup {

  final case class Shop(carts: ActorRef, cartNames: ActorRef)

  /** The application of a release whose projection of cart names is at `version`. */
  def start(system: ActorSystem, version: Int): Shop = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val carts = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    // #start
    val registry = new PostgresProjectionRegistry().initSchema()
    val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry,
      contexts = Seq(new ShoppingCartContext)))

    val cartNames = projections.start[CartNames]("e10-cart-names", version, "cartNames") { build =>
      new CartNames(subscriptions,
        new PostgresDocumentStore[String](build.documentStore("e10_cart_names"), mpjsons, new NoopDocumentStoreCache),
        capitals = build.version >= 2)
    }
    eventBus ! EventsBusActor.RegisterSubscribers(List(cartNames)) // the supervisor, where the projection would be
    // #start
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Shop(carts, cartNames)
  }

  // #builds
  /** The builds once the projection's `version` serves alone, the switch to it done; asked every 200 ms. */
  @tailrec
  def switchedTo(shop: Shop, version: Int, deadline: Deadline)(implicit timeout: Timeout): Builds = {
    val builds = Await.result((shop.cartNames ? GetBuilds).mapTo[Builds], timeout.duration)
    if (builds.active.exists(_.version == version) && builds.building.isEmpty) builds
    else if (deadline.isOverdue()) throw new IllegalStateException(s"Version $version does not serve yet: $builds")
    else {
      Thread.sleep(200)
      switchedTo(shop, version, deadline)
    }
  }
  // #builds

  // #repair
  def repair(shop: Shop, cartId: AggregateId)(implicit timeout: Timeout): AggregatesRebuilt =
    Await.result(shop.cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId)), timeout.duration) match {
      case rebuilt: AggregatesRebuilt => rebuilt // AggregatesRebuilt(OneAggregate(cartId), 1)
      case refused => throw new IllegalStateException("Not rebuilt: " + refused)
    }
  // #repair

  /** A hand-written SQL fix gone wrong: a cart's document overwritten in the tables of the build that serves. */
  def damage(serving: Builds, cartId: AggregateId): Unit = {
    val table = "projection_e10_cart_names" + serving.active.get.suffix
    DB.autoCommit(implicit session => SQL(s"UPDATE $table SET document = to_jsonb('???'::text) WHERE id = ?").bind(cartId.asLong).update())
  }

  def create(shop: Shop, user: UserId, name: String)(implicit timeout: Timeout): AggregateId =
    Await.result((shop.carts ? CreateShoppingCart(user, name)).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case SuccessResponse(cartId, _) => cartId
      case refused => throw new IllegalStateException("Not created: " + refused)
    }

  def name(shop: Shop, cartId: AggregateId)(implicit timeout: Timeout): Option[String] =
    Await.result((shop.cartNames ? GetCartName(cartId)).mapTo[Option[String]], timeout.duration)

  /** What two releases showed: the names version 1 served, the builds once version 2 took over and the names it serves,
    * then a damaged cart's name, its targeted rebuild, and its name after. */
  final case class Release(before: Seq[Option[String]], switched: Builds, after: Seq[Option[String]], damaged: Option[String],
                           rebuilt: AggregatesRebuilt, repaired: Option[String])

  def release(newSystem: () => ActorSystem, user: UserId)(implicit timeout: Timeout): Release = {
    val (carts, before) = running(newSystem()) { system =>
      val shop = start(system, version = 1)
      val carts = Seq("Groceries", "Garden", "Gifts").map(create(shop, user, _))
      switchedTo(shop, version = 1, timeout.duration.fromNow)
      (carts, carts.map(name(shop, _)))
    }
    running(newSystem()) { system => // the application again, at version 2
      val shop = start(system, version = 2)
      val switched = switchedTo(shop, version = 2, timeout.duration.fromNow)
      val after = carts.map(name(shop, _))
      damage(switched, carts(1))
      val damaged = name(shop, carts(1))
      Release(before, switched, after, damaged, repair(shop, carts(1)), name(shop, carts(1)))
    }
  }

  private def running[T](system: ActorSystem)(work: ActorSystem => T): T =
    try work(system) finally Await.ready(system.terminate(), 30.seconds)

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    implicit val timeout: Timeout = Timeout(70.seconds)
    val release = RebuildSetup.release(() => ActorSystem("e10"), UserId(1))
    println("Version 1 served: " + release.before.flatten.mkString(", "))   // Groceries, Garden, Gifts
    println("Then: " + release.switched)                                     // Builds(e10-cart-names, Some(... version 2 ...), None, ...)
    println("Version 2 serves: " + release.after.flatten.mkString(", "))    // GROCERIES, GARDEN, GIFTS
    println("Damaged: " + release.damaged + "; " + release.rebuilt + "; repaired: " + release.repaired) // Some(???); ...; Some(GARDEN)
  }
}
