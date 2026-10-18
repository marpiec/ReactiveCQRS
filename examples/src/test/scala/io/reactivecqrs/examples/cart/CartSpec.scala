package io.reactivecqrs.examples.cart

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.{EventStoreState, PostgresEventStoreState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCart, ShoppingCartAggregateContext}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

/** The specs of the chapters whose examples use the test domain's cart (5, 6, 7, 13, 14 and 15): its carts wired as in
  * chapter 4, on the test database. Each spec has its chapter's blocks as regions, each followed by the lines that
  * check what the chapter says it answers. */
abstract class CartSpec extends CommonSpec with BeforeAndAfterAll {

  TestDatabase.init()

  protected val mpjsons = new MPJsons
  protected val typesNames = new PostgresTypesNamesState().initSchema()
  protected val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
  protected val responseState = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
  protected val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")

  /** A command bus of carts run by `context`, with an event bus of its own: a bus has one publisher per type. */
  protected def carts(context: AggregateContext[ShoppingCart], name: String, store: EventStoreState = eventStore): ActorRef = {
    val eventBus = system.actorOf(Props(new EventsBusActor()), name + "Events")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(AggregateCommandBusActor(context, uidGenerator, store, responseState, eventBus), name)
  }

  /** Chapter 4's command bus, here for the test domain's carts. */
  protected val commandBus: ActorRef = carts(new ShoppingCartAggregateContext, "carts")

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}
