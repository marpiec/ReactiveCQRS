package io.reactivecqrs.testdomain.spec

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{MemoryDocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus._
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.eventsreplayer.{EventsReplayerActor, ReplayerConfig, ReplayerRepositoryActorFactory}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, PostgresVersionsState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCart, ShoppingCartAggregateContext, ShoppingCartsListProjectionAggregatesBased, ShoppingCartsListProjectionEventsBased}
import io.reactivecqrs.testutils.CommonSpec
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.duration._

class EventsReplaySpec extends CommonSpec {

  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)



  def Fixture = new {

    val system = ActorSystem("main-actor-system")

    val mpjsons = new MPJsons
    val typesTypesState = new PostgresTypesNamesState().initSchema()
    val eventStoreState = new PostgresEventStoreState(mpjsons, typesTypesState) // or MemoryEventStore
    eventStoreState.initSchema()


    val eventBusSubscriptionsManager = new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(0))))
    val subscriptionState = new PostgresSubscriptionsState(typesTypesState, true)
    val versionsState = new PostgresVersionsState()
    subscriptionState.initSchema()

    val inMemory = false

    val typesNamesState = new PostgresTypesNamesState().initSchema()


    private val eventBusState = if(inMemory) {
      new MemoryEventBusState
    } else {
      new PostgresEventBusState().initSchema()
    }

    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState, eventBusSubscriptionsManager)), "eventBus")

    val commandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
    val aggregatesUidGenerator = new PostgresUidGenerator("aggregates_uids_seq") // or MemoryUidGenerator
    val commandsUidGenerator = new PostgresUidGenerator("commands_uids_seq") // or MemoryUidGenerator
    val sagasUidGenerator = new PostgresUidGenerator("sagas_uids_seq") // or MemoryUidGenerator
    val uidGenerator = system.actorOf(Props(new UidGeneratorActor(aggregatesUidGenerator, commandsUidGenerator, sagasUidGenerator)), "uidGenerator")
    val shoppingCartContext = new ShoppingCartAggregateContext
    val shoppingCartCommandBus: ActorRef = system.actorOf(
      AggregateCommandBusActor(shoppingCartContext, uidGenerator, eventStoreState, commandResponseState, eventBusActor, false), "ShoppingCartCommandBus")

    private val storeA = if(inMemory) {
      new MemoryDocumentStore[String]
    } else {
      new PostgresDocumentStore[String]("storeA", mpjsons, new NoopDocumentStoreCache)
    }
    private val storeB = if(inMemory) {
      new MemoryDocumentStore[String]
    } else {
      new PostgresDocumentStore[String]("storeB", mpjsons, new NoopDocumentStoreCache)
    }

    val shoppingCartsListProjectionEventsBased = system.actorOf(Props(new ShoppingCartsListProjectionEventsBased(eventBusSubscriptionsManager, subscriptionState, shoppingCartCommandBus, storeA)), "ShoppingCartsListProjectionEventsBased")
    val shoppingCartsListProjectionAggregatesBased = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(eventBusSubscriptionsManager, subscriptionState, storeB)), "ShoppingCartsListProjectionAggregatesBased")



    val replayerActor = system.actorOf(Props(new EventsReplayerActor(eventStoreState, eventBusActor, subscriptionState, ReplayerConfig(), List(
      ReplayerRepositoryActorFactory(new ShoppingCartAggregateContext)
    ))))

    Thread.sleep(100) // Wait until all subscriptions in place

  }

  feature("System is able to recreate projections from historical events") {
    scenario("Ability to replay events") {

      val fixture = Fixture
      import fixture._

      val start = System.nanoTime() / 1_000_000
      val result: EventsReplayed = replayerActor.askActor[EventsReplayed](ReplayAllEvents(false, Seq(AggregateType(classOf[ShoppingCart].getName)), 50))(50.seconds)

      println(result+" in "+(System.nanoTime() / 1_000_000 - start)+"mills")

      Thread.sleep(20000)

    }
  }

}
