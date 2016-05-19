package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorSystem, Props}
import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.core.documentstore.{MemoryDocumentStore, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.{EventsBusActor, MemoryEventBusState}
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.eventsreplayer.{EventsReplayerActor, ReplayerRepositoryActorFactory}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCartAggregateContext, ShoppingCartsListProjectionAggregatesBased, ShoppingCartsListProjectionEventsBased}
import io.reactivecqrs.testutils.CommonSpec
import org.apache.commons.dbcp.BasicDataSource

class EventsReplaySpec extends CommonSpec {

  def Fixture = new {

    val system = ActorSystem("main-actor-system")

    val mpjsons = new MPJsons
    val eventStoreState = new PostgresEventStoreState(mpjsons) // or MemoryEventStore
    eventStoreState.initSchema()


    val eventBusState = new MemoryEventBusState

    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState)), "eventBus")

    val replayerActor = system.actorOf(Props(new EventsReplayerActor(eventStoreState, eventBusActor, List(
      ReplayerRepositoryActorFactory(new ShoppingCartAggregateContext)
    ))))

    val dataSource = new BasicDataSource()
    dataSource.setUsername("reactivecqrs")
    dataSource.setPassword("reactivecqrs")
    dataSource.setDriverClassName("org.postgresql.Driver")
    dataSource.setUrl("jdbc:postgresql://localhost:5432/reactivecqrs")
    dataSource.setInitialSize(5)

    val inMemory = false

    private val storeA = if(inMemory) {
      new MemoryDocumentStore[String, AggregateVersion]
    } else {
      new PostgresDocumentStore[String, AggregateVersion]("storeA", dataSource, mpjsons)
    }
    private val storeB = if(inMemory) {
      new MemoryDocumentStore[String, AggregateVersion]
    } else {
      new PostgresDocumentStore[String, AggregateVersion]("storeB", dataSource, mpjsons)
    }

    val shoppingCartsListProjectionEventsBased = system.actorOf(Props(new ShoppingCartsListProjectionEventsBased(eventBusActor, null, storeA)), "ShoppingCartsListProjectionEventsBased")
    val shoppingCartsListProjectionAggregatesBased = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(eventBusActor, storeB)), "ShoppingCartsListProjectionAggregatesBased")

    Thread.sleep(100) // Wait until all subscriptions in place

  }

  feature("System is able to recreate projections from historical events") {
    scenario("Ability to replay events") {

      val fixture = Fixture
      import fixture._

      Thread.sleep(500)

      val result: EventsReplayed = replayerActor ?? ReplayAllEvents(10)

      println("------------------------------------------------------"+result)

      Thread.sleep(50000)
    }
  }

}
