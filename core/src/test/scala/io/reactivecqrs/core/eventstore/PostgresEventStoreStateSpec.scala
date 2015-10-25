package io.reactivecqrs.core.eventstore

import akka.actor.ActorRef
import io.mpjsons.MPJsons
import io.reactivecqrs.api.{DuplicationEvent, UndoEvent, Event, AggregateVersion}
import io.reactivecqrs.api.id.{UserId, CommandId, AggregateId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.testutils.CommonSpec
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

case class SomeAggregate()

case class EventA(text: String) extends Event[SomeAggregate]
case class EventB(number: Int) extends Event[SomeAggregate]
case class EventC(predicate: Boolean) extends Event[SomeAggregate]

case class Undo(eventsCount: Int) extends UndoEvent[SomeAggregate]

case class Copy(baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[SomeAggregate]


class TestFixture {
  val eventStoreState = new PostgresEventStoreState(new MPJsons)
  eventStoreState.initSchema()

  val aggregateId = AggregateId(System.nanoTime() + Math.random().toLong)
  val commandId = CommandId(101)
  val userId = UserId(201)
  var expectedVersion = AggregateVersion(0)

  def storeEvents(events: Seq[Event[SomeAggregate]]): Unit = {
    eventStoreState.persistEvents(aggregateId,
      PersistEvents(ActorRef.noSender, aggregateId, commandId, userId, expectedVersion, events))
    expectedVersion = expectedVersion.incrementBy(events.length)
  }

  def getEvents(): Vector[Event[SomeAggregate]] = {
    var events = Vector[Event[SomeAggregate]]()
    eventStoreState.readAndProcessEvents[SomeAggregate](aggregateId, None)((event: Event[SomeAggregate], id: AggregateId, noop: Boolean) => {
        if(!noop) {
          events :+= event
        }
      })
    events
  }
}


class PostgresEventStoreStateSpec extends CommonSpec {

  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

  feature("Can store and retrieve correct events for aggregate") {

    scenario("Simple adding and reading events") {

      Given("Event store state")

      val f = new TestFixture
      import f._


      When("Adding multiple events")

      storeEvents(List(EventA("one")))
      storeEvents(List(EventB(2)))
      storeEvents(List(EventC(false)))
      storeEvents(List(EventA("four")))
      storeEvents(List(EventB(5)))
      storeEvents(List(EventC(true)))
      storeEvents(List(EventA("seven")))

      Then("We can get all events in correct order")


      getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"), EventB(5), EventC(true), EventA("seven"))

    }


    scenario("Adding and reading events in batches") {

      Given("Event store state")

      val f = new TestFixture
      import f._

      When("Adding multiple events")

      storeEvents(List(EventA("one"), EventB(2), EventC(false), EventA("four")))
      storeEvents(List(EventB(5), EventC(true), EventA("seven")))

      Then("We can get all events in correct order")

      getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"), EventB(5), EventC(true), EventA("seven"))

    }
  }

  scenario("Adding and reading undo events") {

    Given("Event sotre state")
    val f = new TestFixture
    import f._

    When("Adding events and undoing some")

    storeEvents(List(EventA("one")))
    storeEvents(List(EventB(2)))
    storeEvents(List(EventC(false)))
    storeEvents(List(EventA("four")))
    storeEvents(List(Undo(1)))
    storeEvents(List(EventB(5)))
    storeEvents(List(EventC(true)))
    storeEvents(List(Undo(2)))
    storeEvents(List(EventA("seven")))

    Then("We can get all events in correct order")

    getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("seven"))
  }

}
