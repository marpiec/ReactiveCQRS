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

  def storeEvents(events: Seq[Event[SomeAggregate]], id: AggregateId = aggregateId, exVersion: AggregateVersion = expectedVersion): Unit = {
    eventStoreState.persistEvents(id,
      PersistEvents(ActorRef.noSender, id, commandId, userId, exVersion, events))
    if(exVersion == expectedVersion) {
      expectedVersion = expectedVersion.incrementBy(events.length)
    }
  }

  def getEvents(version: Option[AggregateVersion] = None, id: AggregateId = aggregateId): Vector[Event[SomeAggregate]] = {
    var events = Vector[Event[SomeAggregate]]()
    eventStoreState.readAndProcessEvents[SomeAggregate](id, version)((event: Event[SomeAggregate], id: AggregateId, noop: Boolean) => {
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

    Given("Event store state")
    val f = new TestFixture
    import f._

    When("Adding events and undoing some")

    storeEvents(List(EventA("one"))) //1
    storeEvents(List(EventB(2))) //2
    storeEvents(List(EventC(false)))//3
    storeEvents(List(EventA("four")))//4
    storeEvents(List(Undo(1)))//5
    storeEvents(List(EventB(5)))//6
    storeEvents(List(EventC(true)))//7
    storeEvents(List(Undo(2)))//8
    storeEvents(List(EventA("seven")))//9

    Then("We can get all events in correct order")

    getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventA("seven"))

    Then("We can get correct events for different versions")
    getEvents(Some(AggregateVersion(8))) mustBe List(EventA("one"), EventB(2), EventC(false))

    getEvents(Some(AggregateVersion(7))) mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), EventC(true))

    getEvents(Some(AggregateVersion(4))) mustBe List(EventA("one"), EventB(2), EventC(false), EventA("four"))

  }

  scenario("Adding and reading events for duplicated aggregates") {

    Given("Event store state")
    val f = new TestFixture
    import f._

    val aggregateBId = AggregateId(System.nanoTime() + Math.random().toLong)

    When("Adding events and undoing some")

    storeEvents(List(EventA("one"))) //1
    storeEvents(List(EventB(2))) //2
    storeEvents(List(EventC(false)))//3
    storeEvents(List(EventA("four")))//4
    storeEvents(List(Undo(1)))//5
    storeEvents(List(EventB(5)))//6

    storeEvents(List(Copy(aggregateId, AggregateVersion(6))), aggregateBId, AggregateVersion(0))
    storeEvents(List(EventA("newSix")), aggregateBId, AggregateVersion(1))

    storeEvents(List(EventA("oldSix")))

    Then("We can get all events in correct order")

    getEvents() mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), EventA("oldSix"))
    getEvents(None, aggregateBId) mustBe List(EventA("one"), EventB(2), EventC(false), EventB(5), Copy(aggregateId, AggregateVersion(6)), EventA("newSix"))

  }

  //TODO test for undo and duplication

}
