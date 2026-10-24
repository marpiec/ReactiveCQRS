package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{PostgresEventStoreState, RestoreFailedAtEventException}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, LogCapture, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Try

// A tree of any depth, so a stored event can nest as deep as its JSON does: an aggregate of this spec only.
case class Tree(children: List[Tree])
case class Forest(tree: Tree)
case class ForestPlanted(tree: Tree) extends FirstEvent[Forest] {
  def spaceId: SpaceId = SpaceId(0)
}

class ForestContext extends AggregateContext[Forest] {
  override val version: Int = 1
  override def initialAggregateRoot: Forest = Forest(Tree(Nil))
  override def commandHandlers = _ => PartialFunction.empty
  override def eventHandlers = (_, _, _) => {
    case e: ForestPlanted => Forest(e.tree)
  }
}

// F10: an aggregate whose restore fails answers at once, and a later command retries the restore (Neula R4).
class FailedRestoreSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus,
    failedRestoreRetryAfter = 300.millis))

  private val userId = UserId(1)

  /** A cart written straight to the store, so no actor has loaded it yet. */
  private def storedCart(): AggregateId = {
    val cartId = AggregateId(TestIds.aggregate())
    eventStore.localTx { session =>
      eventStore.persistEvents(cartId,
        NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries"))))(session)
    }.get
    cartId
  }

  private val validCreation = mpjsons.serialize(ShoppingCartCreated("Groceries"), classOf[ShoppingCartCreated].getName)

  private def setStoredEvent(cartId: AggregateId, json: String): Unit = DB.autoCommit { implicit session =>
    sql"UPDATE events SET event = $json WHERE aggregate_id = ${cartId.asLong}".update()
  }

  Feature("An aggregate whose stored events cannot be read") {

    Scenario("commands and queries get an error at once instead of waiting for the timeout") {
      val cartId = storedCart()
      setStoredEvent(cartId, "not json")

      try {
        val commandResponse = commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(1), "apple"))
        commandResponse mustBe a[CommandHandlingError]

        val query = commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
        query.failed.get mustBe an[AggregateRestoreFailedException]
      } finally {
        setStoredEvent(cartId, validCreation) // later specs in this schema read every stored event
      }
    }

    Scenario("F191 the failure answered and logged carries none of the stored values") {
      val cartId = storedCart()
      val value = "marker" + System.nanoTime()
      setStoredEvent(cartId, "{\"name\":\"" + value + "\",")
      val capture = new LogCapture(system)
      try {
        val failure = commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).failed.get
        failure mustBe an[AggregateRestoreFailedException]
        val answered = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(_.toString).mkString(" ")
        answered must (include("JsonInnerException") and not include value)
        awaitAssert(capture.all.exists(_.contains("Restoring aggregate " + cartId.asLong)) mustBe true, 5.seconds, 50.millis)
        capture.containing(value) mustBe empty
      } finally {
        capture.close()
        setStoredEvent(cartId, validCreation)
      }
    }

    // D94: mpjsons 0.7.0 overflowed the stack on such input, which the owner could only report as "The stack overflowed"
    // (F250), naming no event; mpjsons 0.7.6 and later refuse it as an unreadable event.
    Scenario("an event nested deeper than the stack allows fails the restore as an unreadable event, naming it") {
      Given("a forest whose stored event nests its tree 100 000 levels deep")
      val forests = system.actorOf(AggregateCommandBusActor(new ForestContext, uidGenerator, eventStore,
        new PostgresCommandResponseState(mpjsons, typesNamesState), eventBus))
      val forestId = AggregateId(TestIds.aggregate())
      eventStore.localTx { session =>
        eventStore.persistEvents(forestId, NewEvents[Forest](userId, AggregateVersion.ZERO, Instant.now, Seq(ForestPlanted(Tree(Nil)))))(session)
      }.get
      val depth = 100000
      setStoredEvent(forestId, "{\"tree\":" + "{\"children\":[" * depth + "]}" * depth + "}")
      try {
        When("it is restored")
        val failure = forests.askFor[Try[Aggregate[Forest]]](GetAggregate(forestId)).failed.get

        Then("the restore failed at that event, which could not be read (CR-4), not on an overflowed stack")
        failure mustBe an[AggregateRestoreFailedException]
        // The answer crossed the test system's serialization, so its causes keep their class names, not their classes.
        val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(_.toString).mkString(" | ")
        causes must include(classOf[RestoreFailedAtEventException].getName + ": Reading " + classOf[ForestPlanted].getName +
          " at version 1 of aggregate " + forestId.asLong + " failed")
        causes must (include("JsonInnerException") and not include "The stack overflowed")
      } finally {
        setStoredEvent(forestId, mpjsons.serialize(ForestPlanted(Tree(Nil)), classOf[ForestPlanted].getName)) // later specs read every event
      }
    }

    Scenario("after the data is repaired, a later command restores the aggregate and succeeds") {
      Given("a cart whose restore failed")
      val cartId = storedCart()

      setStoredEvent(cartId, "not json")
      val failed = commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(1), "apple"))
      failed mustBe a[CommandHandlingError]

      When("the event is repaired and the retry window has passed")
      setStoredEvent(cartId, validCreation)

      Then("a command succeeds")
      awaitAssert({
        val response = commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(1), "apple"))
        response mustBe SuccessResponse(cartId, AggregateVersion(2))
      }, 5.seconds, 200.millis)
    }
  }
}
