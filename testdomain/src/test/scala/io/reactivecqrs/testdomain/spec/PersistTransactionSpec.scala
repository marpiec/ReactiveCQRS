package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.commandhandler.{CommandExecutorActor, Idempotency, MemoryCommandResponseState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

// F68: the events of one command are stored together or not at all.
class PersistTransactionSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  // Undo events may be at most one character long here, so the second event of a command fails its size check
  // after the first one was already written.
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState,
    eventSizeLimitPerClassName = Map(classOf[ShoppingCartChangesUndone].getName -> 1)).initSchema()
  private val context = new ShoppingCartAggregateContext

  private val userId = UserId(1)

  private def storedCart(): AggregateId = {
    val cartId = AggregateId(System.nanoTime())
    eventStore.localTx { session =>
      eventStore.persistEvents(Map.empty, cartId,
        PersistEvents[ShoppingCart](ActorRef.noSender, CommandId(0), userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries")), None))(session)
    }.get
    cartId
  }

  private def repositoryOf(cartId: AggregateId): ActorRef = system.actorOf(Props(new AggregateRepositoryActor[ShoppingCart](
    cartId, eventStore, new Idempotency(new MemoryCommandResponseState), TestProbe().ref, context.eventHandlers, () => context.initialAggregateRoot,
    None, Map.empty, Map.empty, 5.seconds)))

  private def storedEvents(cartId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single().get
  }

  Feature("A command whose events cannot all be stored") {

    Scenario("stores none of them, and the aggregate carries on from its previous version") {
      val cartId = storedCart()
      val repository = repositoryOf(cartId)
      val executor = TestProbe()

      When("the second of two events fails")
      repository ! PersistEvents[ShoppingCart](executor.ref, CommandId(1), userId, AggregateVersion(1), Instant.now,
        Seq(ItemAdded("apple"), ShoppingCartChangesUndone(1)), None)

      Then("the command fails and the first event is not stored either")
      executor.expectMsgType[EventHandlingError](10.seconds)
      storedEvents(cartId) mustBe 1

      And("the next command, expecting version 1, is stored")
      repository ! PersistEvents[ShoppingCart](executor.ref, CommandId(2), userId, AggregateVersion(1), Instant.now, Seq(ItemAdded("pear")), None)
      executor.expectMsg(10.seconds, CommandExecutorActor.AggregateModified)
      storedEvents(cartId) mustBe 2
    }
  }
}
