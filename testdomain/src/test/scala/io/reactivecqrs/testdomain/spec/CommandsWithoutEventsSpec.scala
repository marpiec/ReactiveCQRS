package io.reactivecqrs.testdomain.spec

import java.util.concurrent.ConcurrentLinkedQueue

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManager, EventBusSubscriptionsManagerApi, EventsBusActor, MemoryEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

// R9: a command whose handler returns no events leaves no trace: no statement about the aggregate, no new version,
// and the caller still gets its reply.
class CommandsWithoutEventsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor(new MemoryEventBusState,
    new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(0)))))))
  private val commandBus = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus, eventsReplayMode = false))

  private val userId = UserId(1)

  private def storedEvents(cartId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single().get
  }

  private def waitingForPublication(cartId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events_to_publish WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single().get
  }

  Feature("Commands that produce no events") {

    Scenario("10 000 of them run no statement about the aggregate and leave its version unchanged") {
      Given("a cart that is loaded in its repository actor and whose creation was published")
      val created: SuccessResponse = commandBus ?? CreateShoppingCart(None, userId, "Checked cart")
      val cartId = created.aggregateId
      val loaded: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      loaded.get.version mustBe AggregateVersion(1)
      awaitAssert(waitingForPublication(cartId) mustBe 0, 10.seconds, 50.millis) // its outbox row is deleted asynchronously
      val eventsBefore = storedEvents(cartId)

      When("10 000 checks are sent, 100 at a time")
      // Statements whose parameters name this cart; other actors in this JVM may run unrelated statements.
      val statementsAboutCart = new ConcurrentLinkedQueue[String]()
      GlobalSettings.queryCompletionListener = (sql, params, _) => if (params.contains(cartId.asLong)) statementsAboutCart.add(sql)
      val responses = try {
        import system.dispatcher
        implicit val timeout: Timeout = Timeout(30.seconds)
        (1 to 100).flatMap { _ =>
          Await.result(Future.sequence((1 to 100).map(_ => (commandBus ? CheckCart(userId, cartId)).mapTo[CustomCommandResponse[_]])), 60.seconds)
        }
      } finally {
        GlobalSettings.queryCompletionListener = (_, _, _) => ()
      }

      Then("every caller got the cart's current version, and nothing was written")
      responses.size mustBe 10000
      responses.distinct mustBe Seq(SuccessResponse(cartId, AggregateVersion(1)))
      statementsAboutCart.asScala.toList mustBe Nil
      storedEvents(cartId) mustBe eventsBefore
      val after: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      after.get.version mustBe AggregateVersion(1)
    }
  }
}
