package io.reactivecqrs.testdomain.spec

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
import org.apache.pekko.actor.{ActorIdentity, ActorRef, Identify, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.Promise
import scala.concurrent.duration._

case class Sluggish()

case class StartSluggish(userId: UserId) extends FirstCommand[Sluggish, CustomCommandResponse[_]]

/** An aggregate whose command handler never completes, with a short command timeout. */
class SluggishAggregateContext extends AggregateContext[Sluggish] {
  override val version: Int = 1
  override val commandTimeout: FiniteDuration = 300.millis
  override def initialAggregateRoot: Sluggish = Sluggish()
  override def commandHandlers = _ => {
    case _: StartSluggish => AsyncCommandResult(Promise[CustomCommandResult[Nothing]]().future)
  }
  override def eventHandlers = (_, _, root) => {
    case _ => root
  }
}

// F13, F20, F67: every command execution ends with a reply to the caller, and its executor stops.
class CommandOutcomesSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val commandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor(new MemoryEventBusState,
    new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(0)))))))

  private val cartBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    commandResponseState, eventBus, eventsReplayMode = false), "CommandOutcomesCartBus")
  private val sluggishBus: ActorRef = system.actorOf(AggregateCommandBusActor(new SluggishAggregateContext, uidGenerator, eventStore,
    commandResponseState, eventBus, eventsReplayMode = false), "CommandOutcomesSluggishBus")

  private val userId = UserId(1)

  /** Live command executors: children of the command handlers under a command bus. */
  private def liveExecutors(busName: String): Int = {
    val probe = TestProbe()
    system.actorSelection(s"/user/$busName/*/*").tell(Identify(busName), probe.ref)
    probe.receiveWhile(500.millis) { case ActorIdentity(_, Some(ref)) => ref }.size
  }

  Feature("A command handler that reports a failure") {
    Scenario("the caller gets the failure and the executor stops") {
      val response: CustomCommandResponse[_] = cartBus ?? CreateShoppingCart(None, userId, "Cart M 4")

      response mustBe FailureResponse(List("Cannot add cart *M 4"))
      awaitAssert(liveExecutors("CommandOutcomesCartBus") mustBe 0, 3.seconds, 200.millis)
    }
  }

  Feature("A command handler that never completes") {
    Scenario("the caller gets an error after the context's command timeout and the executor stops") {
      val response: CustomCommandResponse[_] = sluggishBus ?? StartSluggish(userId)

      response mustBe a[CommandHandlingError]
      awaitAssert(liveExecutors("CommandOutcomesSluggishBus") mustBe 0, 3.seconds, 200.millis)
    }
  }

  Feature("A command for an aggregate that does not exist") {
    Scenario("the caller gets a concurrent-modification error at once instead of waiting for the timeout (F67)") {
      val missing = AggregateId(System.nanoTime())

      val response: CustomCommandResponse[_] = cartBus ?? AddItem(userId, missing, AggregateVersion(1), "apple")

      response mustBe AggregateConcurrentModificationError(missing, AggregateType(classOf[ShoppingCart].getName), AggregateVersion(1), AggregateVersion.ZERO)
    }
  }
}
