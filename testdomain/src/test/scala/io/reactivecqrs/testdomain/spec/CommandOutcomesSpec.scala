package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{IdsPool, PostgresUidGenerator, UidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorIdentity, ActorRef, Identify, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.Try

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

// F13, F20, F67, F70, F74, F75: every command execution ends with a reply to the caller, and its executor stops.
class CommandOutcomesSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val commandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)

  private val cartBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    commandResponseState, eventBus, eventsReplayMode = false), "CommandOutcomesCartBus")
  private val sluggishBus: ActorRef = system.actorOf(AggregateCommandBusActor(new SluggishAggregateContext, uidGenerator, eventStore,
    commandResponseState, eventBus, eventsReplayMode = false), "CommandOutcomesSluggishBus")

  // Its aggregate and command ids run out after one command, and the generators then fail.
  private val idlessBus: ActorRef = system.actorOf(AggregateCommandBusActor(new SluggishAggregateContext,
    system.actorOf(Props(new UidGeneratorActor(new OnePoolUidGenerator(1), new OnePoolUidGenerator(1), new PostgresUidGenerator("sagas_uids_seq")))),
    eventStore, commandResponseState, eventBus, eventsReplayMode = false), "CommandOutcomesIdlessBus")

  private val userId = UserId(1)

  /** Live command executors: children of the command handlers under a command bus. */
  private def liveExecutors(busName: String): Int = liveActors(s"/user/$busName/*/*")

  private def liveActors(path: String): Int = {
    val probe = TestProbe()
    system.actorSelection(path).tell(Identify(path), probe.ref)
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

      response mustBe AggregateConcurrentModificationError(missing, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion.ZERO)
    }
  }

  Feature("A message the command bus does not understand") {
    Scenario("is answered with a failure, and commands in flight are not affected (F70)") {
      implicit val timeout: Timeout = Timeout(5.seconds)
      Given("a command in flight")
      val inFlight = sluggishBus ? StartSluggish(userId)

      When("the bus receives a message it does not understand")
      val unknown = Await.ready(sluggishBus ? "not a command", 10.seconds).value.get

      Then("the sender gets a failure")
      unknown.failed.get mustBe an[IllegalArgumentException]

      And("the command in flight still ends with its own reply")
      Await.result(inFlight, 10.seconds) mustBe a[CommandHandlingError]
    }
  }

  Feature("Queries for an earlier version") {
    Scenario("leave no temporary actor behind (F74)") {
      val created: SuccessResponse = cartBus ?? CreateShoppingCart(None, userId, "Cart with history")

      (1 to 3).foreach { _ =>
        val events: Try[List[EventInfo[ShoppingCart]]] = cartBus ?? GetEventsForAggregateForVersion(created.aggregateId, AggregateVersion(1))
        events.get.size mustBe 1
      }
      val aggregate: Try[Aggregate[ShoppingCart]] = cartBus ?? GetAggregateForVersion(created.aggregateId, AggregateVersion(1))
      aggregate.get.version mustBe AggregateVersion(1)

      awaitAssert(liveActors("/user/CommandOutcomesCartBus/*_ARV_*") mustBe 0, 3.seconds, 100.millis)
    }
  }

  Feature("A command bus whose id generator fails") {
    Scenario("answers the command with an error, and commands in flight are not affected (F75)") {
      implicit val timeout: Timeout = Timeout(5.seconds)
      Given("a command in flight, which took the last command id of the pool")
      val inFlight = idlessBus ? StartSluggish(userId)

      When("the next command needs a new pool and the generator fails")
      val next: CustomCommandResponse[_] = idlessBus ?? StartSluggish(userId)

      Then("it gets an error at once")
      next mustBe a[CommandHandlingError]

      And("the command in flight still ends with its own reply")
      Await.result(inFlight, 10.seconds) mustBe a[CommandHandlingError]

      And("reserving an aggregate id fails at once too")
      Await.ready(idlessBus ? ReserveAggregateId, 10.seconds).value.get.failed.get.getMessage mustBe "sequence unavailable"
    }
  }
}

/** Hands out one pool of ids, then fails, like a sequence that became unavailable. */
class OnePoolUidGenerator(size: Long) extends UidGenerator {
  private val handedOut = new java.util.concurrent.atomic.AtomicBoolean(false)
  override def nextIdsPool: IdsPool =
    if (handedOut.compareAndSet(false, true)) IdsPool(System.nanoTime(), size) else throw new IllegalStateException("sequence unavailable")
}
