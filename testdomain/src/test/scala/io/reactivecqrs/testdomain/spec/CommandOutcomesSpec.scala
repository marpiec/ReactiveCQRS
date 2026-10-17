package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{IdsPool, PostgresUidGenerator, UidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase, TestIds}
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

case class ThreadsProbe()
case class StartThreadsProbe(userId: UserId) extends FirstCommand[ThreadsProbe, CustomCommandResponse[_]]
case class ThreadsProbeStarted() extends FirstEvent[ThreadsProbe] {
  def spaceId: SpaceId = SpaceId(0)
}

/** Records the threads its command handler and its event handler run on (F27). */
class ThreadsProbeContext extends AggregateContext[ThreadsProbe] {
  val commandHandlerThread = new java.util.concurrent.atomic.AtomicReference[String]()
  val eventHandlerThread = new java.util.concurrent.atomic.AtomicReference[String]()
  override val version: Int = 1
  override def initialAggregateRoot: ThreadsProbe = ThreadsProbe()
  override def commandHandlers = _ => {
    case _: StartThreadsProbe =>
      commandHandlerThread.set(Thread.currentThread.getName)
      CommandSuccess(ThreadsProbeStarted())
  }
  override def eventHandlers = (_, _, root) => {
    case _: ThreadsProbeStarted =>
      eventHandlerThread.set(Thread.currentThread.getName)
      root
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
    commandResponseState, eventBus), "CommandOutcomesCartBus")
  private val sluggishBus: ActorRef = system.actorOf(AggregateCommandBusActor(new SluggishAggregateContext, uidGenerator, eventStore,
    commandResponseState, eventBus), "CommandOutcomesSluggishBus")

  // Its aggregate and command ids run out after one command, and the generators then fail.
  private val idlessBus: ActorRef = system.actorOf(AggregateCommandBusActor(new SluggishAggregateContext,
    system.actorOf(Props(new UidGeneratorActor(new OnePoolUidGenerator(1), new OnePoolUidGenerator(1), new PostgresUidGenerator("sagas_uids_seq")))),
    eventStore, commandResponseState, eventBus), "CommandOutcomesIdlessBus")

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
      val missing = AggregateId(TestIds.aggregate())

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

  Feature("Where the work runs (F27, F127)") {
    Scenario("an owner persists, and applies events, on the blocking dispatcher; command handlers run on the handlers' one") {
      val probe = new ThreadsProbeContext
      val bus = system.actorOf(AggregateCommandBusActor(probe, uidGenerator, eventStore, commandResponseState, eventBus))
      val started: CustomCommandResponse[_] = bus ?? StartThreadsProbe(userId)
      started mustBe a[SuccessResponse]

      probe.eventHandlerThread.get must include("reactivecqrs.blocking-dispatcher")
      probe.commandHandlerThread.get must include("reactivecqrs.handler-dispatcher")
    }
  }

  Feature("A command bus whose id generator is slow (F23)") {
    Scenario("keeps answering other messages while a command waits for ids, and runs it when they come") {
      implicit val timeout: Timeout = Timeout(10.seconds)
      val commandIds = new GatedUidGenerator(poolSize = 2)
      val bus = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext,
        system.actorOf(Props(new UidGeneratorActor(new PostgresUidGenerator("aggregates_uids_seq"), commandIds, new PostgresUidGenerator("sagas_uids_seq")))),
        eventStore, commandResponseState, eventBus))
      val cartId = Await.result((bus ? ReserveAggregateId).mapTo[AggregateId], 5.seconds)

      Given("a cart made with the two ids of the first pool; the next pool is held back")
      Await.result(bus ? CreateShoppingCartWithId(None, userId, cartId, "Groceries"), 5.seconds) mustBe SuccessResponse(cartId, AggregateVersion(1))
      Await.result(bus ? AddItem(userId, cartId, AggregateVersion(1), "apple"), 5.seconds) mustBe SuccessResponse(cartId, AggregateVersion(2))

      When("a command needs an id")
      val waiting = bus ? AddItem(userId, cartId, AggregateVersion(2), "pear")

      Then("a query is answered meanwhile, and the command waits")
      Await.result((bus ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]], 1.second).get.version mustBe AggregateVersion(2)
      waiting.isCompleted mustBe false

      And("it runs once the pool comes")
      commandIds.release()
      Await.result(waiting, 5.seconds) mustBe SuccessResponse(cartId, AggregateVersion(3))
    }

    Scenario("a pool asked for ahead that does not come fails no command while ids of its kind are still at hand") {
      implicit val timeout: Timeout = Timeout(10.seconds)
      val aggregateIds = new GatedUidGenerator(poolSize = 1)
      val bus = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext,
        system.actorOf(Props(new UidGeneratorActor(aggregateIds, new OnePoolUidGenerator(10), new PostgresUidGenerator("sagas_uids_seq")))),
        eventStore, commandResponseState, eventBus))

      Given("a cart made with the only aggregate id at hand, and commands that leave two of the ten command ids")
      val cartId = Await.result(bus ? CreateShoppingCart(None, userId, "Groceries"), 5.seconds).asInstanceOf[SuccessResponse].aggregateId
      (1 to 7).foreach(_ => Await.result(bus ? CheckCart(userId, cartId), 5.seconds) mustBe a[SuccessResponse])

      When("a new cart waits for aggregate ids, and the pool of command ids asked for ahead fails")
      val second = bus ? CreateShoppingCart(None, userId, "Second")

      Then("it keeps waiting, and runs once the aggregate ids come")
      Try(Await.result(second, 1.second)).isFailure mustBe true // not answered, so not failed
      aggregateIds.release()
      Await.result(second, 5.seconds) mustBe a[SuccessResponse]
    }
  }
}

/** Hands out its first pool at once; every later one only after `release`, like a slow sequence. */
class GatedUidGenerator(poolSize: Long) extends UidGenerator {
  private val gate = new java.util.concurrent.CountDownLatch(1)
  private val pools = new java.util.concurrent.atomic.AtomicInteger(0)
  private val next = new java.util.concurrent.atomic.AtomicLong(System.nanoTime())
  def release(): Unit = gate.countDown()
  override def nextIdsPool: IdsPool = {
    if (pools.getAndIncrement() > 0) gate.await(20, java.util.concurrent.TimeUnit.SECONDS)
    IdsPool(next.getAndAdd(poolSize), poolSize)
  }
}

/** Hands out one pool of ids, then fails, like a sequence that became unavailable. */
class OnePoolUidGenerator(size: Long) extends UidGenerator {
  private val handedOut = new java.util.concurrent.atomic.AtomicBoolean(false)
  override def nextIdsPool: IdsPool =
    if (handedOut.compareAndSet(false, true)) IdsPool(System.nanoTime(), size) else throw new IllegalStateException("sequence unavailable")
}
