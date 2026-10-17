package io.reactivecqrs.testdomain.spec

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

object AggregateOwnerSpec {
  case class Tally(count: Int)

  case class TallyStarted() extends FirstEvent[Tally] {
    def spaceId: SpaceId = SpaceId(0)
  }
  case class Counted(by: Int) extends Event[Tally]
  case class TallyStartedIn(space: Long) extends FirstEvent[Tally] {
    def spaceId: SpaceId = SpaceId(space)
  }

  case class StartTally(userId: UserId) extends FirstCommand[Tally, CustomCommandResponse[_]]
  case class StartTallyIn(userId: UserId, space: Long, chosen: AggregateId) extends FirstCommand[Tally, CustomCommandResponse[_]] {
    override def chosenAggregateId: Option[AggregateId] = Some(chosen)
  }
  case class Count(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, times: Int)
    extends Command[Tally, CustomCommandResponse[_]]
  /** Counts once the test completes the promise registered under `key`. */
  case class CountWhenReleased(userId: UserId, aggregateId: AggregateId, key: String) extends ConcurrentCommand[Tally, CustomCommandResponse[_]]

  val releases = new ConcurrentHashMap[String, Promise[Unit]]()
  def release(key: String): Unit = releases.computeIfAbsent(key, _ => Promise[Unit]()).trySuccess(())

  /** How often the event handlers ran. */
  val handledEvents = new AtomicInteger()

  class TallyContext extends AggregateContext[Tally] {
    override val version = 1
    override val commandTimeout: FiniteDuration = 1.second
    override def initialAggregateRoot: Tally = Tally(0)

    override def commandHandlers = tally => {
      case _: StartTally => CommandSuccess(TallyStarted())
      case c: StartTallyIn => CommandSuccess(TallyStartedIn(c.space))
      case c: Count => CommandSuccess((1 to c.times).map(_ => Counted(1)))
      case c: CountWhenReleased =>
        import scala.concurrent.ExecutionContext.Implicits.global
        AsyncCommandResult(releases.computeIfAbsent(c.key, _ => Promise[Unit]()).future.map(_ => CommandSuccess(Counted(1))))
    }

    override def eventHandlers = (_, _, tally) => {
      case _: TallyStarted | _: TallyStartedIn => handledEvents.incrementAndGet(); tally
      case c: Counted => handledEvents.incrementAndGet(); tally.copy(count = tally.count + c.by)
    }
  }
}

// AR1, R5, F25, F81: the aggregate owner's command lifecycle, through the command bus.
class AggregateOwnerSpec extends CommonSpec {

  import AggregateOwnerSpec._

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new TallyContext, uidGenerator, new MemoryEventStoreState,
    new MemoryCommandResponseState, eventBus))

  private val userId = UserId(1)

  private def started(): AggregateId = {
    val response: CustomCommandResponse[_] = commandBus ?? StartTally(userId)
    response.asInstanceOf[SuccessResponse].aggregateId
  }

  private def state(id: AggregateId): Aggregate[Tally] = {
    val aggregate: Try[Aggregate[Tally]] = commandBus ?? GetAggregate(id)
    aggregate.get
  }

  private def sent(command: AnyRef): Future[Any] = commandBus.ask(command)(Timeout(10.seconds))

  Feature("A command that does not finish in time (R5)") {

    Scenario("is answered with an error at the timeout, and its events are never stored afterwards") {
      val id = started()

      val response: CustomCommandResponse[_] = commandBus ?? CountWhenReleased(userId, id, "late")
      response mustBe a[CommandHandlingError]

      When("its handler finishes after the timeout")
      release("late")
      Thread.sleep(300)

      Then("the aggregate is unchanged")
      state(id).version mustBe AggregateVersion(1)
    }
  }

  Feature("Stopping an owner (passivation)") {

    Scenario("it answers the command it runs, and a command sent meanwhile is answered by a new owner") {
      val id = started()
      val running = sent(CountWhenReleased(userId, id, "stopping"))
      Thread.sleep(100) // the handler runs and waits for its release

      commandBus ! PassivateAggregate(id)
      val sentMeanwhile = sent(Count(userId, id, AggregateVersion(1), 1))

      release("stopping")
      Await.result(running, 10.seconds) mustBe SuccessResponse(id, AggregateVersion(2))
      // It ran after the stopped owner's commit, on a new owner that read version 2.
      Await.result(sentMeanwhile, 10.seconds) mustBe AggregateConcurrentModificationError(id, AggregateType.of[Tally], AggregateVersion(1), AggregateVersion(2))
      state(id).version mustBe AggregateVersion(2)
    }
  }

  Feature("A query for a version the aggregate does not reach (F81)") {

    Scenario("is answered with an error when its time is up") {
      val id = started()
      val query = sent(GetAggregateMinVersion(id, AggregateVersion(5), 200))
      Await.result(query, 5.seconds).asInstanceOf[Try[Aggregate[Tally]]].failed.get mustBe an[AggregateInIncorrectVersionException]
    }

    Scenario("is answered with the aggregate as soon as it reaches the version") {
      val id = started()
      val query = sent(GetAggregateMinVersion(id, AggregateVersion(3), 5000))
      val counted: CustomCommandResponse[_] = commandBus ?? Count(userId, id, AggregateVersion(1), 2)
      counted mustBe SuccessResponse(id, AggregateVersion(3))
      Await.result(query, 5.seconds).asInstanceOf[Try[Aggregate[Tally]]].get.aggregateRoot mustBe Some(Tally(2))
    }
  }

  Feature("A new aggregate's organization (D38)") {

    Scenario("a first event without a space is refused, and nothing is stored") {
      val id = AggregateId(TestIds.aggregate())
      val refused: CustomCommandResponse[_] = commandBus ?? StartTallyIn(userId, SpaceId.unknown.asLong, id)
      refused mustBe a[FailureResponse]
      refused.asInstanceOf[FailureResponse].exceptions.head must include("space")
      state(id).version mustBe AggregateVersion.ZERO

      val inPlatform: CustomCommandResponse[_] = commandBus ?? StartTallyIn(userId, 0, id) // space 0 is the platform
      inPlatform mustBe SuccessResponse(id, AggregateVersion(1))
    }
  }

  Feature("Event handlers (F25)") {

    Scenario("run once per persisted event") {
      val id = started()
      val before = handledEvents.get()
      val response: CustomCommandResponse[_] = commandBus ?? Count(userId, id, AggregateVersion(1), 3)
      response mustBe SuccessResponse(id, AggregateVersion(4))
      handledEvents.get() - before mustBe 3
      state(id).aggregateRoot mustBe Some(Tally(3))
    }
  }
}
