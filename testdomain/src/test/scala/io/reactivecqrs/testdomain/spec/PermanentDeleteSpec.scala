package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor.Poll
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RepublishOutbox}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, SubscriptionFailure, SubscriptionType}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._
import scala.util.Try

case class Memo(text: String)
case class MemoWritten(text: String) extends FirstEvent[Memo] {
  def spaceId: SpaceId = SpaceId(0)
}
case class MemoErased() extends PermanentDeleteEvent[Memo]
case class MemoCopied(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[Memo]
case class MemoRestated(text: String) extends FirstEvent[Memo] {
  def spaceId: SpaceId = SpaceId(0)
}

case class WriteMemo(userId: UserId, memoId: AggregateId, text: String) extends FirstCommand[Memo, CustomCommandResponse[_]] {
  override def chosenAggregateId: Option[AggregateId] = Some(memoId)
}
case class EraseMemo(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion) extends Command[Memo, CustomCommandResponse[_]]
case class CopyMemo(userId: UserId, baseId: AggregateId, baseVersion: AggregateVersion) extends FirstCommand[Memo, CustomCommandResponse[_]]

// With `squashable`, a memo duplicated from one that is deleted for good keeps its text (F76).
class MemoContext(squashable: Boolean = true) extends AggregateContext[Memo] {
  override val version: Int = 1
  override def initialAggregateRoot: Memo = Memo("")
  override def commandHandlers = _ => {
    case c: WriteMemo => CommandSuccess(MemoWritten(c.text))
    case _: EraseMemo => CommandSuccess(MemoErased())
    case c: CopyMemo => CommandSuccess(MemoCopied(SpaceId(0), c.baseId, c.baseVersion))
  }
  override def eventHandlers = (_, _, memo) => {
    case e: MemoWritten => Memo(e.text)
    case _: MemoErased => memo
    case _: MemoCopied => memo
    case e: MemoRestated => Memo(e.text)
  }
  override val squash: Option[SquashPolicy[Memo]] = if (squashable) Some(SquashPolicy(memo => MemoRestated(memo.text))) else None
}

// A PermanentDeleteEvent deletes the aggregate for good, but only once every subscriber has it, so read models can
// remove their copies first. The owner carries out the delete when the bus acknowledges that event.
class PermanentDeleteSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val subscriptions = new PostgresSubscriptionsState(typesNamesState, keepInMemory = false).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

  private val userId = UserId(1)
  private val memoType = AggregateType(classOf[Memo].getName)

  private class Fixture(context: MemoContext = new MemoContext) {
    val subscriber = TestProbe()
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()))
    bus ! RegisterSubscribers(Seq(subscriber.ref))
    subscriber.expectMsg(GetSubscriptionRequests)
    subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(memoType, subscriber.ref))))
    val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore,
      new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), bus, eventsReplayMode = false))

    def delivered(memoId: AggregateId): Seq[Int] =
      subscriber.fishForSpecificMessage(10.seconds) { case e: IdentifiableEvents[_] if e.aggregateId == memoId => e.events.map(_.version.asInt) }

    def ack(memoId: AggregateId, versions: Int*): Unit = bus ! MessageAck(subscriber.ref, memoId, versions.map(AggregateVersion(_)), EventsDelivery)
  }

  private def storedVersions(memoId: AggregateId): Seq[Int] =
    eventStore.readEventsRange[Memo](memoId, AggregateVersion(1), AggregateVersion(100)).map(_.version.asInt)

  private def deletedForGood(memoId: AggregateId): Unit = awaitAssert({
    storedVersions(memoId) mustBe empty
    eventStore.oldestVersionToPublish(memoId) mustBe None
  }, 5.seconds, 50.millis)

  Feature("Deleting an aggregate for good") {

    Scenario("happens once every subscriber has acknowledged the permanent delete event") {
      val f = new Fixture
      val memoId = AggregateId(System.nanoTime())
      val written: CustomCommandResponse[_] = f.commandBus ?? WriteMemo(userId, memoId, "note")
      written mustBe SuccessResponse(memoId, AggregateVersion(1))
      f.delivered(memoId) mustBe Seq(1)
      f.ack(memoId, 1)

      val erased: CustomCommandResponse[_] = f.commandBus ?? EraseMemo(userId, memoId, AggregateVersion(1))
      erased mustBe SuccessResponse(memoId, AggregateVersion(2))
      f.delivered(memoId) mustBe Seq(2)
      storedVersions(memoId) mustBe Seq(1, 2)

      f.ack(memoId, 2)
      deletedForGood(memoId)
    }

    Scenario("waits while a subscription of the aggregate is failed, and happens on a later delivery once it is cleared (D20a)") {
      Given("a memo that a projection failed on")
      val f = new Fixture
      val memoId = AggregateId(System.nanoTime())
      val written: CustomCommandResponse[_] = f.commandBus ?? WriteMemo(userId, memoId, "note")
      written mustBe SuccessResponse(memoId, AggregateVersion(1))
      f.delivered(memoId) mustBe Seq(1)
      f.ack(memoId, 1)
      val failure = SubscriptionFailure(SubscriptionType.EVENTS, memoType, memoId, AggregateVersion(1), "listener failed", Instant.now)
      subscriptions.recordFailure("PermanentDeleteSpecProjection", failure)

      When("it is erased and the erase is acknowledged")
      val erased: CustomCommandResponse[_] = f.commandBus ?? EraseMemo(userId, memoId, AggregateVersion(1))
      erased mustBe SuccessResponse(memoId, AggregateVersion(2))
      f.delivered(memoId) mustBe Seq(2)
      f.ack(memoId, 2)

      Then("the memo and the erase event's outbox row stay")
      awaitAssert(eventStore.oldestVersionToPublish(memoId) mustBe Some(AggregateVersion(2)), 5.seconds, 50.millis)
      storedVersions(memoId) mustBe Seq(1, 2)

      When("the failure is cleared and the erase is delivered again")
      subscriptions.localTx { implicit session => subscriptions.clearFailure("PermanentDeleteSpecProjection", SubscriptionType.EVENTS, memoId) }
      f.commandBus ! RepublishOutbox(memoId)
      f.delivered(memoId) mustBe Seq(2)
      f.ack(memoId, 2)

      Then("the memo is deleted for good")
      deletedForGood(memoId)
    }

    Scenario("still waiting in the outbox after a restart, it is carried out when delivered again") {
      val memoId = AggregateId(System.nanoTime())
      eventStore.localTx { session =>
        eventStore.persistEvents(memoId, NewEvents[Memo](userId, AggregateVersion.ZERO,
          Instant.now, Seq(MemoWritten("note"), MemoErased())))(session)
      }.get
      val f = new Fixture
      val poller = system.actorOf(Props(new OutboxPollerActor(eventStore, f.bus, OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour))))

      poller ! Poll
      f.delivered(memoId) mustBe Seq(1, 2)
      f.ack(memoId, 1, 2)

      deletedForGood(memoId)
    }
  }

  Feature("Deleting an aggregate that others were duplicated from (F76)") {

    /** A memo and a copy of it, both delivered, and the memo erased. */
    def erasedWithCopy(f: Fixture): (AggregateId, AggregateId) = {
      val memoId = AggregateId(System.nanoTime())
      val written: CustomCommandResponse[_] = f.commandBus ?? WriteMemo(userId, memoId, "note")
      written mustBe SuccessResponse(memoId, AggregateVersion(1))
      f.delivered(memoId) mustBe Seq(1)
      f.ack(memoId, 1)
      val copied: CustomCommandResponse[_] = f.commandBus ?? CopyMemo(userId, memoId, AggregateVersion(1))
      val copyId = copied.asInstanceOf[SuccessResponse].aggregateId
      f.delivered(copyId) mustBe Seq(1)
      f.ack(copyId, 1)
      val erased: CustomCommandResponse[_] = f.commandBus ?? EraseMemo(userId, memoId, AggregateVersion(1))
      erased mustBe SuccessResponse(memoId, AggregateVersion(2))
      f.delivered(memoId) mustBe Seq(2)
      (memoId, copyId)
    }

    def storedText(f: Fixture, memoId: AggregateId): String = {
      f.commandBus ! PassivateAggregate(memoId) // read from the store, not from a live owner
      val memo: Try[Aggregate[Memo]] = f.commandBus ?? GetAggregate(memoId)
      memo.get.aggregateRoot.get.text
    }

    Scenario("detaches the duplicates first, with the type's squash event, so they keep their state") {
      val f = new Fixture
      val (memoId, copyId) = erasedWithCopy(f)

      f.ack(memoId, 2)

      deletedForGood(memoId)
      eventStore.readEventsRange[Memo](copyId, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe Seq(MemoRestated("note"))
      storedText(f, copyId) mustBe "note"
    }

    Scenario("is not carried out for a type without a squash event: the event waits in the outbox, the duplicates keep reading") {
      val f = new Fixture(new MemoContext(squashable = false))
      val (memoId, copyId) = erasedWithCopy(f)

      f.ack(memoId, 2)

      awaitAssert(eventStore.oldestVersionToPublish(memoId) mustBe Some(AggregateVersion(2)), 5.seconds, 50.millis)
      storedVersions(memoId) mustBe Seq(1, 2)
      storedText(f, copyId) mustBe "note"
    }
  }
}
