package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import java.time.Instant

import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.{EphemeralBlock, EphemeralMode}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{EphemeralEventStoreState, NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.metrics.{Metrics, ReactiveMetrics}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{NewCommandsIdsPool, PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Try

/** Carts whose commands time out after 2 s instead of 60 s. */
class QuickCarts extends ShoppingCartAggregateContext {
  override val commandTimeout: FiniteDuration = 2.seconds
}

/** Carts that cannot be materialized: their context has no squash event. */
class UnsquashableCarts extends ShoppingCartAggregateContext {
  override val squash: Option[SquashPolicy[ShoppingCart]] = None
}

// D77, R16: aggregates that live in one node's memory, and their materialization.
class EphemeralAggregatesSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val carts = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responses, eventBus))
  private val user = UserId(1)

  private def ephemeralBus(context: ShoppingCartAggregateContext = new ShoppingCartAggregateContext, persistent: ActorRef = carts,
                           maxAggregates: Int = 100000, idleTime: FiniteDuration = 12.hours, sweepEvery: FiniteDuration = 5.minutes,
                           eventSizeLimit: Int = 100000): ActorRef =
    system.actorOf(AggregateCommandBusActor.ephemeral(context, persistent, uidGenerator, mpjsons, maxAggregates, idleTime, sweepEvery, eventSizeLimit))

  // As `ephemeral` makes it, with a chosen token, fewer ids, or another uid generator.
  private def testedBus(token: Option[Long], ids: Long, uids: ActorRef): ActorRef =
    system.actorOf(Props(new AggregateCommandBusActor[ShoppingCart](new ShoppingCartAggregateContext, uids, new EphemeralEventStoreState(mpjsons),
      new MemoryCommandResponseState(), ActorRef.noSender, 200, 43200000L, None, 5.seconds, Some(EphemeralMode(carts, 100000, 12.hours, 5.minutes, token, ids)))))

  private def send(bus: ActorRef, command: AnyRef): CustomCommandResponse[_] = bus.askFor[CustomCommandResponse[_]](command)
  private def created(bus: ActorRef): AggregateId = send(bus, CreateShoppingCart(None, user, "Draft")) match {
    case SuccessResponse(id, _) => id
    case other => fail("not created: " + other)
  }
  private def cart(bus: ActorRef, id: AggregateId): Try[Aggregate[ShoppingCart]] = bus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(id))
  private def materialize(bus: ActorRef, id: AggregateId): MaterializeResponse = bus.askFor[MaterializeResponse](Materialize(id, user))
  private def storedEvents(): Long = DB.readOnly(implicit session => sql"SELECT count(*) FROM events".map(_.long(1)).single().get)

  // The statements that write, run by anyone in this JVM while `block` runs.
  private def writesDuring[A](block: => A): (A, Long) = {
    val writes = new java.util.concurrent.atomic.AtomicLong()
    val previous = GlobalSettings.queryCompletionListener
    GlobalSettings.queryCompletionListener = (sql, params, millis) => {
      previous(sql, params, millis)
      if (sql.trim.toUpperCase.matches("(?s)(INSERT|UPDATE|DELETE).*")) writes.incrementAndGet()
    }
    try (block, writes.get) finally GlobalSettings.queryCompletionListener = previous
  }

  Feature("Ephemeral aggregates (D77)") {

    Scenario("answer commands as persistent ones do, write nothing, and materialize into a persistent aggregate of the same state") {
      val drafts = ephemeralBus()
      val before = storedEvents()

      val ((id, draft), writes) = writesDuring {
        val id = created(drafts)
        send(drafts, AddItem(user, id, AggregateVersion(1), "apple")) mustBe SuccessResponse(id, AggregateVersion(2))
        send(drafts, AddItem(user, id, AggregateVersion(2), "pear")) mustBe SuccessResponse(id, AggregateVersion(3))
        send(drafts, AddItem(user, id, AggregateVersion(2), "late")) mustBe an[AggregateConcurrentModificationError]
        (id, cart(drafts, id).get)
      }
      writes mustBe 0L
      id.asLong must be < 0L
      math.abs(id.asLong) must be < (1L << 53) // D39: the UI reads ids as JavaScript numbers
      draft.aggregateRoot.get.items.map(_.name) mustBe Vector("apple", "pear")
      storedEvents() mustBe before

      val persistent = materialize(drafts, id) match {
        case Materialized(`id`, persistentId) => persistentId
        case other => fail("not materialized: " + other)
      }
      persistent.asLong must be > 0L
      val stored = cart(carts, persistent).get
      (stored.version, stored.aggregateRoot) mustBe ((AggregateVersion(1), draft.aggregateRoot))
      eventStore.readEventsRange[ShoppingCart](persistent, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe
        Seq(CartSquashed(SpaceId(0), "Draft", draft.aggregateRoot.get.items))

      send(drafts, AddItem(user, id, AggregateVersion(3), "plum")) mustBe EphemeralAggregateUnknown(id)
      cart(drafts, id).failed.get mustBe a[NoEventsForAggregateException]
      system.stop(drafts)
    }

    Scenario("an id of another start is lost, and one of this start that the bus does not hold is unknown") {
      val (first, second) = (ephemeralBus(), ephemeralBus())
      val id = created(first)

      send(second, AddItem(user, id, AggregateVersion(1), "x")) mustBe EphemeralAggregateLost(id)
      cart(second, id).failed.get mustBe an[EphemeralAggregateLostException]
      val notMade = AggregateId(-(AggregateCommandBusActor.ephemeralToken(id) << 24)) // the last id of the block, not given yet
      send(first, AddItem(user, notMade, AggregateVersion(1), "x")) mustBe EphemeralAggregateUnknown(notMade)
      send(first, AddItem(user, AggregateId(TestIds.aggregate()), AggregateVersion(1), "x")) mustBe a[EphemeralAggregateUnknown]
      Seq(first, second).foreach(system.stop)
    }

    Scenario("a materialization holds the aggregate's commands until the persistent bus answers") {
      val persistent = TestProbe()
      val drafts = ephemeralBus(persistent = persistent.ref)
      val id = created(drafts)
      val requester = TestProbe()

      requester.send(drafts, Materialize(id, user))
      persistent.expectMsgType[StoreFirstEvent[ShoppingCart]](5.seconds).idempotencyId.map(_.ephemeralId) mustBe Some(id)
      send(drafts, AddItem(user, id, AggregateVersion(1), "meanwhile")) mustBe a[CommandRejected]
      persistent.reply(FailureResponse(List("refused")))
      requester.expectMsgType[MaterializeRefused](5.seconds).reason must include("refused")

      And("refused, it stays and takes commands again; stored, it is dropped")
      send(drafts, AddItem(user, id, AggregateVersion(1), "after")) mustBe SuccessResponse(id, AggregateVersion(2))
      requester.send(drafts, Materialize(id, user))
      persistent.expectMsgType[StoreFirstEvent[ShoppingCart]](5.seconds)
      persistent.reply(SuccessResponse(AggregateId(77), AggregateVersion(1)))
      requester.expectMsg(5.seconds, Materialized(id, AggregateId(77)))
      awaitAssert(send(drafts, AddItem(user, id, AggregateVersion(2), "x")) mustBe EphemeralAggregateUnknown(id), 5.seconds, 50.millis)
      system.stop(drafts)
    }

    Scenario("a materialization is stored once however often it is sent, and refused without a squash event") {
      val key = Some(MaterializationKey(AggregateId(-TestIds.aggregate()), "start"))
      val first = send(carts, StoreFirstEvent[ShoppingCart](user, CartSquashed(SpaceId(0), "Twice", Vector.empty), key))
      first mustBe a[SuccessResponse]
      send(carts, StoreFirstEvent[ShoppingCart](user, CartSquashed(SpaceId(0), "Twice", Vector.empty), key)) mustBe first

      val drafts = ephemeralBus(context = new UnsquashableCarts)
      val id = created(drafts)
      materialize(drafts, id).asInstanceOf[MaterializeRefused].reason must include("squash")
      cart(drafts, id).get.version mustBe AggregateVersion(1)
      system.stop(drafts)
    }

    Scenario("refuses what the persistent store refuses, and holds at most its maximum") {
      val drafts = ephemeralBus(maxAggregates = 2, eventSizeLimit = 60)
      val (one, _) = (created(drafts), created(drafts))
      send(drafts, AddItem(user, one, AggregateVersion(1), "x" * 100)) mustBe an[EventHandlingError] // JSON over 60 characters
      cart(drafts, one).get.version mustBe AggregateVersion(1)
      send(drafts, CreateShoppingCart(None, user, "third")) mustBe a[CommandRejected]
      system.stop(drafts)
    }

    Scenario("F208 a materialization without an answer keeps the aggregate as it was, and sends the same request until one comes") {
      val persistent = TestProbe()
      val drafts = ephemeralBus(context = new QuickCarts, persistent = persistent.ref)
      val id = created(drafts)
      val (first, second) = (TestProbe(), TestProbe())

      first.send(drafts, Materialize(id, user))
      val sent = persistent.expectMsgType[StoreFirstEvent[ShoppingCart]](5.seconds)
      first.expectMsgType[MaterializeRefused](5.seconds).reason must include("not known")
      send(drafts, AddItem(user, id, AggregateVersion(1), "after the timeout")) mustBe a[CommandRejected]
      persistent.expectMsg(5.seconds, sent) // the same state, under the same key
      second.send(drafts, Materialize(id, user))
      persistent.reply(SuccessResponse(AggregateId(77), AggregateVersion(1)))

      second.expectMsg(5.seconds, Materialized(id, AggregateId(77)))
      system.stop(drafts)
    }

    Scenario("F209 drafts of two starts with the same ids are stored apart") {
      val (first, second) = (testedBus(Some(5L), EphemeralBlock, uidGenerator), testedBus(Some(5L), EphemeralBlock, uidGenerator))
      val id = created(first)
      send(second, CreateShoppingCart(None, user, "Other")) mustBe SuccessResponse(id, AggregateVersion(1))

      val stored = Seq(first, second).map(materialize(_, id) match {
        case Materialized(_, persistentId) => persistentId
        case other => fail("not materialized: " + other)
      })
      stored.distinct.size mustBe 2
      cart(carts, stored(1)).get.aggregateRoot.get.name mustBe "Other"
      Seq(first, second).foreach(system.stop)
    }

    Scenario("F236 a new aggregate that waited while the ids ran out is refused, and the commands after it go on") {
      val uids = TestProbe()
      val drafts = testedBus(None, 2, uids.ref)
      uids.expectMsg(5.seconds, UidGeneratorActor.GetNewCommandsIdsPool)
      val creators = Seq.fill(3)(TestProbe())
      creators.foreach(_.send(drafts, CreateShoppingCart(None, user, "Draft")))
      uids.reply(NewCommandsIdsPool(1L, 1000L))

      val made = creators.take(2).map(_.expectMsgType[SuccessResponse](5.seconds).aggregateId)
      creators(2).expectMsgType[CommandRejected](5.seconds).reason must include("spent")
      send(drafts, AddItem(user, made.head, AggregateVersion(1), "x")) mustBe SuccessResponse(made.head, AggregateVersion(2))
      system.stop(drafts)
    }

    Scenario("F256 an ephemeral aggregate's context writes no rows of its own, and a failed write leaves no events") {
      val drafts = system.actorOf(AggregateCommandBusActor.ephemeral(new CartsWithJobs, carts, uidGenerator, mpjsons))
      val id = created(drafts)
      send(drafts, AddItem(user, id, AggregateVersion(1), "a")) mustBe SuccessResponse(id, AggregateVersion(2))
      send(drafts, AddItem(user, id, AggregateVersion(2), "b")) mustBe SuccessResponse(id, AggregateVersion(3))
      system.stop(drafts)

      val store = new EphemeralEventStoreState(mpjsons)
      val draft = AggregateId(-1)
      Try(store.localTx { implicit session =>
        store.persist(draft, NewEvents[ShoppingCart](user, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("lost")))).get
        throw new IllegalStateException("the key is taken")
      }).isFailure mustBe true
      store.holds(draft) mustBe false
    }

    Scenario("F257 a bus whose message handling fails keeps its drafts, and makes new ones") {
      val persistent = TestProbe()
      val drafts = ephemeralBus(persistent = persistent.ref)
      val (kept, materialized) = (created(drafts), created(drafts))
      val metrics = ReactiveMetrics(system).metrics
      ReactiveMetrics(system).use(new Metrics {
        override def count(name: String, tags: Map[String, String], amount: Long): Unit =
          if (name == "reactivecqrs.ephemeral.materialized") throw new IllegalStateException("the metrics system is away")
          else metrics.count(name, tags, amount)
        override def time(name: String, tags: Map[String, String], nanos: Long): Unit = metrics.time(name, tags, nanos)
        override def gauge(name: String, tags: Map[String, String], value: Long): Unit = metrics.gauge(name, tags, value)
      })
      try {
        val requester = TestProbe()
        requester.send(drafts, Materialize(materialized, user))
        persistent.expectMsgType[StoreFirstEvent[ShoppingCart]](5.seconds)
        persistent.reply(SuccessResponse(AggregateId(78), AggregateVersion(1)))
        requester.expectMsg(5.seconds, Materialized(materialized, AggregateId(78)))
        Thread.sleep(200) // the bus drops the materialized one, and its metrics throw
      } finally ReactiveMetrics(system).use(metrics)

      send(drafts, AddItem(user, kept, AggregateVersion(1), "x")) mustBe SuccessResponse(kept, AggregateVersion(2))
      created(drafts).asLong must be < 0L
      system.stop(drafts)
    }

    Scenario("drops an aggregate unused for its idle time, and never one a command may still write") {
      val drafts = ephemeralBus(context = new QuickCarts, idleTime = 300.millis, sweepEvery = 100.millis)
      val id = created(drafts)
      send(drafts, AddItem(user, id, AggregateVersion(1), "x")) mustBe SuccessResponse(id, AggregateVersion(2))
      Thread.sleep(600) // past the idle time, within the command timeout: kept
      send(drafts, AddItem(user, id, AggregateVersion(2), "y")) mustBe SuccessResponse(id, AggregateVersion(3))
      Thread.sleep(2500) // past both: a message now would count as a use
      send(drafts, AddItem(user, id, AggregateVersion(3), "z")) mustBe EphemeralAggregateUnknown(id)
      system.stop(drafts)
    }
  }
}
