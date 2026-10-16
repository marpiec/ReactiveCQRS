package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.aggregate.{AggregateRestorer, Restored, SquashScheduler, SquashSchedulerOptions}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState, Snapshot}
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Random, Try}

case class Gauge(level: Int)
case class GaugeSet(level: Int) extends FirstEvent[Gauge] {
  def spaceId: SpaceId = SpaceId(0)
}
case class GaugeMoved(by: Int) extends Event[Gauge]
case class GaugeRestated(level: Int) extends FirstEvent[Gauge] {
  def spaceId: SpaceId = SpaceId(0)
}
case class SetGauge(userId: UserId, gaugeId: AggregateId, level: Int) extends FirstCommand[Gauge, CustomCommandResponse[_]] {
  override def chosenAggregateId: Option[AggregateId] = Some(gaugeId)
}
case class MoveGauge(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, by: Int) extends Command[Gauge, CustomCommandResponse[_]]

/** Squashed automatically once 3 events are older than the newest one (D52). */
class GaugeContext extends AggregateContext[Gauge] {
  override val version: Int = 1
  override def initialAggregateRoot: Gauge = Gauge(0)
  override def commandHandlers = _ => {
    case c: SetGauge => CommandSuccess(GaugeSet(c.level))
    case c: MoveGauge => CommandSuccess(GaugeMoved(c.by))
  }
  override def eventHandlers = (_, _, gauge) => {
    case e: GaugeSet => Gauge(e.level)
    case e: GaugeMoved => Gauge(gauge.level + e.by)
    case e: GaugeRestated => Gauge(e.level)
  }
  override val squash: Option[SquashPolicy[Gauge]] = Some(SquashPolicy(gauge => GaugeRestated(gauge.level), Some(AutomaticSquash(afterEvents = 3, keepLast = 1))))
}

case class CartNameKept(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart]
case class CartArchived(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart] with SquashMayChangeState

/** Squashes a cart into its name only, leaving the items out: a squash the check refuses (D85), unless the event says
  * it may change the state (`onPurpose`). */
class NameOnlyCarts(onPurpose: Boolean) extends ShoppingCartAggregateContext {
  override def eventHandlers = (userId, time, cart) => super.eventHandlers(userId, time, cart).orElse[Any, ShoppingCart] {
    case e: CartNameKept => ShoppingCart(e.name, Vector.empty)
    case e: CartArchived => ShoppingCart(e.name, Vector.empty)
  }
  override val squash: Option[SquashPolicy[ShoppingCart]] =
    Some(SquashPolicy(cart => if (onPurpose) CartArchived(SpaceId(0), cart.name) else CartNameKept(SpaceId(0), cart.name)))
  // Owners write a snapshot when they stop: one stopped after the history changed under it must not.
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = 1000))
}

// D47–D52: squashing old history into one event.
class SquashSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  new PostgresSubscriptionsState(typesNamesState).initSchema() // the subscription_failures table
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val context = new ShoppingCartAggregateContext
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responses, eventBus, eventsReplayMode = false))
  private val restorer = new AggregateRestorer[ShoppingCart](eventStore, context.eventHandlers, () => context.initialAggregateRoot)

  private val userId = UserId(1)
  private implicit val timeout: Timeout = Timeout(10.seconds)

  private def send(command: AnyRef): CustomCommandResponse[_] = {
    val response: CustomCommandResponse[_] = commandBus ?? command
    response
  }

  private def created(items: String*): AggregateId = createdOn(commandBus, items: _*)

  private def createdOn(bus: ActorRef, items: String*): AggregateId = {
    def sent(command: AnyRef): CustomCommandResponse[_] = {
      val response: CustomCommandResponse[_] = bus ?? command
      response
    }
    val cartId = Await.result((bus ? ReserveAggregateId).mapTo[AggregateId], 5.seconds)
    sent(CreateShoppingCartWithId(None, userId, cartId, "Groceries")) mustBe SuccessResponse(cartId, AggregateVersion(1))
    items.zipWithIndex.foreach { case (item, i) => sent(AddItem(userId, cartId, AggregateVersion(i + 1), item)) mustBe SuccessResponse(cartId, AggregateVersion(i + 2)) }
    cartId
  }

  private def squash(id: AggregateId, upTo: Int, metadata: Option[EventMetadata] = None, bus: ActorRef = commandBus): SquashResponse =
    Await.result((bus ? SquashAggregate(id, AggregateVersion(upTo), userId, metadata)).mapTo[SquashResponse], 10.seconds)

  private def refusal(id: AggregateId, upTo: Int): String = squash(id, upTo) match {
    case SquashRefused(_, reason) => reason
    case other => fail("Expected a refusal, got " + other)
  }

  private def outboxEmpty(ids: AggregateId*): Unit = awaitAssert(ids.foreach(id => count("events_to_publish", id) mustBe 0), 5.seconds, 20.millis)

  private def count(table: String, id: AggregateId): Int = DB.readOnly { implicit session =>
    SQL(s"SELECT count(*) FROM $table WHERE aggregate_id = ?").bind(id.asLong).map(_.int(1)).single().get
  }

  private def eventClassAt(id: AggregateId, version: Int): String = DB.readOnly { implicit session =>
    sql"SELECT n.name FROM events e JOIN types_names n ON n.id = e.event_type_id WHERE e.aggregate_id = ${id.asLong} AND e.version = $version"
      .map(_.string(1)).single().get
  }

  private def same(a: Restored[ShoppingCart], b: Restored[ShoppingCart]): Boolean = a.version == b.version && a.root == b.root

  /** The aggregate at each of its versions, or the error reading it. */
  private def history(id: AggregateId): Vector[Either[String, (Int, ShoppingCart)]] =
    (1 to restorer.restore(id).version.asInt).toVector.map(version =>
      Try(restorer.restore(id, Some(Left(AggregateVersion(version))))).toEither.left.map(_.getClass.getSimpleName).map(r => (r.version.asInt, r.root)))

  Feature("Squashing an aggregate (D47)") {

    Scenario("keeps its state and version, removes the history below the point, and it restores the same afterwards") {
      val cartId = created("apple", "pear", "plum")
      send(RemoveItem(userId, cartId, AggregateVersion(4), 1)) mustBe SuccessResponse(cartId, AggregateVersion(5))
      send(AddItem(userId, cartId, AggregateVersion(5), "fig")) mustBe SuccessResponse(cartId, AggregateVersion(6))
      val before = restorer.restore(cartId)
      outboxEmpty(cartId)
      val metadata = EventMetadata("operator:retention", causationId = Some("policy 2026"))

      squash(cartId, 5, Some(metadata)) mustBe AggregateSquashed(cartId, AggregateVersion(5))

      Then("the stored history is the squash event at version 5 and the event after it")
      count("events", cartId) mustBe 2
      eventClassAt(cartId, 5) mustBe classOf[CartSquashed].getName
      DB.readOnly(implicit session => sql"SELECT squashed_version FROM aggregates WHERE id = ${cartId.asLong} AND base_id = ${cartId.asLong}".map(_.int(1)).single()) mustBe Some(5)
      val events: Try[List[EventInfo[ShoppingCart]]] = commandBus ?? GetEventsForAggregate(cartId)
      events.get.map(e => (e.version.asInt, e.metadata)) mustBe List((5, metadata), (6, EventMetadata.ofUser(userId)))

      And("the live owner, a new one, and a fresh read all give the state of before")
      same(restorer.restore(cartId), before) mustBe true
      val live: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      (live.get.version, live.get.aggregateRoot.get) mustBe ((before.version, before.root))
      commandBus ! PassivateAggregate(cartId)
      val restored: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      (restored.get.version, restored.get.aggregateRoot.get) mustBe ((before.version, before.root))
      send(AddItem(userId, cartId, AggregateVersion(6), "kiwi")) mustBe SuccessResponse(cartId, AggregateVersion(7))

      And("a historical read at the point works, below it fails with an explicit error (D51)")
      val atPoint: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregateForVersion(cartId, AggregateVersion(5))
      atPoint.get.aggregateRoot.get.items.map(_.name) mustBe Vector("pear", "plum")
      val below: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregateForVersion(cartId, AggregateVersion(3))
      below.failed.get mustBe an[AggregateSquashedException]
    }

    Scenario("detaches first the duplicates that read below the point, also a duplicate of a duplicate, and they keep their state (D50)") {
      val baseId = created("apple", "pear", "plum", "fig")
      val firstCopy = send(DuplicateShoppingCart(userId, baseId, AggregateVersion(3))).asInstanceOf[SuccessResponse].aggregateId
      send(AddItem(userId, firstCopy, AggregateVersion(1), "kiwi")) mustBe SuccessResponse(firstCopy, AggregateVersion(2))
      val copyOfCopy = send(DuplicateShoppingCart(userId, firstCopy, AggregateVersion(2))).asInstanceOf[SuccessResponse].aggregateId
      val lateCopy = send(DuplicateShoppingCart(userId, baseId, AggregateVersion(5))).asInstanceOf[SuccessResponse].aggregateId
      val before = Seq(firstCopy, copyOfCopy, lateCopy).map(id => id -> restorer.restore(id)).toMap
      outboxEmpty(baseId, firstCopy, copyOfCopy, lateCopy)

      squash(baseId, 4) mustBe AggregateSquashed(baseId, AggregateVersion(4))

      before.foreach { case (id, state) => withClue(s"${id.asLong}: ")(same(restorer.restore(id), state) mustBe true) }
      eventClassAt(firstCopy, 1) mustBe classOf[CartSquashed].getName
      eventClassAt(copyOfCopy, 1) mustBe classOf[CartSquashed].getName
      eventClassAt(lateCopy, 1) mustBe classOf[ShoppingCartDuplicated].getName // it reads the base at 5, after the point
      DB.readOnly { implicit session =>
        sql"SELECT count(*) FROM aggregates WHERE id IN (${firstCopy.asLong}, ${copyOfCopy.asLong}) AND base_id <> id".map(_.int(1)).single()
      } mustBe Some(0)
    }

    Scenario("an undo or a duplication that would read the squashed history is refused; at or after the point they work (D49, D50)") {
      val cartId = created("apple", "pear", "plum", "fig")
      outboxEmpty(cartId)
      squash(cartId, 4) mustBe AggregateSquashed(cartId, AggregateVersion(4))

      send(UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 2)) mustBe an[EventHandlingError]
      send(UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 1)) mustBe SuccessResponse(cartId, AggregateVersion(6))
      send(DuplicateShoppingCart(userId, cartId, AggregateVersion(3))) mustBe an[EventHandlingError]
      val copyId = send(DuplicateShoppingCart(userId, cartId, AggregateVersion(4))).asInstanceOf[SuccessResponse].aggregateId
      restorer.restore(copyId).root mustBe restorer.restore(cartId, Some(Left(AggregateVersion(4)))).root
    }

    Scenario("deletes the snapshot at or below the point and keeps one above it (D51)") {
      val cartId = created("apple", "pear", "plum")
      outboxEmpty(cartId)
      val stateClass = classOf[ShoppingCart].getName
      eventStore.writeSnapshot(cartId, Snapshot(AggregateVersion(4), context.version, 1, restorer.restore(cartId).root), stateClass)
      def snapshotVersion: Option[Int] = eventStore.readSnapshot[ShoppingCart](cartId, stateClass).map(_.get.version.asInt)

      squash(cartId, 3) mustBe AggregateSquashed(cartId, AggregateVersion(3))
      snapshotVersion mustBe Some(4)
      squash(cartId, 4) mustBe AggregateSquashed(cartId, AggregateVersion(4))
      snapshotVersion mustBe None
    }

    Scenario("keeps every state at and above the point, of the aggregate and of its duplicates, for random histories with undo and duplication") {
      val random = new Random(20261017)
      var squashes = 0
      (1 to 5).foreach { run =>
        var carts = Vector(created())
        var cartId = carts.head
        (1 to 30).foreach { step =>
          if (step % 5 == 0) {
            val target = carts(random.nextInt(carts.size))
            val upTo = 1 + random.nextInt(restorer.restore(target).version.asInt)
            outboxEmpty(carts: _*)
            val before = carts.map(id => id -> history(id)).toMap
            val expected = squash(target, upTo) match {
              case AggregateSquashed(_, _) =>
                squashes += 1
                before.updated(target, before(target).zipWithIndex.map { case (state, i) => if (i + 1 < upTo) Left("AggregateSquashedException") else state })
              case SquashRefused(_, reason) =>
                reason must (include("already squashed") or include("undo"))
                before
            }
            carts.foreach(id => withClue(s"run $run, step $step, squash of ${target.asLong} up to $upTo, cart ${id.asLong}: ")(history(id) mustBe expected(id)))
          } else {
            val cart = restorer.restore(cartId)
            val command: AnyRef = random.nextInt(10) match {
              case 0 | 1 if cart.version.asInt >= 3 => UndoShoppingCartChange(userId, cartId, cart.version, 1 + random.nextInt(2))
              case 2 => DuplicateShoppingCart(userId, cartId, AggregateVersion(1 + random.nextInt(cart.version.asInt)))
              case 3 | 4 if cart.root.items.nonEmpty => RemoveItem(userId, cartId, cart.version, cart.root.items(random.nextInt(cart.root.items.size)).id)
              case _ => AddItem(userId, cartId, cart.version, "item " + run + "." + step)
            }
            send(command) match {
              case SuccessResponse(id, _) if command.isInstanceOf[DuplicateShoppingCart] =>
                carts :+= id
                cartId = id // a duplicate goes on with the copy
              case _: SuccessResponse => ()
              case _: FailureResponse => () // a sixth item is refused
              case _: EventHandlingError if eventStore.squashedVersion(cartId).isDefined => () // it would read the squashed history
              case other => fail("Unexpected response to " + command + ": " + other)
            }
          }
        }
      }
      squashes must be > 10
    }
  }

  Feature("A squash is refused, changing nothing (D51)") {

    Scenario("while events at or below the point wait in the outbox") {
      val cartId = AggregateId(System.nanoTime())
      eventStore.localTx(session => eventStore.persistEvents(cartId,
        NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Unpublished"), ItemAdded("apple"))))(session)).get
      refusal(cartId, 2) must include("outbox")
      count("events", cartId) mustBe 2
    }

    Scenario("while a subscription of the aggregate is failed") {
      val cartId = created("apple", "pear")
      outboxEmpty(cartId)
      DB.autoCommit { implicit session =>
        sql"""INSERT INTO subscription_failures (subscriber_type_id, subscription_type, aggregate_id, aggregate_type_id, failed_version, error, failed_since)
             | VALUES (1, 2, ${cartId.asLong}, 1, 2, 'failing', now())""".stripMargin.update()
      }
      try refusal(cartId, 2) must include("failed")
      finally DB.autoCommit(implicit session => sql"DELETE FROM subscription_failures WHERE aggregate_id = ${cartId.asLong}".update())
      count("events", cartId) mustBe 3
    }

    Scenario("when a later undo undid events at or below the point, but not at the undo itself, which removes the noop rows") {
      val cartId = created("apple", "pear", "plum")
      send(UndoShoppingCartChange(userId, cartId, AggregateVersion(4), 2)) mustBe SuccessResponse(cartId, AggregateVersion(5))
      outboxEmpty(cartId)
      val eventIds = DB.readOnly(implicit session => sql"SELECT id FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.long(1)).list())
      def noopRows: Int = DB.readOnly(implicit session => sql"SELECT count(*) FROM noop_events WHERE id IN ($eventIds)".map(_.int(1)).single().get)
      noopRows mustBe 3

      refusal(cartId, 4) must include("undo")
      squash(cartId, 5) mustBe AggregateSquashed(cartId, AggregateVersion(5))

      noopRows mustBe 0
      val cart: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      cart.get.aggregateRoot.get.items.map(_.name) mustBe Vector("apple")
    }

    Scenario("for a type that does not enable squash, a version out of range, or one squashed already") {
      val withoutSquash = new ShoppingCartAggregateContext { override val squash: Option[SquashPolicy[ShoppingCart]] = None }
      val bus = system.actorOf(AggregateCommandBusActor(withoutSquash, uidGenerator, eventStore, responses, eventBus, eventsReplayMode = false))
      val cartId = created("apple", "pear")
      Await.result((bus ? SquashAggregate(cartId, AggregateVersion(1))).mapTo[SquashResponse], 5.seconds) mustBe a[SquashRefused]
      refusal(cartId, 4) must include("not between 1 and 3")
      outboxEmpty(cartId)
      squash(cartId, 2) mustBe AggregateSquashed(cartId, AggregateVersion(2))
      refusal(cartId, 2) must include("already squashed up to version 2")
      refusal(cartId, 1) must include("already squashed up to version 2")
    }
  }

  Feature("The squash event must give back the state it replaces (D85)") {

    Scenario("a squash whose event does not is refused, and nothing changes") {
      val bus = system.actorOf(AggregateCommandBusActor(new NameOnlyCarts(onPurpose = false), uidGenerator, eventStore, responses, eventBus, eventsReplayMode = false))
      val cartId = createdOn(bus, "apple", "pear")
      outboxEmpty(cartId)

      squash(cartId, 3, bus = bus) match {
        case SquashRefused(_, reason) => reason must include("CartNameKept does not give back the state it replaces")
        case other => fail("Expected a refusal, got " + other)
      }
      count("events", cartId) mustBe 3
      eventStore.squashedVersion(cartId) mustBe None
    }

    Scenario("one whose event may change the state changes it on purpose: the owners read the new states, the snapshot goes") {
      val carts = new NameOnlyCarts(onPurpose = true)
      val bus = system.actorOf(AggregateCommandBusActor(carts, uidGenerator, eventStore, responses, eventBus, eventsReplayMode = false))
      val cartsRestorer = new AggregateRestorer[ShoppingCart](eventStore, carts.eventHandlers, () => carts.initialAggregateRoot)
      def sent(command: AnyRef): CustomCommandResponse[_] = {
        val response: CustomCommandResponse[_] = bus ?? command
        response
      }
      def itemsOf(id: AggregateId): Vector[String] = {
        val cart: Try[Aggregate[ShoppingCart]] = bus ?? GetAggregate(id)
        cart.get.aggregateRoot.get.items.map(_.name)
      }
      val cartId = createdOn(bus, "apple", "pear", "plum")
      val copyId = sent(DuplicateShoppingCart(userId, cartId, AggregateVersion(2))).asInstanceOf[SuccessResponse].aggregateId
      sent(AddItem(userId, copyId, AggregateVersion(1), "fig")) mustBe SuccessResponse(copyId, AggregateVersion(2))
      val lateCopyId = sent(DuplicateShoppingCart(userId, cartId, AggregateVersion(4))).asInstanceOf[SuccessResponse].aggregateId
      // Their owners run, with the states of before.
      itemsOf(copyId) mustBe Vector("apple", "fig")
      itemsOf(lateCopyId) mustBe Vector("apple", "pear", "plum")
      val stateClass = classOf[ShoppingCart].getName
      eventStore.writeSnapshot(cartId, Snapshot(AggregateVersion(4), carts.version, 1, cartsRestorer.restore(cartId).root), stateClass)
      eventStore.writeSnapshot(lateCopyId, Snapshot(AggregateVersion(1), carts.version, 1, cartsRestorer.restore(lateCopyId).root), stateClass)
      outboxEmpty(cartId, copyId, lateCopyId)

      squash(cartId, 3, bus = bus) mustBe AggregateSquashed(cartId, AggregateVersion(3))

      Then("the history now gives the cart without the items squashed away, and its live owner shows that")
      itemsOf(cartId) mustBe Vector("plum")
      cartsRestorer.restore(cartId).root.items.map(_.name) mustBe Vector("plum")
      // Made from the histories of before: the cart's, and that of the copy that reads the cart after the squash point.
      eventStore.readSnapshot[ShoppingCart](cartId, stateClass) mustBe None
      eventStore.readSnapshot[ShoppingCart](lateCopyId, stateClass) mustBe None
      And("the copy, detached with its own state left out the same way, is read again by a new owner, not from a snapshot of before")
      itemsOf(copyId) mustBe Vector("fig")
      And("the copy that reads the cart after the squash point now reads the squash event, and its owner reads it again")
      itemsOf(lateCopyId) mustBe Vector("plum")
      And("commands go on from the new states")
      sent(AddItem(userId, cartId, AggregateVersion(4), "kiwi")) mustBe SuccessResponse(cartId, AggregateVersion(5))
      itemsOf(cartId) mustBe Vector("plum", "kiwi")
    }
  }

  Feature("The squash scheduler (D52)") {

    Scenario("squashes, through their owners, the aggregates its rule selects, keeping the newest events") {
      val gauges = new GaugeContext
      val bus = system.actorOf(AggregateCommandBusActor(gauges, uidGenerator, eventStore, responses, eventBus, eventsReplayMode = false))
      def gauge(moves: Int): AggregateId = {
        val gaugeId = Await.result((bus ? ReserveAggregateId).mapTo[AggregateId], 5.seconds)
        val set: CustomCommandResponse[_] = bus ?? SetGauge(userId, gaugeId, 10)
        set mustBe SuccessResponse(gaugeId, AggregateVersion(1))
        (1 to moves).foreach { i =>
          val moved: CustomCommandResponse[_] = bus ?? MoveGauge(userId, gaugeId, AggregateVersion(i), 1)
          moved mustBe SuccessResponse(gaugeId, AggregateVersion(i + 1))
        }
        gaugeId
      }
      val long = gauge(moves = 5) // 6 events: squashed up to 5
      val short = gauge(moves = 1) // 2 events: left as it is
      outboxEmpty(long, short)
      val scheduler = system.actorOf(Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(gauges, bus)), SquashSchedulerOptions(firstRunAfter = 1.hour))))

      scheduler ! SquashScheduler.Run

      awaitAssert({
        val status: SquashScheduler.SquashStatus = scheduler ?? SquashScheduler.GetSquashStatus
        status mustBe SquashScheduler.SquashStatus(squashed = 1, refused = 0, waiting = 0)
      }, 10.seconds, 50.millis)
      eventStore.readEventsRange[Gauge](long, AggregateVersion(1), AggregateVersion(10)).map(e => (e.version.asInt, e.event, e.metadata)) mustBe
        Seq((5, GaugeRestated(14), SquashScheduler.metadata), (6, GaugeMoved(1), EventMetadata.ofUser(userId)))
      eventStore.squashedVersion(short) mustBe None
      val squashed: Try[Aggregate[Gauge]] = bus ?? GetAggregate(long)
      (squashed.get.version, squashed.get.aggregateRoot) mustBe ((AggregateVersion(6), Some(Gauge(15))))
    }
  }
}
