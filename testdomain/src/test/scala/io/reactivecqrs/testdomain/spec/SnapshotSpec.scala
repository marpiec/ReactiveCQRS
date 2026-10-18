package io.reactivecqrs.testdomain.spec

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregate.{AggregateRestorer, Restored}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.{Random, Try}

// D30, R10: performance snapshots.
class SnapshotSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState, aggregateVersionLimit = 200000).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)

  private class SnapshottingCarts(every: Int) extends ShoppingCartAggregateContext {
    override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = every))
  }
  private val everyThird = new SnapshottingCarts(3)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(everyThird, uidGenerator, eventStore, responses, eventBus))

  private val userId = UserId(1)

  /** Counts the events it handles, so a test sees how much of a history a restore read. */
  private class CountingRestorer(contextVersion: Int = everyThird.version, policy: Option[SnapshotPolicy] = everyThird.snapshots) {
    val handled = new AtomicLong()
    val restorer = new AggregateRestorer[ShoppingCart](eventStore,
      (userId, timestamp, cart) => { case event => handled.incrementAndGet(); everyThird.eventHandlers(userId, timestamp, cart)(event) },
      () => everyThird.initialAggregateRoot, policy, contextVersion, classOf[ShoppingCart].getName)
    def restore(id: AggregateId, at: Option[AggregateVersion] = None): Restored[ShoppingCart] = restorer.restore(id, at.map(Left(_)))
  }
  private val fullReplay = new CountingRestorer(policy = None)

  private def storedCart(): AggregateId = {
    val cartId = AggregateId(TestIds.aggregate())
    eventStore.localTx { session =>
      eventStore.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries"))))(session)
    }.get
    cartId
  }

  private def snapshotVersion(id: AggregateId): Option[Int] = DB.readOnly { implicit session =>
    sql"SELECT version FROM snapshots WHERE aggregate_id = ${id.asLong}".map(_.int(1)).single()
  }

  private def same(a: Restored[ShoppingCart], b: Restored[ShoppingCart]): Boolean = a.version == b.version && a.root == b.root

  Feature("Restoring from a snapshot (D30)") {

    Scenario("gives the state of a full replay, at every version, for random histories with undo and duplication") {
      val random = new Random(20261016)
      val withSnapshots = new CountingRestorer()
      var snapshotsUsed = 0
      (1 to 10).foreach { run =>
        var cartId = storedCart()
        (1 to 25).foreach { step =>
          val cart = fullReplay.restore(cartId)
          val command: AnyRef = random.nextInt(10) match {
            case 0 | 1 if cart.version.asInt >= 3 => UndoShoppingCartChange(userId, cartId, cart.version, 1)
            case 2 => DuplicateShoppingCart(userId, cartId, cart.version)
            case 3 | 4 if cart.root.items.nonEmpty => RemoveItem(userId, cartId, cart.version, cart.root.items(random.nextInt(cart.root.items.size)).id)
            case _ => AddItem(userId, cartId, cart.version, "item " + run + "." + step)
          }
          val response = commandBus.askFor[CustomCommandResponse[_]](command)
          response match {
            case SuccessResponse(id, _) => cartId = id // a duplicate goes on with the copy
            case _: FailureResponse => () // a sixth item is refused
            case other => fail("Unexpected response to " + command + ": " + other)
          }
          val restored = withSnapshots.restore(cartId)
          if (restored.fromSnapshot.isDefined) snapshotsUsed += 1
          withClue(s"run $run, step $step, $command: ")(same(restored, fullReplay.restore(cartId)) mustBe true)
        }
        val newest = fullReplay.restore(cartId).version.asInt
        (1 to newest).foreach { version =>
          withClue(s"run $run, version $version: ") {
            same(withSnapshots.restore(cartId, Some(AggregateVersion(version))), fullReplay.restore(cartId, Some(AggregateVersion(version)))) mustBe true
          }
        }
      }
      snapshotsUsed must be > 50
    }
  }

  Feature("A long history (R10)") {

    Scenario("an aggregate with 100 000 events is restored from one snapshot plus at most `every` events") {
      val every200 = new SnapshottingCarts(200)
      val bus = system.actorOf(AggregateCommandBusActor(every200, uidGenerator, eventStore, responses, eventBus))
      Given("a cart with 100 000 events, written straight to the store")
      val cartId = storedCart()
      def renamed(count: Int, from: Int): Unit = (0 until count by 1000).foreach { offset =>
        val events = (offset until math.min(offset + 1000, count)).map(i => CartNameRewritten("name " + (from + i + 1)))
        eventStore.localTx(session => eventStore.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion(from + offset), Instant.now, events))(session)).get
      }
      renamed(99999, from = 1)

      When("its owner restores it and stops (a snapshot is written at its passivation)")
      val loaded = bus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
      loaded.get.version mustBe AggregateVersion(100000)
      bus ! PassivateAggregate(cartId)
      awaitAssert(snapshotVersion(cartId) mustBe Some(100000), 10.seconds, 50.millis)

      Then("a restore reads the snapshot and no events")
      val restorer = new CountingRestorer(policy = every200.snapshots)
      val restored = restorer.restore(cartId)
      restored.version mustBe AggregateVersion(100000)
      restored.fromSnapshot mustBe Some(AggregateVersion(100000))
      restorer.handled.get mustBe 0L

      And("after 150 more events, it reads those 150")
      renamed(150, from = 100000)
      val later = restorer.restore(cartId)
      later.version mustBe AggregateVersion(100150)
      restorer.handled.get mustBe 150L
      same(later, fullReplay.restore(cartId)) mustBe true

      And("the owner restores the same way and accepts a command")
      val next = bus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(100150), "after"))
      next mustBe SuccessResponse(cartId, AggregateVersion(100151))
    }
  }

  Feature("A snapshot that does not fit is not used, and the restore still works (R10)") {

    def cartWithSnapshot(): AggregateId = {
      val cartId = storedCart()
      (1 to 2).foreach(i => commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(i), "item " + i)) mustBe a[SuccessResponse])
      snapshotVersion(cartId) mustBe Some(3)
      cartId
    }

    Scenario("one of another schema version") {
      val cartId = cartWithSnapshot()
      DB.autoCommit { implicit session => sql"UPDATE snapshots SET schema_version = 99 WHERE aggregate_id = ${cartId.asLong}".update() }
      val restorer = new CountingRestorer()
      val restored = restorer.restore(cartId)
      restored.fromSnapshot mustBe None
      same(restored, fullReplay.restore(cartId)) mustBe true
    }

    Scenario("one of another context version") {
      val cartId = cartWithSnapshot()
      val restorer = new CountingRestorer(contextVersion = everyThird.version + 1)
      val restored = restorer.restore(cartId)
      restored.fromSnapshot mustBe None
      same(restored, fullReplay.restore(cartId)) mustBe true
    }

    Scenario("one that cannot be read") {
      val cartId = cartWithSnapshot()
      DB.autoCommit { implicit session => sql"UPDATE snapshots SET state = 'not json' WHERE aggregate_id = ${cartId.asLong}".update() }
      val restorer = new CountingRestorer()
      val restored = restorer.restore(cartId)
      restored.fromSnapshot mustBe None
      same(restored, fullReplay.restore(cartId)) mustBe true
    }

    // F196: mpjsons reads a field the stored JSON lacks as zero, false or null, silently; the shape tells them apart.
    Scenario("F196 one of another shape of the state class, or of 1.0.0 without a shape, though it reads") {
      val cartId = cartWithSnapshot()
      new CountingRestorer().restore(cartId).fromSnapshot mustBe Some(AggregateVersion(3))
      Seq("0123456789abcdef0123456789abcdef", null).foreach { shape =>
        DB.autoCommit { implicit session => sql"UPDATE snapshots SET shape = $shape WHERE aggregate_id = ${cartId.asLong}".update() }
        val restored = new CountingRestorer().restore(cartId)
        restored.fromSnapshot mustBe None
        same(restored, fullReplay.restore(cartId)) mustBe true
      }
    }
  }

  Feature("An aggregate is restored only by its own type's command bus (F197)") {

    Scenario("a query, a command and a squash sent to another type's bus are refused, and the aggregate is unchanged") {
      Given("a cart with a snapshot, and a bus of folders, whose context also takes snapshots at version 1")
      val cartId = storedCart()
      (1 to 2).foreach(i => commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(i), "item " + i)) mustBe a[SuccessResponse])
      commandBus ! PassivateAggregate(cartId)
      awaitAssert(snapshotVersion(cartId) mustBe Some(3), 10.seconds, 50.millis)
      val folders = system.actorOf(AggregateCommandBusActor(new FolderContext, uidGenerator, eventStore, responses, eventBus))

      When("the cart's id is sent to the folders' bus")
      val query = folders.askFor[Try[Aggregate[Folder]]](GetAggregate(cartId))
      val command = folders.askFor[CustomCommandResponse[_]](AddNote(userId, cartId, AggregateVersion(3), "into a cart"))
      val squash = folders.askFor[SquashResponse](SquashAggregate(cartId, AggregateVersion(3), userId))

      Then("each is refused, naming both types, and the cart is as it was")
      query.failed.get.getMessage must include("is a " + classOf[ShoppingCart].getName + ", not a " + classOf[Folder].getName)
      command mustBe a[CommandHandlingError]
      squash mustBe a[SquashRefused]
      fullReplay.restore(cartId).version mustBe AggregateVersion(3)
      commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get.version mustBe AggregateVersion(3)
      system.stop(folders)
    }
  }

  Feature("Snapshots that would be wrong are deleted with the change that makes them wrong") {

    Scenario("an undo deletes the aggregate's snapshot") {
      val cartId = storedCart()
      (1 to 2).foreach(i => commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(i), "item " + i)) mustBe a[SuccessResponse])
      snapshotVersion(cartId) mustBe Some(3)

      val undone = commandBus.askFor[CustomCommandResponse[_]](UndoShoppingCartChange(userId, cartId, AggregateVersion(3), 1))
      undone mustBe SuccessResponse(cartId, AggregateVersion(4))
      snapshotVersion(cartId) mustBe None
    }

    Scenario("a history rewrite deletes the snapshots of the aggregate and of its duplicates") {
      val cartId = storedCart()
      (1 to 2).foreach(i => commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(i), "item " + i)) mustBe a[SuccessResponse])
      val duplicated = commandBus.askFor[CustomCommandResponse[_]](DuplicateShoppingCart(userId, cartId, AggregateVersion(3)))
      val copyId = duplicated.asInstanceOf[SuccessResponse].aggregateId
      (1 to 2).foreach(i => commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, copyId, AggregateVersion(i), "copy item " + i)) mustBe a[SuccessResponse])
      snapshotVersion(cartId) mustBe Some(3)
      snapshotVersion(copyId) mustBe Some(3)

      val rewritten = commandBus.askFor[CustomCommandResponse[_]](RewriteCartName(userId, cartId, AggregateVersion(3), "Renamed"))
      rewritten mustBe SuccessResponse(cartId, AggregateVersion(4))
      snapshotVersion(cartId) mustBe None
      snapshotVersion(copyId) mustBe None
    }
  }
}
