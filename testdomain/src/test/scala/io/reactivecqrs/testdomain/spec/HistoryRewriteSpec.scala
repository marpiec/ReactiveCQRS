package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
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

import scala.util.Try

/** Renames the cart throughout its history without a compensating event: only the rebuilt state shows the new name. */
case class RenameQuietly(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes: Set[Class[_]] = Set(classOf[ShoppingCartCreated])
}

/** Renames the cart throughout its history and adds no event (F146). */
case class RenameSilently(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes: Set[Class[_]] = Set(classOf[ShoppingCartCreated])
}

/** Tries to rewrite the creation as an event of another class. */
case class RewriteCreationAsItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes: Set[Class[_]] = Set(classOf[ShoppingCartCreated])
}

class RewritingCarts extends ShoppingCartAggregateContext {
  override def rewriteHistoryCommandHandlers: RewriteHistoryCommandHandler = (events, cart) => {
    case c: RenameQuietly => RewriteCommandSuccess(events.map(ev => ev.event match {
      case created: ShoppingCartCreated => EventWithVersion(ev.version, created.copy(name = c.name))
      case _ => ev
    }).toSeq, ItemAdded("audit"))
    case c: RenameSilently => RewriteCommandSuccess(events.map(ev => ev.event match {
      case created: ShoppingCartCreated => EventWithVersion(ev.version, created.copy(name = c.name))
      case _ => ev
    }).toSeq, Seq.empty[Event[ShoppingCart]])
    case _: RewriteCreationAsItem => RewriteCommandSuccess(events.map(ev => EventWithVersion[ShoppingCart](ev.version, ItemAdded("not a creation"))).toSeq, ItemAdded("audit"))
    case other => super.rewriteHistoryCommandHandlers(events, cart)(other)
  }
}

// D17, F8: the history rewrite contract.
class HistoryRewriteSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new RewritingCarts, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus))

  private val userId = UserId(1)

  private def cartWithAnItem(): AggregateId = {
    val cartId = AggregateId(TestIds.aggregate())
    eventStore.localTx(session => eventStore.persistEvents(cartId,
      NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries"))))(session)).get
    val added = commandBus.askFor[CustomCommandResponse[_]](AddItem(userId, cartId, AggregateVersion(1), "apple"))
    added mustBe SuccessResponse(cartId, AggregateVersion(2))
    cartId
  }

  private def cart(cartId: AggregateId): Aggregate[ShoppingCart] = {
    val aggregate = commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
    aggregate.get
  }

  Feature("A history rewrite (D17)") {

    Scenario("leaves the live aggregate as the rewritten history makes it") {
      val cartId = cartWithAnItem()
      cart(cartId).aggregateRoot.get.name mustBe "Groceries"

      val renamed = commandBus.askFor[CustomCommandResponse[_]](RenameQuietly(userId, cartId, AggregateVersion(2), "Renamed"))
      renamed mustBe SuccessResponse(cartId, AggregateVersion(3))

      val rewritten = cart(cartId)
      rewritten.version mustBe AggregateVersion(3)
      rewritten.aggregateRoot.get.name mustBe "Renamed"
      rewritten.aggregateRoot.get.items.map(_.name) mustBe Vector("apple", "audit")
    }

    Scenario("F146 that adds no event is stored too, and keeps the version") {
      val cartId = cartWithAnItem()

      val renamed = commandBus.askFor[CustomCommandResponse[_]](RenameSilently(userId, cartId, AggregateVersion(2), "Silent"))
      renamed mustBe SuccessResponse(cartId, AggregateVersion(2))

      cart(cartId).version mustBe AggregateVersion(2)
      cart(cartId).aggregateRoot.get.name mustBe "Silent"
      eventStore.readEventsRange[ShoppingCart](cartId, AggregateVersion(1), AggregateVersion(2)).map(_.event) mustBe
        Seq(ShoppingCartCreated("Silent"), ItemAdded("apple"))
    }

    Scenario("of a base reaches its duplicates: their running owners read the rewritten history again") {
      val baseId = cartWithAnItem()
      val copied = commandBus.askFor[CustomCommandResponse[_]](DuplicateShoppingCart(userId, baseId, AggregateVersion(2)))
      val copyId = copied.asInstanceOf[SuccessResponse].aggregateId
      cart(copyId).aggregateRoot.get.name mustBe "Groceries" // its owner runs

      val renamed = commandBus.askFor[CustomCommandResponse[_]](RenameQuietly(userId, baseId, AggregateVersion(2), "Renamed"))
      renamed mustBe SuccessResponse(baseId, AggregateVersion(3))

      cart(copyId).aggregateRoot.get.name mustBe "Renamed"
    }

    Scenario("of a duplicate reaches only the duplicate's own events, never those of its base") {
      val baseId = cartWithAnItem()
      val copied = commandBus.askFor[CustomCommandResponse[_]](DuplicateShoppingCart(userId, baseId, AggregateVersion(2)))
      val copyId = copied.asInstanceOf[SuccessResponse].aggregateId

      // The copy has no creation event of its own: nothing is rewritten, the audit item is added.
      val renamed = commandBus.askFor[CustomCommandResponse[_]](RenameQuietly(userId, copyId, AggregateVersion(1), "Copy"))
      renamed mustBe SuccessResponse(copyId, AggregateVersion(2))

      val baseEvents = commandBus.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(baseId))
      baseEvents.get.map(_.event) mustBe List(ShoppingCartCreated("Groceries"), ItemAdded("apple"))
      eventStore.readEventsRange[ShoppingCart](copyId, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe
        Seq(ShoppingCartDuplicated(SpaceId(0), baseId, AggregateVersion(2)))
    }

    Scenario("that would change an event's class is refused, and nothing of the command is stored") {
      val cartId = cartWithAnItem()

      val refused = commandBus.askFor[CustomCommandResponse[_]](RewriteCreationAsItem(userId, cartId, AggregateVersion(2)))
      refused mustBe an[EventHandlingError]

      DB.readOnly { implicit session =>
        sql"SELECT count(*) FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single()
      } mustBe Some(2)
      val events = commandBus.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(cartId))
      events.get.map(_.event) mustBe List(ShoppingCartCreated("Groceries"), ItemAdded("apple"))
      cart(cartId).version mustBe AggregateVersion(2)
    }
  }
}
