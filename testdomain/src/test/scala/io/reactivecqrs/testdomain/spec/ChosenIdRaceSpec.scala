package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// Two owners of one aggregate at once (two nodes of a `Local` type, or two owners during a hand-over, D43) create it under
// the id the command chose: one wins, the other is answered as for any conflict, never with an error.
class ChosenIdRaceSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(new PostgresUidGenerator("aggregates_uids_seq"),
    new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

  /** A node's command bus for carts, with owners of its own. */
  private def node(): ActorRef = {
    val eventBus = system.actorOf(Props(new EventsBusActor()))
    eventBus ! RegisterSubscribers(Nil)
    system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responses, eventBus))
  }

  private val nodes = Seq(node(), node())
  private val userId = UserId(1)

  /** Sends the command to both nodes at once. Until both owners wait to write, `aggregates` is locked, so both find the
    * aggregate missing before either creates it. */
  private def race(command: Any): Seq[Any] = {
    implicit val timeout: Timeout = Timeout(30.seconds)
    import scala.concurrent.ExecutionContext.Implicits.global
    val lock = DB(ConnectionPool.borrow())
    lock.begin()
    try {
      lock.withinTx(implicit session => sql"LOCK TABLE aggregates IN SHARE ROW EXCLUSIVE MODE".execute())
      val answers = Future.sequence(nodes.map(_ ? command))
      Thread.sleep(1000)
      lock.commit()
      Await.result(answers, 30.seconds)
    } finally lock.close()
  }

  private def stored(cartId: AggregateId): Aggregate[ShoppingCart] = {
    val read: Try[Aggregate[ShoppingCart]] = nodes.head ?? GetAggregate(cartId)
    read.get
  }

  feature("A first command with a chosen id, run by two owners at once") {

    scenario("an idempotent one: both are answered with the response of the one that won") {
      val cartId = AggregateId(System.nanoTime())
      race(CreateShoppingCartWithId(Some(ClientKey("race-" + cartId.asLong)), userId, cartId, "Groceries")) mustBe
        Seq.fill(2)(SuccessResponse(cartId, AggregateVersion(1)))
      stored(cartId).version mustBe AggregateVersion(1)
    }

    scenario("another one: the one that lost is answered with the conflict") {
      val cartId = AggregateId(System.nanoTime())
      val answers = race(CreateShoppingCartWithId(None, userId, cartId, "Groceries"))
      answers must contain(SuccessResponse(cartId, AggregateVersion(1)))
      answers must contain(AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion.ZERO, AggregateVersion(1)))
      stored(cartId).version mustBe AggregateVersion(1)
    }
  }
}
