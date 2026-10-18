package io.reactivecqrs.examples.e8

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.BasicSetup
import io.reactivecqrs.examples.e5.{CreateList, ShoppingList, ShoppingListContext, WriteItem}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * E8, ephemeral aggregates (chapter 20): drafts of E5's shopping lists live in this node's memory only, under negative
  * ids, with nothing stored or published; the one worth keeping is materialized into a stored list, from the type's
  * squash event. Run it with `sbt "examples/runMain io.reactivecqrs.examples.e8.DraftsSetup"`; `DraftsSpec` runs the same
  * code against the test database.
  */
object DraftsSetup {

  final case class Lists(lists: ActorRef, drafts: ActorRef)

  def start(system: ActorSystem): Lists = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    // #buses
    val lists = system.actorOf(
      AggregateCommandBusActor(new ShoppingListContext, uidGenerator, eventStore, responses, eventBus), "lists")
    val drafts = system.actorOf(
      AggregateCommandBusActor.ephemeral(new ShoppingListContext, persistentBus = lists, uidGenerator, mpjsons), "draftLists")
    // #buses
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Lists(lists, drafts)
  }

  /** A draft list written in memory, then kept: the draft's id, the stored list's id and what happens to the draft after. */
  final case class Kept(draftId: AggregateId, materialized: MaterializeResponse, stored: Aggregate[ShoppingList], draftAfter: CustomCommandResponse[_])

  def draftAndKeep(buses: Lists, user: UserId)(implicit timeout: Timeout): Kept = {
    def send(bus: ActorRef, command: AnyRef): CustomCommandResponse[_] = Await.result((bus ? command).mapTo[CustomCommandResponse[_]], timeout.duration)
    // #draft
    val draft = send(buses.drafts, CreateList(user, "Maybe this weekend")).asInstanceOf[SuccessResponse] // a negative id
    send(buses.drafts, WriteItem(user, draft.aggregateId, draft.aggregateVersion, "tent"))
    send(buses.drafts, WriteItem(user, draft.aggregateId, AggregateVersion(2), "stove"))
    // #draft
    // #keep
    val kept = Await.result((buses.drafts ? Materialize(draft.aggregateId, user)).mapTo[MaterializeResponse], timeout.duration)
    // #keep
    val stored = kept match {
      case Materialized(_, persistentId) => Await.result((buses.lists ? GetAggregate(persistentId)).mapTo[Try[Aggregate[ShoppingList]]], timeout.duration).get
      case refused => throw new IllegalStateException("Not kept: " + refused)
    }
    Kept(draft.aggregateId, kept, stored, send(buses.drafts, WriteItem(user, draft.aggregateId, AggregateVersion(3), "matches")))
  }

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e8")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val kept = draftAndKeep(start(system), UserId(1))
      println("Draft " + kept.draftId + " kept as: " + kept.materialized)            // Materialized(Id(-...), Id(...))
      println("The stored list: " + kept.stored.aggregateRoot + " at " + kept.stored.version) // tent and stove, at version 1
      println("The draft afterwards: " + kept.draftAfter)                                  // EphemeralAggregateUnknown(...)
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
