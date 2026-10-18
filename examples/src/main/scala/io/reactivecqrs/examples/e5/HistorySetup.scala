package io.reactivecqrs.examples.e5

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
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * E5, the history of an aggregate (chapters 6, 7, 13 and 14): snapshots, an undo, a copy, and a squash that folds the
  * oldest events into one, after detaching the copy that read them. Run it with `sbt "examples/runMain
  * io.reactivecqrs.examples.e5.HistorySetup"`; `HistorySpec` runs the same code against the test database.
  */
object HistorySetup {

  def start(system: ActorSystem): ActorRef = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val lists = system.actorOf(AggregateCommandBusActor(new ShoppingListContext, uidGenerator, eventStore, responses, eventBus), "lists")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    lists
  }

  /** What the story leaves: the list and its copy, as they are, and the list as it was at two versions. */
  final case class Story(listId: AggregateId, list: Aggregate[ShoppingList], copy: Aggregate[ShoppingList], squashed: SquashResponse,
                         atVersion2: Try[Aggregate[ShoppingList]], atVersion4: Try[Aggregate[ShoppingList]])

  def story(lists: ActorRef, user: UserId)(implicit timeout: Timeout): Story = {
    def send(command: AnyRef): SuccessResponse = Await.result((lists ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    def read(message: AnyRef): Try[Aggregate[ShoppingList]] = Await.result((lists ? message).mapTo[Try[Aggregate[ShoppingList]]], timeout.duration)

    // #story
    val listId = send(CreateList(user, "Weekend")).aggregateId                     // version 1
    val written = Seq("apples", "pears", "plums", "figs").foldLeft(AggregateVersion(1)) { (version, item) =>
      send(WriteItem(user, listId, version, item)).aggregateVersion                  // versions 2 to 5
    }
    send(UndoChanges(user, listId, written, steps = 1))                             // version 6: the figs are undone
    val copyId = send(CopyList(user, listId, AggregateVersion(3))).aggregateId      // a copy of the list at version 3
    send(WriteItem(user, copyId, AggregateVersion(1), "bread"))                     // the copy lives on its own
    val squashed = Await.result((lists ? SquashAggregate(listId, AggregateVersion(4), user)).mapTo[SquashResponse], timeout.duration)
    // #story

    Story(listId, read(GetAggregate(listId)).get, read(GetAggregate(copyId)).get, squashed,
      read(GetAggregateForVersion(listId, AggregateVersion(2))), read(GetAggregateForVersion(listId, AggregateVersion(4))))
  }

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e5")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val told = story(start(system), UserId(1))
      println("The list: " + told.list.aggregateRoot + " at " + told.list.version) // apples, pears, plums at version 6
      println("Its copy: " + told.copy.aggregateRoot)                              // apples, pears, bread
      println("The squash: " + told.squashed)                                       // AggregateSquashed(..., 4)
      println("At version 2: " + told.atVersion2)                                   // refused: below the squash point
      println("At version 4: " + told.atVersion4.map(_.aggregateRoot))             // apples, pears, plums
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
