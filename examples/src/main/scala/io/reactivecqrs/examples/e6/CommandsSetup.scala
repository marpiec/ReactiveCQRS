package io.reactivecqrs.examples.e6

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
import scalikejdbc._

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * E6, command processing (chapter 5): a command sent twice with one key takes effect once; bookings of one concert run
  * one at a time, so none is sold twice; and each booking's ticket row is written in the transaction of its event. Run
  * it with `sbt "examples/runMain io.reactivecqrs.examples.e6.CommandsSetup"`; `CommandsSpec` runs the same code
  * against the test database.
  */
object CommandsSetup {

  def start(system: ActorSystem): ActorRef = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    DB.autoCommit(implicit session =>
      sql"CREATE TABLE IF NOT EXISTS e6_tickets (concert_id BIGINT NOT NULL, version INT NOT NULL, customer TEXT NOT NULL)".execute())
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val concerts = system.actorOf(AggregateCommandBusActor(new ConcertContext, uidGenerator, eventStore, responses, eventBus), "concerts")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    concerts
  }

  def send(concerts: ActorRef, command: AnyRef)(implicit timeout: Timeout): CustomCommandResponse[_] =
    Await.result((concerts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)

  // #idempotent
  /** The same request sent twice, as a client retrying it would: one concert, and the same answer for both. */
  def scheduleTwice(concerts: ActorRef, user: UserId, seats: Int)(implicit timeout: Timeout): (CustomCommandResponse[_], CustomCommandResponse[_]) = {
    val key = Some(RequestKey(UUID.randomUUID().toString)) // one key per user action
    val schedule = ScheduleConcert(key, user, "Evening concert", seats)
    (send(concerts, schedule), send(concerts, schedule))
  }
  // #idempotent

  // #bookings
  /** `customers` bookings of one concert sent at once; each is answered on its own. */
  def bookAtOnce(concerts: ActorRef, user: UserId, concertId: AggregateId, customers: Int)(implicit timeout: Timeout): Seq[CustomCommandResponse[_]] = {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val sent = (1 to customers).map(n => (concerts ? BookSeat(user, concertId, "customer " + n)).mapTo[CustomCommandResponse[_]])
    Await.result(Future.sequence(sent), timeout.duration)
  }
  // #bookings

  def tickets(concertId: AggregateId): Int =
    DB.readOnly(implicit session => sql"SELECT count(*) FROM e6_tickets WHERE concert_id = ${concertId.asLong}".map(_.int(1)).single().get)

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e6")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val concerts = start(system)
      val (first, again) = scheduleTwice(concerts, UserId(1), seats = 5)
      println("Scheduled: " + first + ", and the retry: " + again) // the same SuccessResponse twice
      val concertId = first.asInstanceOf[SuccessResponse].aggregateId
      val answers = bookAtOnce(concerts, UserId(1), concertId, customers = 8)
      println("Booked: " + answers.count(_.isInstanceOf[SuccessResponse]) + ", refused: " + answers.count(_.isInstanceOf[FailureResponse]) +
        ", tickets: " + tickets(concertId)) // 5, 3, 5
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
