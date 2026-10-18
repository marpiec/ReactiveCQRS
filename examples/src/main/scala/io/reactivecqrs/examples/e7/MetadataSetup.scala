package io.reactivecqrs.examples.e7

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.examples.e1.BasicSetup
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * E7, metadata (chapter 6): a command that carries an envelope (who acted, the case it belongs to, the trace context)
  * gives it to its events; an audit trail reads it from each event, and so does a read of the history. Run it with
  * `sbt "examples/runMain io.reactivecqrs.examples.e7.MetadataSetup"`; `MetadataSpec` runs the same code against the
  * test database.
  */
object MetadataSetup {

  final case class Desk(tickets: ActorRef, audit: ActorRef)

  def start(system: ActorSystem): Desk = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val tickets = system.actorOf(AggregateCommandBusActor(new TicketContext, uidGenerator, eventStore, responses, eventBus), "tickets")
    val audit = system.actorOf(Props(new TicketAuditProjection(subscriptions,
      new PostgresDocumentStore[AuditTrail]("e7_ticket_audit", mpjsons, new NoopDocumentStoreCache))).withDispatcher(BlockingDispatcher.id), "ticketAudit")
    eventBus ! EventsBusActor.RegisterSubscribers(List(audit))
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Desk(tickets, audit)
  }

  /** A ticket opened by a user, and a note an assistant added for them in a support case. */
  def openAndNote(desk: Desk, user: UserId)(implicit timeout: Timeout): AggregateId = {
    def send(command: AnyRef): SuccessResponse = Await.result((desk.tickets ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    // #commands
    val opened = send(OpenTicket(user, "Printer offline"))
    val onBehalf = EventMetadata("agent:assistant", correlationId = Some("12"),
      traceContext = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
    send(AddNote(user, opened.aggregateId, opened.aggregateVersion, "restarted the spooler", onBehalf))
    // #commands
    opened.aggregateId
  }

  def history(desk: Desk, ticketId: AggregateId)(implicit timeout: Timeout): List[EventInfo[Ticket]] =
    Await.result((desk.tickets ? GetEventsForAggregate(ticketId)).mapTo[Try[List[EventInfo[Ticket]]]], timeout.duration).get

  def auditTrail(desk: Desk, ticketId: AggregateId)(implicit timeout: Timeout): Option[AuditTrail] =
    Await.result((desk.audit ? GetAuditTrail(ticketId)).mapTo[Option[AuditTrail]], timeout.duration)

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    val system = ActorSystem("e7")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val desk = start(system)
      val ticketId = openAndNote(desk, UserId(1))
      history(desk, ticketId).foreach(e => println(s"${e.version.asInt}  ${e.event}  ${e.metadata.actor}"))
      Thread.sleep(1000) // the projection follows the commit
      println("Audit trail: " + auditTrail(desk, ticketId))
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}
