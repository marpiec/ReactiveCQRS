package io.reactivecqrs.examples.e7

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}

// E7's aggregate, a support ticket, and an audit trail that reads each event's metadata envelope.

// #domain
case class Ticket(title: String, notes: Vector[String])

case class TicketOpened(title: String) extends FirstEvent[Ticket] {
  def spaceId: SpaceId = SpaceId(0)
}
case class NoteAdded(text: String) extends Event[Ticket]

case class OpenTicket(userId: UserId, title: String) extends FirstCommand[Ticket, CustomCommandResponse[_]]
/** A note added for someone: its event carries the envelope, who acted and in which case. */
case class AddNote(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, text: String,
                   metadata: EventMetadata) extends Command[Ticket, CustomCommandResponse[_]] with WithMetadata

class TicketContext extends AggregateContext[Ticket] {
  override val version: Int = 1
  override def initialAggregateRoot: Ticket = Ticket("", Vector.empty)

  override def commandHandlers = _ => {
    case c: OpenTicket => CommandSuccess(TicketOpened(c.title))
    case c: AddNote => CommandSuccess(NoteAdded(c.text))
  }

  override def eventHandlers = (_, _, ticket) => { // the envelope never reaches them
    case e: TicketOpened => Ticket(e.title, Vector.empty)
    case e: NoteAdded => ticket.copy(notes = ticket.notes :+ e.text)
  }
}
// #domain

// #audit
case class AuditTrail(lines: Vector[String])
case class GetAuditTrail(ticketId: AggregateId) // answered with Option[AuditTrail]

class TicketAuditProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[AuditTrail]) extends ProjectionActor {

  override protected val projectionName: String = "TicketAudit"

  protected val listeners = List(EventsListener[Ticket] { (ticketId, events) => implicit session =>
    val lines = events.map { e =>
      val m = e.metadata // an event stored without an envelope reads as EventMetadata.ofUser(its user)
      val context = (m.correlationId.map("case " + _) ++ m.traceContext.map("trace " + _)).mkString(", ")
      val what = e.event match {
        case TicketOpened(title) => "opened " + title
        case NoteAdded(text) => "noted " + text
        case other => other.getClass.getSimpleName
      }
      m.actor + (if (context.isEmpty) "" else " (" + context + ")") + ": " + what
    }
    store.updateDocument(0, ticketId.asLong, existing => Document(AuditTrail(existing.map(_.document.lines).getOrElse(Vector.empty) ++ lines)))
  })

  override protected def receiveQuery: Receive = {
    case GetAuditTrail(ticketId) => sender() ! store.getDocument(ticketId.asLong).map(_.document)
  }
}
// #audit
