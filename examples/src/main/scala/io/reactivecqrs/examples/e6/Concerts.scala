package io.reactivecqrs.examples.e6

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.{OutboxWriter, WithOutboxWriter}
import scalikejdbc._

// E6's aggregate: a concert whose seats are booked one at a time (serial processing), scheduled with a key the client
// chooses (an idempotent command), and whose bookings become ticket rows written with the events (the outbox hook).

// #domain
case class Concert(name: String, seatsLeft: Int)

case class ConcertScheduled(name: String, seats: Int) extends FirstEvent[Concert] {
  def spaceId: SpaceId = SpaceId(0)
}
case class SeatBooked(customer: String) extends Event[Concert]

/** A key the client chooses for one request, and sends again with a retry of it. */
case class RequestKey(value: String) extends CommandIdempotencyId {
  override def asDbKey: String = "e6|" + value
}

case class ScheduleConcert(idempotencyId: Option[RequestKey], userId: UserId, name: String, seats: Int)
  extends FirstCommand[Concert, CustomCommandResponse[_]] with IdempotentCommand[RequestKey]
case class BookSeat(userId: UserId, aggregateId: AggregateId, customer: String)
  extends ConcurrentCommand[Concert, CustomCommandResponse[_]]
// #domain

// #context
class ConcertContext extends AggregateContext[Concert] with WithOutboxWriter[Concert] {
  override val version: Int = 1
  override def initialAggregateRoot: Concert = Concert("", 0)

  override def commandHandlers = concert => {
    case c: ScheduleConcert => CommandSuccess(ConcertScheduled(c.name, c.seats))
    case c: BookSeat => // runs once, on the state the booking before it left (Serial)
      if (concert.seatsLeft > 0) CommandSuccess(SeatBooked(c.customer)) else CommandFailure("Sold out")
  }

  override def eventHandlers = (_, _, concert) => {
    case e: ConcertScheduled => Concert(e.name, e.seats)
    case _: SeatBooked => concert.copy(seatsLeft = concert.seatsLeft - 1)
  }

  // One booking at a time per concert; at most 20 wait, one more is answered CommandRejected at once.
  override val commandProcessing: CommandProcessing = CommandProcessing.Serial(maxQueuePerAggregate = 20)

  // #outboxWriter
  // A ticket to email for each booking, in the transaction of its event: both are stored, or neither.
  override def outboxWriter: OutboxWriter[Concert] = new OutboxWriter[Concert] {
    override def write(concertId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[Concert]])(implicit session: DBSession): Unit =
      events.foreach(_.event match {
        case SeatBooked(customer) =>
          sql"INSERT INTO e6_tickets (concert_id, version, customer) VALUES (${concertId.asLong}, ${version.asInt}, $customer)".update()
        case _ => ()
      })
  }
  // #outboxWriter
}
// #context
