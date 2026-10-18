package io.reactivecqrs.examples.cart

// Chapter 6's library loan: event handlers that use the user and the time of their event.

// #loan
import java.time.Instant
import java.time.temporal.ChronoUnit
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}

case class Loan(title: String, borrower: UserId, due: Instant, returned: Boolean)

case class LoanStarted(title: String) extends FirstEvent[Loan] {
  def spaceId: SpaceId = SpaceId(0)
}
case class LoanExtended(days: Int) extends Event[Loan]
case class BookReturned() extends Event[Loan]
// #loan

class LoanContext extends AggregateContext[Loan] {

  override val version: Int = 1

  override def initialAggregateRoot: Loan = Loan("", UserId(0), Instant.EPOCH, returned = false)

  override def commandHandlers = _ => PartialFunction.empty // the chapter is about the event handlers

  // #loanHandlers
  override def eventHandlers = (userId, timestamp, loan) => {
    case e: LoanStarted  => Loan(e.title, borrower = userId, due = timestamp.plus(14, ChronoUnit.DAYS), returned = false)
    case e: LoanExtended => loan.copy(due = loan.due.plus(e.days.toLong, ChronoUnit.DAYS))
    case _: BookReturned => loan.copy(returned = true)
  }
  // #loanHandlers
}
