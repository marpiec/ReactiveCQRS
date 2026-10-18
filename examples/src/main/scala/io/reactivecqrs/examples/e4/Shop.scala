package io.reactivecqrs.examples.e4

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

// E4's aggregates: a stock of items and a customer's payment account, each a command bus of its own. The saga's
// commands carry its `SagaStep` as their idempotency key, so a step that runs again applies nothing twice.

// #stock
case class Stock(available: Int)

case class StockOpened(available: Int) extends FirstEvent[Stock] {
  def spaceId: SpaceId = SpaceId(0)
}
case class ItemsReserved(items: Int) extends Event[Stock]
case class ItemsReleased(items: Int) extends Event[Stock]

case class OpenStock(userId: UserId, available: Int) extends FirstCommand[Stock, CustomCommandResponse[_]]
// #reserveItems
case class ReserveItems(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, items: Int)
  extends ConcurrentCommand[Stock, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
// #reserveItems
case class ReleaseItems(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, items: Int)
  extends ConcurrentCommand[Stock, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

class StockContext extends AggregateContext[Stock] {
  override val version: Int = 1
  override def initialAggregateRoot: Stock = Stock(0)

  override def commandHandlers = stock => {
    case c: OpenStock => CommandSuccess(StockOpened(c.available))
    case c: ReserveItems =>
      if (c.items > stock.available) CommandFailure("Only " + stock.available + " items left")
      else CommandSuccess(ItemsReserved(c.items))
    case c: ReleaseItems => CommandSuccess(ItemsReleased(c.items))
  }

  override def eventHandlers = (_, _, stock) => {
    case e: StockOpened => Stock(e.available)
    case e: ItemsReserved => Stock(stock.available - e.items)
    case e: ItemsReleased => Stock(stock.available + e.items)
  }
}
// #stock

// #payment
case class Payment(limitCents: Long, chargedCents: Long)

case class PaymentOpened(limitCents: Long) extends FirstEvent[Payment] {
  def spaceId: SpaceId = SpaceId(0)
}
case class PaymentCharged(amountCents: Long) extends Event[Payment]

case class OpenPayment(userId: UserId, limitCents: Long) extends FirstCommand[Payment, CustomCommandResponse[_]]
case class ChargePayment(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, amountCents: Long)
  extends ConcurrentCommand[Payment, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

class PaymentContext extends AggregateContext[Payment] {
  override val version: Int = 1
  override def initialAggregateRoot: Payment = Payment(0, 0)

  override def commandHandlers = payment => {
    case c: OpenPayment => CommandSuccess(PaymentOpened(c.limitCents))
    case c: ChargePayment =>
      if (payment.chargedCents + c.amountCents > payment.limitCents) CommandFailure("Over the limit")
      else CommandSuccess(PaymentCharged(c.amountCents))
  }

  override def eventHandlers = (_, _, payment) => {
    case e: PaymentOpened => Payment(e.limitCents, 0)
    case e: PaymentCharged => payment.copy(chargedCents = payment.chargedCents + e.amountCents)
  }
}
// #payment
