package io.reactivecqrs.bench

import io.mpjsons.jsonName
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import scalikejdbc.DBSession

// The domain of the short JSON keys measurement (IMPLEMENTATION-PLAN §7.1): one business event and one document, with
// the descriptive keys Neula's classes have and with one-character keys (mpjsons `@jsonName`), on aggregate types of
// their own so the store keeps them apart. Only the keys differ: the values are the same.

case class Order(lines: Int)
case class ShortOrder(lines: Int)

case class OrderOpened(spaceId: SpaceId) extends FirstEvent[Order]
case class ShortOrderOpened(spaceId: SpaceId) extends FirstEvent[ShortOrder]

case class OrderLineAdded(productIdentifier: String, productDisplayName: String, quantityOrdered: Int, unitPriceInCents: Long,
                          discountPercentage: Int, warehouseLocationCode: String, requestedDeliveryDate: String,
                          customerReference: String) extends Event[Order]

case class ShortOrderLineAdded(@jsonName("p") productIdentifier: String, @jsonName("n") productDisplayName: String,
                               @jsonName("q") quantityOrdered: Int, @jsonName("u") unitPriceInCents: Long,
                               @jsonName("d") discountPercentage: Int, @jsonName("w") warehouseLocationCode: String,
                               @jsonName("r") requestedDeliveryDate: String, @jsonName("c") customerReference: String) extends Event[ShortOrder]

/** The control: the counter's event, whose 200-character note outweighs its one key, with a one-character key. Never stored. */
case class IncrementedShortKey(@jsonName("n") note: String) extends Event[Counter]

/** What a projection of the orders keeps per order: the same fields, keyed the two ways. */
case class OrderSummary(productDisplayName: String, quantityOrdered: Int, unitPriceInCents: Long, warehouseLocationCode: String,
                        customerReference: String, lineCount: Int)
case class ShortOrderSummary(@jsonName("n") productDisplayName: String, @jsonName("q") quantityOrdered: Int,
                             @jsonName("u") unitPriceInCents: Long, @jsonName("w") warehouseLocationCode: String,
                             @jsonName("c") customerReference: String, @jsonName("l") lineCount: Int)

case class OpenOrder(userId: UserId) extends FirstCommand[Order, CustomCommandResponse[_]]
case class AddOrderLine(userId: UserId, aggregateId: AggregateId, line: OrderLineAdded) extends ConcurrentCommand[Order, CustomCommandResponse[_]]
case class OpenShortOrder(userId: UserId) extends FirstCommand[ShortOrder, CustomCommandResponse[_]]
case class AddShortOrderLine(userId: UserId, aggregateId: AggregateId, line: ShortOrderLineAdded)
  extends ConcurrentCommand[ShortOrder, CustomCommandResponse[_]]

class OrderContext extends AggregateContext[Order] {
  override val version: Int = 1
  override def initialAggregateRoot: Order = Order(0)
  override def commandHandlers = _ => {
    case _: OpenOrder => CommandSuccess(OrderOpened(SpaceId(0)))
    case c: AddOrderLine => CommandSuccess(c.line)
  }
  override def eventHandlers = (_, _, order) => {
    case _: OrderOpened => Order(0)
    case _: OrderLineAdded => Order(order.lines + 1)
  }
}

class ShortOrderContext extends AggregateContext[ShortOrder] {
  override val version: Int = 1
  override def initialAggregateRoot: ShortOrder = ShortOrder(0)
  override def commandHandlers = _ => {
    case _: OpenShortOrder => CommandSuccess(ShortOrderOpened(SpaceId(0)))
    case c: AddShortOrderLine => CommandSuccess(c.line)
  }
  override def eventHandlers = (_, _, order) => {
    case _: ShortOrderOpened => ShortOrder(0)
    case _: ShortOrderLineAdded => ShortOrder(order.lines + 1)
  }
}

/** Order lines with short values, as Neula's are: codes, a product name, numbers, dates. */
object OrderLines {
  private val products = Vector("Blue mug", "Steel kettle", "Oak cutting board", "Linen napkins (4)", "Ceramic bowl")

  def long(i: Int): OrderLineAdded = OrderLineAdded("SKU-" + (100000 + i % 900000), products(i % products.size), 1 + i % 5, 499L + i % 3000,
    i % 25, "A-" + (i % 40) + "-" + (i % 9), "2026-10-" + (10 + i % 18), "PO-" + (80000 + i % 20000))

  def short(i: Int): ShortOrderLineAdded = {
    val l = long(i)
    ShortOrderLineAdded(l.productIdentifier, l.productDisplayName, l.quantityOrdered, l.unitPriceInCents, l.discountPercentage,
      l.warehouseLocationCode, l.requestedDeliveryDate, l.customerReference)
  }

  def summary(i: Int, lineCount: Int): OrderSummary = {
    val l = long(i)
    OrderSummary(l.productDisplayName, l.quantityOrdered, l.unitPriceInCents, l.warehouseLocationCode, l.customerReference, lineCount)
  }

  def shortSummary(i: Int, lineCount: Int): ShortOrderSummary = {
    val s = summary(i, lineCount)
    ShortOrderSummary(s.productDisplayName, s.quantityOrdered, s.unitPriceInCents, s.warehouseLocationCode, s.customerReference, s.lineCount)
  }
}

/** Keeps each order's last line and its line count, from its events, as most of Neula's projections do: each event
  * reads the order's document and writes it back. The rebuild of the measurement starts one per run, managed (M4). */
class OrderSummaries(val subscriptionsState: SubscriptionsState, documents: DocumentStore[OrderSummary],
                     override protected val projectionName: String) extends ProjectionActor {

  protected val listeners = List(EventsListener(update))

  private def update(id: AggregateId, events: Seq[EventInfo[Order]]) = { implicit session: DBSession =>
    events.foreach(_.event match {
      case _: OrderOpened => documents.insertDocument(0, id.asLong, OrderSummary("", 0, 0, "", "", 0))
      case l: OrderLineAdded => documents.updateDocument(0, id.asLong, { document =>
        Document(OrderSummary(l.productDisplayName, l.quantityOrdered, l.unitPriceInCents, l.warehouseLocationCode, l.customerReference,
          document.map(_.document.lineCount).getOrElse(0) + 1))
      })
      case _ => ()
    })
  }

  override protected def receiveQuery: Receive = PartialFunction.empty
}

class ShortOrderSummaries(val subscriptionsState: SubscriptionsState, documents: DocumentStore[ShortOrderSummary],
                          override protected val projectionName: String) extends ProjectionActor {

  protected val listeners = List(EventsListener(update))

  private def update(id: AggregateId, events: Seq[EventInfo[ShortOrder]]) = { implicit session: DBSession =>
    events.foreach(_.event match {
      case _: ShortOrderOpened => documents.insertDocument(0, id.asLong, ShortOrderSummary("", 0, 0, "", "", 0))
      case l: ShortOrderLineAdded => documents.updateDocument(0, id.asLong, { document =>
        Document(ShortOrderSummary(l.productDisplayName, l.quantityOrdered, l.unitPriceInCents, l.warehouseLocationCode, l.customerReference,
          document.map(_.document.lineCount).getOrElse(0) + 1))
      })
      case _ => ()
    })
  }

  override protected def receiveQuery: Receive = PartialFunction.empty
}
