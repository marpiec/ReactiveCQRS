package io.reactivecqrs.examples.cart

// Chapter 15's order, whose customer a complete rewrite erases: from the order's events, its squash event, its archive
// and a read model of orders.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{ProjectionActor, RebuildTarget, SubscriptionsState}
import scalikejdbc.DBSession

case class CustomerId(asLong: Long)

case class Order(customer: CustomerId, customerName: String, email: Option[String], items: Vector[String])

case class OrderPlaced(customer: CustomerId, customerName: String, email: Option[String]) extends FirstEvent[Order] {
  def spaceId: SpaceId = SpaceId(0)
}
case class OrderLineAdded(item: String) extends Event[Order]
/** The order's squash event: its state at the squash point. */
case class OrderSquashed(customer: CustomerId, customerName: String, email: Option[String], items: Vector[String])
  extends FirstEvent[Order] {
  def spaceId: SpaceId = SpaceId(0)
}

case class PlaceOrder(userId: UserId, customer: CustomerId, customerName: String, email: Option[String])
  extends FirstCommand[Order, CustomCommandResponse[_]]
case class AddOrderLine(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, item: String)
  extends Command[Order, CustomCommandResponse[_]]

// #eraseCustomer
import io.reactivecqrs.api.HistoryChange

case class EraseCustomer(customer: CustomerId) extends HistoryChange
// #eraseCustomer

class OrdersContext extends AggregateContext[Order] {

  override val version: Int = 1

  override def initialAggregateRoot: Order = Order(CustomerId(0), "", None, Vector.empty)

  override def commandHandlers = _ => {
    case c: PlaceOrder => CommandSuccess(OrderPlaced(c.customer, c.customerName, c.email))
    case c: AddOrderLine => CommandSuccess(OrderLineAdded(c.item))
  }

  override def eventHandlers = (_, _, order) => {
    case e: OrderPlaced => Order(e.customer, e.customerName, e.email, Vector.empty)
    case e: OrderLineAdded => order.copy(items = order.items :+ e.item)
    case e: OrderSquashed => Order(e.customer, e.customerName, e.email, e.items)
  }

  // A squash keeps the placing of the order in the archive.
  override val squash: Option[SquashPolicy[Order]] =
    Some(SquashPolicy(o => OrderSquashed(o.customer, o.customerName, o.email, o.items), keep = Set(classOf[OrderPlaced])))

  // #historyChanges
  override def historyChanges: HistoryChange => PartialFunction[Event[Order], Event[Order]] = {
    case EraseCustomer(c) => {
      case e: OrderPlaced if e.customer == c => e.copy(customerName = "erased", email = None)
      case s: OrderSquashed if s.customer == c => s.copy(customerName = "erased", email = None)
    }
    case _ => PartialFunction.empty
  }
  // #historyChanges
}

case class OrderCustomer(customerName: String, email: Option[String])
case class GetOrderCustomer(orderId: AggregateId) // answered with Option[OrderCustomer]

/** Who placed each order: a managed projection, which a complete rewrite rebuilds for the orders it changed. */
class OrderCustomers(val subscriptionsState: SubscriptionsState, store: DocumentStore[OrderCustomer]) extends ProjectionActor {

  override protected val projectionName: String = "OrderCustomers"

  protected val listeners = List(AggregateListener[Order] { (orderId, _, _, order) => implicit session =>
    order match {
      case Some(o) => store.updateDocument(0, orderId.asLong, _ => Document(OrderCustomer(o.customerName, o.email)))
      case None => store.removeDocument(orderId.asLong)
    }
  })

  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space) => store.clearSpace(space.asLong)
    case RebuildTarget.OneType(_) => store.clearAllData()
  }

  override protected def receiveQuery: Receive = {
    case GetOrderCustomer(orderId) => sender() ! store.getDocument(orderId.asLong).map(_.document)
  }
}
