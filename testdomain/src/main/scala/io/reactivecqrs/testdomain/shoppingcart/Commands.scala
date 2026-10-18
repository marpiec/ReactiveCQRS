package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}

// The regions (`// #name`) are code blocks of the book, docs/book (BookSnippetsSpec checks them).

// #create
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
// #create

// #duplicate
case class DuplicateShoppingCart(userId: UserId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]
// #duplicate

// #addItem
case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]
// #addItem
// #onBehalf
/** Adds an item for someone else: its event carries a metadata envelope. */
case class AddItemOnBehalf(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String,
                           metadata: EventMetadata)
  extends Command[ShoppingCart, CustomCommandResponse[_]] with WithMetadata
// #onBehalf
/** Adds several items in one command, one event each. */
case class AddItems(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, names: Seq[String])
  extends Command[ShoppingCart, CustomCommandResponse[_]]

// #concurrently
// No expected version: the framework uses the current one and retries on a concurrent change.
case class AddItemConcurrently(userId: UserId, aggregateId: AggregateId, name: String,
                               idempotencyId: Option[ClientKey] = None)
  extends ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey]
// #concurrently

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]

// #undo
case class UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                                  stepsToUndo: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
// #undo

case class DeleteShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]


// #rewriteCommands
case class RewriteCartName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                           name: String) extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}

/** Renames the cart throughout its history, on whatever version it has (like an anonymization). */
case class RewriteCartNameConcurrently(userId: UserId, aggregateId: AggregateId, name: String)
  extends RewriteHistoryConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}
// #rewriteCommands

// #clientKey
/** A key chosen by the caller, the way a client retrying a request would send it. */
case class ClientKey(value: String) extends CommandIdempotencyId {
  override def asDbKey: String = value
}
// #clientKey

// #createWithId
/** Creates a cart under an id the caller reserved beforehand, so a retry cannot create a second cart. */
case class CreateShoppingCartWithId(idempotencyId: Option[ClientKey], userId: UserId, cartId: AggregateId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey] {
  override def chosenAggregateId: Option[AggregateId] = Some(cartId)
}
// #createWithId

/** Adds an item and answers with a receipt, so the stored response carries custom info. */
case class AddItemWithReceipt(idempotencyId: Option[ClientKey], userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                              name: String) extends Command[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey]

case class ItemReceipt(itemName: String)

/** A recurring check that changes nothing: its handler returns no events (Neula R9). */
case class CheckCart(userId: UserId, aggregateId: AggregateId) extends ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]]
