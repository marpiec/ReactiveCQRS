package io.reactivecqrs.examples.e5

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

// E5's aggregate: a shopping list whose history can be undone, copied and squashed, with snapshots on.

// #domain
case class ShoppingList(name: String, items: Vector[String])

case class ListCreated(name: String) extends FirstEvent[ShoppingList] {
  def spaceId: SpaceId = SpaceId(0)
}
case class ItemWritten(item: String) extends Event[ShoppingList]
case class ChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingList]
case class ListCopied(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[ShoppingList]
/** The squash event: the list's whole state at the squash point. */
case class ListSquashed(spaceId: SpaceId, name: String, items: Vector[String]) extends FirstEvent[ShoppingList]

case class CreateList(userId: UserId, name: String) extends FirstCommand[ShoppingList, CustomCommandResponse[_]]
case class WriteItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, item: String)
  extends Command[ShoppingList, CustomCommandResponse[_]]
case class UndoChanges(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, steps: Int)
  extends Command[ShoppingList, CustomCommandResponse[_]]
case class CopyList(userId: UserId, baseId: AggregateId, baseVersion: AggregateVersion)
  extends FirstCommand[ShoppingList, CustomCommandResponse[_]]

class ShoppingListContext extends AggregateContext[ShoppingList] {
  override val version: Int = 1
  override def initialAggregateRoot: ShoppingList = ShoppingList("", Vector.empty)

  override def commandHandlers = list => {
    case c: CreateList => CommandSuccess(ListCreated(c.name))
    case c: WriteItem => CommandSuccess(ItemWritten(c.item))
    case c: UndoChanges => CommandSuccess(ChangesUndone(c.steps))
    case c: CopyList => CommandSuccess(ListCopied(SpaceId(0), c.baseId, c.baseVersion))
  }

  override def eventHandlers = (_, _, list) => {
    case e: ListCreated => ShoppingList(e.name, Vector.empty)
    case e: ItemWritten => list.copy(items = list.items :+ e.item)
    case _: ListCopied => list // the state that arrives is already the copy's
    case e: ListSquashed => ShoppingList(e.name, e.items)
  }

  // #policies
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = 3)) // small, to watch it work
  override val squash: Option[SquashPolicy[ShoppingList]] =
    Some(SquashPolicy(list => ListSquashed(SpaceId(0), list.name, list.items)))
  // #policies
}
// #domain
