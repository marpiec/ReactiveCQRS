package io.reactivecqrs.examples.e9

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

// E9's aggregate: a memo of an organization, whose space its first event names (chapter 16).

// #domain
case class Memo(space: SpaceId, text: String)

case class MemoWritten(spaceId: SpaceId, text: String) extends FirstEvent[Memo] // the memo's organization
case class MemoEdited(text: String) extends Event[Memo]
/** The squash event, which also detaches copies in other organizations before an export or a purge. */
case class MemoSquashed(spaceId: SpaceId, text: String) extends FirstEvent[Memo]

case class WriteMemo(userId: UserId, space: SpaceId, text: String) extends FirstCommand[Memo, CustomCommandResponse[_]]
case class EditMemo(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, text: String)
  extends Command[Memo, CustomCommandResponse[_]]

class MemoContext extends AggregateContext[Memo] {
  override val version: Int = 1
  override def initialAggregateRoot: Memo = Memo(SpaceId(0), "")

  override def commandHandlers = _ => {
    case c: WriteMemo => CommandSuccess(MemoWritten(c.space, c.text))
    case c: EditMemo => CommandSuccess(MemoEdited(c.text))
  }

  override def eventHandlers = (_, _, memo) => {
    case e: MemoWritten => Memo(e.spaceId, e.text)
    case e: MemoEdited => memo.copy(text = e.text)
    case e: MemoSquashed => Memo(e.spaceId, e.text)
  }

  override val squash: Option[SquashPolicy[Memo]] = Some(SquashPolicy(memo => MemoSquashed(memo.space, memo.text)))
}
// #domain
