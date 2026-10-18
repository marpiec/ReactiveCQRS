package io.reactivecqrs.core.aggregate

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateContext, AggregateVersion, EventInfo, HistoryChange}
import scalikejdbc.DBSession

/**
  * Writes rows of the application's own in the transaction that stores a command's events (R12, D33): jobs, timers,
  * messages to send. A failure rolls the events back with them; a copy of an idempotent command that was applied
  * already never reaches it. Cache updates it makes through `AfterCommit` take effect once the transaction committed.
  */
trait OutboxWriter[AGGREGATE_ROOT] {
  /** `version`: the aggregate's version after these events. */
  def write(aggregateId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[AGGREGATE_ROOT]])(implicit session: DBSession): Unit

  /** D76: changes the rows this writer wrote for the aggregate as `change` asks (an erasure), in the transaction of the
    * aggregate's complete rewrite. It may run more than once for one change. Nothing by default. */
  def rewrite(aggregateId: AggregateId, change: HistoryChange)(implicit session: DBSession): Unit = ()
}

/** Mixed into the `AggregateContext` of a type whose commands write such rows. */
trait WithOutboxWriter[AGGREGATE_ROOT] { self: AggregateContext[AGGREGATE_ROOT] =>
  /** The writer of the type's rows. */
  def outboxWriter: OutboxWriter[AGGREGATE_ROOT]
}
