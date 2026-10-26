package io.reactivecqrs.core.rebuild

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.EventStoreState

/** What a build reads of one aggregate type to give its state listeners the states (D60, D96): its restorer (snapshot
  * plus tail, D30). */
class ReplaySource(val context: AggregateContext[Any], eventStore: EventStoreState) {
  private val restorer = new AggregateRestorer[Any](eventStore, context.eventHandlers, () => context.initialAggregateRoot,
    context.snapshots, context.version, context.aggregateRootClass.getName)

  /** The aggregate's state at `version`, or at its newest version; null once it was deleted. */
  def stateAt(id: AggregateId, version: Option[AggregateVersion]): Any = restorer.restore(id, version.map(Left(_))).root
}

object ReplaySource {
  /** A source per aggregate type of `contexts`. */
  def of(contexts: Seq[AggregateContext[_]], eventStore: EventStoreState): Map[AggregateType, ReplaySource] =
    contexts.map(c => c.aggregateType -> new ReplaySource(c.asInstanceOf[AggregateContext[Any]], eventStore)).toMap
}

