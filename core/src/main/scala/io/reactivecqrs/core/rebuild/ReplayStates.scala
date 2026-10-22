package io.reactivecqrs.core.rebuild

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.{EventStoreState, RestoreFailedAtEventException}

import scala.util.control.NonFatal

/** What a build reads of one aggregate type to give its state listeners the states (D59, D60): its restorer (snapshot
  * plus tail, D30) and its event handlers. */
class ReplaySource(val context: AggregateContext[Any], eventStore: EventStoreState) {
  private val restorer = new AggregateRestorer[Any](eventStore, context.eventHandlers, () => context.initialAggregateRoot,
    context.snapshots, context.version, context.aggregateRootClass.getName)

  /** The aggregate's state at `version`, or at its newest version; null once it was deleted. */
  def stateAt(id: AggregateId, version: Option[AggregateVersion]): Any = restorer.restore(id, version.map(Left(_))).root

  /** The state after `event`, through the context's event handlers. */
  def applied(state: Any, event: EventInfo[Any]): Any = context.eventHandlers(event.userId, event.timestamp, state)(event.event)

  /** The state before the first event. */
  def initial: Any = context.initialAggregateRoot
}

object ReplaySource {
  /** A source per aggregate type of `contexts`. */
  def of(contexts: Seq[AggregateContext[_]], eventStore: EventStoreState): Map[AggregateType, ReplaySource] =
    contexts.map(c => c.aggregateType -> new ReplaySource(c.asInstanceOf[AggregateContext[Any]], eventStore)).toMap
}

/**
  * The states of the aggregates a commit-order replay has open (D59): at most `capacity`, the least recently used
  * dropped and restored from the store (snapshot plus tail) when needed again. A state is as it was at each version
  * (D60): an undo or a duplication reads the state the store gives at that version, as the aggregate's owner did when
  * it stored them; every other event goes through the event handlers. Used by one thread at a time.
  */
class ReplayStates(sources: Map[AggregateType, ReplaySource], capacity: Int) {

  private val states = new java.util.LinkedHashMap[AggregateId, (AggregateVersion, Any)](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[AggregateId, (AggregateVersion, Any)]): Boolean = size() > capacity
  }

  /** The aggregate's state after `events`, its consecutive events from a version on; `squashedUpTo`: its squash point. */
  def after(aggregateType: AggregateType, id: AggregateId, events: Seq[EventInfo[Any]], squashedUpTo: Option[AggregateVersion]): Any = {
    val source = sources.getOrElse(aggregateType, throw new IllegalStateException(
      "No aggregate context of " + aggregateType.typeName + " for the build: its state listeners need the states"))
    val first = events.head.version
    val known = Option(states.get(id)).collect { case (version, state) if version.asInt == first.asInt - 1 => state }
    var state: Any = known.getOrElse {
      // The aggregate's first event, or its squash event (nothing exists before it), apply to the initial state.
      if (first.isOne || squashedUpTo.contains(first)) source.initial else {
        restores += 1
        source.stateAt(id, Some(AggregateVersion(first.asInt - 1)))
      }
    }
    events.foreach { event =>
      state = event.event match {
        case duplication: DuplicationEvent[_] => baseState(duplication).map(source.applied(_, event)).getOrElse(source.stateAt(id, Some(event.version)))
        case _: UndoEvent[_] => source.stateAt(id, Some(event.version))
        case _ =>
          // CR-4: a handler that throws names the event it failed at.
          try source.applied(state, event) catch {
            case NonFatal(e) => throw new RestoreFailedAtEventException(id, event.version.asInt, event.event.getClass.getName, None, unreadable = false, e)
          }
      }
    }
    states.put(id, (events.last.version, state))
    state
  }

  // D59: a duplicate starts from its base's state when the replay has the base exactly at the duplicated version.
  private def baseState(duplication: DuplicationEvent[_]): Option[Any] =
    Option(states.get(duplication.baseAggregateId)).collect { case (version, state) if version == duplication.baseAggregateVersion => state }

  /** How many states are open. */
  def open: Int = states.size()

  /** CR-7: how many states were read again from the store, as more were open than `capacity` (or a run came back). */
  def restored: Long = restores

  private var restores = 0L
}
