package io.reactivecqrs.core.aggregate

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventstore.EventStoreState

/** An aggregate's version and root as read from the store; the root is null once the aggregate was deleted. */
case class Restored[AGGREGATE_ROOT](version: AggregateVersion, root: AGGREGATE_ROOT)

/**
  * Reads aggregates of one type from the event store: their state at the newest version, at a version or at an
  * instant, and the events behind it. It hides how the store keeps a history: duplication chains (a duplicate starts
  * with its base's events) and undone events. The owner restores through it, and the command bus answers historical
  * reads with it. Synchronous: it reads on the caller's thread.
  */
class AggregateRestorer[AGGREGATE_ROOT](eventStore: EventStoreState,
                                        eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                        initialState: () => AGGREGATE_ROOT) {

  /** The aggregate at its newest version, or at `at`. A version counts only the aggregate's own events, not those of
    * the base it was duplicated from. Fails when an event cannot be read or handled. */
  def restore(aggregateId: AggregateId, at: Option[Either[AggregateVersion, Instant]] = None): Restored[AGGREGATE_ROOT] = {
    var version = AggregateVersion.ZERO
    var root = initialState()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](aggregateId, at) { (userId, timestamp, event, eventAggregateId, _, noop) =>
      if (!noop) root = eventHandlers(userId, timestamp, root)(event)
      if (eventAggregateId == aggregateId) version = version.increment
    }
    Restored(version, root)
  }

  /** Every event of the aggregate up to its newest version or `at`, its base's included, undone ones too, in order. */
  def events(aggregateId: AggregateId, at: Option[Either[AggregateVersion, Instant]] = None): List[EventInfo[AGGREGATE_ROOT]] = {
    var events = List[EventInfo[AGGREGATE_ROOT]]()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](aggregateId, at) { (userId, timestamp, event, _, eventVersion, _) =>
      events ::= EventInfo(AggregateVersion(eventVersion), event, userId, timestamp)
    }
    events.reverse
  }

  /** The aggregate's events of the given classes, for a history rewrite. */
  def eventsOfClasses(aggregateId: AggregateId, classNames: Set[String]): List[EventWithVersion[AGGREGATE_ROOT]] = {
    var events = List[EventWithVersion[AGGREGATE_ROOT]]()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](aggregateId, None) { (_, _, event, _, eventVersion, _) =>
      if (classNames.contains(event.getClass.getName)) events ::= EventWithVersion(AggregateVersion(eventVersion), event)
    }
    events.reverse
  }
}
