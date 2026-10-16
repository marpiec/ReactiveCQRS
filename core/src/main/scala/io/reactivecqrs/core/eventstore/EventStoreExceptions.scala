package io.reactivecqrs.core.eventstore

/** A stored event with a non-zero `event_type_version`: event versioning was removed in 1.0 (D29), so the class it
  * stands for is unknown. Such rows have to be migrated before the upgrade; `StoredDataCheck` counts them. */
class UnsupportedEventVersionException(val className: String, val typeVersion: Short, val aggregateId: Long, val version: Int)
  extends IllegalStateException("Event " + className + " of aggregate " + aggregateId + " at version " + version +
    " is stored with event_type_version " + typeVersion + ", but event versioning was removed in 1.0 (D29): " +
    "events of a changed shape are stored as a new class. Migrate these rows before the upgrade.")

/** A history rewrite gave an event of another class than the one stored at that version, or a version the aggregate
  * does not have (D17): the rewrite is refused and nothing of the command is stored. */
class RewriteChangesClassException(val aggregateId: io.reactivecqrs.api.id.AggregateId, val version: io.reactivecqrs.api.AggregateVersion, val className: String)
  extends IllegalArgumentException("A history rewrite must keep each event's class: aggregate " + aggregateId.asLong + " has no event of class " +
    className + " at version " + version.asInt)
