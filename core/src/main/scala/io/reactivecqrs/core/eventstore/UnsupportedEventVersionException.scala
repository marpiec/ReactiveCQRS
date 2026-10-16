package io.reactivecqrs.core.eventstore

/** A stored event with a non-zero `event_type_version`: event versioning was removed in 1.0 (D29), so the class it
  * stands for is unknown. Such rows have to be migrated before the upgrade; `StoredDataCheck` counts them. */
class UnsupportedEventVersionException(val className: String, val typeVersion: Short, val aggregateId: Long, val version: Int)
  extends IllegalStateException("Event " + className + " of aggregate " + aggregateId + " at version " + version +
    " is stored with event_type_version " + typeVersion + ", but event versioning was removed in 1.0 (D29): " +
    "events of a changed shape are stored as a new class. Migrate these rows before the upgrade.")
