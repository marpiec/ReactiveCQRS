package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

class NoEventsForAggregateException(val aggregateId: AggregateId, val aggregateType: AggregateType) extends Exception("No events for aggregate " + aggregateId.asLong)

/** The aggregate's stored events could not be restored; requests fail at once until a later retry succeeds. */
class AggregateRestoreFailedException(val aggregateId: AggregateId, val aggregateType: AggregateType, cause: Throwable)
  extends Exception("Restoring aggregate " + aggregateId.asLong + " of type " + aggregateType.typeName + " failed: " + cause.getMessage, cause)

class AggregateInIncorrectVersionException(val aggregateId: AggregateId,
                                           val aggregateType: AggregateType,
                                           val currentVersion: AggregateVersion,
                                           val requestedVersion: AggregateVersion) extends Exception("Requested version " + requestedVersion.asInt+" but current version is " + currentVersion.asInt)


class EventTooLargeException(val aggregateId: AggregateId,
                             val aggregateTypeName: String,
                             val currentVersion: AggregateVersion,
                             val eventTypeName: String,
                             val eventSize: Int,
                             val maxSize: Int) extends Exception("Event of type "+eventTypeName+" is too large: " + eventSize + " bytes, max size is " + maxSize + " bytes, aggregate " + aggregateId.asLong +"@"+aggregateTypeName+" version "+currentVersion.asInt) {
  override def fillInStackTrace(): Throwable = this
}

class TooManyEventsException(val aggregateId: AggregateId,
                             val aggregateTypeName: String,
                             val maxCount: Int) extends Exception("Too many versions for aggregate " + aggregateId.asLong +"@"+aggregateTypeName+", max version is " + maxCount) {
  override def fillInStackTrace(): Throwable = this
}

/** The aggregate's history before `squashedUpTo` was squashed into one event (D51): it cannot be read at `requested`. */
class AggregateSquashedException(val aggregateId: AggregateId, val squashedUpTo: AggregateVersion, val requested: AggregateVersion)
  extends Exception("The history of aggregate " + aggregateId.asLong + " is squashed up to version " + squashedUpTo.asInt +
    "; version " + requested.asInt + " cannot be read")
