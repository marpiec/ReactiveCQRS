package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

class NoEventsForAggregateException(val aggregateId: AggregateId, val aggregateType: AggregateType) extends Exception("No events for aggregate " + aggregateId.asLong)

class AggregateInIncorrectVersionException(val aggregateId: AggregateId,
                                           val aggregateType: AggregateType,
                                           val currentVersion: AggregateVersion,
                                           val requestedVersion: AggregateVersion) extends Exception("Requested version " + requestedVersion.asInt+" but current version is " + currentVersion.asInt)