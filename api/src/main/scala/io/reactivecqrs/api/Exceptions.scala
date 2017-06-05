package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

class NoEventsForAggregateException(aggregateId: AggregateId) extends Exception("No events for aggregate " + aggregateId.asLong)

class AggregateInIncorrectVersionException(aggregateId: AggregateId, currentVersion: AggregateVersion, requestedVersion: AggregateVersion) extends Exception("Requested version " + requestedVersion.asInt+" but current version is " + currentVersion.asInt)