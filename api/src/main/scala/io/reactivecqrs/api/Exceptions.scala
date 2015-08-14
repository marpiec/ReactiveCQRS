package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

class NoEventsForAggregateException(aggregateId: AggregateId) extends Exception("No events for aggregate " + aggregateId.asLong)