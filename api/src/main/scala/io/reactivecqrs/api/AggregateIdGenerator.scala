package io.reactivecqrs.api

import io.reactivecqrs.api.guid.AggregateId

/**
 * Responsible for generating globally unique identifiers for aggregates.
 */
trait AggregateIdGenerator {
  def nextAggregateId: AggregateId
}
