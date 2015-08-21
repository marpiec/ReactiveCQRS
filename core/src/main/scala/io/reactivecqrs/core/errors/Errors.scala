package io.reactivecqrs.core.errors

import io.reactivecqrs.api.AggregateVersion

case class AggregateConcurrentModificationError(expected: AggregateVersion, was: AggregateVersion)
