package io.reactivecqrs.core

import io.reactivecqrs.api.AggregateVersion

case class AggregateConcurrentModificationError(expected: AggregateVersion, was: AggregateVersion)
