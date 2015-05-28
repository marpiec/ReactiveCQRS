package io.reactivecqrs.actor

import io.reactivecqrs.core.AggregateVersion

case class AggregateConcurrentModificationError(expected: AggregateVersion, was: AggregateVersion)
