package io.reactivecqrs.core

import io.reactivecqrs.api.event.Event

case class NoopEvent[AGGREGATE_ROOT]()(implicit ev: Manifest[AGGREGATE_ROOT]) extends Event[AGGREGATE_ROOT]
