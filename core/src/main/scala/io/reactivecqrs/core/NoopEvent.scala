package io.reactivecqrs.core

import io.reactivecqrs.api.event.Event

case class NoopEvent[AGGREGATE]()(implicit ev: Manifest[AGGREGATE]) extends Event[AGGREGATE]
