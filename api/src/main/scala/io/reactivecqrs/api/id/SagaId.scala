package io.reactivecqrs.api.id

/** The id of one run of a saga, unique within the saga's name; with a step's number it makes the idempotency key of
  * the step's commands (`SagaStep`). */
case class SagaId(asLong: Long)
