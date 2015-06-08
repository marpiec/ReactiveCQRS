package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId
import scala.reflect.runtime.universe._

case class AggregateWithType[AGGREGATE_ROOT](aggregateType: String, id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

case class Aggregate[AGGREGATE_ROOT: TypeTag](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])
