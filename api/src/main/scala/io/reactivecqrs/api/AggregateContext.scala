package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.concurrent.Future
import scala.reflect.{ClassTag, classTag}

case class GetAggregate(id: AggregateId)
case class GetAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class GetEventsForAggregate(id: AggregateId)
case class GetEventsForAggregateForVersion(id: AggregateId, version: AggregateVersion)

case class SimulateEvent[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

case class EventTypeVersion(eventType: String, version: Short)

case class EventVersion[AGGREGATE_ROOT](eventBaseType: String, mapping: List[EventTypeVersion])

abstract class AggregateContext[AGGREGATE_ROOT] {

  protected def EV[EVENT_BASE <: Event[AGGREGATE_ROOT] : ClassTag](versionedType: (Int, Class[_ <: Event[AGGREGATE_ROOT]])*) = {
    EventVersion[AGGREGATE_ROOT](classTag[EVENT_BASE].toString, versionedType.map(vt => EventTypeVersion(vt._2.getTypeName, vt._1.toShort)).toList)
  }

  def initialAggregateRoot: AGGREGATE_ROOT

  type CommandHandler = AGGREGATE_ROOT => PartialFunction[Any, Future[CustomCommandResult[Any]]]

  type EventHandler = (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]

  def eventHandlers: EventHandler

  def commandHandlers: CommandHandler

  val eventsVersions: List[EventVersion[AGGREGATE_ROOT]] = List.empty

}