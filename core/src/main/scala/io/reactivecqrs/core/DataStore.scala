package io.reactivecqrs.core

import io.reactivecqrs.api.Aggregate
import io.reactivecqrs.api.event._
import io.reactivecqrs.api.exception.{AggregateWasAlreadyDeletedException, NoEventsForAggregateException, RepositoryException}
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId}
import io.reactivecqrs.utils.{Success, Failure, Result}

import scala.collection.mutable


class DataStore[AGGREGATE_ROOT](handlers: EventHandler[AGGREGATE_ROOT, _ <: Event[AGGREGATE_ROOT]]*) {

  // fields

  protected val eventHandlers = mutable.HashMap[Class[Event[AGGREGATE_ROOT]], EventHandler[AGGREGATE_ROOT, _ <: Event[AGGREGATE_ROOT]]]()


  // constructor

  handlers.foreach(registerHandler)

  // methods

  private def registerHandler(eventHandler: EventHandler[AGGREGATE_ROOT, _ <: Event[AGGREGATE_ROOT]]): Unit = {
    val eventClass = eventHandler.eventClass.asInstanceOf[Class[Event[AGGREGATE_ROOT]]]
    eventHandlers += eventClass -> eventHandler
  }


  def buildAggregate(aggregateId: AggregateId, events: Stream[EventRow[AGGREGATE_ROOT]]): Result[Aggregate[AGGREGATE_ROOT], RepositoryException] = {

    if (events.isEmpty) {
      Failure(NoEventsForAggregateException("No events for aggregate " + aggregateId))
    } else {
      val creationEventRow = events.head

      val creationEventHandler: CreationEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]] =
        eventHandlers(creationEventRow.event.getClass.asInstanceOf[Class[Event[AGGREGATE_ROOT]]]).asInstanceOf[CreationEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]]

      val initialAggregate = Aggregate(aggregateId, AggregateVersion.INITIAL, Some(creationEventHandler.handle(creationEventRow.event)))
      val finalAggregate = events.tail.foldLeft(initialAggregate)((aggregate, eventRow) => updateAggregateWithEvent(eventRow, aggregate))

      Success(finalAggregate)

    }
  }

  private def updateAggregateWithEvent(eventRow: EventRow[AGGREGATE_ROOT], aggregate: Aggregate[AGGREGATE_ROOT]): Aggregate[AGGREGATE_ROOT] = {

    if (eventRow.version == aggregate.version.version + 1) {
      // Body:
      if (aggregate.aggregateRoot.isDefined) {
        eventRow.event match {
          case event: NoopEvent[AGGREGATE_ROOT] =>
            Aggregate(aggregate.id, aggregate.version.increment, aggregate.aggregateRoot)
          case event: DeleteEvent[AGGREGATE_ROOT] =>
            handleDeletionEvent(aggregate)
          case event =>
            val handler: EventHandler[AGGREGATE_ROOT, _] = eventHandlers(event.getClass.asInstanceOf[Class[Event[AGGREGATE_ROOT]]])
            handler match {
              case h: ModificationEventHandler[_, _] => handleModificationEvent(event, aggregate, h.asInstanceOf[ModificationEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
              case _ => throw new IllegalStateException("No handler registered for event " + event.getClass.getName)
            }
        }
      } else {
        // TODO change into result
        throw new IllegalStateException(AggregateWasAlreadyDeletedException(
          "Unexpected modification of already deleted aggregate").toString)
      }
    } else {
      // TODO change into result
      throw new IllegalStateException(
        "Unexpected version for aggregate when applying eventRow. " + "[aggregateType: TODO - extract" +
          ", aggregateId:" + aggregate.id + ", aggregateVersion:" +
          aggregate.version.version + ", eventType:" + eventRow.event.getClass.getName +
          ", expectedVersion:" + eventRow.version + "]")
    }


  }


  private def handleModificationEvent(event: Event[AGGREGATE_ROOT],
                                      aggregate: Aggregate[AGGREGATE_ROOT],
                                      handler: ModificationEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]) = {
    Aggregate[AGGREGATE_ROOT](
      aggregate.id,
      aggregate.version.increment,
      Some(handler.handle(aggregate.aggregateRoot.get, event)))
  }

  private def handleDeletionEvent(aggregate: Aggregate[AGGREGATE_ROOT]) = {
    Aggregate[AGGREGATE_ROOT](
      aggregate.id,
      aggregate.version.increment,
      None)
  }
}
