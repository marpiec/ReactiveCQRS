package io.reactivecqrs.core

import io.reactivecqrs.api.Aggregate
import io.reactivecqrs.api.event._
import io.reactivecqrs.api.exception.{AggregateWasAlreadyDeletedException, NoEventsForAggregateException, RepositoryException}
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId}
import io.reactivecqrs.utils.{Success, Failure, Result}

import scala.collection.mutable


class DataStore[AGGREGATE](handlers: Array[EventHandler[AGGREGATE, _ <: Event[AGGREGATE]]]) {

  // fields

  protected val eventHandlers = mutable.HashMap[Class[Event[AGGREGATE]], EventHandler[AGGREGATE, _ <: Event[AGGREGATE]]]()


  // constructor

  handlers.foreach(registerHandler)

  // methods

  private def registerHandler(eventHandler: EventHandler[AGGREGATE, _ <: Event[AGGREGATE]]): Unit = {
    val eventClass = eventHandler.eventClass.asInstanceOf[Class[Event[AGGREGATE]]]
    eventHandlers += eventClass -> eventHandler
  }


  def buildAggregate(aggregateId: AggregateId, events: Stream[EventRow[AGGREGATE]]): Result[Aggregate[AGGREGATE], RepositoryException] = {

    if (events.isEmpty) {
      Failure(NoEventsForAggregateException("No events for aggregate " + aggregateId))
    } else {
      val creationEventRow = events.head

      val creationEventHandler: CreationEventHandler[AGGREGATE, Event[AGGREGATE]] =
        eventHandlers(creationEventRow.event.getClass.asInstanceOf[Class[Event[AGGREGATE]]]).asInstanceOf[CreationEventHandler[AGGREGATE, Event[AGGREGATE]]]

      val initialAggregate = Aggregate(aggregateId, AggregateVersion.INITIAL, Some(creationEventHandler.handle(creationEventRow.event)))
      val finalAggregate = events.tail.foldLeft(initialAggregate)((aggregate, eventRow) => updateAggregateWithEvent(eventRow, aggregate))

      Success(finalAggregate)

    }
  }

  private def updateAggregateWithEvent(eventRow: EventRow[AGGREGATE], aggregate: Aggregate[AGGREGATE]): Aggregate[AGGREGATE] = {

    if (eventRow.version == aggregate.version.version + 1) {
      // Body:
      if (aggregate.aggregateRoot.isDefined) {
        val event = eventRow.event
        if (event == NoopEvent) {
          Aggregate(aggregate.id, aggregate.version.increment, aggregate.aggregateRoot)
        } else {
          val handler: EventHandler[AGGREGATE, _] = eventHandlers(event.getClass.asInstanceOf[Class[Event[AGGREGATE]]])

          handler match {
            case h: ModificationEventHandler[AGGREGATE, Event[AGGREGATE]] => handleModificationEvent(event, aggregate, h)
            case h: DeletionEventHandler[AGGREGATE, Event[AGGREGATE]] => handleDeletionEvent(aggregate)
            case _ => throw new IllegalStateException("No handler registered for event " + event.getClass.getName)
          }

        }
      } else {
        throw new IllegalStateException(AggregateWasAlreadyDeletedException(
          "Unexpected modification of already deleted aggregate").toString)
      }
    } else {
      throw new IllegalStateException(
        "Unexpected version for aggregate when applying eventRow. " + "[aggregateType: TODO - extract" +
          ", aggregateId:" + aggregate.id + ", aggregateVersion:" +
          aggregate.version.version + ", eventType:" + eventRow.event.getClass.getName +
          ", expectedVersion:" + eventRow.version + "]")
    }


  }


  private def handleModificationEvent(event: Event[AGGREGATE],
                                      aggregate: Aggregate[AGGREGATE],
                                      handler: ModificationEventHandler[AGGREGATE, Event[AGGREGATE]]) = {
    Aggregate[AGGREGATE](
      aggregate.id,
      aggregate.version.increment,
      Some(handler.handle(aggregate.aggregateRoot.get, event)))
  }

  private def handleDeletionEvent(aggregate: Aggregate[AGGREGATE]) = {
    Aggregate[AGGREGATE](
      aggregate.id,
      aggregate.version.increment,
      None)
  }
}
