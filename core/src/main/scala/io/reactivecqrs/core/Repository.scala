package io.reactivecqrs.core

import io.reactivecqrs.api.{AggregateUpdatedNotification, NewEventForAggregateNotification, Aggregate}
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.{UserId, CommandId, AggregateId}


trait Repository[AGGREGATE] {

  /** Adding events */

  def addFirstEvent(commandId: CommandId, userId: UserId, newAggregateId: AggregateId, event: Event[AGGREGATE])

  def addEvent(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: Int, event: Event[AGGREGATE])

  def addEventListener(eventListener: NewEventForAggregateNotification[AGGREGATE] => Unit): Unit
  
  /** Getting events */


  def getAllEventsForAggregate(aggregateId: AggregateId): Seq[EventRow[AGGREGATE]]

  /**
   * Might be used when requester has cached previous version of aggregate and needs an update only.
   */
  def getEventsForAggregateFromVersion(aggregateId: AggregateId, fromVersion: Int): Seq[EventRow[AGGREGATE]]

  /**
   * Might be used to get old version of aggregate.
   */
  def getEventsForAggregateToVersion(aggregateId: AggregateId, toVersion: Int): Seq[EventRow[AGGREGATE]]

  /**
   * Might be used to get old version of aggregate, when requester has cached previous version of aggregate and needs an update only.
   */
  def getEventsForAggregateFromToVersion(aggregateId: AggregateId, fromVersion: Int, toVersion: Int): Seq[EventRow[AGGREGATE]]

  /** Getting aggregates */
  
  def getAggregate(id: AggregateId): Either[RepositoryException, Aggregate[AGGREGATE]]

  def getAggregates(ids: Seq[AggregateId]): Seq[Either[RepositoryException, Aggregate[AGGREGATE]]]

  def getAggregateForVersion(id: AggregateId, version: Int): Either[RepositoryException, Aggregate[AGGREGATE]]


  def countAllAggregates(): Long

  def findAllAggregateIds(): Seq[AggregateId]

  def addAggregateListener(eventListener: AggregateUpdatedNotification[AGGREGATE] => Unit): Unit

}
