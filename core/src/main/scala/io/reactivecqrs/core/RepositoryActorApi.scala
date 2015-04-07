package io.reactivecqrs.core

import io.reactivecqrs.api.command.StoreEventsResponse
import io.reactivecqrs.api.exception.{AggregateDoesNotExistException, RepositoryException, CqrsException}
import io.reactivecqrs.api.{AggregateUpdatedNotification, NewEventForAggregateNotification, Aggregate}
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.{AggregateVersion, UserId, CommandId, AggregateId}
import io.reactivecqrs.utils.Result




trait RepositoryActorApi[AGGREGATE] {

  /** Adding events */

  def storeFirstEvent(commandId: CommandId, userId: UserId, newAggregateId: AggregateId, event: Event[AGGREGATE]): StoreEventsResponse

  def storeEvent(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE]): StoreEventsResponse

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
  
  def getAggregate(id: AggregateId): Result[Aggregate[AGGREGATE], AggregateDoesNotExistException]

  def getAggregates(ids: Seq[AggregateId]): Seq[Result[Aggregate[AGGREGATE], AggregateDoesNotExistException]]

  def getAggregateForVersion(id: AggregateId, version: Int): Result[Aggregate[AGGREGATE], RepositoryException]


  def countAllAggregates(): Long

  def findAllAggregateIds(): Seq[AggregateId]

  def addAggregateListener(eventListener: AggregateUpdatedNotification[AGGREGATE] => Unit): Unit

}
