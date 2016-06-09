package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateType
import scalikejdbc._

import scala.util.{Failure, Success, Try}

class OptimisticLockingFailed extends Exception

object SubscriptionsState {
  val AGGREGATES = "aggregates"
  val EVENTS = "events"
  val AGGREGATES_WITH_EVENTS = "aggregatesWithEvents"
}

trait SubscriptionsState {
  def lastEventIdForAggregatesSubscription(name: String, aggregateType: AggregateType): Long
  def lastEventIdForEventsSubscription(name: String, aggregateType: AggregateType): Long
  def lastEventIdForAggregatesWithEventsSubscription(name: String, aggregateType: AggregateType): Long

  def newEventIdForAggregatesSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit
  def newEventIdForEventsSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit
  def newEventIdForAggregatesWithEventsSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit
}


class PostgresSubscriptionsState extends SubscriptionsState {

  def initSchema(): Unit = {
    new PostgresSubscriptionsStateSchemaInitializer().initSchema()
  }

  override def lastEventIdForAggregatesSubscription(name: String, aggregateType: AggregateType): Long = {
    eventsCount(name, aggregateType, SubscriptionsState.AGGREGATES)
  }

  override def lastEventIdForEventsSubscription(name: String, aggregateType: AggregateType): Long = {
    eventsCount(name, aggregateType, SubscriptionsState.EVENTS)
  }

  override def lastEventIdForAggregatesWithEventsSubscription(name: String, aggregateType: AggregateType): Long = {
    eventsCount(name, aggregateType, SubscriptionsState.AGGREGATES_WITH_EVENTS)
  }

  private def eventsCount(name: String, aggregateType: AggregateType, subscriptionType: String): Long = {
    DB.localTx { implicit session =>

      val eventIdOption = sql"""SELECT last_event_id FROM subscriptions WHERE name = ? AND aggregate_type = ? AND subscription_type = ?"""
        .bind(name, aggregateType.typeName, subscriptionType).map(rs => rs.long(1)).single().apply()

      eventIdOption match {
        case Some(eventId) => eventId
        case None => addSubscriptionEntry(name, aggregateType, subscriptionType)
      }
    }
  }

  private def addSubscriptionEntry(name: String, aggregateType: AggregateType, subscriptionType: String)(implicit session: DBSession): Long = {
    sql"""INSERT INTO subscriptions (id, name, aggregate_type, subscription_type, last_event_id) VALUES (nextval('subscriptions_seq'), ?, ?, ?, 0)"""
      .bind(name, aggregateType.typeName, subscriptionType).executeUpdate().apply()
    0L
  }

  override def newEventIdForAggregatesSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit = {
    newEventId(name, aggregateType, SubscriptionsState.AGGREGATES, lastEventId, eventId)
  }

  override def newEventIdForEventsSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit = {
    newEventId(name, aggregateType, SubscriptionsState.EVENTS, lastEventId, eventId)
  }

  override def newEventIdForAggregatesWithEventsSubscription(name: String, aggregateType: AggregateType, lastEventId: Long, eventId: Long): Unit = {
    newEventId(name, aggregateType, SubscriptionsState.AGGREGATES_WITH_EVENTS, lastEventId, eventId)
  }

  private def newEventId(name: String, aggregateType: AggregateType, subscriptionType: String, lastEventId: Long, eventId: Long): Try[Unit] = {
    DB.localTx { implicit session =>
      val rowsUpdated = sql"""UPDATE subscriptions SET last_event_id = ? WHERE name = ? AND aggregate_type = ? AND subscription_type = ? AND last_event_id = ?"""
        .bind(eventId, name, aggregateType.typeName, subscriptionType, lastEventId).map(rs => rs.int(1)).single().executeUpdate().apply()
      if(rowsUpdated == 1) {
        Success(())
      } else {
        Failure(new OptimisticLockingFailed)
      }
    }
  }


}
