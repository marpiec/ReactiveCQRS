package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateType
import scalikejdbc._

object SubscriptionsState {
  val AGGREGATES = "aggregates"
  val EVENTS = "events"
  val AGGREGATES_WITH_EVENTS = "aggregatesWithEvents"
}

class PostgresSubscriptionsState {

  def initSchema(): Unit = {
    new PostgresSubscriptionsStateSchemaInitializer().initSchema()
  }

  def eventsCountForAggregatesSubscription(name: String, aggregateType: AggregateType): Long = {
    eventsCount(name, aggregateType, SubscriptionsState.AGGREGATES)
  }

  def eventsCountForEventsSubscription(name: String, aggregateType: AggregateType): Long = {
    eventsCount(name, aggregateType, SubscriptionsState.EVENTS)
  }

  def eventsCountForAggregatesWithEventsSubscription(name: String, aggregateType: AggregateType): Long = {
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

  def addSubscriptionEntry(name: String, aggregateType: AggregateType, subscriptionType: String)(implicit session: DBSession): Long = {
    sql"""INSERT INTO subscriptions (id, name, aggregate_type, subscription_type, last_event_id) VALUES (nextval('subscriptions_seq'), ?, ?, ?, 0)"""
      .bind(name, aggregateType.typeName, subscriptionType).executeUpdate().apply()
    0L
  }
}
