package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import scalikejdbc._

import scala.util.{Failure, Success, Try}
import scala.collection.mutable

class OptimisticLockingFailed extends Exception

object SubscriptionsState {
  val AGGREGATES = "aggregates"
  val EVENTS = "events"
  val AGGREGATES_WITH_EVENTS = "aggregatesWithEvents"
}

trait SubscriptionsState {
  def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion
  def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion
  def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion

  def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit
  def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit
  def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit

  def localTx(block: DBSession => Unit): Unit
}

class MemorySubscriptionsState extends SubscriptionsState {

  private case class SubscriptionsKey(subscriberName: String, subscriptionType: String, aggregateId: AggregateId)

  val subscriptions = new mutable.HashMap[SubscriptionsKey, AggregateVersion]()

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionsState.AGGREGATES)

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionsState.EVENTS)

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionsState.AGGREGATES_WITH_EVENTS)

  override def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionsState.AGGREGATES, lastAggregateVersion, aggregateVersion)

  override def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionsState.EVENTS, lastAggregateVersion, aggregateVersion)

  override def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionsState.AGGREGATES_WITH_EVENTS, lastAggregateVersion, aggregateVersion)

  override def localTx(block: DBSession => Unit): Unit = block(NoSession)

  private def eventsCount(subscriberName: String, aggregateId: AggregateId, subscriptionType: String): AggregateVersion = {
    val key = SubscriptionsKey(subscriberName, subscriptionType, aggregateId)
    subscriptions.getOrElse(key, {
       subscriptions += key -> AggregateVersion.ZERO
      AggregateVersion.ZERO
     })
  }

  private def newEventId(subscriberName: String, aggregateId: AggregateId, subscriptionType: String, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = {
    val key = SubscriptionsKey(subscriberName, subscriptionType, aggregateId)
    if (subscriptions.get(key).contains(lastAggregateVersion)) {
      subscriptions += key -> aggregateVersion
      Success(())
    } else {
      Failure(new OptimisticLockingFailed)
    }
  }

}


class PostgresSubscriptionsState extends SubscriptionsState {

  def initSchema(): Unit = {
    new PostgresSubscriptionsStateSchemaInitializer().initSchema()
  }

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionsState.AGGREGATES, aggregateId: AggregateId)
  }

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionsState.EVENTS, aggregateId: AggregateId)
  }

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionsState.AGGREGATES_WITH_EVENTS, aggregateId: AggregateId)
  }

  private def lastAggregateVersion(subscriberName: String, subscriptionType: String, aggregateId: AggregateId): AggregateVersion = {
    DB.localTx { implicit session =>

      val eventIdOption = sql"""SELECT aggregate_version FROM subscriptions WHERE subscriber_name = ? AND subscription_type = ? AND aggregate_id = ?"""
        .bind(subscriberName, subscriptionType, aggregateId.asLong).map(rs => AggregateVersion(rs.int(1))).single().apply()

      eventIdOption match {
        case Some(eventId) => eventId
        case None => addSubscriptionEntry(subscriberName, subscriptionType, aggregateId)
      }
    }
  }

  private def addSubscriptionEntry(subscriberName: String, subscriptionType: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    sql"""INSERT INTO subscriptions (id, subscriber_name, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, 0)"""
      .bind(subscriberName, subscriptionType, aggregateId.asLong).executeUpdate().apply()
    AggregateVersion.ZERO
  }

  override def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit = {
    newEventId(subscriberName, SubscriptionsState.AGGREGATES, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  override def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit = {
    newEventId(subscriberName, SubscriptionsState.EVENTS, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  override def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Unit = {
    newEventId(subscriberName, SubscriptionsState.AGGREGATES_WITH_EVENTS, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  private def newEventId(subscriberName: String, subscriptionType: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = {
    DB.localTx { implicit session =>
      val rowsUpdated = sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_name = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""
        .bind(aggregateVersion.asInt, subscriberName, subscriptionType, aggregateId.asLong, lastAggregateVersion.asInt).map(rs => rs.int(1)).single().executeUpdate().apply()
      if (rowsUpdated == 1) {
        Success(())
      } else {
        Failure(new OptimisticLockingFailed) // TODO handle this
      }
    }
  }

  override def localTx(block: DBSession => Unit): Unit =
    DB.localTx { session =>
      block(session)
    }

}
