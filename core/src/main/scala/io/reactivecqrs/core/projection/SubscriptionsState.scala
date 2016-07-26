package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.TypesNamesState
import org.postgresql.util.PSQLException
import scalikejdbc._

import scala.util.{Failure, Success, Try}
import scala.collection.mutable

class OptimisticLockingFailed extends Exception

case class SubscriptionType(id: Short)
object SubscriptionType {
  val AGGREGATES = SubscriptionType(1)
  val EVENTS = SubscriptionType(2)
  val AGGREGATES_WITH_EVENTS = SubscriptionType(3)
}

trait SubscriptionsState {
  def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion
  def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion
  def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion

  def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit
  def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit
  def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit

  def localTx[A](block: DBSession => A): A
}

class MemorySubscriptionsState extends SubscriptionsState {

  private case class SubscriptionsKey(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)

  private val state = new mutable.HashMap[SubscriptionsKey, AggregateVersion]()

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.AGGREGATES)

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.EVENTS)

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.AGGREGATES_WITH_EVENTS)

  override def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.AGGREGATES, lastAggregateVersion, aggregateVersion)

  override def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.EVENTS, lastAggregateVersion, aggregateVersion)

  override def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.AGGREGATES_WITH_EVENTS, lastAggregateVersion, aggregateVersion)

  override def localTx[A](block: DBSession => A): A = block(NoSession)

  private def eventsCount(subscriberName: String, aggregateId: AggregateId, subscriptionType: SubscriptionType): AggregateVersion = {
    val key = SubscriptionsKey(subscriberName, subscriptionType, aggregateId)
    state.getOrElse(key, {
       state += key -> AggregateVersion.ZERO
      AggregateVersion.ZERO
     })
  }

  private def newEventId(subscriberName: String, aggregateId: AggregateId, subscriptionType: SubscriptionType, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = {
    val key = SubscriptionsKey(subscriberName, subscriptionType, aggregateId)
    if (state.get(key).contains(lastAggregateVersion)) {
      state += key -> aggregateVersion
      Success(())
    } else {
      Failure(new OptimisticLockingFailed)
    }
  }

}


class PostgresSubscriptionsState(typesNamesState: TypesNamesState) extends SubscriptionsState {

  def initSchema(): PostgresSubscriptionsState = {
    createSubscriptionsTable()
    try {
      createSubscriptionsSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    try {
      createSubscriberTypeAggregateIdIndex()
    } catch {
      case e: PSQLException => () //ignore until CREATE UNIQUE INDEX IF NOT EXISTS is available in PostgreSQL
    }
    this
  }

  private def createSubscriptionsTable() = DB.autoCommit { implicit session =>
    sql"""
         CREATE TABLE IF NOT EXISTS subscriptions (
           id BIGINT NOT NULL PRIMARY KEY,
           subscriber_type_id SMALLINT NOT NULL,
           subscription_type SMALLINT NOT NULL,
           aggregate_id BIGINT NOT NULL,
           aggregate_version INT NOT NULL)
       """.execute().apply()

  }

  private def createSubscriptionsSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE subscriptions_seq""".execute().apply()
  }

  private def createSubscriberTypeAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX subscriptions_sub_type_agg_id_idx ON subscriptions (subscriber_type_id, subscription_type, aggregate_id)""".execute().apply()
  }

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.AGGREGATES, aggregateId: AggregateId)
  }

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.EVENTS, aggregateId: AggregateId)
  }

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.AGGREGATES_WITH_EVENTS, aggregateId: AggregateId)
  }

  private def lastAggregateVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    val versionOption = sql"""SELECT aggregate_version FROM subscriptions WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ?"""
      .bind(typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong).map(rs => AggregateVersion(rs.int(1))).single().apply()

    versionOption match {
      case Some(version) => version
      case None => addSubscriptionEntry(subscriberName, subscriptionType, aggregateId)
    }
  }

  private def addSubscriptionEntry(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    sql"""INSERT INTO subscriptions (id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, 0)"""
      .bind(typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong).executeUpdate().apply()
    AggregateVersion.ZERO
  }

  override def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = {
    newEventId(subscriberName, SubscriptionType.AGGREGATES, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  override def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = {
    newEventId(subscriberName, SubscriptionType.EVENTS, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  override def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = {
    newEventId(subscriberName, SubscriptionType.AGGREGATES_WITH_EVENTS, aggregateId, lastAggregateVersion, aggregateVersion)
  }

  private def newEventId(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Try[Unit] = {
    val rowsUpdated = sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""
      .bind(aggregateVersion.asInt, typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, lastAggregateVersion.asInt).map(rs => rs.int(1)).single().executeUpdate().apply()
    if (rowsUpdated == 1) {
      Success(())
    } else {
      Failure(new OptimisticLockingFailed) // TODO handle this
    }
  }

  override def localTx[A](block: DBSession => A): A =
    DB.localTx { session =>
      block(session)
    }

}
