package io.reactivecqrs.core.projection

import java.sql.BatchUpdateException
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

import scala.util.{Failure, Success, Try}
import scala.collection.mutable

class OptimisticLockingFailed extends Exception

class SubscriptionType(val id: Short)

object SubscriptionType {
  val AGGREGATES = new SubscriptionType(1)
  val EVENTS = new SubscriptionType(2)
  val AGGREGATES_WITH_EVENTS = new SubscriptionType(3)

  def apply(id: Short): SubscriptionType = {
    if(id == 1) {
      AGGREGATES
    } else if(id == 2) {
      EVENTS
    } else if(id == 3) {
      AGGREGATES_WITH_EVENTS
    } else {
      throw new IllegalArgumentException("Incorrect subscription type ["+id+"]")
    }
  }
}

trait SubscriptionsState {


  def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion
  def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion
  def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion

  def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit
  def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit
  def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit

  def clearSubscriptionsInfo(subscriberName: String): Unit

  def readOnly[A](block: DBSession => A): A
  def localTx[A](block: DBSession => A): A
  def autoCommit[A](block: DBSession => A): A

  def dump(): String
}

class MemorySubscriptionsState extends SubscriptionsState {

  private case class SubscriptionsKey(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)

  private val state = new mutable.HashMap[SubscriptionsKey, AggregateVersion]()

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.AGGREGATES)

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.EVENTS)

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion =
    eventsCount(subscriberName, aggregateId, SubscriptionType.AGGREGATES_WITH_EVENTS)

  override def newVersionForAggregatesSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.AGGREGATES, lastAggregateVersion, aggregateVersion)

  override def newVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.EVENTS, lastAggregateVersion, aggregateVersion)

  override def newVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit =
    newEventId(subscriberName, aggregateId, SubscriptionType.AGGREGATES_WITH_EVENTS, lastAggregateVersion, aggregateVersion)

  override def clearSubscriptionsInfo(subscriberName: String): Unit = {
    state --= state.keys.filter(_.subscriberName != subscriberName)
  }

  override def readOnly[A](block: DBSession => A): A = block(NoSession)
  override def localTx[A](block: DBSession => A): A = block(NoSession)
  override def autoCommit[A](block: DBSession => A): A = block(NoSession)

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


  override def dump(): String = ""

}


object SubscriptionCacheKey {
  def get(typesNamesState: TypesNamesState, subscriberName: String, subscriptionType: SubscriptionType): String = {
    typesNamesState.typeIdByClassName(subscriberName)+"|"+subscriptionType.id
  }
  def getById(subscriberNameId: Short, subscriptionTypeId: Short): String = {
    subscriberNameId+"|"+subscriptionTypeId
  }
}

class PostgresSubscriptionsState(typesNamesState: TypesNamesState, keepInMemory: Boolean) extends SubscriptionsState {

  def initSchema(): PostgresSubscriptionsState = {
    createSubscriptionsTable()
    createSubscriptionsSequence()
    createSubscriberTypeAggregateIdIndex()
    createAggregateIdIndex()
    this
  }


  private var dumped = Map[AggregateId, mutable.HashMap[String, AggregateVersion]]() //which subscriptions info has already been dumped
  private var perAggregate = Map[AggregateId, mutable.HashMap[String, AggregateVersion]]() // String is subscriber | subscriptionType

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
    sql"""CREATE SEQUENCE IF NOT EXISTS subscriptions_seq""".execute().apply()
  }

  private def createSubscriberTypeAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_sub_type_agg_id_idx ON subscriptions (subscriber_type_id, subscription_type, aggregate_id)""".execute().apply()
  }


  private def createAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE INDEX IF NOT EXISTS subscriptions_agg_id_idx ON subscriptions (aggregate_id)""".execute().apply()
  }

  override def lastVersionForAggregateSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.AGGREGATES, aggregateId: AggregateId)
  }

  override def lastVersionForEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.EVENTS, aggregateId: AggregateId)
  }

  override def lastVersionForAggregatesWithEventsSubscription(subscriberName: String, aggregateId: AggregateId): AggregateVersion = {
    lastAggregateVersion(subscriberName, SubscriptionType.AGGREGATES_WITH_EVENTS, aggregateId: AggregateId)
  }

  private def lastAggregateVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion = {
    val versionsForAggregate = synchronized {
      getVersionsForAggregate(aggregateId)
    }
    versionsForAggregate.getOrElse(SubscriptionCacheKey.get(typesNamesState, subscriberName, subscriptionType), AggregateVersion.ZERO)
  }


  private def getVersionsForAggregate(aggregateId: AggregateId) = {
    perAggregate.get(aggregateId) match {
      case Some(versions) => versions
      case None =>
        val versions = lastAggregateVersionFromDB(aggregateId, typesNamesState)
        perAggregate += aggregateId -> versions
        if(keepInMemory) {
          dumped += aggregateId -> mutable.HashMap[String, AggregateVersion](versions.toList: _*)
        }
        versions
    }
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

  private def newEventId(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = {
    val key = SubscriptionCacheKey.get(typesNamesState, subscriberName, subscriptionType)
    var saveInDB = false
    synchronized {
      val versions = getVersionsForAggregate(aggregateId)
      if(lastAggregateVersion == versions.getOrElse(key, AggregateVersion.ZERO)) {
        saveInDB = true
        versions += key -> aggregateVersion
      }
      versions
    }
    if(saveInDB) {
      if(!keepInMemory) {
        newEventIdInDB(aggregateId, subscriberName, subscriptionType, lastAggregateVersion, aggregateVersion, typesNamesState)
      }
    } else {
      throw new OptimisticLockingFailed // TODO handle this
    }
  }

  override def readOnly[A](block: DBSession => A): A =
    DB.readOnly { session =>
      block(session)
    }

  override def localTx[A](block: DBSession => A): A =
    DB.localTx { session =>
      block(session)
    }

  override def autoCommit[A](block: DBSession => A): A =
    DB.autoCommit { session =>
      block(session)
    }



  override def clearSubscriptionsInfo(subscriberName: String): Unit = DB.autoCommit { implicit session =>
    sql"""DELETE FROM subscriptions WHERE subscriber_type_id = ?"""
      .bind(typesNamesState.typeIdByClassName(subscriberName)).update().apply()
  }

  override def dump(): String = DB.autoCommit { implicit session =>
    try {
      synchronized {
        val start = System.currentTimeMillis()
        val toInsert: Seq[Seq[Any]] = perAggregate.flatMap(a => batchParams(a._1, a._2, typesNamesState, dumpedOnly = false)).toSeq
        val toUpdate: Seq[Seq[Any]] = perAggregate.flatMap(a => batchParams(a._1, a._2, typesNamesState, dumpedOnly = true)).toSeq
        sql"""INSERT INTO subscriptions (id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, ?)"""
          .batch(toInsert: _*).apply()
        sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""
          .batch(toUpdate: _*).apply()
        perAggregate = Map.empty
        dumped = Map.empty
        "Subscriptions dump: inserted = "+toInsert.size+", updated = "+toUpdate.size+" in "+(System.currentTimeMillis() - start)+" ms"
      }
    } catch {
      case e: BatchUpdateException => throw e.getNextException
    }
  }

  def batchParams(aggregateId: AggregateId, aggregateVersions: mutable.HashMap[String, AggregateVersion], typesNamesState: TypesNamesState, dumpedOnly: Boolean)(implicit session: DBSession): Seq[Seq[Any]] = {
    val dumpedInfo = dumped.getOrElse(aggregateId, new mutable.HashMap[String, AggregateVersion])
    aggregateVersions.filter(entry => {
      if(dumpedOnly) {
        dumpedInfo.get(entry._1) match {
          case Some(version) => version < entry._2
          case None => false
        }
      } else {
        !dumpedInfo.contains(entry._1)
      }
    }).map {
      case (key, value) =>
        val splitted = key.split("\\|")
        val subscriberNameId = splitted(0).toShort
        val subscriptionTypeId = splitted(1).toShort
        if(dumpedOnly) {
          Seq(value.asInt, subscriberNameId, subscriptionTypeId, aggregateId.asLong, dumpedInfo(key).asInt)
        } else {
          Seq(subscriberNameId, subscriptionTypeId, aggregateId.asLong, value.asInt)
        }
    } toSeq
  }


  private val aggregateVersionsQuery = sql"""SELECT aggregate_version, subscriber_type_id, subscription_type FROM subscriptions WHERE aggregate_id = ?"""

  private def lastAggregateVersionFromDB(aggregateId: AggregateId, typesNamesState: TypesNamesState): mutable.HashMap[String, AggregateVersion] = synchronized {
    DB.readOnly { implicit session =>
      val m = aggregateVersionsQuery
        .bind(aggregateId.asLong)
        .map(rs => {
          val cacheKey = SubscriptionCacheKey.getById(rs.short(2), rs.short(3))
          val version = AggregateVersion(rs.int(1))
          cacheKey -> version
        }).list().apply()
      mutable.HashMap(m: _*)
    }
  }


  private val insertQuery = sql"""INSERT INTO subscriptions (id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, ?)"""
  private val updateQuery = sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""

  def newEventIdInDB(aggregateId: AggregateId, subscriberName: String, subscriptionType: SubscriptionType, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion, typesNamesState: TypesNamesState)(implicit session: DBSession): Unit = synchronized {
    if(lastAggregateVersion == AggregateVersion.ZERO) {
      insertQuery.bind(typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, aggregateVersion.asInt).update().apply()
    } else {
      val rowsUpdated = updateQuery.bind(aggregateVersion.asInt, typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, lastAggregateVersion.asInt).update().apply()
      if (rowsUpdated != 1) {
        throw new OptimisticLockingFailed // TODO handle this
      }
    }
  }

}
