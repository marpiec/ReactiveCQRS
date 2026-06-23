package io.reactivecqrs.core.projection

import java.sql.BatchUpdateException
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.TypesNamesState
import org.slf4j.Logger
import scalikejdbc._

import scala.util.{Failure, Success, Try}
import scala.collection.mutable

class OptimisticLockingFailed(message: String) extends Exception(message)

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
      Failure(new OptimisticLockingFailed(getClass.getSimpleName+": optimistic locking failed for subscriber "+subscriberName+", subscription type "+subscriptionType.id+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was "+state.get(key).map(_.asInt).getOrElse("-")))
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

class PostgresSubscriptionsState(typesNamesState: TypesNamesState, keepInMemory: Boolean, eventsLogger: Option[Logger] = None) extends SubscriptionsState {

  def initSchema(): PostgresSubscriptionsState = {
    createSubscriptionsTable()
    createSubscriptionsSequence()
    createSubscriberTypeAggregateIdIndex()
    createAggregateIdIndex()
    this
  }


  private var dumped = Map[AggregateId, mutable.HashMap[String, AggregateVersion]]() //which subscriptions info has already been dumped
  private var perAggregate = Map[AggregateId, mutable.HashMap[String, AggregateVersion]]() // String is subscriber | subscriptionType

  // Single-flight load locks: striped so that concurrent cache-miss loads for the SAME aggregate
  // serialize on one monitor (only one DB query is issued), while different aggregates almost always
  // map to different stripes and still load in parallel.
  private val LOAD_STRIPES = 64
  private val loadLocks: Array[AnyRef] = Array.fill(LOAD_STRIPES)(new Object)
  private def loadLockFor(aggregateId: AggregateId): AnyRef =
    loadLocks(((aggregateId.asLong % LOAD_STRIPES) + LOAD_STRIPES).toInt % LOAD_STRIPES)

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
    val key = SubscriptionCacheKey.get(typesNamesState, subscriberName, subscriptionType)
    val versionsForAggregate = getVersionsForAggregate(aggregateId)
    // Read the shared mutable map under the monitor (it may be mutated concurrently by newEventId).
    synchronized {
      versionsForAggregate.getOrElse(key, AggregateVersion.ZERO)
    }
  }


  // Double-checked population guarded by a per-aggregate single-flight stripe: the DB read happens OUTSIDE
  // the instance monitor so cache-miss reads for different aggregates run concurrently (e.g. across
  // projection worker threads) instead of serializing on one lock. The stripe lock coalesces concurrent
  // misses for the SAME aggregate so only one DB query is issued (avoids the cache-stampede that caused
  // repeated identical AGGREGATE_VERSIONS_QUERY calls). Only the cache install is guarded by the instance
  // monitor, and if another thread installed the aggregate first we return that shared instance so all
  // callers mutate the same map.
  private def getVersionsForAggregate(aggregateId: AggregateId): mutable.HashMap[String, AggregateVersion] = {
    synchronized(perAggregate.get(aggregateId)) match {
      case Some(versions) => versions
      case None =>
        // Single-flight per aggregate: only one thread loads from DB for a given aggregate; concurrent
        // callers for the SAME aggregate wait on this stripe and then hit the freshly-installed cache
        // entry below (no duplicate query). Different aggregates use (almost always) different stripes,
        // so they still load in parallel.
        loadLockFor(aggregateId).synchronized {
          synchronized(perAggregate.get(aggregateId)) match {
            case Some(versions) => versions
            case None =>
              val loaded = lastAggregateVersionFromDB(aggregateId, typesNamesState)
              synchronized {
                perAggregate.get(aggregateId) match {
                  case Some(existing) => existing
                  case None =>
                    perAggregate += aggregateId -> loaded
                    if (keepInMemory) {
                      dumped += aggregateId -> mutable.HashMap[String, AggregateVersion](loaded.toList: _*)
                    }
                    loaded
                }
              }
          }
        }
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
    // Fetch (and possibly DB-load) the aggregate's versions outside the monitor; only the compare-and-set
    // on the shared map needs the lock.
    val versions = getVersionsForAggregate(aggregateId)

    def mismatch(current: AggregateVersion): OptimisticLockingFailed =
      new OptimisticLockingFailed(getClass.getSimpleName+": Subscription state update failed last version mismatch for subscriber "+subscriberName+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was "+current.asInt)

    if (keepInMemory) {
      // In-memory is the source of truth (flushed later by dump()): compare-and-advance atomically under the monitor.
      synchronized {
        val current = versions.getOrElse(key, AggregateVersion.ZERO)
        if (lastAggregateVersion == current) {
          versions += key -> aggregateVersion
        } else {
          throw mismatch(current)
        }
      }
    } else {
      // DB is the source of truth: validate against the cache early (cheap reject), then persist. The DB
      // UPDATE re-checks the version, so it is the real arbiter. Advance the in-memory cache only AFTER the
      // write succeeds, so a failed/rolled-back write can never leave the cache ahead of the persisted state.
      synchronized {
        val current = versions.getOrElse(key, AggregateVersion.ZERO)
        if (lastAggregateVersion != current) {
          throw mismatch(current)
        }
      }
      eventsLogger.foreach(_.debug("Updating subscription state for " + aggregateId.asLong+": " + lastAggregateVersion.asInt+" -> "+aggregateVersion.asInt+" for subscriber "+subscriberName))
      newEventIdInDB(aggregateId, subscriberName, subscriptionType, lastAggregateVersion, aggregateVersion, typesNamesState)
      synchronized {
        versions += key -> aggregateVersion
      }
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

  private val INSERT_INTO_SUBSCRIPTIONS = sql"""INSERT INTO subscriptions (id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, ?)"""
  private val UPDATE_SUBSCRIPTIONS = sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""

  // Serializes concurrent dump() calls (so two dumps can't compute and write the same INSERTs). This is a
  // distinct monitor from `this`, so it does NOT block the read / CAS hot path while the DB write runs.
  private val dumpLock = new AnyRef

  override def dump(): String = dumpLock.synchronized {
    val start = System.nanoTime() / 1_000_000

    // Phase 1 (locked): compute the batch params and snapshot the exact versions we are about to persist,
    // from a consistent view of the in-memory state. We do NOT clear the cache here, so concurrent reads /
    // CAS updates keep hitting the live maps (no stale DB reload window) while the DB write runs unlocked.
    val (toInsert, toUpdate, persistedVersions) = synchronized {
      val ins: Seq[Seq[Any]] = perAggregate.flatMap(a => batchParams(a._1, a._2, dumpedOnly = false)).toSeq
      val upd: Seq[Seq[Any]] = perAggregate.flatMap(a => batchParams(a._1, a._2, dumpedOnly = true)).toSeq
      val snapshot: Map[AggregateId, Map[String, AggregateVersion]] =
        perAggregate.map { case (aggId, versions) => aggId -> versions.toMap }
      (ins, upd, snapshot)
    }

    // Phase 2 (unlocked): the actual DB round-trips. The instance monitor is free during this, so the
    // subscription read/CAS hot path does not stall on the dump.
    DB.autoCommit { implicit session =>
      try {
        INSERT_INTO_SUBSCRIPTIONS
          .batch(toInsert: _*).apply()
        UPDATE_SUBSCRIPTIONS
          .batch(toUpdate: _*).apply()
      } catch {
        case e: BatchUpdateException => throw e.getNextException
      }
    }

    // Phase 3 (locked): now that the snapshot is durably persisted, evict the entries that were untouched
    // during the write (bounds memory; a later cache-miss reload then sees the committed state). Entries
    // that were mutated mid-write are kept in the cache, with `dumped` advanced to the persisted versions
    // so the next dump writes only the delta.
    synchronized {
      persistedVersions.foreach { case (aggId, persistedForAgg) =>
        perAggregate.get(aggId).foreach { currentVersions =>
          if (currentVersions.toMap == persistedForAgg) {
            perAggregate -= aggId
            dumped -= aggId
          } else {
            val alreadyDumped = dumped.getOrElse(aggId, new mutable.HashMap[String, AggregateVersion])
            persistedForAgg.foreach { case (k, v) => alreadyDumped += k -> v }
            dumped += aggId -> alreadyDumped
          }
        }
      }
    }

    "Subscriptions dump: inserted = "+toInsert.size+", updated = "+toUpdate.size+" in "+(System.nanoTime() / 1_000_000 - start)+" ms"
  }

  private def batchParams(aggregateId: AggregateId, aggregateVersions: mutable.HashMap[String, AggregateVersion], dumpedOnly: Boolean): Seq[Seq[Any]] = {
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


  private val AGGREGATE_VERSIONS_QUERY = sql"""SELECT aggregate_version, subscriber_type_id, subscription_type FROM subscriptions WHERE aggregate_id = ?"""

  // No monitor here: this only issues a read-only query and builds a fresh local map (no shared state),
  // so concurrent callers (different aggregates) can read in parallel. Caller installs the result under lock.
  private def lastAggregateVersionFromDB(aggregateId: AggregateId, typesNamesState: TypesNamesState): mutable.HashMap[String, AggregateVersion] = {
    DB.readOnly { implicit session =>
      val m = AGGREGATE_VERSIONS_QUERY
        .bind(aggregateId.asLong)
        .map(rs => {
          val cacheKey = SubscriptionCacheKey.getById(rs.short(2), rs.short(3))
          val version = AggregateVersion(rs.int(1))
          cacheKey -> version
        }).list().apply()
      mutable.HashMap(m: _*)
    }
  }


  // No monitor here: operates only on the supplied DB session and the self-synchronized typesNamesState,
  // so subscription writes from different projection worker threads no longer serialize on this lock.
  def newEventIdInDB(aggregateId: AggregateId, subscriberName: String, subscriptionType: SubscriptionType, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion, typesNamesState: TypesNamesState)(implicit session: DBSession): Unit = {
    if(lastAggregateVersion == AggregateVersion.ZERO) {
      INSERT_INTO_SUBSCRIPTIONS.bind(typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, aggregateVersion.asInt).update().apply()
    } else {
      val rowsUpdated = UPDATE_SUBSCRIPTIONS.bind(aggregateVersion.asInt, typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, lastAggregateVersion.asInt).update().apply()
      if (rowsUpdated != 1) {

        var subscriptionState = "Aggregate " + aggregateId.asLong+" subscription state: "

        try {
          subscriptionState += AGGREGATE_VERSIONS_QUERY.bind(aggregateId.asLong)
          .map(rs => {
            "version: "+rs.short(1)+", subscriber: "+ rs.short(2)+", subscription: "+ rs.int(3)
          }).list().apply().mkString("; ")
        } catch {
          case e: Exception => subscriptionState += " (error reading current state: "+e.getMessage+")"
        }

        throw new OptimisticLockingFailed(getClass.getSimpleName +": optimistic locking failed for subscriber "+subscriberName+" ("+typesNamesState.typeIdByClassName(subscriberName)+"), subscription type "+subscriptionType.id+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was not updated (rowsUpdated="+rowsUpdated+"). " + subscriptionState)
      }
    }
  }

}
