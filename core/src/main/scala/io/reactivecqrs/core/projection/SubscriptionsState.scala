package io.reactivecqrs.core.projection

import java.sql.BatchUpdateException
import java.time.Instant

import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.TypesNamesState
import io.reactivecqrs.core.util.AfterCommit
import org.slf4j.Logger
import scalikejdbc._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

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

/** A subscription that gave up on an aggregate (D20a): the first version it could not apply, why, and since when. */
case class SubscriptionFailure(subscriptionType: SubscriptionType, aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion,
                               error: String, since: Instant)

trait SubscriptionsState {

  /** The last version of the aggregate that the subscriber applied in this kind of subscription; zero when none. */
  def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion

  /** Compare-and-set in the caller's transaction: moves the subscription from `lastAggregateVersion` to
    * `aggregateVersion`, or throws `OptimisticLockingFailed` when it is not at `lastAggregateVersion`, so the caller
    * rolls back the listener's work. */
  def newVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId,
                 lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit

  /** Forgets everything about the subscriber: its versions, cached ones included (F6), and its failures. */
  def clearSubscriptionsInfo(subscriberName: String): Unit

  // D20a: a subscriber that gave up on an aggregate records it; it answers deliveries of that aggregate with
  // MessageFailed until the failure is cleared (a retry, or a skip of the failed version), and lists it in its status.
  // A later failure of the same subscription replaces the earlier one.
  def recordFailure(subscriberName: String, failure: SubscriptionFailure): Unit
  def failures(subscriberName: String): Seq[SubscriptionFailure]
  def clearFailure(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): Unit

  def readOnly[A](block: DBSession => A): A
  def localTx[A](block: DBSession => A): A
  def autoCommit[A](block: DBSession => A): A

  def dump(): String
}

class MemorySubscriptionsState extends SubscriptionsState {

  private case class SubscriptionsKey(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)

  private val state = new mutable.HashMap[SubscriptionsKey, AggregateVersion]()

  // Locked: projections in parallel mode call these from their pool threads.
  override def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion = synchronized {
    state.getOrElse(SubscriptionsKey(subscriberName, subscriptionType, aggregateId), AggregateVersion.ZERO)
  }

  // Same contract as PostgresSubscriptionsState.
  override def newVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId,
                          lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = synchronized {
    val key = SubscriptionsKey(subscriberName, subscriptionType, aggregateId)
    val current = state.getOrElse(key, AggregateVersion.ZERO)
    if (current == lastAggregateVersion) {
      state += key -> aggregateVersion
    } else {
      throw new OptimisticLockingFailed(getClass.getSimpleName+": optimistic locking failed for subscriber "+subscriberName+", subscription type "+subscriptionType.id+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was "+current.asInt)
    }
  }

  private val failuresState = new mutable.LinkedHashMap[(String, SubscriptionType, AggregateId), SubscriptionFailure]()

  override def clearSubscriptionsInfo(subscriberName: String): Unit = synchronized {
    state --= state.keys.filter(_.subscriberName == subscriberName)
    failuresState --= failuresState.keys.filter(_._1 == subscriberName)
  }

  override def recordFailure(subscriberName: String, failure: SubscriptionFailure): Unit = synchronized {
    failuresState += (subscriberName, failure.subscriptionType, failure.aggregateId) -> failure
  }

  override def failures(subscriberName: String): Seq[SubscriptionFailure] = synchronized {
    failuresState.collect { case ((subscriber, _, _), failure) if subscriber == subscriberName => failure }.toSeq
  }

  override def clearFailure(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): Unit = synchronized {
    failuresState -= ((subscriberName, subscriptionType, aggregateId))
  }

  override def readOnly[A](block: DBSession => A): A = block(NoSession)
  override def localTx[A](block: DBSession => A): A = block(NoSession)
  override def autoCommit[A](block: DBSession => A): A = block(NoSession)

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

/**
  * @param keepInMemory replay mode (until M4): versions live in memory and `dump()` writes them; otherwise every
  *                     update is written in the listener's transaction and the database is the source of truth
  * @param maxCachedAggregates live mode only: the versions of at most this many aggregates are cached, the least
  *                            recently used are dropped and read from the database again when needed (F29)
  */
class PostgresSubscriptionsState(typesNamesState: TypesNamesState, keepInMemory: Boolean, eventsLogger: Option[Logger] = None,
                                 maxCachedAggregates: Int = 10000) extends SubscriptionsState {

  def initSchema(): PostgresSubscriptionsState = {
    createSubscriptionsTable()
    createSubscriptionsSequence()
    createSubscriberTypeAggregateIdIndex()
    createAggregateIdIndex()
    createFailuresTable()
    this
  }


  private var dumped = Map[AggregateId, mutable.HashMap[String, AggregateVersion]]() //which subscriptions info has already been dumped
  // String is subscriber | subscriptionType. Guarded by the instance monitor; access order, for the LRU bound.
  private val perAggregate = new java.util.LinkedHashMap[AggregateId, mutable.HashMap[String, AggregateVersion]](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[AggregateId, mutable.HashMap[String, AggregateVersion]]): Boolean =
      !keepInMemory && size() > maxCachedAggregates
  }

  private[projection] def cachedAggregates: Int = synchronized(perAggregate.size())

  private def cached(aggregateId: AggregateId): Option[mutable.HashMap[String, AggregateVersion]] = synchronized(Option(perAggregate.get(aggregateId)))

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
       """.execute()

  }

  private def createSubscriptionsSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE IF NOT EXISTS subscriptions_seq""".execute()
  }

  private def createSubscriberTypeAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_sub_type_agg_id_idx ON subscriptions (subscriber_type_id, subscription_type, aggregate_id)""".execute()
  }


  private def createAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE INDEX IF NOT EXISTS subscriptions_agg_id_idx ON subscriptions (aggregate_id)""".execute()
  }

  // D20a. A table of its own rather than columns of `subscriptions`: failures are rare, read whole at a projection's
  // start, and stay apart from the version rows that the cache and dump() manage.
  private def createFailuresTable() = DB.autoCommit { implicit session =>
    sql"""
         CREATE TABLE IF NOT EXISTS subscription_failures (
           subscriber_type_id SMALLINT NOT NULL,
           subscription_type SMALLINT NOT NULL,
           aggregate_id BIGINT NOT NULL,
           aggregate_type_id SMALLINT NOT NULL,
           failed_version INT NOT NULL,
           error TEXT NOT NULL,
           failed_since TIMESTAMP NOT NULL,
           PRIMARY KEY (subscriber_type_id, subscription_type, aggregate_id))
       """.execute()
    sql"""CREATE INDEX IF NOT EXISTS subscription_failures_agg_id_idx ON subscription_failures (aggregate_id)""".execute()
  }

  private val maxErrorLength = 4000

  override def recordFailure(subscriberName: String, failure: SubscriptionFailure): Unit = DB.autoCommit { implicit session =>
    sql"""INSERT INTO subscription_failures (subscriber_type_id, subscription_type, aggregate_id, aggregate_type_id, failed_version, error, failed_since)
         | VALUES (${typesNamesState.typeIdByClassName(subscriberName)}, ${failure.subscriptionType.id}, ${failure.aggregateId.asLong},
         |   ${typesNamesState.typeIdByClassName(failure.aggregateType.typeName)},
         |   ${failure.version.asInt}, ${failure.error.take(maxErrorLength)}, ${java.sql.Timestamp.from(failure.since)})
         | ON CONFLICT (subscriber_type_id, subscription_type, aggregate_id)
         | DO UPDATE SET aggregate_type_id = EXCLUDED.aggregate_type_id, failed_version = EXCLUDED.failed_version, error = EXCLUDED.error, failed_since = EXCLUDED.failed_since""".stripMargin.update()
  }

  override def failures(subscriberName: String): Seq[SubscriptionFailure] = DB.readOnly { implicit session =>
    sql"""SELECT subscription_type, aggregate_type_id, aggregate_id, failed_version, error, failed_since FROM subscription_failures WHERE subscriber_type_id = ${typesNamesState.typeIdByClassName(subscriberName)}"""
      .map(rs => SubscriptionFailure(SubscriptionType(rs.short(1)), AggregateType(typesNamesState.classNameById(rs.short(2))), AggregateId(rs.long(3)),
        AggregateVersion(rs.int(4)), rs.string(5), rs.timestamp(6).toInstant))
      .list()
  }

  override def clearFailure(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): Unit = {
    sql"""DELETE FROM subscription_failures WHERE subscriber_type_id = ${typesNamesState.typeIdByClassName(subscriberName)}
         | AND subscription_type = ${subscriptionType.id} AND aggregate_id = ${aggregateId.asLong}""".stripMargin.update()
  }

  override def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion = {
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
    cached(aggregateId) match {
      case Some(versions) => versions
      case None =>
        // Single-flight per aggregate: only one thread loads from DB for a given aggregate; concurrent
        // callers for the SAME aggregate wait on this stripe and then hit the freshly-installed cache
        // entry below (no duplicate query). Different aggregates use (almost always) different stripes,
        // so they still load in parallel.
        loadLockFor(aggregateId).synchronized {
          cached(aggregateId) match {
            case Some(versions) => versions
            case None =>
              val loaded = lastAggregateVersionFromDB(aggregateId, typesNamesState)
              synchronized {
                Option(perAggregate.get(aggregateId)) match {
                  case Some(existing) => existing
                  case None =>
                    perAggregate.put(aggregateId, loaded)
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

  override def newVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId,
                          lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit = {
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
      // UPDATE re-checks the version, so it is the real arbiter. The in-memory cache advances only once the
      // transaction commits, so a rolled-back write can never leave it ahead of the persisted state (F18).
      synchronized {
        val current = versions.getOrElse(key, AggregateVersion.ZERO)
        if (lastAggregateVersion != current) {
          throw mismatch(current)
        }
      }
      eventsLogger.foreach(_.debug("Updating subscription state for " + aggregateId.asLong+": " + lastAggregateVersion.asInt+" -> "+aggregateVersion.asInt+" for subscriber "+subscriberName))
      try {
        newEventIdInDB(aggregateId, subscriberName, subscriptionType, lastAggregateVersion, aggregateVersion, typesNamesState)
      } catch {
        case e: Exception =>
          // The stored version is not the cached one (another writer, a repair): read it again next time (F77).
          synchronized(perAggregate.remove(aggregateId))
          throw e
      }
      // Into the aggregate's cached versions as they are now: if they were dropped meanwhile, the next read loads the
      // committed version anyway.
      AfterCommit(session) {
        synchronized {
          Option(perAggregate.get(aggregateId)).foreach(_ += key -> aggregateVersion)
        }
      }
    }
  }

  override def readOnly[A](block: DBSession => A): A =
    DB.readOnly { session =>
      block(session)
    }

  // Caches written inside a projection update (subscription versions, document stores) change only after commit.
  override def localTx[A](block: DBSession => A): A =
    AfterCommit.localTx(block)

  override def autoCommit[A](block: DBSession => A): A =
    DB.autoCommit { session =>
      block(session)
    }



  override def clearSubscriptionsInfo(subscriberName: String): Unit = {
    val subscriberTypeId = typesNamesState.typeIdByClassName(subscriberName)
    DB.localTx { implicit session =>
      sql"""DELETE FROM subscriptions WHERE subscriber_type_id = $subscriberTypeId""".update()
      sql"""DELETE FROM subscription_failures WHERE subscriber_type_id = $subscriberTypeId""".update()
    }
    // F6: the cached versions of this subscriber go too, also those dump() still has to write.
    val ofSubscriber = subscriberTypeId + "|"
    synchronized {
      perAggregate.values().asScala.foreach(_.filterInPlace((key, _) => !key.startsWith(ofSubscriber)))
      dumped.values.foreach(_.filterInPlace((key, _) => !key.startsWith(ofSubscriber)))
    }
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
      val ins: Seq[Seq[Any]] = perAggregate.asScala.flatMap(a => batchParams(a._1, a._2, dumpedOnly = false)).toSeq
      val upd: Seq[Seq[Any]] = perAggregate.asScala.flatMap(a => batchParams(a._1, a._2, dumpedOnly = true)).toSeq
      val snapshot: Map[AggregateId, Map[String, AggregateVersion]] =
        perAggregate.asScala.map { case (aggId, versions) => aggId -> versions.toMap }.toMap
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
        Option(perAggregate.get(aggId)).foreach { currentVersions =>
          if (currentVersions.toMap == persistedForAgg) {
            perAggregate.remove(aggId)
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
        }).list()
      mutable.HashMap(m: _*)
    }
  }


  // No monitor here: operates only on the supplied DB session and the self-synchronized typesNamesState,
  // so subscription writes from different projection worker threads no longer serialize on this lock.
  private def newEventIdInDB(aggregateId: AggregateId, subscriberName: String, subscriptionType: SubscriptionType, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion, typesNamesState: TypesNamesState)(implicit session: DBSession): Unit = {
    if(lastAggregateVersion == AggregateVersion.ZERO) {
      INSERT_INTO_SUBSCRIPTIONS.bind(typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, aggregateVersion.asInt).update()
    } else {
      val rowsUpdated = UPDATE_SUBSCRIPTIONS.bind(aggregateVersion.asInt, typesNamesState.typeIdByClassName(subscriberName), subscriptionType.id, aggregateId.asLong, lastAggregateVersion.asInt).update()
      if (rowsUpdated != 1) {

        var subscriptionState = "Aggregate " + aggregateId.asLong+" subscription state: "

        try {
          subscriptionState += AGGREGATE_VERSIONS_QUERY.bind(aggregateId.asLong)
          .map(rs => {
            "version: "+rs.short(1)+", subscriber: "+ rs.short(2)+", subscription: "+ rs.int(3)
          }).list().mkString("; ")
        } catch {
          case e: Exception => subscriptionState += " (error reading current state: "+e.getMessage+")"
        }

        throw new OptimisticLockingFailed(getClass.getSimpleName +": optimistic locking failed for subscriber "+subscriberName+" ("+typesNamesState.typeIdByClassName(subscriberName)+"), subscription type "+subscriptionType.id+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was not updated (rowsUpdated="+rowsUpdated+"). " + subscriptionState)
      }
    }
  }

}
