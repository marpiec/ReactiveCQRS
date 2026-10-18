package io.reactivecqrs.core.projection

import java.time.Instant

import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.TypesNamesState
import io.reactivecqrs.core.util.{AfterCommit, SchemaLock}
import org.slf4j.Logger
import scalikejdbc._

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import io.reactivecqrs.core.util.SqlLogging.withoutValues
import io.reactivecqrs.core.util.Errors

/** A subscription was not at the version a write expected: another writer moved it (`SubscriptionsState.newVersion`). */
class OptimisticLockingFailed(message: String) extends Exception(message)

/** A kind of subscription; equal by its id, so one read from another node is the same kind. */
class SubscriptionType(val id: Short) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case that: SubscriptionType => that.id == id
    case _ => false
  }
  override def hashCode(): Int = id.toInt
  override def toString: String = "SubscriptionType(" + id + ")"
}

object SubscriptionType {
  /** Aggregate listeners: the aggregate's state. */
  val AGGREGATES = new SubscriptionType(1)
  /** Event listeners: the events. */
  val EVENTS = new SubscriptionType(2)
  /** Listeners of aggregates with events: both. */
  val AGGREGATES_WITH_EVENTS = new SubscriptionType(3)

  /** The kind stored as `id`. */
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

/** Where subscribers keep, per kind of subscription and aggregate, the last version they applied (`subscriptions`), and
  * the aggregates they gave up on (`subscription_failures`): `PostgresSubscriptionsState`, or `MemorySubscriptionsState`
  * for tests. */
trait SubscriptionsState {

  /** The last version of the aggregate that the subscriber applied in this kind of subscription; zero when none. */
  def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion

  /** `lastVersion` of many aggregates at once, in one read of the store for those not cached (D61). `cache`: the
    * aggregates read are cached, as `lastVersion` caches them (live deliveries); otherwise the cache stays as it is (a
    * builder's batches, which would push out what live deliveries need). */
  def lastVersions(subscriberName: String, subscriptionType: SubscriptionType, aggregateIds: Seq[AggregateId], cache: Boolean = false): Map[AggregateId, AggregateVersion]

  /** Compare-and-set in the caller's transaction: moves the subscription from `lastAggregateVersion` to
    * `aggregateVersion`, or throws `OptimisticLockingFailed` when it is not at `lastAggregateVersion`, so the caller
    * rolls back the listener's work. One transaction may move a subscription several times (D61: a batch of many
    * aggregates in commit order); each move starts where the one before it in the transaction ended. */
  def newVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId,
                 lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion)(implicit session: DBSession): Unit

  /** Forgets everything about the subscriber: its versions, cached ones included (F6), and its failures. */
  def clearSubscriptionsInfo(subscriberName: String): Unit

  /** Whether the subscriber applied any version of any aggregate: a projection 0.12 built has (D81). */
  def hasSubscriptions(subscriberName: String): Boolean

  /** In the caller's transaction, forgets the subscriber's versions of these aggregates, of every kind, and their
    * failures: a targeted rebuild delivers them anew (D63). */
  def resetVersions(subscriberName: String, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Unit

  // D20a: a subscriber that gave up on an aggregate records it; it answers deliveries of that aggregate with
  // MessageFailed until the failure is cleared (a retry, or a skip of the failed version), and lists it in its status.
  // A later failure of the same subscription replaces the earlier one.

  /** Records that the subscriber gave up on an aggregate. */
  def recordFailure(subscriberName: String, failure: SubscriptionFailure): Unit
  /** The subscriber's failures. */
  def failures(subscriberName: String): Seq[SubscriptionFailure]
  /** In the caller's transaction, clears the subscriber's failure on an aggregate (retried, or its version skipped). */
  def clearFailure(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)(implicit session: DBSession): Unit

  /** Runs `block` in a read-only session of the state's store. */
  def readOnly[A](block: DBSession => A): A
  /** Runs `block` in a transaction of the state's store. */
  def localTx[A](block: DBSession => A): A
  /** Runs `block` in an auto-commit session of the state's store. */
  def autoCommit[A](block: DBSession => A): A
}

/** `SubscriptionsState` in memory, for tests: no transactions. */
class MemorySubscriptionsState extends SubscriptionsState {

  private case class SubscriptionsKey(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId)

  private val state = new mutable.HashMap[SubscriptionsKey, AggregateVersion]()

  // Locked: projections in parallel mode call these from their pool threads.
  override def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion = synchronized {
    state.getOrElse(SubscriptionsKey(subscriberName, subscriptionType, aggregateId), AggregateVersion.ZERO)
  }

  override def lastVersions(subscriberName: String, subscriptionType: SubscriptionType, aggregateIds: Seq[AggregateId], cache: Boolean): Map[AggregateId, AggregateVersion] =
    aggregateIds.map(id => id -> lastVersion(subscriberName, subscriptionType, id)).toMap

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

  override def hasSubscriptions(subscriberName: String): Boolean = synchronized(state.keys.exists(_.subscriberName == subscriberName))

  override def resetVersions(subscriberName: String, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Unit = synchronized {
    val reset = aggregateIds.toSet
    state --= state.keys.filter(key => key.subscriberName == subscriberName && reset.contains(key.aggregateId))
    failuresState --= failuresState.keys.filter { case (subscriber, _, id) => subscriber == subscriberName && reset.contains(id) }
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

}


private[projection] object SubscriptionCacheKey {
  def get(typesNamesState: TypesNamesState, subscriberName: String, subscriptionType: SubscriptionType): String = {
    s"${typesNamesState.typeIdByClassName(subscriberName)}|${subscriptionType.id}"
  }
  def getById(subscriberNameId: Short, subscriptionTypeId: Short): String = {
    s"$subscriberNameId|$subscriptionTypeId"
  }
}

/**
  * Every update is written in the listener's transaction; the database is the source of truth (F3: the in-memory mode
  * of the 0.12 replayer is gone, D61).
  * @param maxCachedAggregates the versions of at most this many aggregates are cached, the least recently used are
  *                            dropped and read from the database again when needed (F29)
  */
class PostgresSubscriptionsState(typesNamesState: TypesNamesState, eventsLogger: Option[Logger] = None,
                                 maxCachedAggregates: Int = 10000) extends SubscriptionsState {

  /** Creates the tables and indexes it needs, when missing; returns this state. */
  def initSchema(): PostgresSubscriptionsState = SchemaLock {
    createSubscriptionsTable()
    createSubscriptionsSequence()
    createSubscriberTypeAggregateIdIndex()
    createAggregateIdIndex()
    createFailuresTable()
    this
  }


  // String is subscriber | subscriptionType. Guarded by the instance monitor; access order, for the LRU bound.
  private val perAggregate = new java.util.LinkedHashMap[AggregateId, mutable.HashMap[String, AggregateVersion]](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[AggregateId, mutable.HashMap[String, AggregateVersion]]): Boolean =
      size() > maxCachedAggregates
  }

  private[projection] def cachedAggregates: Int = synchronized(perAggregate.size())

  // F115: aggregates whose versions are being read to fill the cache (how many reads), and those of them that a commit
  // changed meanwhile, whose reads may have missed it and are not cached. Guarded by the instance monitor.
  private val loading = mutable.HashMap[AggregateId, Int]()
  private val changedWhileLoading = mutable.HashSet[AggregateId]()

  private def startLoading(ids: Iterable[AggregateId]): Unit = synchronized(ids.foreach(id => loading(id) = loading.getOrElse(id, 0) + 1))

  // Under the monitor: ends one read of the aggregate; true when nothing changed it while it was read.
  private def loadedUnchanged(id: AggregateId): Boolean = {
    val unchanged = !changedWhileLoading.contains(id)
    loading.get(id) match {
      case Some(reads) if reads > 1 => loading(id) = reads - 1
      case _ =>
        loading -= id
        changedWhileLoading -= id
    }
    unchanged
  }

  // Under the monitor, with every change of the cached versions after a commit.
  private def changed(id: AggregateId): Unit = if (loading.contains(id)) changedWhileLoading += id

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
  // start, and stay apart from the version rows that the cache manages.
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

  // The cache holds every subscriber's versions of an aggregate, so filling it reads every subscriber's rows of the
  // aggregates missing; without filling, only this subscriber's are read.
  override def lastVersions(subscriberName: String, subscriptionType: SubscriptionType, aggregateIds: Seq[AggregateId], cache: Boolean): Map[AggregateId, AggregateVersion] = {
    val key = SubscriptionCacheKey.get(typesNamesState, subscriberName, subscriptionType)
    val (cached, missing) = synchronized {
      aggregateIds.distinct.partitionMap(id => Option(perAggregate.get(id)).map(versions => id -> versions.getOrElse(key, AggregateVersion.ZERO)).toLeft(id))
    }
    val stored = if (missing.isEmpty) {
      Map.empty[AggregateId, AggregateVersion]
    } else if (cache) {
      startLoading(missing)
      val loaded = try storedVersions(missing).groupBy(_._1) catch {
        case e: Throwable =>
          synchronized(missing.foreach(loadedUnchanged))
          throw e
      }
      // As `getVersionsForAggregate`: an aggregate cached meanwhile keeps the entry it has, and one a commit changed while
      // it was read is not cached (F115).
      synchronized {
        missing.foreach(id => if (loadedUnchanged(id) && !perAggregate.containsKey(id)) {
          perAggregate.put(id, mutable.HashMap(loaded.getOrElse(id, Nil).map { case (_, k, version) => k -> version }: _*))
        })
      }
      loaded.flatMap { case (id, rows) => rows.collectFirst { case (_, `key`, version) => id -> version } }
    } else DB.readOnly { implicit session =>
      sql"""SELECT aggregate_id, aggregate_version FROM subscriptions
           | WHERE subscriber_type_id = ${typesNamesState.typeIdByClassName(subscriberName)} AND subscription_type = ${subscriptionType.id}
           |   AND aggregate_id IN (${missing.map(_.asLong)})""".stripMargin.map(rs => AggregateId(rs.long(1)) -> AggregateVersion(rs.int(2))).list().toMap
    }
    cached.toMap ++ missing.map(id => id -> stored.getOrElse(id, AggregateVersion.ZERO))
  }


  /** Every subscriber's versions of these aggregates, as (aggregate, cache key, version). */
  protected def storedVersions(aggregateIds: Seq[AggregateId]): List[(AggregateId, String, AggregateVersion)] = DB.readOnly { implicit session =>
    sql"""SELECT aggregate_id, aggregate_version, subscriber_type_id, subscription_type FROM subscriptions WHERE aggregate_id IN (${aggregateIds.map(_.asLong)})"""
      .map(rs => (AggregateId(rs.long(1)), SubscriptionCacheKey.getById(rs.short(3), rs.short(4)), AggregateVersion(rs.int(2)))).list()
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
              startLoading(Seq(aggregateId))
              val loaded = try lastAggregateVersionFromDB(aggregateId, typesNamesState) catch {
                case e: Throwable =>
                  synchronized(loadedUnchanged(aggregateId))
                  throw e
              }
              synchronized {
                val unchanged = loadedUnchanged(aggregateId)
                Option(perAggregate.get(aggregateId)) match {
                  case Some(existing) => existing
                  case None =>
                    if (unchanged) perAggregate.put(aggregateId, loaded) // otherwise read again next time (F115)
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

    def mismatch(current: AggregateVersion): OptimisticLockingFailed =
      new OptimisticLockingFailed(getClass.getSimpleName+": Subscription state update failed last version mismatch for subscriber "+subscriberName+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was "+current.asInt)

    // The database is the source of truth: validate against the cache early (cheap reject), then persist. The UPDATE
    // re-checks the version, so it is the real arbiter; an aggregate that is not cached is not loaded for the early
    // check, so a batch of many aggregates costs no read per aggregate (D61). The cache advances only once the
    // transaction commits, so a rolled-back write can never leave it ahead of the stored state (F18); until then a later
    // move of the same subscription in the transaction starts from this one's version.
    val moved = AfterCommit.local(session, this)(mutable.HashMap[(AggregateId, String), AggregateVersion]())
    synchronized {
      moved.flatMap(_.get((aggregateId, key)))
        .orElse(Option(perAggregate.get(aggregateId)).map(_.getOrElse(key, AggregateVersion.ZERO)))
        .filter(_ != lastAggregateVersion)
        .foreach(current => throw mismatch(current))
    }
    eventsLogger.filter(_.isDebugEnabled).foreach(_.debug("Updating subscription state for " + aggregateId.asLong+": " + lastAggregateVersion.asInt+" -> "+aggregateVersion.asInt+" for subscriber "+subscriberName))
    try {
      newEventIdInDB(aggregateId, subscriberName, subscriptionType, lastAggregateVersion, aggregateVersion, typesNamesState)
    } catch {
      case e: Exception =>
        // The stored version is not the cached one (another writer, a repair): read it again next time (F77).
        synchronized {
          perAggregate.remove(aggregateId)
          changed(aggregateId)
        }
        throw e
    }
    moved.foreach(_ += (aggregateId, key) -> aggregateVersion)
    // Into the aggregate's cached versions as they are now: if they were dropped meanwhile, the next read loads the
    // committed version anyway.
    AfterCommit(session) {
      synchronized {
        Option(perAggregate.get(aggregateId)).foreach(_ += key -> aggregateVersion)
        changed(aggregateId)
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
    // F6: the cached versions of this subscriber go too.
    val ofSubscriber = s"$subscriberTypeId|"
    synchronized {
      perAggregate.values().asScala.foreach(_.filterInPlace((key, _) => !key.startsWith(ofSubscriber)))
      changedWhileLoading ++= loading.keys
    }
  }

  override def hasSubscriptions(subscriberName: String): Boolean = DB.readOnly { implicit session =>
    sql"""SELECT EXISTS (SELECT 1 FROM subscriptions WHERE subscriber_type_id = ${typesNamesState.typeIdByClassName(subscriberName)})"""
      .map(_.boolean(1)).single().getOrElse(false)
  }

  // The cache keeps every subscriber's versions of an aggregate: the aggregates are read again after the commit.
  override def resetVersions(subscriberName: String, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Unit = if (aggregateIds.nonEmpty) {
    val subscriber = typesNamesState.typeIdByClassName(subscriberName)
    aggregateIds.grouped(10000).foreach { ids =>
      val longs = ids.map(_.asLong)
      sql"""DELETE FROM subscriptions WHERE subscriber_type_id = $subscriber AND aggregate_id IN ($longs)""".update()
      sql"""DELETE FROM subscription_failures WHERE subscriber_type_id = $subscriber AND aggregate_id IN ($longs)""".update()
    }
    AfterCommit(session)(synchronized(aggregateIds.foreach { id =>
      perAggregate.remove(id)
      changed(id)
    }))
  }

  private val INSERT_INTO_SUBSCRIPTIONS = sql"""INSERT INTO subscriptions (id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version) VALUES (nextval('subscriptions_seq'), ?, ?, ?, ?)"""
  private val UPDATE_SUBSCRIPTIONS = sql"""UPDATE subscriptions SET aggregate_version = ? WHERE subscriber_type_id = ? AND subscription_type = ? AND aggregate_id = ? AND aggregate_version = ?"""


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
            "version: "+rs.int(1)+", subscriber: "+ rs.short(2)+", subscription: "+ rs.short(3)
          }).list().mkString("; ")
        } catch {
          case e: Exception => subscriptionState += " (error reading current state: " + Errors.describe(e) + ")"
        }

        throw new OptimisticLockingFailed(getClass.getSimpleName +": optimistic locking failed for subscriber "+subscriberName+" ("+typesNamesState.typeIdByClassName(subscriberName)+"), subscription type "+subscriptionType.id+" and aggregate "+aggregateId.asLong+" expected version "+lastAggregateVersion.asInt+" but was not updated (rowsUpdated="+rowsUpdated+"). " + subscriptionState)
      }
    }
  }

}
