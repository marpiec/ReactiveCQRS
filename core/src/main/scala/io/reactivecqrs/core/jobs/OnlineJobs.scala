package io.reactivecqrs.core.jobs

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.Executor

import io.reactivecqrs.api.{AggregateType, Event}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.cluster.{Members, Singletons}
import io.reactivecqrs.core.documentstore.PostgresDocumentStore
import io.reactivecqrs.core.eventstore._
import io.reactivecqrs.core.metrics.ReactiveMetrics
import io.reactivecqrs.core.util.{BlockingDispatcher, Errors, MyActorLogging}
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props}
import scalikejdbc._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
  * CR-3, D91: the online jobs of an upgrade (chapter 18, step 7), run in the background inside the application, after it
  * started, in this order: the indexes (`buildIndexesConcurrently` and, unless off, the document tables' space indexes),
  * the event keys (`backfillEventKeys`, once no member of the cluster runs an older app-version, D65, D67), and the
  * organizations of old aggregates (`backfillSpaces` with the host's `spaceOf`). Each is paced as when called by hand
  * and idempotent; their state is saved in `online_jobs`, so a runner started again skips what is done and continues
  * the event keys from the last batch saved. One runs at a time: a cluster singleton, which also holds a PostgreSQL
  * advisory lock while it works. Its long statements run on a connection of their own whose network timeout is
  * `longStatementTimeout`, not the pool's `socketTimeout`. With `BuildHold.untilStoreReady` the full builds wait for the
  * first two jobs; a failed job keeps them waiting and says why, until it is run again (`RetryOnlineJobs`) or an operator
  * releases the hold (`ReleaseBuildHold`). Off unless the host starts it (`OnlineJobs.start`).
  */
object OnlineJobs {

  /** The jobs, in the order they run. */
  val Indexes = "indexes"
  val EventKeys = "event-keys"
  val Spaces = "spaces"
  val Names: Seq[String] = Seq(Indexes, EventKeys, Spaces)

  /** Asks the runner for its `OnlineJobsStatus`. */
  case object GetOnlineJobsStatus
  /** Each job's status, in order; whether an operator released the build hold; what the store lacks for full builds, as
    * last read (None before the first read). */
  case class OnlineJobsStatus(jobs: Seq[OnlineJobStatus], buildHoldReleased: Boolean, readiness: Option[StoreReadiness])
  /** One job: `state` is `Waiting`, `Running`, `Done` or `Failed`; `detail` says what it waits for, or why it failed;
    * `counts`: the indexes job's `documentTables`, the event keys' `written`, `missing`, `position` and `eventsLeft` (ids
    * after the position), the spaces' `filled` and `left`; `orphans` (F342), from the event keys. */
  case class OnlineJobStatus(name: String, state: String, detail: Option[String], counts: Map[String, Long], orphans: OrphanEvents,
                             started: Option[Instant], finished: Option[Instant])

  /** Runs the failed jobs again; answered with the status. */
  case object RetryOnlineJobs
  /** The operator's override: full builds held by `BuildHold.untilStoreReady` start, ready or not (on every node, at its
    * next check). Answered `BuildHoldReleased`. */
  case object ReleaseBuildHold
  /** The answer to `ReleaseBuildHold`. */
  case object BuildHoldReleased

  /** Starts the runner, a singleton in a cluster (call it on every node); returns this node's proxy to it. */
  def start(system: ActorSystem, settings: OnlineJobsSettings, name: String = "reactivecqrs-online-jobs"): ActorRef =
    Singletons.start(system, name, Props(new OnlineJobsRunner(settings)).withDispatcher(BlockingDispatcher.id))

  // The advisory lock that one runner holds while it works: "RCQRJOBS".
  private[jobs] val LockKey = 0x524351524A4F4253L
}

/**
  * @param spaceOf the organization of an aggregate whose first event does not know it (`backfillSpaces`, chapter 16)
  * @param documentSpaceIndexes also index the document tables of the schema by organization (`PostgresDocumentStore.buildSpaceIndexesConcurrently`)
  * @param batchSize, pause the event keys' batches and the pause between them (`backfillEventKeys`)
  * @param spaceBatchSize the space backfill's batches
  * @param longStatementTimeout the network timeout of the runner's connection for the index builds, which may run long
  * @param checkEvery how often it looks again while it waits: for older nodes to leave, or for another runner's lock
  * @param orphansListed how many orphan aggregates the status lists (F342)
  */
case class OnlineJobsSettings(eventStore: PostgresEventStoreState,
                              spaceOf: (AggregateType, AggregateId, Event[_]) => Option[SpaceId] = (_, _, _) => None,
                              documentSpaceIndexes: Boolean = true,
                              batchSize: Int = 10000, pause: FiniteDuration = 100.millis, spaceBatchSize: Int = 1000,
                              longStatementTimeout: FiniteDuration = 6.hours, checkEvery: FiniteDuration = 10.seconds, orphansListed: Int = 100)

/** CR-3: runs the online jobs; see `OnlineJobs`. The jobs run on a thread of the blocking dispatcher, which tells this
  * actor how they go. */
class OnlineJobsRunner(settings: OnlineJobsSettings) extends Actor with MyActorLogging {
  import OnlineJobs._

  private val store = settings.eventStore
  private val metrics = ReactiveMetrics(context.system).metrics
  private val members = Members(context.system)
  private val me = self

  private var statuses: Map[String, OnlineJobStatus] = Names.map(n => n -> OnlineJobStatus(n, "Waiting", None, Map.empty, OrphanEvents.none, None, None)).toMap
  private var released = false
  private var readiness: Option[StoreReadiness] = None
  private var running = false
  @volatile private var stopping = false

  private case object Run
  private case class Report(status: OnlineJobStatus)
  private case class Ended(again: Boolean)
  private case class Read(released: Boolean, readiness: StoreReadiness)

  override def preStart(): Unit = self ! Run

  override def postStop(): Unit = stopping = true

  override def receive: Receive = {
    case Run => if (!running) {
      running = true
      Future(work())(BlockingDispatcher(context.system)).onComplete(ended => me ! Ended(ended.getOrElse(true)))(context.dispatcher)
    }
    case Report(status) => reported(status)
    case Read(holdReleased, ready) =>
      released = holdReleased
      readiness = Some(ready)
    case Ended(again) =>
      running = false
      if (again && !stopping) context.system.scheduler.scheduleOnce(settings.checkEvery, self, Run)(context.dispatcher)
    case GetOnlineJobsStatus => sender() ! status
    case RetryOnlineJobs =>
      Names.flatMap(store.onlineJob).filter(_.state == "Failed").foreach(row => store.saveOnlineJob(row.copy(state = "Waiting", error = None)))
      statuses = statuses.map { case (n, s) => n -> (if (s.state == "Failed") s.copy(state = "Waiting", detail = None) else s) }
      self ! Run
      sender() ! status
    case ReleaseBuildHold =>
      store.releaseBuildHold()
      released = true
      log.warning("The hold of full builds is released by an operator (CR-3); held builds start at their next check")
      sender() ! BuildHoldReleased
  }

  private def status: OnlineJobsStatus = OnlineJobsStatus(Names.map(statuses), released, readiness)

  private def reported(status: OnlineJobStatus): Unit = {
    val before = statuses(status.name)
    statuses += status.name -> status
    if (before.state != status.state || before.detail != status.detail) status.state match {
      case "Running" => log.info("Online job " + status.name + " runs" + status.detail.fold("")(" (" + _ + ")"))
      case "Done" => log.info("Online job " + status.name + " is done: " + counted(status))
      case "Failed" => log.error("Online job " + status.name + " failed: " + status.detail.getOrElse("") + "; it runs again on RetryOnlineJobs or at the next start")
      case _ => log.info("Online job " + status.name + " waits: " + status.detail.getOrElse(""))
    }
    metrics.gauge("reactivecqrs.online.job", Map("job" -> status.name), status.state match {
      case "Waiting" => 0L
      case "Running" => 1L
      case "Done" => 2L
      case _ => 3L
    })
    status.counts.get(progressCount(status.name)).foreach(metrics.gauge("reactivecqrs.online.job.progress", Map("job" -> status.name), _))
  }

  private def counted(status: OnlineJobStatus): String =
    status.counts.toSeq.sorted.map { case (k, v) => k + " " + v }.mkString(", ") + (if (status.orphans.events > 0) ", orphan events " + status.orphans.events else "")

  private def progressCount(job: String): String = job match {
    case Indexes => "documentTables"
    case EventKeys => "written"
    case _ => "filled"
  }

  // ************** On the jobs' thread: nothing here touches the actor's state; it reports with messages.

  private def report(status: OnlineJobStatus): Unit = me ! Report(status)

  private def readStore(): Unit = Try(me ! Read(store.buildHoldReleased(), store.buildReadiness()))

  // Whether to look again later: another runner held the lock, a job waits for older nodes, or it was stopped.
  private def work(): Boolean = {
    readStore()
    val connection = ConnectionPool.borrow()
    try {
      val locked = DB(connection).autoClose(false).autoCommit(implicit session => sql"SELECT pg_try_advisory_lock(${OnlineJobs.LockKey})".map(_.boolean(1)).single().get)
      if (!locked) {
        Names.foreach(n => if (stateOf(n) != "Done") report(waiting(n, "another runner holds the jobs' lock")))
        true
      } else try {
        val timeout = connection.getNetworkTimeout
        connection.setNetworkTimeout(DirectExecutor, settings.longStatementTimeout.toMillis.toInt)
        try runJobs(connection) finally connection.setNetworkTimeout(DirectExecutor, timeout)
      } finally {
        Try(DB(connection).autoClose(false).autoCommit(implicit session => sql"SELECT pg_advisory_unlock(${OnlineJobs.LockKey})".map(_.boolean(1)).single()))
        readStore()
      }
    } finally connection.close()
  }

  private def stateOf(name: String): String = store.onlineJob(name).map(_.state).getOrElse("Waiting")

  private def waiting(name: String, why: String): OnlineJobStatus = OnlineJobStatus(name, "Waiting", Some(why), Map.empty, OrphanEvents.none, None, None)

  // Each job in turn; one that fails does not stop the others. True when one waits for older nodes, to look again.
  private def runJobs(connection: Connection): Boolean = Names.foldLeft(false) { (again, name) =>
    if (stopping) true
    else {
      val row = store.onlineJob(name).getOrElse(OnlineJobRow(name, "Waiting", 0L, None, None, None, None))
      if (row.state == "Done") {
        report(statusOf(row))
        again
      } else if (row.state == "Failed") {
        report(statusOf(row))
        again
      } else if (name == EventKeys && members.olderVersionsRun) {
        report(waiting(name, "a member of the cluster runs an older app-version (D67): its events would have no keys"))
        true
      } else {
        val started = row.started.orElse(Some(Instant.now))
        val runningRow = row.copy(state = "Running", started = started, error = None)
        store.saveOnlineJob(runningRow)
        report(statusOf(runningRow))
        val outcome = Try(name match {
          case Indexes => indexes(connection, runningRow)
          case EventKeys => eventKeys(runningRow)
          case _ => spaces(runningRow)
        })
        outcome.fold(e => {
          val failed = runningRow.copy(state = "Failed", error = Some(Errors.describe(e)), finished = Some(Instant.now))
          Try(store.saveOnlineJob(failed))
          report(statusOf(failed))
        }, row => if (row.state != "Running") { // a stopped walk stays Running, to resume
          store.saveOnlineJob(row)
          report(statusOf(row))
        })
        again || outcome.toOption.exists(_.state == "Running")
      }
    }
  }

  private def indexes(connection: Connection, row: OnlineJobRow): OnlineJobRow = {
    val tables = DB(connection).autoClose(false).autoCommit { implicit session =>
      store.buildIndexesIn(session)
      if (settings.documentSpaceIndexes) PostgresDocumentStore.buildSpaceIndexesIn(session) else Nil
    }
    row.copy(state = "Done", result = Some(store.jsons.serialize(JobCounts(Map("documentTables" -> tables.size.toLong)), classOf[JobCounts].getName)),
      finished = Some(Instant.now))
  }

  private def eventKeys(row: OnlineJobRow): OnlineJobRow = {
    val before = row.result.flatMap(json => Try(store.jsons.deserialize[EventKeysBackfill](json, classOf[EventKeysBackfill].getName)).toOption)
      .getOrElse(EventKeysBackfill(0L, 0L))
    val lastId = DB.readOnly(implicit session => sql"SELECT coalesce(max(id), 0) FROM events".map(_.long(1)).single().getOrElse(0L))
    var last = row
    val result = store.backfillEventKeysFrom(EventKeysProgress(row.position, before.written, before.missing, before.orphans), settings.batchSize,
      settings.pause, settings.orphansListed) { progress =>
      last = row.copy(position = progress.after, result = Some(json(EventKeysBackfill(progress.written, progress.missing, progress.orphans))))
      store.saveOnlineJob(last)
      report(statusOf(last, Map("eventsLeft" -> math.max(lastId - progress.after, 0L))))
      !stopping
    }
    if (stopping) last
    else {
      val done = last.copy(position = math.max(last.position, lastId), result = Some(json(result)), finished = Some(Instant.now))
      if (result.missing == 0) done.copy(state = "Done")
      else done.copy(state = "Failed", error = Some(s"${result.missing} events have no key although their aggregate has its row: a node of an " +
        "older version wrote meanwhile; run the job again"))
    }
  }

  private def json(backfill: EventKeysBackfill): String = store.jsons.serialize(backfill, classOf[EventKeysBackfill].getName)

  private def spaces(row: OnlineJobRow): OnlineJobRow = {
    val result = store.backfillSpaces(settings.spaceOf, settings.spaceBatchSize)
    row.copy(state = "Done", result = Some(store.jsons.serialize(JobCounts(Map("filled" -> result.filled.toLong, "left" -> result.left.size.toLong)),
      classOf[JobCounts].getName)), error = if (result.left.isEmpty) None else Some("aggregates left without a space (the first 100): " +
      result.left.take(100).map(_.asLong).mkString(", ")), finished = Some(Instant.now))
  }

  private def statusOf(row: OnlineJobRow, extra: Map[String, Long] = Map.empty): OnlineJobStatus = {
    val (counts, orphans) = row.name match {
      case EventKeys => row.result.flatMap(json => Try(store.jsons.deserialize[EventKeysBackfill](json, classOf[EventKeysBackfill].getName)).toOption)
        .fold((Map.empty[String, Long], OrphanEvents.none))(b => (Map("written" -> b.written, "missing" -> b.missing, "position" -> row.position),
          b.orphans.copy(listed = b.orphans.listed.sortBy(_.aggregateId.asLong).take(settings.orphansListed))))
      case _ => (row.result.flatMap(json => Try(store.jsons.deserialize[JobCounts](json, classOf[JobCounts].getName)).toOption).fold(Map.empty[String, Long])(_.counts),
        OrphanEvents.none)
    }
    OnlineJobStatus(row.name, row.state, row.error, counts ++ extra, orphans, row.started, row.finished)
  }

  private object DirectExecutor extends Executor {
    override def execute(command: Runnable): Unit = command.run()
  }
}

/** CR-3: a job's counts, as saved. */
private[jobs] case class JobCounts(counts: Map[String, Long])
