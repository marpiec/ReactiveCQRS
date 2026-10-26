package io.reactivecqrs.core.jobs

import java.sql.Connection
import java.time.Instant
import java.util.concurrent.Executor

import io.reactivecqrs.api.{AggregateType, Event}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.cluster.Singletons
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
  * started, in this order: the indexes (`buildIndexesConcurrently` and the document tables' space indexes), and the
  * organizations of old aggregates (`backfillSpaces` with the host's `spaceOf`). Each is paced as when called by hand and
  * idempotent; their state is saved in `online_jobs`, so a runner started again skips what is done. A failed job runs
  * again at the next start, or on `RetryOnlineJobs`. One runner works at a
  * time: it is a cluster singleton, and it also holds a PostgreSQL advisory lock while it works, as two installations'
  * runners, or a runner and a job started by hand, must not build the same index at once (F333). Its index builds run
  * on a connection of their own whose network timeout is `longStatementTimeout`, not the pool's `socketTimeout`. With
  * `BuildHold.untilStoreReady` the full builds wait for the indexes' job; a failed job keeps them waiting and says why.
  * Off unless the host starts it (`OnlineJobs.start`).
  */
object OnlineJobs {

  /** The jobs, in the order they run. */
  val Indexes = "indexes"
  val Spaces = "spaces"
  val Names: Seq[String] = Seq(Indexes, Spaces)

  /** Asks the runner for its `OnlineJobsStatus`. */
  case object GetOnlineJobsStatus
  /** Each job's status, in order; whether an operator released the build hold (`releaseBuildHold`); what the store lacks
    * for full builds (None when it could not be read). */
  case class OnlineJobsStatus(jobs: Seq[OnlineJobStatus], buildHoldReleased: Boolean, readiness: Option[StoreReadiness])
  /** One job: its state; `detail`: what it waits for, why it failed, or what it left (the spaces job's aggregates left
    * without one); `counts`: the indexes job's `documentTables`, the spaces' `filled` and `left`. */
  case class OnlineJobStatus(name: String, state: OnlineJobState, detail: Option[String], counts: Map[String, Long],
                             started: Option[Instant], finished: Option[Instant])

  /** Runs the failed jobs again; answered with the status. */
  case object RetryOnlineJobs

  /** Starts the runner, a singleton in a cluster (call it on every node); returns this node's proxy to it. */
  def start(system: ActorSystem, settings: OnlineJobsSettings, name: String = "reactivecqrs-online-jobs"): ActorRef =
    Singletons.start(system, name, Props(new OnlineJobsRunner(settings)).withDispatcher(BlockingDispatcher.id))

  private[jobs] val log = org.slf4j.LoggerFactory.getLogger(classOf[OnlineJobsRunner]) // for the jobs' thread

  // The advisory lock that one runner holds while it works: "RCQRJOBS".
  private[jobs] val LockKey = 0x524351524A4F4253L
}

/** The state of an online job; `code` is its gauge's value. */
sealed abstract class OnlineJobState(val code: Long)
object OnlineJobState {
  case object Waiting extends OnlineJobState(0)
  case object Running extends OnlineJobState(1)
  case object Done extends OnlineJobState(2)
  case object Failed extends OnlineJobState(3)
  private[jobs] def of(name: String): OnlineJobState = Seq(Waiting, Running, Done, Failed).find(_.toString == name).getOrElse(Waiting)
}

/**
  * @param spaceOf the organization of an aggregate whose first event does not know it (`backfillSpaces`, chapter 16)
  * @param longStatementTimeout the network timeout of the runner's own connection, on which the index builds run long
  * @param checkEvery how often it looks again while another runner holds the jobs' lock
  */
case class OnlineJobsSettings(eventStore: PostgresEventStoreState,
                              spaceOf: (AggregateType, AggregateId, Event[_]) => Option[SpaceId] = (_, _, _) => None,
                              longStatementTimeout: FiniteDuration = 6.hours, checkEvery: FiniteDuration = 10.seconds)

/** CR-3: runs the online jobs; see `OnlineJobs`. The jobs run on a thread of the blocking dispatcher, which tells this
  * actor how they go. */
class OnlineJobsRunner(settings: OnlineJobsSettings) extends Actor with MyActorLogging {
  import OnlineJobs._
  import OnlineJobState._

  private val store = settings.eventStore
  private val metrics = ReactiveMetrics(context.system).metrics
  private val me = self

  private var statuses: Map[String, OnlineJobStatus] = Names.map(n => n -> OnlineJobStatus(n, Waiting, None, Map.empty, None, None)).toMap
  private var running = false
  private var retryFailed = true // the first run after a start runs failed jobs again
  @volatile private var stopping = false

  private case object Run
  private case class Report(status: OnlineJobStatus)
  private case class Ended(again: Boolean)

  override def preStart(): Unit = self ! Run

  override def postStop(): Unit = stopping = true

  override def receive: Receive = {
    case Run => if (!running) {
      running = true
      val failedToo = retryFailed
      retryFailed = false
      Future(work(failedToo))(BlockingDispatcher(context.system)).onComplete(ended => me ! Ended(ended.getOrElse(true)))(context.dispatcher)
    }
    case Report(status) => reported(status)
    case Ended(again) =>
      running = false
      if (retryFailed) self ! Run // asked for while it ran
      else if (again && !stopping) context.system.scheduler.scheduleOnce(settings.checkEvery, self, Run)(context.dispatcher)
    case GetOnlineJobsStatus => sender() ! status
    case RetryOnlineJobs =>
      retryFailed = true
      self ! Run
      sender() ! status
  }

  // The hold's state is read when asked (this actor runs on the blocking dispatcher).
  private def status: OnlineJobsStatus =
    OnlineJobsStatus(Names.map(statuses), Try(store.buildHoldReleased()).getOrElse(false), Try(store.buildReadiness()).toOption)

  private def reported(status: OnlineJobStatus): Unit = {
    val before = statuses(status.name)
    statuses += status.name -> status
    if (before.state != status.state || before.detail != status.detail) status.state match {
      case Running => log.info("Online job " + status.name + " runs")
      case Done => log.info("Online job " + status.name + " is done: " + counted(status) + status.detail.fold("")("; " + _))
      case Failed => log.error("Online job " + status.name + " failed: " + status.detail.getOrElse("") + "; it runs again on RetryOnlineJobs or at the next start")
      case Waiting => log.info("Online job " + status.name + " waits: " + status.detail.getOrElse(""))
    }
    metrics.gauge("reactivecqrs.online.job", Map("job" -> status.name), status.state.code)
    status.counts.get(if (status.name == Indexes) "documentTables" else "filled").foreach(metrics.gauge("reactivecqrs.online.job.progress", Map("job" -> status.name), _))
  }

  private def counted(status: OnlineJobStatus): String = status.counts.toSeq.sorted.map { case (k, v) => k + " " + v }.mkString(", ")

  // ************** On the jobs' thread: nothing here touches the actor's state; it reports with messages.

  private def report(status: OnlineJobStatus): Unit = me ! Report(status)

  // Whether to look again later: another runner held the lock, or it was stopped.
  private def work(failedToo: Boolean): Boolean = {
    val connection = ConnectionPool.borrow()
    try {
      val locked = DB(connection).autoClose(false).autoCommit(implicit session => sql"SELECT pg_try_advisory_lock(${OnlineJobs.LockKey})".map(_.boolean(1)).single().get)
      if (!locked) {
        Names.foreach(n => if (!store.onlineJob(n).exists(_.state == Done.toString)) report(waiting(n, "another runner holds the jobs' lock")))
        true
      } else try {
        val timeout = connection.getNetworkTimeout
        connection.setNetworkTimeout(DirectExecutor, settings.longStatementTimeout.toMillis.toInt)
        try runJobs(connection, failedToo) finally connection.setNetworkTimeout(DirectExecutor, timeout)
      } finally {
        // A session lock: one this connection keeps (the unlock failed) is the pool's until the connection is closed;
        // meanwhile only a runner that borrows it again gets it.
        Try(DB(connection).autoClose(false).autoCommit(implicit session => sql"SELECT pg_advisory_unlock(${OnlineJobs.LockKey})".map(_.boolean(1)).single()))
          .failed.foreach(e => OnlineJobs.log.warn("The online jobs' lock could not be released: " + Errors.describe(e)))
      }
    } finally connection.close()
  }

  private def waiting(name: String, why: String): OnlineJobStatus = OnlineJobStatus(name, Waiting, Some(why), Map.empty, None, None)

  // Each job in turn; one that fails does not stop the others. True when it was stopped, to look again.
  private def runJobs(connection: Connection, failedToo: Boolean): Boolean = Names.foldLeft(false) { (again, name) =>
    val row = store.onlineJob(name).getOrElse(OnlineJobRow(name, Waiting.toString, 0L, None, None, None, None))
    val state = OnlineJobState.of(row.state)
    if (stopping) true
    else if (state == Done || state == Failed && !failedToo) {
      report(statusOf(row))
      again
    } else {
      val fresh = if (state == Failed) OnlineJobRow(name, Waiting.toString, 0L, None, None, None, None) else row
      val runningRow = fresh.copy(state = Running.toString, started = fresh.started.orElse(Some(Instant.now)), error = None)
      store.saveOnlineJob(runningRow)
      report(statusOf(runningRow))
      val outcome = Try(name match {
        case Indexes => indexes(connection, runningRow)
        case _ => spaces(runningRow)
      })
      val ended = outcome.fold(e => runningRow.copy(state = Failed.toString, error = Some(Errors.describe(e)), finished = Some(Instant.now)), identity)
      Try(store.saveOnlineJob(ended))
      report(statusOf(ended))
      again
    }
  }

  private def indexes(connection: Connection, row: OnlineJobRow): OnlineJobRow = {
    val tables = DB(connection).autoClose(false).autoCommit { implicit session =>
      store.buildIndexesIn(session)
      PostgresDocumentStore.buildSpaceIndexesIn(session)
    }
    row.copy(state = Done.toString, result = Some(json(JobCounts(Map("documentTables" -> tables.size.toLong)))), finished = Some(Instant.now))
  }

  private def spaces(row: OnlineJobRow): OnlineJobRow = {
    val result = store.backfillSpaces(settings.spaceOf)
    row.copy(state = Done.toString, result = Some(json(JobCounts(Map("filled" -> result.filled.toLong, "left" -> result.left.size.toLong)))),
      error = if (result.left.isEmpty) None else Some("aggregates left without a space (the first 100): " + result.left.take(100).map(_.asLong).mkString(", ")),
      finished = Some(Instant.now))
  }

  private def json(value: AnyRef): String = store.jsons.serialize(value, value.getClass.getName)

  private def read[T <: AnyRef : scala.reflect.ClassTag](text: String): Option[T] =
    Try(store.jsons.deserialize[T](text, implicitly[scala.reflect.ClassTag[T]].runtimeClass.getName)).toOption

  private def statusOf(row: OnlineJobRow): OnlineJobStatus = OnlineJobStatus(row.name, OnlineJobState.of(row.state), row.error,
    row.result.flatMap(read[JobCounts]).fold(Map.empty[String, Long])(_.counts), row.started, row.finished)

  private object DirectExecutor extends Executor {
    override def execute(command: Runnable): Unit = command.run()
  }
}

/** CR-3: a job's counts, as saved. */
private[jobs] case class JobCounts(counts: Map[String, Long])
