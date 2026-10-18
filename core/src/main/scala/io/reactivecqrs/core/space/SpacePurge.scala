package io.reactivecqrs.core.space

import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor._
import io.reactivecqrs.core.eventstore.{PostgresEventStoreState, StoredAggregate}
import io.reactivecqrs.core.projection.{PurgeCheck, PurgeCheckAnswer, PurgeSpace, PurgedAggregates, PurgedAggregatesForgotten, SpacePurged}
import io.reactivecqrs.core.util.BlockingDispatcher
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import io.reactivecqrs.core.util.SqlLogging.withoutValues
import io.reactivecqrs.core.util.Errors

/** R17: the host's own rows of purged aggregates (an outbox writer's, a projection's own tables), deleted in the purge's
  * transaction with the library's. */
trait PurgeHook {
  /** For the report. */
  def name: String
  /** Deletes the host's rows of these aggregates of the space, in the purge's transaction; answers how many it deleted. */
  def purge(space: SpaceId, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Long
}

/** Batches: at most `aggregatesPerBatch` aggregates and `eventsPerBatch` events (by version) per transaction, a larger
  * aggregate alone; passes repeat while one finds aggregates of the space, at most `maxPasses`. */
case class SpacePurgeOptions(aggregatesPerBatch: Int = 1000, eventsPerBatch: Int = 100000, maxPasses: Int = 3,
                             answerTimeout: FiniteDuration = 2.minutes)

/** What a purge did. `rows`: the rows deleted per table (a hook's under its name); `detached`: the aggregates of other
  * organizations that no longer read the space's; `projections`: those that cleared the space; `failed`: the projections
  * and command buses that did not do their part, with why (run the purge again); `refused`: why it did not start, having
  * deleted nothing; `remaining`: the space's aggregates still stored after the last pass. */
case class PurgeReport(space: SpaceId, passes: Int, aggregates: Int, rows: Map[String, Long], detached: Seq[AggregateId],
                       projections: Seq[String], failed: Map[String, String], refused: Seq[String], remaining: Int) {
  /** Whether the space is gone, everywhere. */
  def complete: Boolean = refused.isEmpty && failed.isEmpty && remaining == 0
}

/**
  * R17: removes an organization (a space) from the installation: its aggregates with everything the library stores for
  * them, its sagas, what the projections derived from it, and the host's rows through `hooks`. Run once, from one node,
  * while the organization is quiet. `commandBuses` are this node's, one per type of the space's aggregates; `projections`
  * are those that hold the space's documents (a managed one's supervisor). Every step is idempotent: a purge that stopped
  * or failed is run again. Design: docs/design/M6-organizations.md, "Purge".
  */
class SpacePurge(eventStore: PostgresEventStoreState, commandBuses: Map[AggregateType, ActorRef], projections: Seq[ActorRef],
                 hooks: Seq[PurgeHook] = Nil, options: SpacePurgeOptions = SpacePurgeOptions())(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(classOf[SpacePurge])
  private implicit val timeout: Timeout = Timeout(options.answerTimeout)
  private implicit val blocking: ExecutionContext = BlockingDispatcher(system)

  /** Purges the organization `space`. The platform's space (0) and the legacy one (-1: what was stored before spaces,
    * until `backfillSpaces`) are not organizations: their purge is refused before anything is deleted (F200). */
  def purge(space: SpaceId): Future[PurgeReport] = {
    val start = PurgeReport(space, passes = 0, aggregates = 0, rows = Map.empty, detached = Nil, projections = Nil, failed = Map.empty,
      refused = Nil, remaining = 0)
    if (space.asLong <= 0) {
      val reason = "space " + space.asLong + " is " + (if (space.asLong == 0) "the platform's" else "not an organization's") +
        ": a purge removes one organization only"
      log.error("Purge refused: " + reason)
      return Future.successful(start.copy(refused = Seq(reason)))
    }
    log.info("Purging space " + space.asLong)
    passes(space, start).flatMap { report =>
      if (report.refused.nonEmpty) Future.successful(report)
      else for {
        sagas <- Future(deleteSagas(space))
        (cleared, failed) <- clearProjections(space)
      } yield report.copy(rows = report.rows ++ sagas, projections = cleared, failed = report.failed ++ failed)
    }.map { report =>
      if (report.complete) log.info("Space " + space.asLong + " purged: " + report)
      else log.warn("Space " + space.asLong + " not purged completely: " + report)
      report
    }
  }

  // ************** Passes

  private case class Plan(types: Set[AggregateType], duplicates: Seq[(AggregateType, AggregateId, AggregateId)])

  private def passes(space: SpaceId, report: PurgeReport): Future[PurgeReport] =
    onePass(space, report.copy(passes = report.passes + 1)).flatMap { case (after, found) =>
      if (after.refused.nonEmpty || found == 0 || after.passes >= options.maxPasses) Future(after.copy(remaining = eventStore.spaceSize(space)))
      else passes(space, after)
    }

  // Answers the report after the pass and how many aggregates of the space the pass found.
  private def onePass(space: SpaceId, report: PurgeReport): Future[(PurgeReport, Int)] =
    Future(Plan(eventStore.spaceTypes(space), eventStore.duplicatesInOtherSpaces(space))).flatMap { plan =>
      refusals(space, plan).flatMap {
        case refused if refused.nonEmpty => Future.successful((report.copy(refused = refused), 0))
        case _ => detach(plan).flatMap {
          case Left(refused) => Future.successful((report.copy(refused = refused), 0))
          case Right(detached) => deleteAll(space).map { case (rows, purged, failed) =>
            (report.copy(aggregates = report.aggregates + purged, rows = sum(report.rows, rows), detached = report.detached ++ detached,
              failed = report.failed ++ failed), purged)
          }
        }
      }
    }

  // Before a row changes: every type has its command bus here, and every projection can clear a space.
  private def refusals(space: SpaceId, plan: Plan): Future[Seq[String]] = {
    val missingBuses = (plan.types ++ plan.duplicates.map(_._1)).filterNot(commandBuses.contains).toSeq.map(t =>
      "aggregates of type " + t.typeName + " are in the space or read it, and no command bus of the type was given")
    Future.sequence(projections.map(projection => (projection ? PurgeCheck(space)).map {
      case PurgeCheckAnswer(_, refusal) => refusal
      case other => Some("projection " + projection.path.name + " answered " + other)
    }.recover { case e => Some("projection " + projection.path.name + " did not answer: " + Errors.describe(e)) }))
      .map(missingBuses ++ _.flatten)
  }

  // The aggregates of other organizations that read the space's, base by base, each once (a detach takes a copy off all
  // its bases); the first refusal ends the purge.
  private def detach(plan: Plan): Future[Either[Seq[String], Seq[AggregateId]]] =
    plan.duplicates.groupBy(d => (d._1, d._2)).toSeq.sortBy(_._1._2.asLong)
      .foldLeft(Future.successful[Either[Seq[String], Seq[AggregateId]]](Right(Nil))) { case (done, ((aggregateType, baseId), rows)) =>
        done.flatMap {
          case Right(detached) =>
            val pending = rows.map(_._3).filterNot(detached.contains)
            if (pending.isEmpty) Future.successful(Right(detached))
            else (commandBuses(aggregateType) ? DetachDuplicates(baseId, pending)).map {
              case DuplicatesDetached(_, duplicates) => Right(detached ++ duplicates)
              case DetachRefused(_, reason) => Left(Seq("duplicates of aggregate " + baseId.asLong + " cannot be detached: " + reason))
              case other => Left(Seq("the command bus of " + aggregateType.typeName + " answered " + other))
            }.recover { case e => Left(Seq("detaching the duplicates of aggregate " + baseId.asLong + " failed: " + Errors.describe(e))) }
          case refused => Future.successful(refused)
        }
      }

  // ************** Deleting

  // Page by page, newest first (a copy usually goes before its base); answers the rows deleted, the aggregates purged and
  // what did not do its part.
  private def deleteAll(space: SpaceId): Future[(Map[String, Long], Int, Map[String, String])] = {
    def from(before: AggregateId, rows: Map[String, Long], purged: Int, failed: Map[String, String]): Future[(Map[String, Long], Int, Map[String, String])] =
      Future(eventStore.spaceAggregates(space, before, options.aggregatesPerBatch)).flatMap { page =>
        if (page.isEmpty) Future.successful((rows, purged, failed))
        else batches(page).foldLeft(Future.successful((rows, purged, failed))) { case (done, batch) =>
          done.flatMap { case (r, p, f) => deleteBatch(space, batch).map { case (batchRows, batchPurged, batchFailed) =>
            (sum(r, batchRows), p + batchPurged, f ++ batchFailed)
          }}
        }.flatMap { case (r, p, f) => from(page.last.aggregateId, r, p, f) }
      }
    from(AggregateId(Long.MaxValue), Map.empty, 0, Map.empty)
  }

  private def batches(page: Seq[StoredAggregate]): Seq[Seq[StoredAggregate]] = {
    val cut = Vector.newBuilder[Seq[StoredAggregate]]
    var batch = Vector.empty[StoredAggregate]
    var events = 0L
    page.foreach { aggregate =>
      if (batch.nonEmpty && events + aggregate.version.asInt > options.eventsPerBatch) {
        cut += batch
        batch = Vector.empty
        events = 0L
      }
      batch :+= aggregate
      events += aggregate.version.asInt
    }
    if (batch.nonEmpty) cut += batch
    cut.result()
  }

  // One transaction: the chains locked, the hooks, the library's rows; then the owners and the projections forget them.
  private def deleteBatch(space: SpaceId, batch: Seq[StoredAggregate]): Future[(Map[String, Long], Int, Map[String, String])] =
    Future {
      DB.localTx { implicit session =>
        val ids = eventStore.lockForPurge(space, batch.map(_.aggregateId))
        val hookRows = if (ids.isEmpty) Map.empty[String, Long] else hooks.map(hook => hook.name -> hook.purge(space, ids)).toMap
        (sum(eventStore.purgeAggregates(ids), hookRows), ids)
      }
    }.flatMap { case (rows, ids) =>
      val gone = ids.toSet
      val owners = batch.filter(a => gone.contains(a.aggregateId)).groupBy(_.aggregateType).toSeq.map { case (aggregateType, aggregates) =>
        (commandBuses(aggregateType) ? ForgetAggregates(aggregates.map(_.aggregateId))).map(_ => None)
          .recover { case e => Some(("command bus of " + aggregateType.typeName) -> ("did not stop the owners of purged aggregates: " + Errors.describe(e))) }
      }
      val told = if (ids.isEmpty) Nil else projections.map(projection => (projection ? PurgedAggregates(space, ids)).map {
        case PurgedAggregatesForgotten(_, _) => None
        case other => Some(projection.path.name -> ("answered " + other + " to the purged aggregates"))
      }.recover { case e => Some(projection.path.name -> ("did not forget the purged aggregates: " + Errors.describe(e))) })
      Future.sequence(owners ++ told).map(failures => (rows, ids.size, failures.flatten.toMap))
    }

  // R17: sagas tagged with the space (SagaDefinition.spaceOf); untagged ones are not purged.
  private def deleteSagas(space: SpaceId): Map[String, Long] = DB.autoCommit { implicit session =>
    val tagged = sql"""SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema()
                     |  AND table_name = 'sagas' AND column_name = 'space_id')""".stripMargin.map(_.boolean(1)).single().getOrElse(false)
    if (tagged) Map("sagas" -> sql"""DELETE FROM sagas WHERE space_id = ${space.asLong}""".update().toLong) else Map.empty
  }

  // Last, once nothing of the space is stored: each projection clears what it derived from the space.
  private def clearProjections(space: SpaceId): Future[(Seq[String], Map[String, String])] =
    Future.sequence(projections.map(projection => (projection ? PurgeSpace(space)).map {
      case SpacePurged(name, _) => Left(name)
      case other => Right(projection.path.name -> ("answered " + other + " to clearing the space"))
    }.recover { case e => Right(projection.path.name -> ("could not clear the space: " + Errors.describe(e))) }))
      .map(answers => (answers.collect { case Left(name) => name }, answers.collect { case Right(failure) => failure }.toMap))

  private def sum(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] =
    (a.keySet ++ b.keySet).map(table => table -> (a.getOrElse(table, 0L) + b.getOrElse(table, 0L))).toMap
}
