package io.reactivecqrs.core.rewrite

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.projection.RebuildTarget
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{AggregatesRebuilt, RebuildAggregates, RebuildRefused}
import io.reactivecqrs.core.util.BlockingDispatcher
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** What a complete rewrite did (D76). `rewritten`: the aggregates whose events or archived events it wrote again;
  * `unchanged`: those it left as they were; `refused`: those it wrote nothing of, with why; `bases`: the aggregates the
  * first one reads, which it does not change (the value may have come from there); `rebuilt`: the projections that
  * rebuilt every changed aggregate; `failed`: the projections that did not, with why. Run it again for the rest: a change
  * finds nothing to change where it was applied. */
case class RewriteReport(aggregateId: AggregateId, rewritten: Seq[HistoryRewritten], unchanged: Seq[AggregateId], refused: Seq[RewriteRefused],
                         bases: Seq[AggregateId], rebuilt: Seq[String], failed: Map[String, String]) {
  /** Whether everything was rewritten and rebuilt. */
  def complete: Boolean = refused.isEmpty && failed.isEmpty
}

/**
  * D76, R15: a complete rewrite, for erasure. Applies `change` to an aggregate and to every aggregate copied from it, at
  * any depth (attached duplicates, and detached copies through the lineage), each in its owner through `commandBus`, one
  * at a time; then has each projection given rebuild the aggregates that changed, one at a time. Nothing is published:
  * projections that are not given keep what they hold. Run from one node, for aggregates of `commandBus`'s type.
  * Design: docs/design/M6-organizations.md, "The complete rewrite".
  */
class HistoryRewriter(commandBus: ActorRef, eventStore: EventStoreState, projections: Map[String, ActorRef] = Map.empty,
                      answerTimeout: FiniteDuration = 5.minutes)(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(classOf[HistoryRewriter])
  private implicit val timeout: Timeout = Timeout(answerTimeout)
  private implicit val blocking: ExecutionContext = BlockingDispatcher(system)

  /** Rewrites the aggregate and its copies with `change`, as `userId`, and rebuilds what changed in the projections given. */
  def rewrite(aggregateId: AggregateId, change: HistoryChange, userId: UserId): Future[RewriteReport] = {
    log.info("Rewriting aggregate " + aggregateId.asLong + " and its copies with " + change.getClass.getSimpleName)
    for {
      bases <- Future(eventStore.basesOf(aggregateId))
      walked <- walk(Vector(aggregateId), Set(aggregateId), change, userId,
        RewriteReport(aggregateId, Nil, Nil, Nil, bases, Nil, Map.empty))
      report <- rebuild(walked.rewritten.map(_.id), projections.toList, walked)
    } yield {
      if (report.complete) log.info("Aggregate " + aggregateId.asLong + " rewritten: " + report)
      else log.warn("Aggregate " + aggregateId.asLong + " not rewritten completely: " + report)
      report
    }
  }

  // Breadth first, one aggregate at a time, each once: a copy of a copy is found through the copy.
  private def walk(queue: Vector[AggregateId], seen: Set[AggregateId], change: HistoryChange, userId: UserId,
                   report: RewriteReport): Future[RewriteReport] = queue.headOption match {
    case None => Future.successful(report)
    case Some(id) =>
      (commandBus ? RewriteEverywhere(id, change, userId)).mapTo[RewriteResponse]
        .recover { case e => RewriteRefused(id, "no answer: " + e.getMessage) }
        .flatMap { answer =>
          Future(eventStore.copiesOf(id)).flatMap { copies =>
            val found = copies.filterNot(seen.contains)
            val next = answer match {
              case done: HistoryRewritten if done.events + done.archived > 0 => report.copy(rewritten = report.rewritten :+ done)
              case _: HistoryRewritten => report.copy(unchanged = report.unchanged :+ id)
              case refused: RewriteRefused => report.copy(refused = report.refused :+ refused)
            }
            walk(queue.tail ++ found, seen ++ found, change, userId, next)
          }
        }
  }

  // Each projection rebuilds each changed aggregate in place, one target at a time (D63).
  private def rebuild(changed: Seq[AggregateId], left: List[(String, ActorRef)], report: RewriteReport): Future[RewriteReport] = left match {
    case Nil => Future.successful(report)
    case (name, projection) :: rest =>
      changed.foldLeft(Future.successful(Option.empty[String])) { (previous, id) =>
        previous.flatMap {
          case failure @ Some(_) => Future.successful(failure)
          case None => (projection ? RebuildAggregates(RebuildTarget.OneAggregate(id))).map {
            case _: AggregatesRebuilt => None
            case RebuildRefused(reason) => Some("aggregate " + id.asLong + ": " + reason)
            case other => Some("aggregate " + id.asLong + ": " + other)
          }.recover { case e => Some("aggregate " + id.asLong + ": no answer: " + e.getMessage) }
        }
      }.flatMap { failure =>
        rebuild(changed, rest, failure.fold(report.copy(rebuilt = report.rebuilt :+ name))(f => report.copy(failed = report.failed + (name -> f))))
      }
  }
}
