package io.reactivecqrs.core.tools

import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.{EventStoreState, RestoreFailureSeen, RestoreFailureSource, VersionGap}
import io.reactivecqrs.core.projection.MissingVersions
import io.reactivecqrs.core.util.Errors

import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * CR-4: restores every aggregate of the given contexts' types with this code, as their owners would (snapshots
  * included), and records those that cannot be restored in the store's records of failed restores
  * (`RestoreFailureSource.Check`, `EventStoreState.restoreFailures`), clearing the record of each that restores now. CR-6:
  * it also finds the aggregates whose own stored versions are not contiguous, which restore but stop every build of
  * their type, and records them the same way. Run it from the application, which has every class and context, against a
  * copy of production before an upgrade, or online: it reads one page of aggregates at a time, `pause` apart. A database
  * that cannot be reached stops it with its failure.
  * {{{
  * val report = RestoreCheck.run(eventStore, Seq(cartContext, orderContext))
  * println(report.text)
  * }}}
  */
object RestoreCheck {

  /** Aggregates restored per type (its class name), how many could not be, and the first `listed` of those; why is in
    * the store's records (`restoreFailures`). `gaps` (CR-6): the first `listed` aggregates with versions missing from the
    * store, `gapsTotal` all of them. */
  case class Report(checked: Map[String, Long], failedTotal: Long, failed: Seq[AggregateId], gaps: Seq[VersionGap] = Nil, gapsTotal: Long = 0) {
    def text: String = s"Restore check: ${checked.values.sum} aggregates restored, $failedTotal could not be, $gapsTotal miss versions.\n" +
      checked.toSeq.sorted.map { case (t, n) => s"$t: $n" }.mkString("\n") +
      (if (failed.isEmpty) "" else "\nThe first that could not (restoreFailures() says why): " + failed.map(_.asLong).mkString(", ")) +
      (if (gaps.isEmpty) "" else "\nThe first that miss versions: " + gaps.map(g => s"${g.aggregateId.asLong} (${g.missing} from ${g.firstMissing.asInt})").mkString(", ")) + "\n"
  }

  /** Restores every aggregate of each context's type; see the object's description. */
  def run(eventStore: EventStoreState, contexts: Seq[AggregateContext[_]], pageSize: Int = 1000, pause: FiniteDuration = Duration.Zero,
          listed: Int = 100): Report = {
    var checked = Map[String, Long]()
    var (failedTotal, gapsTotal) = (0L, 0L)
    val failed = Vector.newBuilder[AggregateId]
    val gaps = Vector.newBuilder[VersionGap]
    contexts.map(_.asInstanceOf[AggregateContext[Any]]).foreach { context =>
      val aggregateType = context.aggregateType
      val restorer = new AggregateRestorer[Any](eventStore, context.eventHandlers, () => context.initialAggregateRoot, context.snapshots,
        context.version, context.aggregateRootClass.getName, Some(aggregateType))
      var after = AggregateId(Long.MinValue)
      var more = true
      while (more) {
        val page = eventStore.aggregatesOf(Seq(aggregateType), after, pageSize)
        page.foreach { stored =>
          checked += aggregateType.typeName -> (checked.getOrElse(aggregateType.typeName, 0L) + 1)
          try {
            restorer.restore(stored.aggregateId)
            eventStore.clearRestoreFailure(stored.aggregateId, RestoreFailureSource.Check)
          } catch {
            case NonFatal(e) if !Errors.transient(e) =>
              eventStore.recordRestoreFailure(RestoreFailureSeen.of(stored.aggregateId, aggregateType, RestoreFailureSource.Check, e))
              failedTotal += 1
              if (failedTotal <= listed) failed += stored.aggregateId
          }
        }
        // CR-6: a restore applies what it reads, with no check of the versions: the gaps are looked for apart.
        eventStore.versionGaps(page.map(_.aggregateId)).foreach { gap =>
          eventStore.recordRestoreFailure(RestoreFailureSeen(gap.aggregateId, aggregateType, RestoreFailureSource.Check, gap.firstMissing.asInt, None,
            gap.aggregateId, "missing versions", MissingVersions.Prefix + gap.firstMissing.asInt + " on are missing from the store: " + gap.missing + " in all"))
          gapsTotal += 1
          if (gapsTotal <= listed) gaps += gap
        }
        after = page.lastOption.map(_.aggregateId).getOrElse(after)
        more = page.size == pageSize
        if (more && pause > Duration.Zero) Thread.sleep(pause.toMillis)
      }
    }
    Report(checked, failedTotal, failed.result(), gaps.result(), gapsTotal)
  }
}
