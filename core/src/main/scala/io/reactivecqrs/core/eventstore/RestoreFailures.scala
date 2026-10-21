package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.util.Errors

/** CR-4: restoring an aggregate failed at one of its events: the event of `version` of `eventAggregateId` (the aggregate,
  * or a base it was duplicated from), stored as `eventId` when known; `getCause` is what failed: the event handler, or
  * reading the stored event. */
class RestoreFailedAtEventException(val eventAggregateId: AggregateId, val version: Int, val eventClass: String, val eventId: Option[Long], cause: Throwable)
  extends RuntimeException("Applying " + eventClass + " at version " + version + " of aggregate " + eventAggregateId.asLong + " failed", cause)

/** Where an aggregate could not be restored (CR-4): by its owner, by `RestoreCheck`, or by a build of a projection. */
sealed trait RestoreFailureSource
object RestoreFailureSource {
  /** The aggregate's owner, for a command, a query or a re-publication. */
  case object Owner extends RestoreFailureSource
  /** `RestoreCheck`, which restores every aggregate of some types. */
  case object Check extends RestoreFailureSource
  /** A build of a managed projection (its bulk pass, reconciliation, final states or a targeted rebuild), under the
    * build's subscriber name. */
  case class Build(projection: String, build: Int, subscriber: String) extends RestoreFailureSource
}

/** CR-4: one aggregate that could not be restored, as recorded: its type and space (-1 when its row is gone), the
  * version and event id of the event it failed at (0 and None when not known), what failed (the exception's class, and
  * its message as `Errors.describe` gives it, cut to 1 000 characters), where, when first and last, how many times, and
  * since when an operator skipped it in that build (`SkipAggregate`). */
case class RestoreFailure(aggregateId: AggregateId, aggregateType: AggregateType, spaceId: SpaceId, version: Int, eventId: Option[Long],
                          errorClass: String, error: String, source: RestoreFailureSource, firstSeen: Instant, lastSeen: Instant,
                          times: Int, skipped: Option[Instant])

/** CR-4: which records `restoreFailures` lists; `projection` keeps the records of that projection's builds. */
case class RestoreFailureFilter(aggregateType: Option[AggregateType] = None, spaceId: Option[SpaceId] = None, projection: Option[String] = None)

/** CR-4: a failed restore to record. */
case class RestoreFailureSeen(aggregateId: AggregateId, aggregateType: AggregateType, source: RestoreFailureSource, version: Int,
                              eventId: Option[Long], eventAggregateId: AggregateId, errorClass: String, error: String)

object RestoreFailureSeen {
  /** The failure `e` of restoring the aggregate: at the event a `RestoreFailedAtEventException` in it names, if any. */
  def of(aggregateId: AggregateId, aggregateType: AggregateType, source: RestoreFailureSource, e: Throwable): RestoreFailureSeen = {
    val at = causes(e).collectFirst { case r: RestoreFailedAtEventException => r }
    val cause = at.flatMap(r => Option(r.getCause)).getOrElse(innermost(e))
    RestoreFailureSeen(aggregateId, aggregateType, source, at.fold(0)(_.version), at.flatMap(_.eventId), at.fold(aggregateId)(_.eventAggregateId),
      cause.getClass.getName, Errors.describe(cause))
  }

  // The library's wrappers (the owner's AggregateRestoreFailedException) name nothing the cause does not.
  private def innermost(e: Throwable): Throwable = causes(e).filterNot(_.isInstanceOf[io.reactivecqrs.api.AggregateRestoreFailedException]).headOption.getOrElse(e)

  private def causes(e: Throwable): List[Throwable] = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).toList
}
