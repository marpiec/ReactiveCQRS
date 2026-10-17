package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import scalikejdbc.DBSession

import scala.util.Try

/**
  * D77: the store of an ephemeral command bus. Each aggregate's events stay in memory on this node, are never published
  * to subscribers, and are gone when the node stops. It refuses what the PostgreSQL store refuses, so an aggregate that
  * lives here can be materialized there: an event whose JSON, written by the host's `MPJsons`, is longer than the limit
  * for its class, and more events than the version limit. It refuses what ephemeral aggregates do not do: copies,
  * squash, archive and history rewrites. A permanent delete drops the aggregate.
  */
class EphemeralEventStoreState(mpjsons: MPJsons, eventSizeLimit: Int = 100000, eventSizeLimitPerClassName: Map[String, Int] = Map.empty,
                               aggregateVersionLimit: Int = 10000) extends MemoryEventStoreState(commitOrder = false) {

  override def persist[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[StoredEvent[AGGREGATE_ROOT]]] =
    Try(eventsEnvelope.events.zipWithIndex.foreach { case (event, i) =>
      val before = eventsEnvelope.expectedVersion.incrementBy(i) // the version before the event, as PostgreSQL counts it
      val typeName = AggregateType.ofClass(event.aggregateRootClass).typeName
      if (event.isInstanceOf[DuplicationEvent[_]]) throw new IllegalArgumentException("An ephemeral aggregate is not made as a copy (D77): " +
        event.getClass.getSimpleName)
      val size = mpjsons.serialize(event, event.getClass.getName).length
      val limit = eventSizeLimitPerClassName.getOrElse(event.getClass.getName, eventSizeLimit)
      if (size > limit) throw new EventTooLargeException(aggregateId, typeName, before, event.getClass.getName, size, limit)
      if (before.asInt >= aggregateVersionLimit) throw new TooManyEventsException(aggregateId, typeName, aggregateVersionLimit)
    }).flatMap(_ => super.persist(aggregateId, eventsEnvelope))

  override def squashBlocker(aggregateId: AggregateId, upTo: AggregateVersion): Option[String] = Some(notSquashed)

  override def squash[AGGREGATE_ROOT](aggregateId: AggregateId, upTo: AggregateVersion, squashEvent: FirstEvent[AGGREGATE_ROOT], userId: UserId,
                                      metadata: Option[EventMetadata], detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] =
    Left(notSquashed)

  override def detachDuplicates(baseId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] =
    Left("an ephemeral aggregate has no copies (D77)")

  override def rewriteEverywhere[AGGREGATE_ROOT](aggregateId: AggregateId, userId: UserId, change: HistoryChange,
                                                alter: (Event[AGGREGATE_ROOT], Option[EventMetadata]) => (Event[AGGREGATE_ROOT], Option[EventMetadata]))
                                               (implicit session: DBSession): Either[String, RewriteCount] =
    Left("an ephemeral aggregate's history is not rewritten (D77); materialize it first")

  override def deleteAggregatePermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = {
    forgetAggregate(aggregateId)
    Right(())
  }

  private val notSquashed = "an ephemeral aggregate is not squashed (D77); materialize it first"
}
