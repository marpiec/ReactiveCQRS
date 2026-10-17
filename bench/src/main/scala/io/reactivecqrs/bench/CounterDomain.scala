package io.reactivecqrs.bench

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import scalikejdbc.DBSession

// The benchmark domain: a counter whose handlers take constant time, so the measurements show the library's cost
// and not the domain's (the test domain's cart handlers scan all items on every event). Events carry a note of a
// configurable size, so serialization costs about what a real event costs.

case class Counter(value: Long)

case class CreateCounter(userId: UserId) extends FirstCommand[Counter, CustomCommandResponse[_]]
case class Increment(userId: UserId, aggregateId: AggregateId, note: String) extends ConcurrentCommand[Counter, CustomCommandResponse[_]]

case class CounterCreated(spaceId: SpaceId) extends FirstEvent[Counter]
case class Incremented(note: String) extends Event[Counter]

class CounterContext extends AggregateContext[Counter] {
  override val version: Int = 1
  override def initialAggregateRoot: Counter = Counter(0)

  override def commandHandlers = _ => {
    case _: CreateCounter => CommandSuccess(CounterCreated(SpaceId(0)))
    case c: Increment => CommandSuccess(Incremented(c.note))
  }

  override def eventHandlers = (_, _, counter) => {
    case _: CounterCreated => Counter(0)
    case _: Incremented => counter.copy(value = counter.value + 1)
  }
}

/** R8: one command at a time per counter. */
class SerialCounterContext extends CounterContext {
  override val commandProcessing: CommandProcessing = CommandProcessing.Serial()
}

/** D30: a snapshot every 200 events and when the owner stops. */
class SnapshottingCounterContext extends CounterContext {
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = 200))
}

case class CounterProgress(increments: Long)

/** Counts the increments it received per counter; the benchmark reads the sum to see when it caught up. `failing` makes
  * the listener throw for a counter, as a listener with a temporary problem does. */
class CountingProjection(
                         val subscriptionsState: SubscriptionsState,
                         documentStore: DocumentStore[CounterProgress],
                         override protected val projectionName: String,
                         failing: AggregateId => Boolean = _ => false) extends ProjectionActor {

  protected val listeners = List(EventsListener(update))

  private def update(aggregateId: AggregateId, events: Seq[EventInfo[Counter]]) = { implicit session: DBSession =>
    if (failing(aggregateId)) throw new IllegalStateException("the listener fails for a while on counter " + aggregateId.asLong)
    events.foreach(_.event match {
      case _: CounterCreated => documentStore.insertDocument(0, aggregateId.asLong, CounterProgress(0))
      case _: Incremented => documentStore.updateDocument(0, aggregateId.asLong, {
        case Some(doc) => Document(CounterProgress(doc.document.increments + 1))
        case None => Document(CounterProgress(1))
      })
      case _ => ()
    })
  }

  override protected def receiveQuery: Receive = PartialFunction.empty
}

/** Keeps the latest counter value, from the aggregate state the bus sends with the events. */
class CounterStateProjection(
                             val subscriptionsState: SubscriptionsState,
                             documentStore: DocumentStore[Counter],
                             override protected val projectionName: String) extends ProjectionActor {

  protected val listeners = List(AggregateListener(update))

  private def update(aggregateId: AggregateId, version: AggregateVersion, created: Boolean, counter: Option[Counter]) = { implicit session: DBSession =>
    counter match {
      case Some(c) if created => documentStore.insertDocument(0, aggregateId.asLong, c)
      case Some(c) => documentStore.overwriteDocument(aggregateId.asLong, c)
      case None => documentStore.removeDocument(aggregateId.asLong)
    }
  }

  override protected def receiveQuery: Receive = PartialFunction.empty
}

// The managed projections of the rebuild scenario (M4) have classes of their own: the earlier scenarios ran the classes
// above unmanaged, and a managed projection whose class has subscription rows is adopted as one 0.12 built (D81).

class ManagedCountingProjection(subscriptionsState: SubscriptionsState, documentStore: DocumentStore[CounterProgress], projectionName: String,
                                failing: AggregateId => Boolean)
  extends CountingProjection(subscriptionsState, documentStore, projectionName, failing)

class ManagedCounterStateProjection(subscriptionsState: SubscriptionsState, documentStore: DocumentStore[Counter], projectionName: String)
  extends CounterStateProjection(subscriptionsState, documentStore, projectionName)
