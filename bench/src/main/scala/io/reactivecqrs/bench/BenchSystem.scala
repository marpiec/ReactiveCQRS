package io.reactivecqrs.bench

import java.sql.DriverManager

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, SubscriptionsState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

/** The benchmark database: its own schema in the configured database, dropped at the end unless kept. */
object BenchDatabase {

  val url: String = sys.props.getOrElse("bench.url", "jdbc:postgresql://localhost:5432/reactivecqrs")
  private val user = sys.props.getOrElse("bench.user", "reactivecqrs")
  private val password = sys.props.getOrElse("bench.password", "reactivecqrs")

  val schema: String = "bench_" + System.currentTimeMillis() / 1000

  def init(): Unit = {
    Class.forName("org.postgresql.Driver")
    withPlainConnection(_.createStatement().execute("CREATE SCHEMA " + schema))
    ConnectionPool.singleton(url + "?currentSchema=" + schema, user, password,
      ConnectionPoolSettings(initialSize = 10, maxSize = 40, connectionTimeoutMillis = 5000L))
  }

  def drop(): Unit = {
    ConnectionPool.closeAll()
    if (!sys.props.contains("bench.keepSchema")) {
      withPlainConnection(_.createStatement().execute("DROP SCHEMA " + schema + " CASCADE"))
    }
  }

  private def withPlainConnection(f: java.sql.Connection => Unit): Unit = {
    val connection = DriverManager.getConnection(url, user, password)
    try f(connection) finally connection.close()
  }
}

/** The stores shared by every system of one benchmark run. */
class BenchStores {
  val mpjsons = new MPJsons
  val typesNamesState: PostgresTypesNamesState = new PostgresTypesNamesState().initSchema()
  /** Without the default limit of 10 000 events per aggregate, for the restore measurements. */
  val eventStore: PostgresEventStoreState = new PostgresEventStoreState(mpjsons, typesNamesState, aggregateVersionLimit = 10000000).initSchema()
  val commandResponseState: PostgresCommandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  val aggregatesUids = new PostgresUidGenerator("aggregates_uids_seq")
  val commandsUids = new PostgresUidGenerator("commands_uids_seq")
  val sagasUids = new PostgresUidGenerator("sagas_uids_seq")
}

/**
  * One running system, wired the way a host application wires it (see `ReactiveTestDomainSpec`): a command bus for
  * counters and, optionally, two projections, one events-based and one aggregate-based. `rebuild` wires a system
  * whose projections start empty, for replaying stored events into new projection tables. `liveSubscriptions` keeps the
  * subscription versions in the database, as live nodes do (the default in-memory mode is the one the M1 baseline used).
  * `failing` makes the events-based projection fail for a counter.
  */
class BenchSystem(name: String, stores: BenchStores, projections: Boolean, rebuild: Boolean = false, liveSubscriptions: Boolean = false,
                  failing: io.reactivecqrs.api.id.AggregateId => Boolean = _ => false) {

  val system: ActorSystem = ActorSystem(name)

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(stores.aggregatesUids, stores.commandsUids, stores.sagasUids)))

  // PostgreSQL in every system: the projections write to PostgreSQL document stores inside its transactions. A rebuild
  // starts empty because its projections have names of their own.
  val subscriptionsState: SubscriptionsState = {
    val state = new PostgresSubscriptionsState(stores.typesNamesState, keepInMemory = !liveSubscriptions)
    state.initSchema()
    state
  }

  val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor()), "eventBus")

  val counters: ActorRef = system.actorOf(AggregateCommandBusActor(new CounterContext, uidGenerator, stores.eventStore,
    stores.commandResponseState, eventBus, eventsReplayMode = rebuild), "counters")

  /** The table whose increments sum shows how far the events-based projection got. */
  val progressTable: String = "projection_" + name + "_progress"

  private val subscribers = if (projections) {
    val progress = new PostgresDocumentStore[CounterProgress](name + "_progress", stores.mpjsons, new NoopDocumentStoreCache[CounterProgress, AnyRef])
    val state = new PostgresDocumentStore[Counter](name + "_state", stores.mpjsons, new NoopDocumentStoreCache[Counter, AnyRef])
    List(system.actorOf(Props(new CountingProjection(subscriptionsState, progress, name + "Progress", failing)), "progressProjection"),
      system.actorOf(Props(new CounterStateProjection(subscriptionsState, state, name + "State")), "stateProjection"))
  } else Nil
  eventBus ! EventsBusActor.RegisterSubscribers(subscribers)

  // Re-delivers what waits in the outbox (D8); a rebuild system publishes nothing of its own.
  if (!rebuild) system.actorOf(Props(new OutboxPollerActor(stores.eventStore, eventBus)), "outboxPoller")

  /** Increments the events-based projection has applied so far. */
  def projectedIncrements(): Long = DB.readOnly { implicit session =>
    SQL(s"SELECT coalesce(sum((document->>'increments')::bigint), 0) FROM $progressTable").map(_.long(1)).single().get
  }

  /** The event bus's deliveries still waiting, and those that ended undelivered. */
  def deliveryStatus(): EventsBusActor.DeliveryStatus =
    Await.result(org.apache.pekko.pattern.ask(eventBus, EventsBusActor.GetDeliveryStatus)(10.seconds), 10.seconds).asInstanceOf[EventsBusActor.DeliveryStatus]

  def shutdown(): Unit = Await.result(system.terminate(), 30.seconds)
}
