package io.reactivecqrs.bench

import java.sql.DriverManager

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
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
  * whose bus starts without a cursor and whose projections start empty, for replaying stored events into new projection tables.
  */
class BenchSystem(name: String, stores: BenchStores, projections: Boolean, rebuild: Boolean = false) {

  val system: ActorSystem = ActorSystem(name)

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(stores.aggregatesUids, stores.commandsUids, stores.sagasUids)))

  private val subscribers = if (projections) 2 else 0
  val subscriptionsManager = new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(subscribers))))

  // PostgreSQL in every system: the projections write to PostgreSQL document stores inside its transactions. A rebuild
  // starts empty because its projections have names of their own.
  val subscriptionsState: SubscriptionsState = {
    val state = new PostgresSubscriptionsState(stores.typesNamesState, true)
    state.initSchema()
    state
  }

  private val eventBusState: EventBusState = if (rebuild) new MemoryEventBusState else new PostgresEventBusState().initSchema()

  val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor(eventBusState, subscriptionsManager)), "eventBus")

  val counters: ActorRef = system.actorOf(AggregateCommandBusActor(new CounterContext, uidGenerator, stores.eventStore,
    stores.commandResponseState, eventBus, eventsReplayMode = rebuild), "counters")

  /** The table whose increments sum shows how far the events-based projection got. */
  val progressTable: String = "projection_" + name + "_progress"

  if (projections) {
    val progress = new PostgresDocumentStore[CounterProgress](name + "_progress", stores.mpjsons, new NoopDocumentStoreCache[CounterProgress, AnyRef])
    val state = new PostgresDocumentStore[Counter](name + "_state", stores.mpjsons, new NoopDocumentStoreCache[Counter, AnyRef])
    system.actorOf(Props(new CountingProjection(subscriptionsManager, subscriptionsState, progress, name + "Progress")), "progressProjection")
    system.actorOf(Props(new CounterStateProjection(subscriptionsManager, subscriptionsState, state, name + "State")), "stateProjection")
  }

  /** Increments the events-based projection has applied so far. */
  def projectedIncrements(): Long = DB.readOnly { implicit session =>
    SQL(s"SELECT coalesce(sum((document->>'increments')::bigint), 0) FROM $progressTable").map(_.long(1)).single().get
  }

  def shutdown(): Unit = Await.result(system.terminate(), 30.seconds)
}
