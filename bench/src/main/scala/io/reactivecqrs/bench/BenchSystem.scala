package io.reactivecqrs.bench

import java.sql.DriverManager

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, SubscriptionsState}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{Builds, GetBuilds}
import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuilderSettings
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

/** The benchmark database: its own schema in the configured database, dropped at the end unless kept. */
object BenchDatabase {
  val poolSize = 40

  val url: String = sys.props.getOrElse("bench.url", "jdbc:postgresql://localhost:5432/reactivecqrs")
  private val user = sys.props.getOrElse("bench.user", "reactivecqrs")
  private val password = sys.props.getOrElse("bench.password", "reactivecqrs")

  val schema: String = "bench_" + System.currentTimeMillis() / 1000

  def init(): Unit = {
    Class.forName("org.postgresql.Driver")
    withPlainConnection(_.createStatement().execute("CREATE SCHEMA " + schema))
    ConnectionPool.singleton(url + "?currentSchema=" + schema, user, password,
      ConnectionPoolSettings(initialSize = 10, maxSize = poolSize, connectionTimeoutMillis = 5000L))
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
  /** Without the default limit of 10 000 events per aggregate, for the restore measurements, and with documents of up
    * to 6 000 000 characters in one event (the default limit is 100 000). */
  val eventStore: PostgresEventStoreState = new PostgresEventStoreState(mpjsons, typesNamesState, aggregateVersionLimit = 10000000,
    eventSizeLimitPerClassName = Map(classOf[DocumentAttached].getName -> 6000000)).initSchema()
  val commandResponseState: PostgresCommandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  val aggregatesUids = new PostgresUidGenerator("aggregates_uids_seq")
  val commandsUids = new PostgresUidGenerator("commands_uids_seq")
  val sagasUids = new PostgresUidGenerator("sagas_uids_seq")
  val registry: PostgresProjectionRegistry = new PostgresProjectionRegistry().initSchema()
}

/**
  * One running system, wired the way a host application wires it (see `ReactiveTestDomainSpec`): a command bus for
  * counters and, optionally, two projections, one events-based and one aggregate-based. `managed` starts them through
  * the projections runtime (M4), so new ones are first built from the stored events; otherwise they are created
  * directly and take only what is delivered live. Subscription versions live in the database (F3). `failing` makes the
  * events-based projection fail for a counter; `context` sets the counters' command processing and snapshots. Managed,
  * `builder` sets how they are built, and `stateOnly` starts the aggregate-based one alone.
  */
class BenchSystem(name: String, stores: BenchStores, projections: Boolean, managed: Boolean = false,
                  failing: io.reactivecqrs.api.id.AggregateId => Boolean = _ => false, context: CounterContext = new CounterContext,
                  builder: BuilderSettings = BuilderSettings(), stateOnly: Boolean = false) {

  val system: ActorSystem = ActorSystem(name)

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(stores.aggregatesUids, stores.commandsUids, stores.sagasUids)))

  // PostgreSQL in every system: the projections write to PostgreSQL document stores inside its transactions. A rebuild
  // starts empty because its projections have names of their own.
  val subscriptionsState: SubscriptionsState = {
    val state = new PostgresSubscriptionsState(stores.typesNamesState)
    state.initSchema()
    state
  }

  val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor()), "eventBus")

  val counters: ActorRef = system.actorOf(AggregateCommandBusActor(context, uidGenerator, stores.eventStore,
    stores.commandResponseState, eventBus), "counters")

  /** The table whose increments sum shows how far the events-based projection got (its first build's, when managed). */
  val progressTable: String = "projection_" + name + "_progress" + (if (managed) "_v1" else "")

  private def progressStore(table: String) = new PostgresDocumentStore[CounterProgress](table, stores.mpjsons, new NoopDocumentStoreCache[CounterProgress, AnyRef])
  private def stateStore(table: String) = new PostgresDocumentStore[Counter](table, stores.mpjsons, new NoopDocumentStoreCache[Counter, AnyRef])

  private val subscribers = if (!projections) Nil else if (managed) {
    val runtime = new Projections(system, ProjectionsSettings(stores.eventStore, subscriptionsState, stores.registry, Seq(context), builder))
    val state = runtime.start[ManagedCounterStateProjection](name + "State", 1, "stateProjection") { build =>
      new ManagedCounterStateProjection(subscriptionsState, stateStore(build.documentStore(name + "_state")), name + "State")
    }
    if (stateOnly) List(state) else List(runtime.start[ManagedCountingProjection](name + "Progress", 1, "progressProjection") { build =>
        new ManagedCountingProjection(subscriptionsState, progressStore(build.documentStore(name + "_progress")), name + "Progress", failing)
      }, state)
  } else {
    List(system.actorOf(Props(new CountingProjection(subscriptionsState, progressStore(name + "_progress"), name + "Progress", failing))
      .withDispatcher(BlockingDispatcher.id), "progressProjection"),
      system.actorOf(Props(new CounterStateProjection(subscriptionsState, stateStore(name + "_state"), name + "State"))
        .withDispatcher(BlockingDispatcher.id), "stateProjection"))
  }
  eventBus ! EventsBusActor.RegisterSubscribers(subscribers)

  // Re-delivers what waits in the outbox (D8).
  system.actorOf(Props(new OutboxPollerActor(stores.eventStore, eventBus)), "outboxPoller")

  /** Whether every managed projection serves the build it has and builds nothing (M4). */
  def built(): Boolean = subscribers.forall { supervisor =>
    val builds = Await.result(org.apache.pekko.pattern.ask(supervisor, GetBuilds)(10.seconds), 10.seconds).asInstanceOf[Builds]
    builds.active.isDefined && builds.building.isEmpty
  }

  /** Increments the events-based projection has applied so far; none before its table exists (a managed build makes it). */
  def projectedIncrements(): Long = DB.readOnly { implicit session =>
    if (!SQL(s"SELECT to_regclass('$progressTable') IS NOT NULL").map(_.boolean(1)).single().get) 0L
    else SQL(s"SELECT coalesce(sum((document->>'increments')::bigint), 0) FROM $progressTable").map(_.long(1)).single().get
  }

  /** The event bus's deliveries still waiting, and those that ended undelivered. */
  def deliveryStatus(): EventsBusActor.DeliveryStatus =
    Await.result(org.apache.pekko.pattern.ask(eventBus, EventsBusActor.GetDeliveryStatus)(10.seconds), 10.seconds).asInstanceOf[EventsBusActor.DeliveryStatus]

  def shutdown(): Unit = Await.result(system.terminate(), 30.seconds)
}
