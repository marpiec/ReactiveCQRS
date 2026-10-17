package io.reactivecqrs.bench

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.bench.Baseline.{Config, Outcomes, Report, parallel, seconds, send}
import io.reactivecqrs.core.cluster.{ClusterSerializer, MessageSize}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.documentstore.{DocumentToInsert, MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, SubscriptionsState}
import io.reactivecqrs.core.rebuild.{Projections, ProjectionsSettings}
import io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Random, Try}

/**
  * Short JSON keys (IMPLEMENTATION-PLAN §7.1): the same orders with descriptive keys and with mpjsons `@jsonName` keys of one
  * character, from the JSON to the store, the command path, a restore, a projection rebuild, a delivery between nodes and
  * document queries. `sbt "bench/run jsonnames-M6 scenarios=jsonNames"`; it runs only when listed. Each timed measure runs
  * `jsonRuns` times, the two forms alternately, and the report gives the median and the range of the runs.
  */
private[bench] object JsonNames {

  private val userId = UserId(1)

  /** One form of the orders: its events, commands and command bus, its documents and their projection. */
  private sealed abstract class Form(val key: String, val label: String) {
    def aggregateType: AggregateType
    def first: Event[Any]
    def line(i: Int): Event[Any]
    def open: AnyRef
    def add(id: AggregateId, i: Int): AnyRef
    def bus(system: OrdersSystem): ActorRef
    def lineCountKey: String
    def customerPath: Seq[String]
    def warehousePath: Seq[String]
    def documents(name: String, mpjsons: MPJsons): Documents
    def startProjection(name: String, mpjsons: MPJsons)(projections: Projections, subscriptions: SubscriptionsState): ActorRef
  }

  private object LongKeys extends Form("long", "long keys") {
    def aggregateType: AggregateType = AggregateType.of[Order]
    def first: Event[Any] = OrderOpened(SpaceId(0)).asInstanceOf[Event[Any]]
    def line(i: Int): Event[Any] = OrderLines.long(i).asInstanceOf[Event[Any]]
    def open: AnyRef = OpenOrder(userId)
    def add(id: AggregateId, i: Int): AnyRef = AddOrderLine(userId, id, OrderLines.long(i))
    def bus(system: OrdersSystem): ActorRef = system.orders
    val lineCountKey = "lineCount"
    val customerPath: Seq[String] = Seq("customerReference")
    val warehousePath: Seq[String] = Seq("warehouseLocationCode")
    def documents(name: String, mpjsons: MPJsons): Documents =
      new TypedDocuments[OrderSummary](name, mpjsons, Seq(customerPath, warehousePath), k => OrderLines.summary(k, 1 + k % 50))
    def startProjection(name: String, mpjsons: MPJsons)(projections: Projections, subscriptions: SubscriptionsState): ActorRef =
      projections.start[OrderSummaries](name, 1, name) { build =>
        new OrderSummaries(subscriptions, new PostgresDocumentStore[OrderSummary](build.documentStore(name), mpjsons,
          new NoopDocumentStoreCache[OrderSummary, AnyRef]), name)
      }
  }

  private object ShortKeys extends Form("short", "short keys") {
    def aggregateType: AggregateType = AggregateType.of[ShortOrder]
    def first: Event[Any] = ShortOrderOpened(SpaceId(0)).asInstanceOf[Event[Any]]
    def line(i: Int): Event[Any] = OrderLines.short(i).asInstanceOf[Event[Any]]
    def open: AnyRef = OpenShortOrder(userId)
    def add(id: AggregateId, i: Int): AnyRef = AddShortOrderLine(userId, id, OrderLines.short(i))
    def bus(system: OrdersSystem): ActorRef = system.shortOrders
    val lineCountKey = "l"
    val customerPath: Seq[String] = Seq("c")
    val warehousePath: Seq[String] = Seq("w")
    def documents(name: String, mpjsons: MPJsons): Documents =
      new TypedDocuments[ShortOrderSummary](name, mpjsons, Seq(customerPath, warehousePath), k => OrderLines.shortSummary(k, 1 + k % 50))
    def startProjection(name: String, mpjsons: MPJsons)(projections: Projections, subscriptions: SubscriptionsState): ActorRef =
      projections.start[ShortOrderSummaries](name, 1, name) { build =>
        new ShortOrderSummaries(subscriptions, new PostgresDocumentStore[ShortOrderSummary](build.documentStore(name), mpjsons,
          new NoopDocumentStoreCache[ShortOrderSummary, AnyRef]), name)
      }
  }

  private val forms = Seq(LongKeys, ShortKeys)

  /** The forms in the order of a run: alternately, so neither always goes first. */
  private def inTurn(run: Int): Seq[Form] = if (run % 2 == 1) forms else forms.reverse

  /** A document store of one form, with text indexes on its customer and warehouse paths. */
  private trait Documents {
    def table: String
    def insert(keys: Range): Unit
    def find(path: Seq[String], value: String): Int
  }

  private class TypedDocuments[T <: AnyRef : TypeTag](name: String, mpjsons: MPJsons, paths: Seq[Seq[String]], make: Int => T) extends Documents {
    private val store = new PostgresDocumentStore[T](name, mpjsons, new NoopDocumentStoreCache[T, AnyRef],
      paths.zipWithIndex.map { case (path, i) => MultipleTextIndex(i + 1, path) })
    val table: String = "projection_" + name
    def insert(keys: Range): Unit = keys.grouped(1000).foreach(group => DB.localTx { implicit session =>
      store.insertDocuments(0, group.map(k => DocumentToInsert(k.toLong, make(k))))
    })
    def find(path: Seq[String], value: String): Int = store.findDocumentByPath(path, value).size
  }

  /** The two forms' command buses, wired as a host wires them, without subscribers or with one managed projection. */
  private class OrdersSystem(name: String, stores: BenchStores,
                             projection: Option[(Projections, SubscriptionsState) => ActorRef] = None) {
    val system: ActorSystem = ActorSystem(name)
    private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(stores.aggregatesUids, stores.commandsUids, stores.sagasUids)))
    private val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val orders: ActorRef = system.actorOf(AggregateCommandBusActor(new OrderContext, uidGenerator, stores.eventStore,
      stores.commandResponseState, eventBus), "orders")
    val shortOrders: ActorRef = system.actorOf(AggregateCommandBusActor(new ShortOrderContext, uidGenerator, stores.eventStore,
      stores.commandResponseState, eventBus), "shortOrders")
    private val subscribers = projection.toList.map { start =>
      val subscriptions = new PostgresSubscriptionsState(stores.typesNamesState)
      subscriptions.initSchema()
      start(new Projections(system, ProjectionsSettings(stores.eventStore, subscriptions, stores.registry, Seq(new OrderContext, new ShortOrderContext))),
        subscriptions)
    }
    eventBus ! EventsBusActor.RegisterSubscribers(subscribers)

    def shutdown(): Unit = Await.result(system.terminate(), 30.seconds)
  }

  /** The values of each timed measure, per form, in the order of the runs. */
  private class Runs {
    private val values = mutable.Map[(String, Form), ListBuffer[Double]]()
    def add(measure: String, form: Form, value: Double): Unit = values.getOrElseUpdate((measure, form), ListBuffer()) += value
    def of(measure: String, form: Form): Seq[Double] = values.get((measure, form)).map(_.toSeq).getOrElse(Nil)
  }

  def run(stores: BenchStores, config: Config, report: Report): Unit = {
    val runs = new Runs
    val sizes = ListBuffer[Seq[String]]()
    val note = "n" * config.noteSize

    // Measure 1: the JSON of one event, the forms' and the control's.
    def json(event: AnyRef): String = stores.mpjsons.serialize(event, event.getClass.getName)
    val lineChars = forms.map(form => form -> meanLength((0 until 1000).map(i => json(form.line(i))))).toMap
    sizes += row("JSON of an order line event, characters (mean of 1 000 lines)", lineChars(LongKeys), lineChars(ShortKeys), whole)
    sizes += row(s"JSON of the control event, a ${config.noteSize}-character note, characters", json(Incremented(note)).length,
      json(IncrementedShortKey(note)).length, whole)

    cpu(stores.mpjsons, config, runs, note)
    println("[bench] jsonNames: serialization done")
    rebuild(stores, config, runs, sizes)
    println("[bench] jsonNames: rebuild done")
    restore(stores, config, runs)
    println("[bench] jsonNames: restore done")
    val outcomes = commands(stores, config, runs, sizes, note, lineChars)
    println("[bench] jsonNames: commands done")
    documents(stores, config, runs, sizes)

    report.section("Short JSON keys: sizes", Seq("Measure", "Long keys", "Short keys", "Change"), sizes.toSeq,
      "The same order line event (eight fields with descriptive names of 8 to 21 characters and short values) and the same " +
        "projection document (six fields) with their Scala names as JSON keys and with one-character keys (mpjsons `@jsonName`); " +
        "the control is the bench counter's event, whose note outweighs its one key. Sizes do not vary between runs except the " +
        "command path's, whose WAL includes the full-page images written after each checkpoint; those give the median and the range " +
        s"of ${config.jsonRuns} runs.")
    report.section("Short JSON keys: time", Seq("Measure", "Long keys", "Short keys", "Change of the median"), Seq(
      timed("serialize one order line event, µs", runs, "serialize line", decimals(3)),
      timed("deserialize one order line event, µs", runs, "deserialize line", decimals(3)),
      timed("serialize the control event, µs", runs, "serialize control", decimals(3)),
      timed("deserialize the control event, µs", runs, "deserialize control", decimals(3)),
      timed(s"command path: ${config.jsonCommands} commands from ${config.workers} callers, commands/s", runs, "commands/s", whole),
      timed("command path latency p50, ms", runs, "latency p50", decimals(2)),
      timed("command path latency p95, ms", runs, "latency p95", decimals(2)),
      timed("command path latency p99, ms", runs, "latency p99", decimals(2)),
      Seq("command path outcomes", outcomes(LongKeys), outcomes(ShortKeys), ""),
      timed(s"restore of an order of ${config.jsonRestoreEvents} events by a new owner, ms", runs, "restore", whole),
      timed("restore: heap peak, MB", runs, "restore heap", whole),
      timed(s"rebuild of an events-based managed projection over ${config.jsonRebuildEvents} events, events/s", runs, "rebuild", whole),
      // OrderLines has 20 000 customers and 360 warehouse locations.
      timed(s"document lookup by an indexed path, ${math.max(1, config.jsonDocuments / 20000)} of ${config.jsonDocuments} documents: p50, ms",
        runs, "lookup p50", decimals(2)),
      timed("document lookup: p95, ms", runs, "lookup p95", decimals(2)),
      timed(s"document list by an indexed path, about ${config.jsonDocuments / 360} documents: p50, ms", runs, "list p50", decimals(2)),
      timed("document list: p95, ms", runs, "list p95", decimals(2))),
      s"Median of ${config.jsonRuns} runs, with the range in brackets; the two forms ran alternately. Serialization: one thread, " +
        s"${config.jsonCalls} calls after a warm-up of ${config.jsonCalls / 5}, with the store's own `MPJsons`. Command path: " +
        s"`ConcurrentCommand`s, each caller on 100 orders of its own, no subscribers. Restore: no snapshots. Rebuild: a projection keeping " +
        "each order's last line and line count, each event reading and writing its order's document, built from the store in commit " +
        "order (M4), from its start until it had every event. Documents: 1 000 lookups and 200 lists per run, the same values for both forms.")
  }

  // ---- measures ----

  /** Measure 2: microseconds per call to serialize and to deserialize one event, on one thread, after a warm-up. */
  private def cpu(mpjsons: MPJsons, config: Config, runs: Runs, note: String): Unit = {
    case class Kind(name: String, form: Form, events: IndexedSeq[AnyRef]) {
      val className: String = events.head.getClass.getName
      val jsons: IndexedSeq[String] = events.map(mpjsons.serialize(_, className))
    }
    val kinds = Seq(Kind("line", LongKeys, (0 until 1000).map(LongKeys.line)), Kind("line", ShortKeys, (0 until 1000).map(ShortKeys.line)),
      Kind("control", LongKeys, IndexedSeq(Incremented(note))), Kind("control", ShortKeys, IndexedSeq(IncrementedShortKey(note))))

    def serializing(kind: Kind, calls: Int): Long = {
      var characters = 0L
      val started = System.nanoTime()
      var i = 0
      while (i < calls) {
        characters += mpjsons.serialize(kind.events(i % kind.events.size), kind.className).length
        i += 1
      }
      val nanos = System.nanoTime() - started
      if (characters == 0) println("[bench] nothing serialized") // uses the result, so the work is not optimized away
      nanos
    }
    def deserializing(kind: Kind, calls: Int): Long = {
      var read = 0L
      val started = System.nanoTime()
      var i = 0
      while (i < calls) {
        if (mpjsons.deserialize[AnyRef](kind.jsons(i % kind.jsons.size), kind.className) ne null) read += 1
        i += 1
      }
      val nanos = System.nanoTime() - started
      if (read == 0) println("[bench] nothing deserialized")
      nanos
    }

    kinds.foreach { kind =>
      serializing(kind, config.jsonCalls / 5)
      deserializing(kind, config.jsonCalls / 5)
    }
    (1 to config.jsonRuns).foreach { run =>
      inTurn(run).foreach(form => kinds.filter(_.form == form).foreach { kind =>
        runs.add("serialize " + kind.name, form, serializing(kind, config.jsonCalls) / 1000.0 / config.jsonCalls)
        runs.add("deserialize " + kind.name, form, deserializing(kind, config.jsonCalls) / 1000.0 / config.jsonCalls)
      })
    }
  }

  /** Measures 1 and 5: each form's history written directly, its size in the store and in the WAL; then the rebuilds. */
  private def rebuild(stores: BenchStores, config: Config, runs: Runs, sizes: ListBuffer[Seq[String]]): Unit = {
    val orders = math.max(1, config.jsonRebuildEvents / 1000)
    val perOrder = config.jsonRebuildEvents / orders
    val firstIds = Map[Form, Long](LongKeys -> 4000000000L, ShortKeys -> 4100000000L)
    case class Written(tablePerEvent: Double, walPer100k: Option[Double], eventsPerSecond: Double, json: Double, column: Double)
    val written = forms.map { form =>
      val (tableBefore, walBefore) = (eventsTableBytes(), walPosition())
      val started = System.nanoTime()
      writeOrders(stores, form, firstIds(form), orders, perOrder)
      val nanos = System.nanoTime() - started
      val events = orders.toLong * perOrder
      val (json, column) = storedJson(firstIds(form), firstIds(form) + orders - 1)
      form -> Written((eventsTableBytes() - tableBefore).toDouble / events, walBefore.flatMap(walSince).map(_ * 100000.0 / events),
        events / seconds(nanos), json, column)
    }.toMap
    sizes += row("stored event JSON (`events.event`), bytes per event", written(LongKeys).json, written(ShortKeys).json, decimals(1))
    sizes += row("the same column as PostgreSQL keeps it (`pg_column_size`), bytes", written(LongKeys).column, written(ShortKeys).column, decimals(1))
    sizes += row(s"`events` with its indexes, growth per event over ${orders.toLong * perOrder} events written directly",
      written(LongKeys).tablePerEvent, written(ShortKeys).tablePerEvent, decimals(1))
    sizes += optionalRow("WAL per 100 000 events written directly (events, keys, outbox), MB", written(LongKeys).walPer100k,
      written(ShortKeys).walPer100k, mb)
    sizes += row("writing them directly through the event store, events/s (one run, batches of 1 000)", written(LongKeys).eventsPerSecond,
      written(ShortKeys).eventsPerSecond, whole)

    (1 to config.jsonRuns).foreach { run =>
      inTurn(run).foreach { form =>
        val name = s"jn_${form.key}_r$run"
        val table = "projection_" + name + "_v1" // the first build's tables have the suffix `_v1` (M4)
        val started = System.nanoTime()
        val system = new OrdersSystem("jsonnames-rebuild-" + form.key + "-" + run, stores, Some(form.startProjection(name, stores.mpjsons)))
        val done = waitFor(60.minutes)(linesIn(table, form.lineCountKey) >= orders.toLong * (perOrder - 1))
        val nanos = System.nanoTime() - started
        system.shutdown()
        if (done) runs.add("rebuild", form, orders.toLong * perOrder / seconds(nanos))
        else println(s"[bench] jsonNames: the rebuild of $name did not end within 60 minutes")
      }
    }
  }

  /** Measure 4: an order's whole history read by a new owner, each run. */
  private def restore(stores: BenchStores, config: Config, runs: Runs): Unit = {
    val ids = Map[Form, Long](LongKeys -> 4200000000L, ShortKeys -> 4300000000L)
    forms.foreach(form => writeOrders(stores, form, ids(form), 1, config.jsonRestoreEvents))
    val system = new OrdersSystem("jsonnames-restore", stores)
    (1 to config.jsonRuns).foreach { run =>
      inTurn(run).foreach { form =>
        val id = AggregateId(ids(form))
        Heap.usedAfterGc()
        Heap.resetPeak()
        val started = System.nanoTime()
        val restored = Try(Await.result((form.bus(system) ? GetAggregate(id))(Timeout(10.minutes)), 11.minutes).asInstanceOf[Try[Aggregate[_]]].get)
        val nanos = System.nanoTime() - started
        val peak = Heap.peak()
        if (restored.toOption.exists(_.version.asInt == config.jsonRestoreEvents)) {
          runs.add("restore", form, nanos / 1e6)
          runs.add("restore heap", form, peak / 1024.0 / 1024.0)
        } else println(s"[bench] jsonNames: the restore of ${form.label} failed: $restored")
        form.bus(system) ! PassivateAggregate(id) // the next run's owner reads the history again
        Thread.sleep(1000)
      }
    }
    system.shutdown()
  }

  /** Measures 1, 3 and 6: commands from many callers, the events table's growth and the WAL they make; then one delivery's size. */
  private def commands(stores: BenchStores, config: Config, runs: Runs, sizes: ListBuffer[Seq[String]], note: String,
                       lineChars: Map[Form, Double]): Map[Form, String] = {
    val system = new OrdersSystem("jsonnames-commands", stores)
    val perCaller = config.jsonCommands / config.workers
    val summaries = mutable.Map[Form, ListBuffer[String]]()

    def opened(form: Form): AggregateId = send(form.bus(system), form.open) match {
      case SuccessResponse(id, _) => id
      case other => throw new IllegalStateException("Could not open an order: " + other)
    }
    def load(form: Form, ordersPerCaller: Int, commandsPerCaller: Int): (Long, Latencies, Outcomes) = {
      val orders = parallel(config.workers)(_ => (1 to ordersPerCaller).map(_ => opened(form)))
      val latencies = new Latencies
      val outcomes = new Outcomes
      val started = System.nanoTime()
      parallel(config.workers)(w => (0 until commandsPerCaller).foreach { c =>
        val command = form.add(orders(w)(c % ordersPerCaller), w * commandsPerCaller + c)
        val before = System.nanoTime()
        outcomes.add(send(form.bus(system), command))
        latencies.record(System.nanoTime() - before)
      })
      (System.nanoTime() - started, latencies, outcomes)
    }

    forms.foreach(form => load(form, 10, 2000 / config.workers)) // warm-up, not measured
    (1 to config.jsonRuns).foreach { run =>
      inTurn(run).foreach { form =>
        val (tableBefore, walBefore) = (eventsTableBytes(), walPosition())
        val (nanos, latencies, outcomes) = load(form, 100, perCaller)
        val commands = perCaller.toLong * config.workers
        runs.add("commands/s", form, commands / seconds(nanos))
        Seq(50, 95, 99).foreach(p => runs.add("latency p" + p, form, latencies.percentileMillis(p)))
        runs.add("table per event", form, (eventsTableBytes() - tableBefore).toDouble / commands)
        walBefore.flatMap(walSince).foreach(bytes => runs.add("wal per 100k", form, bytes * 100000.0 / commands))
        summaries.getOrElseUpdate(form, ListBuffer()) += outcomes.summary
      }
    }
    sizes += timed("`events` with its indexes, growth per event on the command path", runs, "table per event", decimals(1))
    sizes += timed("WAL per 100 000 commands, each writing one event (with its key, the outbox row and its deletion), MB", runs, "wal per 100k", mb)

    // Measure 6: one re-publication part of 1 000 events, as the cluster serializer writes it for another node (F121).
    val serializer = new ClusterSerializer(system.system.asInstanceOf[ExtendedActorSystem])
    val budget = MessageSize(system.system).partBudget
    def delivery(aggregateType: AggregateType, event: Int => Event[Any]): Double =
      serializer.sizeOf(IdentifiableEvents[Any](aggregateType, AggregateId(1), (1 to 1000).map(v => EventInfo[Any](AggregateVersion(v), event(v), userId, Instant.now))))
    val (longDelivery, shortDelivery) = (delivery(LongKeys.aggregateType, LongKeys.line), delivery(ShortKeys.aggregateType, ShortKeys.line))
    sizes += row("1 000 order line events in one delivery, serialized for another node, KiB", longDelivery, shortDelivery, kib)
    // What each event takes there besides its JSON: class and field names, version, user, time (D68).
    sizes += row("of which each event's envelope besides its JSON, bytes", longDelivery / 1000 - lineChars(LongKeys),
      shortDelivery / 1000 - lineChars(ShortKeys), whole)
    sizes += row(s"order line events that fit one delivery part (half the ${MessageSize(system.system).limit / 1024} KiB frame)",
      budget / (longDelivery / 1000), budget / (shortDelivery / 1000), whole)
    val counterType = AggregateType.of[Counter]
    sizes += row("1 000 control events in one delivery, KiB", delivery(counterType, _ => Incremented(note).asInstanceOf[Event[Any]]),
      delivery(counterType, _ => IncrementedShortKey(note).asInstanceOf[Event[Any]]), kib)
    system.shutdown()
    summaries.map { case (form, all) => form -> all.distinct.mkString("; ") }.toMap
  }

  /** Measures 1 and 7: the projection documents' size, and queries on an indexed JSON path. */
  private def documents(stores: BenchStores, config: Config, runs: Runs, sizes: ListBuffer[Seq[String]]): Unit = {
    val stored = forms.map(form => form -> form.documents("jn_docs_" + form.key, stores.mpjsons)).toMap
    forms.foreach { form =>
      stored(form).insert(0 until config.jsonDocuments)
      DB.autoCommit(implicit session => SQL("ANALYZE " + stored(form).table).execute())
    }
    val measured = forms.map(form => form -> documentSizes(stored(form).table)).toMap
    sizes += row("projection document (JSONB, `pg_column_size`), bytes", measured(LongKeys)._1, measured(ShortKeys)._1, decimals(1))
    sizes += row("projection table with its two path indexes, bytes per document", measured(LongKeys)._2, measured(ShortKeys)._2, decimals(1))

    (1 to config.jsonRuns).foreach { run =>
      inTurn(run).foreach { form =>
        val random = new Random(run) // the same values for both forms
        val (lookups, lists) = (new Latencies, new Latencies)
        (1 to 1000).foreach { _ =>
          val customer = OrderLines.long(random.nextInt(config.jsonDocuments)).customerReference
          val started = System.nanoTime()
          stored(form).find(form.customerPath, customer)
          lookups.record(System.nanoTime() - started)
        }
        (1 to 200).foreach { _ =>
          val warehouse = OrderLines.long(random.nextInt(config.jsonDocuments)).warehouseLocationCode
          val started = System.nanoTime()
          stored(form).find(form.warehousePath, warehouse)
          lists.record(System.nanoTime() - started)
        }
        runs.add("lookup p50", form, lookups.percentileMillis(50))
        runs.add("lookup p95", form, lookups.percentileMillis(95))
        runs.add("list p50", form, lists.percentileMillis(50))
        runs.add("list p95", form, lists.percentileMillis(95))
      }
    }
  }

  // ---- helpers ----

  /** `orders` orders of `perOrder` events each, from `firstId` on, written in transactions of up to 1 000 events. */
  private def writeOrders(stores: BenchStores, form: Form, firstId: Long, orders: Int, perOrder: Int): Unit = {
    var line = 0
    (0 until orders).foreach { k =>
      val id = AggregateId(firstId + k)
      var version = 0
      while (version < perOrder) {
        val batch = (version until math.min(perOrder, version + 1000)).map { v =>
          if (v == 0) form.first else {
            line += 1
            form.line(line)
          }
        }
        stores.eventStore.localTx { session =>
          stores.eventStore.persistEvents(id, NewEvents[Any](userId, AggregateVersion(version), Instant.now, batch))(session).get
          sql"DELETE FROM events_to_publish WHERE aggregate_id = ${id.asLong}".update()(session)
        }
        version += batch.size
      }
    }
  }

  private def eventsTableBytes(): Long = DB.readOnly { implicit session => sql"SELECT pg_total_relation_size('events')".map(_.long(1)).single().get }

  /** The current WAL position, when this database user may read it. */
  private def walPosition(): Option[String] =
    Try(DB.readOnly { implicit session => sql"SELECT pg_current_wal_lsn()::text".map(_.string(1)).single().get }).toOption

  private def walSince(position: String): Option[Long] = Try(DB.readOnly { implicit session =>
    sql"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), ${position}::pg_lsn)::bigint".map(_.long(1)).single().get
  }).toOption

  /** The mean JSON bytes of the events of the aggregates `from` to `to`, as text and as stored. */
  private def storedJson(from: Long, to: Long): (Double, Double) = DB.readOnly { implicit session =>
    sql"SELECT avg(octet_length(event)), avg(pg_column_size(event)) FROM events WHERE aggregate_id BETWEEN $from AND $to"
      .map(rs => (rs.double(1), rs.double(2))).single().get
  }

  private def documentSizes(table: String): (Double, Double) = DB.readOnly { implicit session =>
    SQL(s"SELECT avg(pg_column_size(document)), pg_total_relation_size('$table')::float8 / count(*) FROM $table")
      .map(rs => (rs.double(1), rs.double(2))).single().get
  }

  /** The lines a rebuilding projection has applied so far; none before its table exists. */
  private def linesIn(table: String, key: String): Long = DB.readOnly { implicit session =>
    if (!SQL(s"SELECT to_regclass('$table') IS NOT NULL").map(_.boolean(1)).single().get) 0L
    else SQL(s"SELECT coalesce(sum((document->>'$key')::bigint), 0) FROM $table").map(_.long(1)).single().get
  }

  /** Polls four times a second, so the polling costs the projection being measured little. */
  private def waitFor(timeout: FiniteDuration)(condition: => Boolean): Boolean = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) return false
      Thread.sleep(250)
    }
    true
  }

  private def meanLength(jsons: Seq[String]): Double = jsons.map(_.length).sum.toDouble / jsons.size

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    if (sorted.size % 2 == 1) sorted(sorted.size / 2) else (sorted(sorted.size / 2 - 1) + sorted(sorted.size / 2)) / 2
  }

  private def change(long: Double, short: Double): String = if (long == 0) "-" else f"${(short / long - 1) * 100}%+.0f %%"

  private def row(label: String, long: Double, short: Double, format: Double => String): Seq[String] =
    Seq(label, format(long), format(short), change(long, short))

  private def optionalRow(label: String, long: Option[Double], short: Option[Double], format: Double => String): Seq[String] =
    (long, short) match {
      case (Some(l), Some(s)) => row(label, l, s, format)
      case _ => Seq(label, "not readable", "not readable", "-")
    }

  /** A timed measure's median with the range of its runs, per form. */
  private def timed(label: String, runs: Runs, measure: String, format: Double => String): Seq[String] = {
    def spread(values: Seq[Double]): String =
      if (values.isEmpty) "not measured"
      else if (values.size == 1) format(values.head)
      else s"${format(median(values))} (${format(values.min)}–${format(values.max)})"
    val (long, short) = (runs.of(measure, LongKeys), runs.of(measure, ShortKeys))
    Seq(label, spread(long), spread(short), if (long.isEmpty || short.isEmpty) "-" else change(median(long), median(short)))
  }

  private val whole: Double => String = value => f"$value%.0f"
  private def decimals(n: Int): Double => String = value => s"%.${n}f".format(value)
  private val mb: Double => String = bytes => f"${bytes / 1024 / 1024}%.1f"
  private val kib: Double => String = bytes => f"${bytes / 1024}%.1f"
}
