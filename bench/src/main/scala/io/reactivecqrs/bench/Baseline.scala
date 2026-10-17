package io.reactivecqrs.bench

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.{Instant, LocalDate}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, Executors}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.eventstore.NewEvents
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The performance baseline of docs/IMPLEMENTATION-PLAN.md §7, run end to end against PostgreSQL:
  * `sbt "bench/run baseline-M1"` writes docs/perf/baseline-M1.md. Sizes can follow the name as `key=value` arguments
  * named like the fields of `Config`, for example `sbt "bench/run smoke workers=2 restoreSizes=100,1000 soakSeconds=5"`.
  */
object Baseline {

  case class Config(workers: Int = 16,
                    commandsPerWorker: Int = 200,
                    contendedCommandsPerWorker: Int = 50,
                    noteSize: Int = 200,
                    rebuildAggregates: Int = 1000,
                    rebuildEventsPerAggregate: Int = 100,
                    restoreSizes: Seq[Int] = Seq(1000, 10000, 100000, 500000),
                    soakSeconds: Int = 60,
                    soakAggregates: Int = 20000,
                    soakEventsPerAggregate: Int = 10,
                    soakFailingSeconds: Int = 30,
                    restartCycles: Int = 5,
                    scenarios: Set[String] = Set.empty)

  private def configFrom(arguments: Seq[String]): Config = {
    val named = arguments.map(_.split("=", 2)).collect { case Array(key, value) => key -> value }.toMap
    def int(key: String, default: Int): Int = named.get(key).map(_.toInt).getOrElse(default)
    val defaults = Config()
    Config(int("workers", defaults.workers), int("commandsPerWorker", defaults.commandsPerWorker),
      int("contendedCommandsPerWorker", defaults.contendedCommandsPerWorker), int("noteSize", defaults.noteSize),
      int("rebuildAggregates", defaults.rebuildAggregates), int("rebuildEventsPerAggregate", defaults.rebuildEventsPerAggregate),
      named.get("restoreSizes").map(_.split(",").toSeq.map(_.trim.toInt)).getOrElse(defaults.restoreSizes),
      int("soakSeconds", defaults.soakSeconds), int("soakAggregates", defaults.soakAggregates),
      int("soakEventsPerAggregate", defaults.soakEventsPerAggregate), int("soakFailingSeconds", defaults.soakFailingSeconds),
      int("restartCycles", defaults.restartCycles),
      named.get("scenarios").map(_.split(",").map(_.trim).toSet).getOrElse(defaults.scenarios))
  }

  private val userId = UserId(1)
  private val askTimeout: Timeout = Timeout(120.seconds)

  def main(args: Array[String]): Unit = {
    java.util.Locale.setDefault(java.util.Locale.ROOT) // decimal points in the report, whatever the machine's locale
    val reportName = args.headOption.getOrElse("run-" + LocalDate.now)
    val config = configFrom(args.toSeq.drop(1))
    val report = new Report(reportName, config)
    BenchDatabase.init()
    StatementCounter.install()
    try {
      val stores = new BenchStores
      report.environment(DatabaseCounters.serverVersion())

      // `scenarios=commands,contended,serial,rebuild,restore,soak,deliverySoak` runs only those; all by default.
      def runs(scenario: String): Boolean = config.scenarios.isEmpty || config.scenarios.contains(scenario)
      if (runs("commands")) {
        val withProjections = startup(stores, report)
        commandsOnManyAggregates("Commands on many aggregates, two projections", withProjections, config, report, projections = true)
        shutdownDelivered(withProjections)
      }
      if (runs("commands") || runs("contended")) {
        val plain = new BenchSystem("plain", stores, projections = false)
        if (runs("commands")) commandsOnManyAggregates("Commands on many aggregates, no subscribers", plain, config, report, projections = false)
        if (runs("contended")) commandsOnOneAggregate("Concurrent commands on one aggregate", plain, config, report,
          "Conflicting commands are re-run with jittered back-off, at most 10 times (D37).")
        plain.shutdown()
      }
      if (runs("serial")) {
        val serial = new BenchSystem("serial", stores, projections = false, context = new SerialCounterContext)
        commandsOnManyAggregates("Commands on many aggregates, no subscribers, serial mode (R8)", serial, config, report, projections = false)
        commandsOnOneAggregate("Concurrent commands on one aggregate, serial mode (R8)", serial, config, report,
          "Each command waits for the one before it on the aggregate; none conflicts, none is re-run.")
        serial.shutdown()
      }
      if (runs("rebuild")) rebuild(stores, config, report)
      if (runs("restore")) restore(stores, config, report)
      if (runs("soak")) soak(stores, config, report)
      if (runs("deliverySoak")) deliverySoak(stores, config, report)
    } finally {
      BenchDatabase.drop()
    }
    val file = report.write()
    println(report.text)
    println("Report written to " + file)
    sys.exit(0)
  }

  // ---- scenarios ----

  private def startup(stores: BenchStores, report: Report): BenchSystem = {
    val started = System.nanoTime()
    val system = new BenchSystem("startup", stores, projections = true)
    send(system.counters, CreateCounter(userId))
    val firstReply = System.nanoTime() - started
    val projected = awaitUntil(120.seconds)(rows(system.progressTable) >= 1)
    report.section("Startup to ready", Seq("Measure", "Value"), Seq(
      Seq("first command answered", millis(firstReply)),
      Seq("first event seen by a projection", projected.map(_ => millis(System.nanoTime() - started)).getOrElse("not within 120 s"))),
      "A new system with two projections; the event bus waits for its subscribers before it publishes.")
    system
  }

  private def commandsOnManyAggregates(title: String, system: BenchSystem, config: Config, report: Report, projections: Boolean): Unit = {
    val note = "n" * config.noteSize
    val counters = parallel(config.workers)(_ => created(system))
    // Warm-up, not measured: classes, connections, actors.
    parallel(config.workers)(w => (1 to 20).foreach(_ => send(system.counters, Increment(userId, counters(w), note))))
    awaitProjection(system, projections, counters.size * 20L)

    DatabaseCounters.settle()
    val statementsBefore = StatementCounter.current
    val commitsBefore = DatabaseCounters.committedTransactions()
    val latencies = new Latencies
    val outcomes = new Outcomes
    val started = System.nanoTime()
    parallel(config.workers)(w => (1 to config.commandsPerWorker).foreach { _ =>
      val before = System.nanoTime()
      outcomes.add(send(system.counters, Increment(userId, counters(w), note)))
      latencies.record(System.nanoTime() - before)
    })
    val loadNanos = System.nanoTime() - started
    val commands = config.workers.toLong * config.commandsPerWorker
    val caughtUp = awaitProjection(system, projections, counters.size * (20L + config.commandsPerWorker))
    val projectionNanos = System.nanoTime() - started
    DatabaseCounters.settle()
    val statements = StatementCounter.current - statementsBefore
    val commits = DatabaseCounters.committedTransactions() - commitsBefore

    val rowsOut = ListBuffer(
      Seq("commands", s"$commands (${config.workers} callers, each on its own aggregate, one command at a time)"),
      Seq("outcomes", outcomes.summary),
      Seq("latency p50 / p95 / p99 / max", latencySummary(latencies)),
      Seq("throughput", f"${commands / seconds(loadNanos)}%.0f commands/s = events/s"),
      Seq("SQL statements per command", f"${statements.toDouble / commands}%.1f" + (if (projections) " (including the two projections)" else "")),
      Seq("committed transactions per command", f"${commits.toDouble / commands}%.1f (pg_stat_database; autocommit reads count too)"))
    if (projections) {
      rowsOut += Seq("projection throughput", caughtUp.map(_ => f"${commands / seconds(projectionNanos)}%.0f events/s (events-based projection, from load start until it caught up)").getOrElse("did not catch up within 120 s"))
      rowsOut += Seq("projection lag after the last command", caughtUp.map(_ => millis(projectionNanos - loadNanos)).getOrElse("-"))
    }
    report.section(title, Seq("Measure", "Value"), rowsOut.toSeq, s"Each command adds one event with a ${config.noteSize}-character note.")
  }

  private def commandsOnOneAggregate(title: String, system: BenchSystem, config: Config, report: Report, about: String): Unit = {
    val note = "n" * config.noteSize
    val counter = created(system)
    val latencies = new Latencies
    val outcomes = new Outcomes
    val started = System.nanoTime()
    parallel(config.workers)(_ => (1 to config.contendedCommandsPerWorker).foreach { _ =>
      val before = System.nanoTime()
      outcomes.add(send(system.counters, Increment(userId, counter, note)))
      latencies.record(System.nanoTime() - before)
    })
    val loadNanos = System.nanoTime() - started
    val commands = config.workers.toLong * config.contendedCommandsPerWorker
    val stored = DB.readOnly { implicit session => sql"SELECT count(*) - 1 FROM events WHERE aggregate_id = ${counter.asLong}".map(_.long(1)).single().get }
    report.section(title, Seq("Measure", "Value"), Seq(
      Seq("commands", s"$commands (${config.workers} callers at once, all on one aggregate, `ConcurrentCommand`)"),
      Seq("outcomes", outcomes.summary),
      Seq("events stored", stored.toString),
      Seq("latency p50 / p95 / p99 / max", latencySummary(latencies)),
      Seq("throughput", f"${commands / seconds(loadNanos)}%.0f commands/s")),
      about)
  }

  private def rebuild(stores: BenchStores, config: Config, report: Report): Unit = {
    val note = "n" * config.noteSize
    val written = (1 to config.rebuildAggregates).map(i => AggregateId(2000000000L + i))
    val writeStarted = System.nanoTime()
    written.foreach(id => writeHistory(stores, id, config.rebuildEventsPerAggregate, note))
    val writeNanos = System.nanoTime() - writeStarted
    val expectedIncrements = DB.readOnly { implicit session =>
      sql"SELECT (SELECT count(*) FROM events) - (SELECT count(*) FROM aggregates WHERE base_order = 1)".map(_.long(1)).single().get
    }
    val allEvents = DB.readOnly { implicit session => sql"SELECT count(*) FROM events".map(_.long(1)).single().get }

    System.gc()
    val heapBefore = Heap.usedAfterGc()
    Heap.resetPeak()
    val started = System.nanoTime()
    val system = new BenchSystem("rebuild", stores, projections = true, managed = true)
    val incrementsNanos = awaitUntil(20.minutes)(system.projectedIncrements() >= expectedIncrements).map(_ => System.nanoTime() - started)
    val builtNanos = awaitUntil(20.minutes)(system.built()).map(_ => System.nanoTime() - started)
    val peak = Heap.peak()
    system.shutdown()

    report.section("Projection rebuild (the M4 builder)", Seq("Measure", "Value"), Seq(
      Seq("events in the store", s"$allEvents (${config.rebuildAggregates} aggregates of ${config.rebuildEventsPerAggregate} events written for this, plus the earlier scenarios' events)"),
      Seq("until the events-based projection had every event", incrementsNanos.map(millis).getOrElse("not within 20 minutes")),
      Seq("until both projections were built and switched", builtNanos.map(millis).getOrElse("not within 20 minutes")),
      Seq("rebuild throughput (events-based projection)", incrementsNanos.map(nanos => f"${allEvents / seconds(nanos)}%.0f events/s").getOrElse("-")),
      Seq("heap before / peak during", s"${Heap.mb(heapBefore)} / ${Heap.mb(peak)}"),
      Seq("writing the history directly through the event store", f"${config.rebuildAggregates.toLong * config.rebuildEventsPerAggregate / seconds(writeNanos)}%.0f events/s (batches of up to 1000 events per transaction)")),
      "Two new managed projections of the counters, built from the store when they start (D57–D62): the events-based one " +
        "in commit order, in chunks of 1 000 events applied in one transaction each; the state-only one from each counter's " +
        "final state (D62). Times count from the start of the system.")
  }

  private def restore(stores: BenchStores, config: Config, report: Report): Unit = {
    val note = "n" * config.noteSize
    val system = new BenchSystem("restore", stores, projections = false, context = new SnapshottingCounterContext)
    val rowsOut = config.restoreSizes.zipWithIndex.map { case (size, i) =>
      val id = AggregateId(3000000000L + i)
      writeHistory(stores, id, size, note)
      val heapBefore = Heap.usedAfterGc()
      Heap.resetPeak()
      val statementsBefore = StatementCounter.current
      val started = System.nanoTime()
      val restored = Try(Await.result((system.counters ? GetAggregate(id))(Timeout(10.minutes)), 11.minutes).asInstanceOf[Try[Aggregate[Counter]]].get)
      val nanos = System.nanoTime() - started
      val statements = StatementCounter.current - statementsBefore
      val peak = Heap.peak()
      val outcome = restored.map(a => if (a.version.asInt == size) "ok" else "version " + a.version.asInt).getOrElse("failed: " + restored.failed.get.getMessage)
      // D30: the owner stops, writing a snapshot of the state it restored; the next owner reads it, not the history.
      system.counters ! PassivateAggregate(id)
      awaitUntil(1.minute)(DB.readOnly { implicit session => sql"SELECT count(*) FROM snapshots WHERE aggregate_id = ${id.asLong}".map(_.int(1)).single().get } == 1)
      val fromSnapshotStarted = System.nanoTime()
      val again = Try(Await.result((system.counters ? GetAggregate(id))(Timeout(1.minute)), 2.minutes).asInstanceOf[Try[Aggregate[Counter]]].get)
      val fromSnapshot = if (again.toOption.exists(_.version.asInt == size)) millis(System.nanoTime() - fromSnapshotStarted) else "failed"
      Seq(size.toString, millis(nanos), f"${size / seconds(nanos)}%.0f", s"${Heap.mb(heapBefore)} / ${Heap.mb(peak)}", statements.toString, outcome, fromSnapshot)
    }
    system.shutdown()
    report.section("Aggregate restore", Seq("Events", "Restore time", "Events/s", "Heap before / peak", "SQL statements", "Result", "From a snapshot (D30)"), rowsOut,
      s"A new owner reads one aggregate's whole history; then it stops, writing a snapshot, and a new owner restores from that. Events carry a ${config.noteSize}-character note.")
  }

  private def soak(stores: BenchStores, config: Config, report: Report): Unit = {
    val note = "n" * config.noteSize
    val system = new BenchSystem("soak", stores, projections = true)
    val counters = parallel(config.workers)(_ => created(system))
    awaitProjection(system, projections = true, 0)
    val heapStart = Heap.usedAfterGc()
    val sent = new AtomicLong()
    val deadline = System.nanoTime() + config.soakSeconds.seconds.toNanos
    parallel(config.workers)(w => while (System.nanoTime() < deadline) {
      send(system.counters, Increment(userId, counters(w), note))
      sent.incrementAndGet()
    })
    val loadEnded = System.nanoTime()
    val heapAfterLoad = Heap.usedAfterGc()
    // Commands can come faster than the projections apply them (F93): the idle heap is measured once they caught up.
    val caughtUp = awaitUntil(20.minutes)(system.projectedIncrements() >= sent.get)
    val catchUpNanos = System.nanoTime() - loadEnded
    Thread.sleep(20000)
    val heapIdle = Heap.usedAfterGc()
    shutdownDelivered(system)
    report.section("Memory over a soak run", Seq("Measure", "Value"), Seq(
      Seq("load", s"${sent.get} commands in ${config.soakSeconds} s from ${config.workers} callers, two projections"),
      Seq("projections caught up", caughtUp.map(_ => millis(catchUpNanos) + " after the load").getOrElse("not within 20 minutes")),
      Seq("heap after GC: before / right after the load / caught up and 20 s idle", s"${Heap.mb(heapStart)} / ${Heap.mb(heapAfterLoad)} / ${Heap.mb(heapIdle)}")),
      "Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right " +
        "after the load holds the projections' backlog (F93).")
  }

  // M2 verification (docs/IMPLEMENTATION-PLAN.md): many aggregates, a tenth of whose events-based listener fails for a
  // while and recovers within its retries; afterwards every event is projected, the bus waits for nothing, the outbox is
  // empty, and the heap stays flat, also over restarts of the system (R4). Subscriptions are kept in the database.
  private def deliverySoak(stores: BenchStores, config: Config, report: Report): Unit = {
    val note = "n" * config.noteSize
    val failUntil = new AtomicLong(0L)
    val failing = (id: AggregateId) => id.asLong % 10 == 0 && System.nanoTime() < failUntil.get
    val system = new BenchSystem("deliverysoak", stores, projections = true, failing = failing)
    val counters = parallel(config.workers)(_ => (1 to config.soakAggregates / config.workers).map(_ => created(system)))
    val aggregates = counters.map(_.size).sum
    awaitUntil(5.minutes)(rows(system.progressTable) >= aggregates)
    val heapStart = Heap.usedAfterGc()

    failUntil.set(System.nanoTime() + config.soakFailingSeconds.seconds.toNanos)
    val started = System.nanoTime()
    parallel(config.workers)(w => (1 to config.soakEventsPerAggregate).foreach(_ => counters(w).foreach(id => send(system.counters, Increment(userId, id, note)))))
    val loadNanos = System.nanoTime() - started
    val events = aggregates.toLong * config.soakEventsPerAggregate
    val caughtUp = awaitUntil(20.minutes)(system.projectedIncrements() >= events)
    val catchUpNanos = System.nanoTime() - started
    awaitUntil(60.seconds)(system.deliveryStatus().waiting == 0 && rows("events_to_publish") == 0)
    val status = system.deliveryStatus()
    val outbox = rows("events_to_publish")
    val failures = rows("subscription_failures")
    val heapAfterLoad = Heap.usedAfterGc()
    Thread.sleep(20000)
    val heapIdle = Heap.usedAfterGc()
    system.shutdown()

    val heapAfterRestarts = (1 to config.restartCycles).map { cycle =>
      val again = new BenchSystem("deliverysoak", stores, projections = true)
      parallel(config.workers)(w => (1 to 10).foreach(_ => send(again.counters, Increment(userId, counters(w).head, note))))
      awaitUntil(60.seconds)(again.projectedIncrements() >= events + cycle * config.workers * 10L)
      again.shutdown()
      Heap.mb(Heap.usedAfterGc())
    }

    report.section("Delivery soak (M2)", Seq("Measure", "Value"), Seq(
      Seq("load", s"$aggregates counters x ${config.soakEventsPerAggregate} increments = $events events from ${config.workers} callers; " +
        s"the events-based listener fails on every tenth counter for the first ${config.soakFailingSeconds} s"),
      Seq("command throughput", f"${events / seconds(loadNanos)}%.0f commands/s"),
      Seq("every event projected", caughtUp.map(_ => millis(catchUpNanos) + " after the load started").getOrElse("not within 20 minutes")),
      Seq("bus deliveries waiting / undelivered afterwards", s"${status.waiting} / ${status.undelivered.size}"),
      Seq("outbox rows afterwards", outbox.toString),
      Seq("recorded subscription failures", failures.toString + " (the failures lasted less than the retries)"),
      Seq("heap after GC: before / after the load / after 20 s idle", s"${Heap.mb(heapStart)} / ${Heap.mb(heapAfterLoad)} / ${Heap.mb(heapIdle)}"),
      Seq(s"heap after GC after each of ${config.restartCycles} restarts", heapAfterRestarts.mkString(", "))),
      "Subscriptions in the database, as in every scenario since M4 (F3), with F29's bounded cache of 10 000 aggregates.")
  }

  // ---- helpers ----

  /** Stops a system once every subscriber has taken its events, so a later scenario's outbox poller finds nothing of it
    * to re-publish. */
  private def shutdownDelivered(system: BenchSystem): Unit = {
    awaitUntil(20.minutes)(rows("events_to_publish") == 0)
    system.shutdown()
  }

  /** Writes an aggregate's history straight to the event store, as already published: each batch removes its outbox rows in
    * its own transaction, so no outbox scan starts an actor for the aggregate while it is being written. */
  private def writeHistory(stores: BenchStores, id: AggregateId, events: Int, note: String): Unit = {
    var version = 0
    while (version < events) {
      val batch: Seq[Event[Counter]] = (version until math.min(events, version + 1000)).map(v => if (v == 0) CounterCreated(io.reactivecqrs.api.id.SpaceId(0)) else Incremented(note))
      val expected = AggregateVersion(version)
      stores.eventStore.localTx { session =>
        stores.eventStore.persistEvents(id, NewEvents[Counter](userId, expected, Instant.now, batch))(session).get
        sql"DELETE FROM events_to_publish WHERE aggregate_id = ${id.asLong}".update()(session)
      }
      version += batch.size
    }
  }

  private def created(system: BenchSystem): AggregateId = send(system.counters, CreateCounter(userId)) match {
    case SuccessResponse(id, _) => id
    case other => throw new IllegalStateException("Could not create a counter: " + other)
  }

  private def send(bus: ActorRef, command: AnyRef): CustomCommandResponse[_] =
    Await.result((bus ? command)(askTimeout), 130.seconds).asInstanceOf[CustomCommandResponse[_]]

  private def awaitProjection(system: BenchSystem, projections: Boolean, increments: Long): Option[Long] =
    if (projections) awaitUntil(120.seconds)(system.projectedIncrements() >= increments) else Some(0L)

  private def rows(table: String): Long = Try(DB.readOnly { implicit session => SQL(s"SELECT count(*) FROM $table").map(_.long(1)).single().get }).getOrElse(0L)

  /** Runs one task per worker on its own thread and returns their results in worker order. */
  private def parallel[A](workers: Int)(task: Int => A): IndexedSeq[A] = {
    val pool = Executors.newFixedThreadPool(workers)
    try {
      (0 until workers).map(w => pool.submit(new java.util.concurrent.Callable[A] { def call(): A = task(w) })).map(_.get())
    } finally {
      pool.shutdown()
    }
  }

  private def awaitUntil(timeout: FiniteDuration)(condition: => Boolean): Option[Long] = {
    val started = System.nanoTime()
    val deadline = started + timeout.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) return None
      Thread.sleep(20)
    }
    Some(System.nanoTime() - started)
  }

  private def seconds(nanos: Long): Double = nanos / 1e9
  private def millis(nanos: Long): String = f"${nanos / 1e6}%.0f ms"
  private def latencySummary(l: Latencies): String =
    f"${l.percentileMillis(50)}%.1f / ${l.percentileMillis(95)}%.1f / ${l.percentileMillis(99)}%.1f / ${l.percentileMillis(100)}%.1f ms"

  private class Outcomes {
    private val counts = new ConcurrentHashMap[String, AtomicLong]()
    def add(response: CustomCommandResponse[_]): Unit = counts.computeIfAbsent(response.getClass.getSimpleName, _ => new AtomicLong()).incrementAndGet()
    def summary: String = counts.asScala.toSeq.sortBy(_._1).map { case (k, v) => s"$k ${v.get}" }.mkString(", ")
  }

  // ---- report ----

  private class Report(name: String, config: Config) {
    private val parts = ListBuffer[String]()

    def environment(postgresVersion: String): Unit = {
      val runtime = Runtime.getRuntime
      parts += s"# Performance: $name\n\nMeasured on ${LocalDate.now} with `sbt \"bench/run $name\"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). " +
        "All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.\n"
      parts += table(Seq("Environment", "Value"), Seq(
        Seq("commit", sys.props.getOrElse("bench.commit", "unknown")),
        Seq("OS", System.getProperty("os.name") + " " + System.getProperty("os.version") + ", " + System.getProperty("os.arch")),
        Seq("CPU threads", runtime.availableProcessors.toString),
        Seq("JVM", System.getProperty("java.vm.name") + " " + System.getProperty("java.version") + f", max heap ${runtime.maxMemory / 1024.0 / 1024.0}%.0f MB"),
        Seq("PostgreSQL", postgresVersion + ", same machine (" + BenchDatabase.url + ")"),
        Seq("sizes", s"${config.workers} callers; ${config.commandsPerWorker} commands each on many aggregates, ${config.contendedCommandsPerWorker} each on one; " +
          s"notes of ${config.noteSize} characters; rebuild data ${config.rebuildAggregates} x ${config.rebuildEventsPerAggregate} events; " +
          s"restore sizes ${config.restoreSizes.mkString(", ")}; soak ${config.soakSeconds} s; " +
          s"delivery soak ${config.soakAggregates} x ${config.soakEventsPerAggregate} events, failing ${config.soakFailingSeconds} s, ${config.restartCycles} restarts")))
    }

    def section(title: String, header: Seq[String], rowsIn: Seq[Seq[String]], note: String): Unit = {
      parts += s"## $title\n\n$note\n\n" + table(header, rowsIn)
      println(s"[bench] $title done")
    }

    private def table(header: Seq[String], rowsIn: Seq[Seq[String]]): String =
      (Seq(header, header.map(_ => "---")) ++ rowsIn).map(_.mkString("| ", " | ", " |")).mkString("\n") + "\n"

    def text: String = parts.mkString("\n")

    def write(): File = {
      val root = new File(sys.props.getOrElse("bench.root", "."))
      val file = new File(new File(root, "docs/perf"), name + ".md")
      file.getParentFile.mkdirs()
      Files.write(file.toPath, text.getBytes(StandardCharsets.UTF_8))
      file
    }
  }
}
