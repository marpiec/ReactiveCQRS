package io.reactivecqrs.examples.e2

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Readiness
import io.reactivecqrs.core.util.SchemaLock
import io.reactivecqrs.examples.e1.{AddItem, ItemAdded, ShoppingCart, ShoppingCartCreated}
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.cluster.{Cluster, Member, MemberStatus}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Random, Try}

// E2's fault tests (chapter 17): the cluster under load while nodes are killed, stopped, or cut off from the others.
// Every node of examples/deploy/e2/faults runs `FaultTests node`: E2's node, whose carts take two more commands, and a
// client that sends them. The client gives every command a key, records it in a ledger before sending it, and sends it
// again, with the same key, after every lost answer, until it is answered. `run.sh` injects the faults; `FaultTests
// check` then compares what the database holds with the ledger.

/** The key of one command of the fault tests' client: the idempotency key of every copy it sends. */
case class LedgerKey(value: String) extends CommandIdempotencyId {
  override def asDbKey: String = value
}

/** Creates a cart under an id reserved beforehand (`ReserveAggregateId`), once per key. */
case class CreateCartOnce(idempotencyId: Option[LedgerKey], userId: UserId, cartId: AggregateId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[LedgerKey] {
  override def chosenAggregateId: Option[AggregateId] = Some(cartId)
}

/** Adds an item, once per key, at whatever version the cart is. */
case class AddItemOnce(idempotencyId: Option[LedgerKey], userId: UserId, aggregateId: AggregateId, name: String)
  extends ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[LedgerKey]

/** E2's carts with the fault tests' two commands. E1's rule for `AddItem` decides `AddItemOnce`: five items at most. */
class FaultTestCartContext extends ShardedCartContext {
  override def commandHandlers: CommandHandler = cart => {
    val once: PartialFunction[Any, GenericCommandResult[Any]] = {
      case c: CreateCartOnce => CommandSuccess(ShoppingCartCreated(c.name))
      case c: AddItemOnce => super.commandHandlers(cart)(AddItem(c.userId, c.aggregateId, AggregateVersion.ZERO, c.name))
    }
    super.commandHandlers(cart).orElse(once)
  }
}

/** The tables of the fault tests, next to the library's: the client's ledger, the carts the clients fill, each node's
  * view of the cluster, and the switch that ends a run. */
object FaultTables {
  def create(): Unit = SchemaLock(DB.autoCommit { implicit session => // nodes start together: one creates, the others wait
    sql"""CREATE TABLE IF NOT EXISTS fault_ledger (
         |  key VARCHAR(64) PRIMARY KEY,
         |  incarnation VARCHAR(64) NOT NULL,
         |  kind VARCHAR(8) NOT NULL,
         |  cart_id BIGINT NOT NULL,
         |  outcome VARCHAR(8) NOT NULL,
         |  attempts INT NOT NULL DEFAULT 1,
         |  last_error TEXT,
         |  sent TIMESTAMP NOT NULL DEFAULT now(),
         |  resolved TIMESTAMP)""".stripMargin.execute()
    sql"""CREATE TABLE IF NOT EXISTS fault_carts (cart_id BIGINT PRIMARY KEY, filled BOOLEAN NOT NULL DEFAULT false)""".execute()
    sql"""CREATE TABLE IF NOT EXISTS fault_nodes (
         |  node VARCHAR(32) PRIMARY KEY,
         |  incarnation VARCHAR(64) NOT NULL,
         |  oldest VARCHAR(64),
         |  members TEXT NOT NULL,
         |  seen TIMESTAMP NOT NULL)""".stripMargin.execute()
    sql"""CREATE TABLE IF NOT EXISTS fault_control (id INT PRIMARY KEY, stopped BOOLEAN NOT NULL)""".execute()
    sql"""INSERT INTO fault_control (id, stopped) VALUES (1, false) ON CONFLICT (id) DO NOTHING""".update()
  })

  /** Ends a run: the clients send no new command, and give the ones under way three minutes more. */
  def stop(): Unit = DB.autoCommit(implicit session => sql"UPDATE fault_control SET stopped = true".update())
}

/**
  * The fault tests' client on one node: `workers` threads that make carts and fill them, with the carts shared by the
  * clients of all nodes (fault_carts). Each command is recorded in the ledger (fault_ledger) as `sent` before it is sent,
  * and once answered as `applied` (a success) or `refused` (a failure: the cart was full). A command whose answer is
  * lost (none in 15 seconds, or an error) is sent again, with its key, until it is answered; one whose node dies first
  * stays `sent`. `refresh` keeps the open carts and the switch current; it is called every second or so.
  */
final class FaultClient(carts: ActorRef, incarnation: String, workers: Int, pause: FiniteDuration) {
  private implicit val timeout: Timeout = Timeout(15.seconds)
  private val user = UserId(1)
  private val sequence = new AtomicLong()
  private val open = new AtomicReference(Vector.empty[AggregateId])
  @volatile private var stopping: Option[Deadline] = None
  private val threads = (1 to workers).map { n =>
    val worker = new Thread(() => while (stopping.isEmpty) { next(); Thread.sleep(pause.toMillis) }, "faultClient" + n)
    worker.setDaemon(true)
    worker
  }

  def start(): Unit = threads.foreach(_.start())

  /** Whether every worker has ended: after the switch, once its last command is answered. */
  def finished: Boolean = threads.forall(!_.isAlive)

  /** The newest open carts, and whether the run is over (then the commands under way get three minutes more). */
  def refresh(): Unit = DB.readOnly { implicit session =>
    open.set(sql"SELECT cart_id FROM fault_carts WHERE NOT filled ORDER BY cart_id DESC LIMIT 50".map(rs => AggregateId(rs.long(1))).list().toVector)
    if (stopping.isEmpty && sql"SELECT stopped FROM fault_control".map(_.boolean(1)).single().contains(true)) {
      stopping = Some(3.minutes.fromNow)
    }
  }

  private def next(): Unit = {
    val carts = open.get()
    if (carts.size < 10) createCart() else addItem(carts(Random.nextInt(carts.size)))
  }

  private def createCart(): Unit = {
    val cartId = Iterator.continually(Try(Await.result((carts ? ReserveAggregateId).mapTo[AggregateId], timeout.duration))
      .toOption.orElse { Thread.sleep(1000); None }).flatten.next()
    val key = newKey("create", cartId)
    send(key, CreateCartOnce(Some(LedgerKey(key)), user, cartId, key)).foreach {
      case _: SuccessResponse =>
        DB.autoCommit(implicit session => sql"INSERT INTO fault_carts (cart_id) VALUES (${cartId.asLong})".update())
        open.updateAndGet(_ :+ cartId)
      case _ => ()
    }
  }

  private def addItem(cartId: AggregateId): Unit = {
    val key = newKey("add", cartId)
    send(key, AddItemOnce(Some(LedgerKey(key)), user, cartId, key)).foreach {
      case _: FailureResponse => // full
        DB.autoCommit(implicit session => sql"UPDATE fault_carts SET filled = true WHERE cart_id = ${cartId.asLong}".update())
        open.updateAndGet(_.filterNot(_ == cartId))
      case _ => ()
    }
  }

  private def newKey(kind: String, cartId: AggregateId): String = {
    val key = incarnation + "-" + sequence.incrementAndGet()
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO fault_ledger (key, incarnation, kind, cart_id, outcome) VALUES ($key, $incarnation, $kind, ${cartId.asLong}, 'sent')""".update()
    }
    key
  }

  /** The answer to `command`, sent until it is answered, and recorded; None if the run ended first. */
  private def send(key: String, command: AnyRef): Option[CustomCommandResponse[_]] = {
    var attempts = 0
    var answer: Option[CustomCommandResponse[_]] = None
    while (answer.isEmpty && stopping.forall(_.hasTimeLeft())) {
      attempts += 1
      val error = Try(Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)).fold(_.toString, {
        case response @ (_: SuccessResponse | _: FailureResponse) => answer = Some(response); ""
        case other => other.toString // an error that a copy may not meet: AggregateConcurrentModificationError, ...
      })
      if (answer.isEmpty) {
        DB.autoCommit(implicit session => sql"UPDATE fault_ledger SET attempts = $attempts, last_error = $error WHERE key = $key".update())
        Thread.sleep(1000)
      }
    }
    answer.foreach { response =>
      val outcome = if (response.isInstanceOf[SuccessResponse]) "applied" else "refused"
      DB.autoCommit(implicit session => sql"UPDATE fault_ledger SET outcome = $outcome, attempts = $attempts, resolved = now() WHERE key = $key".update())
    }
    answer
  }
}

/** What the database holds after a run of the fault tests, compared with the clients' ledger. */
object FaultCheck {

  final case class Result(description: String, violations: Long) {
    def holds: Boolean = violations == 0
  }

  private def number(query: String, params: Any*): Long =
    DB.readOnly(implicit session => SQL(query).bind(params: _*).map(_.long(1)).single().getOrElse(0L))

  // The events of the class its first parameter names, with the name each carries: a cart's or an item's, which is the
  // key of the command that stored it.
  private val named =
    """WITH named AS (SELECT e.aggregate_id, e.event::json->>'name' AS name FROM events e
      |  JOIN types_names t ON t.id = e.event_type_id WHERE t.name = ?)""".stripMargin

  private def cartsListTable: Option[String] = DB.readOnly { implicit session =>
    sql"""SELECT o.object_name FROM projections p JOIN projection_build_objects o ON o.name = p.name AND o.build = p.active_build
         | WHERE p.name = 'e2-carts-list' AND NOT o.is_sequence""".stripMargin.map(_.string(1)).single()
  }

  /** Carts whose summary in the carts list is missing or at another version than the cart (all of them, before the
    * carts list has a table). */
  def cartsBehind(): Long = {
    val carts = "FROM aggregates a JOIN types_names t ON t.id = a.type_id"
    val cartType = classOf[ShoppingCart].getName
    cartsListTable match {
      case Some(table) => number(s"""SELECT count(*) $carts LEFT JOIN $table d ON d.id = a.id
                                    | WHERE a.id = a.base_id AND t.name = ?
                                    |   AND (d.id IS NULL OR (d.document->>'version')::int <> a.base_version)""".stripMargin, cartType)
      case None => number(s"SELECT count(*) $carts WHERE a.id = a.base_id AND t.name = ?", cartType)
    }
  }

  def outbox(): Long = number("SELECT count(*) FROM events_to_publish")

  /** Commands of clients still running that are not answered yet. */
  def unansweredOfLiveClients(): Long =
    number("""SELECT count(*) FROM fault_ledger l JOIN fault_nodes n ON n.incarnation = l.incarnation
             | WHERE l.outcome = 'sent' AND n.seen > now() - interval '15 seconds'""".stripMargin)

  def results(): Seq[Result] = {
    def perKind(kind: String, eventClass: Class[_]): Seq[Result] = {
      val events = eventClass.getName
      Seq(
        Result(s"every applied $kind is in the store, in its cart", number(
          s"""$named SELECT count(*) FROM fault_ledger l WHERE l.kind = ? AND l.outcome = 'applied'
             |   AND NOT EXISTS (SELECT 1 FROM named WHERE named.name = l.key AND named.aggregate_id = l.cart_id)""".stripMargin, events, kind)),
        Result(s"no $kind is in the store twice", number(
          s"$named SELECT count(*) FROM (SELECT name FROM named GROUP BY name HAVING count(*) > 1) twice", events)),
        Result(s"no refused $kind is in the store", number(
          s"""$named SELECT count(*) FROM fault_ledger l WHERE l.kind = ? AND l.outcome = 'refused'
             |   AND EXISTS (SELECT 1 FROM named WHERE named.name = l.key)""".stripMargin, events, kind)),
        Result(s"every $kind in the store was sent by a client", number(
          s"$named SELECT count(*) FROM named WHERE NOT EXISTS (SELECT 1 FROM fault_ledger l WHERE l.key = named.name)", events)))
    }

    Seq(Result("no version of an aggregate is in the store twice",
        number("SELECT count(*) FROM (SELECT aggregate_id, version FROM events GROUP BY 1, 2 HAVING count(*) > 1) twice")),
      Result("every command of a client still running was answered", unansweredOfLiveClients())) ++
      perKind("create", classOf[ShoppingCartCreated]) ++
      perKind("add", classOf[ItemAdded]) ++
      Seq(Result("no cart has more than five items", number(
          s"$named SELECT count(*) FROM (SELECT aggregate_id FROM named GROUP BY 1 HAVING count(*) > 5) over", classOf[ItemAdded].getName)),
        Result("the carts list has every cart at its version", cartsBehind()),
        Result("no event waits in the outbox", outbox()))
  }

  /** What the run did: the commands, their outcomes, and the copies sent after lost answers. */
  def summary(): String = DB.readOnly { implicit session =>
    val byOutcome = sql"SELECT kind, outcome, count(*) FROM fault_ledger GROUP BY 1, 2 ORDER BY 1, 2"
      .map(rs => s"${rs.string(1)} ${rs.string(2)}: ${rs.long(3)}").list()
    val again = sql"""SELECT count(*), count(*) FILTER (WHERE outcome = 'applied'), coalesce(max(attempts), 0) FROM fault_ledger WHERE attempts > 1"""
      .map(rs => s"sent again after a lost answer: ${rs.long(1)} commands (${rs.long(2)} applied), at most ${rs.int(3)} copies of one").single()
    val unknown = number(s"""$named SELECT count(*) FROM fault_ledger l WHERE l.kind = 'add' AND l.outcome = 'sent'
                            |   AND EXISTS (SELECT 1 FROM named WHERE named.name = l.key)""".stripMargin, classOf[ItemAdded].getName)
    (byOutcome ++ again ++ Seq(s"adds left unanswered by a node that died, yet applied: $unknown",
      s"events: ${number("SELECT count(*) FROM events")}, carts: ${number("SELECT count(*) FROM fault_carts")}")).mkString("\n")
  }

  /** Waits, up to `patience`, until the clients have their answers, the carts list has every cart at its version and the
    * outbox is empty; then checks. True when every check holds. */
  def run(patience: FiniteDuration): Boolean = {
    val started = System.nanoTime()
    val deadline = patience.fromNow
    def settled = unansweredOfLiveClients() == 0 && cartsBehind() == 0 && outbox() == 0
    while (!settled && deadline.hasTimeLeft()) {
      println(s"waiting: ${unansweredOfLiveClients()} unanswered, ${cartsBehind()} carts behind, ${outbox()} events in the outbox")
      Thread.sleep(10000)
    }
    println(s"waited ${(System.nanoTime() - started) / 1000000000L} s")
    println(summary())
    val checked = results()
    checked.foreach(r => println((if (r.holds) "OK    " else "FAILED") + " " + r.description + (if (r.holds) "" else s" (${r.violations})")))
    checked.forall(_.holds)
  }
}

/** `FaultTests node` runs a node of the fault tests, `FaultTests check` checks a run (examples/deploy/e2/faults). */
object FaultTests {

  def main(args: Array[String]): Unit = args.headOption match {
    case Some("node") => node()
    case Some("check") =>
      ClusterSetup.connectFromEnvironment()
      sys.exit(if (FaultCheck.run(sys.env.getOrElse("CHECK_PATIENCE_MINUTES", "12").toInt.minutes)) 0 else 1)
    case _ => println("Usage: FaultTests node | check")
  }

  private def node(): Unit = {
    val (system, node) = ClusterSetup.startProcess(new FaultTestCartContext)
    FaultTables.create()
    val name = sys.env.getOrElse("NODE_NAME", "node")
    val incarnation = name + "-" + java.lang.Long.toString(System.currentTimeMillis(), 36)
    while (!Readiness(system).ready) Thread.sleep(500)
    println(s"$incarnation is ready")
    val client = new FaultClient(node.carts, incarnation,
      sys.env.getOrElse("CLIENT_WORKERS", "3").toInt, sys.env.getOrElse("CLIENT_PAUSE_MS", "20").toInt.millis)
    client.refresh()
    client.start()
    var oldest: Option[String] = None
    val watcher = new Thread(() => while (true) {
      Try {
        oldest = recordView(system, name, incarnation, oldest)
        client.refresh()
      }
      Thread.sleep(1000)
    }, "faultWatcher")
    watcher.setDaemon(true)
    watcher.start()
    Await.ready(system.whenTerminated, Duration.Inf)
  }

  /** Records which members this node sees up, the oldest first, and says when the oldest changes. */
  private def recordView(system: ActorSystem, name: String, incarnation: String, before: Option[String]): Option[String] = {
    val up = Cluster(system).state.members.filter(_.status == MemberStatus.Up).toSeq.sorted(Member.ageOrdering)
    val hosts = up.flatMap(_.address.host)
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO fault_nodes (node, incarnation, oldest, members, seen) VALUES ($name, $incarnation, ${hosts.headOption}, ${hosts.mkString(",")}, now())
           | ON CONFLICT (node) DO UPDATE SET incarnation = EXCLUDED.incarnation, oldest = EXCLUDED.oldest,
           |   members = EXCLUDED.members, seen = EXCLUDED.seen""".stripMargin.update()
    }
    if (hosts.headOption != before) println(s"$incarnation sees ${hosts.headOption.getOrElse("no member")} as the oldest, members: ${hosts.mkString(", ")}")
    hosts.headOption
  }
}
