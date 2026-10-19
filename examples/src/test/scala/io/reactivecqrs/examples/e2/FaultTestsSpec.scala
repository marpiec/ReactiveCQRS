package io.reactivecqrs.examples.e2

import java.util.UUID

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1.ItemAdded
import io.reactivecqrs.testkit.{ActorAskSupport, TestCluster, TestDatabase}
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import scalikejdbc._

import scala.concurrent.duration._

// The parts of E2's fault tests, in one JVM and without faults: the carts' two commands, a run of the client, and the
// check, which must find the faults it looks for. The faults themselves need Docker: examples/deploy/e2/faults/run.sh.
class FaultTestsSpec extends AnyFeatureSpec with Matchers with Eventually with ActorAskSupport {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(200, Millis))

  private def count(query: SQL[scala.Nothing, NoExtractor]): Long = DB.readOnly(implicit session => query.map(_.long(1)).single().getOrElse(0L))

  Feature("E2's fault tests, without the faults") {

    Scenario("the carts' two commands take effect once per key, wherever they are sent; E1's rule refuses a sixth item") {
      TestDatabase.inSchema("faults_commands") {
        val systems = TestCluster.start(2, name = "shop")
        try {
          val Vector(nodeA, nodeB) = systems.map(system => ClusterSetup.startNode(system, new FaultTestCartContext)): @unchecked
          def key() = Some(LedgerKey(UUID.randomUUID().toString))
          val cartId = nodeA.carts.askFor[AggregateId](ReserveAggregateId)
          val create = CreateCartOnce(key(), UserId(1), cartId, "Ledger")

          nodeA.carts.askFor[CustomCommandResponse[_]](create, 70.seconds) mustBe SuccessResponse(cartId, AggregateVersion(1))
          nodeB.carts.askFor[CustomCommandResponse[_]](create, 70.seconds) mustBe SuccessResponse(cartId, AggregateVersion(1))
          val apples = AddItemOnce(key(), UserId(1), cartId, "apples")
          nodeB.carts.askFor[CustomCommandResponse[_]](apples, 70.seconds) mustBe SuccessResponse(cartId, AggregateVersion(2))
          nodeA.carts.askFor[CustomCommandResponse[_]](apples, 70.seconds) mustBe SuccessResponse(cartId, AggregateVersion(2))
          (3 to 6).foreach(version => nodeA.carts.askFor[CustomCommandResponse[_]](AddItemOnce(key(), UserId(1), cartId, "pears"), 70.seconds) mustBe
            SuccessResponse(cartId, AggregateVersion(version)))
          nodeB.carts.askFor[CustomCommandResponse[_]](AddItemOnce(key(), UserId(1), cartId, "plums"), 70.seconds) mustBe
            FailureResponse(List("Cannot have more than 5 items in your cart"))
        } finally TestCluster.stop(systems)
      }
    }

    Scenario("a run of the client: every command answered and every check holds; then forged faults are found") {
      TestDatabase.inSchema("faults_run") {
        val systems = TestCluster.start(3, name = "shop")
        try {
          val nodes = systems.map(system => ClusterSetup.startNode(system, new FaultTestCartContext))
          FaultTables.create()
          val clients = nodes.take(2).zipWithIndex.map { case (node, n) => new FaultClient(node.carts, "client" + n, workers = 2, pause = 5.millis) }
          @volatile var running = true
          val refresher = new Thread(() => while (running) { clients.foreach(_.refresh()); Thread.sleep(200) })
          refresher.start()
          clients.foreach(_.start())
          try {
            eventually(count(sql"SELECT count(*) FROM fault_carts WHERE filled") must be >= 20L) // ...and so refusals too
            FaultTables.stop()
            eventually(clients.forall(_.finished) mustBe true)
          } finally running = false
          count(sql"SELECT count(*) FROM fault_ledger WHERE outcome = 'sent'") mustBe 0L

          FaultCheck.run(2.minutes) mustBe true
          count(sql"SELECT count(*) FROM fault_ledger WHERE kind = 'add' AND outcome = 'refused'") must be > 0L

          // A command whose success the ledger records and the store lacks, one the ledger says was refused, and an event
          // stored twice, at the same version: lost, wrongly answered, and applied twice.
          DB.autoCommit { implicit session =>
            sql"""INSERT INTO fault_ledger (key, incarnation, kind, cart_id, outcome)
                 |  SELECT 'forged', 'forged', 'add', cart_id, 'applied' FROM fault_carts LIMIT 1""".stripMargin.update()
            sql"""UPDATE fault_ledger SET outcome = 'refused'
                 | WHERE key = (SELECT key FROM fault_ledger WHERE kind = 'add' AND outcome = 'applied' AND key <> 'forged' LIMIT 1)""".stripMargin.update()
            sql"""DROP INDEX events_aggregate_version_uidx""".execute() // F198 keeps a version from being stored twice
            sql"""INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event)
                 |  SELECT nextval('events_seq'), user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event
                 |    FROM events WHERE id = (SELECT max(e.id) FROM events e JOIN types_names t ON t.id = e.event_type_id
                 |                             WHERE t.name = ${classOf[ItemAdded].getName} -- of a cart with room for one more
                 |                               AND (SELECT count(*) FROM events o WHERE o.aggregate_id = e.aggregate_id) < 6)""".stripMargin.update()
          }
          FaultCheck.results().filterNot(_.holds).map(_.description) mustBe Seq(
            "no version of an aggregate is in the store twice",
            "every applied add is in the store, in its cart",
            "no add is in the store twice",
            "no refused add is in the store")
        } finally TestCluster.stop(systems)
      }
    }
  }
}
