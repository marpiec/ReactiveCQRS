package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.types.{PostgresTypesNamesState, TypesNamesState}
import io.reactivecqrs.core.util.{Backoff, BlockingDispatcher}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

case class GroupTask(done: Boolean)
case class GroupTaskDone() extends Event[GroupTask]

case object ReadAppliedRows

/** Counts the reads of applied versions: one aggregate at a time, or many at once. */
class CountedReads(typesNamesState: TypesNamesState) extends PostgresSubscriptionsState(typesNamesState) {
  val singleReads = new AtomicInteger()
  val manyReads = new AtomicInteger()
  override def lastVersion(subscriberName: String, subscriptionType: SubscriptionType, aggregateId: AggregateId): AggregateVersion = {
    singleReads.incrementAndGet()
    super.lastVersion(subscriberName, subscriptionType, aggregateId)
  }
  override def lastVersions(subscriberName: String, subscriptionType: SubscriptionType, aggregateIds: Seq[AggregateId], cache: Boolean): Map[AggregateId, AggregateVersion] = {
    manyReads.incrementAndGet()
    super.lastVersions(subscriberName, subscriptionType, aggregateIds, cache)
  }
}

/** Writes each note event it applies to a table, with the transaction that wrote it, and counts the calls of its task
  * listener, which has no transaction. Its listeners fail for an aggregate as `failures` says, after writing; the note
  * listener also as `failuresBeforeWork` says, while it makes its work. */
class TableProjection(val subscriptionsState: SubscriptionsState, failures: ConcurrentHashMap[AggregateId, AtomicInteger],
                      failuresBeforeWork: ConcurrentHashMap[AggregateId, AtomicInteger], slow: AtomicReference[AggregateId],
                      taskCalls: ConcurrentHashMap[AggregateId, AtomicInteger])
  extends ProjectionActor(ProjectionActorOptions.DEFAULT.withRetries(Backoff(20.millis, 50.millis, maxAttempts = 3))) {

  private def failIfAsked(id: AggregateId, asked: ConcurrentHashMap[AggregateId, AtomicInteger] = failures): Unit =
    Option(asked.get(id)).filter(_.getAndDecrement() > 0).foreach(_ => throw new IllegalStateException("listener failed on " + id.asLong))

  protected val listeners = List(
    EventsListener[GateNote] { (id, events) =>
      failIfAsked(id, failuresBeforeWork)
      (session: DBSession) => {
        implicit val s: DBSession = session
        if (slow.get() == id) Thread.sleep(300) // the deliveries sent meanwhile wait in the mailbox
        events.foreach(e => sql"INSERT INTO groups_spec_applied (aggregate_id, version, txid) VALUES (${id.asLong}, ${e.version.asInt}, txid_current())".update())
        failIfAsked(id)
      }
    },
    EventsListener.noDBSession[GroupTask] { (id, _) =>
      taskCalls.computeIfAbsent(id, _ => new AtomicInteger()).incrementAndGet()
      failIfAsked(id)
    })

  override protected def receiveQuery: Receive = {
    case ReadAppliedRows => sender() ! DB.readOnly { implicit session =>
      sql"SELECT aggregate_id, version, txid FROM groups_spec_applied".map(rs => (AggregateId(rs.long(1)), rs.int(2), rs.long(3))).list()
    }
  }

  override protected val projectionName: String = "TableProjection"
}

// D61, F93: without parallel processing, the batches of many aggregates that arrive together are applied in one
// transaction; a group that fails is applied again one batch at a time, and nothing else waits behind a group.
class ProjectionGroupsSpec extends CommonSpec {

  TestDatabase.init()
  DB.autoCommit { implicit session =>
    sql"CREATE TABLE IF NOT EXISTS groups_spec_applied (aggregate_id BIGINT NOT NULL, version INT NOT NULL, txid BIGINT NOT NULL)".execute()
  }

  private val noteType = AggregateType.of[GateNote]
  private val taskType = AggregateType.of[GroupTask]
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  new PostgresSubscriptionsState(typesNamesState).initSchema()

  private class Fixture {
    val subscriptionsState = new CountedReads(typesNamesState)
    val failures = new ConcurrentHashMap[AggregateId, AtomicInteger]()
    val failuresBeforeWork = new ConcurrentHashMap[AggregateId, AtomicInteger]()
    val taskCalls = new ConcurrentHashMap[AggregateId, AtomicInteger]()
    private val slow = new AtomicReference[AggregateId]()
    val bus = TestProbe()
    val subscriber: String = "ProjectionGroupsSpec-" + System.nanoTime() // no failures of earlier scenarios to retry
    val projection: ActorRef = system.actorOf(Props(new TableProjection(subscriptionsState, failures, failuresBeforeWork, slow, taskCalls)).withDispatcher(BlockingDispatcher.id))
    projection ! BindSubscriber(subscriber)
    bus.send(projection, GetSubscriptionRequests(projection))
    bus.expectMsgType[SubscriptionRequests](3.seconds)

    def slowOn(id: AggregateId): Unit = slow.set(id)
    def failTimes(id: AggregateId, times: Int): Unit = failures.put(id, new AtomicInteger(times))

    def note(id: AggregateId, version: Int): Unit =
      bus.send(projection, IdentifiableEvents[GateNote](noteType, id, Seq(EventInfo[GateNote](AggregateVersion(version), GateNoteChanged(version), UserId(1), Instant.now))))

    def task(id: AggregateId): Unit =
      bus.send(projection, IdentifiableEvents[GroupTask](taskType, id, Seq(EventInfo[GroupTask](AggregateVersion(1), GroupTaskDone(), UserId(1), Instant.now))))

    /** The next `n` answers: the aggregates acknowledged and those failed. */
    def answers(n: Int): (Set[AggregateId], Set[AggregateId]) = {
      val received = bus.receiveN(n, 10.seconds)
      (received.collect { case MessageAck(_, id, _, _) => id }.toSet, received.collect { case MessageFailed(_, id, _, _, _) => id }.toSet)
    }

    /** The rows written for these aggregates, asked through the bus's own channel, so after its deliveries. */
    def rows(ids: Seq[AggregateId]): Seq[(AggregateId, Int, Long)] = {
      bus.send(projection, ReadAppliedRows)
      bus.fishForSpecificMessage(5.seconds) { case rows: List[_] => rows.asInstanceOf[List[(AggregateId, Int, Long)]] }.filter(row => ids.contains(row._1))
    }
  }

  private def newIds(n: Int): Seq[AggregateId] = (1 to n).map(_ => AggregateId(TestIds.aggregate()))

  Feature("Groups of batches (D61, F93)") {

    Scenario("deliveries that arrive while the projection is busy are applied together, in one transaction") {
      val f = new Fixture
      val ids = newIds(30)
      f.slowOn(ids.head)

      ids.foreach(f.note(_, 1))

      f.answers(30) mustBe ((ids.toSet, Set.empty))
      val rows = f.rows(ids)
      rows.map(_._1).sortBy(_.asLong) mustBe ids.sortBy(_.asLong) // each applied once
      rows.map(_._3).distinct.size must be <= 2 // the slow first one, maybe alone, and the rest together
      f.subscriptionsState.singleReads.get mustBe 0 // the versions of each group were read at once
      f.subscriptionsState.manyReads.get must be <= 2
      ids.map(f.subscriptionsState.lastVersion(f.subscriber, SubscriptionType.EVENTS, _)).toSet mustBe Set(AggregateVersion(1))
    }

    Scenario("a group that fails is applied one batch at a time: the failing aggregate fails alone, the others are applied once") {
      val f = new Fixture
      val ids = newIds(10)
      val broken = ids(5)
      f.slowOn(ids.head)
      f.failTimes(broken, 100)

      ids.foreach(f.note(_, 1))

      f.answers(10) mustBe ((ids.toSet - broken, Set(broken)))
      f.rows(ids).map(_._1).sortBy(_.asLong) mustBe (ids.toSet - broken).toSeq.sortBy(_.asLong) // the group's writes were rolled back
    }

    Scenario("a listener that fails while it makes its work is retried like any failing batch, and the rest of the group is applied") {
      val f = new Fixture
      val ids = newIds(10)
      f.slowOn(ids.head)
      f.failuresBeforeWork.put(ids(3), new AtomicInteger(1))

      ids.foreach(f.note(_, 1))

      f.answers(10) mustBe ((ids.toSet, Set.empty))
      f.rows(ids).map(_._1).sortBy(_.asLong) mustBe ids.sortBy(_.asLong)
    }

    Scenario("a message other than a delivery is handled once the deliveries taken before it are applied") {
      val f = new Fixture
      val ids = newIds(20)
      f.slowOn(ids.head)

      ids.foreach(f.note(_, 1))

      f.rows(ids).size mustBe 20
    }

    Scenario("a listener without a transaction is applied alone, so another aggregate's failure does not repeat it") {
      val f = new Fixture
      val Seq(first, broken) = newIds(2)
      val task = newIds(1).head
      f.slowOn(first)
      f.failTimes(broken, 100)

      f.note(first, 1)
      f.task(task)
      f.note(broken, 1)

      f.answers(3) mustBe ((Set(first, task), Set(broken)))
      f.taskCalls.get(task).get mustBe 1
    }
  }
}
