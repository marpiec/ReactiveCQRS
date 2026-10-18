package io.reactivecqrs.core.util

import java.io.{PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.{CommonSpec, LogCapture, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import org.postgresql.util.PSQLException
import scalikejdbc._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

case class Customer(name: String)
case class CustomerCreated(name: String) extends FirstEvent[Customer] {
  def spaceId: SpaceId = SpaceId(0)
}
case class CustomerRenamed(name: String) extends Event[Customer]

/** Its listener has a case for creations only: a rename fails it with a MatchError, whose message is the whole event. */
class CreationsOnlyProjection(val subscriptionsState: SubscriptionsState)
  extends ProjectionActor(ProjectionActorOptions.DEFAULT.withRetries(Backoff(10.millis, 20.millis, maxAttempts = 2))) {
  protected val listeners = List(EventsListener.noDBSession[Customer] { (_, events) =>
    events.foreach(e => (e.event: @unchecked) match { case CustomerCreated(_) => () })
  })
  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = ()
  override protected val projectionName: String = "CreationsOnlyProjection"
}

// F190, F191: the values that events, states and statements carry reach neither the logs nor what is stored or answered
// about a failure; the failure is still named.
class LogPrivacySpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val eventStore = new PostgresEventStoreState(mpjsons, new PostgresTypesNamesState().initSchema()).initSchema()

  private def marker(): String = "marker" + System.nanoTime()

  private def stackTrace(e: Throwable): String = {
    val text = new StringWriter()
    e.printStackTrace(new PrintWriter(text))
    text.toString
  }

  Feature("Values in failures") {

    Scenario("F190 a statement of the library that fails logs none of the values it was given") {
      val value = marker()
      val capture = new LogCapture(system)
      try {
        val failed = eventStore.localTx { session =>
          eventStore.persistEvents(AggregateId(TestIds.aggregate()),
            NewEvents[Customer](UserId(1), AggregateVersion(3), Instant.now, Seq(CustomerCreated(value))))(session)
        }
        failed.isFailure mustBe true
        capture.containing(value) mustBe empty
      } finally capture.close()
    }

    Scenario("F190 every file of the library that opens a session opens it with SqlLogging's settings") {
      val root = Iterator.iterate(Paths.get("").toAbsolutePath)(_.getParent).takeWhile(_ != null)
        .find(dir => Files.isDirectory(dir.resolve("core/src/main/scala"))).get.resolve("core/src/main/scala")
      val opening = """DB\.(localTx|readOnly|autoCommit)""".r
      val sources = Files.walk(root).iterator().asScala.filter(_.toString.endsWith(".scala")).toSeq
      val withoutSettings = sources.filter { file =>
        val text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
        opening.findFirstIn(text).isDefined && !text.contains("SqlLogging.withoutValues")
      }
      withoutSettings mustBe empty
    }

    Scenario("F191 a failure is described by its class and what names it, without the values it failed on") {
      val value = marker()

      val renamed: Any = CustomerRenamed(value)
      val unmatched = Try((renamed: @unchecked) match { case CustomerCreated(_) => () }).failed.get
      Errors.describe(unmatched) mustBe "scala.MatchError: a value of class " + classOf[CustomerRenamed].getName

      val unreadable = Try(mpjsons.deserialize[CustomerRenamed]("{\"name\":\"" + value + "\",", classOf[CustomerRenamed].getName)).failed.get
      Errors.describe(unreadable) must (include("Problem deserializing") and not include value)

      DB.autoCommit { implicit session =>
        sql"CREATE TABLE IF NOT EXISTS log_privacy_names (name TEXT CONSTRAINT log_privacy_names_unique UNIQUE)".execute()
        sql"INSERT INTO log_privacy_names (name) VALUES ($value)".update()
      }
      val duplicate = Try(DB.autoCommit(implicit session => sql"INSERT INTO log_privacy_names (name) VALUES ($value)".update())).failed.get
      duplicate mustBe a[PSQLException]
      Errors.describe(duplicate) must (include("23505") and include("log_privacy_names_unique") and not include value)

      Errors.describe(new IllegalStateException("listener failed on 42")) mustBe "java.lang.IllegalStateException: listener failed on 42"
      Errors.describe(new IllegalStateException("x" * 5000)).length must be <= Errors.MaxLength + 100

      And("logged, it keeps its stack trace and its causes, without the values")
      val wrapped = new RuntimeException("restore failed", unmatched)
      val logged = stackTrace(Errors.redacted(wrapped))
      logged must (include("restore failed") and include("scala.MatchError") and include("LogPrivacySpec") and not include value)
    }

    Scenario("F191 a listener's MatchError is answered, recorded, reported and logged without the event's values") {
      val value = marker()
      val capture = new LogCapture(system)
      try {
        val state = new MemorySubscriptionsState
        val bus = TestProbe()
        val projection: ActorRef = system.actorOf(Props(new CreationsOnlyProjection(state)))
        bus.send(projection, GetSubscriptionRequests(projection))
        bus.expectMsgType[SubscriptionRequests](3.seconds)
        val id = AggregateId(TestIds.aggregate())
        val customerType = AggregateType.of[Customer]

        bus.send(projection, IdentifiableEvents[Customer](customerType, id, Seq(
          EventInfo[Customer](AggregateVersion(1), CustomerCreated("first"), UserId(1), Instant.now),
          EventInfo[Customer](AggregateVersion(2), CustomerRenamed(value), UserId(1), Instant.now))))

        val answered = bus.fishForSpecificMessage(5.seconds) { case MessageFailed(_, `id`, _, error, _) => error }
        answered must (include("MatchError") and not include value)
        val reported = projection.askFor[ProjectionStatus](GetProjectionStatus, 3.seconds).failures
        reported.map(_.error) must have size 1
        reported.foreach(_.error must not include value)
        state.failures(classOf[CreationsOnlyProjection].getName).foreach(_.error must not include value)
        awaitAssert(capture.all.exists(_.contains("MatchError")) mustBe true, 5.seconds, 50.millis)
        capture.containing(value) mustBe empty
      } finally capture.close()
    }
  }
}
