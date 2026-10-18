package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.saga._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, LogCapture, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.annotation.nowarn
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

case class Greet(userId: UserId, text: String) extends SagaOrder
case class Greeted(text: String) extends SagaResponse

/** A saga as data and functions (AR5): answers each greeting at once. */
class Greeter(val name: String) extends SagaDefinition {
  override def handleOrder(step: SagaStep): ReceiveOrder = { case Greet(_, text) => Future.successful(SagaSucceded(Greeted("Hello, " + text))) }
  override def handleRevert(step: SagaStep): ReceiveRevert = PartialFunction.empty
}

case class Shout(userId: UserId, text: String) extends SagaOrder

/** Takes no order, so its first step fails with a MatchError whose message is the whole order; and it cannot undo. */
class Mute(val name: String) extends SagaDefinition {
  override val stepRetries: io.reactivecqrs.core.util.Backoff = io.reactivecqrs.core.util.Backoff(10.millis, 10.millis, maxAttempts = 2)
  override def handleOrder(step: SagaStep): ReceiveOrder = PartialFunction.empty
  override def handleRevert(step: SagaStep): ReceiveRevert = { case _ => Future.successful(SagaRevertFailed(List("nothing can be undone"))) }
}

/** The same saga written as before 1.0, as an actor; `hold` keeps its first step from ending, so the saga stays stored. */
@nowarn("cat=deprecation")
class LegacyGreeter(val state: SagaState, val uidGenerator: ActorRef, override val name: String, hold: Boolean) extends SagaActor {
  override def handleOrder(step: SagaStep): ReceiveOrder = {
    case Greet(_, text) => if (hold) Promise[SagaHandlingStatus]().future else Future.successful(SagaSucceded(Greeted("Hi, " + text)))
  }
  override def handleRevert(step: SagaStep): ReceiveRevert = PartialFunction.empty
}

// AR5, D86: sagas as definitions run by the library's runner; the deprecated SagaActor runs on the same state machine.
class SagaRunnerSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val sagaState = new PostgresSagaState(mpjsons, new PostgresTypesNamesState().initSchema())
  sagaState.initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val user = UserId(1)

  private def stored(name: String): Int = DB.readOnly(implicit session => sql"SELECT count(*) FROM sagas WHERE name = $name".map(_.int(1)).single().get)

  Feature("Sagas run by the library (AR5)") {

    Scenario("a definition's saga answers its caller, and its row goes") {
      val name = "Greeter-" + System.nanoTime()
      val runner = system.actorOf(SagaRunner.props(new Greeter(name), uidGenerator, sagaState))

      runner.askFor[SagaResponse](Greet(user, "Ann")) mustBe Greeted("Hello, Ann")
      awaitAssert(stored(name) mustBe 0, 5.seconds, 50.millis)
    }

    Scenario("F191 a step and a revert that fail are answered and logged without the order's values") {
      val value = "marker" + System.nanoTime()
      val capture = new LogCapture(system)
      try {
        val runner = system.actorOf(SagaRunner.props(new Mute("Mute-" + System.nanoTime()), uidGenerator, sagaState))

        val answer = runner.askFor[SagaResponse](Shout(user, value))

        answer mustBe a[SagaFailureResponse]
        answer.asInstanceOf[SagaFailureResponse].exceptions.foreach(_ must (include("MatchError") and not include value))
        awaitAssert(capture.all.exists(_.contains("Saga revert failed")) mustBe true, 5.seconds, 50.millis)
        capture.containing(value) mustBe empty
      } finally capture.close()
    }

    Scenario("a saga written as a SagaActor, as before 1.0, still runs") {
      val saga = system.actorOf(Props(new LegacyGreeter(sagaState, uidGenerator, "LegacyGreeter-" + System.nanoTime(), hold = false)))

      saga.askFor[SagaResponse](Greet(user, "Bob")) mustBe Greeted("Hi, Bob")
    }

    Scenario("a saga a SagaActor stored resumes under the definition of its name, and answers its caller") {
      Given("a saga that a SagaActor stored and did not finish")
      val name = "Greeter-" + System.nanoTime()
      val caller = TestProbe()
      val legacy = system.actorOf(Props(new LegacyGreeter(sagaState, uidGenerator, name, hold = true)))
      legacy.tell(Greet(user, "Cleo"), caller.ref)
      awaitAssert(stored(name) mustBe 1, 5.seconds, 50.millis)
      val watcher = TestProbe()
      watcher.watch(legacy)
      system.stop(legacy)
      watcher.expectTerminated(legacy, 5.seconds)

      When("the saga, moved to a definition under the same name, starts")
      system.actorOf(SagaRunner.props(new Greeter(name), uidGenerator, sagaState))

      Then("it resumes the stored saga and answers the caller")
      caller.expectMsg(10.seconds, Greeted("Hello, Cleo"))
      awaitAssert(stored(name) mustBe 0, 5.seconds, 50.millis)
    }
  }
}
