package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.saga._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
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

      runner.askActor[SagaResponse](Greet(user, "Ann"))(10.seconds) mustBe Greeted("Hello, Ann")
      awaitAssert(stored(name) mustBe 0, 5.seconds, 50.millis)
    }

    Scenario("a saga written as a SagaActor, as before 1.0, still runs") {
      val saga = system.actorOf(Props(new LegacyGreeter(sagaState, uidGenerator, "LegacyGreeter-" + System.nanoTime(), hold = false)))

      saga.askActor[SagaResponse](Greet(user, "Bob"))(10.seconds) mustBe Greeted("Hi, Bob")
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
