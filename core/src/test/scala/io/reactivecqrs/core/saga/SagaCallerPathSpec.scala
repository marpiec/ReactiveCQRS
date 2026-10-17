package io.reactivecqrs.core.saga

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._

// D46: a saga stores its caller's path, which the runner answers after it moved to another node.
class SagaCallerPathSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem("saga-caller", ConfigFactory.parseString(
    """pekko.actor.provider = cluster
      |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
      |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on""".stripMargin).withFallback(ConfigFactory.load()))

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  Feature("The caller of a saga (D46)") {

    Scenario("an ask made on the runner's node is stored with that node's address, so no other node takes it for one of its own (F137)") {
      val runner = TestProbe()(system)
      runner.ref.ask("order")(Timeout(5.seconds))
      runner.expectMsg("order")
      val caller = runner.lastSender

      val path = SagaActor.callerPath(system, caller)

      path must include("@127.0.0.1:")
      system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(path).path mustBe caller.path
    }
  }
}
