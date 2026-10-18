package io.reactivecqrs.core.cluster

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.testkit.TestSystems
import org.apache.pekko.actor.ActorSystem
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

// F178: a node whose messages to other nodes would not go through the library's serializer is not ready.
class SerializerBindingSpec extends AnyFeatureSpec with Matchers {

  private def withNode(bindings: String)(test: ActorSystem => Unit): Unit = {
    val system = ActorSystem("binding", ConfigFactory.parseString(bindings)
      .withFallback(TestSystems.clusterConfig).withFallback(ConfigFactory.load()))
    try test(system) finally Await.ready(system.terminate(), 30.seconds)
  }

  Feature("The serializer of messages between nodes (F178)") {

    Scenario("a node whose case classes go to Java serialization, as when pekko-actor's reference.conf came first, is not ready") {
      withNode("""pekko.actor.serialization-bindings { "java.io.Serializable" = java }""") { system =>
        Readiness(system).ready mustBe false
        Readiness(system).waitingFor.mkString must include("serializer")
      }
    }

    Scenario("a node whose case classes go to the library's serializer waits for its start only, and is ready once its bus is (F233)") {
      withNode("") { system =>
        Readiness(system).waitingFor mustBe Set(Readiness.Starting)
        Readiness(system).waitFor("a bus")
        Readiness(system).done("a bus")
        Readiness(system).ready mustBe true
      }
    }

    Scenario("F233 a node that shuts down is not ready") {
      withNode("") { system =>
        Readiness(system).waitFor("a bus")
        Readiness(system).done("a bus")
        Await.ready(org.apache.pekko.actor.CoordinatedShutdown(system).run(org.apache.pekko.actor.CoordinatedShutdown.UnknownReason), 30.seconds)
        Readiness(system).ready mustBe false
      }
    }
  }
}
