package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): the messages of E1 that cross nodes in a cluster go through the cluster serializer.

// #nodes
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1._
import io.reactivecqrs.testkit.TestSystems
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension, Serializers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CrossNodeSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  // An actor system with the cluster provider serializes as between nodes; it need not join a cluster.
  private val node = ActorSystem("serialization", TestSystems.clusterConfig.withFallback(ConfigFactory.load()))
  private val serialization = SerializationExtension(node)

  override def afterAll(): Unit = Await.ready(node.terminate(), 30.seconds)

  private def roundTrip(value: AnyRef): AnyRef =
    Serialization.withTransportInformation(node.asInstanceOf[ExtendedActorSystem]) { () =>
      val serializer = serialization.findSerializerFor(value)
      serialization.deserialize(serializer.toBinary(value), serializer.identifier, Serializers.manifestFor(serializer, value)).get
    }

  Feature("Messages between nodes") {

    Scenario("the carts' commands, events, states and questions cross nodes") {
      Seq(CreateShoppingCart(UserId(1), "Groceries"), AddItem(UserId(1), AggregateId(7), AggregateVersion(3), "pears"),
        ItemAdded("pears"), ShoppingCart("Groceries", Vector("apples")), GetCartSummary(AggregateId(7)),
        CartSummary("Groceries", 1, 2)).foreach(message => roundTrip(message) mustBe message)
    }
  }
}
// #nodes
