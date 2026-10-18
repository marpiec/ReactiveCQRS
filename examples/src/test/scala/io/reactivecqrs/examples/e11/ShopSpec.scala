package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): the base of the specs that need actors, as an application writes it once.

// #base
import io.reactivecqrs.testkit.{ActorAskSupport, TestSystems}
import org.apache.pekko.testkit.TestKit
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

/** Pekko's TestKit on an actor system of the test kit: a cluster of one when the tests run with
  * -Dreactivecqrs.test.clusterOfOne=true. */
abstract class ShopSpec extends TestKit(TestSystems.newSystem()) with AnyFeatureSpecLike
  with GivenWhenThen with ActorAskSupport with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}
// #base
