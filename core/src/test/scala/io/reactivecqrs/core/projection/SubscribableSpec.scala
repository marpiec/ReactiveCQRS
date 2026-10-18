package io.reactivecqrs.core.projection

import java.time.Instant

import org.apache.pekko.actor._
import org.apache.pekko.testkit.{TestActorRef, TestProbe}
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.projection.SubscribableProjectionActor.{CancelProjectionSubscriptions, ProjectionSubscriptionsCancelled, RenewSubscription, SubscribedForProjectionUpdates, SubscriptionUpdated}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import org.scalatest.{BeforeAndAfter, GivenWhenThen}
import org.scalatest.featurespec.AnyFeatureSpecLike
import io.reactivecqrs.testkit.TestDatabase
import scalikejdbc.DBSession

import scala.concurrent.duration._


case class StringEvent(aggregate: String) extends Event[String]

case class SubscribeForAll(subscriptionCode: String, listener: ActorRef)

class SimpleProjection(val subscriptionsState: PostgresSubscriptionsState) extends SubscribableProjectionActor {

  override def receiveSubscriptionRequest: Receive = {
    case SubscribeForAll(code, listener) => handleSubscribe(code, listener, (s: String) => Some(s))
  }

  override protected def receiveQuery: Receive = {
    case ref: ActorRef => ref ! "test"
  }

  override protected val listeners: List[Listener[Any]] = List(EventsListener(handleUpdate))

  private def handleUpdate(aggregateId: AggregateId, events: Seq[EventInfo[String]]) = { (_: DBSession) =>
    events.foreach(eventInfo => {
      sendUpdate(eventInfo.event.asInstanceOf[StringEvent].aggregate)
    })
  }

  override protected val projectionName: String = "SimpleProjection"
}

class SimpleListener(simpleListenerProbe: TestProbe) extends Actor {
  private var subscriptionId: Option[String] = None
  override def receive: Receive = {
    case SubscribedForProjectionUpdates(id) => {
      subscriptionId = Some(id)
      simpleListenerProbe.ref ! SubscribedForProjectionUpdates(id)
    }
    case ProjectionSubscriptionsCancelled(id :: Nil) => {
      if (subscriptionId.get == id) {
        subscriptionId = None
      }
      simpleListenerProbe.ref ! ProjectionSubscriptionsCancelled(id :: Nil)
    }
    case SubscriptionUpdated(id, data) => simpleListenerProbe.ref ! data // TODO validate subscription id?
    case actor: ActorRef => actor ! CancelProjectionSubscriptions(List(subscriptionId.get))
  }
}

class SubscribableSpec extends AnyFeatureSpecLike with GivenWhenThen with BeforeAndAfter {
  TestDatabase.init()
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = Timeout(10.seconds)

  private class Fixture {
    val typesNamesState = new PostgresTypesNamesState().initSchema() // run alone, no other spec created the table
    val subscriptionsState = new PostgresSubscriptionsState(typesNamesState)
    subscriptionsState.initSchema()

    val simpleProjectionActor = TestActorRef(Props(new SimpleProjection(subscriptionsState)))

    // use this actor/probe combo - the actor is necessary to store subscription id
    val simpleListenerProbe = TestProbe()
    val simpleListener = actorSystem.actorOf(Props(new SimpleListener(simpleListenerProbe)))

    val secondSimpleListenerProbe = TestProbe()
    val secondSimpleListener = actorSystem.actorOf(Props(new SimpleListener(secondSimpleListenerProbe)))

    // Subscription versions are stored, so each scenario updates an aggregate of its own.
    val aggregateId = AggregateId(System.nanoTime())
  }

  private def fixture = new Fixture

  val stringType = AggregateType.of[String]

  Feature("Can subscribe to all events") {

    Scenario("Single listener subscribes and receives updates") {

      Given("a projection")

      val f = fixture

      f.simpleProjectionActor ! f.simpleListenerProbe.ref
      f.simpleListenerProbe.expectMsg(2 seconds, "test")

      When("listener subscribes")

      f.simpleProjectionActor ! SubscribeForAll("some code", f.simpleListener)

      Then("listener receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvents(stringType, f.aggregateId, Seq(EventInfo(AggregateVersion(1), StringEvent("some string"), UserId(1), Instant.now)))

      Then("listener receives update")

      f.simpleListenerProbe.expectMsg(2 seconds, "some string")
    }

    Scenario("Single listener subscribes and unsubscribes") {
      val f = fixture

      Given("a projection and subscribed listener")

      f.simpleListenerProbe.send(f.simpleProjectionActor, SubscribeForAll("some code", f.simpleListener))
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("listener unsubscribes")

      f.simpleListenerProbe.send(f.simpleListener, f.simpleProjectionActor)
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[ProjectionSubscriptionsCancelled])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvents(stringType, f.aggregateId, Seq(EventInfo(AggregateVersion(1), StringEvent("another string"), UserId(1), Instant.now)))

      Then("listener receives nothing")

      f.simpleListenerProbe.expectNoMessage(2 seconds)
    }

    Scenario("Multiple listeners subscribe and receive updates") {
      Given("a projection")

      val f = fixture

      When("listener one subscribes")

      f.simpleProjectionActor ! SubscribeForAll("some code", f.simpleListener)

      Then("listener one receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvents(stringType, f.aggregateId, Seq(EventInfo(AggregateVersion(1), StringEvent("some string"), UserId(1), Instant.now)))

      Then("only listener one receives update")

      f.simpleListenerProbe.expectMsg(2 seconds, "some string")
      f.secondSimpleListenerProbe.expectNoMessage()

      When("listener two subscribes")

      f.simpleProjectionActor ! SubscribeForAll("second code", f.secondSimpleListener)

      Then("listener two receives subscription id")

      f.secondSimpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvents(stringType, f.aggregateId, Seq(EventInfo(AggregateVersion(2), StringEvent("another string"), UserId(1), Instant.now)))

      Then("both listeners receive update")

      f.simpleListenerProbe.expectMsg(2 seconds, "another string")
      f.secondSimpleListenerProbe.expectMsg(2 seconds, "another string")
    }

    Scenario("Multiple listeners subscribe, some unsubscribe") {
      Given("a projection")

      val f = fixture

      When("listener one subscribes")

      f.simpleProjectionActor ! SubscribeForAll("some code", f.simpleListener)

      Then("listener one receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("listener two subscribes")

      f.simpleProjectionActor ! SubscribeForAll("second code", f.secondSimpleListener)

      Then("listener two receives subscription id")

      f.secondSimpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("listener unsubscribes")

      f.simpleListener ! f.simpleProjectionActor
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[ProjectionSubscriptionsCancelled])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvents(stringType, f.aggregateId, Seq(EventInfo(AggregateVersion(1), StringEvent("another string"), UserId(1), Instant.now)))

      Then("only listener two receives update")

      f.simpleListenerProbe.expectNoMessage(2 seconds)
      f.secondSimpleListenerProbe.expectMsg(2 seconds, "another string")
    }
  }

  Feature("Renewing a subscription") {

    Scenario("F216 a listener that stops loses its subscriptions at once, not at their expiry") {
      val f = fixture
      val code = "stopping " + System.nanoTime()
      f.simpleListenerProbe.send(f.simpleProjectionActor, SubscribeForAll(code, f.simpleListener))
      f.simpleListenerProbe.expectMsg(2 seconds, SubscribedForProjectionUpdates(code))

      actorSystem.stop(f.simpleListener)
      Thread.sleep(300)
      val renewer = TestProbe()
      renewer.send(f.simpleProjectionActor, RenewSubscription(code))
      renewer.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List(code)))
    }

    Scenario("a renewal of a subscription the projection does not know is answered as cancelled, so the listener subscribes again (F133)") {
      val f = fixture
      val renewer = TestProbe()

      Given("a projection that does not hold the subscription: it expired, or the instance that held it stopped")
      renewer.send(f.simpleProjectionActor, RenewSubscription("held by an instance that stopped"))

      Then("the renewer is told it is cancelled")
      renewer.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List("held by an instance that stopped")))

      And("a renewal of a subscription it holds is not answered")
      renewer.send(f.simpleProjectionActor, SubscribeForAll("held", renewer.ref))
      renewer.expectMsg(2 seconds, SubscribedForProjectionUpdates("held"))
      renewer.send(f.simpleProjectionActor, RenewSubscription("held"))
      renewer.expectNoMessage(500 millis)
    }

    Scenario("F241 only a subscription's listener renews or cancels it") {
      val f = fixture
      val code = "own " + System.nanoTime()
      val listener = TestProbe()
      val stranger = TestProbe()
      listener.send(f.simpleProjectionActor, SubscribeForAll(code, listener.ref))
      listener.expectMsg(2 seconds, SubscribedForProjectionUpdates(code))

      When("another sender cancels and renews it")
      stranger.send(f.simpleProjectionActor, CancelProjectionSubscriptions(List(code, "unknown")))
      stranger.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List("unknown")))
      stranger.send(f.simpleProjectionActor, RenewSubscription(code))
      stranger.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List(code)))

      Then("the subscription stays its listener's")
      listener.send(f.simpleProjectionActor, RenewSubscription(code))
      listener.expectNoMessage(500 millis)
      listener.send(f.simpleProjectionActor, CancelProjectionSubscriptions(List(code)))
      listener.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List(code)))
    }
  }
}
