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
import io.reactivecqrs.testutils.TestDatabase
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

  private def handleUpdate(aggregateId: AggregateId, events: Seq[EventInfo[String]]) = { implicit session: DBSession =>
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
  implicit val actorSystem = ActorSystem()
  implicit val timeout = Timeout(10 second)

  def fixture = new {


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

  val stringType = AggregateType.of[String]

  feature("Can subscribe to all events") {

    scenario("Single listener subscribes and receives updates") {

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

    scenario("Single listener subscribes and unsubscribes") {
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

      f.simpleListenerProbe.expectNoMsg(2 seconds)
    }

    scenario("Multiple listeners subscribe and receive updates") {
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
      f.secondSimpleListenerProbe.expectNoMsg()

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

    scenario("Multiple listeners subscribe, some unsubscribe") {
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

      f.simpleListenerProbe.expectNoMsg(2 seconds)
      f.secondSimpleListenerProbe.expectMsg(2 seconds, "another string")
    }
  }

  feature("Renewing a subscription") {

    scenario("a renewal of a subscription the projection does not know is answered as cancelled, so the listener subscribes again (F133)") {
      val f = fixture
      val renewer = TestProbe()

      Given("a projection that does not hold the subscription: it expired, or the instance that held it stopped")
      renewer.send(f.simpleProjectionActor, RenewSubscription("held by an instance that stopped"))

      Then("the renewer is told it is cancelled")
      renewer.expectMsg(2 seconds, ProjectionSubscriptionsCancelled(List("held by an instance that stopped")))

      And("a renewal of a subscription it holds is not answered")
      f.simpleListenerProbe.send(f.simpleProjectionActor, SubscribeForAll("held", f.simpleListener))
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])
      renewer.send(f.simpleProjectionActor, RenewSubscription("held"))
      renewer.expectNoMessage(500 millis)
    }
  }
}
