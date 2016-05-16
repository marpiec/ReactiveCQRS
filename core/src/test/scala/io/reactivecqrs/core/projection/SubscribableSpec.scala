package io.reactivecqrs.core.projection

import java.time.Instant

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Timeout
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}
import io.reactivecqrs.core.aggregaterepository.IdentifiableEvent
import io.reactivecqrs.core.documentstore.NothingMetadata
import io.reactivecqrs.core.eventbus.EventsBusActor.SubscribedForEvents
import io.reactivecqrs.core.projection.Subscribable.{CancelProjectionSubscriptions, ProjectionSubscriptionsCancelled, SubscribedForProjectionUpdates, SubscriptionUpdated}
import org.scalatest.{BeforeAndAfter, FeatureSpecLike, GivenWhenThen}

import scala.concurrent.duration._


case class StringEvent(aggregate: String) extends Event[String]

case class SubscribeForAll(subscriptionCode: String, listener: ActorRef)

class SimpleProjection(val eventBusActor: ActorRef) extends ProjectionActor with Subscribable {

  override def receiveSubscriptionRequest: Receive = {
    case SubscribeForAll(code, listener) => handleSubscribe(code, listener, (s: String) => Some((s, NothingMetadata())))
  }

  override protected def receiveQuery: Receive = {
    case ref: ActorRef => ref ! "test"
  }

  override protected val listeners: List[Listener[Any]] = List(EventListener(handleUpdate))

  private def handleUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[String], userId: UserId, instant: Instant) = {
    sendUpdate(event.asInstanceOf[StringEvent].aggregate)
  }

}

class SimpleListener(simpleListenerProbe: TestProbe) extends Actor {
  private var subscriptionId: Option[String] = None
  override def receive: Receive = {
    case SubscribedForProjectionUpdates(code, id) => {
      subscriptionId = Some(id)
      simpleListenerProbe.ref ! SubscribedForProjectionUpdates(code, id)
    }
    case ProjectionSubscriptionsCancelled(id :: Nil) => {
      if (subscriptionId.get == id) {
        subscriptionId = None
      }
      simpleListenerProbe.ref ! ProjectionSubscriptionsCancelled(id :: Nil)
    }
    case SubscriptionUpdated(id, data, metadata) => simpleListenerProbe.ref ! data // TODO validate subscription id?
    case actor: ActorRef => actor ! CancelProjectionSubscriptions(List(subscriptionId.get))
  }
}

class SubscribableSpec extends FeatureSpecLike with GivenWhenThen with BeforeAndAfter {
  implicit val actorSystem = ActorSystem()
  implicit val timeout = Timeout(1 second)

  def fixture = new {
    val eventBusActorStub = actorSystem.actorOf(Props(new Actor {
      override def receive: Receive = { case _ => () }
    }))

    val simpleProjectionActor = TestActorRef(Props(new SimpleProjection(eventBusActorStub)))

    simpleProjectionActor ! SubscribedForEvents("someMessageId", AggregateType(classOf[String].getSimpleName), "someId1")


    // use this actor/probe combo - the actor is necessary to store subscription id
    val simpleListenerProbe = TestProbe()
    val simpleListener = actorSystem.actorOf(Props(new SimpleListener(simpleListenerProbe)))

    val secondSimpleListenerProbe = TestProbe()
    val secondSimpleListener = actorSystem.actorOf(Props(new SimpleListener(secondSimpleListenerProbe)))
  }

  val stringType = AggregateType(classOf[String].getSimpleName)

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

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("some string"), UserId(1), Instant.now)

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

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"), UserId(1), Instant.now)

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

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("some string"), UserId(1), Instant.now)

      Then("only listener one receives update")

      f.simpleListenerProbe.expectMsg(2 seconds, "some string")
      f.secondSimpleListenerProbe.expectNoMsg()

      When("listener two subscribes")

      f.simpleProjectionActor ! SubscribeForAll("some code", f.secondSimpleListener)

      Then("listener two receives subscription id")

      f.secondSimpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"), UserId(1), Instant.now)

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

      f.simpleProjectionActor ! SubscribeForAll("some code", f.secondSimpleListener)

      Then("listener two receives subscription id")

      f.secondSimpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForProjectionUpdates])

      When("listener unsubscribes")

      f.simpleListener ! f.simpleProjectionActor
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[ProjectionSubscriptionsCancelled])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"), UserId(1), Instant.now)

      Then("only listener two receives update")

      f.simpleListenerProbe.expectNoMsg(2 seconds)
      f.secondSimpleListenerProbe.expectMsg(2 seconds, "another string")
    }
  }
}
