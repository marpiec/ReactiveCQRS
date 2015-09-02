package io.reactivecqrs.core.projection

import java.time.Clock

import akka.actor.{Actor, Props, ActorSystem, ActorRef}
import akka.event.LoggingReceive
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.Timeout
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.IdentifiableEvent
import io.reactivecqrs.core.eventbus.EventsBusActor.SubscribedForEvents
import io.reactivecqrs.core.projection.Subscribable.{SubscribeForAggregateInfoUpdates, CancelInfoSubscriptions, InfoSubscriptionsCanceled, SubscribedForAggregateInfo}
import org.scalatest.{BeforeAndAfter, GivenWhenThen, FeatureSpecLike}
import scala.concurrent.duration._

case class StringEvent(aggregate: String) extends Event[String]

class SimpleProjection(val eventBusActor: ActorRef) extends ProjectionActor with Subscribable[String] {
  override protected def receiveQuery: Receive = LoggingReceive {
    case ref: ActorRef => ref ! "test"
  }

  override protected val listeners: List[Listener[Any]] = List(EventListener(handleUpdate))

  private def handleUpdate(aggregateId: AggregateId, version: AggregateVersion, event: Event[String]) = {
    sendUpdate(aggregateId, event.asInstanceOf[StringEvent].aggregate)
  }
}


class SubscribableSpec extends FeatureSpecLike with GivenWhenThen with BeforeAndAfter {
  implicit val actorSystem = ActorSystem()
  implicit val timeout = Timeout(1 second)

  val clock = Clock.systemDefaultZone()

  def fixture = new {
    val eventBusActorStub = actorSystem.actorOf(Props(new Actor {
      override def receive: Receive = {case _ => ()}
    }))

    val simpleProjectionActor = TestActorRef(Props(new SimpleProjection(eventBusActorStub)))

    simpleProjectionActor ! SubscribedForEvents("someMessageId", AggregateType(classOf[String].getSimpleName), "someId1")


    // use this actor/probe combo for unsubscribing scenarios - the actor is necessary to store subscription id
    val simpleListenerProbe = TestProbe()

    val simpleListener = actorSystem.actorOf(Props(new Actor {
      private var subscriptionId: Option[String] = None
      override def receive: Receive = {
        case SubscribedForAggregateInfo(code, id) => {
          subscriptionId = Some(id)
          simpleListenerProbe.ref ! SubscribedForAggregateInfo(code, id)
        }
        case InfoSubscriptionsCanceled(id :: Nil) => {
          if (subscriptionId.get == id) {
            subscriptionId = None
          }
          simpleListenerProbe.ref ! InfoSubscriptionsCanceled(id :: Nil)
        }
        case message: String => simpleListenerProbe.ref ! message
        case actor: ActorRef => actor ! CancelInfoSubscriptions(List(subscriptionId.get))
      }
    }))

    // this is a simple probe - use for no-unsubscribe scenarios
    val listenerProbe = TestProbe()
  }

  val stringType = AggregateType(classOf[String].getSimpleName)

  feature("Can subscribe to events") {

    scenario("Single listener subscribes and receives updates") {

      Given("a projection")

      val f = fixture

      f.simpleProjectionActor ! f.simpleListenerProbe.ref
      f.simpleListenerProbe.expectMsg(2 seconds, "test")

      When("listener subscribes")

      f.simpleProjectionActor ! SubscribeForAggregateInfoUpdates("some code", f.simpleListenerProbe.ref, AggregateId(0))

      Then("listener receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("some string"))

      Then("listener receives update")

      f.simpleListenerProbe.expectMsg(2 seconds, "some string")
    }

    scenario("Single listener subscribes and unsubscribes") {
      val f = fixture

      Given("a projection and subscribed listener")

      f.simpleListenerProbe.send(f.simpleProjectionActor, SubscribeForAggregateInfoUpdates("some code", f.simpleListener, AggregateId(0)))
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("listener unsubscribes")

      f.simpleListenerProbe.send(f.simpleListener, f.simpleProjectionActor)
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[InfoSubscriptionsCanceled])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"))

      Then("listener receives nothing")

      f.simpleListenerProbe.expectNoMsg(2 seconds)
    }

    scenario("Multiple listeners subscribe and receive updates") {
      Given("a projection")

      val f = fixture

      When("listener one subscribes")

      f.simpleProjectionActor ! SubscribeForAggregateInfoUpdates("some code", f.simpleListenerProbe.ref, AggregateId(0))

      Then("listener one receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("some string"))

      Then("only listener one receives update")

      f.simpleListenerProbe.expectMsg(2 seconds, "some string")
      f.listenerProbe.expectNoMsg()

      When("listener two subscribes")

      f.simpleProjectionActor ! SubscribeForAggregateInfoUpdates("some code", f.listenerProbe.ref, AggregateId(0))

      Then("listener two receives subscription id")

      f.listenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"))

      Then("both listeners receive update")

      f.simpleListenerProbe.expectMsg(2 seconds, "another string")
      f.listenerProbe.expectMsg(2 seconds, "another string")
    }

    scenario("Multiple listeners subscribe, some unsubscribe") {
      Given("a projection")

      val f = fixture

      When("listener one subscribes")

      f.simpleProjectionActor ! SubscribeForAggregateInfoUpdates("some code", f.simpleListener, AggregateId(0))

      Then("listener one receives subscription id")

      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("listener two subscribes")

      f.simpleProjectionActor ! SubscribeForAggregateInfoUpdates("some code", f.listenerProbe.ref, AggregateId(0))

      Then("listener two receives subscription id")

      f.listenerProbe.expectMsgAnyClassOf(2 seconds, classOf[SubscribedForAggregateInfo])

      When("listener unsubscribes")

      f.simpleListener ! f.simpleProjectionActor
      f.simpleListenerProbe.expectMsgAnyClassOf(2 seconds, classOf[InfoSubscriptionsCanceled])

      When("projection is updated")

      f.simpleProjectionActor ! IdentifiableEvent(stringType, AggregateId(0), AggregateVersion(1), StringEvent("another string"))

      Then("only listener two receives update")

      f.simpleListenerProbe.expectNoMsg(2 seconds)
      f.listenerProbe.expectMsg(2 seconds, "another string")
    }
  }
}
