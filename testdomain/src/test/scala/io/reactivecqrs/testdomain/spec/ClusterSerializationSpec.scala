package io.reactivecqrs.testdomain.spec

import java.time.Instant

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, AggregateIdWithVersion, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.aggregate.SquashScheduler.{GetSquashStatus, SquashStatus}
import io.reactivecqrs.core.cluster.{ClusterSerialization, ClusterSerializer, RemoteFailure}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus}
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.WaitingInOutbox
import io.reactivecqrs.core.projection.SubscribableProjectionActor._
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuildPosition
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild.BuildRecord
import io.reactivecqrs.core.saga.SagaFailureResponse
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CreateMultipleCarts}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.pattern.ask
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/** Sends back what it is sent. */
class Echo extends Actor {
  override def receive: Receive = { case message => sender() ! message }
}

// D45, D68: every message that crosses nodes round-trips through the cluster serializer.
class ClusterSerializationSpec extends AnyFeatureSpec with Matchers with Eventually with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Millis))

  private def node(): ActorSystem = ActorSystem("serialization", ConfigFactory.parseString(
    """pekko.actor.provider = cluster
      |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
      |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
      |pekko.coordinated-shutdown.exit-jvm = off
      |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      |pekko.loglevel = WARNING""".stripMargin).withFallback(ConfigFactory.load()))

  private val nodeA = node()
  private val nodeB = node()

  override def afterAll(): Unit = Seq(nodeA, nodeB).foreach(system => TestKit.shutdownActorSystem(system, 20.seconds, verifySystemShutdown = false))

  private val serialization = SerializationExtension(nodeA)
  private val ref: ActorRef = TestProbe()(nodeA).ref

  private def roundTrip(value: AnyRef): AnyRef = Serialization.withTransportInformation(nodeA.asInstanceOf[org.apache.pekko.actor.ExtendedActorSystem]) { () =>
    val serializer = serialization.findSerializerFor(value)
    val bytes = serializer.toBinary(value)
    val manifest = org.apache.pekko.serialization.Serializers.manifestFor(serializer, value)
    serialization.deserialize(bytes, serializer.identifier, manifest).get
  }

  private val cart = AggregateType.of[ShoppingCart]
  private val id = AggregateId(7)
  private val v3 = AggregateVersion(3)
  private val now = Instant.parse("2026-10-17T08:00:00.123456Z")
  private val events = Seq(EventInfo[ShoppingCart](AggregateVersion(1), ShoppingCartCreated("cart"), UserId(1), now),
    EventInfo[ShoppingCart](AggregateVersion(2), ItemAdded("apple"), UserId(1), now, Some(EventMetadata.ofUser(UserId(2)))))
  private val state = ShoppingCart("cart", Vector(Item(1, "apple")))

  Feature("Round trips (D45)") {

    Scenario("the messages between the command bus and an owner, and what owners answer") {
      Seq(
        CommandEnvelope(ref, CommandId(5), AddItem(UserId(1), id, v3, "pear")),
        GetState(ref), GetStateMinVersion(ref, v3, 1000), OwnerPassivate, PassivateAggregate(id), ReserveAggregateId,
        GetAggregate(id), GetAggregateMinVersion(id, v3, 1000), GetAggregateForVersion(id, v3), GetAggregateAtInstant(id, now),
        GetEventsForAggregate(id), GetEventsForAggregateForVersion(id, v3), SimulateEvent[ShoppingCart](id, v3, ItemAdded("x")),
        Squash(ref, SquashAggregate(id, v3, UserId(1), Some(EventMetadata.ofUser(UserId(1))))),
        StopOwner, StopOwnerStale, HistoryChanged(Seq(id, AggregateId(8))),
        RepublishOutbox(id), RepublishRange(id, v3, ref), PublishEventsAck(id, Seq(v3), Some(v3)),
        SuccessResponse(id, v3), CustomSuccessResponse(id, v3, ItemReceipt("pear")), FailureResponse(List("no", "never")),
        AggregateConcurrentModificationError(id, cart, v3, AggregateVersion(4)), CommandHandlingError("AddItem", "e-1", CommandId(5)),
        EventHandlingError("ItemAdded", "e-2", CommandId(5)), CommandRejected(id, "full"),
        Success(Aggregate[ShoppingCart](id, v3, Some(state))), Success(events.toList),
        AggregateSquashed(id, v3), SquashRefused(id, "waiting"),
        Rewrite(ref, RewriteEverywhere(id, EraseNote("secret"), UserId(1))), HistoryRewritten(id, 2, 1, Set("notes", "text")), RewriteRefused(id, "D17")
      ).foreach(message => roundTrip(message) mustBe message)
    }

    Scenario("deliveries, acknowledgements and subscriptions between a bus and its subscribers") {
      Seq(
        IdentifiableEvents[ShoppingCart](cart, id, events, startsAtSquash = true),
        AggregateWithType[ShoppingCart](cart, id, AggregateVersion(2), Seq(AggregateVersion(1), AggregateVersion(2)), Some(state)),
        AggregateWithTypeAndEvents[ShoppingCart](cart, id, None, events),
        MessageAck(ref, id, Seq(v3), EventsDelivery), MessageFailed(ref, id, Seq(v3), "broken", AggregatesWithEventsDelivery),
        RequestRepublish(ref, cart, id, v3), Republish(cart, id),
        GetSubscriptionRequests(ref), SubscriptionRequests(Seq(SubscribeForEvents(cart, ref), SubscribeForAggregates(cart, ref)), ref),
        AddSubscriptions(Seq(SubscribeForAggregatesWithEvents(cart, ref))), RemoveSubscriber(ref, keepOutbox = true), SubscriberRemoved(ref),
        GetDeliveryStatus, DeliveryStatus(1, Seq(Undelivered(cart, id, v3, "projection", AggregatesDelivery, now)), Seq("stopped"))
      ).foreach(message => roundTrip(message) mustBe message)
    }

    Scenario("the questions and answers of projections, builds, the poller and the squash scheduler") {
      Seq(
        GetProjectionStatus, GetListeners, Listeners(Seq((cart, SubscriptionType.EVENTS))),
        ProjectionStatus("carts", Seq(SubscriptionFailure(SubscriptionType.EVENTS, cart, id, v3, "broken", now)), Seq(id), Nil),
        RetryFailedSubscription(id), RetryAllFailedSubscriptions, FailedSubscriptionsRetried(2), SkipFailedVersion(id), FailedSubscriptionsHandled(id, 1),
        RebuildProjection, RebuildStarted(2), RebuildRefused("busy"), RebuildAggregates(RebuildTarget.OneSpace(SpaceId(4))),
        AggregatesRebuilt(RebuildTarget.OneAggregate(id), 1), ToBuildInProgress(RetryAllFailedSubscriptions), GetBuilds,
        Builds("carts", Some(BuildRecord("carts", 1, 2, "_v2", "projection:carts_v2", Map("Cart" -> 1), BuildPosition(bulkDone = false, 99L))), None, caughtUp = true),
        GetOutboxStatus, OutboxStatus(3, Seq(WaitingInOutbox(cart, id, v3, now))), GetSquashStatus, SquashStatus(1, 2, 3),
        SubscribedForProjectionUpdates("s-1"), CancelProjectionSubscriptions(List("s-1")), ProjectionSubscriptionsCancelled(List("s-1")),
        RenewSubscription("s-1"), SubscriptionUpdated("s-1", state)
      ).foreach(message => roundTrip(message) mustBe message)
      roundTrip(SubscriptionType.AGGREGATES) mustBe SubscriptionType.AGGREGATES
    }

    Scenario("the application's commands, events, states, saga orders and responses, and query answers") {
      Seq(
        AddItem(UserId(1), id, v3, "pear"), ItemAdded("pear"), ShoppingCartCreated("cart"), state,
        CreateMultipleCarts(UserId(1), "cart", 3), CartsCreated(List(AggregateIdWithVersion(id, v3))), SagaFailureResponse(List("no")),
        Vector(state, ShoppingCart("other", Vector())), List("a", "b"), Map("a" -> 1, "b" -> 2), Set(1L, 2L), (id, "pair"), (1, 2),
        Some(state), Some(Vector(Some(1), None))
      ).foreach(message => roundTrip(message) mustBe message)
    }

    Scenario("the library's exceptions keep their class and fields; any other becomes a RemoteFailure") {
      roundTrip(Failure(new AggregateInIncorrectVersionException(id, cart, v3, AggregateVersion(9)))) match {
        case Failure(e: AggregateInIncorrectVersionException) => (e.aggregateId, e.currentVersion, e.requestedVersion) mustBe ((id, v3, AggregateVersion(9)))
        case other => fail("Not rebuilt: " + other)
      }
      roundTrip(Failure(new AggregateRestoreFailedException(id, cart, new IllegalStateException("bad row")))) match {
        case Failure(e: AggregateRestoreFailedException) =>
          e.getMessage must include("bad row")
          e.getCause match {
            case cause: RemoteFailure => (cause.className, cause.getMessage) mustBe (("java.lang.IllegalStateException", "bad row"))
            case other => fail("Cause not kept: " + other)
          }
        case other => fail("Not rebuilt: " + other)
      }
      roundTrip(Status.Failure(new AggregateInIncorrectVersionException(id, cart, v3, AggregateVersion(9)))) match {
        case Status.Failure(e: AggregateInIncorrectVersionException) => e.requestedVersion mustBe AggregateVersion(9)
        case other => fail("Not rebuilt: " + other)
      }
      roundTrip(Failure(new AggregateSquashedException(id, v3, AggregateVersion(1), Some(now)))) match {
        case Failure(e: AggregateSquashedException) => (e.squashedUpTo, e.at) mustBe ((v3, Some(now)))
        case other => fail("Not rebuilt: " + other)
      }
      roundTrip(Failure(new IllegalArgumentException("odd"))) match {
        case Failure(e: RemoteFailure) =>
          (e.className, e.getMessage) mustBe (("java.lang.IllegalArgumentException", "odd"))
          roundTrip(e) match { // passed on to a third node, it stays the same
            case again: RemoteFailure => (again.className, again.getMessage) mustBe ((e.className, e.getMessage))
            case other => fail("Not a remote failure: " + other)
          }
        case other => fail("Not a remote failure: " + other)
      }
      roundTrip(Failure(new IllegalStateException())) match {
        case Failure(e: RemoteFailure) => e.getMessage mustBe null
        case other => fail("Not a remote failure: " + other)
      }
    }

    Scenario("the library's and the application's messages go to the cluster serializer; Some and None stay Pekko's") {
      serialization.findSerializerFor(GetState(ref)) mustBe a[ClusterSerializer]
      serialization.findSerializerFor(ItemAdded("x")) mustBe a[ClusterSerializer]
      serialization.findSerializerFor(Vector(1)) mustBe a[ClusterSerializer]
      serialization.findSerializerFor(Some(1)) must not be a[ClusterSerializer]
    }
  }

  Feature("Tolerant reading (D68)") {

    Scenario("a field the reader does not know is skipped; one it misses reads as null") {
      val json = ClusterSerialization(nodeA).mpjsons
      json.deserialize[Item]("""{"id":1,"name":"apple","colour":"red"}""", classOf[Item].getName) mustBe Item(1, "apple")
      json.deserialize[Item]("""{"id":1}""", classOf[Item].getName) mustBe Item(1, null)
    }
  }

  Feature("Between nodes") {

    Scenario("messages with events, states and refs go to another node and back, and a ref still reaches its actor") {
      Cluster(nodeA).join(Cluster(nodeA).selfAddress)
      Cluster(nodeB).join(Cluster(nodeA).selfAddress)
      eventually(Cluster(nodeB).selfMember.status mustBe MemberStatus.Up)
      nodeB.actorOf(Props(new Echo), "echo")
      val echo = Await.result(nodeA.actorSelection(Cluster(nodeB).selfAddress.toString + "/user/echo").resolveOne(10.seconds), 10.seconds)
      implicit val timeout: Timeout = Timeout(10.seconds)
      val probe = TestProbe()(nodeA)

      Seq[AnyRef](
        IdentifiableEvents[ShoppingCart](cart, id, events),
        CommandEnvelope(probe.ref, CommandId(5), AddItem(UserId(1), id, v3, "pear")),
        Success(Aggregate[ShoppingCart](id, v3, Some(state)))
      ).foreach(message => Await.result(echo ? message, 10.seconds) mustBe message)

      Await.result(echo ? CommandEnvelope(probe.ref, CommandId(5), "x"), 10.seconds) match {
        case CommandEnvelope(respondTo, _, _, _) =>
          respondTo ! "reached"
          probe.expectMsg(10.seconds, "reached")
        case other => fail("Unexpected: " + other)
      }
    }
  }
}
