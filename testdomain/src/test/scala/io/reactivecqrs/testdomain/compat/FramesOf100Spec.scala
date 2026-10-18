package io.reactivecqrs.testdomain.compat

import java.nio.charset.StandardCharsets
import java.time._

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, AggregateIdWithVersion, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.delivery.RepublishOutbox
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.projection.SubscribableProjectionActor._
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.saga.SagaFailureResponse
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CreateMultipleCarts}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.util.{Failure, Success}

// A node of this version reads what a node of 1.0.0 sends during a rolling upgrade: the frames in
// resources/compat/1.0.0 were written by 1.0.0's own code (docs/compat/WriteFramesOf100.scala).
class FramesOf100Spec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem("frames", ConfigFactory.parseString(
    "pekko.actor.provider = cluster\npekko.remote.artery.canonical { hostname = \"127.0.0.1\", port = 0 }").withFallback(ConfigFactory.load()))

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def resource(name: String): Array[Byte] = {
    val in = getClass.getResourceAsStream("/compat/1.0.0/" + name)
    try in.readAllBytes() finally in.close()
  }

  // Name, then the serializer's id and the manifest, or "unsendable" and the error.
  private val index: Seq[(String, String, String)] = new String(resource("index.tsv"), StandardCharsets.UTF_8).split("\n").toSeq
    .filter(_.nonEmpty).map(_.split("\t")).map(row => (row(0), row(1), row.lift(2).getOrElse("")))

  private def read(name: String, serializerId: String, manifest: String): AnyRef =
    Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
      SerializationExtension(system).deserialize(resource(name + ".bin"), serializerId.toInt, manifest).get
    }

  private val cart = AggregateType.of[ShoppingCart]
  private val id = AggregateId(7)
  private val v3 = AggregateVersion(3)
  private val now = Instant.parse("2026-10-17T08:00:00.123456Z")
  private val events = Seq(EventInfo[ShoppingCart](AggregateVersion(1), ShoppingCartCreated("cart"), UserId(1), now),
    EventInfo[ShoppingCart](AggregateVersion(2), ItemAdded("apple"), UserId(1), now, Some(EventMetadata.ofUser(UserId(2)))))
  private val state = ShoppingCart("cart", Vector(Item(1, "apple")))

  // What each frame must read as here; where a message changed since 1.0.0, as D68 reads its old form.
  private val expected: Map[String, AnyRef] = Map(
    "command" -> AddItem(UserId(1), id, v3, "pear"),
    "getAggregate" -> GetAggregate(id), "getAggregateMinVersion" -> GetAggregateMinVersion(id, v3, 1000),
    "getAggregateAtInstant" -> GetAggregateAtInstant(id, now), "getEventsForVersion" -> GetEventsForAggregateForVersion(id, v3),
    "stopOwner" -> StopOwner, "historyChanged" -> HistoryChanged(Seq(id, AggregateId(8))), "passivate" -> PassivateAggregate(id),
    "republishOutbox" -> RepublishOutbox(id), "publishAck" -> PublishEventsAck(id, Seq(v3), Some(v3)),
    "success" -> SuccessResponse(id, v3), "customSuccess" -> CustomSuccessResponse(id, v3, ItemReceipt("pear")),
    "failure" -> FailureResponse(List("no", "never")), "conflict" -> AggregateConcurrentModificationError(id, cart, v3, AggregateVersion(4)),
    "handlingError" -> CommandHandlingError("AddItem", "e-1", CommandId(5)), "rejected" -> CommandRejected(id, "full"),
    "aggregate" -> Success(Aggregate[ShoppingCart](id, v3, Some(state))), "eventList" -> Success(events.toList),
    "squashed" -> AggregateSquashed(id, v3), "rewritten" -> HistoryRewritten(id, 2, 1, Set("notes", "text")),
    "materialized" -> Materialized(AggregateId(-5), id), "materializeRefused" -> MaterializeRefused(AggregateId(-5), "no"),
    // F209 added the start to the key: a 1.0.0 frame reads without it
    "storeFirstEvent" -> StoreFirstEvent[ShoppingCart](UserId(1), CartSquashed(SpaceId(0), "cart", Vector(Item(1, "apple"))), Some(MaterializationKey(AggregateId(-5), null))),
    "identifiableEvents" -> IdentifiableEvents[ShoppingCart](cart, id, events, startsAtSquash = true),
    "aggregateWithType" -> AggregateWithType[ShoppingCart](cart, id, AggregateVersion(2), Seq(AggregateVersion(1), AggregateVersion(2)), Some(state)),
    "aggregateWithEvents" -> AggregateWithTypeAndEvents[ShoppingCart](cart, id, None, events),
    "republish" -> Republish(cart, id), "deliveryStatusQuestion" -> GetDeliveryStatus,
    "projectionStatusQuestion" -> GetProjectionStatus, "listeners" -> Listeners(Seq((cart, SubscriptionType.EVENTS))),
    "rebuildAggregates" -> RebuildAggregates(RebuildTarget.OneSpace(SpaceId(4))), "aggregatesRebuilt" -> AggregatesRebuilt(RebuildTarget.OneAggregate(id), 1),
    "subscribed" -> SubscribedForProjectionUpdates("s-1"), "cancel" -> CancelProjectionSubscriptions(List("s-1")),
    "renew" -> RenewSubscription("s-1"), "subscriptionUpdated" -> SubscriptionUpdated("s-1", state),
    "itemAdded" -> ItemAdded("pear"), "state" -> state, "sagaOrder" -> CreateMultipleCarts(UserId(1), "cart", 3),
    "sagaResponse" -> CartsCreated(List(AggregateIdWithVersion(id, v3))), "sagaFailure" -> SagaFailureResponse(List("no")),
    "vector" -> Vector(state, ShoppingCart("other", Vector())), "map" -> Map("a" -> 1, "b" -> 2), "set" -> Set(1L, 2L),
    "tuple" -> (id, "pair"), "someState" -> Some(state), "nested" -> Some(Vector(Some(1), None)),
    "localDate" -> LocalDate.of(2026, 10, 18), "localDateTime" -> LocalDateTime.of(2026, 10, 18, 22, 45),
    "duration" -> java.time.Duration.ofMinutes(90), "zoned" -> ZonedDateTime.of(2026, 10, 18, 22, 45, 0, 0, ZoneOffset.UTC),
    "bigInt" -> BigInt("12345678901234567890"), "javaBigDecimal" -> new java.math.BigDecimal("1.25"),
    "dateList" -> List(LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2))
  )

  Feature("What a node of 1.0.0 sends") {

    Scenario("every frame is read as the message it was (F314: java.time values sent on their own too)") {
      val sendable = index.filterNot(_._2 == "unsendable")
      sendable.map(_._1).filterNot(_ == "incorrectVersion").toSet mustBe expected.keySet
      sendable.filterNot(_._1 == "incorrectVersion").foreach { case (name, serializerId, manifest) =>
        withClue(name + ": ")(Option(read(name, serializerId, manifest)) mustBe Some(expected(name)))
      }
      sendable.find(_._1 == "incorrectVersion").foreach { case (name, serializerId, manifest) =>
        read(name, serializerId, manifest) match {
          case Failure(e: AggregateInIncorrectVersionException) => (e.aggregateId, e.currentVersion, e.requestedVersion) mustBe ((id, v3, AggregateVersion(9)))
          case other => fail("Not rebuilt: " + other)
        }
      }
    }

    Scenario("1.0.0 could not send enums, arrays or FiniteDuration on their own, so none of those is in flight") {
      index.filter(_._2 == "unsendable").map(_._1).toSet mustBe Set("dayOfWeek", "intArray", "stringArray", "finiteDuration")
    }
  }
}
