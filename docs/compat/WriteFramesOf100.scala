package io.reactivecqrs.testdomain.compat

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time._

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, AggregateIdWithVersion, CommandId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.projection.SubscribableProjectionActor._
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.saga.SagaFailureResponse
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CreateMultipleCarts}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension, Serializers}
import org.apache.pekko.testkit.TestKit
import org.scalatest.featurespec.AnyFeatureSpec

import scala.concurrent.duration._
import scala.util.{Failure, Success}

// Writes the frames a 1.0.0 node sends, for the compatibility spec of later versions (testdomain/.../compat).
class WriteFramesOf100 extends AnyFeatureSpec {

  Scenario("write the frames") {
    val system = ActorSystem("frames", ConfigFactory.parseString(
      "pekko.actor.provider = cluster\npekko.remote.artery.canonical { hostname = \"127.0.0.1\", port = 0 }").withFallback(ConfigFactory.load()))
    try {
      val cart = AggregateType.of[ShoppingCart]
      val id = AggregateId(7)
      val v3 = AggregateVersion(3)
      val now = Instant.parse("2026-10-17T08:00:00.123456Z")
      val events = Seq(EventInfo[ShoppingCart](AggregateVersion(1), ShoppingCartCreated("cart"), UserId(1), now),
        EventInfo[ShoppingCart](AggregateVersion(2), ItemAdded("apple"), UserId(1), now, Some(EventMetadata.ofUser(UserId(2)))))
      val state = ShoppingCart("cart", Vector(Item(1, "apple")))
      val catalogue: Seq[(String, AnyRef)] = Seq(
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
        "storeFirstEvent" -> StoreFirstEvent[ShoppingCart](UserId(1), CartSquashed(SpaceId(0), "cart", Vector(Item(1, "apple"))), Some(MaterializationKey(AggregateId(-5)))),
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
        "incorrectVersion" -> Failure(new AggregateInIncorrectVersionException(id, cart, v3, AggregateVersion(9))),
        // values sent on their own: an answer, a query result
        "localDate" -> LocalDate.of(2026, 10, 18), "localDateTime" -> LocalDateTime.of(2026, 10, 18, 22, 45),
        "duration" -> java.time.Duration.ofMinutes(90), "zoned" -> ZonedDateTime.of(2026, 10, 18, 22, 45, 0, 0, ZoneOffset.UTC),
        "dayOfWeek" -> DayOfWeek.SUNDAY, "intArray" -> Array(1, 2, 3), "stringArray" -> Array("a", "b"),
        "finiteDuration" -> 5.seconds, "bigInt" -> BigInt("12345678901234567890"), "javaBigDecimal" -> new java.math.BigDecimal("1.25"),
        "dateList" -> List(LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2))
      )
      val serialization = SerializationExtension(system)
      val dir = Paths.get(sys.props("frames.out"))
      Files.createDirectories(dir)
      val index = new StringBuilder
      Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
        catalogue.foreach { case (name, message) =>
          val serializer = serialization.findSerializerFor(message)
          scala.util.Try(serializer.toBinary(message)) match {
            case Success(bytes) =>
              Files.write(dir.resolve(name + ".bin"), bytes)
              index.append(name).append('\t').append(serializer.identifier).append('\t').append(Serializers.manifestFor(serializer, message)).append('\n')
            case Failure(e) => index.append(name).append("\tunsendable\t").append(e.getClass.getName).append('\n')
          }
        }
      }
      Files.write(dir.resolve("index.tsv"), index.toString.getBytes(StandardCharsets.UTF_8))
    } finally TestKit.shutdownActorSystem(system)
  }
}
