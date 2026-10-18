package io.reactivecqrs.examples.e10

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, RebuildTarget}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.examples.e1.{ShoppingCart, ShoppingCartContext}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestSystems}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

// E10 as its main runs it, against the test database; each release in an actor system of its own. Then chapter 10's
// messages to the projection's supervisor, each a region followed by the lines that check its answer.
class RebuildSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("E10: rebuilding projections") {

    Scenario("version 2 is built while version 1 serves and takes over; a damaged document is repaired by a targeted rebuild") {
      val release = RebuildSetup.release(() => TestSystems.newSystem("e10"), UserId(1))

      release.before mustBe Seq(Some("Groceries"), Some("Garden"), Some("Gifts"))
      (release.switched.active.map(b => (b.version, b.suffix)), release.switched.building) mustBe ((Some((2, "_v2")), None))
      release.after mustBe Seq(Some("GROCERIES"), Some("GARDEN"), Some("GIFTS"))
      release.damaged mustBe Some("???")
      release.rebuilt.target mustBe a[RebuildTarget.OneAggregate]
      release.rebuilt.aggregates mustBe 1
      release.repaired mustBe Some("GARDEN")
    }

    Scenario("the supervisor answers about its builds, rebuilds on request and in place, and the projection retires") {
      val app = TestSystems.newSystem("e10-messages")
      try {
        val shop = RebuildSetup.start(app, version = 2)
        val cartNames = shop.cartNames
        val cartId = RebuildSetup.create(shop, UserId(1), "Groceries")
        val serving = RebuildSetup.switchedTo(shop, version = 2, timeout.duration.fromNow)

        // #getBuilds
        import io.reactivecqrs.core.rebuild.ProjectionSupervisor._

        val builds = (cartNames ? GetBuilds).mapTo[Builds]
        // while it builds:  active = build 1, building = build 2
        // after the switch: active = build 2, building = None
        // #getBuilds
        Await.result(builds, timeout.duration).building mustBe None

        // #buildStatus
        import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, GetBuildStatus}

        val buildStatus = (cartNames ? GetBuildStatus).mapTo[BuildStatus]
        // BuildStatus("Bulk", 182044, 91000, 0, 0, 0): reading the store, 91 000 events sent so far
        // #buildStatus
        Await.result(buildStatus, timeout.duration) mustBe BuildStatus.NoBuild // no build runs now

        // #rebuild
        val started = cartNames ? RebuildProjection
        // answers RebuildStarted(2): build 2 of version 1, in tables with the suffix "_v1_b2"
        // #rebuild
        // #toBuild
        import io.reactivecqrs.core.projection.{GetProjectionStatus, RetryAllFailedSubscriptions, SkipFailedVersion}

        cartNames ? ToBuildInProgress(GetProjectionStatus)          // the build's ProjectionStatus
        cartNames ? ToBuildInProgress(RetryAllFailedSubscriptions)  // FailedSubscriptionsRetried(count)
        cartNames ? ToBuildInProgress(SkipFailedVersion(cartId))    // FailedSubscriptionsHandled(cartId, count)
        // #toBuild
        val build = serving.active.get.number + 1
        Await.result(started, timeout.duration) mustBe RebuildStarted(build)
        RebuildSetup.switchedTo(shop, version = 2, timeout.duration.fromNow).active.map(b => (b.number, b.suffix)) mustBe
          Some((build, "_v2_b" + build))

        // #oneCart
        val one = cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId))
        // answers AggregatesRebuilt(RebuildTarget.OneAggregate(cartId), 1)
        // #oneCart
        Await.result(one, timeout.duration) mustBe AggregatesRebuilt(RebuildTarget.OneAggregate(cartId), 1)

        // #everyCart
        import io.reactivecqrs.api.AggregateType

        val everyCart = cartNames ? RebuildAggregates(RebuildTarget.OneType(AggregateType.of[ShoppingCart]))
        // #everyCart
        Await.result(everyCart, timeout.duration) must matchPattern { case AggregatesRebuilt(RebuildTarget.OneType(_), n) if n >= 1 => }

        // #organization
        import io.reactivecqrs.api.id.SpaceId

        val organization = cartNames ? RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))
        // #organization
        Await.result(organization, timeout.duration) mustBe AggregatesRebuilt(RebuildTarget.OneSpace(SpaceId(7)), 0)
        RebuildSetup.name(shop, cartId) mustBe Some("GROCERIES")
      } finally Await.ready(app.terminate(), 30.seconds)

      Then("the application without the projection retires it at its start")
      val later = TestSystems.newSystem("e10-retire")
      try {
        val mpjsons = new MPJsons
        val typesNames = new PostgresTypesNamesState().initSchema()
        val registry = new PostgresProjectionRegistry().initSchema()
        val projections = new Projections(later, ProjectionsSettings(new PostgresEventStoreState(mpjsons, typesNames).initSchema(),
          new PostgresSubscriptionsState(typesNames).initSchema(), registry, contexts = Seq(new ShoppingCartContext)))
        // #retire
        projections.retire("e10-cart-names")
        // #retire
        registry.builds("e10-cart-names") mustBe empty
      } finally Await.ready(later.terminate(), 30.seconds)
    }
  }
}
