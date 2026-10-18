package io.reactivecqrs.testdomain.spec

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Try

// M7: what the API declares works; what did nothing is gone (F157).
class ApiSurfaceSpec extends AnyFeatureSpec with Matchers {

  Feature("The 1.0 API") {

    Scenario("has none of the classes that did nothing (F157), nor the one that hid scala.Nothing (F165)") {
      Seq("io.reactivecqrs.api.command.OnConcurrentModification", "io.reactivecqrs.api.command.Retry$",
        "io.reactivecqrs.api.command.Fail$", "io.reactivecqrs.api.SimulateEvent", "io.reactivecqrs.api.Nothing", "io.reactivecqrs.api.Nothing$")
        .filter(name => Try(Class.forName(name)).isSuccess) mustBe empty
    }

    Scenario("has none of the classes and methods that neither the library nor Neula uses (F183)") {
      Seq("io.reactivecqrs.api.EventHandler", "io.reactivecqrs.api.AbstractEventHandler", "io.reactivecqrs.api.IdentifiableEvent",
        "io.reactivecqrs.api.EventIdentifier", "io.reactivecqrs.api.EventsIdentifiers", "io.reactivecqrs.api.EventWithIdentifier",
        "io.reactivecqrs.api.id.LRUCache")
        .filter(name => Try(Class.forName(name)).isSuccess) mustBe empty
      classOf[io.reactivecqrs.api.AggregateVersion].getMethods.map(_.getName) must not contain "isJustAfter"
      io.reactivecqrs.api.AggregateVersion.getClass.getMethods.map(_.getName) must not contain "upTo"
    }
  }
}
