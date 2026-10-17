package io.reactivecqrs.testdomain.spec

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Try

// M7: what the API declares works; what did nothing is gone (F157).
class ApiSurfaceSpec extends AnyFeatureSpec with Matchers {

  Feature("The 1.0 API") {

    Scenario("has none of the classes that did nothing (F157)") {
      Seq("io.reactivecqrs.api.command.OnConcurrentModification", "io.reactivecqrs.api.command.Retry$",
        "io.reactivecqrs.api.command.Fail$", "io.reactivecqrs.api.SimulateEvent")
        .filter(name => Try(Class.forName(name)).isSuccess) mustBe empty
    }
  }
}
