package io.reactivecqrs.core

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.annotation.nowarn

// F165, L4: code written for 0.12 says Nothing for the library's response information after `import io.reactivecqrs.api._`;
// it keeps meaning what it meant (with a deprecation warning at each use), not scala.Nothing.
@nowarn("cat=deprecation")
class OldNothingSpec extends AnyFeatureSpec with Matchers {

  Feature("Nothing, the name of NoInfo before 1.0") {

    Scenario("is NoInfo: its value, its type in a response, and a pattern on it") {
      val info: Nothing = Nothing.empty
      info mustBe NoInfo()
      val response: CustomCommandResponse[Nothing] = CustomSuccessResponse(AggregateId(1), AggregateVersion(1), Nothing())
      (response match {
        case CustomSuccessResponse(_, _, _: Nothing) => "matched"
        case _ => "not matched"
      }) mustBe "matched"
    }
  }
}
