package io.reactivecqrs.testkit

import org.apache.pekko.testkit.TestKit
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

/** The library's own specs: a feature spec on an actor system of its own, a cluster of one when the tests run as one
  * (`TestSystems`). Test-scoped: the published test kit depends on no test framework. */
abstract class CommonSpec extends TestKit(TestSystems.newSystem()) with AnyFeatureSpecLike with GivenWhenThen with ActorAskSupport with Matchers {

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = false,
    singleLineMode = true,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 1,
    logLevel = "debug",
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = "warn"
  )

  def step(description: String): Unit = {
    // do nothing
  }

}
