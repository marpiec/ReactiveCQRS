package io.reactivecqrs.testutils

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

abstract class CommonSpec extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
          pekko.loglevel = "WARNING"
          pekko.actor.debug.receive = off
          pekko.actor.debug.receive = off
          pekko.actor.debug.fsm = off
          pekko.actor.debug.lifecycle = off"""))) with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = false,
    singleLineMode = true,
    printUnprocessedStackTrace = false,
    stackTraceDepth= 1,
    logLevel = 'debug,
    warningEnabled = false,
    warningThresholdMillis = 3000L,
    warningLogLevel = 'warn
  )

  def step(description: String): Unit = {
    // do nothing
  }

}
