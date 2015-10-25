package io.reactivecqrs.testutils

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

abstract class CommonSpec extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
          akka.loglevel = "DEBUG"
          akka.actor.debug.receive = on
          akka.actor.debug.receive = on
          akka.actor.debug.fsm = on
          akka.actor.debug.lifecycle = on"""))) with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    enabled = true,
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
