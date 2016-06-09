package io.reactivecqrs.testutils

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

abstract class CommonSpec extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
          akka.loglevel = "WARNING"
          akka.actor.debug.receive = off
          akka.actor.debug.receive = off
          akka.actor.debug.fsm = off
          akka.actor.debug.lifecycle = off"""))) with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

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
