package io.reactivecqrs.testdomain.spec.utils

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}

abstract class CommonSpec extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
          akka.loglevel = "DEBUG"
          akka.actor.debug.receive = on
          akka.actor.debug.receive = on
          akka.actor.debug.fsm = on
          akka.actor.debug.lifecycle = on"""))) with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  def step(description: String): Unit = {
    // do nothing
  }

}
