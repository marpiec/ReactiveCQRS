package io.reactivecqrs.testdomain

import io.reactivesqrs.testdomain.{UserRepository, UserCommandBus}
import org.scalatest.{GivenWhenThen, FeatureSpec}

class TestDomainSpec extends FeatureSpec with GivenWhenThen {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      Given("EvenStore, DataStore and UID generator, and UserService")

      val userRepository = new UserRepository
      val userCommandBus: UserCommandBus = new UserCommandBus(uidGenerator, commandStore, eventStore)


    }
  }
}
