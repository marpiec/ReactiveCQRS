package io.reactivecqrs.testdomain

import java.time.Clock

import akka.actor.{ActorSystem, Props}
import io.reactivecqrs.api.Aggregate
import io.reactivecqrs.api.guid.UserId
import io.reactivecqrs.core.{GetAggregate, MemoryCommandLogActorApi, MemorySequentialAggregateIdGenerator, MemorySequentialCommandIdGenerator}
import io.reactivecqrs.testdomain.utils.ActorAskSupport
import io.reactivesqrs.testdomain.api.{User, RegisterUser, RegisterUserResult}
import io.reactivesqrs.testdomain.{UserCommandBus, UserRepository}
import org.scalatest.MustMatchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class TestDomainSpec extends FeatureSpec with GivenWhenThen with ActorAskSupport {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      Given("EvenStore, DataStore and UID generator, and UserService")

      val system = ActorSystem()

      val userRepository = system.actorOf(Props(classOf[UserRepository]))
      val aggregateIdGenerator = new MemorySequentialAggregateIdGenerator
      val commandIdGenerator = new MemorySequentialCommandIdGenerator
      val commandLog = new MemoryCommandLogActorApi
      val userCommandBus =
        system.actorOf(Props(classOf[UserCommandBus], Clock.systemDefaultZone(), commandIdGenerator, aggregateIdGenerator, commandLog, userRepository))


      When("User is registered")

      val currentUserId = UserId.fromAggregateId(aggregateIdGenerator.nextAggregateId)
      val registrationResult: RegisterUserResult = userCommandBus ?? RegisterUser("Marcin Pieciukiewicz")

      registrationResult.success mustBe true

      val registeredUserId = registrationResult.registeredUserId

      Then("We can get aggregate from repository")

      val userAggregate: Aggregate[User] = userRepository ?? GetAggregate("123", registeredUserId)

      userAggregate.id mustBe registeredUserId
      userAggregate.version mustBe 1
      userAggregate.aggregateRoot must be 'defined
      userAggregate.aggregateRoot.get mustBe User("Marcin Pieciukiewicz", None)

    }
  }
}
