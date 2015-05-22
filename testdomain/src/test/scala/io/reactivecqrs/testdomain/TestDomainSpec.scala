package io.reactivecqrs.testdomain

import io.reactivecqrs.testdomain.spec.utils.ActorAskSupport
import org.scalatest.{FeatureSpec, GivenWhenThen}

class TestDomainSpec extends FeatureSpec with GivenWhenThen with ActorAskSupport {

//
//  feature("Aggregate storing and getting with event sourcing") {
//
//    scenario("Creation and modification of user aggregate") {
//
//      Given("EvenStore, DataStore and UID generator, and UserService")
//
//      //val system = ActorSystem()
//
//      implicit val system = ActorSystem("testsystem", ConfigFactory.parseString("""
//          akka.loglevel = "DEBUG"
//          akka.actor.debug.receive = on
//          akka.actor.debug.autoreceive = on
//          akka.actor.debug.lifecycle = on
//      """))
//
//      val userRepository = system.actorOf(Props(classOf[UserRepository], Clock.systemDefaultZone(), new MemoryEventStore[User]), "UserRepository")
//      val aggregateIdGenerator = new MemorySequentialAggregateIdGenerator
//      val commandIdGenerator = new MemorySequentialCommandIdGenerator
//      val commandLog = new MemoryCommandLogActorApi
//      val userCommandBus =
//        system.actorOf(Props(classOf[UserCommandBusOld], Clock.systemDefaultZone(), commandIdGenerator, aggregateIdGenerator, commandLog, userRepository), "UserCommandBus")
//
//
//      When("User is registered")
//
//      val currentUserId = UserId.fromAggregateId(aggregateIdGenerator.nextAggregateId)
//      val registrationResult: CommandResponseEnvelope[RegisterUserResult] = userCommandBus ?? CommandEnvelopeOld("123", currentUserId, RegisterUser("Marcin Pieciukiewicz"))
//
//      registrationResult.acknowledgeId mustBe "123"
//      registrationResult.response mustBe 'success
//
//      val registeredUserId = registrationResult.response.value.registeredUserId
//
//      Then("We can get aggregate from repository")
//
//      var userAggregateResponse: GetAggregateResponse[Aggregate[User]] = userRepository ?? LoadAggregate("321", registeredUserId)
//
//      userAggregateResponse.messageId mustBe "321"
//      userAggregateResponse.result mustBe 'success
//      var userAggregate = userAggregateResponse.result.value
//      userAggregate.id mustBe registeredUserId
//      userAggregate.version.version mustBe 1
//      userAggregate.aggregateRoot mustBe 'defined
//      userAggregate.aggregateRoot.get mustBe User("Marcin Pieciukiewicz", None)
//
//
//      When("User address is changed")
//      var changeAddressResponse: CommandResponseEnvelope[EmptyResult] = userCommandBus ?? CommandEnvelopeOld("123", currentUserId,
//        ChangeUserAddress(registeredUserId, userAggregate.version, "Warsaw", "Center", "1"))
//      changeAddressResponse.response mustBe 'success
//
//      Then("We can get user with modified address")
//      userAggregateResponse = userRepository ?? LoadAggregate("321", registeredUserId)
//
//      userAggregate = userAggregateResponse.result.value
//      userAggregate.version.version mustBe 2
//      userAggregate.aggregateRoot.get mustBe User("Marcin Pieciukiewicz", Some(Address("Warsaw", "Center", "1")))
//
//      When("User address is changed again")
//      changeAddressResponse = userCommandBus ?? CommandEnvelopeOld("123", currentUserId,
//        ChangeUserAddress(registeredUserId, userAggregate.version, "Cracow", "Market", "2"))
//      changeAddressResponse.response mustBe 'success
//
//      Then("We can get user with again modified address")
//      userAggregateResponse = userRepository ?? LoadAggregate("321", registeredUserId)
//
//      userAggregate = userAggregateResponse.result.value
//      userAggregate.version.version mustBe 3
//      userAggregate.aggregateRoot.get mustBe User("Marcin Pieciukiewicz", Some(Address("Cracow", "Market", "2")))
//
//
//      When("User is deleted")
//      val deleteUserResponse: CommandResponseEnvelope[EmptyResult] = userCommandBus ?? CommandEnvelopeOld("123", currentUserId, DeleteUser(registeredUserId, userAggregate.version))
//      deleteUserResponse.response mustBe 'success
//
//      Then("When we try to get user we'll have empty aggregate")
//      userAggregateResponse = userRepository ?? LoadAggregate("321", registeredUserId)
//      userAggregate = userAggregateResponse.result.value
//      userAggregate.version.version mustBe 4
//      userAggregate.aggregateRoot mustBe 'empty
//
//      When("We want to get first version of user")
//      userAggregateResponse = userRepository ?? LoadAggregateForVersion("321", registeredUserId, AggregateVersion(1))
//      Then("We have correct first vesion of user")
//      userAggregateResponse.result mustBe 'success
//      userAggregate = userAggregateResponse.result.value
//      userAggregate.id mustBe registeredUserId
//      userAggregate.version.version mustBe 1
//      userAggregate.aggregateRoot mustBe 'defined
//      userAggregate.aggregateRoot.get mustBe User("Marcin Pieciukiewicz", None)
//    }
//  }
//
//  feature("Concurrent modification detection") {
//
//  }
//
//  feature("Versioning of events") {
//
//  }
}
