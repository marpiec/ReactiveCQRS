package io.reactivecqrs.testdomain.spec

import akka.actor.{Props, ActorSystem, ActorRef}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.actor.{AkkaAggregate, AggregateRoot}
import io.reactivecqrs.core.{Aggregate, AggregateVersion, GetAggregateRoot, AggregateId}
import io.reactivecqrs.testdomain.{UserCommandBus}
import io.reactivecqrs.testdomain.api.{User, RegisterUser, RegisterUserResult}
import io.reactivecqrs.testdomain.spec.utils.ActorAskSupport
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}

class ReactiveTestDomainSpec  extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
          akka.loglevel = "DEBUG"
          akka.actor.debug.receive = on
          akka.actor.debug.autoreceive = on
          akka.actor.debug.lifecycle = on""")))
    with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {


      val usersCommandBus: ActorRef = AkkaAggregate.create(new UserCommandBus)(system).commandBus

      val registerUserResponse: RegisterUserResult = usersCommandBus ?? RegisterUser("Marcin Pieciukiewicz")


      registerUserResponse mustBe RegisterUserResult(AggregateId(1))


      val user:AggregateRoot[User] = usersCommandBus ?? GetAggregateRoot(registerUserResponse.registeredUserId)

      user mustBe AggregateRoot(registerUserResponse.registeredUserId, AggregateVersion(1), Some(User("Marcin Pieciukiewicz", None)))


    }


  }

}
