package io.reactivecqrs.testdomain.spec

import akka.actor.{Props, ActorSystem, ActorRef}
import akka.testkit.TestKit
import io.reactivecqrs.core.AggregateId
import io.reactivecqrs.testdomain.UserCommandBus
import io.reactivecqrs.testdomain.api.{RegisterUserResult, RegisterUser}
import io.reactivecqrs.testdomain.spec.utils.ActorAskSupport
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}

class ReactiveTestDomainSpec  extends TestKit(ActorSystem("testsystem"))
    with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {


      val usersCommandBus: ActorRef = system.actorOf(Props(new UserCommandBus))

      val registerUserResponse: AnyRef = usersCommandBus ?? RegisterUser("Marcin Pieciukiewicz1")


      registerUserResponse mustBe RegisterUserResult(AggregateId(1))




    }


  }

}
