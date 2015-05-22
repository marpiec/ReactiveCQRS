package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.actor.{AggregateRoot, AkkaAggregate, EventStore}
import io.reactivecqrs.api.guid.{AggregateId, UserId}
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.UserCommandBus
import io.reactivecqrs.testdomain.api.{RegisterUser, RegisterUserResult, User}
import io.reactivecqrs.testdomain.spec.utils.ActorAskSupport
import org.scalatest.{FeatureSpecLike, GivenWhenThen, MustMatchers}



class ReactiveTestDomainSpec  extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
                          akka.persistence.journal.plugin = "postgres-journal"
                              akka.persistence.snapshot-store.plugin = "postgres-snapshot-store"
        postgres-journal.class = "io.reactivecqrs.persistance.postgres.PostgresSyncJournal"
        postgres-snapshot-store.class = "io.reactivecqrs.persistance.postgres.PostgresSyncSnapshotStore"
          akka.loglevel = "DEBUG"
          akka.actor.debug.receive = on
          akka.actor.debug.receive = on
          akka.actor.debug.fsm = on
          akka.actor.debug.lifecycle = on""")))
    with FeatureSpecLike with GivenWhenThen with ActorAskSupport with MustMatchers {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      (new EventStore).initSchema()

      val userId = UserId(1L)

      val usersCommandBus: ActorRef = AkkaAggregate.create(new UserCommandBus)(system).commandBus



      val registerUserResponse: RegisterUserResult = usersCommandBus ?? FirstCommandEnvelope(userId, RegisterUser("Marcin Pieciukiewicz"))


      registerUserResponse mustBe RegisterUserResult(AggregateId(1))


      val user:AggregateRoot[User] = usersCommandBus ?? GetAggregateRoot(registerUserResponse.registeredUserId)

      user mustBe AggregateRoot(registerUserResponse.registeredUserId, AggregateVersion(1), Some(User("Marcin Pieciukiewicz", None)))


    }


  }

}
