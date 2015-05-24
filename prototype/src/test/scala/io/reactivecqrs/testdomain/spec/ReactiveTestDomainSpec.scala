package io.reactivecqrs.testdomain.spec

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.actor.{Aggregate, AkkaAggregate, EventStore}
import io.reactivecqrs.api.guid.{AggregateId, UserId}
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.UserCommandBus
import io.reactivecqrs.testdomain.api._
import io.reactivecqrs.testdomain.spec.utils.ActorAskSupport
import io.reactivecqrs.uid.UidGeneratorActor
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

      val uidGenerator = system.actorOf(Props(new UidGeneratorActor), "uidGenerator")

      val usersCommandBus: ActorRef = AkkaAggregate.create(new UserCommandBus, uidGenerator)(system).commandBus

      val registerUserResponse: CommandSuccessful = usersCommandBus ?? FirstCommandEnvelope(userId, RegisterUser("Marcin Pieciukiewicz"))

      val registeredUserId = registerUserResponse.aggregateId


      var user:Aggregate[User] = usersCommandBus ?? GetAggregateRoot(registeredUserId)

      user mustBe Aggregate(registeredUserId, AggregateVersion(1), Some(User("Marcin Pieciukiewicz", None)))


      val response: CommandSuccessful = usersCommandBus ?? CommandEnvelope(userId, user.id, user.version, ChangeUserAddress("Warsaw", "Center", "1"))

      response mustBe CommandSuccessful(user.id, AggregateVersion(2))

      user = usersCommandBus ?? GetAggregateRoot(registeredUserId)
      user mustBe Aggregate(response.aggregateId, response.aggregateVersion, Some(User("Marcin Pieciukiewicz", Some(Address("Warsaw", "Center", "1")))))

    }


  }

}
