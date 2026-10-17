package io.reactivecqrs.core.uid

import io.reactivecqrs.testkit.CommonSpec
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

// F40: a failing id store is reported to the requester, which otherwise waits out its whole timeout.
class UidGeneratorActorSpec extends CommonSpec {

  private val failing = new UidGenerator {
    override def nextIdsPool: IdsPool = throw new IllegalStateException("sequence unavailable")
  }

  Feature("A uid generator whose store fails") {
    Scenario("answers every kind of request with the failure at once") {
      val generator = system.actorOf(Props(new UidGeneratorActor(failing, failing, failing)))
      implicit val timeout: Timeout = Timeout(5.seconds)

      for (request <- List(UidGeneratorActor.GetNewAggregatesIdsPool, UidGeneratorActor.GetNewCommandsIdsPool, UidGeneratorActor.GetNewSagasIdsPool)) {
        val reply = Await.ready(generator ? request, 10.seconds).value.get
        reply.failed.get.getMessage mustBe "sequence unavailable"
      }
    }
  }
}
