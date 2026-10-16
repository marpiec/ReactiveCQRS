package io.reactivecqrs.core.types

import java.util.concurrent.Executors

import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class PostgresTypesNamesStateSpec extends CommonSpec {

  TestDatabase.init()
  new PostgresTypesNamesState().initSchema()

  private implicit val threads: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

  Feature("Type names (F78)") {

    Scenario("a name new to several nodes and threads at once gets one id, which every node reads back") {
      val nodes = (1 to 4).map(_ => new PostgresTypesNamesState)
      (1 to 5).foreach { round =>
        val name = "io.example.New" + round + "." + System.nanoTime()
        val ids = Await.result(Future.sequence((1 to 40).map(i => Future(nodes(i % nodes.size).typeIdByClassName(name)))), 20.seconds)
        ids.distinct must have size 1
        nodes.map(_.classNameById(ids.head)).distinct mustBe Seq(name)
      }
    }
  }
}
