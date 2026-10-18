package io.reactivecqrs.core.types

import java.util.concurrent.Executors

import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}

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

    Scenario("F230 a name that waits for another node's insert holds up no other name") {
      val (state, waiting, other) = (new PostgresTypesNamesState().initSchema(), "io.example.Waiting." + System.nanoTime(), "io.example.Other." + System.nanoTime())
      scalikejdbc.DB.localTx { implicit session => // another node inserts the name, not committed yet
        scalikejdbc.SQL("INSERT INTO types_names (id, name) VALUES (nextval('types_names_seq'), ?)").bind(waiting).update()
        val blocked = Future(state.typeIdByClassName(waiting))
        Thread.sleep(300) // it waits for this transaction
        Await.result(Future(state.typeIdByClassName(other)), 3.seconds) must be > 0.toShort
        blocked.isCompleted mustBe false
      }
    }
  }
}
