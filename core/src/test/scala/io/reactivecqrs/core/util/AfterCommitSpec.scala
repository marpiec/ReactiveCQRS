package io.reactivecqrs.core.util

import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc.{DB, NoSession}

class AfterCommitSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  Feature("Actions registered inside AfterCommit.localTx") {

    Scenario("run after the commit, in registration order, not before") {
      var log = List[String]()
      AfterCommit.localTx { session =>
        AfterCommit(session)(log :+= "first")
        AfterCommit(session)(log :+= "second")
        log mustBe Nil
      }
      log mustBe List("first", "second")
    }

    Scenario("are dropped when the transaction rolls back") {
      var ran = false
      an[IllegalStateException] must be thrownBy AfterCommit.localTx { session =>
        AfterCommit(session) { ran = true }
        throw new IllegalStateException("rollback")
      }
      ran mustBe false
    }

    Scenario("F219 of transactions running at once, each runs its own after its own commit") {
      import scala.concurrent.{Await, ExecutionContext, Future}
      import scala.concurrent.duration._
      implicit val threads: ExecutionContext = ExecutionContext.global
      val ran = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
      val transactions = (1 to 8).map(i => Future(AfterCommit.localTx { session =>
        AfterCommit(session)(ran.add(i))
        Thread.sleep(50)
        ran.contains(i) mustBe false
        if (i % 2 == 0) throw new IllegalStateException("rollback")
      }).recover { case _: IllegalStateException => () })
      Await.result(Future.sequence(transactions), 10.seconds)
      ran.toArray.toSet mustBe Set(1, 3, 5, 7)
    }
  }

  Feature("Actions registered for any other session") {

    Scenario("run immediately") {
      var ran = 0
      AfterCommit(NoSession)(ran += 1)
      AfterCommit(null)(ran += 1)
      DB.autoCommit { session => AfterCommit(session)(ran += 1) }
      ran mustBe 3
    }
  }

  Feature("Values kept for one transaction (local)") {

    Scenario("are made once per transaction and owner, and start afresh in the next transaction") {
      val owner, other = new Object
      var made = 0
      def counter(session: scalikejdbc.DBSession) = AfterCommit.local(session, owner) { made += 1; new java.util.concurrent.atomic.AtomicInteger }

      AfterCommit.localTx { session =>
        counter(session).get.incrementAndGet()
        counter(session).get.incrementAndGet() mustBe 2
        AfterCommit.local(session, other)("another owner's") mustBe Some("another owner's")
      }
      AfterCommit.localTx(session => counter(session).get.get() mustBe 0)
      an[IllegalStateException] must be thrownBy AfterCommit.localTx { session =>
        counter(session).get.incrementAndGet()
        throw new IllegalStateException("rollback")
      }
      AfterCommit.localTx(session => counter(session).get.get() mustBe 0)
      made mustBe 4
    }

    Scenario("do not exist outside such a transaction") {
      AfterCommit.local(NoSession, this)("value") mustBe None
      DB.autoCommit(session => AfterCommit.local(session, this)("value") mustBe None)
    }
  }
}
