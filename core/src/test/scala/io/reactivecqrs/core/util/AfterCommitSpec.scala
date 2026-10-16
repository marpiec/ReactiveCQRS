package io.reactivecqrs.core.util

import io.reactivecqrs.testutils.TestDatabase
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
}
