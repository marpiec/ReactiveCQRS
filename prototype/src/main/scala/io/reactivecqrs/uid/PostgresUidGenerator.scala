package io.reactivecqrs.uid

import scalikejdbc._

class PostgresUidGenerator {

  new UidGeneratorSchemaInitializer().initSchema()

  val poolSize = readSequenceStep

  def nextIdsPool: NewAggregatesIdsPool = {

    DB.autoCommit { implicit session =>
      val poolFrom = sql"""SELECT NEXTVAL('aggregates_uids_seq')""".map(rs => rs.long(1)).single().apply().getOrElse {
        throw new IllegalStateException("Query returned no values, that should not happen.")
      }
      NewAggregatesIdsPool(poolFrom, poolSize)
    }
  }


  private def readSequenceStep: Long = {
    DB.readOnly { implicit session =>
      sql"""SELECT increment_by FROM aggregates_uids_seq""".map(rs => rs.long(1)).single().apply().get
    }
  }

}
