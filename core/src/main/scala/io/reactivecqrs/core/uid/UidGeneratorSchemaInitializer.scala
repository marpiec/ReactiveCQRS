package io.reactivecqrs.core.uid

import scalikejdbc._

class UidGeneratorSchemaInitializer  {


  def initSchema(): Unit = {
    DB.autoCommit { implicit session =>
      initAggregatesUidsSequence()
      initCommandUidsSequence()
      initSagasUidsSequence()
    }
  }

  private def initAggregatesUidsSequence()(implicit session: DBSession): Unit = {
    // first 1000 is reserved for system ids
    sql"""CREATE SEQUENCE IF NOT EXISTS aggregates_uids_seq INCREMENT BY 100 START 1001;""".execute().apply()
  }

  private def initCommandUidsSequence()(implicit session: DBSession): Unit = {
    // first 1000 is reserved for system ids
    sql"""CREATE SEQUENCE IF NOT EXISTS commands_uids_seq INCREMENT BY 100 START 1;""".execute().apply()
  }

  private def initSagasUidsSequence()(implicit session: DBSession): Unit = {
    // first 1000 is reserved for system ids
    sql"""CREATE SEQUENCE IF NOT EXISTS sagas_uids_seq INCREMENT BY 100 START 1;""".execute().apply()
  }
}
