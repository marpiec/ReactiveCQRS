package io.reactivecqrs.core.uid

import scalikejdbc._

class UidGeneratorSchemaInitializer  {


  def initSchema(): Unit = {
    initAggregatesUidsSequence()
    initCommandUidsSequence()
  }

  private def initAggregatesUidsSequence(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE aggregates_uids_seq INCREMENT BY 100 START 1001;""".execute().apply()
      }
    } catch {
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

  private def initCommandUidsSequence(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE commands_uids_seq INCREMENT BY 100 START 1;""".execute().apply()
      }
    } catch {
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

}
