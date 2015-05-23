package io.reactivecqrs.uid

import scalikejdbc._

class UidGeneratorSchemaInitializer  {

  def initSchema(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE aggregates_uids_seq INCREMENT BY 100 START 1000;""".execute().apply()
      }
    } catch {
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

}