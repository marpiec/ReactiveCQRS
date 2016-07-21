package io.reactivecqrs.core.uid

import org.postgresql.util.PSQLException
import scalikejdbc._

class UidGeneratorSchemaInitializer  {


  def initSchema(): Unit = {
    initAggregatesUidsSequence()
    initCommandUidsSequence()
    initSagasUidsSequence()
  }

  private def initAggregatesUidsSequence(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE aggregates_uids_seq INCREMENT BY 100 START 1001;""".execute().apply()
      }
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

  private def initCommandUidsSequence(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE commands_uids_seq INCREMENT BY 100 START 1;""".execute().apply()
      }
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

  private def initSagasUidsSequence(): Unit = {
    try {
      // first 1000 is reserved for system ids
      DB.autoCommit { implicit session =>
        sql"""CREATE SEQUENCE sagas_uids_seq INCREMENT BY 100 START 1;""".execute().apply()
      }
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }
}
