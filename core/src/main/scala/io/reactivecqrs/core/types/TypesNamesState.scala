package io.reactivecqrs.core.types

import org.postgresql.util.PSQLException
import scalikejdbc._

import scala.collection.mutable

trait TypesNamesState {
  def typeIdByClass(clazz: Class[_]): Short = typeIdByClassName(clazz.getName)
  def typeIdByClassName(className: String): Short
  def classNameById(id: Short): String
}

class MemoryTypesNamesState extends TypesNamesState {
  private var idGenerator: Short = 0
  private val cache = mutable.HashMap[String, Short]()
  private val cacheReverse = mutable.HashMap[Short, String]()

  override def typeIdByClassName(className: String): Short = synchronized {
    cache.getOrElse(className, {
      idGenerator += 1
      cache += className -> idGenerator
      cacheReverse += idGenerator -> className
      idGenerator
    })
  }

  override def classNameById(id: Short): String = synchronized {
    cacheReverse(id)
  }
}

class PostgresTypesNamesState extends TypesNamesState {

  private val cache = mutable.HashMap[String, Short]()
  private val cacheReverse = mutable.HashMap[Short, String]()

  def initSchema(): PostgresTypesNamesState = {
    createTypesNamesTable()
    try {
      createTypesNamesSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    try {
      createNamesIndex()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    this
  }

  private def createTypesNamesTable() = DB.autoCommit { implicit session =>
    sql"""
       CREATE TABLE IF NOT EXISTS types_names (
         id SMALLINT NOT NULL PRIMARY KEY,
         name VARCHAR(255) NOT NULL)
     """.execute().apply()
  }

  private def createTypesNamesSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE types_names_seq""".execute().apply()
  }

  private def createNamesIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX types_names_names_idx ON types_names (name)""".execute().apply()
  }

  override def typeIdByClassName(className: String): Short = {
    cache.getOrElseUpdate(className, getTypeIdFromDB(className))
  }

  override def classNameById(id: Short): String = {
    cacheReverse.getOrElseUpdate(id, getTypeNameFromDB(id))
  }

  private def getTypeIdFromDB(className: String): Short = synchronized {
    cache.get(className) match {
      case Some(id) => id
      case None => DB.localTx { implicit session =>
        val idOption = sql"""SELECT id FROM types_names WHERE name = ?"""
          .bind(className).map(_.short(1)).single().apply()
        idOption match {
          case Some(id) => id
          case None => insertName(className)
        }
      }
    }
  }

  private def getTypeNameFromDB(id: Short): String = synchronized {
    cacheReverse.get(id) match {
      case Some(className) => className
      case None => DB.localTx { implicit session =>
        sql"""SELECT name FROM types_names WHERE id = ?"""
          .bind(id).map(_.string(1)).single().apply().get
      }
    }
  }

  private def insertName(className: String)(implicit session: DBSession): Short = {
    sql"""INSERT INTO types_names (id, name) VALUES (nextval('types_names_seq'), ?) RETURNING currval('types_names_seq')"""
      .bind(className).map(_.short(1)).single().apply().get
  }

}