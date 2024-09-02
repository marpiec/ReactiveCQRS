package io.reactivecqrs.core.types

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
      idGenerator = (idGenerator + 1).toShort
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

  private var notInitiated = true
  private val cache = mutable.HashMap[String, Short]()
  private val cacheReverse = mutable.HashMap[Short, String]()

  def initSchema(): PostgresTypesNamesState = {
    createTypesNamesTable()
    createTypesNamesSequence()
    createNamesIndex()
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
    sql"""CREATE SEQUENCE IF NOT EXISTS types_names_seq""".execute().apply()
  }

  private def createNamesIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS types_names_names_idx ON types_names (name)""".execute().apply()
  }

  override def typeIdByClassName(className: String): Short = {
    cache.getOrElseUpdate(className, getTypeIdFromDB(className))
  }

  override def classNameById(id: Short): String = {
    cacheReverse.getOrElseUpdate(id, getTypeNameFromDB(id))
  }

  private def getTypeIdFromDB(className: String): Short = synchronized {
    init()
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
    init()
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

  private def init(): Unit = {
    if(notInitiated) {
      DB.readOnly { implicit session =>
        sql"""SELECT id, name FROM types_names"""
          .foreach(rs => {
            val id = rs.short(1)
            val name = rs.string(2)
            cache += name -> id
            cacheReverse += id -> name
          })
      }
      notInitiated = false
    }
  }

}