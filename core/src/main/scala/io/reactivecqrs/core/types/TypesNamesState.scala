package io.reactivecqrs.core.types

import io.reactivecqrs.core.util.{Ddl, SchemaLock}

import scalikejdbc._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/** The short ids under which the store keeps class names (event types, aggregate types, subscribers), one per name. */
trait TypesNamesState {
  /** The id of the class's name, given one the first time. */
  def typeIdByClass(clazz: Class[_]): Short = typeIdByClassName(clazz.getName)
  /** The id of `className`, given one the first time. */
  def typeIdByClassName(className: String): Short
  /** The name stored under `id`. */
  def classNameById(id: Short): String
}

/** Ids of names in memory, for tests. */
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

/** Ids of class names, shared by every node. A name's id never changes once stored, so the cache is never invalidated.
  * F78: the cache is read from many threads (owners, projections, the outbox poller), and a name new to several nodes at
  * once is stored once: an insert that loses the race does nothing, and every node reads the winner's id. */
class PostgresTypesNamesState extends TypesNamesState {

  private val ids = TrieMap[String, Short]()
  private val names = TrieMap[Short, String]()
  @volatile private var loaded = false

  /** Creates the table `types_names`, when missing; returns this state. */
  def initSchema(): PostgresTypesNamesState = SchemaLock {
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
     """.execute()
  }

  private def createTypesNamesSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE IF NOT EXISTS types_names_seq""".execute()
  }

  // F185: only when the index is missing (Ddl).
  private def createNamesIndex(): Unit = Ddl.createIndex("types_names_names_idx", "CREATE UNIQUE INDEX IF NOT EXISTS types_names_names_idx ON types_names (name)")

  override def typeIdByClassName(className: String): Short = ids.getOrElse(className, idFromDB(className))

  override def classNameById(id: Short): String = names.getOrElse(id, nameFromDB(id))

  private def remember(id: Short, name: String): Unit = {
    ids.put(name, id)
    names.put(id, name)
  }

  private def idFromDB(className: String): Short = synchronized {
    loadAll()
    ids.getOrElse(className, {
      val id = DB.localTx { implicit session =>
        sql"""INSERT INTO types_names (id, name) VALUES (nextval('types_names_seq'), $className) ON CONFLICT (name) DO NOTHING""".update()
        sql"""SELECT id FROM types_names WHERE name = $className""".map(_.short(1)).single()
          .getOrElse(throw new IllegalStateException("No entry for type className '" + className + "' in types_names found"))
      }
      remember(id, className)
      id
    })
  }

  private def nameFromDB(id: Short): String = synchronized {
    loadAll()
    names.getOrElse(id, {
      val name = DB.readOnly(implicit session => sql"""SELECT name FROM types_names WHERE id = $id""".map(_.string(1)).single())
        .getOrElse(throw new IllegalStateException("No entry for type id " + id + " in types_names found"))
      remember(id, name)
      name
    })
  }

  private def loadAll(): Unit = if (!loaded) {
    DB.readOnly(implicit session => sql"""SELECT id, name FROM types_names""".foreach(rs => remember(rs.short(1), rs.string(2))))
    loaded = true
  }

}
