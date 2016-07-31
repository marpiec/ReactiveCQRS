package io.reactivecqrs.core.documentstore

import java.sql.{Connection, ResultSet}

import io.mpjsons.MPJsons
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import scalikejdbc.{DB, DBSession}

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scalikejdbc._

sealed trait PostgresDocumentStoreTrait[T <: AnyRef, M <: AnyRef] {

  implicit val t: TypeTag[T]
  implicit val m: TypeTag[M]

  val tableName: String
  val mpjsons: MPJsons

  if (!tableName.matches( """[a-zA-Z0-9\_]+""")) {
    throw new IllegalArgumentException("Invalid table name, only alphanumeric characters and underscore allowed")
  }

  protected final val projectionTableName = "projection_" + tableName

  protected val tableNameSQL = SQLSyntax.createUnsafely(projectionTableName)

  protected def SELECT_DOCUMENT_BY_PATH(path: String) = s"SELECT id, document, metadata FROM $projectionTableName WHERE document #>> '{$path}' = ?"

  protected def SELECT_DOCUMENT_BY_PATH_WITH_ONE_OF_THE_VALUES(path: String, values: Set[String]) =
    s"SELECT id, document, metadata FROM $projectionTableName WHERE document #>> '{$path}' in (${values.map("'"+_+"'").mkString(",")})"

  init()

  protected def init(): Unit = {
    createTableIfNotExists()
  }

  protected def createTableIfNotExists(): Unit = DB.autoCommit { implicit session =>
    sql"""CREATE TABLE IF NOT EXISTS ${tableNameSQL} (
          |id BIGINT NOT NULL PRIMARY KEY,
          |version INT NOT NULL,
          |document JSONB NOT NULL, metadata JSONB NOT NULL)"""
      .stripMargin.execute().apply()
  }

  def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute().apply()
  }

  def overwriteDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      val updated = sql"UPDATE ${tableNameSQL} SET document = ?::JSONB, metadata = ?::JSONB , version = version + 1 WHERE id = ?"
        .bind(mpjsons.serialize[T](document), mpjsons.serialize[M](metadata), key)
        .map(_.int(1)).single().executeUpdate().apply()

      if (updated != 1) {
        throw new IllegalStateException("Expected 1, updated " + updated + " records")
      }
    }
  }

  // Optimistic locking update
  def updateDocument(key: Long, modify: DocumentWithMetadata[T, M] => DocumentWithMetadata[T, M])(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      sql"SELECT version, document, metadata FROM ${tableNameSQL} WHERE id = ?"
        .bind(key).map(rs => {
        (rs.int(1), mpjsons.deserialize[T](rs.string(2)), mpjsons.deserialize[M](rs.string(3)))
      }).single().apply() match {
        case None => throw new IllegalStateException("Document in "+tableName+" not found for key " + key)
        case Some((version, document, metadata)) =>
          val modified = modify(DocumentWithMetadata(document, metadata))

          val updated = sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB, metadata = ?::JSONB WHERE id = ? AND version = ?"
            .bind(version + 1, mpjsons.serialize[T](modified.document), mpjsons.serialize[M](modified.metadata), key, version)
            .map(_.int(1)).single().executeUpdate().apply()

          if(updated == 0) {
            updateDocument(key, modify)
          }
      }
    }
  }


  def getDocument(key: Long)(implicit session: DBSession = null): Option[DocumentWithMetadata[T, M]] = {
    inSession { implicit session =>
      sql"SELECT document, metadata FROM ${tableNameSQL} WHERE id = ?"
          .bind(key).map(rs => {
        DocumentWithMetadata[T, M](mpjsons.deserialize[T](rs.string(1)), mpjsons.deserialize[M](rs.string(2)))
      }).single().apply()
    }
  }

  def removeDocument(key: Long)(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      sql"DELETE FROM ${tableNameSQL} WHERE id = ?"
        .bind(key).executeUpdate().apply()
    }

  }

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    inConnection { connection =>
      val statement = connection.prepareStatement(SELECT_DOCUMENT_BY_PATH(path.mkString(",")))
      try {
        statement.setString(1, value)
        val resultSet = statement.executeQuery()
        try {
          val results = mutable.ListMap[Long, DocumentWithMetadata[T, M]]()
          while (resultSet.next()) {
            results += resultSet.getLong(1) -> DocumentWithMetadata[T,M](mpjsons.deserialize[T](resultSet.getString(2)), mpjsons.deserialize[M](resultSet.getString(3)))
          }
          results.toMap
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    }
  }


  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    if (values.nonEmpty) {
      inConnection { connection =>
        val statement = connection.prepareStatement(SELECT_DOCUMENT_BY_PATH_WITH_ONE_OF_THE_VALUES(path.mkString(","), values))
        try {
          val resultSet = statement.executeQuery()
          try {
            val results = mutable.ListMap[Long, DocumentWithMetadata[T, M]]()
            while (resultSet.next()) {
              results += resultSet.getLong(1) -> DocumentWithMetadata[T, M](mpjsons.deserialize[T](resultSet.getString(2)), mpjsons.deserialize[M](resultSet.getString(3)))
            }
            results.toMap
          } finally {
            resultSet.close()
          }
        } finally {
          statement.close()
        }
      }
    } else Map()
  }


  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    findDocumentByObjectInArray("document", arrayPath, objectPath, value)
  }

  def findDocumentByMetadataObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    findDocumentByObjectInArray("metadata", arrayPath, objectPath, value)
  }

  protected def findDocumentByObjectInArray[V](columnName: String, array: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession): Map[Long, DocumentWithMetadata[T, M]] = {

    //sample query that works:
    // SELECT * FROM projection_processes_flows WHERE document #> '{state, cursors}' @> '[{"currentNodeId":2}]';
    def QUERY(arrayPath: String, path: String) =
      s"SELECT id, document, metadata FROM $projectionTableName WHERE $columnName #> '$arrayPath' @> '[$path]'"

    def makeJson(path: Seq[String], value: V): String =
      path match {
        case head :: tail => "{\"" + head + "\":" + makeJson(tail, value) + "}"
        case Nil => value match {
          case s: String => "\"" + s + "\""
          case anything => anything.toString
        }
      }

    inConnection { connection =>
      val statement = connection.prepareStatement(QUERY(array.mkString("{", ",", "}"), makeJson(objectPath, value)))
      try {
//        statement.setString(1, value)
        val resultSet = statement.executeQuery()
        try {
          val results = mutable.ListMap[Long, DocumentWithMetadata[T, M]]()
          while (resultSet.next()) {
            results += resultSet.getLong(1) -> DocumentWithMetadata[T,M](mpjsons.deserialize[T](resultSet.getString(2)), mpjsons.deserialize[M](resultSet.getString(3)))
          }
          results.toMap
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    }
  }


  def findAll()(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    inSession { implicit session =>
      val tuples = sql"SELECT id, document, metadata FROM ${tableNameSQL}"
          .map(rs => rs.long(1) -> DocumentWithMetadata[T,M](mpjsons.deserialize[T](rs.string(2)), mpjsons.deserialize[M](rs.string(3))))
        .list.apply()
      tuples.toMap
    }
  }

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]] = {
    if (keys.isEmpty) {
      Map[Long, DocumentWithMetadata[T, M]]()
    } else {
      inSession { implicit session =>
        val tuples = sql"SELECT id, document, metadata FROM ${tableNameSQL} WHERE id IN (${keys})"
          .map(rs => {
            rs.long(1) -> DocumentWithMetadata[T, M](mpjsons.deserialize[T](rs.string(2)), mpjsons.deserialize[M](rs.string(3)))
          }).list().apply()
        tuples.toMap
      }
    }
  }


  protected def inSession[RETURN_TYPE](body: DBSession => RETURN_TYPE)(implicit session: DBSession): RETURN_TYPE = {
    if(session == null) {
      DB.readOnly { s =>
        body(s)
      }
    } else {
      body(session)
    }
  }

  protected def inConnection[RETURN_TYPE](body: Connection => RETURN_TYPE)(implicit session: DBSession): RETURN_TYPE = {
    if(session == null) {
      DB.readOnly { s =>
        body(s.connection)
      }
    } else {
      body(session.connection)
    }
  }

}

class PostgresDocumentStore[T <: AnyRef, M <: AnyRef](val tableName: String, val mpjsons: MPJsons)(implicit val t: TypeTag[T], val m: TypeTag[M])
  extends DocumentStore[T, M] with PostgresDocumentStoreTrait[T, M] {

  override def insertDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"INSERT INTO ${tableNameSQL} (id, version, document, metadata) VALUES (?, 1, ?::jsonb, ?::jsonb)"
        .bind(key, mpjsons.serialize(document), mpjsons.serialize(metadata))
        .executeUpdate().apply()
    }
  }

}

class PostgresDocumentStoreAutoId[T <: AnyRef, M <: AnyRef](val tableName: String, val mpjsons: MPJsons)(implicit val t: TypeTag[T], val m: TypeTag[M])
  extends DocumentStoreAutoId[T, M] with PostgresDocumentStoreTrait[T, M] {

  protected final lazy val sequenceName = "sequence_" + tableName

  protected val sequenceNameSQL = SQLSyntax.createUnsafely(sequenceName)


  override protected def createTableIfNotExists(): Unit = {
    super.createTableIfNotExists()
    try {
      DB.autoCommit { implicit session =>
        sql"CREATE SEQUENCE ${sequenceNameSQL} START 1"
          .executeUpdate().apply()
      }
    } catch {
      case e: PSQLException => () // IF NOT EXIST workaround
    }
  }

  override def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute().apply()

    sql"""DROP SEQUENCE ${sequenceNameSQL}"""
      .stripMargin.execute().apply()

  }

  override def insertDocument(document: T, metadata: M)(implicit session: DBSession): Long = {
    inSession {implicit session =>
      sql"INSERT INTO ${tableNameSQL} (id, version, document, metadata) VALUES (nextval('${sequenceNameSQL}'), 1, ?::jsonb, ?::jsonb) RETURNING currval('${sequenceNameSQL}')"
          .bind(mpjsons.serialize(document), mpjsons.serialize(metadata))
          .map(_.long(1)).single().apply().get
    }
  }
}

