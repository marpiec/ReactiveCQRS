package io.reactivecqrs.core.documentstore

import java.sql.ResultSet
import javax.sql.DataSource

import io.mpjsons.MPJsons

import scala.collection.mutable
import scala.reflect.runtime.universe._

class PostgresDocumentStore[T <: AnyRef: TypeTag, M <: AnyRef: TypeTag](tableName: String, dbDataSource: DataSource,
          mpjsons: MPJsons) extends DocumentStore[T,M] {

  if (!tableName.matches( """[a-zA-Z0-9\_]+""")) {
    throw new IllegalArgumentException("Invalid table name, only alphanumeric characters and underscore allowed")
  }

  private final val projectionTableName = "projection_" + tableName

  private val CREATE_TABLE_QUERY = s"CREATE TABLE IF NOT EXISTS $projectionTableName (" +
    "id BIGINT NOT NULL PRIMARY KEY, " +
    "document JSONB NOT NULL, metadata JSONB NOT NULL)"

  private val UPDATE_DOCUMENT_QUERY = s"UPDATE $projectionTableName SET document = ?::jsonb WHERE id = ? "

  private val INSERT_DOCUMENT_QUERY = s"INSERT INTO $projectionTableName (id, document) VALUES (?, ?::jsonb)"

  private val SELECT_DOCUMENT_BY_ID_QUERY = s"SELECT document, metadata FROM $projectionTableName WHERE id = ?"

  private def SELECT_DOCUMENT_BY_IDS_QUERY(ids: Seq[Long]) =
    s"SELECT id, document, metadata FROM $projectionTableName WHERE id IN ( ${ids.mkString(",")} )"

  private val DELETE_DOCUMENT_BY_ID_QUERY = s"DELETE FROM $projectionTableName WHERE id = ?"

  private def SELECT_DOCUMENT_BY_PATH(path: String) = s"SELECT id, document, metadata FROM $projectionTableName WHERE document #>> '{$path}' = ?"

  private def SELECT_DOCUMENT_BY_PATH_WITH_ONE_OF_THE_VALUES(path: String, values: Set[String]) =
    s"SELECT id, document, metadata FROM $projectionTableName WHERE document #>> '{$path}' in (${values.map("'"+_+"'").mkString(",")}})"

  private val SELECT_ALL = s"SELECT id, document, metadata FROM $projectionTableName"

  init()

  private def init(): Unit = {
    createTableIfNotExists()
  }

  private def createTableIfNotExists(): Unit = {
    executeQuery(CREATE_TABLE_QUERY)
  }

  def executeQuery(query: String): Unit = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(query)
      try {
        statement.execute()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  override def insertDocument(key: Long, document: T, metadata: M): Unit = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(INSERT_DOCUMENT_QUERY)
      try {
        statement.setLong(1, key)
        statement.setString(2, mpjsons.serialize(document))
        statement.execute()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  override def updateDocument(key: Long, document: T, metadata: M): Unit = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(UPDATE_DOCUMENT_QUERY)
      try {
        statement.setString(1, mpjsons.serialize[T](document))
        statement.setLong(2, key)

        val numberOfUpdated = statement.executeUpdate()

        if (numberOfUpdated != 1) {
          throw new IllegalStateException("Expected 1, updated " + numberOfUpdated + " records")
        }

      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }


  override def getDocument(key: Long): Option[DocumentWithMetadata[T, M]] = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(SELECT_DOCUMENT_BY_ID_QUERY)
      try {
        statement.setLong(1, key)
        val resultSet = statement.executeQuery()
        try {
          if (resultSet.next()) {
            Some(DocumentWithMetadata[T, M](mpjsons.deserialize[T](resultSet.getString(1)), mpjsons.deserialize[M](resultSet.getString(2))))
          } else {
            None
          }
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  override def removeDocument(key: Long): Unit = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(DELETE_DOCUMENT_BY_ID_QUERY)
      try {
        statement.setLong(1, key)
        statement.execute()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  override def findDocumentByPath(path: Seq[String], value: String): Map[Long, DocumentWithMetadata[T, M]] = {
    val connection = dbDataSource.getConnection
    try {
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
    } finally {
      connection.close()
    }
  }


  override def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String]): Map[Long, DocumentWithMetadata[T, M]] = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(SELECT_DOCUMENT_BY_PATH_WITH_ONE_OF_THE_VALUES(path.mkString(","), values))
      try {
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
    } finally {
      connection.close()
    }
  }

  override def findDocumentByPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    findDocumentByPathWithOneArray("document", array, objectPath, value)
  }

  override def findDocumentByMetadataPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    findDocumentByPathWithOneArray("metadata", array, objectPath, value)
  }

  private def findDocumentByPathWithOneArray[V](columnName: String, array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    val connection = dbDataSource.getConnection

    //sample query that works:
    // SELECT id, document FROM projection_process_info WHERE document -> 'setups' @> '[{"hasActiveInstance":true}]';
    def QUERY(array: String, path: String) =
      s"SELECT id, document, metadata FROM $projectionTableName WHERE $columnName -> '$array' @> '[$path]'"

    def makeJson(path: Seq[String], value: V): String =
      path match {
        case head :: tail => "{\"" + head + "\":" + makeJson(tail, value) + "}"
        case Nil => value match {
          case s: String => "\"" + s + "\""
          case b: Boolean => b.toString
          case i: Int => i.toString
        }
      }

    try {
      val statement = connection.prepareStatement(QUERY(array, makeJson(objectPath, value)))
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
    } finally {
      connection.close()
    }
  }


  override def findAll(): Map[Long, DocumentWithMetadata[T, M]] = {
    val connection = dbDataSource.getConnection
    try {
      val statement = connection.prepareStatement(SELECT_ALL)
      try {
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
    } finally {
      connection.close()
    }
  }

  override def getDocuments(keys: List[Long]): Map[Long, DocumentWithMetadata[T, M]] = {
    if (keys.isEmpty) {
      Map[Long, DocumentWithMetadata[T, M]]()
    }
    else {
      val connection = dbDataSource.getConnection
      try {
        val statement = connection.prepareStatement(SELECT_DOCUMENT_BY_IDS_QUERY(keys))
        try {
          val resultSet: ResultSet = statement.executeQuery()
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
      } finally {
        connection.close()
      }
    }
  }

}
