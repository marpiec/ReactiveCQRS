package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import org.postgresql.util.PSQLException
import scalikejdbc.{DB, DBSession}

import scala.reflect.runtime.universe._
import scalikejdbc._

sealed trait PostgresDocumentStoreTrait[T <: AnyRef] {

  implicit val t: TypeTag[T]

  val tableName: String
  val mpjsons: MPJsons
  val cache: DocumentStoreCache[T]
  val indicies: Seq[Index]


  if (!tableName.matches( """[a-zA-Z0-9\_]+""")) {
    throw new IllegalArgumentException("Invalid table name, only alphanumeric characters and underscore allowed")
  }

  protected final val projectionTableName = "projection_" + tableName

  protected val tableNameSQL = SQLSyntax.createUnsafely(projectionTableName)

  init()

  protected def init(): Unit = {
    createTableIfNotExists()
    addSpaceColumn()
    dropMetadataColumn()

    if(indicies.map(_.uniqueId).distinct.size < indicies.size) {
      throw new IllegalStateException("Indices for projection " + tableName+" are not unique")
    }

    val exisitingIndicesNames = readExistingIndices()

    val indicesToDrop = exisitingIndicesNames.filterNot(ei => {
      val existingId = existingIndexUniqueId(ei)
      indicies.exists(i => i.uniqueId == existingId)
    })

    indicesToDrop.foreach(index => dropIndex(index))

    val indicesToCreate = indicies.filterNot(i => {
      exisitingIndicesNames.exists(ei => existingIndexUniqueId(ei) == i.uniqueId)
    })

    indicesToCreate.foreach(index => createIndex(index))
  }

  private def existingIndexUniqueId(indexName: String): Int = {
    indexName.substring(indexName.lastIndexOf("_") + 1).toInt
  }

  protected def createTableIfNotExists(): Unit = DB.autoCommit { implicit session =>
    sql"""CREATE TABLE IF NOT EXISTS ${tableNameSQL} (
          |space_id BIGINT NOT NULL,
          |id BIGINT NOT NULL PRIMARY KEY,
          |version INT NOT NULL,
          |document JSONB NOT NULL)"""
      .stripMargin.execute().apply()
  }

  protected def addSpaceColumn(): Unit = DB.autoCommit { implicit session =>
    sql"""ALTER TABLE ${tableNameSQL} ADD COLUMN IF NOT EXISTS space_id BIGINT NOT NULL DEFAULT -1"""
      .stripMargin.execute().apply()
  }

  protected def dropMetadataColumn(): Unit = DB.autoCommit { implicit session =>
    sql"""ALTER TABLE ${tableNameSQL} DROP COLUMN IF EXISTS metadata"""
      .stripMargin.execute().apply()
  }


  private def readExistingIndices(): Iterable[String] = {
    DB.readOnly { implicit session =>
      SQL(s"SELECT indexname FROM pg_indexes WHERE tablename = '${projectionTableName}' and indexname like '%_idx_%'")
        .map(rs => rs.string(1)).list().apply()
    }
  }

  private def dropIndex(indexName: String): Unit = DB.autoCommit { implicit session =>
    SQL(s"DROP INDEX IF EXISTS ${indexName}").execute().apply()
  }

  private def createIndex(index: Index): Unit = DB.autoCommit { implicit session =>
    index match {
      case MultipleIndex(uniqueId, path) =>
        val p = path.mkString(",")
        SQL(s"CREATE INDEX ${projectionTableName}_idx_${uniqueId} ON ${projectionTableName} ((document #>> '{$p}'))").execute().apply()
      case UniqueIndex(uniqueId, path) =>
        val p = path.mkString(",")
        SQL(s"CREATE UNIQUE INDEX ${projectionTableName}_idx_${uniqueId} ON ${projectionTableName} ((document #>> '{$p}'))").execute().apply()
    }
  }

  def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute().apply()
  }

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      val updated = sql"UPDATE ${tableNameSQL} SET document = ?::JSONB, version = version + 1 WHERE id = ? RETURNING version"
        .bind(mpjsons.serialize[T](document), key)
        .map(_.int(1)).list().apply()

      if (updated.size != 1) {
        throw new IllegalStateException("Expected 1, updated " + updated + " records")
      } else {
        cache.put(key, Some(VersionedDocument(updated.head, document)))
      }
    }
  }



  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]] = {

    cache.get(key) match {
      case InCache(VersionedDocument(version, document)) => Some(Document(document))
      case InCacheEmpty => None
      case NotInCache =>
        inSession { implicit session =>
          val loaded = sql"SELECT version, document FROM ${tableNameSQL} WHERE id = ?"
            .bind(key).map(rs => {
            VersionedDocument[T](rs.int(1), mpjsons.deserialize[T](rs.string(2)))
          }).single().apply()
          loaded match {
            case Some(doc) =>
              cache.put(key, Some(doc))
              Some(Document(doc.document))
            case None =>
              cache.put(key, None)
              None
          }
        }
      }
  }

  def removeDocument(key: Long)(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      sql"DELETE FROM ${tableNameSQL} WHERE id = ?"
        .bind(key).executeUpdate().apply()
      cache.put(key, None)
    }

  }


  private def createQuery(searchParams: DocumentStoreQuery) = {

    val sortPart = if(searchParams.sortBy.isEmpty) "" else searchParams.sortBy.map({
      case SortAscInt(path) => "(document::json#>>'{"+path.mkString(",")+"}')::bigint " + " ASC"
      case SortDescInt(path) => "(document::json#>>'{"+path.mkString(",")+"}')::bigint " + " DESC"
      case SortAscText(path) => "document::json#>>'{" + path.mkString(",") + "}' " + " ASC"
      case SortDescText(path) => "document::json#>>'{" + path.mkString(",") + "}' " + " DESC"
    }).mkString(" ORDER BY ", ", ", "")

    SQL("SELECT id, version, document FROM " + projectionTableName +
      " WHERE" + constructWhereClauseForExpectedValues(searchParams.where) +
      sortPart +
      " LIMIT " + searchParams.limit + " OFFSET " + searchParams.offset)
  }

  private def createPartsQuery(parts: Seq[Seq[String]], searchParams: DocumentStoreQuery) = {

    val partsQuery = parts.map(part => "document::json#>>'{"+part.mkString(",")+"}'").mkString(", ")

    val sortPart = if(searchParams.sortBy.isEmpty) "" else searchParams.sortBy.map({
      case SortAscInt(path) => "(document::json#>>'{" + path.mkString(",") + "})::bigint' " + " ASC"
      case SortDescInt(path) => "(document::json#>>'{" + path.mkString(",") + "})::bigint' " + " DESC"
      case SortAscText(path) => "document::json#>>'{" + path.mkString(",") + "}' " + " ASC"
      case SortDescText(path) => "document::json#>>'{" + path.mkString(",") + "}' " + " DESC"
    }).mkString(" ORDER BY ", ", ", "")

    SQL("SELECT id, " + partsQuery + " FROM " + projectionTableName +
      " WHERE" + constructWhereClauseForExpectedValues(searchParams.where) +
      sortPart +
      " LIMIT " + searchParams.limit + " OFFSET " + searchParams.offset)

  }

  def findDocument(searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, Document[T]] = {
    val query = createQuery(searchParams)
    inSession { implicit session =>
      val loaded = query
        .bind(getAllValues(searchParams.where): _*).map(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
        .list().apply()
      loaded.foreach(t => cache.put(t._1, Some(t._2)))
      loaded.map({case (key, value) => (key, Document[T](value.document))}).toMap
    }
  }

  def findDocumentWithTransform[TT <: AnyRef](searchParams: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Map[Long, Document[TT]] = {
    val query = createQuery(searchParams)
    inSession { implicit session =>
      val loaded = query
        .bind(getAllValues(searchParams.where): _*).map(rs => {
        val id = rs.long(1)
        val version = rs.int(2)
        val beforeTransform = mpjsons.deserialize[T](rs.string(3))
        cache.put(id, Some(VersionedDocument[T](version, beforeTransform)))
        id -> VersionedDocument[TT](version, transform(beforeTransform))
      }).list().apply()
      loaded.map({case (key, value) => (key, Document[TT](value.document))}).toMap
    }
  }

  def findDocumentPartByPaths[P: TypeTag](part: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, P] = {
    val query = createPartsQuery(Seq(part), searchParams)

    inSession { implicit session =>
      query
        .bind(getAllValues(searchParams.where): _*)
        .map(rs => rs.long(1) -> mpjsons.deserialize[P](rs.string(2)))
        .list().apply().toMap
    }
  }

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: Seq[String], part2: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2)] = {
    val query = createPartsQuery(Seq(part1, part2), searchParams)

    inSession { implicit session =>
      val loaded = query
        .bind(getAllValues(searchParams.where): _*)
        .map(rs => rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3))))
        .list().apply()

      loaded.toMap
    }
  }

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2, P3)] = {
    val query = createPartsQuery(Seq(part1, part2, part3), searchParams)

    inSession { implicit session =>
      val loaded = query
        .bind(getAllValues(searchParams.where): _*)
        .map(rs => rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3)), mpjsons.deserialize[P3](rs.string(4))))
        .list().apply()

      loaded.toMap
    }
  }

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], part4: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2, P3, P4)] = {
    val query = createPartsQuery(Seq(part1, part2, part3, part4), searchParams)

    inSession { implicit session =>
      val loaded = query
        .bind(getAllValues(searchParams.where): _*)
        .map(rs => rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3)), mpjsons.deserialize[P3](rs.string(4)), mpjsons.deserialize[P4](rs.string(5))))
        .list().apply()

      loaded.toMap
    }
  }

  private def constructWhereClauseForExpectedValues(values: Seq[ExpectedValue]): String = {
    values.map{
      case ExpectedNoValue(path) => s"document #>> '{${path.mkString(",")}}' IS NULL"
      case ExpectedMultipleValues(path, v) => s"document #>> '{${path.mkString(",")}}' in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedMultipleIntValues(path, v) => s"(document #>> '{${path.mkString(",")}}')::int in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedMultipleLongValues(path, v) => s"(document #>> '{${path.mkString(",")}}')::bigint in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedSingleValue(path, _) => s"document #>> '{${path.mkString(",")}}' = ?"
      case ExpectedSingleValueLike(path, _) => s"document #>> '{${path.mkString(",")}}' ilike ?"
      case ExpectedSingleValueInArray(path, v) => s"(document #> '{${path.mkString(",")}}') ?? ?"
      case ExpectedMultipleValuesInArray(path, v) => s"(document #> '{${path.mkString(",")}}') ??| ARRAY[${List.fill(v.size)("?").mkString(",")}]"
      case ExpectedSingleIntValue(path, _) => s"(document #>> '{${path.mkString(",")}}')::int = ?"
      case ExpectedSingleLongValue(path, _) => s"(document #>> '{${path.mkString(",")}}')::bigint = ?"
      case ExpectedGreaterThanIntValue(path, _) => s"(document #>> '{${path.mkString(",")}}')::int > ?"
      case ExpectedLessThanIntValue(path, _) => s"(document #>> '{${path.mkString(",")}}')::int < ?"

    }.mkString(" ", " AND ", " ")
  }

  private def getAllValues(values: Seq[ExpectedValue]): Seq[Any] = {
    values.flatMap{
      case ExpectedNoValue(_) => Iterable.empty
      case ExpectedMultipleValues(_, vals) => vals
      case ExpectedMultipleIntValues(_, vals) => vals
      case ExpectedMultipleLongValues(_, vals) => vals
      case ExpectedSingleValue(_, value) => Iterable(value)
      case ExpectedSingleValueInArray(_, value) => Iterable(value)
      case ExpectedMultipleValuesInArray(_, vals) => vals
      case ExpectedSingleValueLike(_, value) => Iterable(value)
      case ExpectedSingleIntValue(_, value) => Iterable(value)
      case ExpectedSingleLongValue(_, value) => Iterable(value)
      case ExpectedGreaterThanIntValue(_, value) => Iterable(value)
      case ExpectedLessThanIntValue(_, value) => Iterable(value)
    }
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T]] = {
    findDocumentByObjectInArray("document", arrayPath, objectPath, value)
  }

  protected def findDocumentByObjectInArray[V](columnName: String, array: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession): Map[Long, Document[T]] = {

    def QUERY(arrayPath: String, path: String) =
      s"SELECT id, version, document FROM $projectionTableName WHERE $columnName #> '$arrayPath' @> '[$path]' LIMIT 10000"

    def makeJson(path: Seq[String], value: V): String =
      path match {
        case head :: tail => "{\"" + head + "\":" + makeJson(tail, value) + "}"
        case Nil => value match {
          case s: String => "\"" + s + "\""
          case anything => anything.toString
        }
      }

    inSession { implicit session =>
      val loaded = SQL(QUERY(array.mkString("{", ",", "}"), makeJson(objectPath, value)))
        .map(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
        .list().apply()

      loaded.foreach(t => cache.put(t._1, Some(t._2)))
      loaded.map({case (key, v) => (key, Document[T](v.document))}).toMap
    }
  }


  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]] = {
    val loaded = inSession { implicit session =>
      sql"SELECT id, version, document FROM ${tableNameSQL}"
          .map(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
        .list.apply()
    }
    loaded.foreach(t => cache.put(t._1, Some(t._2)))
    loaded.map(v => (v._1, Document(v._2.document))).toMap
  }

  def countAll()(implicit session: DBSession = null): Int = {
    inSession { implicit session =>
      sql"SELECT count(*) FROM ${tableNameSQL}"
        .map(rs => rs.int(1))
        .single().apply().get
    }
  }


  def getDocuments(keys: Iterable[Long])(implicit session: DBSession = null): Map[Long, Document[T]] = {

    val keysSet: Set[Long] = keys.toSet
    val fromCache = cache.getAll(keysSet)
    val fromCacheFound = fromCache.collect {
      case (k, InCache(value)) => (k, value)
    }
    val fromCacheFoundOrEmpty = fromCache.collect {
      case (k, InCache(value)) => k
      case (k, InCacheEmpty) => k
    }

    val toLoad = keysSet -- fromCacheFoundOrEmpty

    val fromCacheFoundValues = fromCacheFound.map {case (key, value) => (key, Document(value.document))}

    if (toLoad.isEmpty) {
      fromCacheFoundValues
    } else {
      val loaded = inSession { implicit session =>
        sql"SELECT id, version, document FROM ${tableNameSQL} WHERE id IN (${toLoad})"
          .map(rs => {
            rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3)))
          }).list().apply()
      }
      toLoad.foreach(id => {
        cache.put(id, loaded.find(_._1 == id).map(_._2))
      })
      fromCacheFoundValues ++ loaded.map(v => (v._1, Document(v._2.document))).toMap
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

  protected def getFromCacheOrDB(key: Long)(implicit session: DBSession): Option[VersionedDocument[T]] = {
    cache.get(key).getOrElse(sql"SELECT version, document FROM ${tableNameSQL} WHERE id = ?"
      .bind(key).map(rs => {
      VersionedDocument[T](rs.int(1), mpjsons.deserialize[T](rs.string(2)))
    }).single().apply())
  }


  def clearSpace(spaceId: Long)(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      val idsRemoved = sql"DELETE from ${tableNameSQL} WHERE space_id = ? RETURNING id".
        bind(spaceId)
        .map(_.long(1)).list().apply()

      idsRemoved.foreach(id => cache.remove(id))
    }
  }

}

class PostgresDocumentStore[T <: AnyRef](val tableName: String, val mpjsons: MPJsons,
                                                      val cache: DocumentStoreCache[T], val indicies: Seq[Index] = Seq.empty)(implicit val t: TypeTag[T])
  extends DocumentStore[T] with PostgresDocumentStoreTrait[T] {

  override def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, ?, 1, ?::jsonb)"
        .bind(spaceId, key, mpjsons.serialize(document))
        .executeUpdate().apply()
      cache.put(key, Some(VersionedDocument(1, document)))
    }
  }

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T] = {
    updateExistingDocumentRecur(key, modify, 1)
  }

  private def updateExistingDocumentRecur(key: Long, modify: Document[T] => Document[T], tryNumber: Int)(implicit session: DBSession): Document[T] = {
    inSession { implicit session =>
      getFromCacheOrDB(key) match {
        case None => throw new IllegalStateException("Document for update does not exist " + key)
        case Some(VersionedDocument(version, document)) =>
          val modified = modify(Document(document))

          val updated = sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB WHERE id = ? AND version = ?"
            .bind(version + 1, mpjsons.serialize[T](modified.document), key, version)
            .map(_.int(1)).single().executeUpdate().apply()

          if(updated == 0) {
            if(tryNumber < 10) {
              updateExistingDocumentRecur(key, modify, tryNumber + 1)
            } else {
              throw new IllegalStateException("Too many tries to update document in table = ["+tableNameSQL+"] id = " + key + " expected version " + version)
            }
          } else {
            cache.put(key, Some(VersionedDocument(version + 1, modified.document)))
            modified
          }
      }
    }
  }
  // Optimistic locking update
  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T] = {
    updateDocumentRecur(spaceId, key, modify, 1)
  }


  // Optimistic locking update
  private def updateDocumentRecur(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T], tryNumber: Int)(implicit session: DBSession): Document[T] = {

    inSession { implicit session =>
      getFromCacheOrDB(key) match {
        case None =>
          val modified = modify(None)
          insertDocument(spaceId, key, modified.document)
          modified
        case Some(VersionedDocument(version, document)) =>
          val modified = modify(Some(Document(document)))

          val updated = sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB WHERE id = ? AND version = ?"
            .bind(version + 1, mpjsons.serialize[T](modified.document), key, version)
            .map(_.int(1)).single().executeUpdate().apply()

          if(updated == 0) {
            if(tryNumber < 10) {
              updateDocumentRecur(spaceId, key, modify, tryNumber + 1)
            } else {
              throw new IllegalStateException("Too many tries to update document in table = ["+tableNameSQL+"] id = " + key + " expected version " + version)
            }

          } else {
            cache.put(key, Some(VersionedDocument(version + 1, modified.document)))
            modified
          }
      }
    }
  }


  def clearAllData()(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"TRUNCATE TABLE ${tableNameSQL}".executeUpdate().apply()
      cache.clear()
    }
  }

}

class PostgresDocumentStoreAutoId[T <: AnyRef](val tableName: String, val mpjsons: MPJsons,
                                                            val cache: DocumentStoreCache[T], val indicies: Seq[Index] = Seq.empty)(implicit val t: TypeTag[T])
  extends DocumentStoreAutoId[T] with PostgresDocumentStoreTrait[T] {

  protected final lazy val sequenceName = "sequence_" + tableName

  protected final lazy val sequenceNameSQL = SQLSyntax.createUnsafely(sequenceName)


  override protected def createTableIfNotExists(): Unit = {
    super.createTableIfNotExists()
    try {
      DB.autoCommit { implicit session =>
        sql"CREATE SEQUENCE ${sequenceNameSQL} START 1"
          .executeUpdate().apply()
      }
    } catch {
      case e: PSQLException if e.getServerErrorMessage.getSQLState == "42P07" => () // IF NOT EXIST workaround, 42P07 - PostgreSQL duplicate_table
    }
  }

  override def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute().apply()

    sql"""DROP SEQUENCE ${sequenceNameSQL}"""
      .stripMargin.execute().apply()

  }

  override def insertDocument(spaceId: Long, document: T)(implicit session: DBSession): Long = {
    inSession {implicit session =>
      val key = sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, nextval('${sequenceNameSQL}'), 1, ?::jsonb) RETURNING currval('${sequenceNameSQL}')"
          .bind(spaceId, mpjsons.serialize(document))
          .map(_.long(1)).single().apply().get
      cache.put(key, Some(VersionedDocument(1, document)))
      key
    }
  }

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T] = {

    inSession { implicit session =>
      getFromCacheOrDB(key) match {
        case None => throw new IllegalStateException(s"Document for key $key not found!")
        case Some(VersionedDocument(version, document)) =>
          val modified = modify(Document(document))

          val updated = sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB WHERE id = ? AND version = ?"
            .bind(version + 1, mpjsons.serialize[T](modified.document), key, version)
            .map(_.int(1)).single().executeUpdate().apply()

          if(updated == 0) {
            updateExistingDocument(key, modify)
          } else {
            cache.put(key, Some(VersionedDocument(version + 1, modified.document)))
            modified
          }
      }
    }
  }


  // Optimistic locking update
  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T] = {

    inSession { implicit session =>
      getFromCacheOrDB(key) match {
        case None => throw new IllegalStateException(s"Document for key $key not found!")
        case Some(VersionedDocument(version, document)) =>
          val modified = modify(Some(Document(document)))

          val updated = sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB WHERE id = ? AND version = ?"
            .bind(version + 1, mpjsons.serialize[T](modified.document), key, version)
            .map(_.int(1)).single().executeUpdate().apply()

          if(updated == 0) {
            updateDocument(spaceId, key, modify)
          } else {
            cache.put(key, Some(VersionedDocument(version + 1, modified.document)))
            modified
          }
      }
    }
  }

  def clearAllData()(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"TRUNCATE TABLE ${tableNameSQL}".executeUpdate().apply()
      sql"ALTER SEQUENCE ${sequenceNameSQL} RESTART WITH 1".executeUpdate().apply()
      cache.clear()
    }
  }

}

