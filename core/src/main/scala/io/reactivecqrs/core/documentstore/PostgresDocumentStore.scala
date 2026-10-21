package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import org.postgresql.util.PSQLException
import scalikejdbc.{DB, DBSession}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._
import scalikejdbc._
import io.reactivecqrs.core.util.{AfterCommit, Ddl, SchemaLock}
import io.reactivecqrs.core.util.SqlLogging.withoutValues

object PostgresDocumentStoreTrait {
  private[documentstore] val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresDocumentStore[_]])

  /** F217: the index on `space_id` of `table`, with whether it is valid; none when the table has none. */
  private[documentstore] def spaceIndexes(table: String)(implicit session: DBSession): List[(String, Boolean)] =
    sql"""SELECT c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
         | JOIN pg_class t ON t.oid = i.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace
         | JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = i.indkey[0]
         | WHERE t.relname = $table AND n.nspname = current_schema() AND i.indnatts = 1 AND i.indexprs IS NULL
         | AND a.attname = 'space_id'""".stripMargin.map(rs => (rs.string(1), rs.boolean(2))).list()
}

/** The document tables' tools. */
object PostgresDocumentStore {

  /** F217: gives each document table of the current schema without one an index on `space_id`, by which an
    * organization's purge and targeted rebuild delete, built with `CREATE INDEX CONCURRENTLY`, so writes go on. A table
    * gets it as it is created; run this once, in the background, for tables created before 1.0.1. An index that an
    * interrupted build left invalid is built again. Returns the tables it indexed. */
  def buildSpaceIndexesConcurrently(): Seq[String] = DB.autoCommit(buildSpaceIndexesIn(_))

  /** CR-3: `buildSpaceIndexesConcurrently()` on the connection of `session`, such as the online jobs' own. */
  private[core] def buildSpaceIndexesIn(implicit session: DBSession): Seq[String] = {
    val tables = sql"""SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'space_id'"""
      .map(_.string(1)).list().filter(table => table.startsWith("projection_") && table.matches("[a-z0-9_]+"))
    tables.filterNot(table => PostgresDocumentStoreTrait.spaceIndexes(table).exists(_._2)).map { table =>
      PostgresDocumentStoreTrait.spaceIndexes(table).foreach { case (invalid, _) => SQL(s"DROP INDEX CONCURRENTLY IF EXISTS $invalid").execute.apply() }
      PostgresDocumentStoreTrait.log.info("Building an index on space_id of " + table)
      SQL(s"CREATE INDEX CONCURRENTLY ON $table (space_id)").execute.apply()
      table
    }
  }
}

sealed trait PostgresDocumentStoreTrait[T <: AnyRef] {

  implicit val t: TypeTag[T]

  val tableName: String
  val mpjsons: MPJsons
  val cache: DocumentStoreCache[T]
  val indicies: Seq[Index]
  /** F211: the most documents a query without a limit may find; with more it fails (`TooManyDocumentsException`). */
  val maxRows: Int


  if (!tableName.matches( """[a-zA-Z0-9\_]+""")) {
    throw new IllegalArgumentException("Invalid table name, only alphanumeric characters and underscore allowed")
  }

  protected final val projectionTableName = "projection_" + tableName

  protected val tableNameSQL = SQLSyntax.createUnsafely(projectionTableName)

  // PostgreSQL cuts longer names silently, so two tables or indices could get one name, and an index id would be lost.
  private val longestName = (projectionTableName +: indicies.map(index => projectionTableName + "_idx_" + index.uniqueId)).maxBy(_.length)
  if (longestName.length > 63) {
    throw new IllegalArgumentException("Table name " + tableName + " is too long: " + longestName + " has more than 63 characters")
  }

  init()

  protected def init(): Unit = SchemaLock {
    DB.autoCommit { implicit session =>
      val existed = Ddl.has(projectionTableName)
      createTableIfNotExists()
      // F217: a new table is indexed at once; an older one by `PostgresDocumentStore.buildSpaceIndexesConcurrently`.
      if (!existed && PostgresDocumentStoreTrait.spaceIndexes(projectionTableName).isEmpty) sql"""CREATE INDEX ON $tableNameSQL (space_id)""".execute()
    }
    // F43: the columns change in one transaction; F185: only when they are not as they should be (Ddl).
    Ddl.change("migrating " + projectionTableName,
      implicit session => Ddl.hasColumn(projectionTableName, "space_id") && !Ddl.hasColumn(projectionTableName, "metadata")) { implicit session =>
      addSpaceColumn()
      dropMetadataColumn()
    }
    createSequenceIfNotExists()

    if(indicies.map(_.uniqueId).distinct.size < indicies.size) {
      throw new IllegalStateException("Indices for projection " + tableName+" are not unique")
    }

    if (OutdatedTables.contains(tableName)) {
      // F189: the table of a build older than the code keeps its indexes; the code's come with its own build.
      PostgresDocumentStoreTrait.log.info("Indexes of " + projectionTableName + " are left as they are: it is the table of an older version of its projection")
    } else syncIndices()
  }

  private def syncIndices(): Unit = {
    val existing = readExistingIndices()
    existing.keys.filterNot(id => indicies.exists(_.uniqueId == id)).foreach(id => dropIndex(indexName(id)))
    indicies.foreach { index =>
      existing.get(index.uniqueId) match {
        case None => createIndex(index)
        case Some(definition) if sameDefinition(definition, index) => ()
        case Some(definition) =>
          // F171: declared with the same id and another path (a field renamed, or given a short JSON name).
          PostgresDocumentStoreTrait.log.warn("Index " + indexName(index.uniqueId) + " of " + projectionTableName + " is built again, " +
            "as its definition changed; it was: " + definition)
          dropIndex(indexName(index.uniqueId))
          createIndex(index)
      }
    }
  }

  private def indexName(uniqueId: Int): String = projectionTableName + "_idx_" + uniqueId

  private def createTableIfNotExists()(implicit session: DBSession): Unit = {
    sql"""CREATE TABLE IF NOT EXISTS ${tableNameSQL} (
          |space_id BIGINT NOT NULL,
          |id BIGINT NOT NULL PRIMARY KEY,
          |version INT NOT NULL,
          |document JSONB NOT NULL)"""
      .stripMargin.execute()
  }

  private def addSpaceColumn()(implicit session: DBSession): Unit = {
    sql"""ALTER TABLE ${tableNameSQL} ADD COLUMN IF NOT EXISTS space_id BIGINT NOT NULL DEFAULT -1"""
      .stripMargin.execute()
  }

  private def dropMetadataColumn()(implicit session: DBSession): Unit = {
    sql"""ALTER TABLE ${tableNameSQL} DROP COLUMN IF EXISTS metadata"""
      .stripMargin.execute()
  }

  /** The auto-id store's sequence. */
  protected def createSequenceIfNotExists(): Unit = ()


  // The table's indexes named as this store names them, by id, with their definitions. F171: the pattern '%_idx_%' also
  // took the primary key of a table whose name holds "idx".
  private def readExistingIndices(): Map[Int, String] = {
    val named = (java.util.regex.Pattern.quote(projectionTableName) + "_idx_(\\d+)").r
    DB.readOnly { implicit session =>
      SQL(s"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = '${projectionTableName}'")
        .map(rs => (rs.string(1), rs.string(2))).list()
    }.collect { case (named(id), definition) => id.toInt -> definition }.toMap
  }

  // F171: the database's definition against the declared one, both reduced to what tells this store's indexes apart:
  // uniqueness, method and expressions. PostgreSQL writes a path as '{a,b}'::text[], int as integer, and more parentheses.
  private def sameDefinition(definition: String, index: Index): Boolean = {
    val (unique, method, columns) = indexSql(index)
    def reduced(text: String) = text.replace("::text[]", "").replace("::integer", "::int").replaceAll("[\\s()]", "")
    val methodAt = definition.indexOf(" USING ")
    methodAt >= 0 && definition.startsWith("CREATE UNIQUE INDEX") == unique && reduced(definition.substring(methodAt + 7)) == reduced(method + columns)
  }

  // F185: with a bounded wait for the table's lock (Ddl).
  private def dropIndex(indexName: String): Unit = Ddl.dropIndex(indexName)

  private def createIndex(index: Index): Unit = {
    val (unique, method, columns) = indexSql(index)
    Ddl.createIndex(indexName(index.uniqueId),
      s"CREATE ${if (unique) "UNIQUE " else ""}INDEX ${indexName(index.uniqueId)} ON ${projectionTableName} USING $method ($columns)")
  }

  /** Whether the index is unique, its method, and its expressions, as created. */
  private def indexSql(index: Index): (Boolean, String, String) = {
    def text(path: Seq[String]) = s"(document #>> ${JsonPath.literal(path)})"
    def typed(path: Seq[String], cast: String) = s"((document #> ${JsonPath.literal(path)})::$cast)"
    index match {
      case MultipleCombinedIndex(_, paths) => (false, "btree", pathsToQuery(paths))
      case UniqueCombinedIndex(_, paths) => (true, "btree", pathsToQuery(paths))
      case MultipleTextIndex(_, path) => (false, "btree", text(path))
      case UniqueTextIndex(_, path) => (true, "btree", text(path))
      case MultipleLongIndex(_, path) => (false, "btree", typed(path, "bigint"))
      case UniqueLongIndex(_, path) => (true, "btree", typed(path, "bigint"))
      case MultipleIntIndex(_, path) => (false, "btree", typed(path, "int"))
      case UniqueIntIndex(_, path) => (true, "btree", typed(path, "int"))
      case MultipleTextArrayIndex(_, path) => (false, "gin", s"(document #> ${JsonPath.literal(path)})")
    }
  }

  private def pathsToQuery(paths: Seq[IndexPath]): String = {
    paths.map {
      case path: TextIndexPath => s"(document #>> ${JsonPath.literal(path.path)})"
      case path: LongIndexPath => s"((document #> ${JsonPath.literal(path.path)})::bigint)"
      case path: IntIndexPath => s"((document #> ${JsonPath.literal(path.path)})::int)"
    }.mkString(",")
  }

  def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute()
  }

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit = {

    inSession { implicit session =>

      // Skipping the JSON write is only safe when the cached copy is still current; the version check proves it.
      val versionWhenUnchanged = getFromCacheOrDB(key) match {
        case Some(VersionedDocument(version, cachedDocument)) if cachedDocument == document =>
          sql"UPDATE ${tableNameSQL} SET version = version + 1 WHERE id = ? AND version = ? RETURNING version"
            .bind(key, version)
            .map(_.int(1)).single()
        case _ => None
      }

      val newVersion = versionWhenUnchanged.getOrElse {
        sql"UPDATE ${tableNameSQL} SET document = ?::JSONB, version = version + 1 WHERE id = ? RETURNING version"
          .bind(mpjsons.serialize[T](document), key)
          .map(_.int(1)).list() match {
          case List(version) => version
          case updated => throw new IllegalStateException("Expected 1, updated " + updated + " records")
        }
      }

      cacheAfterCommit(key, Some(VersionedDocument(newVersion, document)))
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
          }).single()
          loaded match {
            case Some(doc) =>
              cacheAfterCommit(key, Some(doc))
              Some(Document(doc.document))
            case None =>
              cacheAfterCommit(key, None)
              None
          }
        }
      }
  }

  def removeDocument(key: Long)(implicit session: DBSession): Unit = {

    inSession { implicit session =>
      sql"DELETE FROM ${tableNameSQL} WHERE id = ?"
        .bind(key).update()
      cacheAfterCommit(key, None)
    }

  }


  private def createQuery(searchParams: DocumentStoreQuery) = {

    SQL("SELECT id, version, document FROM " + projectionTableName + whereClause(searchParams.where) + orderAndPage(searchParams))
  }

  private def createPartsQuery(parts: Seq[Seq[String]], searchParams: DocumentStoreQuery) = {

    val partsQuery = parts.map(part => "document#>" + JsonPath.literal(part)).mkString(", ") // JSON, as the parts are read with mpjsons (F159)

    SQL("SELECT id, " + partsQuery + " FROM " + projectionTableName + whereClause(searchParams.where) + orderAndPage(searchParams))
  }

  // F211: a query without a limit reads one row more than the store's `maxRows`, so that more fail (`run`).
  private def orderAndPage(query: DocumentStoreQuery): String = {
    val order = if (query.sortBy.isEmpty) "" else query.sortBy.map(Sort.of).map { case (path, cast, descending) =>
      cast.fold("document #>> " + JsonPath.literal(path))(c => "(document #> " + JsonPath.literal(path) + ")::" + c) + (if (descending) " DESC" else " ASC")
    }.mkString(" ORDER BY ", ", ", "")
    val limit = if (query.limit >= 0) query.limit.toLong else maxRows.toLong + 1
    order + " LIMIT " + limit + (if (query.offset > 0) " OFFSET " + query.offset else "")
  }

  // Runs a query of `createQuery` or `createPartsQuery` with its values bound, reading each row with `read`.
  private def run[R](searchParams: DocumentStoreQuery, query: SQL[Nothing, NoExtractor])(read: WrappedResultSet => R)(implicit session: DBSession): List[R] = {
    val rows = query.bind(getAllValues(searchParams.where): _*).map(read).list()
    if (searchParams.limit < 0 && rows.size > maxRows) throw new TooManyDocumentsException(tableName, maxRows) else rows
  }

  def findDocument(searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, Document[T])] = inSession { implicit session =>
    val loaded = run(searchParams, createQuery(searchParams))(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
    loaded.foreach(t => cacheAfterCommit(t._1, Some(t._2)))
    loaded.map({case (key, value) => (key, Document[T](value.document))})
  }

  def findDocumentWithTransform[TT <: AnyRef](searchParams: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Seq[(Long, Document[TT])] =
    inSession { implicit session =>
      run(searchParams, createQuery(searchParams)) { rs =>
        val (id, document) = (rs.long(1), mpjsons.deserialize[T](rs.string(3)))
        cacheAfterCommit(id, Some(VersionedDocument[T](rs.int(2), document)))
        id -> Document[TT](transform(document))
      }
    }

  def findDocumentPartByPaths[P: TypeTag](part: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, P)] =
    inSession { implicit session =>
      run(searchParams, createPartsQuery(Seq(part), searchParams))(rs => rs.long(1) -> mpjsons.deserialize[P](rs.string(2)))
    }

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: Seq[String], part2: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2))] =
    inSession { implicit session =>
      run(searchParams, createPartsQuery(Seq(part1, part2), searchParams))(rs =>
        rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3))))
    }

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3))] =
    inSession { implicit session =>
      run(searchParams, createPartsQuery(Seq(part1, part2, part3), searchParams))(rs =>
        rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3)), mpjsons.deserialize[P3](rs.string(4))))
    }

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], part4: Seq[String], searchParams: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3, P4))] =
    inSession { implicit session =>
      run(searchParams, createPartsQuery(Seq(part1, part2, part3, part4), searchParams))(rs =>
        rs.long(1) -> (mpjsons.deserialize[P1](rs.string(2)), mpjsons.deserialize[P2](rs.string(3)), mpjsons.deserialize[P3](rs.string(4)), mpjsons.deserialize[P4](rs.string(5))))
    }

  // F159: no condition, no WHERE (the memory store then finds every document too).
  private def whereClause(values: Seq[ExpectedValue]): String =
    if (values.isEmpty) "" else " WHERE" + constructWhereClauseForExpectedValues(values)

  // F309: one of no values matches nothing (`FALSE`), as in the memory store; `in ()` and an empty array are invalid SQL,
  // which a listener would fail on for ever. The empty lists bind no values, so the others keep their places.
  private def constructWhereClauseForExpectedValues(values: Seq[ExpectedValue]): String = {
    values.map{
      case ExpectedMultipleTextValues(_, v) if v.isEmpty => "FALSE"
      case ExpectedMultipleIntValues(_, v) if v.isEmpty => "FALSE"
      case ExpectedMultipleLongValues(_, v) if v.isEmpty => "FALSE"
      case ExpectedMultipleTextValuesInArray(_, v) if v.isEmpty => "FALSE"
      case ExpectedNoValue(path) => s"document #>> ${JsonPath.literal(path)} IS NULL"
      case ExpectedMultipleTextValues(path, v) => s"document #>> ${JsonPath.literal(path)} in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedMultipleIntValues(path, v) => s"(document #> ${JsonPath.literal(path)})::int in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedMultipleLongValues(path, v) => s"(document #> ${JsonPath.literal(path)})::bigint in (${List.fill(v.size)("?").mkString(",")})"
      case ExpectedSingleTextValue(path, _) => s"document #>> ${JsonPath.literal(path)} = ?"
      case ExpectedSingleTextValueLike(path, _) => s"document #>> ${JsonPath.literal(path)} ilike ?"
      case ExpectedSingleTextValueInArray(path, v) => s"(document #> ${JsonPath.literal(path)}) ?? ?"
      case ExpectedMultipleTextValuesInArray(path, v) => s"(document #> ${JsonPath.literal(path)}) ??| ARRAY[${List.fill(v.size)("?").mkString(",")}]"
      case ExpectedSingleIntValue(path, _) => s"(document #> ${JsonPath.literal(path)})::int = ?"
      case ExpectedSingleLongValue(path, _) => s"(document #> ${JsonPath.literal(path)})::bigint = ?"
      case ExpectedSingleBooleanValue(path, _) => s"(document #> ${JsonPath.literal(path)})::boolean = ?"
      case ExpectedGreaterThanIntValue(path, _) => s"(document #> ${JsonPath.literal(path)})::int > ?"
      case ExpectedLessThanIntValue(path, _) => s"(document #> ${JsonPath.literal(path)})::int < ?"

    }.mkString(" ", " AND ", " ")
  }

  private def getAllValues(values: Seq[ExpectedValue]): Seq[Any] = {
    values.flatMap{
      case ExpectedNoValue(_) => Iterable.empty
      case ExpectedMultipleTextValues(_, vals) => vals
      case ExpectedMultipleIntValues(_, vals) => vals
      case ExpectedMultipleLongValues(_, vals) => vals
      case ExpectedSingleTextValue(_, value) => Iterable(value)
      case ExpectedSingleTextValueInArray(_, value) => Iterable(value)
      case ExpectedMultipleTextValuesInArray(_, vals) => vals
      case ExpectedSingleTextValueLike(_, value) => Iterable(value)
      case ExpectedSingleIntValue(_, value) => Iterable(value)
      case ExpectedSingleLongValue(_, value) => Iterable(value)
      case ExpectedSingleBooleanValue(_, value) => Iterable(value)
      case ExpectedGreaterThanIntValue(_, value) => Iterable(value)
      case ExpectedLessThanIntValue(_, value) => Iterable(value)
    }
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V, limit: Int)(implicit session: DBSession = null): Map[Long, Document[T]] = {
    findDocumentByObjectInArray("document", arrayPath, objectPath, value, limit)
  }

  // F158: the path and the JSON are parameters, and the JSON is written by an encoder, so no value can change the query.
  protected def findDocumentByObjectInArray[V](columnName: String, array: Seq[String], objectPath: Seq[String], value: V, limit: Int)(implicit session: DBSession): Map[Long, Document[T]] = {

    def json(path: List[String]): String = path match {
      case head :: tail => "{" + jsonString(head) + ":" + json(tail) + "}"
      case Nil => value match {
        case number @ (_: Int | _: Long | _: Short | _: Byte | _: Double | _: Float | _: BigInt | _: BigDecimal | _: Boolean) => number.toString
        case anything => jsonString(anything.toString)
      }
    }
    // A text[] literal with quoted elements: '{"a","b"}'.
    val arrayPath = array.map(e => "\"" + e.replace("\\", "\\\\").replace("\"", "\\\"") + "\"").mkString("{", ",", "}")

    inSession { implicit session =>
      val loaded = SQL(s"SELECT id, version, document FROM $projectionTableName WHERE $columnName #> ?::text[] @> ?::jsonb LIMIT ?")
        .bind(arrayPath, "[" + json(objectPath.toList) + "]", limit)
        .map(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
        .list()

      loaded.foreach(t => cacheAfterCommit(t._1, Some(t._2)))
      loaded.map({case (key, v) => (key, Document[T](v.document))}).toMap
    }
  }


  private def jsonString(s: String): String = {
    val json = new StringBuilder("\"")
    s.foreach {
      case '"' => json.append("\\\"")
      case '\\' => json.append("\\\\")
      case c if c < ' ' => json.append(f"\\u${c.toInt}%04x")
      case c => json.append(c)
    }
    json.append('"').toString
  }

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]] = {
    val loaded = inSession { implicit session =>
      sql"SELECT id, version, document FROM ${tableNameSQL}"
          .map(rs => rs.long(1) -> VersionedDocument[T](rs.int(2), mpjsons.deserialize[T](rs.string(3))))
        .list()
    }
    loaded.foreach(t => cacheAfterCommit(t._1, Some(t._2)))
    loaded.map(v => (v._1, Document(v._2.document))).toMap
  }

  def countAll()(implicit session: DBSession = null): Int = {
    inSession { implicit session =>
      sql"SELECT count(*) FROM ${tableNameSQL}"
        .map(rs => rs.int(1))
        .single().get
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
          }).list()
      }
      val loadedById = loaded.toMap
      toLoad.foreach(id => {
        cacheAfterCommit(id, loadedById.get(id))
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
    }).single())
  }

  /**
   * Caches a document read or written through `session`. Inside an `AfterCommit.localTx` the entry is
   * dropped now and stored only after the commit, so a rollback can never leave the cache ahead of the
   * database (F16). An older version never replaces a newer one stored meanwhile by another transaction.
   */
  protected def cacheAfterCommit(key: Long, value: Option[VersionedDocument[T]])(implicit session: DBSession): Unit = {
    cache.remove(key)
    AfterCommit(session) {
      val newerCached = (cache.get(key), value) match {
        case (InCache(cached), Some(document)) => cached.version > document.version
        case _ => false
      }
      if (!newerCached) {
        cache.put(key, value)
      }
    }
  }

  protected val maxUpdateAttempts = 10

  /**
   * Writes `modified` over the stored `previous` version (optimistic locking). Returns false when
   * another writer changed the row first; the cached copy is then evicted, so a retry reads fresh data.
   */
  protected def writeNextVersion(key: Long, previous: VersionedDocument[T], modified: T)(implicit session: DBSession): Boolean = {
    val updated = if (modified == previous.document) {
      sql"UPDATE ${tableNameSQL} SET version = ? WHERE id = ? AND version = ?"
        .bind(previous.version + 1, key, previous.version)
        .update()
    } else {
      sql"UPDATE ${tableNameSQL} SET version = ?, document = ?::JSONB WHERE id = ? AND version = ?"
        .bind(previous.version + 1, mpjsons.serialize[T](modified), key, previous.version)
        .update()
    }
    if (updated == 1) {
      cacheAfterCommit(key, Some(VersionedDocument(previous.version + 1, modified)))
      true
    } else {
      cache.remove(key)
      false
    }
  }

  /**
   * Applies `modify` to the stored document and writes the next version. When another writer changed the
   * document first, it starts again from the fresh data, at most `maxUpdateAttempts` times (F21).
   * `whenMissing` gives the result when there is no document.
   */
  protected def updateWithRetries(key: Long, modify: Document[T] => Document[T], whenMissing: => Document[T])(implicit session: DBSession): Document[T] = {
    inSession { implicit session =>
      @tailrec
      def attempt(number: Int): Document[T] = getFromCacheOrDB(key) match {
        case None => whenMissing
        case Some(previous) =>
          val modified = modify(Document(previous.document))
          if (writeNextVersion(key, previous, modified.document)) {
            modified
          } else if (number < maxUpdateAttempts) {
            attempt(number + 1)
          } else {
            throw new IllegalStateException("Too many tries to update document in table = [" + projectionTableName + "] id = " + key + " expected version " + previous.version)
          }
      }
      attempt(1)
    }
  }


  def clearSpace(spaceId: Long)(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      val idsRemoved = sql"DELETE from ${tableNameSQL} WHERE space_id = ? RETURNING id".
        bind(spaceId)
        .map(_.long(1)).list()

      idsRemoved.foreach(id => cache.remove(id))
      AfterCommit(session)(idsRemoved.foreach(id => cache.remove(id))) // F266: and after the commit, past any read meanwhile
    }
  }

}

class PostgresDocumentStore[T <: AnyRef](val tableName: String, val mpjsons: MPJsons,
                                                      val cache: DocumentStoreCache[T], val indicies: Seq[Index] = Seq.empty,
                                        val maxRows: Int = DocumentStoreQuery.MaxRows)(implicit val t: TypeTag[T])
  extends DocumentStore[T] with PostgresDocumentStoreTrait[T] {

  override def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, ?, 1, ?::jsonb)"
        .bind(spaceId, key, mpjsons.serialize(document))
        .update()
      cacheAfterCommit(key, Some(VersionedDocument(1, document)))
    }
  }

  override def insertDocuments(spaceId: Long, documents: Seq[DocumentToInsert[T]])(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      val batchParams = documents.map(d => Seq[Any](spaceId, d.key, mpjsons.serialize(d.document)))
      sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, ?, 1, ?::jsonb)"
        .batch(batchParams: _*).apply()
      documents.foreach(d => cacheAfterCommit(d.key, Some(VersionedDocument(1, d.document))))
    }
  }

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T] = {
    updateWithRetries(key, modify, whenMissing = throw new IllegalStateException("Document for update does not exist " + key))
  }

  // Optimistic locking update; a missing document is inserted.
  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T] = {
    updateWithRetries(key, document => modify(Some(document)), whenMissing = {
      val created = modify(None)
      insertDocument(spaceId, key, created.document)
      created
    })
  }


  def clearAllData()(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"TRUNCATE TABLE ${tableNameSQL}".update()
      cache.clear()
      AfterCommit(session)(cache.clear()) // F266: and after the commit, past any read meanwhile
    }
  }

}

class PostgresDocumentStoreAutoId[T <: AnyRef](val tableName: String, val mpjsons: MPJsons,
                                                            val cache: DocumentStoreCache[T], val indicies: Seq[Index] = Seq.empty,
                                        val maxRows: Int = DocumentStoreQuery.MaxRows)(implicit val t: TypeTag[T])
  extends DocumentStoreAutoId[T] with PostgresDocumentStoreTrait[T] {

  protected final lazy val sequenceName = "sequence_" + tableName

  protected final lazy val sequenceNameSQL = SQLSyntax.createUnsafely(sequenceName)


  override protected def createSequenceIfNotExists(): Unit = {
    try {
      DB.autoCommit { implicit session =>
        sql"CREATE SEQUENCE IF NOT EXISTS ${sequenceNameSQL} START 1"
          .update()
      }
    } catch {
      case e: PSQLException if e.getServerErrorMessage.getSQLState == "42P07" => () // IF NOT EXIST workaround, 42P07 - PostgreSQL duplicate_table
    }
  }

  override def dropTable(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP TABLE ${tableNameSQL}"""
      .stripMargin.execute()

    sql"""DROP SEQUENCE ${sequenceNameSQL}"""
      .stripMargin.execute()

  }

  override def insertDocument(spaceId: Long, document: T)(implicit session: DBSession): Long = {
    inSession {implicit session =>
      val key = sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, nextval('${sequenceNameSQL}'), 1, ?::jsonb) RETURNING currval('${sequenceNameSQL}')"
          .bind(spaceId, mpjsons.serialize(document))
          .map(_.long(1)).single().get
      cacheAfterCommit(key, Some(VersionedDocument(1, document)))
      key
    }
  }

  override def insertDocuments(spaceId: Long, documents: Seq[T])(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      val batchParams: Seq[Seq[Any]] = documents.map(d => Seq(spaceId, mpjsons.serialize(d)))
      sql"INSERT INTO ${tableNameSQL} (space_id, id, version, document) VALUES (?, nextval('${sequenceNameSQL}'), 1, ?::jsonb)"
        .batch(batchParams: _*).apply()
    }
  }

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T] = {
    updateWithRetries(key, modify, whenMissing = throw new IllegalStateException(s"Document for key $key not found!"))
  }

  // Optimistic locking update; keys come from the sequence, so a missing document cannot be inserted here.
  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T] = {
    updateWithRetries(key, document => modify(Some(document)), whenMissing = throw new IllegalStateException(s"Document for key $key not found!"))
  }

  def clearAllData()(implicit session: DBSession): Unit = {
    inSession { implicit session =>
      sql"TRUNCATE TABLE ${tableNameSQL}".update()
      sql"ALTER SEQUENCE ${sequenceNameSQL} RESTART WITH 1".update()
      cache.clear()
      AfterCommit(session)(cache.clear()) // F266: and after the commit, past any read meanwhile
    }
  }

}

