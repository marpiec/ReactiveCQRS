package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

import scala.collection.mutable

case class Note(text: String)

/** A plain map cache, so tests can see the effect of a cache that is behind the database. */
class MapDocumentStoreCache[D <: AnyRef] extends DocumentStoreCache[D] {
  private val entries = mutable.Map[Long, Option[VersionedDocument[D]]]()

  override def put(key: Long, value: Option[VersionedDocument[D]]): Unit = entries.put(key, value)
  override def get(key: Long): CacheEntry[VersionedDocument[D]] = entries.get(key) match {
    case Some(Some(document)) => InCache(document)
    case Some(None) => InCacheEmpty
    case None => NotInCache
  }
  override def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]] = keys.map(key => key -> get(key)).toMap
  override def remove(key: Long): Unit = entries.remove(key)
  override def clear(): Unit = entries.clear()
}

// F21: a version conflict must never be reported as a successful update, and retries must use fresh data.
class PostgresDocumentStoreConflictSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val tableName = "conflict_test"

  /** Changes the stored document behind the store's back, as another writer would. */
  private def writeBehindTheCache(key: Long, version: Int, note: Note): Unit = DB.autoCommit { implicit session =>
    SQL(s"UPDATE projection_$tableName SET version = ?, document = ?::jsonb WHERE id = ?")
      .bind(version, mpjsons.serialize(note), key).update()
  }

  private def storedNote(key: Long): Option[Note] =
    new PostgresDocumentStore[Note](tableName, mpjsons, new NoopDocumentStoreCache).getDocument(key).map(_.document)

  private def appendC(document: Document[Note]): Document[Note] = Document(Note(document.document.text + "c"))

  private def withStore[S <: PostgresDocumentStoreTrait[Note]](store: S)(test: S => Unit): Unit = {
    try test(store) finally store.dropTable()
  }

  Feature("Updating a document whose cached copy is out of date") {

    Scenario("updateDocument applies the change to the stored version") {
      withStore(new PostgresDocumentStore[Note](tableName, mpjsons, new MapDocumentStoreCache[Note])) { store =>
        Given("a cached document 'a' that another writer changed to 'b'")
        DB.localTx { implicit session => store.insertDocument(0, 1, Note("a")) }
        writeBehindTheCache(1, version = 2, Note("b"))

        When("the document is updated by appending 'c'")
        val result = DB.localTx { implicit session => store.updateDocument(0, 1, { case Some(document) => appendC(document) }) }

        Then("the change is applied to 'b', both in the result and in the database")
        result.document mustBe Note("bc")
        storedNote(1) mustBe Some(Note("bc"))
      }
    }

    Scenario("updateExistingDocument applies the change to the stored version") {
      withStore(new PostgresDocumentStore[Note](tableName, mpjsons, new MapDocumentStoreCache[Note])) { store =>
        DB.localTx { implicit session => store.insertDocument(0, 1, Note("a")) }
        writeBehindTheCache(1, version = 2, Note("b"))

        DB.localTx { implicit session => store.updateExistingDocument(1, appendC) }.document mustBe Note("bc")
        storedNote(1) mustBe Some(Note("bc"))
      }
    }

    Scenario("the auto-id store retries a bounded number of times with fresh data") {
      withStore(new PostgresDocumentStoreAutoId[Note](tableName, mpjsons, new MapDocumentStoreCache[Note])) { store =>
        val key = DB.localTx { implicit session => store.insertDocument(0, Note("a")) }
        writeBehindTheCache(key, version = 2, Note("b"))

        DB.localTx { implicit session => store.updateDocument(0, key, { case Some(document) => appendC(document) }) }.document mustBe Note("bc")
        DB.localTx { implicit session => store.updateExistingDocument(key, appendC) }.document mustBe Note("bcc")
        storedNote(key) mustBe Some(Note("bcc"))
      }
    }

    Scenario("overwriteDocument writes the document even when the stale cached copy equals it") {
      withStore(new PostgresDocumentStore[Note](tableName, mpjsons, new MapDocumentStoreCache[Note])) { store =>
        Given("a cached document 'a' that another writer changed to 'b'")
        DB.localTx { implicit session => store.insertDocument(0, 1, Note("a")) }
        writeBehindTheCache(1, version = 2, Note("b"))

        When("the document is overwritten with 'a'")
        DB.localTx { implicit session => store.overwriteDocument(1, Note("a")) }

        Then("the database holds 'a'")
        storedNote(1) mustBe Some(Note("a"))
      }
    }
  }
}
