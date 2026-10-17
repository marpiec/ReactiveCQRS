package io.reactivecqrs.core

import io.mpjsons.MPJsons
import io.reactivecqrs.core.documentstore.{MultipleLongIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.uid.PostgresUidGenerator
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

case class CatalogDocument(value: Long)

// F71: catalog lookups see only the schema the connection works in, so another schema holding the same objects
// (another installation in the same database, or earlier test runs) cannot mislead them.
class SchemaAwareCatalogSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private val current: String = DB.readOnly { implicit session => sql"SELECT current_schema()".map(_.string(1)).single().get }
  private val other: String = current + "_other"
  DB.autoCommit { implicit session => SQL(s"CREATE SCHEMA IF NOT EXISTS $other").execute() }

  private def indexesInCurrentSchema(table: String): List[String] = DB.readOnly { implicit session =>
    sql"SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = $table AND indexname LIKE '%_idx_%'"
      .map(_.string(1)).list()
  }

  Feature("Catalog lookups when another schema holds objects of the same names") {

    Scenario("the uid generator reads the step of its own schema's sequence") {
      DB.autoCommit { implicit session =>
        SQL(s"CREATE SEQUENCE IF NOT EXISTS $other.catalog_spec_seq INCREMENT BY 7").execute()
        SQL("CREATE SEQUENCE IF NOT EXISTS catalog_spec_seq INCREMENT BY 3").execute()
      }

      new PostgresUidGenerator("catalog_spec_seq").poolSize mustBe 3
    }

    Scenario("the document store creates its indices although the other schema already has them") {
      DB.autoCommit { implicit session =>
        SQL(s"CREATE TABLE IF NOT EXISTS $other.projection_catalog_spec (id BIGINT PRIMARY KEY)").execute()
        SQL(s"CREATE INDEX IF NOT EXISTS projection_catalog_spec_idx_1 ON $other.projection_catalog_spec (id)").execute()
      }

      new PostgresDocumentStore[CatalogDocument]("catalog_spec", new MPJsons, new NoopDocumentStoreCache[CatalogDocument, AnyRef],
        Seq(MultipleLongIndex(1, Seq("value"))))

      indexesInCurrentSchema("projection_catalog_spec") mustBe List("projection_catalog_spec_idx_1")
    }
  }
}
