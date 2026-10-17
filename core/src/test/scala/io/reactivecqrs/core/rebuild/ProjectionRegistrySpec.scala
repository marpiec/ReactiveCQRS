package io.reactivecqrs.core.rebuild

import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuildPosition
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

// D57, D70, F94: builds of projections, the switch in one update, and what a build leaves to drop.
class ProjectionRegistrySpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private val postgres = new PostgresProjectionRegistry().initSchema()
  private def newName(): String = "registry-" + System.nanoTime()

  for ((store, registry) <- Seq("PostgreSQL" -> (() => postgres), "memory" -> (() => new MemoryProjectionRegistry))) {

    Feature("Builds of a projection, in " + store) {

      Scenario("the first build serves while it is built, and a switch makes it the active build only") {
        val (r, name) = (registry(), newName())
        val first = r.startBuild(name, 3, Map("Note" -> 1))
        (first.number, first.suffix, first.subscriber) mustBe ((1, "_v3", "projection:" + name + "_v3"))
        r.projection(name).map(p => (p.active.map(_.number), p.building.map(_.number))) mustBe Some((Some(1), Some(1)))

        r.switch(name, 1) mustBe true
        r.projection(name).map(p => (p.active.map(_.number), p.building)) mustBe Some((Some(1), None))
        r.switch(name, 1) mustBe false
      }

      Scenario("a later build is numbered on, named by its version, and replaces the active one in one switch") {
        val (r, name) = (registry(), newName())
        r.startBuild(name, 1, Map.empty)
        r.switch(name, 1)
        val again = r.startBuild(name, 1, Map("Note" -> 2)) // the same version, built again (D63)
        (again.number, again.suffix, again.contexts) mustBe ((2, "_v1_b2", Map("Note" -> 2)))
        an[IllegalStateException] must be thrownBy r.startBuild(name, 2, Map.empty)

        r.switch(name, 2) mustBe true
        r.projection(name).flatMap(_.active.map(_.number)) mustBe Some(2)
        r.startBuild(name, 2, Map.empty).suffix mustBe "_v2"
      }

      Scenario("a projection 0.12 built is build 0, with no suffix and its class as subscriber") {
        val (r, name) = (registry(), newName())
        val adopted = r.adopt(name, 4, "com.example.NotesProjection", Map("Note" -> 2))
        (adopted.number, adopted.suffix, adopted.subscriber, adopted.position.bulkDone) mustBe ((0, "", "com.example.NotesProjection", true))
        r.startBuild(name, 5, Map.empty).number mustBe 1
      }

      Scenario("an abandoned build is no longer in progress, and an abandoned first build no longer serves") {
        val (r, name) = (registry(), newName())
        r.startBuild(name, 1, Map.empty)
        r.abandon(name, 1)
        r.projection(name).map(p => (p.active, p.building)) mustBe Some((None, None))
        val next = r.startBuild(name, 2, Map.empty)
        r.projection(name).map(p => (p.active.map(_.number), p.building.map(_.number))) mustBe Some((Some(next.number), Some(next.number)))
      }

      Scenario("a build keeps its position (D61)") {
        val (r, name) = (registry(), newName())
        r.startBuild(name, 1, Map.empty)
        r.savePosition(name, 1, BuildPosition(bulkDone = false, 42L))
        r.projection(name).flatMap(_.building.map(_.position)) mustBe Some(BuildPosition(bulkDone = false, 42L))
        new RegistryBuildPositions(r, name, 1).read() mustBe BuildPosition(bulkDone = false, 42L)
      }
    }
  }

  Feature("Dropping a build (F94)") {

    Scenario("drops every table and sequence it named, and forgets it") {
      val name = newName()
      val build = postgres.startBuild(name, 1, Map.empty)
      val handle = new ProjectionBuild(build, postgres)
      val store = handle.documentStore("registry_spec")
      val own = handle.table("registry_spec_own")
      val sequence = handle.sequence("registry_spec_seq")
      DB.autoCommit { implicit s =>
        SQL(s"CREATE TABLE projection_$store (id BIGINT)").execute.apply()
        SQL(s"CREATE TABLE $own (id BIGINT)").execute.apply()
        SQL(s"CREATE SEQUENCE $sequence").execute.apply()
      }
      def exists(objectName: String) = DB.readOnly(implicit s => sql"SELECT to_regclass($objectName) IS NOT NULL".map(_.boolean(1)).single().get)

      postgres.drop(name, build.number)

      Seq("projection_" + store, own, sequence).map(exists) mustBe Seq(false, false, false)
      postgres.projection(name).flatMap(_.building) mustBe None
    }

    Scenario("reads what 0.12 recorded of projections and aggregates") {
      DB.autoCommit { implicit s =>
        sql"CREATE TABLE IF NOT EXISTS components_versions (component_name VARCHAR(255) NOT NULL, component_type SMALLINT NOT NULL, component_version INT NOT NULL)".execute()
        sql"INSERT INTO components_versions VALUES ('RegistrySpecProjection', 2, 7), ('io.example.Note', 1, 3)".update()
      }
      val legacy = postgres.legacyVersions()
      legacy.projections.get("RegistrySpecProjection") mustBe Some(7)
      legacy.aggregates.get("io.example.Note") mustBe Some(3)
    }
  }
}
