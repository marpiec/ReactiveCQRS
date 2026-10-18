package io.reactivecqrs.core.rebuild

import io.reactivecqrs.core.util.{Ddl, SchemaLock}

import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuildPosition
import scalikejdbc._

import scala.collection.mutable
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/** One build of a projection (D57): its number, the projection version it builds, the suffix of its tables, the name
  * its subscriptions are stored under, the context versions of the types whose states it receives (D63), and how far
  * it got while building (D61). Build 0 is a projection adopted from 0.12: no suffix, its class name as subscriber. */
case class BuildRecord(name: String, number: Int, version: Int, suffix: String, subscriber: String, contexts: Map[String, Int],
                       position: BuildPosition)

/** A projection as the registry knows it: the build that serves and the one in progress, if any, and a targeted
  * rebuild of the active build that has not ended (D63). */
case class RegisteredProjection(name: String, active: Option[BuildRecord], building: Option[BuildRecord], pendingTarget: Option[String] = None)

/** What 0.12's rebuild recorded (`components_versions`): the version of each projection by name and of each aggregate
  * context by type, as last built. */
case class LegacyVersions(projections: Map[String, Int], aggregates: Map[String, Int])

/**
  * The projections and their builds (D57, D70): one row per projection name holds the active build and the build in
  * progress, so a switch is one update; each build records the tables and sequences it named, so a retired build is
  * dropped whole (F94).
  */
trait ProjectionRegistry {

  /** The projection `name`, if the registry has it. */
  def projection(name: String): Option[RegisteredProjection]

  /** Every build of `name` the registry has: the active one, the one in progress, and any retired or abandoned one not
    * dropped yet (a stop came first, F116). */
  def builds(name: String): Seq[BuildRecord]

  /** Records a projection 0.12 built (D81) as active build 0, with its tables and subscription rows as they are. */
  def adopt(name: String, version: Int, subscriber: String, contexts: Map[String, Int]): BuildRecord

  /** Starts the next build of `name`, which must have none in progress; the first build of a projection is also
    * recorded as active, as nothing else serves. */
  def startBuild(name: String, version: Int, contexts: Map[String, Int]): BuildRecord

  /** D70: the build in progress becomes the active one, in one update; false when it is not in progress any more. */
  def switch(name: String, build: Int): Boolean

  /** Forgets the build in progress (a newer version of the code supersedes it); its tables go with `drop`. */
  def abandon(name: String, build: Int): Unit

  /** Keeps how far the build got (D61). */
  def savePosition(name: String, build: Int, position: BuildPosition): Unit

  /** A targeted rebuild of the active build begins (`Some`) or ends (`None`); one begun and not ended runs again at start. */
  def setPendingTarget(name: String, target: Option[String]): Unit

  /** A table (or a sequence, `sequence = true`) a build created, dropped with the build. */
  def recordObject(name: String, build: Int, objectName: String, sequence: Boolean): Unit

  /** Drops the build's tables and sequences and forgets it; the caller removes its subscriptions. */
  def drop(name: String, build: Int): Unit

  /** Forgets the projection; its builds must be dropped first. */
  def remove(name: String): Unit

  /** What 0.12's rebuild recorded, empty when it recorded nothing. */
  def legacyVersions(): LegacyVersions
}

/** How builds name their tables and subscriptions. */
object ProjectionRegistry {
  /** The suffix of a build's tables: `_v<version>` for the first build of a version, then `_v<version>_b<build>`. */
  def suffix(version: Int, build: Int, versionBuilt: Boolean): String = if (versionBuilt) "_v" + version + "_b" + build else "_v" + version

  /** The name a build's subscriptions are stored under. */
  def subscriber(name: String, suffix: String): String = "projection:" + name + suffix

  private[rebuild] def contextsText(contexts: Map[String, Int]): String = contexts.toSeq.sorted.map { case (t, v) => t + "=" + v }.mkString(";")
  private[rebuild] def contextsOf(text: String): Map[String, Int] =
    text.split(';').filter(_.nonEmpty).map(entry => entry.takeWhile(_ != '=') -> entry.dropWhile(_ != '=').drop(1).toInt).toMap
}

/** The registry in PostgreSQL: the tables `projections`, `projection_builds` and `projection_build_objects`. */
class PostgresProjectionRegistry extends ProjectionRegistry {

  import ProjectionRegistry._

  /** Creates the tables it needs, when missing; returns this registry. */
  def initSchema(): PostgresProjectionRegistry = SchemaLock {
    DB.autoCommit { implicit session =>
      sql"""CREATE TABLE IF NOT EXISTS projections (
           |  name VARCHAR(255) NOT NULL PRIMARY KEY,
           |  active_build INT,
           |  building_build INT,
           |  last_build INT NOT NULL,
           |  switched TIMESTAMP,
           |  pending_target TEXT)""".stripMargin.execute()
      sql"""CREATE TABLE IF NOT EXISTS projection_builds (
           |  name VARCHAR(255) NOT NULL,
           |  build INT NOT NULL,
           |  version INT NOT NULL,
           |  suffix VARCHAR(64) NOT NULL,
           |  subscriber VARCHAR(255) NOT NULL,
           |  contexts TEXT NOT NULL,
           |  bulk_done BOOLEAN NOT NULL DEFAULT false,
           |  position BIGINT NOT NULL DEFAULT 0,
           |  created TIMESTAMP NOT NULL,
           |  PRIMARY KEY (name, build))""".stripMargin.execute()
      sql"""CREATE TABLE IF NOT EXISTS projection_build_objects (
           |  name VARCHAR(255) NOT NULL,
           |  build INT NOT NULL,
           |  object_name VARCHAR(255) NOT NULL,
           |  is_sequence BOOLEAN NOT NULL,
           |  PRIMARY KEY (name, build, object_name, is_sequence))""".stripMargin.execute()
    }
    Ddl.addColumn("projections", "pending_target", "TEXT") // F185: only when it is missing
    this
  }

  private def build(rs: WrappedResultSet): BuildRecord = BuildRecord(rs.string("name"), rs.int("build"), rs.int("version"), rs.string("suffix"),
    rs.string("subscriber"), contextsOf(rs.string("contexts")), BuildPosition(rs.boolean("bulk_done"), rs.long("position")))

  private def buildOf(name: String, number: Int)(implicit session: DBSession): Option[BuildRecord] =
    sql"""SELECT * FROM projection_builds WHERE name = $name AND build = $number""".map(build).single()

  override def projection(name: String): Option[RegisteredProjection] = DB.readOnly { implicit session =>
    sql"""SELECT active_build, building_build, pending_target FROM projections WHERE name = $name""".map(rs => (rs.intOpt(1), rs.intOpt(2), rs.stringOpt(3)))
      .single().map { case (active, building, pending) =>
        RegisteredProjection(name, active.flatMap(buildOf(name, _)), building.flatMap(buildOf(name, _)), pending)
      }
  }

  override def builds(name: String): Seq[BuildRecord] = DB.readOnly { implicit session =>
    sql"""SELECT * FROM projection_builds WHERE name = $name ORDER BY build""".map(build).list()
  }

  override def adopt(name: String, version: Int, subscriber: String, contexts: Map[String, Int]): BuildRecord = DB.localTx { implicit session =>
    sql"""INSERT INTO projections (name, active_build, last_build) VALUES ($name, 0, 0)""".update()
    insertBuild(BuildRecord(name, 0, version, "", subscriber, contexts, BuildPosition(bulkDone = true, 0L)))
  }

  override def startBuild(name: String, version: Int, contexts: Map[String, Int]): BuildRecord = DB.localTx { implicit session =>
    val registered = sql"""SELECT building_build, last_build FROM projections WHERE name = $name FOR UPDATE""".map(rs => (rs.intOpt(1), rs.int(2))).single()
    registered.flatMap(_._1).foreach(b => throw new IllegalStateException("Projection " + name + " is being built already (build " + b + ")"))
    val number = registered.map(_._2 + 1).getOrElse(1)
    val versionBuilt = sql"""SELECT EXISTS (SELECT 1 FROM projection_builds WHERE name = $name AND version = $version)""".map(_.boolean(1)).single().get
    val suffix = ProjectionRegistry.suffix(version, number, versionBuilt)
    registered match {
      case Some(_) => sql"""UPDATE projections SET building_build = $number, last_build = $number, active_build = coalesce(active_build, $number)
                           | WHERE name = $name""".stripMargin.update()
      case None => sql"""INSERT INTO projections (name, active_build, building_build, last_build) VALUES ($name, $number, $number, $number)""".update()
    }
    insertBuild(BuildRecord(name, number, version, suffix, ProjectionRegistry.subscriber(name, suffix), contexts, BuildPosition.Start))
  }

  private def insertBuild(record: BuildRecord)(implicit session: DBSession): BuildRecord = {
    sql"""INSERT INTO projection_builds (name, build, version, suffix, subscriber, contexts, bulk_done, position, created)
         | VALUES (${record.name}, ${record.number}, ${record.version}, ${record.suffix}, ${record.subscriber}, ${contextsText(record.contexts)},
         |   ${record.position.bulkDone}, ${record.position.position}, current_timestamp)""".stripMargin.update()
    record
  }

  override def switch(name: String, build: Int): Boolean = DB.autoCommit { implicit session =>
    sql"""UPDATE projections SET active_build = building_build, building_build = NULL, switched = current_timestamp
         | WHERE name = $name AND building_build = $build""".stripMargin.update() == 1
  }

  override def abandon(name: String, build: Int): Unit = DB.autoCommit { implicit session =>
    sql"""UPDATE projections SET building_build = NULL WHERE name = $name AND building_build = $build""".update()
    // A first build was active too, with nothing else to serve.
    sql"""UPDATE projections SET active_build = NULL WHERE name = $name AND active_build = $build""".update()
  }

  override def savePosition(name: String, build: Int, position: BuildPosition): Unit = DB.autoCommit { implicit session =>
    sql"""UPDATE projection_builds SET bulk_done = ${position.bulkDone}, position = ${position.position} WHERE name = $name AND build = $build""".update()
  }

  override def setPendingTarget(name: String, target: Option[String]): Unit = DB.autoCommit { implicit session =>
    sql"""UPDATE projections SET pending_target = $target WHERE name = $name""".update()
  }

  override def recordObject(name: String, build: Int, objectName: String, sequence: Boolean): Unit = DB.autoCommit { implicit session =>
    sql"""INSERT INTO projection_build_objects (name, build, object_name, is_sequence) VALUES ($name, $build, $objectName, $sequence)
         | ON CONFLICT DO NOTHING""".stripMargin.update()
  }

  // The names were checked when the stores were made (identifiers of letters, digits and underscores).
  override def drop(name: String, build: Int): Unit = DB.localTx { implicit session =>
    sql"""SELECT object_name, is_sequence FROM projection_build_objects WHERE name = $name AND build = $build""".map(rs => (rs.string(1), rs.boolean(2))).list()
      .foreach { case (objectName, sequence) =>
        require(objectName.matches("[a-zA-Z0-9_]+"), "Invalid table or sequence name of build " + build + " of " + name + ": " + objectName)
        SQL((if (sequence) "DROP SEQUENCE IF EXISTS " else "DROP TABLE IF EXISTS ") + objectName).execute.apply()
      }
    sql"""DELETE FROM projection_build_objects WHERE name = $name AND build = $build""".update()
    sql"""DELETE FROM projection_builds WHERE name = $name AND build = $build""".update()
  }

  override def remove(name: String): Unit = DB.autoCommit { implicit session =>
    sql"""DELETE FROM projections WHERE name = $name""".update()
  }

  override def legacyVersions(): LegacyVersions = DB.readOnly { implicit session =>
    val exists = sql"""SELECT to_regclass('components_versions') IS NOT NULL""".map(_.boolean(1)).single().getOrElse(false)
    if (!exists) LegacyVersions(Map.empty, Map.empty) else {
      val rows = sql"""SELECT component_name, component_type, component_version FROM components_versions""".map(rs => (rs.string(1), rs.short(2), rs.int(3))).list()
      LegacyVersions(rows.collect { case (n, 2, v) => n -> v }.toMap, rows.collect { case (n, 1, v) => n -> v }.toMap)
    }
  }
}

/** The registry in memory, for tests; dropping only forgets. */
/** The registry in memory, for tests; `legacy` stands for what 0.12 recorded. */
class MemoryProjectionRegistry extends ProjectionRegistry {

  private case class Row(active: Option[Int], building: Option[Int], last: Int, pendingTarget: Option[String] = None)
  private val rows = mutable.Map[String, Row]()
  private val records = mutable.Map[(String, Int), BuildRecord]()
  private val objects = mutable.Map[(String, Int), Set[(String, Boolean)]]()
  @volatile var legacy: LegacyVersions = LegacyVersions(Map.empty, Map.empty)

  /** The tables and sequences the build recorded (`true`: a sequence). */
  def objectsOf(name: String, build: Int): Set[(String, Boolean)] = synchronized(objects.getOrElse((name, build), Set.empty))

  override def projection(name: String): Option[RegisteredProjection] = synchronized {
    rows.get(name).map(row => RegisteredProjection(name, row.active.flatMap(b => records.get((name, b))), row.building.flatMap(b => records.get((name, b))),
      row.pendingTarget))
  }

  override def builds(name: String): Seq[BuildRecord] = synchronized(records.collect { case ((n, _), b) if n == name => b }.toSeq.sortBy(_.number))

  override def setPendingTarget(name: String, target: Option[String]): Unit = synchronized {
    rows.get(name).foreach(row => rows(name) = row.copy(pendingTarget = target))
  }

  override def adopt(name: String, version: Int, subscriber: String, contexts: Map[String, Int]): BuildRecord = synchronized {
    require(!rows.contains(name), "Projection " + name + " is registered already")
    rows(name) = Row(Some(0), None, 0)
    val record = BuildRecord(name, 0, version, "", subscriber, contexts, BuildPosition(bulkDone = true, 0L))
    records((name, 0)) = record
    record
  }

  override def startBuild(name: String, version: Int, contexts: Map[String, Int]): BuildRecord = synchronized {
    val registered = rows.get(name)
    registered.flatMap(_.building).foreach(b => throw new IllegalStateException("Projection " + name + " is being built already (build " + b + ")"))
    val number = registered.map(_.last + 1).getOrElse(1)
    val suffix = ProjectionRegistry.suffix(version, number, records.exists { case ((n, _), b) => n == name && b.version == version })
    rows(name) = registered.map(row => row.copy(active = row.active.orElse(Some(number)), building = Some(number), last = number))
      .getOrElse(Row(Some(number), Some(number), number))
    val record = BuildRecord(name, number, version, suffix, ProjectionRegistry.subscriber(name, suffix), contexts, BuildPosition.Start)
    records((name, number)) = record
    record
  }

  override def switch(name: String, build: Int): Boolean = synchronized {
    rows.get(name).filter(_.building.contains(build)).exists { row =>
      rows(name) = row.copy(active = Some(build), building = None)
      true
    }
  }

  override def abandon(name: String, build: Int): Unit = synchronized {
    rows.get(name).foreach(row => rows(name) = row.copy(building = row.building.filterNot(_ == build), active = row.active.filterNot(_ == build)))
  }

  override def savePosition(name: String, build: Int, position: BuildPosition): Unit = synchronized {
    records.get((name, build)).foreach(b => records((name, build)) = b.copy(position = position))
  }

  override def recordObject(name: String, build: Int, objectName: String, sequence: Boolean): Unit = synchronized {
    objects((name, build)) = objectsOf(name, build) + (objectName -> sequence)
  }

  override def drop(name: String, build: Int): Unit = synchronized {
    objects -= ((name, build))
    records -= ((name, build))
  }

  override def remove(name: String): Unit = synchronized(rows -= name)

  override def legacyVersions(): LegacyVersions = legacy
}
