package io.reactivecqrs.core.projection

import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

abstract class VersionsState {
  def versionForAggregate(aggregateType: String): Int
  def versionForProjection(projectionName: String): Int

  def saveVersionForAggregate(aggregateType: String, version: Int): Unit
  def saveVersionForProjection(projectionName: String, version: Int): Unit
}


class PostgresVersionsState() extends VersionsState {

  val AGGREGATE: Short = 1
  val PROJECTION: Short = 2

  def initSchema(): PostgresVersionsState = {
    createSubscriptionsTable()
    this
  }

  private def createSubscriptionsTable() = DB.autoCommit { implicit session =>
    sql"""
         CREATE TABLE IF NOT EXISTS components_versions (
           component_name VARCHAR(255) NOT NULL,
           component_type SMALLINT NOT NULL,
           component_version INT NOT NULL)
       """.execute().apply()

  }

  override def versionForAggregate(aggregateType: String): Int = versionFor(aggregateType, AGGREGATE)
  override def versionForProjection(aggregateType: String): Int = versionFor(aggregateType, PROJECTION)

  override def saveVersionForAggregate(aggregateType: String, version: Int): Unit = saveVersionFor(aggregateType, AGGREGATE, version)
  override def saveVersionForProjection(projectionName: String, version: Int): Unit = saveVersionFor(projectionName, PROJECTION, version)


  private def versionFor(componentName: String, componentType: Short) = {
    DB.readOnly { implicit session =>
      sql"""SELECT component_version FROM components_versions WHERE component_name = ? AND component_type = ?"""
        .bind(componentName, componentType)
        .map(_.int(1)).single().apply().getOrElse(0)
    }
  }

  private def saveVersionFor(componentName: String, componentType: Short, version: Int): Unit = {

    if(versionFor(componentName, componentType) > 0) {
      DB.autoCommit { implicit session =>
        sql"""UPDATE components_versions SET component_version = ? WHERE component_name = ? AND component_type = ?""".stripMargin
          .bind(version, componentName, componentType)
          .executeUpdate()
      }
    } else {
      sql"""INSERT INTO components_versions (component_name, component_type, component_version) VALUES (?, ?, ?)""".stripMargin
        .bind(componentName, componentType, version)
        .executeUpdate()
    }

  }
}