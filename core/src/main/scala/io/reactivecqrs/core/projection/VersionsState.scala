package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateType
import scalikejdbc._

object VersionsState {
  val AGGREGATE: Short = 1
  val PROJECTION: Short = 2
}

abstract class VersionsState {
  def versionForAggregate(aggregateType: AggregateType): Int
  def versionForProjection(projectionName: String): Int

  def saveVersionForAggregate(aggregateType: AggregateType, version: Int): Unit
  def saveVersionForProjection(projectionName: String, version: Int): Unit
}


class PostgresVersionsState() extends VersionsState {



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

  override def versionForAggregate(aggregateType: AggregateType): Int = versionFor(aggregateType.typeName, VersionsState.AGGREGATE)
  override def versionForProjection(aggregateType: String): Int = versionFor(aggregateType, VersionsState.PROJECTION)

  override def saveVersionForAggregate(aggregateType: AggregateType, version: Int): Unit = saveVersionFor(aggregateType.typeName, VersionsState.AGGREGATE, version)
  override def saveVersionForProjection(projectionName: String, version: Int): Unit = saveVersionFor(projectionName, VersionsState.PROJECTION, version)


  private def versionFor(componentName: String, componentType: Short) = DB.readOnly { implicit session =>
    sql"""SELECT component_version FROM components_versions WHERE component_name = ? AND component_type = ?"""
      .bind(componentName, componentType)
      .map(_.int(1)).single().apply().getOrElse(0)
  }

  private def saveVersionFor(componentName: String, componentType: Short, version: Int): Unit = DB.autoCommit { implicit session =>
    if(versionFor(componentName, componentType) > 0) {
      sql"""UPDATE components_versions SET component_version = ? WHERE component_name = ? AND component_type = ?""".stripMargin
        .bind(version, componentName, componentType)
        .executeUpdate().apply()
    } else {
      sql"""INSERT INTO components_versions (component_name, component_type, component_version) VALUES (?, ?, ?)""".stripMargin
        .bind(componentName, componentType, version)
        .executeUpdate().apply()
    }

  }
}