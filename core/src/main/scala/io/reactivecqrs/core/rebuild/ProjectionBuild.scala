package io.reactivecqrs.core.rebuild

import io.reactivecqrs.core.documentstore.OutdatedTables

/**
  * One build of a managed projection (D57), given to the function that creates the projection's instance for it: the
  * names of the build's tables. A document store takes `documentStore(table)` as its table name; the projection's own
  * state takes `table(name)` and `sequence(name)`. Every name is recorded, so the build's tables are dropped with it
  * when it is retired. Build 0, adopted from 0.12, has the names as they were.
  *
  * A build of an older version than the code's (the active one, while the code's version is built) is not `current`: its
  * document stores keep the indexes their tables have, as the code's may not fit the older documents (F189).
  */
class ProjectionBuild private[rebuild](val record: BuildRecord, registry: ProjectionRegistry, val current: Boolean = true) {

  /** The projection's name. */
  def name: String = record.name
  /** The projection's version this build makes. */
  def version: Int = record.version
  /** The build's number, one more for each build of the projection. */
  def number: Int = record.number

  /** The table name of a `PostgresDocumentStore` (or `…AutoId`) of this build. */
  def documentStore(table: String): String = {
    val named = checked(table) + record.suffix
    OutdatedTables.mark(named, outdated = !current)
    registry.recordObject(name, number, "projection_" + named, sequence = false)
    registry.recordObject(name, number, "sequence_" + named, sequence = true) // the auto-id store's
    named
  }

  /** A table of the projection's own state. */
  def table(table: String): String = recorded(table, sequence = false)

  /** A sequence of the projection's own state. */
  def sequence(sequence: String): String = recorded(sequence, sequence = true)

  private def recorded(base: String, sequence: Boolean): String = {
    val named = checked(base) + record.suffix
    registry.recordObject(name, number, named, sequence)
    named
  }

  private def checked(base: String): String = {
    require(base.matches("[a-zA-Z0-9_]+"), "Table and sequence names have letters, digits and underscores only: " + base)
    base
  }
}
