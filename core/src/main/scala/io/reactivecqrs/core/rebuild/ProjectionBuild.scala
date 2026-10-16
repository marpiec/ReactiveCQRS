package io.reactivecqrs.core.rebuild

/**
  * One build of a managed projection (D57), given to the function that creates the projection's instance for it: the
  * names of the build's tables. A document store takes `documentStore(table)` as its table name; the projection's own
  * state takes `table(name)` and `sequence(name)`. Every name is recorded, so the build's tables are dropped with it
  * when it is retired. Build 0, adopted from 0.12, has the names as they were.
  */
class ProjectionBuild private[rebuild](val record: BuildRecord, registry: ProjectionRegistry) {

  def name: String = record.name
  def version: Int = record.version
  def number: Int = record.number

  /** The table name of a `PostgresDocumentStore` (or `…AutoId`) of this build. */
  def documentStore(table: String): String = {
    val named = checked(table) + record.suffix
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
