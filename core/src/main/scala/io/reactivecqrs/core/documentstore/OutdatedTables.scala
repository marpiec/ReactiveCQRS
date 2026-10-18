package io.reactivecqrs.core.documentstore

import java.util.concurrent.ConcurrentHashMap

/** F189: the document stores' tables of managed builds that serve while the code is at a later version of their
  * projection. A store of such a table keeps the indexes the table has: the indexes the code declares are its own
  * version's, built with its build, and the documents of the version before may violate them. */
private[reactivecqrs] object OutdatedTables {
  private val tables = ConcurrentHashMap.newKeySet[String]()

  def mark(table: String, outdated: Boolean): Unit = if (outdated) tables.add(table) else tables.remove(table)

  def contains(table: String): Boolean = tables.contains(table)
}
