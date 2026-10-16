package io.reactivecqrs.testutils

import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

/** The local PostgreSQL database used by the PostgreSQL-backed specs. */
object TestDatabase {

  private lazy val initialized: Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))
  }

  /** Initializes the default ScalikeJDBC connection pool once per JVM, whichever spec runs first. */
  def init(): Unit = initialized
}
