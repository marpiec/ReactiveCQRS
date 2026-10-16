package io.reactivecqrs.testutils

import java.sql.{Connection, DriverManager}
import java.util.concurrent.ThreadLocalRandom

import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

/**
  * The local PostgreSQL database used by the PostgreSQL-backed specs. Each test run gets its own schema, so results
  * never depend on what earlier runs left behind, and every run also creates the schema from scratch. Schemas of runs
  * older than six hours are dropped. `-Dreactivecqrs.test.schema=<name>` uses (and keeps) a fixed schema instead.
  */
object TestDatabase {

  private val url = "jdbc:postgresql://localhost:5432/reactivecqrs"
  private val user = "reactivecqrs"
  private val password = "reactivecqrs"

  private val runSchemaPrefix = "test_run_"
  private val keepRunSchemasSeconds = 6 * 3600L

  /** The schema of this run (one per test module, since each module loads this object on its own). */
  lazy val schema: String = sys.props.getOrElse("reactivecqrs.test.schema",
    runSchemaPrefix + System.currentTimeMillis() / 1000 + "_" + ThreadLocalRandom.current().nextInt(1000, 10000))

  private lazy val initialized: Unit = {
    Class.forName("org.postgresql.Driver")
    val connection = DriverManager.getConnection(url, user, password)
    try {
      dropOldRunSchemas(connection)
      connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schema)
    } finally {
      connection.close()
    }
    ConnectionPool.singleton(url + "?currentSchema=" + schema, user, password,
      ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))
  }

  private def dropOldRunSchemas(connection: Connection): Unit = {
    val oldest = System.currentTimeMillis() / 1000 - keepRunSchemasSeconds
    val names = connection.createStatement().executeQuery(s"SELECT nspname FROM pg_namespace WHERE nspname LIKE '$runSchemaPrefix%'")
    var toDrop = List[String]()
    while (names.next()) {
      val name = names.getString(1)
      val createdAt = name.stripPrefix(runSchemaPrefix).takeWhile(_ != '_').toLongOption
      if (createdAt.exists(_ < oldest)) toDrop ::= name
    }
    toDrop.foreach(name => connection.createStatement().execute("DROP SCHEMA IF EXISTS " + name + " CASCADE"))
  }

  /** Initializes the default ScalikeJDBC connection pool once per JVM, whichever spec runs first. */
  def init(): Unit = initialized
}
