package io.reactivecqrs.testkit

import java.sql.{Connection, DriverManager}
import java.util.concurrent.ThreadLocalRandom

import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

/**
  * The PostgreSQL database for tests that use the store: each test run gets its own schema, so results never depend on
  * what earlier runs left behind, and every run also creates the schema from scratch. Schemas of runs older than six
  * hours are dropped, and only they: `test_run_<epoch>_<n>`. `-Dreactivecqrs.test.schema=<name>` uses (and keeps) a
  * fixed schema instead. The database is `-Dreactivecqrs.test.url`, `.user` and `.password`, by default the library's
  * own (`reactivecqrs` on `localhost:5432`, user and password `reactivecqrs`): a database for tests only. Suites that
  * share it run one at a time, as their tables are one schema's.
  */
object TestDatabase {

  private val url = sys.props.getOrElse("reactivecqrs.test.url", "jdbc:postgresql://localhost:5432/reactivecqrs")
  private val user = sys.props.getOrElse("reactivecqrs.test.user", "reactivecqrs")
  private val password = sys.props.getOrElse("reactivecqrs.test.password", "reactivecqrs")

  private val runSchemaPrefix = "test_run_"

  private def inSchemaUrl(name: String): String = url + (if (url.contains("?")) "&" else "?") + "currentSchema=" + name
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
    ConnectionPool.singleton(inSchemaUrl(schema), user, password,
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

  private val settings = ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L)

  /** Runs `block` with the default pool on a second schema of this run (`<schema>_<suffix>`, created if missing), as a
    * second installation, then on this run's schema again. Nothing else may use the pool meanwhile: the library's
    * objects made inside the block belong to the second installation. */
  def inSchema[A](suffix: String)(block: => A): A = {
    init()
    val other = schema + "_" + suffix
    val connection = DriverManager.getConnection(url, user, password)
    try connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + other) finally connection.close()
    ConnectionPool.singleton(inSchemaUrl(other), user, password, settings)
    try block finally ConnectionPool.singleton(inSchemaUrl(schema), user, password, settings)
  }
}
