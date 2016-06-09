package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object PostgresEventStoreStateSpec {
  def storeState = {
    val settings = ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L)

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

    val store = new PostgresEventStoreState(new MPJsons)
    store.initSchema()
    store
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState)