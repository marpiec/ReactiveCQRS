package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object PostgresEventStoreStateSpec {
  def storeState = {
    val settings = ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L)

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

    val typesNamesState = new PostgresTypesNamesState().initSchema()

    new PostgresEventStoreState(new MPJsons, typesNamesState).initSchema()
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState)