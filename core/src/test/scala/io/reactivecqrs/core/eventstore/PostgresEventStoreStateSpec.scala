package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.core.types.PostgresTypesState
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object PostgresEventStoreStateSpec {
  def storeState = {
    val settings = ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L)

    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

    val typesState = new PostgresTypesState().initSchema()

    new PostgresEventStoreState(new MPJsons, typesState).initSchema()
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState)