package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase

object PostgresEventStoreStateSpec {
  def storeState = {
    TestDatabase.init()

    val typesNamesState = new PostgresTypesNamesState().initSchema()

    new PostgresEventStoreState(new MPJsons, typesNamesState).initSchema()
  }
}

class PostgresEventStoreStateSpec extends EventStoreStateSpec(PostgresEventStoreStateSpec.storeState)
