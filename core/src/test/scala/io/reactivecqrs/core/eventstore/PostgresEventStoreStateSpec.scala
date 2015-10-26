package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons

class PostgresEventStoreStateSpec extends EventStoreStateSpec(new PostgresEventStoreState(new MPJsons))