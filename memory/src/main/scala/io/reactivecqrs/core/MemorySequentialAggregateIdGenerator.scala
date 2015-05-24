package io.reactivecqrs.core

import java.util.concurrent.atomic.AtomicLong
import io.reactivecqrs.api.guid.AggregateId

class MemorySequentialAggregateIdGenerator extends AggregateIdGenerator {

  private val id = new AtomicLong(0L)

  override def nextAggregateId = AggregateId(id.getAndIncrement)

}
