package io.reactivecqrs.core

import java.util.concurrent.atomic.AtomicLong

import io.reactivecqrs.api.CommandIdGenerator
import io.reactivecqrs.api.id.CommandId

class PostgresSequentialCommandIdGenerator extends CommandIdGenerator {

  private val id = new AtomicLong(0L)

  override def nextCommandId = CommandId(id.getAndIncrement)

}
