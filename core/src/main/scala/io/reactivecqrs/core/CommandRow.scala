package io.reactivecqrs.core

import java.time.Instant

import io.reactivecqrs.api.guid.{CommandId, UserId}

case class CommandRow[COMMAND](commandId: CommandId,
                      userId: UserId,
                      creationTimestamp: Instant,
                      command: COMMAND)
