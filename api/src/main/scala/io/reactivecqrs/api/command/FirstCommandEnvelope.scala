package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.UserId

case class FirstCommandEnvelope[AGGREGATE, RESPONSE](acknowledgeId: String,
                                                               userId: UserId,
                                                               command: Command[AGGREGATE, RESPONSE])