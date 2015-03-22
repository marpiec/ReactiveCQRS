package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.UserId

case class CommandEnvelopeForNewAggregate[AGGREGATE, RESPONSE](acknowledgeId: String,
                                                               userId: UserId,
                                                               command: Command[AGGREGATE, RESPONSE])