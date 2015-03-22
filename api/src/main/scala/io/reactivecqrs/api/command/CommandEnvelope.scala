package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, UserId}

case class CommandEnvelope[AGGREGATE, RESPONSE](acknowledgeId: String,
                                                userId: UserId,
                                                aggregateId: AggregateId,
                                                expectedVersion: AggregateVersion,
                                                command: Command[AGGREGATE, RESPONSE])