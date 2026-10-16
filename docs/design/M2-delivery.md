# M2 design: the delivery module

How milestone M2 implements the delivery decisions (D3, D4, D7, D8, D15, D20a–c of
`KNOWN-ISSUES.md`). Written before the work, 16/10/2026; `ARCHITECTURE.md` §5–§7 describes the
result.

**Built as designed, with these differences** (16/10/2026, M2 close): the owner's targeted
re-publish message is `RepublishRange(id, from, subscriber)`; subscription failures are kept
in a table of their own, `subscription_failures`, not in new columns of `subscriptions`; the
PostgreSQL store deletes delivered outbox rows in batches (one statement for many acks); the
poller re-publishes at most 1 000 aggregates per poll, the longest waiting first; re-publications
go in parts of 1 000 events; a delivery waits for each (subscriber, kind), and a stopped subscriber
keeps its subscriptions (its events stay in the outbox); a retry is repeated until applied; a gap
open for 10 minutes becomes a recorded failure (review 4, KNOWN-ISSUES F83–F90).

## Contract (D3)

At least once, in order per aggregate, applied exactly once: every subscriber eventually
applies every committed event of the aggregate types it subscribed to, in version order;
duplicates are dropped by the subscriber's stored version (I4). Failures and silence are
bounded, visible and repairable; delivery bookkeeping returns to zero when idle.

## Modules and their interfaces

| Module | Job | Interface |
|---|---|---|
| Owner (`AggregateRepositoryActor`) | publishes what it committed; re-publishes on request; deletes outbox rows once acked | `PublishEvents` out; `RepublishOutbox(id)`, `RepublishFrom(id, from, onlyTo)`, `PublishEventsAck` in |
| Event bus (`EventsBusActor`) | routes deliveries; tracks each delivery until ack, failure, deadline or subscriber stop; routes re-publish requests to the type's command bus | `RegisterSubscribers`, `RegisterPublisher`, `PublishEvents`, `MessageAck`, `MessageFailed`, `RequestRepublish`, `Republish`, `GetDeliveryStatus` |
| Outbox poller (`OutboxPollerActor`, new) | the single owner of retries (D8): finds outbox rows that waited too long and asks their owners to re-publish | reads `events_to_publish`; sends `Republish(type, id)` to the bus |
| Re-delivery policy (pure) | decides which aggregates to re-publish now and which became undelivered | `(rows, memory, now) => (republish, undelivered, memory)` |
| Sequence gate (pure, D4) | decides for one delivery: already applied, apply a range, or fill a gap | `decide(applied, delivered) => Decision` |
| Subscriber (`ProjectionActor`) | applies deliveries through the gate, one aggregate at a time; retries and records failures (D20a) | `GetSubscriptionRequests` in; `MessageAck`, `MessageFailed`, `RequestRepublish` out; `GetProjectionStatus`, `RetryFailedSubscription`, `SkipFailedVersion` |
| `Backoff` (pure, AR6) | delay and limit for an attempt number | `delay(attempt)`, `exhausted(attempt)` |

## Flows

**Live publication.** After commit the owner publishes the new events with its state. The bus
records one delivery per (event, subscription: subscriber and kind) and sends each subscriber one message per kind
(`IdentifiableEvents`, `AggregateWithType`, `AggregateWithTypeAndEvents`, unchanged). A
subscriber's `MessageAck` or `MessageFailed` ends its deliveries; when all deliveries of an event
have ended by ack or failure, the bus sends the owner `PublishEventsAck`, and the owner deletes
the outbox rows (one statement per ack; a failed delete only means a later harmless re-publish).

**Lost or silent delivery (D20b, D8).** The bus does not re-send by itself. Every poll (30 s)
the poller reads the aggregates that have outbox rows. Its re-delivery policy asks the owner,
through the bus and the type's command bus, to re-publish an aggregate when a row has waited
3 minutes, again at 6 minutes, and gives up at 10 minutes ("undelivered", logged once; the row
stays). Rows written before this process started get a fresh set of attempts, the first one
at the first poll. The owner re-publishes from its oldest outbox version up to its current
version, with its current state, so a state-receiving subscriber always gets a consistent
range; subscribers drop what they already applied.

**Gap (D20c, refined).** A subscriber at version 5 that receives 7 asks for a re-publication
from 6 (`RequestRepublish`); the bus forwards it to the owner, which reads 6..current from the
event store and publishes it to that subscriber only. All three listener kinds work the same
way, and subscribers need no event-store access. While the request is pending, other deliveries
for that aggregate are left unanswered (the re-publication covers them); an unanswered request is
repeated after a short time. Re-publications sent to one subscriber never ack the owner, so they
cannot delete outbox rows that other subscribers still wait for.

**Subscriber failure (D20a).** A listener that throws is retried for that aggregate with back-off
for about 2 minutes; deliveries for the aggregate meanwhile stay unanswered. Then the failure is
stored in the subscription row (failed version, error, since) and the subscriber replies
`MessageFailed` for that delivery and for every later one of that aggregate. Other aggregates keep
flowing. `GetProjectionStatus` lists the failures; `RetryFailedSubscription` (and, rate-limited,
the projection's start) clears one and asks for a re-publication from the failed version;
`SkipFailedVersion` marks the failed version as applied without applying it. A permanent delete
waits while any subscription of the aggregate is failed (F76).

**Start-up (D15).** The host creates its subscribers and sends the bus `RegisterSubscribers`. The
bus asks each subscriber for its subscription requests and stashes publications until all have
answered, logging the missing ones by name meanwhile; there is no blocking wait and no count to
maintain. Command buses register themselves as the publishers of their aggregate type, so the bus
can route re-publish requests. Subscriptions are fixed after start.

## Refinements of the decisions

- **D20c:** subscribers fill a gap by asking the owner to re-publish, for all listener kinds, rather
  than event listeners reading the store themselves. Same result, one mechanism, and subscribers
  keep no event-store dependency (the owner already has the store and the event-class map).
- **D8, D20b:** the re-delivery attempts at 3 and 6 minutes and the 10-minute deadline are counted
  by the poller per aggregate; the bus only keeps deliveries and their deadline. One clock source
  for retries.

## What goes away

`EventBusState` and the `event_bus` table use (F1, F4, F11, F47), `EventBusSubscriptionsManager`
(F41, F42), the owner's `eventsToPublish` and `pendingPublish` and its re-send timers (F2, F72,
F33), the command buses' outbox scans (F37), the projection's delay buffers that wait for gaps
(F45), and the per-aggregate queues that are never pruned (F15). The replay back-pressure protocol
stays until M4 replaces the replayer.

## Order of the work

1. `Backoff` (AR6), moving the saga write retries onto it.
2. Sequence gate with its property test (10 000 generated cases per run).
3. Event-store range read and the owner's re-publish messages.
4. Bus rewrite with registration, deliveries, deadline, subscriber watch, failure replies and the
   publisher registry; subscribers answer `GetSubscriptionRequests`; the manager is removed.
5. Outbox poller and re-delivery policy; the owner's in-memory re-send and the command buses'
   scans are removed; the permanent-delete trigger moves to the owner.
6. Subscription state: failure columns, bounded cache (F29), clearing caches (F6, F77).
7. Projection actor over the gate, with the failure policy, status, retry and skip; no throws from
   `receive` (AR7, F80); delayed queries coalesced (F28).
8. The M2 verification of the plan, the soak run in the bench, documentation, review, tag.
