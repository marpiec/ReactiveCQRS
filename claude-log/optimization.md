Done. With MemoryEventBusState as the production bus, the whole analysis shifts — the EventBus does no Postgres I/O, so its bottleneck is its own in-memory data structures and the single actor thread, not the database. Here's the reevaluated
result and what I changed.

Reevaluated bottleneck (in-memory bus)

The smoking gun is in EventsBusActor.handleMessageAck. It did:

messagesSent.view.filterKeys(e => eventsIds.contains(e)) ... .partition(...)

filterKeys(...).partition has no index — forcing it scans the entire in-flight messagesSent map on every ack. So each ack is O(in-flight), and a burst of N aggregates is O(buffer²). At MAX_BUFFER_SIZE=1000 that's ~10⁷ ops over 10 000
aggregates (tolerable but real); it also meant raising the buffer would make things quadratically worse, not better. That's the core reason propagation lags while storage (batched per aggregate) keeps up.

Changes made (2 files — compiles clean)

1. EventsBusActor.handleMessageAck — O(events-in-ack) instead of O(in-flight).
   Each acked event is now looked up directly in messagesSent; the confirming subscriber is dropped, and an event whose receiver set empties is finished and removed. Cursor advance, publisher confirmation (PublishEventsAck), back-pressure
   accounting, and eventsPropagatedNotPersisted semantics are preserved exactly — only the lookup strategy and now-dead debug lines changed.

2. EventsBusActor — MAX_BUFFER_SIZE default 1000 → 10000. Now safe and beneficial because per-ack cost no longer scales with buffer size; lets far more aggregates pipeline through the bus before back-pressure stalls.

3. SubscriptionsState — narrowed synchronized + eventsLogger = None default. The DB read (lastAggregateVersionFromDB) and write (newEventIdInDB) now run off the monitor (double-checked cache install); only the in-memory map compare-and-set is
   locked. This is what lets parallelUpdateProcessing workers actually run concurrently instead of serializing on the subscription lock — the "more parallelism in projection" you asked for. The = None default also fixes pre-existing 2-arg call
   sites.

I reverted the earlier PostgresEventBusState work — it's the dead path now.

Remaining levers (config / deferred, not code I changed)

- Enable parallelUpdateProcessing(n) on the heavy projections (default is 0 = serial). With the lock narrowing above, the per-aggregate listener localTx writes now genuinely parallelize. Ensure the ScalikeJDBC pool size ≥ total worker
  threads, or they just block on connections. I left the default at 0 deliberately — flipping it globally would silently make every listener multi-threaded (the API doc warns thread-safety becomes the listener's problem).
- Subscription read still on the projection actor thread (one cheap readOnly SELECT per new aggregate). I did not move it into the worker: gap-detection there mutates actor state (delayedAggregateUpdates, triggerScheduledForAggregate), and
  touching those off the actor thread is the exact data race CLAUDE.md warns against. Doing it safely needs a message round-trip refactor — worth a follow-up if that SELECT becomes the next ceiling, but it's secondary to the two fixes above.

Validation note
Each acked event is now looked up directly in messagesSent; the confirming subscriber is dropped, and an event whose receiver set empties is finished and removed. Cursor advance, publisher confirmation (PublishEventsAck), back-pressure
accounting, and eventsPropagatedNotPersisted semantics are preserved exactly — only the lookup strategy and now-dead debug lines changed.

2. EventsBusActor — MAX_BUFFER_SIZE default 1000 → 10000. Now safe and beneficial because per-ack cost no longer scales with buffer size; lets far more aggregates pipeline through the bus before back-pressure stalls.

3. SubscriptionsState — narrowed synchronized + eventsLogger = None default. The DB read (lastAggregateVersionFromDB) and write (newEventIdInDB) now run off the monitor (double-checked cache install); only the in-memory map compare-and-set is
   locked. This is what lets parallelUpdateProcessing workers actually run concurrently instead of serializing on the subscription lock — the "more parallelism in projection" you asked for. The = None default also fixes pre-existing 2-arg call
   sites.

I reverted the earlier PostgresEventBusState work — it's the dead path now.

Remaining levers (config / deferred, not code I changed)

- Enable parallelUpdateProcessing(n) on the heavy projections (default is 0 = serial). With the lock narrowing above, the per-aggregate listener localTx writes now genuinely parallelize. Ensure the ScalikeJDBC pool size ≥ total worker
  threads, or they just block on connections. I left the default at 0 deliberately — flipping it globally would silently make every listener multi-threaded (the API doc warns thread-safety becomes the listener's problem).
- Subscription read still on the projection actor thread (one cheap readOnly SELECT per new aggregate). I did not move it into the worker: gap-detection there mutates actor state (delayedAggregateUpdates, triggerScheduledForAggregate), and
  touching those off the actor thread is the exact data race CLAUDE.md warns against. Doing it safely needs a message round-trip refactor — worth a follow-up if that SELECT becomes the next ceiling, but it's secondary to the two fixes above.

Validation note

I verified via sbt core/compile (clean; all warnings pre-existing) and careful semantic review. I could not run the specs: the test suite has pre-existing ScalaTest-3.2 rot (FeatureSpecLike/MustMatchers/scenario/feature renames, plus stale
IdentifiableEvents signatures) across ~6 files, unrelated to this work — which is also why the broken 2-arg PostgresSubscriptionsState calls were never caught. Migrating the suite is a separate task; say the word if you want me to take it on
so these changes can be exercised against your local Postgres.
