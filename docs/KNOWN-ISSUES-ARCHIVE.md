# ReactiveCQRS — closed issues

The issues of the 1.0 line that are closed: fixed (each with the test that showed it), accepted, decided, or found
not to be a problem. They were recorded in [`KNOWN-ISSUES.md`](KNOWN-ISSUES.md), which keeps the open ones, the
decisions, the invariants and the architecture reviews; the [index by milestone](KNOWN-ISSUES.md#6-index-by-milestone)
lists them all. Each entry is as it was when it was closed. Severity and confidence are explained at the top of
`KNOWN-ISSUES.md`.

### Delivery and subscriptions

#### F201 — A stale failure record becomes a false "versions missing" failure; skipping it moves the subscription past the aggregate, and the next event is acknowledged unapplied
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2 of the risk assessment; `ProjectionDeliverySpec` "F201", two scenarios, red under three mutations) · `ProjectionActor`, `AggregateOwner`
- **Fix:** a retried failure whose version the subscription has applied is cleared at once, and a retried failure keeps its place in its queue until its record is deleted (the clear is tried again). An owner asked for versions beyond its own answers `NothingToRepublish(id, version)`, which closes the gap and clears a failure of versions that never existed. A skip of a "versions from N are missing" failure marks nothing and asks from N again. A failed load of the failure records is tried again, and `ServeUnchanged` keeps the failures. The owner's answer is covered by reading; the projection's handling of it by the spec.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F254 — Transient database errors become durable recorded failures in live projections
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `ProjectionDeliverySpec` "F254", red under a mutation) · `ProjectionActor`, `Errors.transient`
- **Fix:** a batch that fails on the database (`Errors.transient`, the builder's F117 check moved to `Errors`: SQLState classes 08, 40, 53, 57, or a SQL error without a state) counts no attempt: it is tried again every `retries.max` (30 s), with one warning a minute, and never recorded as a failure.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F259 — After a targeted reset, a live event of a target aggregate re-runs side effects for its whole history
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `ProjectionDeliverySpec` "F259", red under a mutation) · `ProjectionActor`
- **Fix:** a gap found while the subscription is at version 0 marks the first live version waiting; a batch that starts below it is applied as history (`rebuilding` true: no pushes). The owner re-publishes up to its version, so the live version may come in that batch and is applied as history too: one push less, never a push of the past. The same covers a subscriber added late.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F188 — The event bus waits forever for a subscriber that cannot answer, and stashes every publication meanwhile
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `EventsBusActorSpec` "F188", red first) · `EventsBusActor`
- **Symptom:** a registered subscriber that never answered (a projection that could not start, a proxy whose singleton runs nowhere yet) kept the bus in its start-up state: nothing was delivered on the node, the node never became ready, and every publication was stashed in an unbounded stash with its events and state.
- **Fix:** D87. Publications are not stashed: the bus keeps their aggregates (at most 100 000) and has the owners re-publish their outboxes once it starts, 1 000 a tick. After `registrationTimeout` (2 minutes) the bus starts without the silent subscribers ("Event bus starts without the subscriptions of: ..."); while one has not answered, every delivery is incomplete, so the outbox rows stay, and once it answers or is removed the kept aggregates are re-published.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F187 — A release that adds a singleton cannot finish a readiness-gated rolling upgrade
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment, with F188; `EventsBusActorSpec` "F188") · `EventsBusActor`, the book's chapter 17
- **Symptom:** a release adding a projection or saga runner registered on every bus: the new nodes' proxies found no singleton (it runs on the oldest node, still of the old release and replaced last), their buses never started, the nodes never became ready, and Kubernetes never went on to the next pod.
- **Fix:** D87: the new nodes' buses start after `registrationTimeout`, keep their events in the outbox until the singleton answers from a new node, and re-publish them then. The book's chapter 17 says when a new singleton starts. Mirroring F132's wait for `start` was not needed.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F255 — A subscriber that stops before answering is dropped silently, and the node reports ready
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `EventsBusActorSpec` "F255", red first) · `EventsBusActor`
- **Symptom:** a registered subscriber that stopped at start was dropped without a word: the bus started and acknowledged every event to the others, so the outbox rows the subscriber would have taken were deleted unseen.
- **Fix:** it is left unanswered (D87): an error says it never subscribed, and every event keeps its outbox row until it is removed (`RemoveSubscriber`) or the node restarts.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F1 — In-memory bus cursor never advances for aggregates that existed before the process started
- **Medium**, likely · **Fixed in M2** (16/10/2026, bus rewrite: no cursor, D7; `EventsBusActorSpec` "Bookkeeping") · Owning module: Delivery (`EventsBusActor`, `EventBusState`)
- **Symptom:** memory grows with uptime; per-ack CPU grows; the bus-side duplicate filter does nothing for older aggregates.
- **Root cause:** `MemoryEventBusState` starts every aggregate at 0 (`EventBusState.scala:22-27`); the first finished version of an older aggregate is never cursor+1, so every acked version is appended to `eventsPropagatedNotPersisted` and re-sorted (`EventsBusActor.scala:435-448`). `publishedEvents` (`:95`) and the state map (`EventBusState.scala:20`) are never evicted either.
- **Related:** F4, F11, F47; D7. **Accepted production risk until 1.0 (D80).**
- **Test:** publish v11 for an aggregate unknown to the bus, ack it, repeat; assert bus bookkeeping returns to empty.
- **Fix:** delete the cursor and its maps (D7).
- **Invariants:** I3, I4.

#### F2 — Unacknowledged events are not re-sent while their repository actor is alive
- **High**, likely · **Fixed in M2** (16/10/2026, outbox poller, D8; `OutboxPollerSpec`) · Delivery, `AggregateRepositoryActor`, `AggregateCommandBusActor`
- **Symptom:** a projection that once failed on an event (transient error) stays behind for that aggregate; later events are buffered as gaps until the actor is reaped (12 h idle, or never for a busy aggregate).
- **Root cause:** the re-send list is loaded only at restore (`AggregateRepositoryActor.scala:77-99`); `pendingPublish` is never re-sent; `EnsureEventsPublished` skips existing actors (`AggregateCommandBusActor.scala:132-153`); listener exceptions are only logged (`ProjectionActor.scala:476-477, 623-626, 709-710`).
- **Related:** F45, J3, R3; D8, D20a–c.
- **Test:** drop the first delivery to a subscriber while the aggregate stays active; assert delivery within the re-delivery interval.
- **Fix:** D8 (single retry owner reading the outbox), D20a–c.
- **Invariants:** I3.

#### F3 — In-memory subscription mode loses progress unless `dump()` is called
- **Low**, likely (Neula uses it only in its rebuild process) · **Fixed in M4** (17/10/2026: the mode and `dump()` are removed; the database is the only source of truth, and one transaction may move a subscription several times, D61; `PostgresSubscriptionsStateSpec`, red under a mutation that drops the transaction's own versions) · `SubscriptionsState`
- **Symptom:** a live node built with `keepInMemory = true` stalls projections after a restart.
- **Root cause:** progress is written only by `dump()` (`SubscriptionsState.scala:246-255, 303-350`), called only by the replayer (`EventsReplayerActor.scala:179-182, 198`).
- **Fix:** remove the mode (D61).

#### F4 — Already-published events are dropped without an ack; the in-flight counter leaks
- **Medium**, confirmed · **Fixed in M2** (16/10/2026, bus rewrite: no cursor, D7) · Delivery
- **Reproduced by:** `EventsReplaySpec` (PostgreSQL bus state) when the schema already holds ShoppingCart events, written by earlier runs or, with per-run schemas, by the specs that run before it: the replay never receives `ConsumerFinished` and times out after 50 s. It passes on a schema without earlier events.
- **Regression test:** since M2 `EventsReplaySpec` replays notes of its own aggregate type after a live projection applied them and their outbox rows were deleted. Replaying the other specs' carts, as before, stalls on their undone events instead (F58).
- **Symptom:** outbox rows of re-sent events are never deleted and re-sent every 180 s; back-pressure capacity shrinks; a replay against an advanced cursor never finishes.
- **Root cause:** events at or below the cursor are filtered (`EventsBusActor.scala:294`) but counted in `receivedInProgressMessages` (`:358`) and never finished; no `PublishEventsAck` is sent (`:365-368`).
- **Related:** F1; D7.
- **Fix:** removed together with the cursor (D7); keep counters balanced in the new bus.

#### F5 — Event listeners receive already-processed events on a partial overlap
- **High**, likely · **Fixed in M2** (16/10/2026: every listener kind goes through the sequence gate and gets only the versions after its applied one; `ProjectionDeliverySpec`) · `ProjectionActor` (sequence gate)
- **Symptom:** non-idempotent event listeners apply events twice (wrong read models); a duplicate `insertDocument` throws, the batch is never acked and the aggregate stalls.
- **Root cause:** `processEventsUpdate` passes `e.events` instead of the new events (`ProjectionActor.scala:437, 445, 461, 467`), unlike the aggregate-with-events path (`:608, 615`). Overlaps happen through re-sends.
- **Related:** F1, F2; D4.
- **Test:** sequence gate with offset 3 receives 2..5; assert the listener sees 4..5 only.
- **Fix:** D4 (one sequence gate for all listener kinds).
- **Invariants:** I4.

#### F6 — Clearing a projection's subscriptions leaves the caches behind
- **Medium**, likely · **Fixed in M2** (16/10/2026: clearing removes the subscriber's cached versions and failures too; `PostgresSubscriptionsStateSpec`, red first), obsolete after **M4** · `SubscriptionsState`
- **Symptom:** rebuilding a live projection skips aggregates still cached; in-memory mode updates rows that no longer exist.
- **Root cause:** `clearSubscriptionsInfo` deletes rows only (`SubscriptionsState.scala:291-294`); caches at `:131-132`.
- **Fix:** clear caches with the rows; shadow rebuild (D57) removes the clear step.

#### F11 — Bus cursor can skip an unfinished version
- **Low**, likely (needs custom classifiers; Neula has none) · **Fixed in M2** (16/10/2026, removed with the cursor, D7) · Delivery
- **Root cause:** one ack finishing a non-contiguous set moves the cursor to its last version (`EventsBusActor.scala:433-441`).

#### F15 — Per-aggregate async queues in parallel mode are never pruned
- **Low**, likely · **Fixed in M2** (16/10/2026: one queue per aggregate and listener kind, removed when idle; in parallel mode only one batch per aggregate runs at a time) · `ProjectionActor`
- **Root cause:** `states` grows per aggregate (`ProjectionActor.scala:120, 566`); used by Neula's two parallel listeners.
- **Fix:** remove a queue when it drains (sequence gate rework).

#### F18 — Subscription cache advanced before the transaction commits
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `SubscriptionsState`
- **Fix:** `PostgresSubscriptionsState.localTx` is an `AfterCommit.localTx`; the cache update is registered with `AfterCommit` and runs only after the commit. Test: `CachesAfterRollbackSpec` (subscription scenarios; the rollback one failed before the fix).
- **Symptom:** if the commit itself fails, later deliveries count as already processed and are skipped (Neula J2).
- **Root cause:** in DB mode the cache update runs inside the transaction block (`SubscriptionsState.scala:256-271`), before commit.
- **Test:** listener transaction whose commit fails; assert the same events are delivered again (Neula R2 acceptance).
- **Fix:** advance the cache after commit.
- **Invariants:** I5.

#### F26 — Outbox write amplification; one DELETE per ack, fire-and-forget
- **Medium**, likely · **Fixed in M2** (16/10/2026: the PostgreSQL store deletes delivered rows in batches on one thread, at most 20 ms late, and logs a failed batch with its cause; the old error log dropped the exception) · Delivery, `PostgresEventStoreState`
- **Root cause:** every event is inserted into and deleted from `events_to_publish`; the delete runs in an unsupervised `Future` per ack (`AggregateRepositoryActor.scala:377-382`).
- **Fix:** batched delete (`event_id = ANY(?)`) from one writer with failure handling; consider `(aggregate_id, version)` as the key. A published-version cursor instead of the table is a post-1.0 option (D44).

#### F28 — Every processed update re-runs all pending delayed queries
- **Low**, likely · **Partly fixed in M2** (16/10/2026: one replay per burst of applied updates. Keying the queries by aggregate needs the query to name its aggregate, an API change of little value; left as is) · `ProjectionActor`
- **Root cause:** `ReplayQueries` after each processed update (`ProjectionActor.scala:492-494, 641-643, 725-727, 771-793`); `scheduleNearest` scans the whole list.
- **Fix:** keyed delayed queries; coalesce replays (D34).

#### F29 — Subscription cache never evicted in live (database) mode
- **Medium**, likely · **Fixed in M2** (16/10/2026: an LRU cache of at most `maxCachedAggregates`, 10 000 by default; the in-memory replay mode keeps everything until `dump()`) · `SubscriptionsState`
- **Symptom:** memory grows with every aggregate a projection ever touched (about 0.5–1 KB each, estimate).
- **Root cause:** `perAggregate` is evicted only by `dump()`, which live mode never calls (`SubscriptionsState.scala:131-132, 196-223`).
- **Fix:** bounded cache; the database is the source of truth (D34).

#### F66 — The periodic check for unpublished events never ran a query
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresEventStoreState.readAggregatesWithEventsToPublish`
- **Symptom:** after the first check 30 s after start, events whose publication was lost were never found again by `EnsureEventsPublished` (every 60 s); only a repository actor restart re-sent them. Worsens F2.
- **Root cause:** `.bind(...).stripMargin.foreach` was chained after an `if/else`, so it applied only to the `else` branch; with `oldOnly = true` the method built a query and returned without running it.
- **Fix:** build the query, then bind and run it. Test: `EventStoreStateSpec` "… ids beyond 32 bits" (uses `oldOnly = true`).

#### F37 — Outbox scan query per aggregate type every minute
- **Low**, likely · **Fixed in M2** (16/10/2026: one outbox poller query for all types every 30 s) · `AggregateCommandBusActor`, `PostgresEventStoreState`
- **Root cause:** each command bus runs its own scan (`PostgresEventStoreState.scala:352-374`), 51 per minute in Neula.
- **Fix:** one outbox poller for all types (D8; a singleton in the cluster, D46).

#### F41 — Bus initialization blocks and depends on a hand-maintained subscription count
- **Medium**, likely · **Fixed in M2** (16/10/2026, D15: `RegisterSubscribers`, no blocking wait, no count) · `EventsBusActor`, `EventBusSubscriptionsManager`
- **Symptom:** a wrong count stalls the bus at startup (180 s cycles that drop the triggering message) or starts it without the last projections.
- **Root cause:** `Await` of up to 180 s in `initSubscriptions` (`EventsBusActor.scala:187-205`); Neula hard-codes 106 (H2).
- **Fix:** D15.

#### F42 — A late subscription crashes the subscriptions manager and wipes its state
- **Medium**, likely · **Fixed in M2** (16/10/2026, manager removed by D15) · `EventBusSubscriptionsManager`
- **Root cause:** `throw` at `EventBusSubscriptionsManager.scala:69`; the default restart clears `subscriptionsRequests`.

#### F45 — Unbounded projection delay buffers
- **Medium**, likely · **Fixed in M2** (16/10/2026: a gap is asked from the owner, again every 10 s while it lasts; only the deliveries in flight wait; `ProjectionDeliverySpec`) · `ProjectionActor`
- **Root cause:** `delayed*Update` maps (`ProjectionActor.scala:105-107`) hold messages until a gap fills, which may never happen (F2).
- **Fix:** gap fetch from the store (D20c).

#### F47 — `PostgresEventBusState` batch update ignores row counts
- **Low** (unused by Neula), likely · **Fixed in M2** (16/10/2026, deleted, D7) · `EventBusState.scala:127-140`

#### F77 — A failed compare-and-set leaves the subscription cache stale
- **Medium**, likely · **Fixed in M2** (16/10/2026: a failed database write evicts the aggregate's cached versions; `PostgresSubscriptionsStateSpec`, red first) · `PostgresSubscriptionsState.newEventIdInDB`
- **Symptom:** when the stored subscription version moved without this instance's cache (another node, a clear in the database, a repair), every later update of that aggregate for that projection fails the same way: the projection is stuck on it until the process restarts.
- **Root cause:** the failed `UPDATE … WHERE aggregate_version = ?` throws, the transaction rolls back, and the cached version is kept; the next attempt starts from it again.
- **Found:** plan review at the M1 close (review 3, §7).
- **Fix:** evict the cached entry when the database check fails, so the next attempt reads the stored version.

#### F79 — The event bus's 120-second debug timer changes actor state from another thread
- **Medium**, likely · **Fixed in M2** (16/10/2026, bus rewrite: the timer sends the bus a `Tick` message) · `EventsBusActor` constructor (`scheduler.schedule(120 s, …, new Runnable …)`)
- **Symptom:** the timer iterates `messagesSent` while the actor changes it, updates `lastLogged` and `orderedMessagePostponedTimestamp`, and calls `orderMoreMessagesToConsume()`: data races on the bus's mutable state, possibly `ConcurrentModificationException` or a corrupted map.
- **Root cause:** the timer runs a `Runnable` on a dispatcher thread instead of sending the actor a message.
- **Found:** plan review at the M1 close.
- **Fix:** the rewritten bus sends itself a tick message; statistics are read in `receive` only.

#### F80 — A failing projection query restarts the projection actor
- **Medium**, likely · **Fixed in M2** (16/10/2026: a failing message or query is answered with `Status.Failure`, never rethrown; the parallel pool is shut down on stop) · `ProjectionActor.delayIfNotAvailable` and the "empty list of events" throws
- **Symptom:** a query whose search throws is answered with the failure and then thrown again, which restarts the actor: its delay buffers and waiting queries are dropped, and with parallel processing a new thread pool is created while the old one is never shut down.
- **Root cause:** exceptions leave `receive`; no supervision policy (AR7); the thread pool is created per actor instance.
- **Found:** plan review at the M1 close.
- **Fix:** answer the failure without rethrowing; one shared, closed-on-stop pool or the blocking dispatcher (F27).

Found at the M2 close (review 4: the delivery soak of the bench, a read-only audit, our own reading). All
but F93 are fixed in M2; the tests named were red first, or red under a mutation that restores the old behaviour.

#### F83 — A long re-publication went out in one message, and a busy projection asked for it again every 10 s
- **High**, confirmed (the soak ran out of a 2 GB heap) · **Fixed in M2** · `AggregateRepositoryActor.republish`, `ProjectionActor.requestMissingVersions`
- **Symptom:** a projection that starts behind an aggregate with 30 000 events asks for the whole history; the owner sends it in one message, and while the projection works through its mailbox the 10-second re-request sends it again and again, until the heap runs out.
- **Fix:** re-publications go in parts of at most 1 000 events (`maxRepublishEvents`): the owner sends the next outbox part when the previous one is acknowledged, a projection asks for the next part of a gap itself. Gap requests repeat after 10 s, doubling up to 5 minutes. Tests: `RepublishSpec` (parts), `ProjectionDeliverySpec` (back-off).

#### F84 — The bus tracked a subscriber, not each of its subscriptions
- **High** if a projection has two listener kinds for one aggregate type, confirmed · **Fixed in M2** · `EventsBusActor`
- **Symptom:** one kind's acknowledgement ended the delivery for the other kind too: the owner deleted the outbox row while the other kind still retried, and a permanent delete could run before the slower kind applied it.
- **Fix:** a delivery waits for each (subscriber, kind); `MessageAck` and `MessageFailed` carry the `DeliveryKind`. Test: `EventsBusActorSpec` "waits for each kind a subscriber takes".

#### F85 — A failure that could not be stored was still answered as failed
- **High**, confirmed · **Fixed in M2** · `ProjectionActor.giveUp`
- **Symptom:** during a database outage longer than the retries, `recordFailure` failed too, but the deliveries were answered `MessageFailed`: the outbox rows were deleted and, after a restart, neither the rows nor the failure existed; the aggregate stayed stale until its next event.
- **Fix:** nothing is answered until the failure is stored; the batch is tried again every 30 s meanwhile. Test: `ProjectionDeliverySpec` "a failure that cannot be stored is not answered".

#### F86 — Deliveries covered by a batch were acknowledged before the batch was applied
- **Medium**, confirmed · **Fixed in M2** · `ProjectionActor.drainFrom`
- **Symptom:** a delivery whose versions all lay in the batch being assembled was acknowledged at once; if the batch then failed and the process stopped, its outbox row was gone (a gap fill has no rows of its own).
- **Fix:** such deliveries join the batch and are acknowledged when it is applied; only deliveries at or below the stored version are acknowledged at once. Test: `ProjectionDeliverySpec` "a delivery covered by a batch that then fails".

#### F87 — A stopped subscriber silently missed later events
- **Medium**, confirmed · **Fixed in M2** · `EventsBusActor.subscriberStopped`
- **Symptom:** the bus dropped a stopped subscriber's subscriptions, so later events were acknowledged without it and their rows deleted; after a restart it never received them for idle aggregates.
- **Fix:** a stopped subscriber keeps its subscriptions, marked stopped: deliveries it would take end without an owner ack, so the rows stay until the next start; `DeliveryStatus.stopped` lists it. Test: `EventsBusActorSpec` "a subscriber that stops ... later events stay in the outbox for it".

#### F88 — Retrying a failure discarded it before the re-published versions arrived
- **Medium**, confirmed · **Fixed in M2** · `ProjectionActor.retry`
- **Symptom:** the failure record was cleared and one untracked request sent; when that request was lost (no command bus registered yet at start, an owner in failed restore) the aggregate stayed stale with no record.
- **Fix:** a retry is a tracked request, repeated like a gap until a batch reaches the failed version; then the record is cleared. `RetryAllFailedSubscriptions` retries every failure, 20 per second. A skip still asks once for the versions after it: there may be none. Test: `ProjectionDeliverySpec` "RetryFailedSubscription asks for the failed version again, until it comes".

#### F89 — In parallel mode a gap check could start a second batch while one was running
- **Low**, likely · **Fixed in M2** · `ProjectionActor` (`Drain`, `Applied`)
- **Fix:** a scheduled drain during a running batch is left to the batch's end, and a batch's result is tied to its queue. Test: `ProjectionDeliverySpec` "in parallel mode a gap check while a batch runs".

#### F90 — A gap that could not be filled was asked for forever
- **Low**, confirmed · **Fixed in M2** · `ProjectionActor.requestMissingVersions`
- **Symptom:** a deleted aggregate, a missing publisher or a failing owner left the projection asking every 10 s, its queue never idle, the owner never reaped.
- **Fix:** after `gapGiveUpAfter` (10 minutes) the gap is recorded as a failure at its first version, visible and repairable. Test: `ProjectionDeliverySpec` "a gap that is never filled".

#### F91 — Delivered rows stayed in the outbox when their owner had been reaped
- **Medium**, confirmed (measured) · **Fixed in M2** (re-measured in `baseline-M2`) · `AggregateRepositoryActor`, `AggregateCommandBusActor`, `PostgresEventStoreState` (`OutboxDeletes`)
- **Symptom:** after 1 000 000 events at about 12 000 commands/s, 555 666 delivered rows (546 704 in a second run) were still in the outbox after the projections caught up; only the poller's re-publications, 1 000 aggregates per 30 s, would clear them.
- **Root cause:** the bus acknowledged the owners, and the command bus reaps idle owners (beyond 200, idle more than 2 minutes, every 5 minutes; re-publications count as activity and trigger it). Acknowledgements that arrived after the reaping, because the projections lagged, went to stopped actors. First taken for slow deletes.
- **Fix:** owners publish on behalf of their command bus (`publicationAcks`), which lives as long as the system and deletes the rows itself; a live owner gets the acknowledgement only to continue re-publishing its outbox. Deletes go as two arrays (`unnest`), up to 5 000 rows per statement. Test: `OutboxPollerSpec` "Deleting delivered rows happens when the delivery is acknowledged after the aggregate's owner has stopped" (red first).

#### F92 — Logs flooded under overload
- **Medium**, confirmed (measured) · **Fixed in M2** · `EventsBusActor.checkDeadlines`, `OutboxPollerActor`, `ProjectionActor`
- **Symptom:** the soak wrote about 960 000 "delivery waits" lines (one per delivery) and one "undelivered" line per aggregate; a failing listener warned at every retry.
- **Fix:** at most 10 lines per tick or poll, with a count and the periodic summary for the rest; a failing batch warns once.

#### F93 — No back-pressure on the live path
- **Medium**, confirmed (measured) · **Throughput part fixed in M4** (17/10/2026: deliveries that arrive together are applied up to 100 aggregates per transaction, their versions read in one query per listener kind (D61 on the live path); measured on one machine, 200 000 events of 20 000 counters with failing listeners in the first 30 s: every event projected after 62 s instead of 96 s, about 3 200 instead of 2 100 events/s; 3 200 commands on 16 counters: 2 600 to 2 900 instead of 1 500 events/s, 3.4 instead of 7.0 statements per command; `ProjectionGroupsSpec`). **Flow control fixed in M5** (17/10/2026: a window per subscriber of 10 000 events and 64 MB of their stored sizes not yet answered; what does not fit is held, its aggregate behind for that subscriber, and pulled from the owners once half the window is free; held deliveries complete when the subscriber answers the pulled part, so the outbox is the queue and its rows go as soon as the slow subscriber has them; `FlowControlSpec`, `EventsBusActorSpec` and `DeliveryEndToEndSpec`, caught under mutations that admit everything or never pull) · `EventsBusActor`, `FlowControl`, `ProjectionActor`
- **Symptom:** when commands come faster than a projection applies them, its mailbox and the bus's deliveries grow with the lag. Measured: 1 000 000 events at about 12 000 commands/s against projections at about 1 700 events/s ended 10 minutes behind; it fit in a 2 GB heap (peak not measured).
- **Fix idea:** a window per subscription in the bus; past it, events are left in the outbox and fetched from the store when the subscriber catches up (`RepublishRange`), so memory holds keys, not payloads.

#### F95 — A subscribable projection's timer outlived it, and its cache of recent updates was trimmed only by new updates
- **Low**, likely (found at the M2 simplification pass) · **Fixed in M2** · `SubscribableProjectionActor`
- **Symptom:** the one-minute `ClearIdleSubscriptions` timer was never cancelled, so a stopped projection (a rebuild's, or one stopped and created again within a system) left a timer sending to dead letters for the life of the actor system. The cache of recent updates (`updatesCacheTTL`) was trimmed only when the next update arrived, so after a burst it kept the burst until then.
- **Fix:** the timer is cancelled when the projection stops, and each tick also trims the cache to its TTL. No test: neither is visible in the actor's answers.

#### F113 — A failure at a squash start could not be skipped
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: `SkipFailedVersion` moves the subscription from the version it has to the failed one; `ProjectionDeliverySpec`, red under the old skip) · `ProjectionActor.skipFailedVersions`
- **Symptom:** a skip moved the subscription from the version before the failed one, but a failure at a squash event the subscription started at (F98) is recorded there while the subscription is at zero, so the compare-and-set failed and the failure stayed, keeping a build from switching and the aggregate's squash and permanent delete waiting.
- **Found:** M4 close (review 6), audit.

#### F115 — A cache fill could miss a commit made while it read
- **Medium**, confirmed (a narrow race) · **Fixed in M4** (17/10/2026: a read that fills the cache is not cached when a commit changed the aggregate's versions while it ran; `PostgresSubscriptionsStateSpec`, red when changes are not tracked) · `PostgresSubscriptionsState`
- **Symptom:** the versions of an aggregate not cached were read outside the monitor and cached after; a commit in between (another projection's new version, a targeted rebuild's reset) changed only cached aggregates, so the read cached an old version: behind the database (a spurious gap, a group applied again one batch at a time), or, after a reset, ahead of it, so the rebuild skipped the aggregate.
- **Found:** M4 close (review 6), audit.

#### F121 — Messages between nodes can exceed Artery's frame and are dropped
- **High**, confirmed (measured sizes; the drop seen in `ClusterMessageSizeSpec`) · **Fixed in M5** (17/10/2026) · `EventsBusActor`, `AggregateCommandBusActor`, `ClusterSerializer`, `MessageSize`
- **Symptom:** in a cluster, Pekko drops a message larger than `pekko.remote.artery.advanced.maximum-frame-size` (256 KiB by default) and logs an error; its sender is not told. A re-publication part of 1 000 events with 200-character notes is 470 KiB serialized (docs/perf/load-M5.md), so a projection on another node never receives it: its gap request asks for the same part again, and the aggregate stays behind in that projection for good. A command with many events does the same on its first publication. A `GetEventsForAggregate` answer from an owner on another node (23 573 KiB for 50 000 events) never arrives, and the asker times out. The application's states, commands and answers above the frame are dropped the same way.
- **Root cause:** parts are bounded by events, not bytes; the frame did not matter before M5, when every message stayed in one JVM.
- **Fix:** `MessageSize` gives the limit, the frame (or the large frame when `large-message-destinations` is set) less 8 KiB, and measures values with the serializer Pekko uses for them. The bus sends a subscriber on another node several events in parts of at most half the limit, each event measured once per publication. `GetEventsForAggregate` is answered on the asking node from the store, like the historical reads; the owner no longer answers it. A command of a sharded type over the limit is refused with `FailureResponse` wherever its owner runs. The cluster serializer writes any message over the limit as a marker that the receiving node reads as `Status.Failure(MessageTooLargeException)`, so an asker fails at once, and logs its class and size (at most once per class every 10 s). Hosts that need larger single messages raise the frame (docs/SCALING.md §6).
- **Test:** `ClusterMessageSizeSpec` (two nodes: a subscriber and a projection on the other node receive 1 000 events of about 450 bytes; every event of an aggregate whose owner runs on the other node; a state over the limit fails at once for an asker on the other node; a command over the limit refused through either node; all five red before the fix), `ClusterSerializerSpec` (the marker and the failure's round trip).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F122 — The outbox poll groups the whole outbox and keeps every waiting aggregate in memory
- **Medium**, likely (estimated, not measured) · **Fixed in M5** (17/10/2026, in part) · `PostgresEventStoreState.readWaitingInOutbox`, `OutboxPollerActor`, `RedeliveryPolicy`
- **Symptom:** every 30 s the poller groups all of `events_to_publish` by aggregate and keeps one entry per waiting aggregate, and re-publishes at most 1 000 aggregates per poll. After a long outage of a projection (millions of rows), each poll scans every row, the poller's memory grows with the aggregates, and draining 1 000 000 aggregates takes about 8.3 h (estimated: 1 000 000 / 1 000 × 30 s).
- **Fix:** the poller reads at most `maxTracked` (100 000) waiting aggregates, those waiting longest first, so its memory is bounded; the grouping itself still reads every row (cheap while the outbox is near empty; documented in SCALING §4). Test: `PostgresEventStoreStateSpec` (the oldest first, at most the limit; red when the limit is dropped).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F123 — Given-up aggregates wait for the poller's next start, even when their subscriber is back
- **Medium**, confirmed (by reading `RedeliveryPolicy`) · **Fixed in M5** (17/10/2026) · `RedeliveryPolicy`
- **Symptom:** after its attempts at 3 and 6 minutes, the poller gives up on an aggregate at 10 minutes and does not re-publish it again while it runs (D20b: the rows stay for the next start). A subscriber that stopped answering for longer (an overload, F93; a partition; a long pause) and recovers without a restart keeps missing the events of aggregates that get no new events; those that do are filled by gap requests. In a cluster the poller is a singleton, which starts again only when the oldest node leaves.
- **Fix:** after the report, a given-up aggregate is re-published again every `retryGivenUpEvery` (30 minutes), within the per-poll limit. Test: `RedeliveryPolicySpec` (re-published at 41 and 71 minutes after giving up at 11; red before).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F128 — A re-publication could send a subscription versions with a hole in one message
- **High**, confirmed (by reading `SequenceGate`; reproduced in `EventsBusActorSpec`) · **Fixed in M5** (17/10/2026) · `EventsBusActor.publish`
- **Symptom:** a full re-publication (the outbox's) sends each subscription only the versions whose deliveries wait for it, or have none. A subscription added after an event that still waits for the others (a new build's, D57; a singleton's new instance, M5) was sent, for versions 1 to 3 where version 2 waited for another subscriber only, one message of versions 1 and 3. The sequence gate takes a message as a range: it applied 1 and 3 and recorded 3, so version 2 was never applied in that read model, and nothing reported it.
- **Fix:** each subscription's events are split into runs of consecutive versions before the parts (F121); a run after a hole is a gap the subscriber asks for (D20c). Test: `EventsBusActorSpec` "sends each subscription consecutive versions only" (red before).
- **Found:** M5 close, audit of delivery.

#### F129 — A singleton's proxy was asked once for its new subscribers
- **Medium**, likely (the ask can be lost; not seen) · **Fixed in M5** (17/10/2026) · `EventsBusActor`
- **Symptom:** when a projection's instance stops (its singleton moves), the bus asks the registered proxy again for its subscriptions, once. The proxy drops what its buffer cannot hold while the singleton moves, and the ask can reach the supervisor that is stopping. Lost, the bus never subscribed the new instance: that node's events of its types stayed in the outbox, and its gap requests to owners on that node were re-published to no subscriber, until the node restarted.
- **Fix:** a registered subscriber asked again is asked every tick until it answers, as at the start. Test: `EventsBusActorSpec` "the proxy is asked again every tick until it answers" (red before).
- **Found:** M5 close, audit of the cluster.

#### F130 — Flow control let live traffic starve what it held, and its accounting drifted
- **Medium**, confirmed (by reading; each part reproduced in `FlowControlSpec` or `EventsBusActorSpec`) · **Fixed in M5** (17/10/2026) · `FlowControl`, `EventsBusActor`
- **Symptom:** (1) A subscriber with aggregates behind kept taking live publications of others up to its whole window, while pulls start only at half of it: under steady load the held aggregates waited until the deadline and then for the poller, minutes later. (2) Live publications could take the room that pulls had asked for, so the subscriber held more than its window once the pulled parts came. (3) An aggregate held again was dropped a deadline after its first hold, while what was held later still waited, so its next live event made the subscriber ask for a gap. (4) Events held again (re-published from the outbox while held) counted their bytes again, and a pull counted every byte held as coming in its part, so pulls asked for more room than they brought and fewer ran at once. (5) A held delivery that its pull sent was logged as stalled at once, its wait counted from when it was held.
- **Fix:** while aggregates are behind, a live publication takes at most half the window, so pulls always find the other half; what pulls asked for counts as in flight; an aggregate stays behind a deadline after its latest hold; only versions new to a held range add their share of the bytes, and a part and what it leaves each count their share; a delivery stalls from when it was last sent. Tests: `FlowControlSpec` (five scenarios), `EventsBusActorSpec` "a held delivery stalls from when its pull sends it"; each red under a mutation that restores the old rule.
- **Found:** M5 close, audit of delivery.

#### F133 — Push subscriptions vanished without notice when a projection's singleton moved
- **Medium**, confirmed · **Fixed in M5** (17/10/2026) · `SubscribableProjectionActor.renewSubscription`
- **Symptom:** push subscriptions live in the instance's memory. When the projection's singleton moves (a rolling upgrade, a node that leaves), the new instance does not know them, and a `RenewSubscription` for an id it does not know was ignored: listeners kept renewing and never received another update, and were never told.
- **Fix:** a renewal of an unknown subscription (expired, or held by an instance that stopped) is answered with `ProjectionSubscriptionsCancelled(List(id))`, so the listener subscribes again. Test: `SubscribableSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F141 — Retries of given-up aggregates could take a poll's whole budget
- **Low**, confirmed (by reading; reproduced in `RedeliveryPolicySpec`) · **Fixed in M5** (17/10/2026) · `RedeliveryPolicy.decide`
- **Symptom:** a poll re-publishes at most 1 000 aggregates, the longest waiting first, and since F123 given-up aggregates are retried every 30 minutes. Behind a subscriber stopped for long (its rows never go), 60 000 given-up aggregates or more took every poll's budget, so newer rows' attempts at 3 and 6 minutes waited without end and were never reported as undelivered. Each retry also re-publishes to every subscriber, which acknowledge duplicates, and restores an owner.
- **Fix:** while other aggregates are due, retries take at most half the budget; each kind takes what the other leaves. Test: `RedeliveryPolicySpec` (red before).
- **Found:** M5 close, audit of delivery.

#### F161 — Subscribable projections push before the listener's transaction commits, and one failing filter stops an update
- **Medium**, confirmed (red first) · **Fixed in M7** (17/10/2026; `SubscribablePushSpec`, four scenarios, serial and parallel) · `SubscribableProjectionActor.sendUpdate`
- **Symptom:** `sendUpdate` sends at once, so a batch that rolls back has pushed a change never stored (and pushes it again when retried); one `try` wraps the loop over subscriptions, so a filter that throws keeps the update from the rest; a reused subscription id gives the newest listener every update twice and the older none.
- **Fix:** `ProjectionActor.afterCommit` keeps what a listener asks to run after its transaction, per thread for the attempt at work (the listener runs on the transaction's thread, the actor's or a pool's), runs it after the commit and drops it on a rollback; `sendUpdate` pushes through it. Each filter runs in a `Try` of its own. An id used again replaces its subscription, and a listener it took the id from gets `ProjectionSubscriptionsCancelled`. A push is lost if the node stops between the commit and the push: pushes are at most once, the store is the truth (M7 design).
- **Found:** writing the book (M6, 17/10/2026).

#### F162 — The event bus ignores a late `RegisterSubscribers`, and waits for a subscriber that answered with a failure
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `EventsBusActorSpec` "F162", three scenarios) · `EventsBusActor`
- **Symptom:** a `RegisterSubscribers` sent after start-up is unhandled, without a log line; a `Status.Failure` from a subscriber during start-up is stashed, so the bus keeps waiting for its subscriptions. Also: a second `RegisterSubscribers` before the start replaced the first list's waiting subscribers, so the bus could start before they answered.
- **Fix:** a second list adds to the first; one after the start registers its subscribers, asked every tick until they answer, with a warning that they missed what was published before; a failure answer is logged with its cause and the subscriber asked again at the next tick.
- **Found:** writing the book (M6, 17/10/2026).

#### F163 — Delayed queries with a `Future` can miss the update they wait for
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `ProjectionDeliverySpec` "F163") · `ProjectionActor` (`delayIfNotAvailableAsync`, `...AsyncCustom`)
- **Symptom:** such a query is parked only once its `Future` answered; an update applied meanwhile asks for no replay, and at the deadline the answer is `None` without a last search. The custom variant answers `None` at its deadline, where it answers a `T` on success.
- **Fix:** the projection counts applied updates; an async search that answers "not yet" after an update was applied runs again instead of being parked. At its deadline every delayed query gets a last search and answers its result, the custom variant its `T`. A build's retirement still answers its parked queries `None`.
- **Found:** writing the book (M6, 17/10/2026).

#### F164 — A second listener of one kind for one type silently replaces the first
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026: refused at start; `ProjectionDeliverySpec` "F164") · `ProjectionActor` (the listener maps)
- **Fix:** the listeners are checked with the rule against `Any` and `Nothing`: two of one kind for one type fail the projection's answer to the bus (or to its supervisor), which names them; the bus keeps waiting and logs the cause (F162).
- **Found:** writing the book (M6, 17/10/2026).

#### F169 — `OutboxStatus.waiting` can count fewer aggregates than wait
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `OutboxPollerActorSpec` "F169") · `OutboxPollerActor`, `RedeliveryPolicy`
- **Symptom:** it is the poller's tracked aggregates; new ones postponed past `maxRepublishPerPoll` are not tracked yet.
- **Fix:** the status counts the aggregates the last poll read, as the `reactivecqrs.outbox.waiting` gauge does. Not tracking a postponed new aggregate loses nothing: its attempts are timed from its rows' age.
- **Found:** writing the book (M6, 17/10/2026).

#### F166 — A permanent delete held by a failed subscription runs only at the next re-publication
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `ProjectionDeliverySpec` "F166") · `AggregateCommandBusActor.deletePermanently`, `ProjectionActor`
- **Symptom:** once the failure is retried or skipped, the delete happens when the poller next re-publishes its event: every 30 minutes after the 10-minute report.
- **Fix:** a projection that clears a failure, by a retry that reaches the failed version or by a skip, sends the bus `Republish` for the aggregate, as the poller does: its owner re-publishes what waits in its outbox, and the acknowledged delete is carried out. A failure record removed otherwise (`clearSubscriptionsInfo`) still waits for the poller.
- **Found:** writing the book (M6, 17/10/2026).

#### F182 — A node that is downed logs an error from its event bus as it ends
- **Low** (operations), confirmed (E2's fault tests in Docker, 18/10/2026) · **Fixed in M7** (18/10/2026; `DownedNodeSpec`, red first; E2's fault tests again, no error) · `EventsBusActor` (`subscriberStopped`)
- **Symptom:** the oldest of E2's three nodes, cut off from the other two while it still reached the database, was downed by the split brain resolver as it should be; its bus logged "Subscriber .../cartsList-proxy stopped; its events stay in the outbox until the next start" at ERROR, 3 ms before its coordinated shutdown started. A host that alerts on ERROR is alerted at every node the resolver downs, though nothing is lost: the other nodes' buses and the moved singletons take over (F110).
- **Root cause:** F176 counts a stop as orderly while a coordinated shutdown runs. A node that is downed first ends its cluster (`Cluster.shutdown`: the cluster's event publisher tells its subscribers the node is removed, and the singletons' proxies stop on that), and starts its coordinated shutdown a moment later; a proxy that stops in that moment found no shutdown reason yet.
- **Fix:** a subscriber that stops when the node's cluster has ended (`Cluster(system).isTerminated`) stopped as its node shuts down, too: logged at INFO.
- **Found:** E2's fault tests in Docker (M7, 18/10/2026).

#### F181 — The stored data check stops at a table the database does not have
- **Low** (a tool of the upgrade), confirmed (chapter 6's spec, 18/10/2026) · **Fixed in M7** (18/10/2026; `StoredDataCheckSpec` "checks a database that lacks some tables", red first) · `StoredDataCheck.run`
- **Symptom:** run on a database without the `sagas` table, the check failed at once with "relation "sagas" does not exist" and reported nothing. An application without sagas never makes that table (the book's chapter 4 makes three tables for a cart), and the same goes for `commands_responses` without its `initSchema()` and for a listed projection table that was never made. The archive (`events_archive`), which a database from before 1.0 lacks, was already skipped, but silently. Neula has sagas, so its rehearsal would not have met it.
- **Fix:** every table the check reads but `events` is read only when it exists, and the report names the tables it did not find (`Report.missingTables`, and a line of `text`), so a missing table is seen, not taken for an empty one.
- **Found:** writing chapter 6's spec (M7, 18/10/2026).

#### F180 — The book's Kubernetes deployment split its cluster in a rolling restart
- **High** (for a host that copies it), confirmed (E2 on Docker Desktop's Kubernetes, 18/10/2026) · **Fixed in M7** (18/10/2026; on Docker Desktop's Kubernetes: one cluster formed, kept through a rolling restart, a frozen node downed by the resolver with the lease and rejoined once thawed; with Docker Compose the first node restarted and rejoined; E2's `NodeSpec`: a node alone forms its cluster through Bootstrap) · E2 (`examples/deploy/e2`, `ClusterSetup.nodeConfig`), the book's chapter 17
- **Symptom:** E2's three pods of a StatefulSet, with `shop-0` and `shop-1` as static seed nodes by their DNS names, formed one cluster at start. In a rolling restart (newest pod first), the new `shop-0` did not hear from the others within `seed-node-timeout` (5 s) and, being the first seed node, joined itself: a second cluster beside the one of `shop-1` and `shop-2`, each running the singletons (the projections' supervisors, the outbox poller, the shard coordinator). Only the database's version check then stands between the two (D46).
- **Root cause:** the first seed node forms a new cluster when no other seed answers in time. A restarted pod has a new IP under the same DNS name, and the other nodes' JVMs still had the old one cached (a successful lookup is kept 30 s by default), so their answers went to the old pod. The book's chapter 17 presented static seed nodes as the way, Cluster Bootstrap as an alternative.
- **Fix:** E2 joins with Pekko Management's Cluster Bootstrap (D74): no seed nodes; a node asks the contact points it discovers (on Kubernetes the pods labelled `app=shop`, through the Kubernetes API; with Docker Compose a fixed list) and joins the cluster they report; a new cluster forms only when the required contact points, all of them, report none. On Kubernetes each node's address is its pod IP, so no name outlives its pod; the pods start in parallel, as the first formation needs all three. The book says why seed nodes are not for a Kubernetes rolling restart.
- **Found:** running E2 on Docker Desktop's Kubernetes (M7, 18/10/2026).

#### F178 — The serializer of messages between nodes depends on the order of the classpath
- **High** (a cluster deployment), confirmed (E2 in Docker, 18/10/2026) · **Fixed in M7** (18/10/2026; `SerializerBindingSpec`, red first; E2's `NodeSpec`; E2's three nodes in Docker Compose afterwards, the oldest stopped and a node restarted) · `core/reference.conf` (`serialization-bindings`), `Readiness`
- **Symptom:** E2's three nodes in Docker Compose, each run as `java -cp lib/* ...`, could not send one message to another: "Failed to serialize message ... GetSubscriptionRequests" every five seconds, and a command for a cart on another node answered "cannot be sent between nodes: ... Java serialization ... was disabled". Every library spec passed: under sbt the library's classes come first on the classpath.
- **Root cause:** pekko-actor's `reference.conf` binds `java.io.Serializable` to Java serialization (off since D45), and core's `reference.conf` binds the same key to the library's serializer (D68), so that case classes and Scala collections reach it. Typesafe Config merges the `reference.conf` files in the order of the classpath, the first one winning a key. `lib/*` gives no order (the JVM leaves it unspecified), and packagings differ: a start script lists jars in its own order, an assembly concatenates the files in its own. Wherever pekko-actor's file comes first, the library's binding is lost, with no error at start.
- **Fix:** the host's configuration binds it (an application's settings win over every `reference.conf`): `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }`, in the book's configuration of chapter 17, in E2's and in the test kit's. And the library checks at start (`Readiness`): when a node can talk to others and a library case class does not go to the library's serializer, it logs an error naming the setting, and the node stays not ready, so a health check keeps traffic away from a node that cannot talk to the others (a single node would fail the same way as soon as a second one joins, in a rolling upgrade for one).
- **Found:** running E2 with Docker (M7, 18/10/2026).

#### F176 — An orderly stop of a cluster node logs errors from the event bus
- **Low** (operations), confirmed (seen in E10's spec as a cluster of one) · **Fixed in M7** (18/10/2026; `EventsBusActorSpec` "F176", two scenarios, and `ClusterDeliverySpec`'s node that leaves, all red first: four errors per leave) · `EventsBusActor` (`stoppedOrUnregistered`)
- **Symptom:** the bus logs "Subscriber X stopped; its events stay in the outbox until the next start" at ERROR whenever a subscriber it watches stops. A cluster node that leaves (each node in a rolling upgrade, D65) stops its singletons while its bus still runs, in the cluster-exiting phase of its shutdown: E10 as a cluster of one logged it twice per stop, for the managed projection's instance and for the supervisor's proxy. When the singleton's node leaves, the buses of the other nodes log it for its instance too. A host that alerts on ERROR is alerted at every deploy, and for an instance behind a singleton the message is wrong: its replacement takes the outbox rows once the moved singleton answers (M5, F110), with no restart.
- **Fix:** ERROR only when nothing replaces the subscriber before the next start. A subscriber that stops while its own node shuts down (a coordinated shutdown runs) is logged at INFO, and so is one that a live registered subscriber answered with (an instance behind a singleton's proxy, or behind a managed projection's supervisor, which logs its own error when its instance stops by itself, F110), naming the subscriber asked for its replacement.
- **Found:** writing E10 (M7, 18/10/2026).

### Commands and aggregates

#### F261 — Deleting an aggregate's idempotent responses removes their deduplication too, so a retried keyed creation creates it again
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `PostgresEventStoreStateSpec` "F167, F261", red under a mutation) · `PostgresEventStoreState.deleteAggregatePermanently`
- **Fix:** a permanent delete clears the info of the aggregate's stored responses (F167's purpose: the values go) and keeps their rows, keys and the response's id and version, until the retention period removes them: a retried keyed command gets the old answer without its info.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F196 — A snapshot of a state class that gained a field is restored with zero, false or null in that field, silently
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2 of the risk assessment; `SnapshotSpec` "F196" and `StateShapeSpec`, red under a mutation) · `StateShape`, `AggregateRestorer`, the `snapshots` table
- **Fix:** a snapshot is written with its state class's shape (a digest of the names and types of its fields and of the classes it holds, type arguments included, by Java reflection; the class's name is part of it), in a new `snapshots.shape` column; a snapshot of another shape, or without one (written by 1.0.0), is not used, and the aggregate is replayed once. `schemaVersion` is left for a field whose meaning changes.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F197 — An aggregate id of another type is restored by the wrong context, which then writes into it
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `SnapshotSpec` "F197", red under a mutation; `StoredDataCheckSpec`) · `AggregateRestorer`, `EventStoreState.generationAndType`, `StoredDataCheck`
- **Fix:** a restore reads the aggregate's type with its history generation (one row) and refuses one of another type than the command bus's, naming both: its queries, commands and squashes are refused, and nothing is written. Snapshots of another class are not used (F196's shape holds the class). `reactivecqrs.aggregate-type-check = "log"` only logs it, for a store whose type names are not the contexts' yet; `StoredDataCheck.run(..., contexts)` reports such types. The procedures do not check the type: a node of 1.0.0 writes as before, and the owner's restore is where an id reaches the wrong type.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F194 — After its base is erased, an attached duplicate is reported unchanged and never rebuilt; a lost history notice is never repaired
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `CompleteRewriteSpec` "F194", red under a mutation) · `HistoryRewriter`, `AggregateOwner.rewriteHistory`
- **Fix:** every visited aggregate is rebuilt (F192), the attached copies that show their base's values among them; an owner tells its duplicates' owners after every rewrite, changed or not, so a run again repairs a lost notice, and reads its state again when the stored version or history generation is not the one it holds.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F195 — A history change that keeps the version is not seen at persist: a command running across an erasure writes the erased values back
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `CompleteRewriteSpec` "F195" and `PostgresEventStoreStateSpec` "F195", red under a mutation) · `AggregateOwner`, `add_events`, `overwriteEvents`
- **Fix:** a command carries the history generation its state was read at; the owner runs it again (a concurrent command) or answers a conflict when the generation moved on while its handler ran. At the commit, `add_events` takes the expected generation (a new overload; the 1.0.0 form calls it with -1 for old nodes) and raises the conflict when the aggregate's generation is another, and a rewrite without new events locks and compares the aggregate's row (`HistoryChangedException`); so another node's rewrite between a catch-up and a commit is seen too.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F245 — An unreadable stored event makes its aggregate impossible to erase
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `CompleteRewriteSpec` "F245" and `PostgresEventStoreStateSpec` "F245", red under a mutation) · `HistoryChange`, `PostgresEventStoreState.rewriteEverywhere`, `AggregateOwner`
- **Fix:** `HistoryChange.inUnreadableEvent(json)` gives the change's text form; a row that cannot be read is written again as it returns (counted in `HistoryRewritten.unreadable`, `(unreadable)` in the audit row's classes), and without one the rewrite is refused, writing nothing, with a reason that names the row. The owner of an aggregate that cannot be restored runs rewrites too. Not a tombstone: the aggregate stays unreadable until its class or row is repaired, as replaying it without the event could give a wrong state.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F200 — The organization purge does not guard the platform space (0) or the legacy space (-1)
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `PurgeSpec` "F200", red first) · `SpacePurge`
- **Fix:** a purge of space 0 or of a negative space is refused before anything is read: a purge removes one organization only. The book's chapter 16 says so.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F7 — Event timestamps differ between live delivery and the store
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`, `PostgresEventStoreState`
- **Symptom:** state and projections that use event timestamps (Neula: process flows, reports, functions) differ after restore, re-send or replay; the difference is the persist queue wait.
- **Root cause:** the live path uses the executor's `Instant` (`CommandExecutorActor.scala:198`); SQL stores a later `Instant.now` (`PostgresEventStoreState.scala:72, 85, 99, 112, 172`).
- **Test:** `EventStoreStateSpec` "Events keep the timestamp they were persisted with" (single and batched writes; failed on PostgreSQL before the fix). An end-to-end check through the actors (Neula R6) is still to be written with the projection work in M2.
- **Fix:** D10. The executor takes one timestamp per command, truncated to microseconds (PostgreSQL's precision); the store writes that timestamp instead of its own `Instant.now`. Follow-up: F65.
- **Invariants:** I6.

#### F8 — History rewrite is half-applied
- **Medium**, confirmed · **Contract part fixed in M3** (16/10/2026), complete rewrite in **M6** (D76) · `PostgresEventStoreState.overwriteEvents`, `AggregateOwner`
- **Symptom:** a rewrite to a different class corrupts history; the live root keeps old values; projections are not told; duplicates and snapshots keep old values (GDPR).
- **Root cause:** `overwriteEvents` updates only the JSON (`PostgresEventStoreState.scala:180-186`); no rebuild after rewrite (`AggregateRepositoryActor.scala:128-134`).
- **Fix (M3, D17):** the store refuses a rewritten event of another class than the stored one (`RewriteChangesClassException`; the command gets `EventHandlingError` and nothing is stored); the owner reads its state again after a rewrite; the snapshots of the aggregate and of its duplicates are deleted. Still open for M6 (D76): projections are told only through a compensating event, and the live owners of duplicates keep their state until they are reloaded. Tests: `HistoryRewriteSpec`, `EventStoreStateSpec` (both stores).
- **Done in M6 (17/10/2026), D76:** the complete rewrite (`HistoryChange`, `RewriteEverywhere`, `HistoryRewriter`) reaches squash events, the archive, attached and detached copies, the host's rows and the projections given, and records an audit row; a plain rewrite without a new event is stored (F146). Tests: `CompleteRewriteSpec`, `EventStoreStateSpec` "D76" (both stores), `HistoryRewriteSpec` "F146", `ExportImportSpec` (lineage).

#### F9 — Idempotent command responses: broken index, ambiguous lookup, race
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandResponseState`, `Idempotency`
- **Status:** index part fixed first (D24): `initSchema()` failed on a fresh database with `column "aggregate_id" does not exist`, which blocked `ReactiveTestDomainSpec` and `EventsReplaySpec`; it now drops the old index if present and creates a unique index on `key`. Collision handling, the race and retention fixed next (D12, R7).
- **Symptom:** `initSchema()` fails on a fresh database, or the lookup scans the table; duplicate keys make `responseByKey` throw; two concurrent identical commands can both execute.
- **Root cause:** unique index on `(aggregate_id, key)` but no `aggregate_id` column (`CommandResponseState.scala:36-53`); `.single()` on a non-unique key (`:60-65`); check-then-act (`CommandHandlerActor.scala:95-106`).
- **Test:** `IdempotentCommandsSpec` (sequential, concurrent first command, concurrent `Command`, concurrent `ConcurrentCommand`, after a restart, caller-chosen id) and `CommandResponseStateSpec` (both adapters). Before the fix every concurrent case got no reply at all: the key collision was thrown out of the persist transaction and restarted the repository actor (AR3).
- **Fix:** D12, D24, through the new `Idempotency` module (AR2). The response insert is `ON CONFLICT (key) DO NOTHING`; a taken key throws `IdempotencyKeyTakenException`, which rolls the persist back, and the executor answers with the stored response; a version conflict of an idempotent command is answered the same way when a copy already stored its response. Stored responses older than the retention period (default 30 days, a `PostgresCommandResponseState` parameter) are removed hourly by each command bus (index on `handling_timestamp`). R7's caller-chosen id: `FirstCommand.chosenAggregateId`, with ids from the new `ReserveAggregateId` bus message.
- **Operations:** the insert needs the unique index, so `initSchema()` must run (H5): without it every idempotent command fails loudly.

#### F10 — A failed restore leaves a dead actor reference cached
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor`, `AggregateRepositoryActor`
- **Symptom:** commands for that aggregate time out (60 s) until reaping.
- **Root cause:** restore in the constructor (`AggregateRepositoryActor.scala:101`) → `ActorInitializationException` → default Stop; refs stay in maps (`AggregateCommandBusActor.scala:84-85, 198-207`); no `watch`.
- **Test:** `FailedRestoreSpec` (before the fix, both the command and the query got no reply and the 10 s ask timed out).
- **Fix:** D11. The repository actor restores on its first message. After a failure it answers every query with `Failure(AggregateRestoreFailedException)` (the command executor turns that into `CommandHandlingError`) and refuses persists with `EventHandlingError`; after `failedRestoreRetryAfter` (default 5 s, an `AggregateCommandBusActor.apply` parameter) it stops. The command bus watches its repository actors and on `Terminated` forgets the actor and its command handler, so the next request tries the restore again. The "remember the failure briefly" part of D11 is the failed actor itself staying alive for that window. A request routed in the moment between the stop and the `Terminated` message still waits for the command timeout.

#### F12 — Aggregate ids read as `Int`
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresEventStoreState`
- **Fix:** read as `Long`. Test: `EventStoreStateSpec` "… ids beyond 32 bits" (with `rs.int` the ids came back wrapped to other, sometimes negative, values).
- **Root cause:** `aggregateHandler(AggregateId(rs.int(1)))` (`PostgresEventStoreState.scala:371`) overflows past 2^31.

#### F13 — Command executor lingers after a `CommandFailure`
- **Low**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`
- **Test:** `CommandOutcomesSpec` (the executor was still alive after the failure reply).
- **Fix:** stop after replying.

#### F19 — Stale aggregate after an external write; concurrent commands spin until timeout
- **Medium**, confirmed (High in a cluster) · **Fixed in M1** (16/10/2026) · `AggregateRepositoryActor`, `CommandExecutorActor`
- **Fix:** the repository actor reloads from the store when asked for a version newer than its own, and after an SQL concurrent-modification error. Test: `ConcurrentModificationSpec` (an external write followed by a concurrent command, or by a command expecting the new version, now succeeds; both failed before the fix).
- **Symptom:** after another node, a rewrite or an SQL repair writes the aggregate, every `ConcurrentCommand` conflicts until the 60 s executor timeout (Neula J6).
- **Root cause:** the SQL conflict is answered with the in-memory version, never reloaded (`AggregateRepositoryActor.scala:300-307`); the executor re-asks the same stale actor without limit (`CommandExecutorActor.scala:78-85`).
- **Test:** external write, then a command on the same aggregate succeeds (Neula R4).
- **Fix:** reload on conflict; D37.

#### F20 — Async handler past 60 s gets no reply
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`, `AggregateContext`
- **Fix:** `AggregateContext.commandTimeout` (default 60 s); when it passes, the caller gets `CommandHandlingError` and the executor stops, in every state. The command may still complete afterwards (the warning log says so); idempotency keys (R7) protect a retry. Test: `CommandOutcomesSpec` (no reply before the fix).
- **Fix:** D56 (R5): configurable timeout and an explicit error reply.

#### F24 — Concurrent-command retry storm
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor`
- **Fix:** D37: re-runs after a conflict wait with jittered exponential back-off (10–500 ms) and stop after 10 attempts with `AggregateConcurrentModificationError`. Test: `ConcurrentModificationSpec` "gives up …" (timed out after 10 s before the fix).
- **Root cause:** unlimited retries without back-off (`CommandExecutorActor.scala:78-85`); handler CPU about N²/2 under a burst of N (estimate).
- **Fix:** D37; serial mode (D31) for contexts that opt in.

#### F25 — Event handlers run twice per persisted event
- **Low**, confirmed · **Fixed in M3** (16/10/2026) · `AggregateOwner.persist`, `nextRoot`
- **Fix:** the version is checked before the handlers run on a copy of the state; after the commit that copy becomes the owner's state (D34). Test: `AggregateOwnerSpec` "run once per persisted event" (red under a mutation that applies the events again after the commit).

#### F44 — No snapshots: every restore replays the full history
- **Medium**, likely · **Fixed in M3** (16/10/2026) · `AggregateRestorer`, `AggregateOwner`, event store
- **Fix:** D30, opt-in per context (`SnapshotPolicy`). Tests: `SnapshotSpec` (a property test over random histories with undo and duplication, red under two mutations; R10 with 100 000 events; the fallbacks; invalidation), `EventStoreStateSpec` for both stores.

#### F46 — Command-bus child cache reap sorts the whole map
- **Low**, likely · **Fixed in M5 for sharded types** (17/10/2026; replaced by passivation, D43) · `LocalOwners.sweep`
- **As fixed:** the owners of a `Sharded` type are passivated by their shards (`pekko.cluster.sharding.passivation`); the bus keeps no children for them and runs no sweep. A `Local` type keeps the sweep, for ephemeral aggregates (M6) and gradual moves. Test: `ShardedOwnersSpec` (an idle owner is passivated by its shard and comes back with its state).

#### F70 — An unknown message restarts the command bus and drops every command in flight
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor.receive` (catch-all)
- **Symptom:** a message the bus does not understand (for example a command class that does not extend a command type) gets no reply, and every command of that aggregate type running at that moment never gets one either.
- **Root cause:** the catch-all threw `IllegalArgumentException`; the default supervision restarts the bus, and the default `preRestart` stops all its children, executors included (AR7).
- **Found:** architecture review 2 (§7).
- **Test:** `CommandOutcomesSpec` "A message the command bus does not understand" (no reply, and the in-flight command's executor was stopped, before the fix).
- **Fix:** the bus logs a warning and answers `Status.Failure(IllegalArgumentException)`.

#### F73 — A concurrent history rewrite re-run after a conflict rewrites nothing
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `CommandExecutorActor.onConflict`; present since 0.12
- **Symptom:** a `RewriteHistoryConcurrentCommand` (Neula's anonymization path) that meets a version conflict is re-run with no events: the retry stores its new event but rewrites none of the history, and the caller gets success.
- **Root cause:** the retry asked the repository for the current state only (`GetAggregateRootCurrentVersion`), while the first attempt asks for the state with the events to rewrite.
- **Found:** documentation audit at the M1 review (§7, review 3).
- **Test:** `ConcurrentModificationSpec` "re-run after a conflict, it still rewrites the history" (the creation event kept its old name before the fix).
- **Fix:** a retry asks for the same data as the first attempt.

#### F74 — Each events query for an earlier version leaves an actor behind
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateRepositoryActor.receiveReturnEvents`; present since 0.12
- **Symptom:** every `GetEventsForAggregateForVersion` starts a temporary repository actor that never stops and is never reaped: memory grows with every call.
- **Root cause:** the other single-read answers stop the actor; this one did not.
- **Found:** documentation audit and plan review at the M1 review.
- **Test:** `CommandOutcomesSpec` "Queries for an earlier version leave no temporary actor behind" (three actors were left before the fix).
- **Fix:** one `stopIfSingleRead()` after every single-read answer.

#### F75 — A failing id generator restarts the command bus or the saga actor
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `AggregateCommandBusActor` routing, `SagaActor`
- **Symptom:** when the uid generator fails, the command that needed an id gets no reply, and the bus restarts, which stops its children and every command in flight for that aggregate type. The saga actor restarts too and, because a restart ran `preStart` again, loaded its pending sagas a second time while the running ones went on: steps ran twice.
- **Root cause:** the blocking id wait (F23) throws inside `receive`; since F40 it does so at once instead of after 60 s. No supervision policy (AR7), and `SagaActor` did not override `postRestart`.
- **Found:** documentation audit and plan review at the M1 review.
- **Test:** `CommandOutcomesSpec` "A command bus whose id generator fails" (no reply, in-flight command lost, before the fix; also `ReserveAggregateId`), `SagaRecoverySpec` "A saga order when no saga id can be had" (no reply and a second load before the fix).
- **Fix:** the bus answers a command it cannot route with `CommandHandlingError` (and `ReserveAggregateId` with `Status.Failure`); the saga actor refuses the order with `SagaFailureResponse`; `SagaActor.postRestart` no longer reloads (not separately tested: no remaining path makes it throw). Other throws inside the bus's `receive` (the outbox scan query) remain for AR7.

#### F72 — Every reload starts another re-send timer chain
- **Low**, likely · **Fixed in M2** (16/10/2026, removed with the in-memory re-send, D8) · `AggregateRepositoryActor.assureRestoredState`, `resendEventsToPublish`
- **Symptom:** while a subscriber does not ack, an aggregate that was reloaded N times re-sends its waiting events N times every 180 s. Subscribers drop the duplicates by version (I4), so the cost is load and log noise.
- **Root cause:** each restore schedules `ResendPersistedMessages`, and each such message schedules the next one while events wait. Since M1 restores also happen on reloads (F19), after undo and duplication events, and after failed persists.
- **Found:** M1 code review (review 3, §7).

#### F67 — A command for an aggregate that does not exist gets no reply until the timeout
- **Medium**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `CommandExecutorActor.scala` `handleFollowingCommand`, `handleRewriteHistoryCommand`
- **Symptom:** a `Command` or `RewriteHistoryCommand` whose expected version does not match an aggregate at version 0 (it does not exist, or the id is wrong) is answered only by the command timeout (`CommandHandlingError` after 60 s) instead of `AggregateConcurrentModificationError`.
- **Root cause:** for version 0 the repository returns the aggregate whatever version was asked for; the executor then sends itself `AggregateConcurrentModificationError` while in the state that waits for the aggregate, which drops it as unhandled (AR4).
- **Found:** architecture review 1 (§7).
- **Test:** `CommandOutcomesSpec` "… a command for an aggregate that does not exist" (no reply within 10 s before the fix).
- **Fix:** the conflict is answered where it is detected, through the same conflict handling as persist conflicts.

#### F68 — A persist that fails part-way commits what it already wrote
- **High**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `AggregateRepositoryActor.persist`
- **Symptom:** a command is reported as failed (`EventHandlingError`) while part of it is stored: the first of several events when a later one fails its size or count check, or a history overwrite when the new event is rejected. The in-memory state does not include the stored part, and projections receive it later through the outbox.
- **Root cause:** `persistEvents` returns failures as a `Failure` value; the transaction block returned that value normally, so `DB.localTx` committed. SQL errors were rolled back by PostgreSQL itself (the transaction is aborted), but checks that throw in Scala after an earlier statement were not. Reachable only on the one-by-one path (a single event, or several with an undo or duplication event) and for rewrites; the batched path checks every event before writing.
- **Found:** architecture review 1, while making every persist outcome a reply (AR3).
- **Test:** `PersistTransactionSpec` (two events stored instead of none before the fix).
- **Fix:** every failure is thrown inside the transaction, which rolls back whole.

#### F69 — A reload after a version conflict applies the previous command's events twice
- **High**, confirmed · **Fixed in M1** (16/10/2026, with F9) · `AggregateRepositoryActor`; introduced on 16/10/2026 by the F19 fix (not in 0.12.47)
- **Symptom:** after two commands race on one aggregate, the actor is one command ahead of the store (version 3 with two items where the store has version 2 and one item); later commands are judged against that wrong state, and the wrong state is sent to state-receiving listeners.
- **Root cause:** after a successful persist the repository sent itself `EventsPersisted` and applied the events only when that message came round. A `PersistEvents` already queued ahead of it passed the in-memory version check, failed in SQL, and the F19 reload read the store, which already held the events; `EventsPersisted` then applied them again.
- **Found:** architecture review 1, while tracing the concurrent idempotent path.
- **Test:** `ConcurrentModificationSpec` "after a version conflict the actor's state still matches the store" (version 3 instead of 2 before the fix).
- **Fix:** the events are applied right after the commit, before the next message; `EventsPersisted` is gone.

#### F81 — An expired min-version query is dropped without a reply
- **Low**, confirmed · **Fixed in M3** (16/10/2026, with the owner) · `AggregateOwner` (min-version queries)
- **Symptom:** a `GetAggregateMinVersion` whose version is not reached within its time gets no answer; the caller only sees its own ask time out and cannot tell "not yet" from "lost".
- **Root cause:** expired queries are filtered out of `delayedQueries` without answering them, and only when another query or event arrives.
- **Found:** plan review at the M1 close.
- **Fix:** a timer for the nearest deadline answers expired queries with `Failure(AggregateInIncorrectVersionException)`; a stopping owner answers its waiting queries the same way. Test: `AggregateOwnerSpec` "is answered with an error when its time is up" (red with the timer removed).

#### F119 — A stopping owner could write a snapshot of a history that changed under it
- **Medium**, likely · **Fixed in M5** (17/10/2026) · `AggregateOwner.writeSnapshot`, `EventStoreState.writeSnapshot`
- **Symptom:** after a rewrite of a base, or a squash or detach that may change the state, the owner of a duplicate that was stopping wrote its state of before as its passivation snapshot, after the change had deleted the snapshots; later restores started from that state. So could another node's owner of a `Local` aggregate after an undo. With sharded owners (D43) the window grows: a stale stop that reaches a passivating owner is kept by its shard for the next owner.
- **Root cause:** a snapshot write was checked only against the aggregate's version, which these changes leave as it was.
- **Fix:** `aggregates.history_generation`, raised in the transaction of every change that deletes snapshots because their state may be wrong (an undo, a rewrite of the aggregate or of its base, a squash or a detach that may change the state); the restore reads it before the history, and a snapshot is written only while the generation is the one its state was read at. A state-preserving squash raises nothing.
- **Test:** `EventStoreStateSpec` "A snapshot of a state read before an undo or a rewrite is not written, also a duplicate's" (both stores; failed without the check in either store, and with a rewrite or an undo that does not raise the generation).
- **Found:** M5 design of sharded owners.

#### F120 — A first command racing another owner on a chosen id was answered with an error
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `AggregateOwner.movedOnWithoutThisOwner`
- **Symptom:** two owners of one aggregate at once (nodes of a `Local` type, or a hand-over, D43) creating it under a chosen id: the one that lost got `EventHandlingError`, also for an idempotent command whose copy had won.
- **Root cause:** both found the aggregate missing and inserted it; the loser's insert broke the primary key of `aggregates`, which the owner did not take for a concurrent modification.
- **Fix:** a unique-key violation on `aggregates` (its only unique key; matched by table, as a database made by an older version may name the key otherwise) counts as a concurrent modification: the owner reads the aggregate again, and the caller gets the stored response of an idempotent command, or the conflict.
- **Test:** `ChosenIdRaceSpec` (both owners pass the existence check before either inserts, as a table lock holds them until both wait; `EventHandlingError` before the fix).
- **Found:** plan review at the M1 close (refinement of M5 item 3).

#### F131 — Owners of a `Local` type on several nodes answered from states the others had moved past
- **High** for clusters of several nodes, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M5** (17/10/2026) · `AggregateOwner.caughtUp`, `Members`, `EventStoreState.storedVersion`
- **Symptom:** `Local` is the default placement, and every node that serves a type has an owner of an aggregate it touched. A running owner read the store again only for a command that expected a newer version (I1 assumed one writer). So after node B wrote version 6, node A's owner answered `GetAggregate` with version 5 for as long as it ran (with 200 owners or fewer, it is never stopped as idle); `GetAggregateMinVersion` for version 6 through A waited and failed; the poller's `RepublishOutbox` and a projection's gap request through A re-published nothing past version 5, and the gap became a recorded failure after its retries, which blocks the aggregate's squash and permanent delete.
- **Fix:** an owner of a `Local` type on a node that is not alone (`Members.alone`: not a cluster, or its only member) reads the aggregate's stored version and history generation (one key lookup) before it answers the state, a min-version query not reached (and again at the query's deadline), a re-publication and a squash, and restores the aggregate when they differ from its own. Commands were left to the store's version check (F19), which misses changes that keep the version: they catch up too since F145. A sharded owner is the only one and never asks; changing a type's placement or `numberOfShards` makes two owners per aggregate while old and new nodes run together, so it needs a full stop (H10).
- **Test:** `LocalOwnersSpec` (two nodes: a read, a re-publication, a min-version query waiting and one answered at once; all four red without the check), `MembersSpec` (whether the node is alone).
- **Found:** M5 close, audit of the cluster.

#### F138 — The write switch allowed every new event class before the node knew its cluster
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `Members` (was `AppVersions`)
- **Symptom:** the lowest app-version started as none, and an empty member list gave none too, which means "write every class". A new node that took a command for a `Local` type before it had joined (the readiness check usually keeps traffic away) could write a class that older members cannot read (D67).
- **Fix:** in a cluster the lowest version is Pekko's 0.0.0 until the node knows a member, and again if it knows none. Test: `MembersSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F134 — A permanent delete detached duplicates with a squash event that older nodes cannot read
- **Low**, confirmed (reproduced in `PermanentDeleteSpec`) · **Fixed in M5** (17/10/2026) · `AggregateCommandBusActor.deletePermanently`
- **Symptom:** a squash checks its detach events against the write switch (D67); a permanent delete, which detaches the duplicates with the same squash events (F76), did not. A squash event class added in a version was written into the duplicates during the rollout, and owners on nodes of the older version could no longer restore them.
- **Fix:** the detach events are checked like a squash's; while a node cannot read their class, the delete waits as for a failed subscription (its event stays in the outbox and is tried again when delivered again). Test: `PermanentDeleteSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F143 — A permanent delete detached duplicates whose first event subscribers did not have yet
- **Low**, confirmed (by reading; reproduced in `EventStoreStateSpec`) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState`, `MemoryEventStoreState` (`detachBlocker`)
- **Symptom:** a squash waits while a duplicate it detaches still has its first event in the outbox or a failed subscription (D51): subscribers must have the original first event before the detach replaces it. A permanent delete detached the same way without those two checks, so a subscriber could get the detach's squash event as the duplicate's first event instead of the duplication event.
- **Fix:** one check for both, which the delete answers as "not deleted yet" (its event stays in the outbox and is tried again when delivered again). The outbox rows this node has acknowledged are deleted before either checks (they were deleted in batches up to 20 ms later), so a delete right after a duplicate's first event was acknowledged is not refused for it. Test: `EventStoreStateSpec` (both stores; red before); `PermanentDeleteSpec` caught the batching race.
- **Found:** M6 survey.

#### F144 — Owners of contexts without snapshots never matched the stored history generation after an undo
- **Medium**, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M6** (17/10/2026; introduced by F131 the same day) · `AggregateRestorer.restore`
- **Symptom:** the restorer read the history generation only for contexts with snapshots, so an owner of any other context held generation 0, while an undo, a rewrite or a squash that may change the state raises the stored one. Since F131 a `Local` owner on a node that is not alone compares both before it answers, so after such a change it never matched again and restored the whole aggregate before every read (three reads, three restores).
- **Fix:** the restorer reads the generation for every context (one key lookup per restore). Test: `LocalOwnersSpec` (one restore for three reads after an undo on the other node; three under the old rule).
- **Found:** M6 survey.

#### F145 — Commands on another node's `Local` owner ran on a state changed without a new version
- **Medium**, confirmed (reproduced in `LocalOwnersSpec`) · **Fixed in M6** (17/10/2026) · `AggregateOwner.attempt`
- **Symptom:** F131 made `Local` owners catch up before reads only. A change that keeps an aggregate's version (a rewrite of its base, a squash that may change its state, a rewrite without a new event) left another node's owner with its state of before, and a command there ran on it and committed: a duplicate whose base was renamed took a command on the old name.
- **Fix:** such an owner catches up before a command too (one key lookup per command, on nodes that are not alone). Test: `LocalOwnersSpec` (red before).
- **Found:** M6 survey.

#### F146 — A history rewrite without a new event was answered with success and not stored
- **Medium**, confirmed (by reading) · **Fixed in M6** (17/10/2026, with the complete rewrite, D76) · `AggregateOwner.persistOrAnswer`
- **Symptom:** a command that gives no events writes nothing (R9), and the owner applied that rule to rewrites too: a `RewriteCommandSuccess` with rewritten events and no new event was answered `SuccessResponse` and dropped. D17 recommends a compensating event, but nothing requires one, and a rewrite for R15 has none to add.
- **Fix:** a rewrite without a new event stores its rewritten events, keeps the version and publishes nothing; the owner reads its state again. Test: `HistoryRewriteSpec` "F146" (red before: the name stayed).
- **Found:** M6 survey.

#### F147 — A duplication at a version its base has not reached was stored, and the copy then read the base's later events
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `EventStoreStateSpec` "F147", both stores) · `add_duplication_event`
- **Symptom:** `add_duplication_event` checks that the base exists and that the version is not below its squash point, but not that the base has reached it. A copy made "at version 10" of a base at version 5 reads the base's versions 6 to 10 as they are stored later: the copy's state changes without an event of its own, and its subscribers are never told.
- **Fix:** the procedure refuses a version above the base's current one, under the lock it already takes.
- **Found:** writing the book (M6, 17/10/2026).

#### F148 — Event handlers must handle undo events, although the owner reads the state again after an undo
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; the test domain's undo case removed, four undo scenarios of `ReactiveTestDomainSpec`, `SnapshotSpec` and `LocalOwnersSpec` failed before the fix) · `AggregateOwner.nextRoot`
- **Symptom:** the owner applies every new event to a copy of the state before the commit, undo events included, so a context without a case for its undo event fails the command with `EventHandlingError`; restores skip undo events, and the state after an undo is read from the store.
- **Fix:** the trial run skips undo events, as restores do; a command with an undo event still has its state read from the store after the commit. A case for the undo event is never called.
- **Found:** writing the book (M6, 17/10/2026).

#### F149 — A refused first event without a space answered with the decision's number
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `AggregateOwner.persistOrAnswer`
- **Symptom:** the `FailureResponse` text, which users see, named the decision "(D38)".
- **Found:** writing the book (M6, 17/10/2026).

#### F154 — Squash and detach events were stored without the event size check
- **Low**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresEventStoreStateSpec` "F154") · `PostgresEventStoreState.squash`, `detachDuplicate`, `overwriteEvents`
- **Symptom:** the three wrote their events with `mpjsons.serialize` rather than `serializedChecked`, so a large state gave a squash event above `eventSizeLimit` (and its per-class limits), and a rewrite could grow an event past it.
- **Fix:** all three are checked; the transaction then fails and nothing changes (a squash is answered `SquashRefused`).
- **Found:** writing the book (M6, 17/10/2026).

#### F155 — An automatic squash checks its event as `UserId(0)`, so a state that records its last user never passes
- **Medium**, confirmed (by reading) · **Decided at review 8** (M6 close, 17/10/2026): D85 holds, and the remedy is the squash event's · `AggregateRestorer.squashEvent`, `SquashScheduler`
- **Symptom:** D85's check applies the squash event as the request's user, as a restore will; the scheduler asks as `UserId(0)`. A context whose event handlers copy the user into the state ("last changed by") gets every automatic squash refused, and a manual one passes only when the right person asks. Options: the squash event takes the user of the event it replaces (keeping the squasher in its metadata), or the check compares states without such fields (the context says which).
- **Decision:** neither option. A squash event already stands for the whole state (D47), so it carries the users and times the state keeps, and its handler takes them from the event, as it takes everything else, never from its `userId` and `timestamp` parameters, which are the squash's. Then the check passes whoever squashes, the scheduler included, and a restore gives the same state. The book's chapter 14 says so, and the refusal now says it too (`SquashEventCheckSpec` and `SquashSpec` match its first words). Nothing else changes.
- **Found:** writing the book (M6, 17/10/2026).

#### F183 — Dead API: unused event and id helpers in `api`
- **Low**, confirmed · **Fixed in M7** (18/10/2026; `ApiSurfaceSpec`, red first) · `api/.../EventHandler.scala`, `Aggregate.scala`, `AggregateVersion.scala`, `id/AggregateId.scala`, `id/SpaceId.scala`
- **Symptom:** documenting the API for 1.0 found classes and methods that nothing uses: `EventHandler` and `AbstractEventHandler` (a class per event handler, beside the context's type `AggregateContext.EventHandler` of the same name), `IdentifiableEvent`, `EventIdentifier`, `EventsIdentifiers`, `EventWithIdentifier`, `AggregateVersion.upTo` and `isJustAfter`, and `LRUCache`, deprecated since F32. `SpaceId` kept a pool of its instances in a global `var` read without a lock, though spaces compare by value, as ids have since F32.
- **Fix:** all of them are removed; Neula's code uses none (a scan of `neulaserver`'s sources, 18/10/2026), and the library none. `SpaceId(n)` makes a new instance.
- **Found:** writing the Scaladoc (M7, 18/10/2026).

#### F157 — Dead API: `OnConcurrentModification` and `SimulateEvent`
- **Low**, confirmed · **Fixed in M7** (17/10/2026; `ApiSurfaceSpec`, red first) · `api/.../command/OnConcurrentModification.scala`, `AggregateContext.scala`
- **Symptom:** `OnConcurrentModification` (`Retry`/`Fail`) is used nowhere: the kind of command alone decides what a clash does. `SimulateEvent` is declared, and the command bus answers it as an unknown message. `NoEventsForAggregateException` is never thrown (it has only a serializer binding).
- **Fix:** `OnConcurrentModification`, `Retry`, `Fail` and `SimulateEvent` are removed; Neula's code uses none of them (a scan of `neulaserver`, 17/10/2026). `NoEventsForAggregateException` stays: since D77 the ephemeral command bus answers a read of an unknown ephemeral id with it.
- **Found:** writing the book (M6, 17/10/2026).

#### F165 — `io.reactivecqrs.api.Nothing` hides `scala.Nothing` after `import io.reactivecqrs.api._`
- **Low**, confirmed · **Fixed in M7** (17/10/2026: renamed to `NoInfo`, the owner's decision; no stored format holds the class: the owner answers it with `SuccessResponse`, and `StoredFormatsSpec`'s references name none) · `api/.../CustomCommandResponse.scala`
- **Found:** writing the book (M6, 17/10/2026).

### Event store and SQL

#### F198 — An organization export is not one snapshot: a write during it gives two events at one version after the import
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2 of the risk assessment; `ExportImportSpec` "F198" and `PostgresEventStoreStateSpec` "F198") · `SpaceExport`, the event store's indexes, `StoredDataCheck`
- **Fix:** the export reads every row in one `REPEATABLE READ, READ ONLY` transaction, so a command that commits meanwhile is in the file whole or not at all; the import refuses an aggregate whose events are not exactly its versions (F199). `events (aggregate_id, version)` is unique (`events_aggregate_version_uidx`): made at start on a small store, and by `buildIndexesConcurrently()` on a large one, which keeps the plain index and logs an error while some aggregate has two events of one version; `StoredDataCheck` names those (`repeatedVersions`).
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F199 — The organization import cannot tell a complete file from a truncated or inconsistent one, and trusts its references and class names
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `ExportImportSpec` "F199") · `SpaceFormat`, `SpaceExport`, `SpaceImport`
- **Fix:** format 2 ends with a trailer (counts and the SHA-256 of the lines before it); the import refuses a file without one (format 1 only with `acceptFormat1`), with lines after it, or whose counts or digest differ. Its first pass requires each aggregate's events to be its versions from its squash point, each once, in order, archived events at most its version, chain orders 1..n and chain and lineage bases in the file; it looks classes up without initializing them, and requires an event class to be an `Event` and a root class to be one of the contexts the import now takes (a required parameter). A second pass maps every event and checks its size before anything is written.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F225 — The organization export builds every cross-space detach event before writing, and binds their ids in one IN list
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; by reading, and `ExportImportSpec`'s export of a detached copy; 70 000 copies were not run) · `SpaceExport`
- **Fix:** the copies that read outside are found by a join, not an IN list of their ids, and each one's detach event is made when its version-1 row is reached and written at once; only the check that its type can detach it (a context with a squash event) runs before the first line.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F226 — The organization import's write batches are bounded by count only
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `ExportImportSpec` "F248" imports with `bytesPerTransaction = 1`; the 64 MB heap run was not made) · `SpaceImport`
- **Fix:** a transaction of events or archived events ends at `eventsPerTransaction` (10 000) or `bytesPerTransaction` (16 MB, as F126 bounds the bus's parts), whichever comes first.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F248 — An organization import keeps ids that the target's own generator will hand out later
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `ExportImportSpec` "F248") · `SpaceImport`
- **Fix:** every exported id in the range of the target's aggregate id sequence (its installation's, D39) gets a new id: below the sequence a node's pool may hold it, above it the sequence reaches it later, and a client may choose it. An import from another installation keeps its ids. Moving the sequence past the ids above it was tried and rejected: it made the pools hand out ids that clients had chosen (the test kit's own ids showed it at once).
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F237 — The snapshot generation guard is not atomic: a snapshot can be written over a history that changed meanwhile
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `PostgresEventStoreStateSpec` "F237", red under a mutation) · `PostgresEventStoreState.writeSnapshot`, `historyChangedFor`
- **Fix:** the guard locks the aggregate's row (`FOR SHARE`), and the changes raise the generation, which locks the row, before they delete the snapshots: a snapshot write that read the old generation waits for the change, then sees the new one and writes nothing.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F244 — The version limit is checked on events that add no version: long aggregates can be neither erased, squashed nor permanently deleted
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 2; `PostgresEventStoreStateSpec` "F244", red under a mutation) · `PostgresEventStoreState.serializedChecked`
- **Fix:** the version limit is checked only for events that add a version (the procedures' calls), and never for a permanent delete; rewritten, squash and detach events meet the size limit only.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F190 — Failed SQL statements are logged with their values: events, documents and saga orders reach the logs
- **High**, suspected · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `LogPrivacySpec` "F190", red first) · every component that opens a ScalikeJDBC session
- **Symptom:** ScalikeJDBC logs a failed statement with its bound values at ERROR, so an event, document or saga order with personal data reached the logs, where erasure does not reach.
- **Fix:** the library's sessions use `SqlLogging.withoutValues` (no statement logging, no failed statements); a spec scans the sources so that every session-opening file imports it. `-Dreactivecqrs.log-values=true` logs the details again, at INFO on the logger `io.reactivecqrs.detail`, for a developer's machine. The book adds `logServerErrorDetail=false` to the JDBC URL, so the server's DETAIL with the key's values stays out of exception texts too.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F191 — Exception texts carry event, state, order and document values into logs, replies, `subscription_failures` and the bus's warnings
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `LogPrivacySpec` "F191", `FailedRestoreSpec`, `SagaRunnerSpec`) · `util/Errors`, `util/ActorLogging`, about 40 sites
- **Symptom:** a `MatchError`'s text is its whole value; a JSON error quotes the input; a PostgreSQL error quotes the row. These texts went into logs, command replies, `subscription_failures` and the bus's warnings, repeated at every re-delivery.
- **Fix:** `Errors.describe` gives one line per cause without values (the class of a `MatchError`'s value; SQLState, constraint, table and column; a JSON problem and its position; a parse error without its input), and `Errors.redacted` the same as an exception with the original stack trace; actors log through a `RedactingLog`, and every library site that wrote an exception's text uses them. The bus logs a subscriber's `MessageFailed` once per subscription and aggregate. ARCHITECTURE §15 lists what reaches the logs.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F186 — No statement, socket or lock timeouts: a hung connection pins owners and threads, and silently stops re-delivery
- **High**, suspected · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `OutboxPollerActorSpec` and `SquashSchedulerSpec` "F186") · `OutboxPollerActor`, `SquashScheduler`, the book's chapter 18, E2
- **Symptom:** a read on a connection that hangs never ended: the outbox poller and the squash scheduler waited for it forever, so re-delivery stopped without a word.
- **Fix:** their reads are numbered and watched (`ReadWatch`): one that has not ended in `readTimeout` (5 minutes) is given up with an error and a metric (`reactivecqrs.outbox.read.abandoned`), its late result is ignored, and the next poll reads again. The book and E2 set the driver's `socketTimeout=600` and `tcpKeepAlive=true`.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F272 — Mismatched defaults: a 32-thread blocking dispatcher against an 8-connection default pool, no validation query, and driver defaults that hang or log values
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `StartupLocksSpec` "F272") · `BlockingDispatcher`, the test kit, the book's chapter 18
- **Fix:** a warning at start, once per system, when the pool's `maxSize` is smaller than the blocking dispatcher's threads (`PoolCheck`); the test kit's pool has 32 connections; the book's pool settings add a `validationQuery` and the driver settings of F186 and F190.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F193 — Squashing a duplicate drops its link to its base, so a later erasure of the base never reaches it
- **High**, suspected · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `EventStoreStateSpec` "F193", red first) · `PostgresEventStoreState`, `MemoryEventStoreState`
- **Fix:** a squash, and the detachment of a copy, record its lineage before the chain rows go, in both stores, so a complete rewrite of the base still finds the copy among its copies.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F22 — Restore loads the whole event stream into memory before handling it
- **Medium**, likely · **Fixed in M3** (16/10/2026; heap measured in `baseline-M3`) · `PostgresEventStoreState.readAndProcessEvents`
- **Root cause:** `DB.readOnly` keeps autocommit on and no `fetchSize` is set (`PostgresEventStoreState.scala:202`), so the driver buffers every row (up to 500 000 in Neula).
- **Fix:** restores read in a transaction (autocommit off) with a fetch size, so the driver streams the rows (D34); snapshots (D30) shorten the history read.

#### F31 — `add_events` loops `add_event`; `add_undo_event` scans and sorts the whole history
- **Low**, likely · **Fixed in M3** (16/10/2026; statements measured in `baseline-M3`) · `PostgresEventStoreSchemaInitializer` (`add_events`, `add_undo_event`)
- **Fix:** `add_events` updates the aggregate's row once for the whole command and inserts the events in a loop inside the function; plain and first events always go through it, one call per command. `add_undo_event` walks back the index on `(aggregate_id, version)` with `LIMIT` instead of sorting the history (D34).

#### F38 — Restore query order has no matching index; one index is redundant
- **Low**, likely · **Fixed in M3** (16/10/2026), the drop later · `PostgresEventStoreState.buildIndexesConcurrently`
- **Fix:** index `events(aggregate_id, version)` built `CONCURRENTLY` by `buildIndexesConcurrently()`, which the host runs once in the background after the upgrade (H8); an index an interrupted build left invalid is built again. Drop `events(aggregate_id)` in a later contract step (D34, D69). Test: `PostgresEventStoreStateSpec`.

#### F65 — Event times are stored without a time zone
- **Low**, confirmed · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState` (`utc`, `utcTime`)
- **Symptom:** on a JVM whose default zone is not UTC, event times can shift by an hour around a daylight-saving change, because `event_time` is `TIMESTAMP` (without time zone) and is converted with the JVM default zone on write and read. Neula forces UTC (`Startup.scala:16`), so it is not affected today.
- **Fix:** `event_time` is bound and read with an explicit UTC calendar; the column type stays (D28). Neula runs in UTC, so its stored values read back unchanged. Promised as a separate entry by D10. Test: `PostgresEventStoreStateSpec` "event times are stored as UTC" (red under a mutation binding with the JVM's zone, on a machine in Central European time).

#### F39 — Instant reads filter in Scala
- **Low**, confirmed (1 call site in Neula) · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState.readAndProcessEvents`
- **Also wrong:** a read at an instant used today's undo flags, so an event undone after that instant showed as undone.
- **Fix:** the version at the instant is found in SQL, then the read runs as for that version, undo flags included. Test: `PostgresEventStoreStateSpec` "a read at an instant".

#### F49 — Legacy `aggregates` rows have `space_id = -1`
- **Medium**, suspected (count unknown) · Pre-check in **M1** (`StoredDataCheck` reports it), **backfill built in M4** (17/10/2026: `PostgresEventStoreState.backfillSpaces(spaceOf)` gives each such aggregate the space of its first event, or the host's function's for a first event that does not know it, online in batches; `ProjectionsSpec`. Neula runs it once, H9) · schema
- **Root cause:** the column was added with `DEFAULT -1` (`PostgresEventStoreSchemaInitializer.scala:117-121`); six deprecated Neula events use `SpaceId.unknown`.
- **Check:** `SELECT count(*) FROM aggregates WHERE space_id = -1` on production.
- **Fix:** D38.

#### F76 — Permanently deleting an aggregate that duplicates read from breaks the duplicates
- **High** if Neula deletes such aggregates, suspected · **Fixed in M3** (16/10/2026, with the duplicate detach of D50) · `PostgresEventStoreState.deletePublishedEventsToPublish` (`PermanentDeleteEvent` branch)
- **Symptom:** after a `PermanentDeleteEvent` on an aggregate that has duplicates, those duplicates restore without the history they share with it, so their state is silently wrong; their chain rows in `aggregates` point at a base that no longer exists.
- **Root cause:** the delete removes the base's events and rows only; duplicates read the base's events through their chain rows (D50 solves the same problem for squash).
- **Found:** plan review at the M1 close.
- **Check:** Neula, on a production copy: `SELECT count(*) FROM aggregates a WHERE a.base_id <> a.id AND NOT EXISTS (SELECT 1 FROM aggregates b WHERE b.id = a.base_id AND b.base_id = b.id)` (duplicates whose base is gone).
- **Fix:** detach every duplicate before deleting its base, as D50 does before a squash. With D20a the delete also needs a precondition like D51 (no failed subscriptions), because it removes the `subscriptions` rows that would hold the failure. The precondition is in since M2 (16/10/2026): the delete waits while the aggregate has a row in `subscription_failures` (`PermanentDeleteSpec`). Since M3 the command bus restores each duplicate at version 1 and the delete, in its transaction, makes that state the duplicate's first event (the type's squash event) and removes its chain rows, under the chain lock of F96; a duplicate made meanwhile postpones the delete. A type without a squash event cannot detach: its aggregate with duplicates is not deleted, with an error log, and the event stays in the outbox (H8).
- **Tests:** `PermanentDeleteSpec` (the duplicate keeps its state; without a squash event the delete waits; failed against the old delete), `EventStoreStateSpec` (both stores).

#### F96 — A duplication could read history a squash removes
- **High** once squash is used, found in the M3 squash design review (16/10/2026) · **Fixed in M3** · `add_duplication_event`, `PostgresEventStoreState.squash`/`deleteAggregatePermanently`
- **Symptom:** a duplication of an aggregate at a version below its squash point would read events that no longer exist, so the duplicate's state is silently wrong; a duplication committed between the owner listing the duplicates to detach and the squash's commit would not be detached; the same for a permanent delete (F76).
- **Fix:** `add_duplication_event` locks the base's chain rows `FOR KEY SHARE` in key order and then refuses a missing base or a version below its `squashed_version`; a squash or permanent delete locks the aggregate's chain and its duplicates' rows to it `FOR UPDATE` in the same order, then checks its preconditions again and compares the duplicates with those it was given (else it changes nothing, "changed meanwhile"). Whichever runs first, the other sees its result. The in-memory store mirrors the checks.
- **Tests:** `PostgresEventStoreStateSpec` (a duplication during a squash waits and is refused; a squash waiting for a duplication sees it and changes nothing), `EventStoreStateSpec`, `SquashSpec`; both failed against the code without the locks and checks.

F100–F107 were found by the read-only audit at the M3 close (review 5, 16/10/2026) and fixed then; each test below failed against the code before the fix.

#### F100 — Owners wrote snapshots of states the store no longer had
- **High**, confirmed · **Fixed in M3** · `AggregateOwner.stopNow`, `AggregateCommandBusActor.stopOwner`, `writeSnapshot`
- **Symptom:** an owner stopped after its aggregate was deleted for good wrote a snapshot of it, so the erased state stayed in `snapshots`; owners of duplicates whose history changed under them (a detach with a state-changing squash event, a rewrite of their base) wrote their old states back as snapshots, which later restores read.
- **Fix:** the command bus stops such owners with `StopOwnerStale`, which writes no snapshot (also when the owner was stopping already); after a rewrite or a state-changing squash the owner names the duplicates whose histories changed (`HistoryChanged`); a snapshot is written only while the aggregate is stored at that version.
- **Tests:** `PermanentDeleteSpec` (no snapshot after a delete), `SquashSpec`, `HistoryRewriteSpec` (a duplicate's live owner shows its base's rewritten history).

#### F101 — A squash that changed the state on purpose left the duplicates still reading the aggregate stale
- **Medium**, confirmed · **Fixed in M3** · `PostgresEventStoreState.squash`, `AggregateOwner.squashHistory` (and the in-memory store)
- **Symptom:** after a `SquashMayChangeState` squash, duplicates that read the aggregate above the squash point read the new squash event, so their states changed, but their snapshots and running owners kept the old ones.
- **Fix:** such a squash deletes the snapshots of every aggregate still reading the squashed one and has the owners of all its duplicates stopped. Test: `SquashSpec`.

#### F102 — A read at an instant before the squash point answered "did not exist"
- **Medium**, confirmed · **Fixed in M3** · `PostgresEventStoreState.processEvents`
- **Symptom:** no event is left before the squash point, so an instant before the squash event's time resolved to version 0 and the read answered an empty aggregate; also, a squash committed between the read's check and its query left the read incomplete.
- **Fix:** the squash point is checked after the read, in its transaction, and an instant before the squash event's time fails with `AggregateSquashedException` (now also naming the instant). Test: `PostgresEventStoreStateSpec`.

#### F103 — A history rewrite of a duplicate reached its base's events
- **Medium**, confirmed (from before 1.0) · **Fixed in M3** · `AggregateRestorer.eventsOfClasses`
- **Symptom:** the rewrite handler got the base's events of the classes asked for, with the base's versions, and the store applied them to the duplicate's own rows: the rewrite failed on the class check, or, with the same class, replaced the duplicate's own event.
- **Fix:** only the aggregate's own events are given to the handler. Test: `HistoryRewriteSpec`.

#### F104 — A squash or delete could deadlock with a duplication of a duplicate
- **Low** (PostgreSQL detected it and aborted one side), confirmed · **Fixed in M3** · `PostgresEventStoreState.lockChain`
- **Symptom:** the squash locked only the rows that point to the aggregate, then deleted the detached duplicate's other chain rows, which a duplication of that duplicate could hold while it waited for a row the squash held.
- **Fix:** one ordered statement locks every chain row of the aggregate and of each aggregate reading it. Test: `PostgresEventStoreStateSpec` (the two run side by side; deadlocked before).

#### F105 — Detaching a duplicate whose first event was undone changed its state
- **Low**, confirmed · **Fixed in M3** · `squashBlocker`, `deleteAggregatePermanently` (both stores)
- **Symptom:** the detach replaced the undone first event with a squash event that was no longer undone, so the duplicate got back what its undo had taken; the in-memory store kept it undone, so the stores differed.
- **Fix:** a squash is refused, and a permanent delete waits, while a duplicate to detach has its first event undone. Test: `EventStoreStateSpec`.

#### F106 — The in-memory store checked a command's undos against the history before the command
- **Low**, confirmed · **Fixed in M3** · `MemoryEventStoreState.persistEvents`
- **Symptom:** after a squash, an undo of an event stored by the same command was refused, and a second undo that reached the squash event was accepted; PostgreSQL does the opposite, as its procedures store the events one after another.
- **Fix:** each event is checked against the events before it, the command's included, before anything changes. Test: `EventStoreStateSpec`.

#### F107 — A failed pool fetched ahead failed commands that had ids at hand
- **Low**, confirmed · **Fixed in M3** · `AggregateCommandBusActor.idsNotReceived`
- **Symptom:** when the next pool of one kind of ids did not come, every waiting command was answered with an error, also while ids of that kind were still at hand (they waited for the other kind).
- **Fix:** a failed pool fails only the commands that need its kind of ids while none is left. Test: `CommandOutcomesSpec`.

#### F125 — The per-aggregate limit lets one event too many through
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `PostgresEventStoreState.serializedChecked`
- **Symptom:** `aggregateVersionLimit = 10 000` let an aggregate reach version 10 001: the check compared the version before the event with the limit, not the event's own.
- **Fix:** an event is refused when the version before it has reached the limit. Test: `PostgresEventStoreStateSpec` (events stored together, one by one and as an undo; red before the fix).
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F142 — An event's size was its characters when written and its bytes when read
- **Low**, confirmed (reproduced in `PostgresEventStoreStateSpec`) · **Fixed in M5** (17/10/2026) · `PostgresEventStoreState`
- **Symptom:** the store reported a written event's size as the length of its JSON string (UTF-16 characters), and a read event's as PostgreSQL's `octet_length` (UTF-8 bytes). For text outside ASCII, such as Polish, they differ (161 bytes against 111 characters for 50 ASCII and 50 Polish letters), so the bus's byte window and the stored-bytes metric counted the same event differently on the live path and in re-publications.
- **Fix:** a written event's size is its UTF-8 bytes, counted without encoding the string. The per-class size limits stay in characters. Test: `PostgresEventStoreStateSpec` (sizes of Polish text, red before; the count against `getBytes`).
- **Found:** M5 close, audit of delivery.

#### F168 — `EventTooLargeException` spoke of bytes for a limit in characters
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `api/.../Exceptions.scala`
- **Symptom:** the check compares the JSON's length in characters (`PostgresEventStoreState.serializedChecked`); the message says bytes.
- **Found:** writing the book (M6, 17/10/2026).

#### F167 — A permanent delete left the aggregate's stored command responses to expire
- **Low**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresEventStoreStateSpec` "F167") · `PostgresEventStoreState.deletingPermanently`
- **Symptom:** since M6 a stored response names its aggregate (R17), and a purge deletes the responses of the aggregates it removes; a permanent delete still left them for the retention (30 days by default), with the aggregate's data in the response's info.
- **Found:** writing the book (M6, 17/10/2026).

### Performance and runtime

#### F172 — A builder's page of events read every event of the store once the tables were analyzed
- **High** (performance at scale), confirmed (measured) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState.readCommittedPage`, `pageOf`, `backfillEventKeys`
- **Symptom:** in the short JSON keys bench (IMPLEMENTATION-PLAN §7.1), a managed rebuild over 1 000 000 events ran at about 4 400 events/s, then at about 2 500 once PostgreSQL's autoanalyze had run on the tables. `EXPLAIN ANALYZE` of one page over 2 000 000 events of 2 000 aggregates: 786 ms, 643 ms of them reading all 2 000 000 events through `events (aggregate_id)`. Every page did so, so a rebuild's reading grew with the store's size times its pages.
- **Root cause:** the page joined each event to its aggregate's own row with `a.id = e.aggregate_id AND a.base_id = e.aggregate_id`. The planner takes the implied `id = base_id` at its default selectivity of 0.5 % (it holds for nearly every row), so it expected 10 aggregates and drove the join from `aggregates`, reading all their events, instead of looking up the page's 1 000 events and their rows.
- **Fix:** each event's own row is looked up by its key in a `LATERAL` subquery, whose `LIMIT 1` keeps the planner from flattening it into such a join: 5 ms for the same page. The same for the page of named aggregates, the walk before the event keys exist (43 ms → 0.6 ms on the same data) and the event-key backfill (F124).
- **Verified:** `EXPLAIN ANALYZE` on the bench's data before and after; the specs pass unchanged (the plans changed, not the rows); the rebuild of `docs/perf/jsonnames-M6.md` ran with the fix.
- **Found:** the short JSON keys bench (M6, 17/10/2026).

#### F175 — The event key of F124 costs about 8 % of the plain write path
- **Medium** (performance), measured · **Accepted** (the owner, 17/10/2026: the cost shows on the bare write path only, with no measured difference under two projections; rebuilds by type and organization stay proportional to their own events) · `add_event`, `add_events` (`PostgresEventStoreSchemaInitializer`)
- **Symptom:** at the M6 close, M6 and the M5 tag run alternately the same day (two runs each): commands without subscribers 9 196 and 8 423/s against 10 140 and 9 010/s, serial mode 11 044 and 11 747/s against 12 805 and 12 414/s; with two projections no difference. M6 with the key's insert taken out of the procedures ran at M5's level (9 918 and 12 206/s). Each event writes one more row with three index entries (its primary key, `(type_id, id)`, `(space_id, type_id, id)`). F124's log had found "nothing measurable".
- **Options:** accept it as the price of builds by type and organization that read only their events (F124); or slim the key: one index fewer if a build of a space reads `(space_id, id)` and filters the types, which costs the space's other types' keys; or the type and space as columns of `events` with the same two indexes, which saves the key's row and primary key but rewrites the events table (D69).
- **Found:** the M6 close's bench (17/10/2026).

#### F170 — Each event in a delivery between nodes carries about 275 bytes besides its JSON
- **Low** (performance), measured · **Fixed in M7** (17/10/2026; `ClusterSerializationSpec` "F170", red first: 287 bytes) · `ClusterSerializer` (D68)
- **Symptom:** the cluster serializer writes the library's case classes field by field with their class and field names (D68), so that a node of another version can read them. In an `IdentifiableEvents`, each `EventInfo` repeats its class name and five field names, `AggregateVersion` and `UserId` as named products, and the event's full class name: 273 to 278 bytes per event besides the event's JSON (measured, `docs/perf/jsonnames-M6.md`), more than a small event's whole JSON (104 to 239 characters there). A delivery part (half of the 248 KiB frame, F121) holds 248 such events of 239 characters; with an envelope of about 30 bytes it would hold about 470 (estimated). More parts mean more messages and acknowledgements.
- **Fix:** a sequence of `EventInfo` is written under a tag of its own: the event classes once, then each event's version, user and time as numbers, its class as an index into those names, its JSON and its envelope (`storedMetadata`, as before). 33 bytes per event besides the JSON (computed from the format: 4 + 8 + 12 + 4 + 4 + 1 for an event without an envelope; the spec measures under 40), so a delivery part of half the 248 KiB frame holds about 466 events of 239 characters instead of 248 (computed). Not behind a version switch, as planned: 1.0.0 is the first release that runs as a cluster, so no released node reads the old form; a cluster of a milestone build (M5, M6) is upgraded with a full stop (book, chapter 17). A new tag after 1.0 needs a switch like D67's.
- **Found:** the short JSON keys bench (IMPLEMENTATION-PLAN §7.1, M6, 17/10/2026).

#### F173 — A page of a type's aggregates costs about 80 ms at a million aggregates of the type
- **Low** (performance at scale), measured · **Fixed in M7** (17/10/2026; `PostgresEventStoreStateSpec` "F173", red first, and `EventStoreStateSpec`'s page of several types, both stores) · `PostgresEventStoreState.aggregatesOf`
- **Symptom:** the builder's final-state pass (D62), its reconciliation and the targeted rebuild of a type walk the type's aggregates 1 000 at a time. With 1 000 000 aggregates of one type, a page in the middle took 70 to 100 ms (`EXPLAIN ANALYZE`, PostgreSQL 17.4): misjudging `id = base_id` at 0.5 % again, the planner scans `aggregates (base_id)` in parallel, and leaves an added index on `(type_id, id)` unused. About 80 s for the whole walk (estimated from the page).
- **Fix:** a partial index `aggregates_own_type_idx ON aggregates (type_id, id) WHERE id = base_id`, made at start on a small installation and by `buildIndexesConcurrently()` on a large one (D69), and a page that reads each type through it (`unnest` of the type ids, a `LATERAL` page per type, then the first `limit`): with `type_id IN (...)` the index gives no order across the types, and a page of two types read all their rows to sort them (298 ms).
- **Measured** (`EXPLAIN ANALYZE`, PostgreSQL 17, this machine, single runs, 2 000 000 aggregates, 1 000 000 of one type, 20 000 duplicates): a page in the middle of one type 110.8 ms before, 0.28 ms after; of two types 174 ms before, 0.94 ms after (0.99 ms with a generic plan of the prepared query); of one type in one of 100 spaces 94 ms before, 10 ms after; the last page 0.24 ms.
- **Found:** checking the queries like F172's (M6, 17/10/2026).

#### F127 — Command handlers ran on Pekko's default dispatcher
- **Medium**, confirmed (measured) · **Fixed in M5** (17/10/2026) · `AggregateOwner` (`handlersContext`)
- **Symptom:** a command handler that holds its thread for long (computing, or waiting for I/O) held one of the default dispatcher's threads, which the command buses, the event bus, projections and sagas run on too. Measured (docs/perf/heavy-before-F127.md): 16 handlers of 5 s on 12 threads held a light command on another aggregate for 4.9 s; handlers of minutes would stall the node's delivery for minutes.
- **Fix:** handlers run on `reactivecqrs.handler-dispatcher` (32 threads). Measured after (docs/perf/heavy-M5.md): with 36 such handlers the event bus answered in 8 ms; commands still wait while every handler thread is held, so heavy work belongs on the application's own threads (`AsyncCommandResult`) or out of the command (docs/SCALING.md §8). Test: `HandlerDispatcherSpec` (red under the default dispatcher).
- **Found:** analysis of command handlers that take minutes at M5 (docs/SCALING.md §8).

#### F126 — Batches counted in events hold whatever their events weigh
- **Medium**, confirmed (measured with 5 MB events; the larger batches estimated) · **Fixed in M5** (17/10/2026) · `AggregateOwner.republish`, `ProjectionBuilder`, `PostgresEventStoreState` (`fetchSize`), `EventsBusActor`
- **Symptom:** an event store that allows large events (`eventSizeLimitPerClassName`) keeps every batch counted in events: a re-publication part of 1 000 events, a rebuild page and chunk of 1 000 events, a fetch of 1 000 rows, and the window F93 plans in events. With events of 5 MB a part is about 5 GB, more than any heap. Measured (docs/perf/large-M5.md): a restore of 20 such events took 206 MB of heap, about 10 MB per event.
- **Fix:** the store reports each event's stored size when it writes it (`persist`) and reads it (`readPart`, `readCommittedPage`), cutting a range by the sizes in SQL (`octet_length`, without detoasting); re-publication parts are at most 1 000 events and 16 MB, rebuild pages 64 MB and chunks 16 MB (a page its bytes cut short is not the last), the reconciliation reads an aggregate's missing range in parts, and the bus's window counts bytes (F93). Restores still fetch 1 000 rows at a time: hosts with large events lower `fetchSize` or set the driver's `adaptiveFetch` (SCALING §7). Tests: `PostgresEventStoreStateSpec`, `ProjectionBuilderSpec` (a page cut short did not end the bulk pass: red under the old `readAll`), `EventsBusActorSpec`.
- **Found:** analysis of commands and events of 5 MB at M5 (docs/SCALING.md §7).

#### F23 — Id pools of 100 and a blocking wait on refill
- **Medium**, likely · **Fixed in M3** (16/10/2026) · `UidGeneratorSchemaInitializer.scala:18-28`, `AggregateCommandBusActor.scala:315-348`, `SagaActor.scala:136-152`
- **Fix:** pools of 10 000 (`ALTER SEQUENCE … INCREMENT BY`), asynchronous prefetch at a low-water mark (D34).
- **Note (16/10/2026):** since F40 a failing id store fails the `Await` at once instead of after 60 s; F75 makes the bus and the saga actor answer that failure instead of restarting. The `Await` itself (the bus blocks while a pool is fetched) goes with the asynchronous prefetch.
- **As fixed:** the sequences step by 10 000 (an older one is changed at start; the next pool starts past every pool handed out, so no id is given twice, also with a node still on the old size). The command bus and the saga actor keep their ids in an `IdPool`, which asks for the next pool when a fifth is left; a command or order that arrives while none is at hand waits, with the ones after it, and the actor keeps answering other messages; a pool that does not come fails the waiting ones (F75) and is asked for again with the next. Tests: `IdPoolSpec`; `CommandOutcomesSpec` (a query is answered while a command waits for a slow pool; failed against the old bus), the F75 scenarios.

#### F99 — Contended concurrent commands wait a back-off even for conflicts their owner sees itself
- **Low** (serial mode is the remedy, R8), confirmed (measured at the M3 close, 16/10/2026) · **Fixed in M5** (17/10/2026) · `AggregateOwner.ownConflict`, `runContended`
- **Symptom:** 16 callers sending `ConcurrentCommand`s at once to one aggregate: 738 commands/s, p99 380 ms (`baseline-M3.md`), against 1 166 commands/s and p99 197 ms before the owner (`baseline-M2.md`). The same load in serial mode runs at 2 501 commands/s, p99 11 ms.
- **Root cause:** every conflict waits D37's jittered back-off (10 ms doubling) before the command runs again, also when the owner itself saw it (another command of the same owner committed first), where no other writer is involved and waiting spreads nothing.
- **Fix idea:** a command that conflicts with a commit of its own owner runs again as soon as that commit is done, one at a time (serial after a first conflict); conflicts found by the store (another writer) keep the back-off. Measure with the bench's contended scenario; the attempt limit must not fail commands that only waited their turn.
- **As fixed:** a concurrent command whose handler ran on a version that a commit of its own owner has since passed waits its turn in a queue and runs again on the newer state, one at a time, oldest first; while any wait, new concurrent commands wait behind them, so a contended owner runs serially until its queue is empty. Waiting is not an attempt. Conflicts found by the store keep D37's back-off, and a command back from it waits its turn too. Test: `ContendedCommandsSpec` (40 commands at once to one aggregate: all succeed, at most 80 handler runs; 233 with the back-off). `SerialCommandsSpec`'s comparison now finds no conflict in concurrent mode under 1 000 commands at once. Measured (single runs, this machine, 16 callers, 800 commands): 887 commands/s, p99 36.4 ms, max 48 ms, against M4's 1 133/s, p99 339 ms, max 613 ms; serial mode in the same run 1 646/s, p99 16.9 ms (M4: 2 384/s), so that run was about 30% slower throughout.

#### F27 — Blocking JDBC runs on the default dispatcher
- **Medium**, likely · **Fixed in M3** (16/10/2026) · persist, restore, projections, futures
- **Fix:** a dedicated blocking dispatcher (D34).
- **Note (16/10/2026):** the command bus carries out permanent deletes on its own thread, and since F76 it first restores each duplicate of the deleted aggregate there; rare, but it holds up every command of the type meanwhile. Step 10 moves that work off the bus.
- **As fixed:** `reactivecqrs.blocking-dispatcher` in core's `reference.conf` (fixed pool of 32, throughput 1). Owners run on it, so persisting stays synchronous on their thread (F69 holds), and their command handlers run on the default dispatcher. The command bus (historical reads, the permanent delete with its restores, expired responses), the uid generator, the saga state, the outbox poller and the squash scheduler run their JDBC in futures on it and take the result back as a message. Projections are made by the host, which should use `Props(...).withDispatcher(BlockingDispatcher.id)`; one on the default dispatcher logs a warning at start (H8). The replayer is left to M4. Test: `CommandOutcomesSpec` (where an owner's event handlers and command handlers run; failed without the change).

#### F30 — Reflection per event on hot paths
- **Low**, likely · **Fixed in M3** (16/10/2026) · `PostgresEventStoreState.scala:69, 82, 96, 109, 127`; `AggregateRepositoryActor.scala:312`
- **Fix:** precompute type ids; cache mpjsons serializers per class (D34).
- **As fixed:** events carry a `ClassTag` of their root instead of a `TypeTag` (D75), so the persist path takes the aggregate type key from the class once per command, without runtime reflection; mpjsons 0.7 already caches its codecs and type lookups per class in thread-safe maps; type ids come from the cache of F78.

#### F32 — Global synchronized LRU pool for `AggregateId`
- **Low**, likely · **Fixed in M3** (16/10/2026) · `api/.../id/AggregateId.scala:8-27`
- **Fix:** drop the pooling (D34).
- **As fixed:** `AggregateId(n)` makes a new instance; ids compare by value. The public `LRUCache` it used is deprecated.

#### F33 — Small hot-path waste
- **Low**, likely · Fix in **M2** for `pendingPublish.distinct` (`AggregateRepositoryActor.scala:95, 136`, removed with D8: **fixed** 16/10/2026) and the ERROR-level debug line (`ProjectionActor.scala:645`, rewritten with the sequence gate: **fixed** 16/10/2026); in **M3** for the eager log strings (**fixed** 16/10/2026: the optional events logger builds its lines only when debug is on)

#### F40 — `println` in production paths
- **Low**, confirmed · **Fixed in M1** (16/10/2026) · `UidGeneratorActor.scala:39, 50, 61` (failures also leave the requester unanswered), `SagaActor.scala:47`, `EventsBusActor.scala:182-185`
- **Test:** `UidGeneratorActorSpec` (the ask timed out before the fix).
- **Fix:** logger instead of `println`; the uid generator answers a failure with `Status.Failure`, so the requester fails at once. The replay tools keep their console output (operator progress), reworked in M4.

#### F48 — In-memory caches have no cross-node invalidation
- **Medium** in a cluster, likely · **Fixed in M5** (17/10/2026) · `PostgresDocumentStore`, `PostgresSubscriptionsState`
- **Fix:** one writer per projection (D53) keeps its caches valid.
- **As fixed:** a managed projection's supervisor, with its instances and builder, runs once in the cluster (`Projections.start` on a cluster makes it a singleton), so its document caches and subscription versions have one writer. Two writers at once, in a partition before the resolver downs a side, are decided by the subscription versions' compare-and-set and the document versions, as before. A subscriber registered on one node's bus only sees the events of that node's owners: whatever must see every event runs once. Test: `ClusterDeliverySpec` (one instance across three nodes, also after its node left; three without the singleton).

#### F78 — The type-name cache is changed from many threads without a lock; two nodes can race on a new name
- **Medium**, likely · **Fixed in M3** (16/10/2026, both parts) · `PostgresTypesNamesState.typeIdByClassName`, `classNameById`, `insertName`
- **Symptom:** the first use of a new class from two threads at once updates a plain `HashMap` concurrently (the lock covers only the database read); two nodes storing a new class at the same moment both `INSERT` the name and one persist fails on the unique index.
- **Root cause:** `cache.getOrElseUpdate` runs outside the monitor; the insert has no conflict handling.
- **Found:** plan review at the M1 close.
- **Fix:** a concurrent map, and `INSERT … ON CONFLICT (name) DO NOTHING` followed by a read.
- **Test:** `PostgresTypesNamesStateSpec` (four instances and forty threads ask for one new name: one id; failed before).

### Document store

#### F16 — Document cache can be ahead of the database after a rollback
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresDocumentStore`
- **Fix:** every cache write goes through `cacheAfterCommit`: inside an `AfterCommit.localTx` (which every projection update is) the entry is dropped at once and stored after the commit; an older version never replaces a newer one. Outside such a transaction caching is immediate, as before. Test: `CachesAfterRollbackSpec` (document scenarios; both rollback ones failed before the fix).
- **Symptom:** after a rolled-back listener transaction, reads serve documents the database does not have; later updates conflict.
- **Root cause:** cache writes inside the listener transaction (`insertDocument`, `updateDocument` in `PostgresDocumentStore.scala`; transaction at `ProjectionActor.scala:466-469`).
- **Fix:** D16.
- **Invariants:** I5.

#### F21 — Document store loses updates on a version conflict
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresDocumentStore`
- **Fix:** one versioned write (`writeNextVersion`) used by all four update paths: it reports a conflict, evicts the stale cache entry so the retry reads fresh data, and every path retries at most 10 times. `overwriteDocument` skips the JSON write only when a version check proves the cached copy is current; before, a stale cached copy equal to the new document left the database unchanged.
- **Tests:** `PostgresDocumentStoreConflictSpec` (failed before the fix: lost update `ac` instead of `bc`, "Too many tries" on a stale cache entry, `StackOverflowError` in the auto-id store). The overwrite case did not run before the fix (the suite aborted); its failure on the old code is inferred from reading.
- **Related:** cache writes before commit were fixed separately (F16).
- **Symptom:** a read-model change that meets a version conflict is silently dropped and the cache holds a document the database does not have; the auto-id store can loop forever. Probably happening in Neula today (about 60 cached stores, parallel listeners). **Accepted production risk until 1.0 (D80).**
- **Root cause:** in `updateDocumentRecur` the changed-document branch ends with `val updated = …` (`PostgresDocumentStore.scala:543-546`), so the block returns `Unit` and `updated == 0` is never true; retries re-read the same stale cache entry; the auto-id variant recurses without a limit (`:665-666`).
- **Test:** force a version conflict on a changed document; assert a retry against fresh data, or an error after the limit, never a silent success.
- **Fix:** return the row count, evict the cache entry before retrying, limit retries in both stores; D16 for commit-safe caching.

#### F43 — Document-store migration is not one transaction
- **Low**, likely · **Fixed in M4** (17/10/2026: the table and its column changes run in one transaction; the indexes stay apart, as before) · `PostgresDocumentStore.scala:30-33`
- **Fix:** run the migration steps in one transaction, following D69 for large tables.

#### F118 — The document store refused valid table names
- **Low**, confirmed · **Fixed in M4** (17/10/2026: only the names the store makes count, the table and each index; `PostgresDocumentStoreSpec`, red before the fix) · `PostgresDocumentStore`
- **Symptom:** the guard against names PostgreSQL cuts at 63 characters always counted an index name, so a table name of 47 to 52 characters (58 to 63 with `projection_`) was refused for a store without indexes; with a build's suffix more names passed the limit, and the refusal stopped the build's instance (F110).
- **Found:** M4 close (review 6), audit.

#### F158 — `findDocumentByObjectInArray` wrote its value into the SQL text
- **High**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresDocumentStoreSpec` "F158") · `PostgresDocumentStore.findDocumentByObjectInArray`
- **Symptom:** the value is put between quotes in the query without escaping, so a value with a quote breaks the query, and one made to do so changes it (SQL injection) when a host passes a user's input.
- **Fix:** the JSON is bound as a parameter, written by a JSON encoder.
- **Found:** writing the book (M6, 17/10/2026).

#### F159 — Some document queries made invalid SQL
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresDocumentStoreSpec` "F159") · `PostgresDocumentStore.createQuery`, `createPartsQuery`
- **Symptom:** `findDocument` with no condition gave `WHERE` with nothing after it (the memory store returns every document); the parts queries' int and long sorts closed the quote in the wrong place, always added `LIMIT`, so `DocumentStoreQuery.basic` (limit -1) was refused, and read string parts as text, which the JSON reader refuses.
- **Found:** writing the book (M6, 17/10/2026).

#### F156 — The memory document store ignores spaces
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `MemoryDocumentStoreSpec` "F156", both memory stores) · `MemoryDocumentStore`
- **Symptom:** `clearSpace` does nothing, so a test of clearing an organization against it passes without testing anything.
- **Fix:** the store keeps each document's space as the `space_id` column does: set when the document is inserted (or created by `updateDocument`), kept after; `clearSpace` removes that space's documents, `removeDocument` and `clearAllData` their spaces.
- **Found:** writing the book (M6, 17/10/2026).

#### F160 — Two finders read outside the listener's transaction
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `PostgresDocumentStoreSpec` "F160") · `DocumentStore.findDocumentByPath(ExpectedValue)`, `findDocumentByPaths`
- **Symptom:** they take no session, so inside a listener they read what is committed, not what the listener's batch wrote; and each takes a second connection from the pool while the listener holds one, which a full pool makes wait.
- **Fix:** `findDocumentByPaths` takes `(implicit session: DBSession = null)` as the other finders do; `findDocumentByPath(value: ExpectedValue)` takes an implicit `CallerSession`, the caller's implicit session or none, since only one alternative of the overloaded `findDocumentByPath` may have a default argument. Callers compile unchanged (Neula has 26 calls of the one and 24 of the other, H12).
- **Found:** writing the book (M6, 17/10/2026).

#### F171 — A document index whose path changes keeps its old definition
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `PostgresDocumentStoreSpec` "F171") · `PostgresDocumentStore.init`
- **Symptom:** the store matches the indexes it declares with those in the database by their `uniqueId` only. An index declared with the same id and a new path (a renamed field, or a field given a short JSON name) is neither dropped nor created again: the old index stays on the old path, and queries on the new path read the whole table, without a warning.
- **Fix:** `init` compares each index's definition (`pg_indexes.indexdef`) with the declared one, both reduced to uniqueness, method and expressions (PostgreSQL prints `'{a}'::text[]`, `integer` and more parentheses), and builds a changed one again, with a warning. Not concurrently, as planned: the table's only writer is its projection, which waits for its store anyway, and reads go on meanwhile; on a large table the projection's start waits for the build, so the book recommends a new projection version (new tables) for a large table. The shapes of the statements are those of 0.10.12 and 0.11.46, so Neula's indexes match and none is built again at the upgrade. Also: indexes are found by the exact name pattern, where `'%_idx_%'` took the primary key of a table whose name holds "idx".
- **Found:** planning the short JSON keys measurement (IMPLEMENTATION-PLAN §7.1, M6, 17/10/2026).

### Sagas

#### F205 — A saga step whose command timed out is reverted, but the command can still be applied later, and nothing undoes it
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 3 of the risk assessment; `SagaFaultsSpec` "F205", red under a mutation) · `SagaRunning`, `SagaDefinition.stepRetries`, the book's examples
- **Fix:** a step whose future fails runs again with the same `SagaStep` key (`stepRetries`: 1 s doubling to 30 s, five attempts) before the saga reverts, so a command applied late answers with its stored response and the saga goes on; revert steps likewise. `SagaStepNotDone` is what a step throws for an answer that may change (`CommandRejected`, `CommandHandlingError`, `EventHandlingError`); the book's sagas and chapter 12 return `SagaFailed` for refusals only.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F206 — A saga answered "failed" can run forward to success at the runner's next start, days later
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 3; `SagaFaultsSpec` "F206" and `PostgresSagaStateSpec`, red under a mutation) · `SagaRunning`, `PostgresSagaState`
- **Fix:** a caller hears a saga's outcome only once it is stored (the move to `REVERTING`, the finish); a progress write that keeps failing is tried again at `persistRetries`' longest delay for as long as the runner runs, with an error and a metric (`reactivecqrs.saga.write.failed`), never dropped. A create is `ON CONFLICT DO NOTHING`, an earlier attempt's row counting as stored. `GetSagaStatus` reports running sagas, waiting orders and failing writes. Left: a new saga whose first write fails every retry is still refused, so one whose commit was stored but not acknowledged on its last attempt runs at the next start; and the runner's in-flight writes are not drained before it stops (the compare-and-set writes of F232 stop a late one).
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F207 — Saga orders cannot be made idempotent, so a caller that retries during a singleton move starts the saga twice
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 3; `SagaFaultsSpec` "F207", `PostgresSagaStateSpec`, red under a mutation) · `IdempotentSagaOrder`, `SagaRunning`, `PostgresSagaState`, the `sagas` table
- **Fix:** an order extending `IdempotentSagaOrder` carries an `orderKey`, stored with its saga under a unique `(name, order_key)` index. A second order with the key joins the saga under way (its caller answered with the first's) or, once it is done, gets the stored answer: a keyed saga's row stays, `finished`, with the answer its caller got, for `answersKeptFor` (30 days), then the runner removes it. A repeat that reads the key before the saga stored its answer reads again.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F232 — One saga can run twice at once in one runner, and saga rows have no compare-and-set to stop the stale copy
- **Medium**, likely · **Fixed for 1.0.1** (18/10/2026, step 3; `SagaFaultsSpec` "F232", `PostgresSagaStateSpec` "F232", red under a mutation) · `SagaRunning`, `PostgresSagaState`
- **Fix:** orders wait until the runner's first load is done (it is tried until it succeeds); the runner keeps the ids of the sagas it runs and ignores loads of them, so `LoadPendingSagas` (kept public) resumes only sagas not under way; every progress write and finish names the phase and step it moves from, and a write that finds the saga elsewhere stops that copy.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F17 — Saga uses `ActorContext` off its thread
- **Low**, likely · **Fixed in M1** (16/10/2026, with F35) · `SagaActor.scala:96, 99`
- **Fix:** step results come back as messages; replies and decisions run on the actor's thread.

#### F34 — `sagas` table has no primary key or index
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresSagaSchemaInitializer.scala:10-23`
- **Test:** `PostgresSagaStateSpec` "holds one row per saga name and id" (a duplicate row was accepted before the fix).
- **Fix:** unique index `sagas_name_saga_id_idx (name, saga_id)` built `CONCURRENTLY`; an invalid index left by an interrupted build is dropped and rebuilt; duplicate rows stop `initSchema()` with an explicit message (Neula: check for duplicates on a production copy before upgrading, `SELECT name, saga_id FROM sagas GROUP BY 1, 2 HAVING count(*) > 1`).

#### F35 — Saga failures thrown inside `onComplete` lose the saga
- **High**, confirmed · **Fixed in M1** (16/10/2026) · `SagaActor.scala:70, 80, 108, 132`
- **Symptom:** a failed persist leaves the saga unfinished, with no retry and only an unsupervised exception.
- **Test:** `SagaRecoverySpec`: writes that fail a few times (before the fix the caller never got an answer), an order that cannot be stored (no answer before), a saga that resumes after a restart without repeating its last step's effects.
- **Fix:** every write result comes back as a message; failed writes are retried with back-off (100 ms doubling to 30 s, at most 10 attempts: `SagaActor.persistRetries`, overridable), then the saga is left at its last stored step and resumes after a restart. An order that cannot be stored is answered with `SagaFailureResponse`. Pending sagas are read completely before any starts, and a failed load is retried.

#### F36 — Saga update and delete ignore row counts
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresSagaState.scala:23-33`
- **Test:** `PostgresSagaStateSpec` (both returned `Unit` before the fix).
- **Fix:** `updateSaga` and `deleteSaga` return whether the row was there; the actor stops a saga whose row vanished, with an error log.

#### F136 — A saga can answer its caller twice after its runner moves
- **Low**, likely · **Accepted in M5** (17/10/2026; documented) · `SagaActor.handleStepResult`
- **Symptom:** a saga answers its caller before the write that ends it (or moves it to reverting) is stored. If the runner's node stops in between, the runner on the next node loads the saga at the same step, runs it again (its commands are idempotent by `SagaStep`, H3) and answers again: a live caller gets the answer twice.
- **Decision:** answers stay at least once. Answering after the write would lose the answer when the node stops between the commit and the answer, and the caller would order the saga again, which runs it all again under a new id. An `ask` takes the first answer (the second goes to dead letters); a caller that is a long-lived actor tells the answers apart by the saga's id, or takes any answer after the first as a repeat. Documented in ARCHITECTURE §9 and H10.
- **Found:** M5 close, audit of the cluster.

#### F137 — A saga stored the caller of an ask made on the runner's node without the node's address
- **Low**, likely (confirmed in `SagaCallerPathSpec`) · **Fixed in M5** (17/10/2026) · `SagaActor.callerPath`
- **Symptom:** Pekko writes an `ask`'s temporary actor with its node's address only while it serializes a message. Stored outside one, its path had no address (`pekko://cluster/temp/$a`), so after the runner moved, the answer went to the actor of that path on the new node: dead letters, or an unrelated ask of the same temporary name. Against D46.
- **Fix:** the path is written within the node's transport information, which gives it the address. Test: `SagaCallerPathSpec` (red before).
- **Found:** M5 close, audit of the cluster.

#### F151 — One unreadable stored saga order kept every saga of its name from resuming
- **Medium**, confirmed (red first) · **Fixed in M6** (17/10/2026; `PostgresSagaStateSpec` "F151") · `PostgresSagaState.loadAllSagas`
- **Symptom:** the load reads every row of a saga name before any starts, and one order that no longer reads fails the whole load, which is retried without end: no saga of that name resumes.
- **Fix:** an unreadable row is logged and left out (`StoredDataCheck` reports it); the others resume.
- **Found:** writing the book (M6, 17/10/2026).

### Serialization (mpjsons 0.7)

#### F51 — mpjsons 0.7 may serialize the `TypeTag` held by every event
- **Closed, not a problem** (16/10/2026): mpjsons 0.7.0 skips the synthetic evidence field. `StoredFormatsSpec` shows event JSON without it, references recorded with 0.6.50 still read back, and the full suite passes on 0.7.0. Removing the `TypeTag` (D75) is still wanted for Scala 3, but no longer urgent.
- Original concern: `api/.../Event.scala:7`
- **Root cause:** 0.7 writes inherited fields; `Event[A: TypeTag]` stores its context bound as a field; 0.7 skips only fields reflection marks synthetic (mpjsons `ReflectionUtilNoCache.scala:50`). If not skipped, every event write fails.
- **Fix:** D75 removes the `TypeTag` from `Event`; the golden test proves the output.
- **Done (M3, 16/10/2026):** `Event` carries a `ClassTag` instead (D75); its evidence field is synthetic like the old one, and `StoredFormatsSpec` (the committed references and "an event carries no reflection data") passes unchanged.

#### F64 — Command responses with custom info cannot be serialized for idempotent commands
- **Medium**, confirmed · **Fixed in M1** (16/10/2026, with R7, D12) · `CommandResponseState.scala:58-61`
- **Symptom:** an idempotent command whose handler returns response info (`CustomSuccessResponse`) fails the whole persist transaction, so its events are not written. Neula has no idempotent commands yet; R7 and H3 will add them.
- **Root cause:** responses are serialized by class name (`mpjsons.serialize(response, response.getClass.getName)`); for `CustomSuccessResponse[INFO]` mpjsons cannot resolve `INFO` ("key not found: type INFO"), with both 0.6.50 and 0.7.0.
- **Test:** found while building `StoredFormatsSpec`; `IdempotentCommandsSpec` "a response with custom info …" (no reply before the fix) and `CommandResponseStateSpec`.
- **Fix:** the id and version are stored as a `SuccessResponse`, the info in new nullable columns `info` and `info_type_id` with its own class (additive schema change, D28). The info must be of a class without type parameters, such as a case class; otherwise storing fails with an explicit message. Existing rows are read as before (no row with custom info could have been written).

#### F135 — The upgrade notes let an added field of an application class rely on its default
- **Medium**, likely (by mpjsons's documented behaviour) · **Fixed in M5** (17/10/2026; documentation) · H10, D68
- **Symptom:** H10 and the design said a field added later to a command, state or answer "must be an `Option` or have a default". The cluster serializer applies Scala defaults to the library's messages only; the application's classes are read by mpjsons with `ignoreNonExistingFields`, where a missing field reads as zero, null or `None`. A command of version 2 with `tags: List[String] = Nil`, sent by a node of version 1, reached its owner with `tags == null`.
- **Fix:** H10, D68 and the design say that a field added to an application class must be an `Option` (or a number or flag whose zero is right); a default is not applied.
- **Found:** M5 close, audit of the cluster.

### Schema initialization

#### F185 — Every node start takes exclusive locks on the event tables, with no lock timeout
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `StartupLocksSpec`: 14 of its 16 scenarios red first) · every `*SchemaInitializer` and `initSchema()`, `PostgresDocumentStore`
- **Symptom:** `ALTER TABLE ... IF NOT EXISTS` takes an ACCESS EXCLUSIVE lock and `CREATE INDEX IF NOT EXISTS` a SHARE lock before they look: a node's start queued behind a long transaction, and every statement on the table queued behind the start.
- **Fix:** `util/Ddl`: each start-up step looks in the catalog first (`information_schema.columns`, `to_regclass`, `pg_sequences`) and changes only what is missing, under `SET LOCAL lock_timeout` (2 s), tried 30 times on SQLState 55P03, then fails with a message that points at `pg_stat_activity`. An unchanged schema takes no table lock at start.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F63 — Concurrent schema initialization fails
- **Medium**, confirmed · **Fixed in M5** (17/10/2026: every schema initialization, the components' `initSchema()` and the document stores' tables, runs under a PostgreSQL advisory lock held on a connection of its own, so processes that start together initialize one at a time; `SchemaLockSpec`, red before the fix with a duplicate `pg_type` key) · `PostgresEventStoreSchemaInitializer` and every `initSchema`
- **Symptom:** two processes initializing the schema at once (test suites in parallel today; cluster nodes starting together later) fail with a duplicate key on `pg_proc_proname_args_nsp_index` for `add_event`.
- **Root cause:** `CREATE OR REPLACE FUNCTION` and the other initialization statements run without any lock.
- **Fix:** run migrations once, under a PostgreSQL advisory lock, outside node startup (D69). Tests now run one suite at a time.

#### F71 — Catalog lookups ignore the schema
- **Medium**, confirmed · **Fixed in M1** (16/10/2026) · `PostgresUidGenerator.readSequenceStep`, `PostgresDocumentStore.readExistingIndices`, `PostgresEventStoreState.countAllEvents` (removed with the replayer in M4), `PostgresSagaSchemaInitializer` (the F34 index check, new code of the same day)
- **Symptom:** in a database where another schema holds objects of the same names (a second installation, a copy, earlier test runs), the uid generator fails to start (`TooManyRowsException` from `pg_sequences`), a projection's indices or the saga key index are not created because the other schema's are seen, and the event count reads `public.events` whatever the current schema is.
- **Root cause:** catalog queries filtered by object name only, and one hard-coded `public`.
- **Found:** per-run test schemas (below): every testdomain spec failed at start.
- **Test:** `SchemaAwareCatalogSpec` (all three scenarios failed before the fix), and the whole suite now runs in a fresh schema per run.
- **Fix:** `schemaname = current_schema()`, `to_regclass(…)` and `'events'::regclass`, so the library works in whatever schema the connection's `search_path` selects. Also `countAllEvents` treats the `-1` estimate of a never-analyzed table (PostgreSQL 14+) like 0 and counts exactly.

### Rebuild

#### F192 — An erasure that could not rebuild a projection reports "complete" when run again; a build in progress switches in with the erased values
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 2 of the risk assessment; `CompleteRewriteSpec` "F192", three scenarios, each red under a mutation that takes its fix out) · `HistoryRewriter`, `ProjectionSupervisor`
- **Symptom:** a second run of an erasure whose first could not rebuild a projection found nothing to change, asked no projection and reported complete; a targeted rebuild was refused while a build or another targeted rebuild ran, and a build that had read the aggregate before the rewrite switched in with the old values.
- **Fix:** the rewriter has every aggregate it visited rebuilt, changed or not, so a run again rebuilds what an earlier one could not, and counts no projection rebuilt when it asked nothing. The supervisor queues targeted rebuilds instead of refusing them, each for the active build or for the build in progress; a build's job runs once it caught up and holds its switch until done; the jobs are kept in the registry (`pending_target`, one per line), so a stop loses none.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F189 — The active build's instance runs the new code's store setup on the old tables; when that fails, the projection and its node's bus stay down
- **High**, likely · **Fixed for 1.0.1** (18/10/2026, step 1 of the risk assessment; `ProjectionsSpec` "F189", red first) · `ProjectionBuild`, `Projections`, `PostgresDocumentStore`
- **Symptom:** a new version declaring an index the old documents violate (a unique one) made the active build's instance fail to start on its own tables, again every few seconds: the supervisor answered nothing, the projection served nothing, and the bus waited for it.
- **Fix:** a build older than the code's version is not `current`; its document stores' tables are marked (`OutdatedTables`) and keep the indexes they have. The code's indexes come with its own build's tables.
- **Found:** the risk assessment of 1.0.0 (18/10/2026); scenario, mechanism and validation under the same number in [RISK-ASSESSMENT.md](RISK-ASSESSMENT.md).

#### F54 — Rebuild needs downtime and clears read models first
- **Medium**, likely · **Fixed in M4** (17/10/2026: managed projections build a new version in tables of their own while the active version serves, and switch in one update; `ProjectionsSpec`) · `EventsReplayOrchestrator.scala:79-81`; Neula runs it as a separate run mode
- **Fix:** D57, D70.

#### F55 — The rebuild clear is not atomic
- **Medium**, likely · **Fixed in M4** (17/10/2026: a build starts in new tables, so nothing is cleared) · `SubscriptionsState.scala:291-294`; Neula clears documents in a separate auto-commit (e.g. `KanbansProjection.scala:30`)
- **Fix:** removed by D57.

#### F56 — Replayed events reach every subscriber of the type
- **Medium**, likely · **Fixed in M4** (17/10/2026: the builder delivers to the projection it builds only; `ProjectionBuilderSpec`) · `EventsBusActor.scala:300-326`
- **Fix:** D58.

#### F57 — Rebuild order differs from live order
- **Medium**, suspected (depends on cross-type Neula projections) · **Fixed in M4** (17/10/2026: the bulk pass reads every type of the projection in commit order; `ProjectionBuilderSpec`) · `EventsReplayerActor.scala:118-168`
- **Fix:** D59.

#### F98 — A subscriber that never saw an aggregate's squash point cannot start at the squash event
- **Medium** once squash is used, confirmed by reading (found at the M3 close, 16/10/2026) · **Fixed in M4** (17/10/2026: a re-publication from the squash point says so, `startsAtSquash`, through the bus to the delivery messages, and the gate starts a subscription below it there; a state listener is told the aggregate is created. Tests: `SequenceGateSpec`, `ProjectionDeliverySpec` (red under a mutation that ignores the flag), `EventsBusActorSpec`, `RepublishSpec`. The rebuild's part, reading each aggregate from its squash event, comes with the builder) · `SequenceGate.decide`, `ProjectionActor.drainFrom`, the delivery messages
- **Symptom:** a projection added after an aggregate was squashed (or one whose progress was reset) asks for the aggregate's events from version 1; the owner re-publishes from the squash event at N (D51), and the gate sees a gap before N, asks again, and gives up after `gapGiveUpAfter` (10 minutes) with a recorded failure. Live subscribers are not affected: D51 lets a squash run only after every subscriber applied the versions up to N.
- **Fix idea:** a delivery that starts with the squash event says so (the owner knows its squash point), and the gate applies such a delivery as the start of the aggregate's sequence when the subscriber is below it; M4's rebuild reads from the store and starts each aggregate at its squash event (D51).
- **Until then (H8):** add a projection before squashing the aggregate types it reads, or expect its failures on squashed aggregates.

#### F58 — Rebuild hides undo, and the hidden versions stall the rebuild
- **High**, confirmed (raised from Medium in M2, 16/10/2026) · **Fixed in M4** (17/10/2026: the builder delivers every event as live delivery sent it, undone ones and undos too, with each state as it was; `ProjectionBuilderSpec`, red under a mutation that ignores the undo in the replay states) · `PostgresEventStoreState.scala:284`
- **Symptom:** a rebuild leaves out undone events and undo events. A projection that rebuilds from cleared state then sees a gap at those versions (a cart with an undo at version 5: 1–3, then 6), keeps the later event waiting for versions that never come, and never acknowledges it. So the replay does not end: it goes on only when the bus deadline (D20b, 10 minutes) ends the stalled deliveries, which leaves those aggregates behind in the rebuilt projection, or the replayer's own 10-minute timeout exits the JVM first (F82).
- **Found:** the full test run in M2. The old `EventsReplaySpec` replayed the carts of the other specs, one of them with an undo, and both projections kept version 6 waiting for 4 and 5. (They had also failed on version 1, because the spec reused the other specs' read-model tables; that was a test fault.)
- **Fix:** D60; see H6.

#### F59 — Rebuild skips events still in the outbox
- **Low**, likely · **Fixed in M4** (17/10/2026: the builder reads the store, outbox or not; the gate drops what live delivery brings again) · `EventsReplayerActor.scala:155-168`
- **Fix:** rebuild reads the store directly (D58); live delivery dedups.

#### F60 — Rebuild progress is not durable
- **Medium**, likely · **Fixed in M4** (17/10/2026: the subscription versions commit with each chunk, and the bulk position is saved after it; a stopped build continues; `ProjectionBuilderSpec`, `ProjectionsSpec`) · `EventsReplayerActor.scala:179-182`
- **Fix:** D61.

#### F61 — Rebuild triggers are too coarse
- **Low**, likely · **Fixed in M4** (17/10/2026: a projection is built when its version changes, when a context whose states it receives changes version, or on request, and only that projection; one aggregate, one type or one organization is rebuilt in place with `RebuildAggregates`; `ProjectionsSpec`, red under mutations that skip the version reset or widen the target) · `EventsReplayOrchestrator.scala:51-63`
- **Fix:** D63.

#### F62 — Rebuild throughput limits
- **Medium**, likely · **Fixed in M4** (17/10/2026: a build reads the store directly, with no bus and no window, and applies chunks of 1 000 events in one transaction each, in commit order, through bounded replay states restored from snapshots, reusing a duplicate's base state; state-only projections take each aggregate's final state; `ProjectionBuilderSpec`; the bench's rebuild scenario measures it, docs/perf/baseline-M4.md. Live deliveries are applied in groups too, F93)
- **Root cause:** one transaction per aggregate per projection; a 100-event window in Neula (H1); every duplicate re-reads its base's history (`ReplayAggregateRepositoryActor.scala:70-77`); state-only projections replay every event; one replay actor per aggregate lingers 30 s; full state with every replayed message.
- **Fix:** D58, D59, D61, D62.

#### F82 — The replayer stops the whole JVM when a replay stalls
- **Medium**, likely · **Fixed in M4** (17/10/2026: the replayer is removed. A build stops nothing but itself: a stored event it cannot read fails only that aggregate's subscription, visibly (D20a), and the build goes on; a store it cannot read is read again after `retryAfter`; `ProjectionBuilderSpec`) · `EventsReplayerActor` (`System.exit(-1)`), removed
- **Symptom:** a replay that times out ends the host application's process; a single unreadable stored event makes the replayer throw, so the replay never answers (found with per-run test schemas, where specs damaged events and left them).
- **Found:** plan review at the M1 close.
- **Fix:** report the failure to the requester and stop the replay only.

#### F94 — Failure records of a removed projection block permanent deletes
- **Low**, likely · **Fixed in M4** (17/10/2026: a retired build's subscription versions and failures are dropped with its tables, and `Projections.retire(name)` drops every build of a projection removed from the code; `ProjectionsSpec`) · `PostgresEventStoreState.deleteAggregatePermanently`
- **Symptom:** `subscription_failures` rows of a projection that was removed or renamed are never retried or cleared, and a permanent delete waits while the aggregate has any.
- **Found:** M2 close (review 4).
- **Until then:** an operator deletes the rows of the retired projection by its `subscriber_type_id`.

#### F108 — A projection supervisor that failed on a message lost its bus
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: the supervisor never restarts (AR7): a message it fails on is logged and answered with `Status.Failure`; a switch that fails in the registry is tried again after `restartAfter`; a context check or a targeted rebuild the registry could not record waits for the next start; `ProjectionsSpec`, red before the fix) · `ProjectionSupervisor`
- **Symptom:** a registry or store error while the supervisor handled a message (the switch, a rebuild request, a targeted rebuild, the context check at start) restarted it. The restarted supervisor made new instances but never subscribed them, as the bus asks a subscriber for its subscriptions once, so the projection took no event until the process restarted while its queries were answered from tables that no longer moved. A targeted rebuild whose end could not be recorded also kept refusing every later one.
- **Found:** M4 close (review 6), by reading.

#### F109 — A listener that failed while making its work stopped its aggregate in a live group
- **Medium**, confirmed (new code of M4, `875b1ba`) · **Fixed in M4** (17/10/2026: making the work is part of the batch, so its failure is retried and recorded like any listener failure; `ProjectionGroupsSpec`, red before the fix) · `ProjectionActor.addToGroup`
- **Symptom:** a listener written as `(id, events) => { ...; session => ... }` that threw before returning its work, with live groups on, threw out of the group's drain: that aggregate's queue stayed at work forever and the other queues due in the same drain were dropped until another delivery came for them.
- **Found:** M4 close (review 6), by reading.

#### F110 — An instance that stopped by itself took the active build down and let events leave the outbox unseen
- **High**, confirmed · **Fixed in M4** (17/10/2026: a build whose instance stops starts again alone after a wait, the active build serving on; the instances that stop stay in the bus as stopped subscribers, which keeps their events in the outbox, until their replacements are subscribed, and then leave it without acknowledging (`RemoveSubscriber(keepOutbox = true)`); build slots are held or awaited once per supervisor, and every child has a name of its own; `ProjectionsSpec`, `BuildSlotsSpec`, red before the fix) · `ProjectionSupervisor`, `BuildSlots`, `EventsBusActor`
- **Symptom:** when any instance of a managed projection stopped by itself (a new build whose stores could not be made, a fatal error), the supervisor removed every instance from the bus, which acknowledged the deliveries they had not applied, stopped the active one too and started over after `restartAfter`, again and again while the cause lasted. Events published meanwhile left the outbox without the projection (I3), the active build stopped serving (D57), and at a start a build that kept failing kept the supervisor from answering the bus, so nothing was published anywhere. A second slot request granted a second builder, and a child created under a name still in use kept the supervisor from starting.
- **Found:** M4 close (review 6), by both audits.

#### F111 — An aggregate deleted for good during a build stayed in the new build
- **High**, confirmed · **Fixed in M4** (17/10/2026: while built, a delivery that removes its aggregate, a permanent delete event or no state for a state listener, goes through the gate like a live one, missing versions coming from the builder, instead of being acknowledged unapplied; `ProjectionsSpec`, red before the fix) · `ProjectionActor.receiveDelivery`
- **Symptom:** the build in progress acknowledged the delete without applying it, as the builder reads the store; the delete then removed the aggregate's events and row, so neither the bulk pass nor the reconciliation could deliver it, and a document the bulk pass had made stayed in the new build.
- **Found:** M4 close (review 6), audit.

#### F112 — A targeted rebuild of a type or an organization lost aggregates made while it began
- **High** under load, confirmed · **Fixed in M4** (17/10/2026: the projection lists the target's aggregates when it handles the reset, after every delivery taken before; an organization is not rebuilt while aggregates of its types have no space; the deliveries waiting for reset aggregates are acknowledged, as the builder gives their events again; `ProjectionsSpec`, `ProjectionDeliverySpec`, red before the fix) · `ProjectionSupervisor.rebuildTarget`, `ProjectionActor`
- **Symptom:** the supervisor listed the aggregates before the reset reached the projection. One made and applied in between lost its document to `clearForRebuild` but kept its version, so the builder found it applied and never gave it back; so did an aggregate still without a space (F49). The queues of reset aggregates were dropped with their deliveries unanswered, which waited for the poller.
- **Found:** M4 close (review 6), both audits.

#### F114 — A targeted rebuild ran listeners' side effects for history
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: `rebuilding` is also true while a listener applies what a builder gives, a chunk or an item handed to a queue; `ProjectionDeliverySpec`, red under the old flag) · `ProjectionActor`
- **Symptom:** a targeted rebuild feeds the live instance, whose `rebuilding` was false, so push updates and other guarded side effects fired again for each historical state of the target.
- **Note:** a `noDBSession` listener in a chunk that fails runs again when the chunk is applied one item at a time: at least once, as documented.
- **Found:** M4 close (review 6), audit.

#### F116 — A build retired or abandoned just before a stop was never dropped
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: a supervisor drops at its start every build of its projection that is neither active nor in progress; a retired build loses its subscription versions and failures before its registry row, so a stop in between leaves it to that sweep; `Projections.retire` drops every build the registry has; `ProjectionsSpec`, red without the sweep) · `ProjectionSupervisor.drop`, `ProjectionRegistry`
- **Symptom:** a retired build was dropped only from memory, so a stop before the drop left its tables, its versions and its failures, which block permanent deletes and squashes (F94 again); a later build of the same version could reuse its suffix and subscriber, and inherit its versions.
- **Found:** M4 close (review 6), audit.

#### F117 — A store that failed for a moment failed a build's subscriptions
- **Medium**, confirmed · **Fixed in M4** (17/10/2026: a read error of the connection, resource or shutdown kind is read again, as a page is, or the builder stops and its supervisor starts it again from the saved position; only what cannot be read at all fails its subscription; `ProjectionBuilderSpec`, red before the fix) · `ProjectionBuilder`
- **Symptom:** any error while reading an aggregate by version or restoring its state, a lost connection too, was recorded as an unreadable event at the first version asked, and after `GoLive` nothing retried it, so the build waited for an operator before it could switch.
- **Found:** M4 close (review 6), audit.

#### F124 — A commit-order build walks the events of every type to find its own
- **Medium**, likely (not measured) · **Fixed in M6** (17/10/2026) · `PostgresEventStoreState.readCommittedPage`, `event_keys`
- **Symptom:** the bulk pass reads `events` in id order joined with `aggregates` to keep its types (and, for a scoped build, its aggregates or space). A projection whose types hold a small share of a large store, or a rebuild of one organization (D56, R17), walks the whole store once to find its events: hours at 100 million events, however few are its own (estimated).
- **Fix:** `event_keys (id → events.id ON DELETE CASCADE, type_id, space_id)`, indexed on `(type_id, id)` and `(space_id, type_id, id)`: the three procedures write each event's key, the cascade deletes it with the event, the space backfill (F49) updates it; `backfillEventKeys()` gives the events from before theirs online (D69) and records the step in `schema_steps` once every event has one (an installation without events records it at once). From then the page reads one index scan per type, in a space by its space; named aggregates are read through `events (aggregate_id, id)` from the start. Columns on `events` were rejected: their backfill would rewrite every event row (D28). Measured: the extra insert per event cost nothing measurable (two single runs each, within their spread, `docs/design/M6-organizations.md`). Tests: `PostgresEventStoreStateSpec` (keys from every procedure and gone with their events; the backfill and the switch of the read path, with the same pages; the space backfill), each red under its mutation; the builder and equivalence specs read through the keys in their fresh schemas.
- **Found:** high-load analysis at M5 (docs/SCALING.md).

#### F139 — A range read by version stopped at a squash far above its start
- **Medium**, low likelihood, confirmed (reproduced in `ProjectionBuilderSpec`) · **Fixed in M5** (17/10/2026) · `ProjectionBuilder.itemsFrom`
- **Symptom:** the reconciliation and gap fills read an aggregate's missing range in parts from the projection's version, and stopped at the first empty part. Since F126 a part is a window of versions; when the aggregate had been squashed far above that version (more than a part's events, for example by the squash scheduler during a long build), the first window held nothing, as the versions below the squash event are gone, and the aggregate got no item: the build switched in with it stale until its next event, or a gap fill ended as a recorded failure.
- **Fix:** a range starts at the squash point when it is below it, and an empty part looks at the squash point again, so a squash made after the aggregate was listed is found too. Test: `ProjectionBuilderSpec` "a range from far below the squash point starts at the squash event" (both builds red before).
- **Found:** M5 close, audit of delivery.

#### F140 — The builder's chunks could hold a whole page, and a long range restored its state once per part
- **Low**, confirmed · **Fixed in M5** (17/10/2026) · `ProjectionBuilder.committed`, `itemsFrom`, `chunked`
- **Symptom:** in the bulk pass, consecutive events of one aggregate in a page became one item, and a chunk ended only after an item took it past `chunkEvents` or `chunkBytes`: a chunk could hold up to a page (1 000 events, 64 MB), against the 16 MB that ARCHITECTURE §8.1 promised. A range read by version (the reconciliation, gap fills) restored the aggregate's state for each part, for a listener of events with state: ten restores, each a replay, for 10 000 events.
- **Fix:** a run ends where it would pass a chunk's events or bytes, and a range is read in parts of a chunk's events; a chunk ends before an item that would not fit, so it holds at most `chunkEvents` and `chunkBytes` (one event larger than that goes alone). A range restores its state once, at its end, and every part carries it, as an owner's re-published parts do. Test: `ProjectionBuilderSpec` "a long run of one aggregate is cut to fit chunks" (chunk sizes, and one restore; each red under its own mutation).
- **Found:** M5 close, audit of delivery.

#### F132 — `Projections.retire` dropped a projection that nodes of the older version still ran
- **Medium**, confirmed (reproduced in `RollingUpgradeSpec`) · **Fixed in M5** (17/10/2026) · `Projections.retire`, `Members.olderVersionsRun`
- **Symptom:** every node calls `retire` for a projection removed from its code as it starts, and it dropped the tables, subscription rows and registry entry at once. In a rolling upgrade the first node of the new version dropped the projection while its singleton still served on the oldest node, of the old version: queries failed, and its listeners wrote new failure rows after the drop, which kept permanent deletes waiting until a later restart retired it again. Against expand-then-contract (D69) and zero-downtime upgrades (D65).
- **Fix:** while a member runs an older app-version than the node (or the node knows no member yet), the drop waits, looked at again every 10 s; it happens once every member runs the node's version or a later one. Test: `RollingUpgradeSpec` (the removed projection still serves while the old nodes run, and is retired once they left; red when the drop does not wait).
- **Found:** M5 close, audit of the cluster.

#### F152 — A host cannot watch a build's progress
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `ProjectionsSpec` "F152") · `ProjectionSupervisor`
- **Symptom:** `GetBuildStatus` is the builder's, and the supervisor passes unknown messages to the serving instance, which ignores it; `GetBuilds` answers the `BuildRecord` captured when the instance was made, whose position is never refreshed. SCALING §10 lists `GetBuildStatus` as a signal.
- **Fix:** the supervisor forwards `GetBuildStatus` to the running builder (a full build's or a targeted rebuild's), and answers `Waiting` or `NoBuild` without one; `GetBuilds` gives the building record with the position read from the registry, where the builder saves it.
- **Found:** writing the book (M6, 17/10/2026).

#### F153 — `ServeStale` holds only for builds begun when the supervisor starts
- **Low**, confirmed (red first) · **Fixed in M7** (17/10/2026; `ProjectionsSpec` "F153") · `ProjectionSupervisor`
- **Symptom:** for a `RebuildProjection` at run time the active instance is already subscribed and keeps taking live events; after a restart during that build it serves stale.
- **Fix:** with `ServeStale`, a build begun at run time removes the active instance from the buses after the new build joined them, and tells it `ServeUnchanged`: it answers the deliveries still on their way without applying them and asks for no missing versions.
- **Found:** writing the book (M6, 17/10/2026).

### Code structure

#### F177 — The test kit's `??` makes the compiler warn at every `val` it gives a value to
- **Low** (the published test kit), confirmed · **Fixed in M7** (18/10/2026; a clean compile of the specs gives no dead-code warning, 135 before, and E11's specs use `askFor`) · `ActorAskSupport` (test kit)
- **Symptom:** `val x: T = actor ?? message` takes `T` from the `val`'s type. Scala 2.13 types the call with its type parameter as `Nothing` first, and `-Ywarn-dead-code` (`-Wdead-code`, on in this build and in sbt-tpolecat's defaults) reports "dead code following this construct" at each such line; an implicit `ClassTag` does not help (it is resolved first), a `def` does not warn, an explicit type argument does not (probed 18/10/2026). The library's specs had about 135 such lines, burying real warnings (noted at review 5); an application that compiles its tests with fatal warnings cannot use `??` at all. `askActor[T](message)(timeout)`, the typed variant, needs a timeout every time.
- **Fix:** one typed ask, `actor.askFor[T](message)` (10 seconds) or `askFor[T](message, timeout)`, whose type is named, so nothing is inferred; it checks the answer's class and names the actor, the message and both classes when it differs (an unnamed type fails, saying to name it). `??` and `askActor` go: the test kit was never published, and the library's specs move to `askFor`.
- **Found:** writing E11 (M7, 18/10/2026); first noted at review 5 (M3) as test hygiene.

#### F174 — The published license differed from the repository's
- **Low**, confirmed · **Fixed in M6** (17/10/2026; the owner chose MIT) · `LICENSE`, `project/Common.scala` (`licenses`)
- **Symptom:** the repository's `LICENSE` file was the Apache License 2.0 (its template, with no copyright line filled in), which GitHub shows, while the build published every artifact's POM with the MIT license (`licenses += ("MIT", ...)`). Users of the artifacts and of the repository were told different terms.
- **Fix:** the owner chose MIT: `LICENSE` is the MIT license, as the POMs say, and the README states it.
- **Found:** writing the README (M6, 17/10/2026).

#### F14 — In-memory test doubles are wrong or incomplete
- **Low**, confirmed · **Fixed in M1, step 0** (16/10/2026) · in-memory adapters
- **Found and fixed:**
  - `MemorySubscriptionsState`: `clearSubscriptionsInfo` removed the *other* subscribers' entries; a stale compare-and-set failed silently; an update without a prior read was dropped.
  - `MemoryEventStoreState`: a command with several events got versions 1, 3, 5; the outbox read compared versions with the aggregate id, so it was always empty; deleting an already-deleted event threw; the re-publish scan ignored the aggregate type; duplicated aggregates did not include their base's history; `countEventsForAggregateTypes` and `overwriteEvents` were `???`.
  - `MemoryDocumentStore`: six query conditions were `???`, two were missing (match error), `ILIKE` was an exact match, batch inserts were `???`.
- **Tests:** `MemorySubscriptionsStateSpec`, `MemoryEventStoreStateTestDoubleSpec`, `MemoryDocumentStoreSpec` (query conditions); `MemoryEventStoreStateSpec` (duplication) now passes.
- **Still not mirrored** (documented gaps of the test double): instant reads, `PermanentDeleteEvent` clean-up, spaces in the document store.

#### F97 — The in-memory store undid an undo instead of the event before it
- **Low**, confirmed (found with the squash tests, 16/10/2026) · **Fixed in M3** · `MemoryEventStoreState.withNoopFlags`
- **Symptom:** an undo right after another undo counted the first undo as the event it undoes, so in memory it brought the first undo's events back, while PostgreSQL's `add_undo_event` undoes the newest event not undone yet. Tests on the in-memory store saw another state than production.
- **Fix:** the flags are computed as the procedure does: each undo marks itself and the newest `eventsCount` events not marked yet. The store's versions also come from the last event, not the count of events, since a squash removes events.
- **Test:** `EventStoreStateSpec` "An undo right after another…" (both stores; failed on the in-memory store before).

#### F50 — Type keys disagree for nested classes; runtime reflection blocks Scala 3
- **Low**, likely · **Fixed in M3** (16/10/2026, with F30) · `AggregateContext.scala:54` vs `ProjectionActor.scala:180-196`; also the outbox scan (`AggregateCommandBusActor`: `runtimeClass.getName`) vs persist (`typeSymbol.fullName`), so a nested aggregate class never gets its waiting events re-sent
- **Fix:** D75.
- **As fixed:** one key, `AggregateType.of[R]`, for the context, the command bus, the owner, the stores, projection listeners, the replayer and the squash scheduler: Scala's full name of the class, which is what the store has always kept (`typeSymbol.fullName`), taken from the class (`getName` with `$` read as `.`). No stored name changes. Events and listeners take a `ClassTag` instead of a `TypeTag`. Test: `EventStoreStateSpec` (a nested root class, both stores). Left: `SubscribableProjectionActor` keys updates by `TypeTag` (a listener-side key, not stored).

---

#### F150 — `StoredDataCheck.usersOf` said "None" for a class nobody uses
- **Low**, confirmed · **Fixed in M6** (17/10/2026) · `StoredDataCheck.usersOf` (its comment)
- **Symptom:** the method always answers `ClassUsers`; a class can go when both its counts are zero.
- **Found:** writing the book (M6, 17/10/2026).
