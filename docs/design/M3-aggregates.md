# M3 design: the aggregate owner and its features

How milestone M3 implements AR1, AR4 and AR7 for the command pipeline, and the decisions D17,
D29–D35 and D47–D52 of `KNOWN-ISSUES.md`. Written before the work, 16/10/2026;
`ARCHITECTURE.md` describes the result.

## Order of the work

1. **Event versioning removed (D29).** Small, and it takes four parameters out of every
   signature the owner work touches.
2. **The aggregate owner (AR1, AR4, AR7; F25, F81).** Everything below lives in it.
3. **Restore and snapshots (D30; F22, F44, F39).** A restore module shared by the owner and
   historical reads; snapshots plug into it.
4. **Serial command mode (D31, R8).** A queue in the owner's command lifecycle.
5. **The schema step (D32, D49, F31, F38, F65)** with the **metadata envelope (D32, R13)**: the
   stored procedures are rewritten once, set-based, with a metadata parameter and the undo guard.
6. **Outbox hook (D33, R12).**
7. **History rewrite contract (D17, F8 first part).**
8. **Squash (D47–D52, F76).**
9. **Performance fixes (D34, D35):** F23, F27, F30, F32, F33, F43, F50, F78, where not done above.

Each step ends with `sbt test` green and a commit; the M3 verification of the plan follows.

## The owner (AR1, AR4)

Today a command travels: command bus → `CommandHandlerActor` (idempotency pre-check, one
executor per command) → `CommandExecutorActor` (asks the repository actor for the state, runs
the handler, sends `PersistEvents`) → `AggregateRepositoryActor` (persist, publish) → executor →
caller. Two per-aggregate actors kept in two maps, one actor per command, four hops.

**Shape.** One per-aggregate actor, `AggregateOwner` (the repository actor, renamed), child of
the command bus; the command bus keeps one map. The owner holds the state and version, and does
everything for its aggregate: restore, the idempotency pre-check, running command handlers,
persistence, publication and re-publication, queries, and its own stop. `CommandHandlerActor`
and `CommandExecutorActor` are deleted.

**Running a handler.** The executor actor existed to run a possibly asynchronous handler with a
timeout. The owner does that with a `Future`: it starts the handler on the current state (an
immutable value) on its dispatcher, flattens an `AsyncCommandResult`, and receives
`HandlerFinished(runId, result)`. A timer per command answers the caller at `commandTimeout`
(R5); a result that arrives after its timeout is dropped, so a timed-out command is never
persisted afterwards (today it may still be). The messages between the owner and its runs are
private and sealed, and every state handles all of them (AR4 by construction).

**One command, concurrent mode (D37, the default):**

1. Restore if not restored (D11).
2. Idempotency: a stored response is the answer (D12).
3. Version check. `Command`: an expected version above the owner's means another writer
   (F19): reload, then check; a mismatch is `AggregateConcurrentModificationError` at once, with
   no handler run. `FirstCommand`: the version must be 0. `ConcurrentCommand` and the concurrent
   rewrite run on the current version.
4. Run the handler on the current state; a rewrite command also gets the selected events.
5. On the result: `CommandFailure` and an empty `CommandSuccess` are answered with no write
   (R9). Events: the owner applies the event handlers to a copy of the state (one run, F25),
   then persists at the version the handler ran on; when another command persisted meanwhile, a
   `ConcurrentCommand` runs again on the new state after a `Backoff` delay (AR6, at most 10
   attempts), any other command gets the conflict error (after the idempotency check).
6. After commit: the state computed in 5 becomes the owner's (undo and duplication events, and
   rewrites, reload from the store instead), the events are published, and the caller gets the
   response with the exact new version.

**Queries.** `GetAggregate`, `GetEvents`, and `GetAggregateMinVersion`, which waits for the
version until its deadline; a timer per deadline answers with
`AggregateInIncorrectVersionException` when it passes (F81).

**Historical reads** (`GetAggregateForVersion`, `GetAggregateAtInstant`,
`GetEventsForAggregateForVersion`) need no actor: the command bus runs them with the restore
module in a `Future` on the blocking dispatcher and answers the caller. Today each creates a
temporary repository actor.

**Lifecycle and stopping (D11, D43, D72).** The command bus stops an owner that is idle (the
reaping rules of today) or permanently deleted. To stop, the bus marks the owner stopping,
keeps the messages that arrive for it, and sends it `StopOwner`; the owner finishes the commands
it has started, answers waiting min-version queries, writes a snapshot if its policy says so,
and stops. When the bus sees it terminated, it starts a new owner for the kept messages, if any.
An owner whose restore failed asks for this stop itself (`OwnerPassivate`) after
`failedRestoreRetryAfter`. This is Cluster Sharding's passivation, so in M5 the shard takes the
bus's place without changes to the owner; today messages sent to a stopping owner are lost.

**Failures (AR7).** The owner never throws from `receive`: a failure that belongs to one command
or query is answered to its caller; a failure of the owner itself (an exception escaping a
handler of its own messages) is logged, its waiting callers are answered with an error, and it
stops. The command bus's supervisor strategy stops a failed owner instead of restarting it,
because a restart would lose the pending callers.

## Restore and snapshots (D30)

**Restore module** (`AggregateRestorer`): given an aggregate id and an optional version or
instant, returns the version and state, and on request the events or the events of chosen
classes. It hides the duplication chains, noop events, snapshots and streaming. The owner uses
it for its state, the command bus for historical reads; M4's rebuild (D59, D62) will use it for
replay states.

**Snapshots**, opt-in per context: `snapshots = Some(SnapshotPolicy(every = 200,
onPassivation = true, schemaVersion = 1))`. Table `snapshots(aggregate_id PRIMARY KEY, version,
context_version, schema_version, state, created)`: the latest snapshot of an aggregate, as the
root's mpjsons JSON. Snapshots never consume versions and are disposable.

- **Write:** by the owner, right after a commit that crosses a multiple of `every`, and at its
  stop when `onPassivation` and the version moved since the last snapshot; in a short
  transaction of its own, on the owner's thread. A failed write is logged and changes nothing
  else. Because the owner also runs every undo, rewrite and squash of its aggregate, a snapshot
  write cannot race their invalidation.
- **Restore:** the snapshot whose context version and schema version match the context's, then
  the aggregate's own events after its version (a duplicate's base events all lie before its
  first own event). A missing, incompatible or unreadable snapshot means a full replay, never
  a failed restore (R10).
- **Invalidation:** an undo deletes the snapshot when it lies at or after the first undone
  event; a rewrite deletes the snapshots at or after the first rewritten version, of the
  aggregate and of the duplicates whose chain includes it; a squash deletes those at or below
  its point. All inside the same transaction.
- **Streaming (F22):** restores read with autocommit off and a fetch size, so a long history is
  not buffered whole; a restore at an instant filters by `event_time` in SQL (F39); the index
  `events(aggregate_id, version)` serves the restore query (F38, built `CONCURRENTLY` by an
  online step, not in the startup path, D69).

## Serial mode (D31, R8)

`commandProcessing = Serial(maxQueuePerAggregate = 1000, onOverflow = Reject)`; default
`Concurrent`. In serial mode the owner runs one command at a time from a queue: the next starts
when the current one is answered, so every handler sees the latest state and `ConcurrentCommand`
never runs twice. The idempotency pre-check runs again when a queued command starts. A command
that finds the queue full is rejected at once (`CommandHandlingError` with a "queue full"
message, logged with its error id). M5's shard entity is the same owner, so the mechanism is the
one D31 asks for.

## The schema step (D32, D49, F31, F65)

One rewrite of the stored procedures and additive columns, built at startup (M5's migration
runner takes over later):

- `events.metadata JSONB NULL` (D32); `aggregates.squashed_version INT NULL` (D49, D51).
- `add_events` set-based for plain and first events; `add_undo_event` without the history sort
  (F31), rejecting an undo that reaches at or below the squash point (D49), deleting the
  snapshot it invalidates; every procedure takes the metadata.
- `event_time` written and read with an explicit UTC calendar (F65); column type unchanged.
- The old function signatures are dropped as today, so a 0.12 node cannot write to a 1.0
  database (D81 is a stop-and-migrate).

## Metadata (D32, R13) and the outbox hook (D33, R12)

**Metadata.** `EventMetadata(actor, correlationId, causationId, traceContext, spaceId, custom)`
as R13 sketches; a command supplies it by mixing in `WithMetadata`; every event of the command
stores it; `EventInfo` carries it, and rows without it get a default envelope built from
`user_id` on read. Event handlers keep their signature. It travels with live deliveries,
re-publications, rebuilds, rewrites (unchanged) and squash (the squash event takes the squash
command's).

**Outbox hook.** `outboxWriter: Option[OutboxWriter[R]]`, called with `(aggregateId, version,
events, session)` inside the persist transaction, after the idempotency record, so a failure
rolls the events back and an idempotent copy never reaches it.

## History rewrite contract (D17)

A rewritten event must keep its class (checked before the transaction; a different class
fails the command); the rewrite deletes the snapshots it invalidates; after commit the owner
reloads its state from the store. Rewrite plus a compensating event stays the documented way to
tell projections.

## Squash (D47–D52, F76)

Opt-in per context with a squash event factory (`squash = Some(SquashPolicy(toEvent: R =>
FirstEvent[R]))`). `SquashAggregate(id, upTo)` goes to the owner, which builds the squash event
from its state at `upTo` (restore module) and, in one transaction: checks the preconditions (no
outbox rows, no failed subscriptions, D51); detaches the duplicates that read the aggregate at
or below `upTo` (their version-1 event becomes a squash event of their own state at version 1,
and their chain rows go, D50); deletes the events and noop rows at or below `upTo`; stores the
squash event at version `upTo`; deletes snapshots at or below it; records `squashed_version`.
Undo below the point is rejected (D49); a historical read below it fails with an explicit
"squashed" error; a `RepublishRange` from below it starts at the squash event. The same detach
makes a permanent delete of a base safe (F76). The optional per-context squash scheduler (D52)
runs on one node now and becomes a singleton in M5.

## Performance (D34, D35)

- **Blocking dispatcher (F27):** a `reactivecqrs.blocking-dispatcher` in the library's
  `reference.conf`; owners, projection actors, the outbox poller and the replayer run on it, so
  their synchronous JDBC never blocks the default dispatcher. Persisting stays synchronous in
  the owner (F69 holds).
- **Id pools (F23):** sequences step by 10 000; the command bus and the saga runner ask for the
  next pool when a fifth of the current one is left and keep commands that need an id while
  none is available, instead of `Await`.
- **Type keys and codecs (F30, F50, F78, D75):** aggregate and event type names come from
  classes (`ClassTag`), computed once; the types cache is concurrent and inserts with
  `ON CONFLICT DO NOTHING`.
- **Smaller ones:** no global lock in the `AggregateId` pool (F32); lazy log strings (F33);
  the document-store migration in one transaction (F43).

## Looking ahead

- **M4 rebuild** reads states through the restore module (snapshot plus tail, D59, D62) and
  sees squash events first (D51); the owner's publication is unchanged.
- **M5 cluster:** the owner becomes the shard entity as it is; `StopOwner`/`OwnerPassivate` map
  to the shard's passivation; the messages that cross nodes are the bus–owner envelope, the
  queries and the delivery messages (AR4's serializable protocol, D45).
- **M6 organizations:** export and import read through the restore module and write squash
  events (D40); the complete rewrite (D76) extends the rewrite contract here.

## Tests

The plan's M3 verification, plus: the owner's command lifecycle through its interface (the
command bus) with the in-memory store and a controlled clock (timeouts, retries, stop with
commands in flight, kept messages after a stop); historical reads; the min-version deadline;
existing specs (`RepublishSpec`, `PermanentDeleteSpec`, `OutboxPollerSpec`,
`DeliveryEndToEndSpec`, the testdomain specs) pass unchanged in behaviour.
