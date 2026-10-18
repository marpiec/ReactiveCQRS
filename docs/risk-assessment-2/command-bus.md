# Second risk assessment: command bus, ids and idempotency (R2-BUS)

- **Area:** the command bus, ids and idempotency.
- **Scope:** `core/.../commandhandler/*` (`AggregateCommandBusActor` with its ephemeral mode and `StoreMaterialized`,
  `Owners`, `CommandResponseState`, `Idempotency`), `core/.../uid/*`, `core/.../eventstore/EphemeralEventStoreState.scala`,
  `api/.../Command.scala`, `CommandResult.scala`; calls followed into `AggregateOwner` (materialization, idempotency,
  failed restore), `AggregateRestorer`, `MemoryEventStoreState`, `PostgresEventStoreState.rewriteEverywhere`,
  `HistoryRewriter`, `MessageSize`, `ClusterSerializer`, `Ddl`.
- **Date:** 18/10/2026. **Commit:** 2453711 (1.0.1-SNAPSHOT). Diff read: `git diff v1.0.0..HEAD` on the scope.
- **Method:** read only; nothing was run. Every number is marked measured (with its source) or estimated.
- **Overlap:** R2-BUS-1 shares its main path with R2-OWN-2 (owner notes); it adds the case of a refusal of a later
  attempt while an earlier one is still in flight.

## Findings

### R2-BUS-1 — After an unknown outcome, any answer that is not a success unfreezes the draft, so F208's lost edits come back
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Where:** `AggregateOwner.scala:842-845` (`case other` sets `materializing = None`); the answers that reach it:
  `AggregateOwner.scala:433` (key lookup fails → `CommandHandlingError`), `:571` (key taken, lookup fails →
  `CommandHandlingError`), `:580-587` (F253: connection lost around the commit → `CommandHandlingError`);
  `AggregateCommandBusActor.scala:395-410` (no id pool → `CommandHandlingError`); `AggregateOwner.scala:499,588-593`
  (D67 `CommandRejected`, `EventHandlingError`).
- **Scenario:** a materialization whose persistent commit lands but whose answer is not a success:
  (a) the first attempt's commit lands and the connection drops (F253); or (b) attempt 1 times out (the F208 case),
  and attempt n > 1 is answered with anything but a success while attempt 1 is still in flight or already committed:
  a failed key lookup, a failed id pool, a statement timeout, a D67 rejection.
- **Mechanism:** only a `Throwable` (ask timeout) keeps the draft frozen. Every other answer is read as "nothing
  stored", although (a) says the outcome is unknown, and in (b) a refusal of attempt n says nothing about attempt 1.
  The draft takes commands again; the caller is told `MaterializeRefused`. The next `Materialize` sends the same key
  (`materialize|start|id`), finds the stored `SuccessResponse(P1)` and drops the draft.
- **Impact:** the edits made after the first attempt are silently lost (the stored aggregate has the older state), as
  in F208; or, if the user never materializes again, P1 exists as an orphan while the user thinks nothing was stored.
- **Validation:** `persistentBus` a probe that forwards `StoreMaterialized` to the real bus and replaces its answer with
  `CommandHandlingError`. Materialize, then send a command to the draft: expected `CommandRejected`, observed applied.
  Materialize again: `Materialized(id, P1)` whose state lacks the command's change.
- **Suggested fix:** once an attempt's outcome is unknown, keep the draft frozen until a `SuccessResponse` or a
  refusal that proves no copy committed. Simplest: retry (same key) on every answer except a definite refusal of
  attempt 1 (`FailureResponse`, `CommandRejected`, `EventHandlingError`); for attempt n > 1 retry on every non-success,
  since the key lookup of a later attempt resolves it.
- **Regression test:** the validation above; and attempt 1 held past the timeout then committed, attempt 2 answered
  `EventHandlingError`: the draft stays frozen and the next Materialize answers P1 with the draft's state.
- **Continues:** F208 (and F253's contract).

### R2-BUS-2 — F218 caps passivation at 200 owners a second: under sustained traffic Local owners grow without bound, and every sweep sorts them all
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Confirmed (code); sizes estimated
- **Where:** `Owners.scala:106-116` (`StopsPerSweep = 200`; `lastSweep = now - 300000 + 1000`); the sweep runs only from
  `send` (`Owners.scala:61`).
- **Scenario:** a `Local` type (the default placement) whose commands, reads or re-publications touch more than 200
  distinct aggregates a second on one node, for minutes: imports, migrations, a busy type with many aggregates. The
  library sustains far more: 4 269 new aggregates/s and 11 226 commands/s on distinct aggregates (measured,
  docs/perf/load-M5.md).
- **Mechanism:** a sweep stops at most 200 owners and the next comes at least 1 s later, so passivation is capped at
  200/s. Before F218 the 5-minute sweep stopped all that were due, bounding the owners at about rate × 7 min. Now, at a
  touch rate r > 200/s, owners grow by (r − 200)/s. Each 1-s sweep sorts the whole `activity` map
  (`toVector.sortBy`) on the bus's thread.
- **Impact (estimated):** at r = 1 000/s, +800 owners/s, about 2.9 million an hour; at about 3 KB per idle owner
  (actor, its collections and the state), about 8–9 GB an hour, so heap exhaustion. Sorting a million boxed entries
  takes about 0.3–1 s, so long before that the bus spends most of each second sorting, and every command of the type on
  the node queues in its mailbox. The docs (ARCHITECTURE §4, book 17, SCALING §1 and §9) present the sweep as bounding
  the owners.
- **Validation:** a bench on a Local type: 1 000 new aggregates/s for 15 minutes. Sample the bus's children count and
  the heap: expected flat after about 7 minutes, observed growing linearly. Time the bus's `sweep` with a profiler.
- **Suggested fix:** bound the owners, not the stops: when due owners exceed a budget, stop them all, but pace or skip
  their passivation snapshots (for example, a snapshot only every N events, or writes on a small bounded executor).
  Keep the owners in an access-ordered `LinkedHashMap` so that a sweep walks the oldest without sorting. Make the clock
  injectable (F218 had no test for that reason).
- **Regression test:** with an injected clock, 1 000 new owners per simulated second for 20 simulated minutes: the
  owner count stays below rate × 7 min + `keepAliveLimit`, and no sweep sorts.
- **Continues:** F218 (a regression of its fix), F46.

### R2-BUS-3 — A materialization that succeeds in the background tells nobody: a later `Materialize` is answered "no such ephemeral aggregate here"
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Where:** `AggregateOwner.scala:837-840` (callers cleared at the timeout), `:826-831` (success: drop, answer only
  `under.callers`); `AggregateCommandBusActor.scala:235,316-321` (the draft is forgotten);
  `AggregateCommandBusActor.scala:309` (a later `Materialize` → `MaterializeRefused("no such ephemeral aggregate here")`).
  Book 20, lines 84–87: "Send `Materialize` again to hear the outcome."
- **Scenario:** the first answer takes longer than the command timeout (60 s): F208's own precondition. The caller is
  told "the outcome is not known… Materialize gets it". The background retry (2 s, 4 s, … 30 s) succeeds before the
  caller's own retry arrives.
- **Mechanism:** the success is answered only to the callers that joined after the timeout (usually none), then the bus
  forgets the draft. Nothing keeps the ephemeral id → persistent id mapping. The key that holds it in the persistent
  response state carries the bus's private start UUID, so the caller cannot look it up.
- **Impact:** the caller is told the draft does not exist although its state is stored as P1, which the caller cannot
  find. The likely user reaction (enter it again) makes duplicates. The book's promise fails.
- **Validation:** a probe holds the first `StoreMaterialized` past the timeout, then forwards it and the retries. After
  the retry's success, send `Materialize(id)`: observed `MaterializeRefused(id, "no such ephemeral aggregate here")`.
- **Suggested fix:** the ephemeral bus remembers materialized ids for `idleTime` (`id → persistentId`) and answers
  `Materialize` with `Materialized(id, P)` and commands with an answer that names P. Alternatively, it looks the key up
  in the persistent response state.
- **Regression test:** the validation above, expecting `Materialized(id, P1)`.
- **Continues:** F208.

### R2-BUS-4 — F197's type check covers restores only: event-history and archive reads return another type's events
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Where:** `AggregateCommandBusActor.scala:222-226` (`GetEventsForAggregate`, `GetEventsForAggregateForVersion` →
  `restorer.events`; `GetArchivedEvents` → `eventStoreState.readArchive`); `AggregateRestorer.scala:149-153`
  (`events` reads no type). The check is only in `AggregateRestorer.restore` (`:49-53`).
- **Scenario:** an id of type B reaches A's bus in an event-history query: an unvalidated client id, or a host bug.
  This is F197's precondition, whose fix says "its queries … are refused".
- **Mechanism:** both reads go to the store by id alone; every event deserializes as its own class, so B's whole
  history, with user ids and metadata envelopes, is answered `Success`.
- **Impact:** a read across types, and so possibly across organizations, of personal data. A host that authorizes by
  the type's projection would deny it; one that passes the id through would not.
- **Validation:** two contexts A and B; create B's X; ask A's bus `GetEventsForAggregate(X)`,
  `GetEventsForAggregateForVersion(X, 1)` and `GetArchivedEvents(X)`: expected a type-mismatch failure, observed
  B's events.
- **Suggested fix:** `events` and the archive read check `generationAndType` as `restore` does, under the same
  `aggregate-type-check` setting.
- **Regression test:** the validation above, plus that `aggregate-type-check = log` still answers them.
- **Continues:** F197.

### R2-BUS-5 — An erasure sent to another type's bus runs with the wrong context and is reported done
- **Severity:** High · **Likelihood:** Unlikely · **Confidence:** Confirmed (code)
- **Where:** `AggregateOwner.scala:265` (`failedRestore`: `Rewrite` runs `rewriteHistory` whatever made the restore fail,
  for F245); `:854-872`; `PostgresEventStoreState.scala:716-765` (no type check); `HistoryRewriter.scala:72-73`
  (`HistoryRewritten` with 0 events counts as "unchanged", and the report is `complete`).
- **Scenario:** a host sends `RewriteEverywhere(X, EraseCustomer(...))` (or runs `HistoryRewriter.rewrite`) through the
  bus of type A for an aggregate X of type B: a wrong id-to-type mapping in the host's erasure job.
- **Mechanism:** the owner's restore fails with `AggregateTypeMismatchException` (F197), so it is in `failedRestore`,
  where F245 lets rewrites through so that erasure reaches unreadable aggregates. A's `historyChanges` match none of
  B's event classes, so the events stay. A's `metadataChanges` do apply to B's envelopes, a library type. The answer
  is `HistoryRewritten(X, 0, 0, …)`, or a count of envelopes only.
- **Impact:** a personal-data erasure that did not reach X's events, reported as complete, `unchanged` (the case
  `RewriteReport.complete` accepts). Nothing logs the type mismatch beyond the failed restore.
- **Validation:** contexts A and B, both with a history change for one customer id in their events. Create B's X with
  it, then run `new HistoryRewriter(busA, …).rewrite(X, change, user)`: expected `refused` with the type mismatch,
  observed `unchanged`, `complete = true`, and the value still in `events`.
- **Suggested fix:** in `failedRestore`, refuse `Rewrite` when the cause is a type mismatch (`RewriteRefused` naming
  both types). Keep F245's path for unreadable events only. Optionally, `rewriteEverywhere` checks the row's `type_id`.
- **Regression test:** the validation above, expecting `RewriteRefused` and `complete = false`.
- **Continues:** F197, F245.

### R2-BUS-6 — A passivation during a materialization makes the bus hold every message for the draft unanswered, and a held `Materialize` is then refused
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Where:** `AggregateOwner.scala:741` (F208: a stop waits for the materialization); `Owners.scala:53-73,92-101`
  (messages for a stopping owner kept, with no bound and no time limit); `AggregateCommandBusActor.scala:92`
  (an ephemeral bus has `keepAliveLimit = 200`, fixed); `:299-302` (each held message refreshes `lastUsed`, so the
  idle drop never comes while a client keeps trying).
- **Scenario:** the persistent side does not answer for more than about 2 minutes: a database outage, or a Sharded
  persistent type's hand-over (up to 90 s, SCALING §1). The node holds more than 200 drafts (the default `maxAggregates`
  is 100 000). A materializing draft goes untouched for 2 minutes, and the sweep, or a `PassivateAggregate`, stops it.
- **Mechanism:** the owner defers its stop until the outcome is known, but the bus already treats it as stopping:
  `GetAggregate`, commands and `Materialize` for the draft are appended to `keptForStopping` and get no answer. F208
  promises "a `Materialize` meanwhile joins it" and commands "answered `CommandRejected`". On success the bus forgets
  the draft. The owner then stops, and the held messages go to a new owner, which restores an empty aggregate (version
  0): a held `Materialize` is answered `MaterializeRefused("no event at version 0")`, commands `CommandHandlingError`,
  and `GetAggregate` an empty aggregate. That new owner is never dropped as a draft.
- **Impact:** clients hang and time out during the outage. Their retries pile up in the bus's memory without a bound.
  After recovery the persistent id is answered to nobody, and the clients are told the materialization was refused
  (see R2-BUS-3).
- **Validation:** an ephemeral bus whose `persistentBus` probe never answers; `Materialize`; `PassivateAggregate(id)`;
  `GetAggregate(id)`: expected an answer, observed none. Let the probe answer success, then check the held
  `Materialize`'s answer.
- **Suggested fix:** a draft under materialization is not stopped for room. The bus skips it in the sweep, or the owner
  answers a stop with "deferred" and the bus releases the kept messages to it. Alternatively, ephemeral buses do not
  passivate drafts at all: their events are in memory anyway.
- **Regression test:** the validation above, expecting `CommandRejected` for commands, the frozen state for
  `GetAggregate`, and `Materialized(id, P1)` for the held `Materialize`.
- **Continues:** F208.

### R2-BUS-7 — A step lowered after a pool was drawn still gives the same ids twice
- **Severity:** Medium · **Likelihood:** Unlikely · **Confidence:** Confirmed (code; nextval's use of the new increment
  is PostgreSQL's documented behaviour). The race inside one draw is Strongly suspected.
- **Where:** `PostgresUidGenerator.scala:148-157`; `UidGeneratorSchemaInitializer.scala:276-281`.
- **Scenario:** a step lowered from 10 000 to s: by a DBA, or by a later version with smaller pools running next to
  1.0.1 nodes. Each start of either version then alters the step again, since 1.0.1 raises any step below 10 000.
- **Mechanism:** node A drew [v, v + 10 000) at the old step. After `ALTER … INCREMENT BY s`, the next `NEXTVAL`
  returns v + s, and node B's pools [v + s, …) overlap A's until v + 10 000. F204 sizes each pool by the step read
  with the draw, which protects pools drawn after the change, not the one drawn before it. Also, the step comes from
  the `pg_sequences` catalog snapshot of the statement, while `NEXTVAL` uses the increment in effect: a concurrent
  ALTER can pair an old, larger step with a new, smaller increment.
- **Impact:** duplicate aggregate ids (creations conflict), duplicate command ids (the owner's `pending` map
  collides: one caller gets another's answer, the other none), and duplicate saga ids (`sagaId|step` keys collide, so
  a step is answered with another saga's response and not applied). F204 claims "a step lowered meanwhile gives no id
  twice".
- **Validation:** generators A and B; A draws a pool; `ALTER SEQUENCE commands_uids_seq INCREMENT BY 5000`; B draws: the
  pools overlap by 5 000 ids.
- **Suggested fix:** allocate with an explicit size per draw, so that no step change can overlap pools: a one-row
  counter updated with `RETURNING`, or `nextval` of a step-1 sequence of pool numbers times a fixed size. Until then,
  document that lowering a step needs a `setval` past `last_value + old step`.
- **Regression test:** the validation above, expecting disjoint pools.
- **Continues:** F204.

### R2-BUS-8 — Every command of a sharded type is serialized whole on the bus's thread to measure it, with no bound
- **Severity:** Low · **Likelihood:** Possible · **Confidence:** Confirmed (code); costs estimated
- **Where:** `AggregateCommandBusActor.scala:335-351` (`refused` → `tooLarge` for every command);
  `ClusterSerializer.scala:88-92` (`sizeOf` writes the whole JSON to a null stream; F227 bounded `toBinary`, not
  this).
- **Scenario:** high command rates on a Sharded type, large commands, or a hostile client that sends a very large
  command through a host endpoint without a body limit.
- **Mechanism:** the size check builds the command's full JSON string on the single bus actor's thread. It does so
  also when the owner is local, and once more if the command is then sent.
- **Impact (estimated):** about 1 ms per 100 KB command, which caps the type's bus at about 1 000 such commands/s per
  node; queries and acknowledgements of the type wait behind it. A 50 MB command holds the bus for about 0.5–1 s before
  it is refused.
- **Validation:** a bench on a Sharded type with 100 KB commands; profile the bus thread (`sizeOf` dominates).
- **Suggested fix:** measure with a counting stream that stops at the limit (as F227's `Bounded`); skip the check for
  commands below a cheap lower bound.
- **Regression test:** a 10 MB command is refused within a few ms (a timing bound), and `sizeOf` stops at the limit.

## Lesser items

1. `AggregateCommandBusActor.scala:152,299-302`: F209 fixed the keys, not the routing. Two ephemeral buses running at
   once with the same token (any nodes or types; P ≈ k²/2^30 for k buses, estimated) accept each other's misrouted
   draft ids (`ephemeralMessage` checks only the token and `holds`), so a misrouted query or command reaches another
   user's draft.
2. `AggregateOwner.scala:431`: since F252, `keyRefusal` computes `asDbKey` outside the `Try`. A key whose `asDbKey`
   throws (a null field in a deserialized command) escapes to `guarded`, which reloads the state and leaves the
   command pending until its timeout (60 s). In serial mode that blocks the aggregate's queue for as long. Before, the
   command was answered at once.
3. `AggregateCommandBusActor.scala:203-209,353-354` (Hypothesis): should `route` throw for the head of
   `waitingForIds`, F257's catch keeps it at the head. Every later message queues behind it, and each attempt takes
   ids. No deterministic throw was found.
4. `AggregateCommandBusActor.scala:62-63,185`: `EphemeralMode.token` and `ids` (for tests) are public and unchecked.
   Token 0 hands out id 0 (no id), and `ids` > 2^24 overlaps the next token's block.
5. `MessageSize.scala:21-24`, `ClusterSerializer.scala:73` (Strongly suspected): with `large-message-destinations` set,
   answers between the frame and the large frame (a `GetAggregate` state, a `CustomSuccessResponse`) are written whole
   and dropped by Artery for an ordinary destination, so the asker waits out its timeout. SCALING §6 says it "fails at
   once". F249 fixed only commands.
6. `PostgresUidGenerator.scala:150-151`: the per-draw step query filters on `current_schema()`. A sequence found
   through a later `search_path` entry reads NULL, and F204's check silently falls back to the cached size;
   `readSequenceStep` (`:173-185`) fails the start with `NoSuchElementException` instead.
7. `AggregateCommandBusActor.scala:91`: F224's retention counts from when a response was stored, not from the draft's
   last use. With a short `idleTime` (minutes), a client retry after it applies a keyed draft command twice.
8. `AggregateOwner.scala:75,835-841`: each materialization retry is a full first command on the persistent bus (an
   aggregate id, an owner, a key lookup), up to every 30 s until the idle drop: about 1 440 per frozen draft over 12 h
   (estimated).

## Fixes checked and found sound

- F197: restores, snapshots and commands are refused for another type, and `aggregate-type-check` fails safe on unknown
  values. Not covered: R2-BUS-4 and R2-BUS-5.
- F204: a used sequence below or past the range is refused at start, and the initializer never lowers the step.
  Residual: R2-BUS-7.
- F208: freezing, joining callers, retry with the same key, and a stale drop that ends it. Residuals: R2-BUS-1, -3, -6.
- F209: a random UUID per bus start in `MaterializationKey`; the key stays under 256 characters (about 66).
- F224: an ephemeral bus keeps its responses for `idleTime`.
- F236: an ephemeral first command that waited past the end of the block is refused, and the queue moves on.
- F242: a bare `StoreFirstEvent` is refused on any bus; `StoreMaterialized` is library-private.
- F249: commands of sharded types are checked against the ordinary frame.
- F252: keys over 256 characters are refused before running (it errs only toward refusing: UTF-16 units ≥ characters);
  URL-encoded owner names are valid Pekko path elements; `AggregateVersion` refuses negatives.
- F256: ephemeral owners get no outbox writer, and the memory store's `localTx` rolls back per aggregate.
- F257: `receive` catches what its work throws, a restart re-runs `preStart` (new token, block, timers), and the
  ephemeral event bus's name is unique per start. A restart waits for the old children, so owner names cannot collide.
- F185 (this area): `Ddl` checks before changing the response table and the sequences' step.
