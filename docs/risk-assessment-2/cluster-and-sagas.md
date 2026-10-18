# Working notes: cluster and sagas (second assessment)

- **Area:** the cluster and sagas (prefix R2-CLSG).
- **Scope:** `core/.../cluster/*` (ClusterSerializer with F227's bounded writer and F240's reader checks, MessageSize, Members,
  Singletons, Readiness), `core/.../saga/*` (SagaRunner/SagaRunning, SagaDefinition, Saga, SagaActor, SagaState,
  PostgresSagaState, PostgresSagaSchemaInitializer), the sharded owners (`Owners.scala`); followed into AggregateOwner
  (idempotency, F253), EventsBusActor (registration, Readiness), Ddl, Errors, MyActorLogging, mpjsons 0.7.0 sources
  (local Maven cache), `git show v1.0.0:` of the saga files, commits 3299713 and 7ac840b.
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT).
- Nothing was run. Every number is computed from the code or estimated, and says which.

## Findings

### R2-CLSG-1 — A compare-and-set miss always stops the saga, but the runner's own write whose acknowledgement was lost (or a dead runner's late write) produces the same miss: the saga then runs nowhere and its caller is never answered
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `SagaRunner.scala:336-343` (`case Success(_)`: `running -= id`, "this copy stops", no metric); `:344-346` (a
  failed write is retried as is); `PostgresSagaState.scala:52, 60, 64, 72` (`WHERE … AND phase = from AND step = from`).
  Untested: `SagaFaultsSpec.FaultySagaState` throws before the write, never after a commit.
- **Scenario A (lost acknowledgement):** a database failover or a connection reset while `StoreProgress(k → k+1)`,
  `FinishSaga` (unkeyed: a DELETE) or the REVERTING write commits. The server committed; the client gets an I/O error
  (the case F253 handles for owners and F206 handles for creates only).
- **Mechanism:** the retry runs the same UPDATE/DELETE with `from = (phase, k)`; the row is at `k+1` (or gone, or
  `finished`), so 0 rows: `Success(false)`. The runner takes it for a stale copy and stops. Only a load resumes a saga,
  and the runner loads only at its start (`preStart`).
- **Scenario B (hand-over):** the old runner's in-flight write (PoisonPill does not drain it; Left in F206's entry)
  commits after the new runner loaded the row at `k`. The new, live copy runs step `k` again and its write misses: the
  live copy stops; the dead one cannot go on.
- **Impact:**
  - A forward saga stops half-done (items reserved, not charged) until the singleton next starts, which may be weeks.
  - Its caller is never answered (answers are sent only on `Success(true)`, F206). For an unkeyed order the user
    retries and a second saga runs the whole process again.
  - A keyed repeat finds the row, `other` not running: it is retried for about 51 s (computed: `persistRetries`) and then
    answered `SagaFailureResponse("… does not run here")`, which the caller cannot tell from a business failure.
  - Visible only as a WARN; no metric, and `GetSagaStatus` no longer counts it.
- **Validation:** a `SagaState` that runs the real update and then throws (commit, then "connection lost"). The saga
  stops at `k+1`, the caller gets nothing, the row stays. Same for the unkeyed delete.
- **Suggested fix:** make each write recognisable as this copy's own: a `revision` (or writer token) column bumped by
  every write; on a 0-row result read the row back, and if it holds this copy's target (revision + 1, phase, step) treat
  the write as stored. When the row is unfinished and not this copy's, resume it from the stored row (as a load would)
  instead of stopping, unless another runner is known to run it. Count a metric for "copy stopped".
- **Regression test:** the validation goes on to the next step and the caller is answered; a dead runner's late write
  makes the live copy resume from the stored step, not stop.
- **Continues:** F206, F232.

### R2-CLSG-2 — F205's re-runs send each failed step's command again; with no idempotency key (Neula's 29 sagas while H3 is open) a late or unknown outcome is applied twice, up to five times
- **Severity** High · **Likelihood** Possible (Likely at Neula until H3 is done) · **Confidence** Confirmed (code);
  that no Neula command is idempotent is H3's own text (KNOWN-ISSUES:265).
- **Where:** `SagaRunner.scala:248-266` (`retried`: `RunAgain` with the same `SagaStep`); `SagaDefinition.scala:48, 63`
  (default `stepRetries` = 5 attempts) inherited by the deprecated `SagaActor` (`SagaActor.scala:15`).
- **Scenario:** after the upgrade to 1.0.1, a `SagaActor` saga's step asks a command without a key. The ask times out
  (a hand-over, a slow owner), or the owner answers `CommandHandlingError` after a commit whose outcome is unknown
  (F253), or the step's own mapping throws on an answer it did not expect after the command succeeded.
- **Mechanism:** in 1.0.0 the saga reverted; now the step runs again 1, 2, 4, 8 s later (computed) with a fresh,
  keyless command. Each copy is applied. A deterministic failure in the mapping runs the command five times.
- **Impact:** duplicated charges, reservations or created aggregates, then (after five failures) a revert that undoes
  only the earlier steps. Silent: a WARN per retry.
- **Validation:** a `SagaActor` saga whose step sends a keyless `ConcurrentCommand` and fails its future after the
  command succeeded: the command is applied five times.
- **Suggested fix:** default `stepRetries` to one attempt for `SagaActor` (the 1.0.0 behaviour) and for definitions that
  do not opt in; a release note that H3 must be done before steps are re-run; optionally a start-up WARN per runner
  whose `stepRetries` allows re-runs.
- **Regression test:** a `SagaActor` saga with a failing step reverts after one attempt.
- **Continues:** F205 (with H3).

### R2-CLSG-3 — For failures that come fast, the step retry window is about 15 s: a database failover longer than that reverts a step whose command committed
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code) for the window; Strongly suspected for
  the outage lengths (managed PostgreSQL failovers take tens of seconds, estimated).
- **Where:** `SagaDefinition.scala:63` (`Backoff(1 s, 30 s, 5)`: waits 1+2+4+8 = 15 s, computed); `SagaRunner.scala:266-269`;
  `AggregateOwner.scala:578-585` (F253: `CommandHandlingError` when the connection failed around the commit).
- **Scenario:** CheckoutSaga's ChargePayment is committed as the primary fails; the owner answers `CommandHandlingError`
  (outcome unknown). Every retry meets the database still down and fails within seconds (connection refused, or the
  driver's connect timeout, 10 s by default, estimated).
- **Mechanism:** after five attempts the saga stores REVERTING (retried until the database is back, F206) and releases
  the items. No retry ever reached the owner with the database up, so the stored response was never seen. Reverts do
  not compensate the failing step itself.
- **Impact:** F205's original inconsistency, for outages longer than the window: charged, items released, caller told
  "failed". The 60 s ask timeouts make the window long only for slow failures (hand-overs), not fast ones.
- **Validation:** a step whose first command commits and answers `CommandHandlingError`, then a database down for 30 s:
  the saga reverts and the charge stays.
- **Suggested fix:** treat a failed step future as "outcome unknown": retry it at the cap until a definitive answer
  (like F206's writes, with a metric), or bound it by a long deadline (minutes) rather than five quick attempts; or let
  the revert of a failed step also compensate that step (an idempotent no-op if it never applied). Book: say so.
- **Regression test:** the validation goes forward once the database returns.
- **Continues:** F205.

### R2-CLSG-4 — F240's reader refuses application values that are not case classes (plain classes, java.time values, arrays, Java enums), which 1.0.0 read and the writer still writes
- **Severity** Medium · **Likelihood** Possible (depends on the application's classes) · **Confidence** Confirmed (code).
- **Where:** `ClusterSerializer.scala:413-420` (`plainValue`: Product, String, Number, Boolean, Character, `scala.collection.`);
  `:250-254` (the writer writes any value as `Json`); `:402` (event classes in deliveries). mpjsons 0.7.0 reads plain
  classes (`BeanDeserializer`), `LocalDate`, `LocalDateTime`, `LocalTime`, `Duration`, `OffsetDateTime`, `ZonedDateTime`
  and arrays (`DeserializerFactoryImpl`). The book's four rules (17-cluster.md:385-396) do not require case classes.
- **Scenario:** a query answer `Option[LocalDate]`, a `Vector` of a plain class, an `Array[Byte]`, or an event that is
  a plain class, sent between nodes; also in the 1.0.0 → 1.0.1 rolling upgrade (old nodes read them, new ones refuse).
- **Mechanism:** the sender serializes without complaint; the receiver throws "names X, which it may not carry";
  Artery drops the message.
- **Impact:** asks time out with no marker; deliveries of such events to subscribers on other nodes fail at every
  re-delivery (the outbox poller reports them undelivered), so remote projections never get them.
- **Validation:** round-trip `Some(LocalDate.now)` and a plain-class event through `toBinary`/`fromBinary`: refused (1.0.0:
  read).
- **Suggested fix:** apply the same rule in the writer, so the sender fails loudly; widen the allow-list to what
  mpjsons reads by class (`java.time.*`, arrays of allowed values, enums, concrete classes outside `java.`/`javax.`/
  `sun.`/`jdk.` with a no-argument or field constructor); state the rule in chapter 17 and the CHANGELOG.
- **Regression test:** the round trips above, plus a refused class failing in `toBinary`.
- **Continues:** F240.

### R2-CLSG-5 — Keyed sagas' finished rows stay 30 days: the table grows with volume, every load and the daily removal read all rows of a name, and the last order (personal data) stays with them
- **Severity** Medium · **Likelihood** Possible (high-volume keyed orders) · **Confidence** Confirmed (code); sizes
  estimated.
- **Where:** `SagaRunner.scala:145, 360-364` (daily `removeFinished`); `PostgresSagaState.scala:67-79, 84` (the load:
  `name = ? AND phase <> 'finished'`; removal: `phase = 'finished' AND update_time < …`); indexes only on
  `(name, saga_id)` and partial `(name, order_key)` (`PostgresSagaSchemaInitializer.scala`).
- **Mechanism / impact (estimated):** 100 000 keyed sagas a day keep about 3 million rows; at about 1 KB of order and
  answer each, about 3 GB. Each runner start reads every row of the name to skip the finished ones (seconds), which
  delays every resume and widens R2-CLSG-7's window; the daily removal deletes a day's rows in one statement. Finished
  rows keep `saga_order` (the last internal order, with the customer's data) and the answer for 30 days, reached only
  by a space purge when `spaceOf` is defined, not by erasure (D76).
- **Validation:** `EXPLAIN` the load with a million finished rows of one name.
- **Suggested fix:** a partial index `(name, saga_id) WHERE phase <> 'finished'` and one on `(name, update_time) WHERE
  phase = 'finished'`; remove in batches; at finish, keep only the key and the answer (blank the order).
- **Regression test:** the finished row keeps no order; the load's plan uses the partial index.
- **Continues:** F207.

### R2-CLSG-6 — A rollback to 1.0.0 after keyed sagas finished leaves every pending saga of that name unresumed
- **Severity** Medium · **Likelihood** Unlikely (a rollback within 30 days of using keyed orders) · **Confidence**
  Confirmed (code; `git show v1.0.0:…/PostgresSagaState.scala`).
- **Where:** 1.0.0's `loadAllSagas` reads all rows of the name and calls `SagaPhase.byName` outside its `Try`; 1.0.0's
  `byName` throws for `finished`. 1.0.1 writes `finished` rows (`PostgresSagaState.scala:67-72`).
- **Mechanism / impact:** under 1.0.0 every load of that name throws and is retried for ever (ERROR each time); new
  orders run, but no stored saga resumes until roll-forward or a manual `DELETE … WHERE phase = 'finished'`.
- **Validation:** a 1.0.0 runner over a table with one `finished` row and one `continues` row: the second never runs.
- **Suggested fix:** a release note (rollback step: delete finished rows first), or ship 1.0.0.x with F252's `Try`.
- **Regression test:** none in code; the release note.
- **Continues:** F252, F151.

### R2-CLSG-7 — A load can run once more one step of a saga that ended while the load's query ran
- **Severity** Low (Medium for steps without keys, R2-CLSG-2) · **Likelihood** Unlikely; Possible with long loads
  (R2-CLSG-5) or an operator's `LoadPendingSagas` under traffic · **Confidence** Confirmed (code).
- **Where:** `SagaRunner.scala:187-195` (`resume` skips only sagas in `running`); `:167-170` (orders start before the
  first load). Commit 7ac840b removed the "orders wait for the first load" gate; `IMPLEMENTATION-PLAN.md:2666` still
  says they wait.
- **Mechanism:** a saga created before the load's snapshot ends (removed from `running`) before `Loaded` is handled;
  the load resumes it at its old step; the step runs again; its write then misses and the copy stops.
- **Impact:** one duplicate step execution (harmless with keyed commands; a duplicate without keys); a WARN.
- **Validation:** a state whose load blocks until a one-step saga started before it has finished: the step runs twice.
- **Suggested fix:** keep the ids that ended while a load is in flight and skip them in `Loaded`, or restore the
  first-load gate.
- **Regression test:** the validation runs the step once.
- **Continues:** F232.

### R2-CLSG-8 — F227 stops early only for library values and event lists; one large application value is still serialized whole
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `ClusterSerializer.scala:254` (`str(json.serialize(other))`), `:322-326` (`getBytes` of the whole string
  before `Bounded` refuses it), `:127-130`.
- **Mechanism:** a state, query answer or command is one `Json` value: mpjsons builds the whole JSON string, `str` copies
  it to UTF-8 bytes, and only then does `Bounded` throw. Peak about 3–5× the value's size (estimated: builder, string,
  bytes), against about 5× before the fix. The archive's "costs at most the limit's bytes" holds only for library
  products and event sequences.
- **Impact:** the large-state case F227 was about (a 20 MB state asked repeatedly) still allocates tens of MB on
  Artery's outbound lane per send.
- **Validation:** `toBinary` of a 50 MB state: allocation (ThreadMXBean) far above the limit.
- **Suggested fix:** in `str`, refuse a string whose `length` already exceeds the remaining room before `getBytes`
  (UTF-8 is at least one byte a char); for the JSON itself, a writer-bounded mpjsons call if one exists, else accept and
  correct the docs.
- **Regression test:** allocation below 2× the limit for a large application value.
- **Continues:** F227.

### R2-CLSG-9 — A keyed saga that failed keeps its failure as the answer for 30 days, so a natural key such as the book's `"pay-" + invoiceId` cannot be paid again after a transient failure
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `SagaRunner.scala:266-269, 277-283` (a failure stores REVERTING with its answer; the revert ends in
  `finishSaga`); `PostgresSagaState.scala:36-41` (a repeat gets the stored answer); `12-sagas.md:375` (natural keys).
- **Scenario:** the payment step fails five times in a short outage (R2-CLSG-3); the saga reverts with
  `SagaFailureResponse`. The user pays the invoice again: same key.
- **Impact:** every retry for 30 days gets the old failure at once; the invoice cannot be paid through the saga.
- **Validation:** keyed order, failing step, revert; send the order again after the database is back: old failure.
- **Suggested fix:** keep the answer of a reverted saga only for a short time, or release the key when the revert ends
  (store the answer in a separate "last outcome" only for in-flight repeats); or document keys per attempt (a client
  request id), not natural keys.
- **Regression test:** a repeat after a reverted saga starts a new saga (or the documented behaviour).
- **Continues:** F207.

## Lesser items
- `definition.spaceOf` runs in `write()` on the actor thread (`SagaRunner.scala:296-298`): if it throws, `receive`
  drops the message: the order is never answered, its saga id stays in `running`, and the orders behind it wait for
  the next message.
- Joined repeat that read the row before the REVERTING write committed but is handled after it (`:320-323`): joins a
  saga whose answer was already sent; the finish carries no answer, so it is never answered and its `joined` entry
  stays.
- A stored answer that no longer reads (response class changed within 30 days; strict mpjsons, `PostgresSagaState.scala:49`)
  makes every repeat fail its create 10 times and hear "The saga could not be stored" after about 51 s.
- A repeat with the same key but a different order gets the first order's answer; nothing compares them.
- `SagaRevertFailed(exceptions)` strings are logged verbatim (`SagaRunner.scala:278`), outside F191's redaction.
- F233: an ephemeral command bus's own `EventsBusActor` clears `Starting` and is done at once
  (`AggregateCommandBusActor.scala:155-158`); if it is constructed before the main bus's constructor runs, the node reads
  ready for a moment (actor constructors run asynchronously).
- `stepRetries` and `persistRetries` have no jitter (`Backoff.jitter = false`): sagas that failed together in an
  outage retry in lockstep, and with F206's endless retry each keeps one write queued on the 32-thread blocking
  dispatcher the owners run on.
- Readiness never becomes true on a node that starts no event bus (a node without buses): fail-safe, undocumented.

## Fixes checked and found sound
- F187/F188: the bus starts after `registrationTimeout`, keeps aggregates (not events) and re-publishes once the silent
  subscriber answers (as far as this area depends on it).
- F206: answers only after the write is stored; creates `ON CONFLICT DO NOTHING` with own-row recognition; writes never
  dropped while the runner lives (but see R2-CLSG-1).
- F207: unique partial index, join of a saga under way, stored answer for a finished one (but see R2-CLSG-5, -9).
- F232: compare-and-set on (phase, step) is monotonic (no ABA); `running` covers sagas from before their create
  (but see R2-CLSG-1, -7).
- F233: `Starting` until the first bus, not ready from the phase before service unbind.
- F240: classes are loaded without initialization; lengths are checked against the frame before allocation; element
  counts cannot allocate ahead of the bytes (but see R2-CLSG-4).
- F252: caller path and order key lengths refused at once; unknown phases left out of the load inside its `Try`.
- F263: the size marker is answered to its sender by the runner.
- F191 for the runner's own logs: causes go through `RedactingLog`; answers use `Errors.describe`.
