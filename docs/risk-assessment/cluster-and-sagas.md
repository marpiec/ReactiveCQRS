# Working notes: cluster and sagas

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (CLSG-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Read in full**
- `core/.../cluster/`:
  - `ClusterSerializer.scala` (Writer, Reader, Shape, tooLarge)
  - `Members.scala`
  - `MessageSize.scala`
  - `Readiness.scala` (with `ReadinessCheck`)
  - `Singletons.scala`
- `core/.../saga/`:
  - `SagaRunner.scala` (the whole `SagaRunning` state machine)
  - `SagaDefinition.scala`, `Saga.scala`, `SagaActor.scala`, `SagaState.scala`
  - `PostgresSagaState.scala`, `PostgresSagaSchemaInitializer.scala`

**Traced for context**
- `AggregateOwner`: attempt, runHandler, handlerFinished, persistOrAnswer, persist, conflict, start/timeout.
- `Idempotency`, `CommandResponseState` (`ON CONFLICT`, 30-day retention), `Command.scala` (`SagaStep`).
- `EventsBusActor`: registering, stash, Readiness.
- `Projections.start`/`retire`, how the ProjectionSupervisor routes messages, `ProjectionActor.receive`.
- `AggregateCommandBusActor`: ephemeral bus, `Members.track`.
- `SchemaLock`, `Backoff`, `IdPool`, core `reference.conf`.
- E2: `ClusterSetup`, `e2.conf`, `kubernetes.yaml`; E4 `CheckoutSaga`; `MultipleCartCreatorSaga`.
- Pekko 1.7.1 sources from the coursier cache:
  - `ClusterSingletonManager`: Start and Younger states.
  - `ClusterSingletonProxy`: identify and buffer.
  - `LocalActorRefProvider.tempPath`.
  - `Serialization.serializedActorPath`.
- Pekko Management 1.2.1 `reference.conf` (it has no default readiness checks).

**Docs**
- CLAUDE.md; ARCHITECTURE §4.9, §9, §12, §13; `design/M5-cluster.md`.
- SCALING §1, §2, §6, §8–§12; the book's chapters 12, 15–17.
- KNOWN-ISSUES: decisions, H3/H10/H12, reviews.
- Archive entries F17, F34–F36, F121, F129, F136, F137, F151, F178.

**Not examined**
- mpjsons 0.7.0 internals: I had no source, so its thread safety and how it builds classes named in JSON are taken from the docs.
- Neula's saga code and deployment.
- `RollingUpgradeSpec` and `ClusterSerializationSpec` were only grepped, not read.
- Nothing was run. Every number below is either computed from the code or quoted from the project's docs, and each says which.

## 2. Findings

### CLSG-1 — A release that adds a singleton stalls the rolling upgrade: the new nodes' event buses wait for a projection that can only start once the oldest node is upgraded
- **Rating:** Severity High · Likelihood Likely (any release that adds a projection registered on the buses) · Confidence Confirmed (code) for every step. The stuck rollout follows from E2's StatefulSet as written; not run.
- **Location:**
  - `Singletons.scala:17-21`
  - Pekko `ClusterSingletonManager.scala:642`: `goto(Younger)` while the oldest member exists, whether or not it runs a manager of that name.
  - Pekko `ClusterSingletonProxy.scala:296-300`: it identifies only at `membersByAge.headOption`.
  - `EventsBusActor.scala:191` (`waitFor`), `:232` (`case _ => stash()`), `:252` (it starts only when `awaitingRegistration.isEmpty`).
  - `Readiness.scala:24`
  - `examples/deploy/e2/kubernetes.yaml:66-69`: "replaces one pod at a time … each once the one before is ready".
- **Preconditions:**
  - Version N+1 starts a singleton name that N lacks:
    - a `Projections.start(...)` proxy registered with `RegisterSubscribers` on every bus, as the book says;
    - a new `SagaRunner.start`;
    - probably a new Sharded type too, since its shard coordinator is Pekko's own singleton.
  - The oldest node is restarted last (D66).
  - The rollout is gated on readiness (E2, book chapter 17).
- **Scenario:** three nodes run N. N+1 adds projection P. The StatefulSet replaces shop-2 first.
- **Mechanism:**
  1. shop-2's manager for P stays Younger, because the oldest member, shop-0, runs N and has no manager for P.
  2. The proxy cannot find `/user/P/singleton` on shop-0 and keeps buffering.
  3. shop-2's bus asks P every 5 s and never gets an answer. It stays in `registering`, stashes every `PublishEvents` without bound, and Readiness stays false.
  4. D66 places new shards only on shop-2. The poller's re-publications go back through those owners and are stashed again.
  5. Kubernetes waits for shop-2 to become Ready before it touches shop-1. That never happens, and shop-0 (whose departure would let P start) is never restarted.
- **Impact:**
  - The rollout hangs until an operator forces it (in effect a full restart).
  - Meanwhile every read model is stale for aggregates owned on shop-2, although their commands succeed.
  - The bus's heap grows.
  - Without a readiness gate there is the same lag for the whole rollout.
  - A new saga definition: its orders sit in the proxy buffer (1 000, then dropped at DEBUG); callers time out.
  - Detection: the WARN "Event bus waits for the subscriptions of: P-proxy" every 5 s.
- **Validation:** a variant of RollingUpgradeSpec in which the 2.0.0 nodes start and register an extra projection. Assert that they become ready and deliver events while the 1.0.0 oldest node runs. Today they never become ready.
- **Fix:**
  - The bus starts without a registered proxy that cannot answer while `Members.olderVersionsRun`. That proxy becomes a late registration (the F162 path: asked every tick, added when it answers, not counted by Readiness). Managed projections catch up from the store.
  - Mirror F132 (`retire` waits) for `start`.
  - Document that a new singleton, saga or sharded type starts only when the oldest node runs its release.
  - Trade-off: an unmanaged late subscriber misses earlier live events (F162 already warns about this).
- **Regression test:** the variant above, plus an `EventsBusActorSpec` case: "a proxy that never answers while an older member runs does not hold the bus".

### CLSG-2 — A step whose command times out counts as failed and the saga reverts, but the command can still be applied later, and nothing undoes it
- **Rating:** Severity High · Likelihood Possible · Confidence Confirmed (code). The late apply is the library's own documented behaviour (SCALING.md:90-99).
- **Location:**
  - `SagaRunner.scala:192-195`: `case Failure(e) => … respond(saga, SagaFailureResponse(...)); write(StoreProgress(saga.copy(phase = REVERTING, step = saga.step + 1)))`.
  - `AggregateOwner.scala:328-339`: the timeout counts from the command's arrival at the owner.
  - `SagaRunner.scala:74`: `Timeout(60.seconds)` for `SagaActor` sagas.
  - The book's sagas also use `Timeout(60.seconds)`: `12-sagas.md:239, 455, 495`.
  - `CheckoutSaga.scala:53-58`: `case other => Left(...)`.
- **Preconditions:** the step's ask timeout is no longer than the command timeout plus the delay before the command reaches its owner. Delays come from:
  - a shard hand-over (up to 90 s);
  - re-homing of a shard;
  - a Local owner being passivated;
  - a `FirstCommand` waiting for an id pool;
  - a command that finishes near the owner's 60 s.
- **Scenario:** during a rolling restart, ChargePayment (CheckoutSaga step 2, Sharded type) waits 70 s in the region while a busy shard is handed over. The saga's ask fails at 60 s.
- **Mechanism:**
  1. The runner answers `SagaFailureResponse`, stores REVERTING and runs `handleRevert(SagaStep(id,2))(Charge)`, which releases the items.
  2. At 70 s the charge reaches its owner and is applied: its own 60 s counts from arrival.
  3. Reverts only undo earlier steps, and their keys differ from the charge's key.
  4. The book's pattern also turns retryable answers into a revert: `CommandRejected` (the D67 write switch during a rollout), `CommandHandlingError`, and `EventHandlingError` (a transient database error, or a commit whose acknowledgement was lost).
- **Impact:** a silent inconsistency. The customer is charged, the items are released, and the caller is told the checkout failed. MultipleCartCreatorSaga leaves an orphan cart. No error is ever logged.
- **Validation:** a spec in which the command bus delays the command 2 s and the step's ask timeout is 1 s. Assert that the command is applied and that the revert also ran.
- **Fix:**
  - On a step failure, run the same step again (same `SagaStep`) with back-off, a bounded number of times (`SagaDefinition.stepRetries`), before answering and reverting. A command applied late then returns its stored response.
  - The book maps AskTimeout, `CommandRejected`, `CommandHandlingError` and `EventHandlingError` to "retry".
  - Step ask timeouts go above command timeout plus hand-over (more than 150 s).
  - Trade-off: failure answers come later.
- **Regression test:** the validation spec continues forward instead of reverting.

### CLSG-3 — A saga answered "failed" (or "could not be stored") can later run forward to success, days later, at the runner's next start
- **Rating:** Severity High · Likelihood Possible (the database unavailable for about 51 s or more while sagas run, for example a managed PostgreSQL failover) · Confidence Confirmed (code).
- **What is new against F35/F136:** the second outcome can contradict the first, and the resume time is unbounded.
- **Location:**
  - `SagaRunner.scala:189-195`: the answer is sent before the REVERTING write.
  - `SagaRunner.scala:246-252`: when retries run out, a CreateSaga is refused; anything else is only logged and the saga is dropped from memory.
  - `SagaRunner.scala:116-118`: sagas are loaded only in `preStart`.
  - `AggregateOwner.scala:448, 465-473, 500`: only persisted successes are recorded under the key. Refusals, rejections and timeouts are not.
  - `PostgresSagaState.scala:23-30`: INSERT without `ON CONFLICT`.
  - Write retries give up after 51.1 s (computed from `Backoff(100 ms, 30 s, 10)`).
- **Scenario:**
  1. A 70 s database failover. ChargePayment gets `EventHandlingError`, so the step returns `SagaFailed`.
  2. The caller is answered "refused". `StoreProgress(REVERTING)` fails 10 times and the saga is dropped from memory; its row still says `continues/2`.
  3. The user retries, and a new checkout succeeds.
  4. Weeks later, the oldest node is redeployed. The new runner re-runs step 2 with key `id|2`. No response was stored for the failure, so the charge is applied again (a double charge) and the success answer goes to a caller that no longer exists.
- **Variants:**
  - (a) A CreateSaga whose commit acknowledgement was lost. The retry hits the unique index 10 times, and the caller hears "The saga could not be stored" (the book says "Then nothing was done"). The row then runs at the next start.
  - (b) At a hand-over, the old runner's INSERT future (PoisonPill does not cancel it) commits after the new runner has loaded. The saga waits at step 0 until the next start, and its caller never got an answer.
  - (c) A resume more than 30 days later re-applies a step whose stored response has expired.
- **Impact:** a duplicated business operation, run out of context and contradicting what the user was told. The only trace is one ERROR line; there is no saga status query or metric.
- **Validation:** a `SagaState` that fails the REVERTING write 10 times, and a step that refuses once and then succeeds. Restart the runner: the saga completes forward.
- **Fix:**
  - Never drop a saga while the runner lives: retry at the 30 s cap, with an ERROR log and a metric.
  - Store REVERTING before answering a failure.
  - CreateSaga with `ON CONFLICT (name, saga_id) DO NOTHING`, treating an existing row as stored.
  - Use a termination message that drains in-flight writes before the runner stops.
  - Add `GetSagaStatus` and saga metrics.
  - Trade-off: failure answers wait for the database.
- **Regression test:** the scenario ends in REVERTING, and a duplicate-key CreateSaga retry counts as stored.

### CLSG-4 — Saga orders cannot be made idempotent, so a caller that retries during a singleton move starts the saga twice
- **Rating:** Severity High (for sagas that move money or stock) · Likelihood Possible · Confidence Confirmed (code). There is no key; the duplicate needs a client that retries.
- **Location:**
  - `SagaRunner.scala:131-134, 148-152`: every order gets a fresh `SagaId(sagaIds.take())`.
  - `Saga.scala:26-32`: `SagaOrder` has no key.
  - Pekko `ClusterSingletonProxy.scala:326-340`: it buffers up to 1 000 messages and delivers them later.
  - SCALING.md:531: 48–49 s from a kill to the singletons running elsewhere (measured by the project).
  - SCALING.md:538: "clients send a command again … for at least a minute".
- **Scenario:**
  1. The oldest node is killed. For about 49 s, orders wait in each node's proxy.
  2. A client times out at 30 s and retries through another node.
  3. When the runner starts, both proxies flush. Two saga ids give different `SagaStep` keys, so both orders reserve and charge.
- **Impact:** the multi-step process runs twice; both runs answer success. Nothing detects it.
- **Validation:** with the runner's node down, send the same order through two nodes; once the runner starts, there are two sagas.
- **Fix:**
  - An optional order key (`IdempotentSagaOrder.orderKey`), stored with a unique (name, order_key).
  - A repeat joins the running saga, or gets the stored final answer (kept for the response retention).
  - Trade-off: a column, an index, a clean-up.
- **Regression test:** a keyed order sent twice gives one saga and two identical answers.

### CLSG-5 — One saga can run twice at once in one runner, and the saga row has no compare-and-set to stop the stale copy
- **Rating:** Severity Medium · Likelihood Possible · Confidence Confirmed (code).
- **Location:**
  - `SagaRunner.scala:36`: `case object LoadPendingSagas` is public.
  - `SagaRunner.scala:96-114, 128-134, 140`: orders are created while the load is pending or being retried, and the load resumes every row of the name, including sagas already running here.
  - `PostgresSagaState.scala:34`: `UPDATE … WHERE name = ? AND saga_id = ?`, with no version or step check.
- **Triggers:**
  - (a) The first load attempt fails or is slow (the 5 s JDBC borrow wait while the new oldest node takes over every singleton) while buffered orders are being created.
  - (b) An operator sends `LoadPendingSagas` to revive sagas stranded by CLSG-3. Its scaladoc says "Loads and resumes the sagas stored unfinished".
  - (c) The old runner's write futures land after a hand-over.
  - (d) A split brain.
- **Mechanism:**
  - Two copies run the same step with the same key. Each writes without a check and each answers the caller.
  - Refusals are not stored (CLSG-3), so the copies can diverge: copy 1 stores REVERTING and releases the items, while copy 2 writes CONTINUES N+1 over it and charges.
  - Progress can also move backwards (a slow copy writes step 3 after the fast one wrote step 5).
- **Impact:** double answers, duplicated side effects, and a saga that is both reverted and completed. While H3 is open for Neula, every duplicate step applies its command twice.
- **Validation:**
  - A `SagaState` whose first `loadAllSagas` fails, and an order sent before the retry: the step runs twice.
  - Sending `LoadPendingSagas` during a slow step: the step runs twice.
- **Fix:**
  - Keep a `running: Set[SagaId]` and ignore `SagaPersisted` for those ids.
  - Stash orders until the first load succeeds.
  - Make `LoadPendingSagas` private.
  - Write with compare-and-set: `UPDATE/DELETE … AND step=? AND phase=?` (the expected previous values). A false result stops that copy.
- **Regression test:** both validation cases run each step once.

### CLSG-6 — Readiness says "ready" before the node has started, and stays ready during its shutdown
- **Rating:** Severity Medium · Likelihood Possible · Confidence Confirmed (code).
- **Location:**
  - `Readiness.scala:17, 24`: `waiting` starts empty and `ready = waiting.isEmpty`.
  - `EventsBusActor.scala:191`: the first `waitFor` happens in the bus's constructor.
  - E2 `startProcess` runs, in order: management start, then Bootstrap, then `startNode`, whose `initSchema()` calls run under the schema lock (up to 5 min) before the bus exists.
  - `e2.conf`: the only readiness check is ReactiveCQRS's; Pekko Management has none by default.
  - `kubernetes.yaml:118-123`: no `initialDelaySeconds`, period 5 s, failureThreshold 3.
- **Scenario:**
  - Pods waiting on the schema lock answer 200 on `/ready`.
  - One success makes a pod Ready, and three failures (15 s) are needed to undo it, so a rolling update moves on early.
  - No hook marks a leaving node not ready during its 90 s coordinated shutdown.
- **Impact:** the rollout outpaces the nodes, and traffic reaches nodes whose command buses do not exist yet.
- **Validation:** on a fresh system with no bus, `Readiness(system).ready` is true.
- **Fix:**
  - `waitFor("start")` when the extension is created, cleared by `Readiness(system).started()` once the host's actors exist.
  - A CoordinatedShutdown task in `before-service-unbind` that makes the node not ready.
  - Document `initialDelaySeconds`.
  - Trade-off: a host that never calls `started()` is never ready. This is fail-safe and needs a release note.
- **Regression test:** not ready until `started()`, and not ready again after `CoordinatedShutdown.run`.

### CLSG-7 — The saga runner writes whole orders and unexpected messages into the logs (GDPR)
- **Rating:** Severity Medium · Likelihood Likely · Confidence Confirmed (code).
- **Location:**
  - `SagaRunner.scala:204`: `log.error(s"Saga revert failed, … order = ${saga.order}, " + exceptions…)`.
  - `SagaRunner.scala:207`: the same, with the exception.
  - `SagaRunner.scala:193`: `log.error(e, …)`, where `e` is a `MatchError` whose message is the order's `toString` when `handleOrder` does not take the order.
  - `SagaRunner.scala:145`: `… + m` for any unexpected message.
- **Trigger:** any failed revert, including every refused first step of the test domain's saga (the book's `12-sagas.md:432` says that error is logged), and any order a definition does not take.
- **Impact:** names, addresses and amounts reach ERROR logs, out of reach of the complete rewrite and of a purge.
- **Validation:** a log-capturing run of the failing scenario in `SagasSpec`; grep the logs for the cart name.
- **Fix:** log the saga name, id, step, phase and the order's class only. For exceptions, log the class and a sanitized message, never `MatchError.getMessage`.
- **Regression test:** the log-capture spec finds no order field value.

### CLSG-8 — Every node start runs `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, which takes an ACCESS EXCLUSIVE lock with no timeout
- **Rating:** Severity Medium for `sagas`, High for the same pattern on `events` and `aggregates` (another area) · Likelihood Possible · Confidence Strongly suspected (PostgreSQL takes the lock before it checks IF NOT EXISTS; not run).
- **Location:**
  - `PostgresSagaSchemaInitializer.scala:18-20`
  - The same pattern in `PostgresEventStoreSchemaInitializer.scala:60, 134, 157, 162, 168` and `CommandResponseState.scala:93, 98`.
  - `SchemaLock.scala:39`: deliberately no `lock_timeout`.
- **Mechanism:**
  1. A rolling restart happens while a long transaction holds any lock on the table (pg_dump/export, `StoredDataCheck`, a long streaming read).
  2. The ALTER waits behind it, and every later statement on the table queues behind the ALTER.
  3. The starting node also holds the global schema lock, so other starting nodes give up after 5 minutes.
- **Impact:** saga writes stall cluster-wide; on `events`, every command persist stalls.
- **Validation:** keep `SELECT … FROM sagas` open in one transaction, run `initSchema()` in a second, then `updateSaga` in a third: it blocks.
- **Fix:** check the catalog first and ALTER only when the column is missing, or run the DDL in a transaction with `SET LOCAL lock_timeout` and retry.
- **Regression test:** the validation case does not block.

### CLSG-9 — An oversized message is fully serialized in memory before it is replaced by the marker
- **Rating:** Severity Medium · Likelihood Unlikely · Confidence Confirmed (code).
- **Location:** `ClusterSerializer.scala:78-81, 93-99`: `written(o)` goes into a `ByteArrayOutputStream`, then `toByteArray`, and only then is the size checked. The JSON is first built as a String (`:245`).
- **Scenario:** a 20 MB state, asked from another node and polled repeatedly.
- **Mechanism:** each ask holds about 5× the message size in memory at once (my estimate: UTF-16 string, UTF-8 bytes, stream buffer, copy). This happens on Artery's outbound lane, which delays every other message to that node.
- **Impact:** GC pressure or OutOfMemoryError, and latency spikes. All of it to send a 50-byte marker.
- **Validation:** `toBinary` of a 50 MB value allocates far more than the limit (measure allocation with ThreadMXBean).
- **Fix:** write through a counting stream that stops at limit + 1, then write the marker.
- **Regression test:** allocation stays below 2 × the limit, and the marker is returned.

## 3. Lesser findings
- **No deadline, status query or metric for sagas.** A step future that never completes leaves the saga invisible until the next start (`SagaRunner.scala:176-181`). R19 defines no saga metric.
- **Thundering herd at start.** All pending sagas are resumed at once (`:105-109`) with no concurrency bound. After CLSG-3 this floods the command buses just as the oldest node's shards and coordinator move, which feeds the timeouts of CLSG-2.
- **`spaceOf` runs outside any `Try`** (`:218-221`). If it throws, the runner restarts and the orders waiting for saga ids are dropped without an answer.
- **Permanent write failures are treated as transient:**
  - a caller path longer than `respond_to VARCHAR(256)` (`PostgresSagaSchemaInitializer.scala:28`) is refused only after 51 s;
  - an internal order mpjsons cannot write strands its saga, which repeats its step at every start.
- **The reader trusts the payload:**
  - ProductV, Module and Json load any class named in the bytes; there is no `isLibrary` check on read (`ClusterSerializer.scala:345-354`), and reading `MODULE$` runs object initializers;
  - `str()` allocates `readInt()` bytes unchecked (`:401-405`), so a corrupt frame can force about 2 GB, run out of memory, and trigger `jvm-exit-on-fatal-error`.
  - Out of the stated threat model, but E2's Artery port has neither TLS nor a NetworkPolicy.
- **Outbound serialization failures.** Artery drops the message with an ERROR log, and askers wait their full timeout; there is no marker like F121's.
- **New application classes during a rollout.** A new `SagaOrder` or query class fails to deserialize on the old oldest node and is dropped. The docs warn about this for commands only (§13.6).
- **SCALING §10's proxy signal is invisible.** It lists singleton-proxy "Buffer is full" as a signal, but Pekko logs those drops at DEBUG (`ClusterSingletonProxy.scala:326-336`).
- **Saga rows that never end:**
  - rows in the `error` phase are re-read and warned about at every start (`SagaRunner.scala:171-172`);
  - rows of a renamed or removed definition are never touched;
  - both keep personal data indefinitely, against "sagas end" in `M6-organizations.md:477`.
- **Neula's 29 `SagaActor` sagas** inherit a 60 s ask timeout (`SagaRunner.scala:74`), exactly CLSG-2's window, and their futures run on the default dispatcher (`:76`).
- **Purge during a running step.** A purge deletes a saga row while its step runs (`SpacePurge.scala:187`); that step's command can still write into the purged space. The "quiet organization" precondition is documented.

## 4. Known, re-checked
- **F121 is incomplete for requests.** The marker reaches the recipient, not the asker:
  - the saga runner logs "Received incorrect message" (`SagaRunner.scala:145`);
  - the supervisor forwards an oversized query to its instance (`Projections.scala:439`), which leaves it unhandled;
  - so askers wait their full timeout, and SCALING §6's "the asker fails at once" for a projection's query is true only for the answer.
- **F136 is worse than recorded.** The repeated answer can contradict the first, and the window includes "write retries exhausted", after which the saga resumes only at the next start (CLSG-3).
- **F137 is incomplete.** Ask temporary paths carry no UID, and Pekko's temp counter restarts at 0 (`ActorRefProvider.scala:428-441`). After the caller's node restarts with the same address (Compose hostnames, on-premise), a late answer can reach an unrelated ask with the same name, carrying another user's result.
- **F151 is incomplete.** `SagaPhase.byName` sits outside the `Try` (`PostgresSagaState.scala:53-54`). A row with an unknown phase fails every load of its name, retried forever.
- **D46 / the M5 table ("two runners … idempotent, H3").** H3 is still open for Neula, so every duplicate execution (CLSG-3, -4, -5, a split brain) applies its commands twice.

## 5. Checked and safe
- Step results and writes come back as messages; their futures use captured vals only. `sender()` is captured when the order arrives. `respond` runs on the actor's thread.
- A failed load starts again from scratch; `postRestart` does not reload (F75).
- Saga id pool: one ask at a time; a failed pool refuses the waiting orders without a restart.
- Unique (name, saga_id) index and checked row counts (F34, F36). The saga SQL binds every parameter.
- `SagaStep` keys are global: saga ids come from one sequence.
- `Backoff`: 9 waits, 51.1 s in total; the shift is capped, so no overflow.
- `Members`:
  - fields are volatile and updates are synchronized, keyed by `UniqueAddress`;
  - `alone` is false until a member is known (F131, F138);
  - a downed peer is counted until it is removed.
- Singleton hand-over: the new instance starts only after the old one terminated (`HandOverDone`). The project measured never two at once without a partition.
- `ClusterSerializer`:
  - a fresh Writer/Reader per call, `ClassValue` caches, a concurrent log throttle;
  - no library message declares `List[EventInfo]`, which is read back as a `Vector`;
  - library exceptions keep their class.
- `MessageSize`: limit, halves, a null stream for measuring.
- The F178 Readiness check works, and the ephemeral bus registers an empty list.
- `Singletons.stop` stops the manager too.

## 6. Open questions
- Does Neula gate rollouts on `ReadinessCheck`, and will 1.0.x releases add projections, sagas or sharded types (CLSG-1)? A variant of RollingUpgradeSpec would also settle the Pekko coordinator question for a new sharded type.
- Neula's saga definitions:
  - What ask timeouts do they use, and how do they map non-success answers?
  - Do their steps call external services?
  - What is the status of H3?
- How many sagas run at once, and how long do database failovers last (CLSG-3)? Do clients retry saga orders (CLSG-4)?
- Where do the logs go, and how long are they kept (CLSG-7)?
- Do nodes keep stable addresses (F137)?
- Do long transactions run on `events` or `sagas` during restarts (CLSG-8; the event store's owner should check their initializer)?
- Is Artery protected by TLS or a NetworkPolicy in Neula's GKE?
- mpjsons: is one shared instance thread-safe, and can it build JDK or library classes named in the JSON?
