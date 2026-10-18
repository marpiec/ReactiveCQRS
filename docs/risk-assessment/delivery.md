# Working notes: delivery

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (DLV-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Read in full:**
- `core/.../eventbus/EventsBusActor.scala` (all handlers: `registering`, `running`, `publish`, `pull`, `partsFor`, `finish`, `completeAll`, `subscriberStopped`, `removeSubscriber`, `completeFor`, `checkDeadlines`, `endDeliveries`, `sampleLevels`, `status`).
- `core/.../eventbus/FlowControl.scala`.
- `core/.../delivery/{OutboxPollerActor, RedeliveryPolicy, Republish, SequenceGate}.scala`.
- `docs/design/M2-delivery.md`.

**Followed as far as needed:**
- `AggregateOwner` (`publish`, `republish`, `delivered`, `caughtUp`, `failedRestore`) and `AggregateCommandBusActor` (`delivered`, `deletePermanently`, routing of `RepublishRequest`).
- `Owners.scala` (Local and Sharded acknowledgements).
- `PostgresEventStoreState` (`readWaitingInOutbox`, `readPart`, `oldestVersionToPublish`, `deletePublished`) and `OutboxDeletes`.
- The `events_to_publish` DDL.
- `ProjectionActor` (`receiveUpdate`, `receiveDelivery`, `drainFrom`, `requestMissingVersions`, `afterBatch`, `giveUp`).
- `Projections.ProjectionSupervisor` (bus protocol, `childName`, `terminated`) and `ProjectionRegistry` (`adopt`, `startBuild`).
- `cluster/{MessageSize, Singletons, Readiness}` and the `ClusterSerializer` writer.

**Docs:** ARCHITECTURE §5, §13.3, §13.4 and §14; SCALING (all); KNOWN-ISSUES decisions D3–D86. Archive entries checked: F1, F2, F4, F26, F37, F41, F42, F79, F83–F93, F110, F121–F123, F126, F128–F130, F141, F162, F164, F166, F169, F176, F182.

**Not examined:**
- Pekko internals (Artery outbound queue behaviour, shard allocation during a rollout).
- Real PostgreSQL plans of the outbox query.
- The rest of `ProjectionActor` (groups, chunks) and `SubscribableProjectionActor`.
- The `ClusterSerializer` reader.

Nothing was run: reasoning only, as instructed.

## 2. Findings

### DLV-1 — A subscriber stalled longer than the deadline gets about one more window every 10 minutes: the "mailbox ≤ window" bound does not hold
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `FlowControl.scala:116-123`: `expire` drops sent records older than the deadline.
  - `FlowControl.scala:41-45`: `admits`.
  - `EventsBusActor.scala:335` (each tick) and `:375` (admission of live and poller publications).
- **Preconditions:** A subscriber stops answering without stopping. Examples: a listener blocked on a DB lock or on an external call without a timeout, or a non-parallel projection whose actor thread is stuck. Traffic of its types continues.
- **Triggering scenario:** A projection blocks for 4 h while commands continue.
- **Failure mechanism:**
  1. The window fills (10 000 events / 64 MB unanswered).
  2. Ten minutes after each send, `expire` removes the record: `while (oldest.exists { case (_, sent) => now - sent.at >= deadlineNanos }) { s.sent.remove(key); take(s, sent) ...}`.
  3. The messages are still in the subscriber's mailbox, but `admits` sees room again and sends a new window: live events, the poller's full re-publications of the ended deliveries (duplicates of what is already queued), and pulls.
  4. Nothing ties the reopening to any answer from the subscriber.
- **Expected impact:**
  - The mailbox grows by about one window per deadline: up to 64 MB of event bytes per 10 min, about 384 MB/h, plus states (DLV-2). These are estimates from the defaults.
  - That is per bus, so ×N nodes for a singleton.
  - The node runs out of memory after hours.
  - SCALING §5/§9 promise "its mailbox holds at most its window", so operators are not warned. Stall lines are logged, but no signal shows mailbox size.
  - No data loss: the outbox keeps everything.
- **Validation:**
  - Bus with `DeliveryOptions(window = 10, deadline = 2.s, tickInterval = 100.millis)` and a TestProbe subscriber that never answers.
  - Publish one event of a new aggregate every 100 ms for 10 s.
  - Count messages received: about 50, against a bound of 10.
- **Recommended fix:** When records expire, keep their counts in a "presumed queued" bucket. Release it only when the subscriber answers anything again, or when it is Terminated or removed. A subscriber that answered nothing for a deadline is "stalled": hold everything (keys only) and pull once it answers.
  - Trade-off: a truly lost message keeps part of the window closed until the next answer, which comes quickly from any live subscriber.
- **Regression test:** The probe scenario above. Assert at most `window` unanswered messages are ever outstanding until the probe answers.

### DLV-2 — The byte window ignores the aggregate state that state subscriptions carry; on another node every message holds its own copy
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `EventsBusActor.scala:363`: `bytesOf` comes from `sizes`, which are the events' stored sizes only.
  - `:375`: `flow.admits(..., wanting.map(w => bytes(w._2)).sum)`.
  - `:414`: `flow.sent(..., part.size, bytes(part), now)`.
  - `:470-471`: `AggregateWithType(..., aggregateRoot, ...)` and `AggregateWithTypeAndEvents(..., aggregateRoot, events, ...)`.
  - `:448`: an `AggregatesDelivery` is one message with the state.
  - `ClusterSerializer.scala:241-245`: the application root is written as JSON per message.
- **Preconditions:** A cluster, a projection singleton with `AggregateListener` or `AggregateWithEventsListener` running on another node than the owners, states of tens of KB, and the projection lagging.
- **Triggering scenario:**
  - 50 KB states, small events, the projection behind on 2 remote buses.
  - Each bus keeps 10 000 messages in flight, about 20 MB of event bytes by its accounting.
  - The receiving node deserializes 10 000 × 50 KB ≈ 500 MB per bus (estimate). With 248 KiB states, about 2.4 GB per bus.
- **Failure mechanism:** Flow control counts events and their stored bytes. Each state-carrying message is serialized separately, so on the receiver each holds a full private copy of the root until its batch is applied (`Pending.aggregateRoot`).
- **Expected impact:** The singleton's node runs out of memory under lag, exactly when flow control is supposed to protect it. In one JVM the roots are shared by reference, so it shows only in a cluster. Hard to detect: `bus.behind` and `held` look normal.
- **Validation:** Use `TestCluster` with two nodes. The projection runs on A with a blocking `AggregateListener` and 100 KB states; send commands on B. Measure A's heap, or sum `MessageSize.of(message)` of what the probe receives, against `flow.status` bytes.
- **Recommended fix:** Count the state in both `admits` and `sent` for the state kinds:
  - For remote subscribers, `Measured.state` already measures it; use it.
  - Locally, use a per-type running estimate sampled every N publications.
  - At least document that `windowBytes` excludes states, and size `window` by count for state subscribers.
  - Trade-off: the CPU cost of measuring (an mpjsons write).
- **Regression test:** A bus spec with a remote-path stub (`onAnotherNode` true) and a 1 MB state, window bytes 4 MB. Assert at most 4 messages are outstanding.

### DLV-3 — While the bus waits for a subscriber's answer, it stashes every publication without bound; sharded owners on that node keep committing
- **Severity:** Medium · **Likelihood:** Possible
- **Confidence:** Confirmed (code) for the unbounded stash; Strongly suspected for shard placement onto the waiting node.
- **Location:**
  - `EventsBusActor.scala:202-233`, with `:232` `case _ => stash()`.
  - `AggregateOwner.scala:855-857`: owners `tell` `PublishEvents` whatever the bus state.
  - `Readiness.scala:58-62`: readiness only gates traffic that the host routes by it.
  - ARCHITECTURE §13.3 / D66: new shards go to the newest app-version.
- **Preconditions:** A registered subscriber never answers. Examples:
  - a projection refusing to start (F164 makes the bus "keep waiting" by design);
  - a supervisor whose instances keep failing to start;
  - a proxy whose singleton cannot run.
- **Triggering scenario:** In a rolling upgrade, the new node's projection answers `Status.Failure` forever. The node stays not ready, but it is a cluster member. Shards handed off by the old node that left are reallocated to it (newest app-version), and commands arrive through other nodes' regions.
- **Failure mechanism:** Every committed event and every poller re-publication is routed by another node's poller → region → owner on this node → `PublishEvents` to the waiting bus. Each is stashed with its events and state, without bound (default stash capacity is unbounded).
- **Expected impact:** Projections see nothing of those aggregates while the node waits. Heap grows with every commit and re-publication until OOM. Logs show only "Event bus waits for the subscriptions of: …" every 5 s. Without a readiness-gated balancer (a single node), HTTP commands keep feeding it too. No data loss: the outbox keeps the rows.
- **Validation:** In `EventsBusActorSpec`, register a probe that never answers. Send 100 000 `PublishEvents` with 10 KB events and check heap growth. In `TestCluster`, a node whose bus waits still commits sharded commands.
- **Recommended fix:** While registering, do not stash publications. Drop `PublishEvents` (the outbox is the queue) and record only their `(type, aggregateId)` in a bounded set. On start, send those owners `RepublishOutbox` (or have the poller run once). Keep stashing small control messages. Log at ERROR, and expose a metric, after N minutes of waiting.
  - Trade-off: first deliveries after start come from re-publication, not the stash.
- **Regression test:** A waiting bus receives 1 000 publications. Assert nothing is retained beyond the id set, and that after the answer each aggregate's owner is asked to re-publish.

### DLV-4 — The bus logs every `MessageFailed` at WARNING with the listener's exception text: unthrottled, and it can carry personal data
- **Severity:** Medium (GDPR / log flood) · **Likelihood:** Likely once any subscription fails on an active aggregate
- **Confidence:** Confirmed (code). That pgjdbc includes row values by default is Strongly suspected.
- **Location:**
  - `EventsBusActor.scala:474-476`: `failed.foreach(error => log.warning("Subscriber " + ... + " versions " + ... + ": " + error))`.
  - `ProjectionActor.scala:759`: the error is `e.getClass.getSimpleName + ": " + e.getMessage`.
  - `ProjectionActor.scala:586-587`: every later delivery of a failed `(aggregate, kind)` is answered `MessageFailed(... failed(key).error ...)`.
- **Preconditions:** A listener fails past its retries (D20a).
- **Triggering scenario:**
  - A unique-constraint violation in a listener. The pgjdbc message includes "Detail: Key (email)=(…) already exists" (its `logServerErrorDetail` default).
  - The aggregate keeps receiving events, and the poller re-publishes it at 3 and 6 minutes and every 30 minutes.
- **Failure mechanism:** One WARNING line with the full error per failed delivery, per event, forever. F92's limits (10 lines per tick) cover stalls and the poller, not this path. A listener bug failing a whole type gives one line per event of the type: 1 000 lines/s at 1 000 events/s.
- **Expected impact:**
  - Log floods.
  - Personal data copied into logs at every event of the aggregate, which erasure (D76) cannot reach.
  - The same text persists in `subscription_failures.error`. A grep finds that table only in `PostgresEventStoreState` and `SubscriptionsState`, so `HistoryRewriter` never touches it.
- **Validation:** A projection whose listener throws `new RuntimeException("email=a@b.c")`, with fast retries. Publish 1 000 more events of that aggregate and count bus WARN lines containing the text: 1 000.
- **Recommended fix:**
  - Log a failure once per `(subscriber, aggregate, kind)`, or at most N lines per tick with a count. The projection already logs it with its cause.
  - Log the exception class only, not the message.
  - Recommend `logServerErrorDetail=false` to hosts.
  - Store a class plus an error id in `subscription_failures`, or have erasure clear it.
- **Regression test:** The scenario above. Assert at most one bus log line, and that it contains no message text.

### DLV-5 — Every tick scans all deliveries several times and copies them twice; at millions of held keys the bus thread and GC become the bottleneck
- **Severity:** Medium · **Likelihood:** Possible
- **Confidence:** Confirmed (code) for the O(n) work; Strongly suspected for its size.
- **Location:**
  - The tick: `EventsBusActor.scala:330-338`.
  - `:559`: `deliveries.filter{...}.toSeq`.
  - `:575` → `:606`: `endDeliveries` → `deliveries.toSeq.foreach`.
  - `:581`: `deliveries.toSeq.filter` for stalls.
  - `:591`: the summary.
  - `:345`: `deliveries.valuesIterator.count`.
  - `FlowControl.scala:130`: `behind.filterInPlace`.
  - The same full copies in `subscriberStopped`, `removeSubscriber` and `completeFor` (`:519`, `:537`, `:542`).
- **Preconditions:** A slow subscriber holding many deliveries.
- **Triggering scenario:**
  - SCALING §5's own estimate is 10 000 events/s × 600 s = 6 M held keys. That counts live traffic only.
  - The poller adds up to 1 000 aggregates × 1 000 events = 1 M keys per poll: `AggregateOwner.scala:868-876` re-publishes from the oldest outbox version to the current one.
  - Pulls and gap fills after the deadline add keys too.
- **Failure mechanism:**
  - Each 5-s tick makes 3–4 passes over n entries and 1–2 `toSeq` copies. Each copy allocates one `Tuple2` plus one list cell per entry: about 290 MB at n = 6 M (estimate).
  - That is roughly 0.5–1 s of the bus's single thread per tick (estimate), plus GC churn next to about 1.2 GB of long-lived keys.
  - Publications and acks for every subscriber of the node wait behind it.
  - That lag produces more held keys: a feedback loop.
- **Expected impact:** Throughput collapse of all deliveries on the node and long GC pauses that also hit owners, during the backlogs flow control was meant to absorb. Detectable only by profiling: `bus.waiting` shows n.
- **Validation:**
  - Window 1 and a never-answering probe; publish 1, 2 and 4 M single-event publications of distinct aggregates.
  - Time the tick: send `GetDeliveryStatus` right after a `Tick` and measure the reply latency.
  - Record allocation with JFR. Expect linear growth.
- **Recommended fix:**
  - Use a `LinkedHashMap` for deliveries. `firstSent` equals insertion order, and updates keep the position, so expiry can pop from the head.
  - Use a separate queue ordered by `lastSent` for stall lines.
  - Keep incremental counters for the waiting and held gauges.
  - Keep a per-subscriber index for stop and remove.
  - Replace `toSeq` copies with `filterInPlace` or iterators.
  - Trade-off: more invariants to keep in step.
- **Regression test:** The same scenario with an assertion that tick time stays below a bound independent of n; or a unit test of the expiry structure.

### DLV-6 — Managed projection instances share actor names ("build-0-1"), and the bus keys its status, gauge and logs by name
- **Severity:** Medium (operations) · **Likelihood:** Likely for Neula after migration · **Confidence:** Confirmed (code)
- **Location:**
  - `Projections.scala:251-254`: `prefix + "-" + build + "-" + children`, with the counter per supervisor starting at 0.
  - `Projections.scala:325`: instances are named `childName("build", record.number)`.
  - `ProjectionRegistry.scala:138-139`: an adopted 0.12 projection is build 0.
  - `ProjectionRegistry.scala:145`: the first new build is 1.
  - `EventsBusActor.scala:347-349`: the gauge tag `"subscriber" -> subscriber.path.name`.
  - `EventsBusActor.scala:620-621`: `DeliveryStatus.behind` is a `Map` keyed by `path.name`.
  - Logs at `:475`, `:516`, `:563-565`, `:584-585`, `:594`, `:598-599`.
- **Preconditions:** Two or more managed projections on a node's bus.
- **Triggering scenario:** Every adopted projection's active instance is named "build-0-1", and every new projection's first instance "build-1-1".
- **Failure mechanism:**
  - The gauge `reactivecqrs.bus.behind{subscriber="build-0-1"}` is set once per such projection per tick in Set order. The last write wins, so the slow projection's backlog can be overwritten by another's 0.
  - `DeliveryStatus.behind` merges entries.
  - `stopped`, `Undelivered.subscriber` and every log line name "build-0-1" without the projection.
- **Expected impact:** Backlog alerts miss the slow projection, nondeterministically, and incident logs cannot be attributed.
- **Validation:** Start two managed projections, make one lag (window 1), and read `DeliveryStatus.behind` and `RecordingMetrics`: one key or series.
- **Recommended fix:** Name instances `<sanitized projection name>-build-<n>-<k>`, or have the bus label subscribers by an identity it is given (the build's subscriber name).
  - Trade-off: metric tag values and dashboards change.
- **Regression test:** The validation, asserting two distinct keys and series.

### DLV-7 — The poller's read has no timeout: one hung read stops every re-delivery in the cluster, silently
- **Severity:** Medium · **Likelihood:** Unlikely
- **Confidence:** Strongly suspected. It depends on the host's JDBC settings (pgjdbc `socketTimeout=0` and `tcpKeepAlive=false` by default).
- **Location:**
  - `OutboxPollerActor.scala:62`: `case Poll => if (!reading) read()`.
  - `OutboxPollerActor.scala:70-73`: `reading` is reset only by `Read(result)`.
  - Gauges are updated only on success (`:88-92`).
  - A grep finds no `statement_timeout` or socket timeout anywhere.
- **Preconditions:** A database failover or network black hole while the poll's query runs. Or a lock wait behind `VACUUM FULL` on `events_to_publish`/`aggregates` (D51 mentions it as the way to shrink files).
- **Failure mechanism:** The future never completes, so `reading` stays true and every later `Poll` is ignored. Being a singleton, it is the only re-delivery owner (D8). One blocking-dispatcher thread is lost.
- **Expected impact:** Lost deliveries, deadline-ended deliveries and given-up retries are never re-published until the singleton moves or the node restarts. No log line appears. The outbox gauges freeze at their last values (often 0, which looks healthy). `GetOutboxStatus` answers stale data.
- **Validation:** An `EventStoreState` stub whose `readWaitingInOutbox` blocks on a latch. Send `Poll` repeatedly and observe that nothing is logged or retried.
- **Recommended fix:**
  - Record when the read started. When a `Poll` finds it running past `max(5 × pollInterval, 5 min)`, log an ERROR each poll, count a metric, and start a new read tagged with a generation. Ignore stale results.
  - Run the query in a transaction with `SET LOCAL statement_timeout`.
  - Recommend `socketTimeout` and `tcpKeepAlive` in the JDBC URL (book, SCALING).
- **Regression test:** The blocking stub. Assert an error and a second read after the threshold.

### DLV-8 — Status answers grow without bound and fail between nodes exactly during incidents
- **Severity:** Low–Medium · **Likelihood:** Likely in an incident · **Confidence:** Confirmed (code). The per-entry size is an estimate.
- **Location:**
  - `OutboxPollerActor.scala:66`: `OutboxStatus(lastWaiting, memory.values.filter(_.gaveUp).map(_.waiting).toSeq)`, up to `maxTracked` = 100 000 entries.
  - `EventsBusActor.scala:181-182, 620`: `DeliveryStatus.undelivered`, up to 1 000 entries.
  - `MessageSize.scala:16-21`: the limit is 248 KiB.
  - `ClusterSerializer.scala:250-256`: library case classes are written with their class name and every field name, nested ones too.
- **Triggering scenario:** About 800 or more given-up aggregates (estimated at about 300 bytes per `WaitingInOutbox`). The operator asks the poller's proxy from a node other than the oldest (H10 tells hosts to use the proxy).
- **Failure mechanism:** The reply exceeds the limit. The serializer writes the too-large marker, and the asker gets `Status.Failure(MessageTooLargeException)`.
- **Expected impact:** The admin tool fails when many aggregates are undelivered. Gauges and the 10-line logs remain.
- **Validation:** `TestCluster` with the poller on node 1 and 2 000 given-up aggregates; ask through node 2's proxy.
- **Recommended fix:** Answer counts, plus the first N items (for example 100), with paging (`GetOutboxStatus(offset, limit)`); the same for `undelivered`.
- **Regression test:** The validation. Assert a successful answer with the total count.

### DLV-9 — Deliveries created by a re-publication to one subscriber never acknowledge the owner, and a later full publication does not adopt them
- **Severity:** Low–Medium · **Likelihood:** Possible (systematic after a lag beyond the deadline) · **Confidence:** Confirmed (code)
- **Location:**
  - `EventsBusActor.scala:392`: `if (onlyTo.isEmpty) Some(publisher) else None`.
  - `:368-369`: `wanted` is `deliveries.get(key).forall(_.waitsFor(...))`.
  - `:395-401`: existing deliveries keep their publisher.
  - `:498`: only deliveries with a publisher acknowledge.
  - `AggregateOwner.scala:883-887`: the outbox continuation needs its last version acked.
- **Triggering scenario:** A delivery ends at the deadline. Then a pull, or the subscriber's gap request, re-publishes it to S and creates a publisher-less delivery. The tick order makes this likely: pulls at `:336` run before deadlines at `:337`. A poller re-publication within the next 10 minutes then finds it.
- **Failure mechanism:**
  1. The full publication goes only to S, because the existing delivery waits for S only.
  2. When S acks, the delivery completes without a publisher: no `PublishEventsAck`.
  3. The owner's part-by-part continuation stops if that was the part's last version.
  4. After any lag beyond the deadline, every pulled or gap-filled ack is in this state.
- **Expected impact:** Rows stay after every subscriber has the events. Each touched aggregate needs another full poller round (given-up retries every 30 minutes, at most 1 000 per poll), with spurious "Undelivered" reports and duplicate traffic to all subscribers.
- **Validation:** In `EventsBusActorSpec`, with subscribers S and T and a short deadline:
  1. Publish v1; T acks.
  2. Let the deadline pass.
  3. S's `RequestRepublish` is answered with `PublishEvents(onlyTo = S)`.
  4. A full `PublishEvents(v1)` arrives.
  5. S acks: no `PublishEventsAck` reaches the publisher probe.
- **Recommended fix:** For a full publication, treat a publisher-less delivery as absent: set the publisher, and add all live subscriptions not yet answered. Better still, skip pulls of ranges whose deliveries have ended and let the poller own them.
  - Trade-off: S may get a duplicate.
- **Regression test:** The validation. Assert one `PublishEventsAck(v1)`.

### DLV-10 — Late first attempts are compressed: an aggregate first re-published when its rows are 10 or more minutes old is reported "Undelivered" 30 s later
- **Severity:** Low · **Likelihood:** Likely during backlog drains · **Confidence:** Confirmed (code)
- **Location:**
  - `RedeliveryPolicy.scala:63-64`: a new aggregate is timed from `w.since`.
  - `:52-55`: a postponed new aggregate is not remembered.
  - `:79-83`: after `attemptsMade = due` (2) comes `gaveUp` at the next poll when `elapsed >= giveUpAfter`.
- **Preconditions:** A row is first seen late. Causes:
  - postponement by the per-poll budget of 1 000;
  - being beyond `maxTracked`;
  - a committing node whose clock is minutes behind;
  - long commands (`event_time` is the command's start, D10).
- **Failure mechanism:** Poll k makes one re-publication (counting both attempts). Poll k+1, 30 s later, gives up if the rows are still there. With a backlog, rows are usually still held in the bus at that point.
- **Expected impact:** A stream of ERROR "Undelivered" lines and a rising given-up gauge for aggregates that had one chance 30 s earlier. They are then retried only every 30 minutes, with at most half the budget (F141): a slower drain and misleading alerts.
- **Validation:** In `RedeliveryPolicySpec`, a new aggregate with `since = now − 11 min` is republished at poll 1 and given up at poll 2 (+30 s).
- **Recommended fix:** Give up only when `elapsed ≥ giveUpAfter` and the last attempt was at least `giveUpAfter − attemptsAfter.last` ago, or remember postponed new aggregates.
- **Regression test:** The case above. Assert that it is not given up until 4 minutes after its re-publication.

## 3. Lesser findings

- **Dead entries in `asking` and `answeredWith`:** `asking` is never cleaned on `Terminated` (`EventsBusActor.scala:286-294, 334`). A late-registered subscriber that stops before answering is asked every tick forever (dead letters), and `answeredWith` keeps dead registered keys.
- **Quadratic indexing on parts:** `publish` indexes `events(i)` and `sizes(i)` (`EventsBusActor.scala:363`) on `List`s from `readPart` (`PostgresEventStoreState.scala:850`, `AggregateOwner.scala:874`). That is O(n²) per 1 000-event part on the bus thread, a few ms each (estimate).
- **Measuring serializes events twice:** `Measured` and `partsFor` (`EventsBusActor.scala:437-465`) mpjsons-serialize every event of a multi-event publication to a remote subscriber on the bus thread (`ClusterSerializer.sizeOf`, `:87-90`), before Artery serializes it again. Remote catch-up throughput is bounded by one thread.
- **Pulls answered on another node:** after a sharded owner moves, its pulls are answered through the other node's bus. The old bus re-pulls every `pullTimeout` until `heldAt` plus the deadline (`FlowControl.scala:124-130`), about 10 extra re-publications of up to 1 000 events, each bypassing the other bus's window. The code comment acknowledges the routing, not the cost.
- **Gap fills bypass the window:** gap fills skip the window (`EventsBusActor.scala:375`) and are repeated at 10 s, then doubling (`ProjectionActor.scala:728-729`), while the mailbox is longer. F83's amplification persists, reduced but unbounded by the window.
- **Pull reservations are guesses:** `DeliveryOptions.pullPartEvents`/`pullPartBytes` estimate owner parts that `OwnerSettings` fixes at 1 000 / 16 MB, and the command bus does not expose them. A host that lowers the options under-reserves pulls.
- **Clock skew:** the poller times attempts from `event_time`, written by the committing node's clock at command start (`RedeliveryPolicy.scala:61-64`, `PostgresEventStoreState.scala:827`). A node minutes behind gets duplicate re-publications and early give-ups; one minutes ahead gets late first attempts.
- **Outbox deletes (hypothesis):** `OutboxDeletes` (`OutboxDeletes.scala:34-66`) uses one thread per JVM and an unbounded queue, and a failed batch is dropped. A slow DB (bloat) grows the queue in memory, and dropped rows return as poller re-publications to all subscribers.
- **Artery queue (hypothesis):** Artery's `outbound-message-queue-size` (3 072 per association, from Pekko's reference.conf; not verified here) is below the 10 000-message window. A burst (`unstashAll`, pull answers) can drop deliveries with only DEBUG logging. Recovery is via gaps and the poller.
- **Unthrottled warning:** `routeToPublisher` warns per message (`EventsBusActor.scala:352-355`). A type without a command bus on the poller's node logs up to the budget each poll and never re-delivers.

## 4. Known, re-checked

- **F93 / SCALING §5 and §9:** "a projection's mailbox holds at most its window" is false for a subscriber stalled past the deadline (DLV-1) and for state-carrying messages to other nodes (DLV-2).
- **SCALING §5 key estimate:** "commit rate × 10 minutes" omits keys from the poller's re-publications (up to 1 000 aggregates × 1 000 events per poll) and from post-deadline pulls and gap fills. The tick's O(n) cost is not recorded (DLV-5).
- **F92:** the fix covered stall lines and the poller; the bus's per-`MessageFailed` warning remains unthrottled (DLV-4).
- **F122 / F141 / SCALING §4:** with one stopped, never-replaced subscriber whose aggregates fill the oldest 100 000 tracked, lost deliveries of every other aggregate, to any subscriber, are never read again until a restart or `RemoveSubscriber`. Gap requests cover only aggregates with new events. Meanwhile, 500 owner restores and full re-publications per poll run for nothing. This is broader than "wait until those are delivered". The grouping query also joins `aggregates` and runs back-to-back during a backlog, with no timeout (DLV-7).

## 5. Checked and safe

- **Large events:** an event larger than the 64 MB window is admitted when nothing is in flight. The first pull is always taken (`FlowControl.scala:43, 93`), and `readPart` returns at least one row (`p.n = 1`).
- **FlowControl accounting:** `sent`, `answered`, `expire`, `remove`, `pulls` and `republished` are balanced. Counters are Long; no drift or overflow found. `SentKey` matches the projection's acks, whose first version is `delivery.versions.head`.
- **Ack routing:** acks go only to the local command bus (the publisher); they never cross nodes. A sharded owner that moved loses only its outbox continuation, which the poller resumes.
- **Ack vs failure:** `MessageAck` and `MessageFailed` complete targets symmetrically. A permanent delete is acked only after every subscription, held ones included, has answered.
- **Ordering and holes:** per aggregate, ordering holds on the live path. Holds keep later events behind a held one, the gate absorbs overlaps, and F128's runs prevent holes.
- **Registration:** a repeated answer is deduplicated; a `Status.Failure` answer is asked again (F162). A late `RegisterSubscribers` is asked every tick.
- **Singleton moves:** stop, then rows kept, then proxy re-asked every tick, then replacement with `keepOutbox` (F110, F129, F176).
- **Pulls:** bounded by `maxPulls` and half the window, so there is no storm when a window frees.
- **Actor hygiene:** timers are cancelled in `postStop`. No `Await`, no actor state in future callbacks, and no late `sender()` in the bus or poller. The poller's query uses the `sql` interpolator.

## 6. Open questions

- During a rollout, does Pekko 1.7's default allocation place handed-off shards on a not-ready newest node (DLV-3)? Needs a `TestCluster` check.
- Neula's state sizes for state listeners, and whether its projections run as singletons on nodes other than the owners (DLV-2).
- Neula's JDBC URL settings (`socketTimeout`, `tcpKeepAlive`, `logServerErrorDetail`) (DLV-4, DLV-7).
- The plan and time of `readWaitingInOutbox` at about 10 M outbox rows and about 10 M aggregates (hash join scanning `aggregates`?).
- A profile of the bus tick at 1 M and 6 M deliveries (DLV-5).
- Whether Artery drops deliveries under burst with the library's serializer (lesser item).
