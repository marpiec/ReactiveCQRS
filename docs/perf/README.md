# Performance reports

Each file here is the report of one run of the `bench` module, the end-to-end load driver of
the implementation plan (§7). `baseline-M1.md` is the reference that later milestones are
compared with; `baseline-M2.md` adds the delivery soak of M2.

## Running it

```bash
sbt "bench/run baseline-M2"
```

The name becomes the report file, `docs/perf/<name>.md`. Sizes can follow the name as
`key=value` arguments (`workers`, `commandsPerWorker`, `contendedCommandsPerWorker`,
`noteSize`, `rebuildAggregates`, `rebuildEventsPerAggregate`, `restoreSizes`,
`soakSeconds`, `soakAggregates`, `soakEventsPerAggregate`, `soakFailingSeconds`,
`restartCycles`), and `scenarios=commands,contended,rebuild,restore,soak,deliverySoak` runs
only those; for example a quick check:

```bash
sbt "bench/run smoke workers=2 commandsPerWorker=10 restoreSizes=100,1000 soakSeconds=5"
```

The bench needs the local test database (`jdbc:postgresql://localhost:5432/reactivecqrs`,
user and password `reactivecqrs`; `-Dbench.url`, `-Dbench.user`, `-Dbench.password` change
it in `bench/build.sbt`). It works in a new schema `bench_<epoch>`, dropped at the end. A
full run takes about 10 minutes, plus the delivery soak (about 15 minutes for 1 000 000
events: `soakAggregates=100000 soakEventsPerAggregate=10`; the default is 20 000 x 10).

## What it measures

The bench uses its own counter aggregate, whose event handlers take constant time and whose
events carry a 200-character note, so the numbers show the library's costs, not a domain's.

| Section | Meaning |
|---|---|
| Startup to ready | From wiring a system with two projections to the first command reply and to the first event seen by a projection |
| Commands on many aggregates | 16 callers, each sending commands one at a time to its own aggregate; latency percentiles, throughput, SQL statements and committed transactions per command (with two projections, and with no subscribers) |
| Concurrent commands on one aggregate | 16 callers at once on one aggregate (`ConcurrentCommand`): retries, latency tail, throughput |
| Projection rebuild | The 0.12 replayer into two new projections: time, throughput, peak heap |
| Aggregate restore | A new repository actor reading 1 000 to 500 000 events: time, peak heap, statements |
| Memory over a soak run | Heap after GC before, right after a 60-second load (with the projections' backlog), and once the projections caught up and 20 s idle. The load itself varies widely between runs (it depends on how far the projections fall behind early, F93), so it is not a throughput measure |
| Delivery soak (M2) | Many counters, a tenth of whose events-based listener fails for a while and recovers within its retries; subscriptions in the database (live mode). When every event is projected; the bus deliveries, outbox rows and recorded failures left (all zero when healthy); heap after GC before, after the load, after 20 s idle and after each of several restarts |

## Reading the numbers

- All numbers come from one run on one machine (named in the report). They are for comparing
  runs on the same machine, not absolute figures; run the bench twice when a difference is
  close to the noise.
- "Committed transactions" comes from `pg_stat_database` and also counts autocommit reads.
- "Peak heap" is the sum of the heap pools' peaks during the measured step, an upper bound.
- SQL statements are counted by ScalikeJDBC's completion listener in the bench's JVM.
