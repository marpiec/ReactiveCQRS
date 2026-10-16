# Performance reports

Each file here is the report of one run of the `bench` module, the end-to-end load driver of
the implementation plan (§7). `baseline-M1.md` is the reference that later milestones are
compared with.

## Running it

```bash
sbt "bench/run baseline-M2"
```

The name becomes the report file, `docs/perf/<name>.md`. Sizes can follow the name as
`key=value` arguments (`workers`, `commandsPerWorker`, `contendedCommandsPerWorker`,
`noteSize`, `rebuildAggregates`, `rebuildEventsPerAggregate`, `restoreSizes`,
`soakSeconds`), for example a quick check:

```bash
sbt "bench/run smoke workers=2 commandsPerWorker=10 restoreSizes=100,1000 soakSeconds=5"
```

The bench needs the local test database (`jdbc:postgresql://localhost:5432/reactivecqrs`,
user and password `reactivecqrs`; `-Dbench.url`, `-Dbench.user`, `-Dbench.password` change
it in `bench/build.sbt`). It works in a new schema `bench_<epoch>`, dropped at the end. A
full run takes about 10 minutes.

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
| Memory over a soak run | Heap after GC before, after a 60-second load, and after 20 s idle |

## Reading the numbers

- All numbers come from one run on one machine (named in the report). They are for comparing
  runs on the same machine, not absolute figures; run the bench twice when a difference is
  close to the noise.
- "Committed transactions" comes from `pg_stat_database` and also counts autocommit reads.
- "Peak heap" is the sum of the heap pools' peaks during the measured step, an upper bound.
- SQL statements are counted by ScalikeJDBC's completion listener in the bench's JVM.
