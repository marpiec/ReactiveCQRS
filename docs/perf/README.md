# Performance reports

Each file here is the report of one run of the `bench` module, the end-to-end load driver of
the implementation plan (§7). `baseline-M1.md` is the reference that later milestones are
compared with; `baseline-M2.md` adds the delivery soak of M2; `baseline-M3.md` adds serial command mode and
restores from snapshots; `baseline-M4.md` measures the rebuild through managed projections (the builder) and
the projections' live groups, with every scenario's subscriptions in the database; `baseline-M5.md` the same
scenarios with the cluster libraries and flow control (F93). M5's high-load scenarios have reports of their own:
`load-M5.md` (10 000 commands at once, an aggregate of 50 000 events), `large-M5.md` (documents of 5 MB),
`flow-M5.md` (the soak with flow control), `heavy-M5.md` and `heavy-before-F127.md` (handlers that hold their
threads); `docs/SCALING.md` reads them. `baseline-M6.md` runs the M5 scenarios again at the M6 close (organizations,
the complete rewrite, ephemeral aggregates and F172 in the code). `jsonnames-M6.md` compares short JSON keys
(mpjsons `@jsonName`) with descriptive ones (§7.1), with its findings and a recommendation. `baseline-M7.md` runs
the same scenarios at the M7 close, on JDK 25; `m6-on-jdk25.md` runs the M6 tag's code the same day for comparison,
and `rebuild-check-M7.md` and `rebuild-check-M6.md` repeat the pair to check the rebuild's difference (F184).

## Running it

```bash
sbt "bench/run baseline-M5 soakAggregates=100000 scenarios=commands,contended,serial,rebuild,restore,soak,deliverySoak"
```

The name becomes the report file, `docs/perf/<name>.md`. Sizes can follow the name as
`key=value` arguments (`workers`, `commandsPerWorker`, `contendedCommandsPerWorker`,
`noteSize`, `rebuildAggregates`, `rebuildEventsPerAggregate`, `restoreSizes`,
`soakSeconds`, `soakAggregates`, `soakEventsPerAggregate`, `soakFailingSeconds`,
`restartCycles`, `burstCommands`, `longAggregateEvents`, `largeSize`, `largeCommands`, `heavyMillis`, and for
`jsonNames` `jsonRuns`, `jsonCalls`, `jsonCommands`, `jsonRestoreEvents`, `jsonRebuildEvents`, `jsonDocuments`), and
`scenarios=commands,contended,serial,rebuild,restore,soak,deliverySoak,burst,long,large,heavy` runs only those (all
by default; the baselines run the first seven); `jsonNames` runs only when listed
(`sbt "bench/run jsonnames-M6 scenarios=jsonNames"`, about 30 minutes), and so does `restoreThreads`, a
final-state build on 1, 2, 4 and 8 threads over `rebuildAggregates` counters of `rebuildEventsPerAggregate` events
(CR-8, `sbt "bench/run restore-threads-1.0.3 scenarios=restoreThreads rebuildAggregates=10000 rebuildEventsPerAggregate=50"`).
`d96-before.md` (1.0.4, commit order) and `d96-after.md` (D96, a type at a time, each aggregate whole) run
`scenarios=rebuild,restoreThreads` with the same sizes, the same night, to compare the two builders.
`d97.md` repeats them with D97 (an aggregate read once, or a part at a time); its projections take events or states, not both, so
the one read does not show there; the heap peak does.
For example a quick check:

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
| Serial mode (M3, R8) | The two command measures again with `CommandProcessing.Serial`: commands of one aggregate wait for each other instead of conflicting |
| Projection rebuild | Since M4, two new managed projections built from the store when the system starts (the builder, D57–D62): time until every event is projected and until both are built and switched, throughput, peak heap. Before M4, the 0.12 replayer |
| Aggregate restore | A new owner reading 1 000 to 500 000 events: time, peak heap, statements; since M3 also the restore from the snapshot the owner writes when it stops (D30) |
| Memory over a soak run | Heap after GC before, right after a 60-second load (with the projections' backlog), and once the projections caught up and 20 s idle. The load itself varies widely between runs (it depends on how far the projections fall behind early, F93), so it is not a throughput measure |
| Delivery soak (M2) | Many counters, a tenth of whose events-based listener fails for a while and recovers within its retries. When every event is projected; the bus deliveries, outbox rows and recorded failures left (all zero when healthy); heap after GC before, after the load, after 20 s idle and after each of several restarts |
| Short JSON keys (§7.1) | The same order line event and projection document with descriptive keys and with one-character `@jsonName` keys: JSON and stored sizes, WAL, serialization time, the command path, a restore, a managed rebuild, one delivery between nodes, indexed document queries; three runs of each form, alternately, median and range |

## Reading the numbers

- Later scenarios run in a database the earlier ones filled: the full replay of 500 000 events
  took 3.3 s in the full M3 run and 1.95 s run alone (`scenarios=restore`). Compare a scenario
  with itself run the same way.
- All numbers come from one run on one machine (named in the report). They are for comparing
  runs on the same machine, not absolute figures; run the bench twice when a difference is
  close to the noise.
- The machine's state moves the numbers between days. M4's code run on the day of `baseline-M5`
  gave 9 788 commands/s without subscribers (10 691 in `baseline-M4`) and its first command after
  574 ms (502). When a difference matters, run the previous milestone's code the same day, from a
  worktree at its tag (`git -c core.longpaths=true worktree add <dir> v1.0.0-M4` on Windows).
- "Committed transactions" comes from `pg_stat_database` and also counts autocommit reads.
- "Peak heap" is the sum of the heap pools' peaks during the measured step, an upper bound.
- SQL statements are counted by ScalikeJDBC's completion listener in the bench's JVM.
