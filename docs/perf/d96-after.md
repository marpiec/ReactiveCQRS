# Performance: d96-after

Measured on 2026-10-27 with `sbt "bench/run d96-after scenarios=rebuild,restoreThreads rebuildAggregates=10000 rebuildEventsPerAggregate=50"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, in one run of the bench on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | c25a06f5 (with uncommitted changes) |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | Java HotSpot(TM) 64-Bit Server VM 25.0.4.1, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 10000 x 50 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Projection rebuild (the M4 builder)

Two new managed projections of the counters, built from the store when they start (D57–D62): the events-based one a counter at a time, read on four threads (D96), in chunks of 1 000 events applied in one transaction each; the state-only one from each counter's final state (D62). Times count from the start of the system.

| Measure | Value |
| --- | --- |
| events in the store | 500000 (10000 aggregates of 50 events written for this, plus the earlier scenarios' events) |
| until the events-based projection had every event | 86968 ms |
| until both projections were built and switched | 87056 ms |
| rebuild throughput (events-based projection) | 5749 events/s |
| heap before / peak during | 14 MB / 211 MB |
| writing the history directly through the event store | 26513 events/s (batches of up to 1000 events per transaction) |

## Final-state build on several threads (CR-8)

A new managed state-only projection of the counters, built from each one's final state (D62), once per thread count: 20000 aggregates, 1000000 events in the store (10000 counters of 50 events written for this, plus the earlier scenarios'). No snapshots, so each restore reads every event of its counter. Times count from the start of the system, after a first build on one thread that warms the JVM and the caches.

| restoreThreads | until built and switched | aggregates restored | events read |
| --- | --- | --- | --- |
| 1 | 10148 ms | 1971 aggregates/s | 98537 events/s |
| 2 | 7950 ms | 2516 aggregates/s | 125793 events/s |
| 4 | 5566 ms | 3593 aggregates/s | 179661 events/s |
| 8 | 5238 ms | 3818 aggregates/s | 190917 events/s |
