# Performance: restore-threads-1.0.3

Measured on 2026-10-23 with `sbt "bench/run restore-threads-1.0.3 scenarios=restoreThreads rebuildAggregates=10000 rebuildEventsPerAggregate=50"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, in one run of the bench on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 7f699f46 (with uncommitted changes) |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | Java HotSpot(TM) 64-Bit Server VM 25.0.4.1, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 10000 x 50 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Final-state build on several threads (CR-8)

A new managed state-only projection of the counters, built from each one's final state (D62), once per thread count: 10000 aggregates, 500000 events in the store (10000 counters of 50 events written for this, plus the earlier scenarios'). No snapshots, so each restore reads every event of its counter. Times count from the start of the system, after a first build on one thread that warms the JVM and the caches.

| restoreThreads | until built and switched | aggregates restored | events read |
| --- | --- | --- | --- |
| 1 | 9280 ms | 1078 aggregates/s | 53880 events/s |
| 2 | 6007 ms | 1665 aggregates/s | 83234 events/s |
| 4 | 4658 ms | 2147 aggregates/s | 107341 events/s |
| 8 | 4227 ms | 2366 aggregates/s | 118284 events/s |
