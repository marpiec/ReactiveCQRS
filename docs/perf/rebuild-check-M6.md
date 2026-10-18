# Performance: rebuild-check-M6

Measured on 2026-10-18 with `sbt "bench/run rebuild-check-M6 scenarios=commands,contended,serial,rebuild"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, in one run of the bench on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 1e83c07 (with uncommitted changes) |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | Java HotSpot(TM) 64-Bit Server VM 25.0.4.1, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 720 ms |
| first event seen by a projection | 768 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.8 / 5.7 / 8.1 / 17.6 ms |
| throughput | 4688 commands/s = events/s |
| SQL statements per command | 3.4 (including the two projections) |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 2722 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 493 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.6 / 2.8 / 3.8 / 11.6 ms |
| throughput | 8745 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 8.7 / 15.6 / 21.0 / 22.2 ms |
| throughput | 1679 commands/s |

## Commands on many aggregates, no subscribers, serial mode (R8)

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.3 / 2.3 / 3.0 / 16.1 ms |
| throughput | 10440 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate, serial mode (R8)

Each command waits for the one before it on the aggregate; none conflicts, none is re-run.

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 7.5 / 9.9 / 11.3 / 12.3 ms |
| throughput | 2044 commands/s |

## Projection rebuild (the M4 builder)

Two new managed projections of the counters, built from the store when they start (D57–D62): the events-based one in commit order, in chunks of 1 000 events applied in one transaction each; the state-only one from each counter's final state (D62). Times count from the start of the system.

| Measure | Value |
| --- | --- |
| events in the store | 112211 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| until the events-based projection had every event | 17603 ms |
| until both projections were built and switched | 17604 ms |
| rebuild throughput (events-based projection) | 6375 events/s |
| heap before / peak during | 17 MB / 155 MB |
| writing the history directly through the event store | 19286 events/s (batches of up to 1000 events per transaction) |
