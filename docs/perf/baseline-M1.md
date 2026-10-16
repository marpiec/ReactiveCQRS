# Performance: baseline-M1

Measured on 2026-10-16 with `sbt "bench/run baseline-M1"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 30388c9 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 470 ms |
| first event seen by a projection | 511 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.4 / 5.0 / 7.2 / 11.7 ms |
| throughput | 5458 commands/s = events/s |
| SQL statements per command | 6.0 (including the two projections) |
| committed transactions per command | 4.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 1698 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 1299 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.6 / 2.3 / 2.8 / 20.9 ms |
| throughput | 8995 commands/s = events/s |
| SQL statements per command | 2.0 |
| committed transactions per command | 2.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 0.6 / 1.3 / 191.4 / 680.8 ms |
| throughput | 1097 commands/s |

## Projection rebuild (replay of all events)

The 0.12 replayer, into two new projections, over an in-memory bus state without a cursor; a PostgreSQL bus cursor that already moved past the events would stall it (F4).

| Measure | Value |
| --- | --- |
| events replayed | 107874 |
| events in the store | 107874 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| replay time | 34133 ms |
| until the events-based projection caught up | 34139 ms |
| rebuild throughput | 3160 events/s |
| heap before / peak during | 14 MB / 210 MB |
| writing the history directly through the event store | 22041 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new repository actor reads one aggregate's whole history (no snapshots before M3); events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result |
| --- | --- | --- | --- | --- | --- |
| 1000 | 14 ms | 71120 | 14 MB / 15 MB | 2 | ok |
| 10000 | 46 ms | 219388 | 14 MB / 30 MB | 2 | ok |
| 100000 | 435 ms | 229846 | 14 MB / 132 MB | 2 | ok |
| 500000 | 2346 ms | 213133 | 14 MB / 452 MB | 2 | ok |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C).

| Measure | Value |
| --- | --- |
| load | 214546 commands in 60 s from 16 callers, two projections |
| heap after GC: before / after the load / after 20 s idle | 15 MB / 36 MB / 19 MB |
