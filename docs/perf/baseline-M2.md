# Performance: baseline-M2

Measured on 2026-10-16 with `sbt "bench/run baseline-M2"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 3b7eeec |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 100000 x 10 events, failing 30 s, 5 restarts |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 521 ms |
| first event seen by a projection | 552 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.4 / 4.7 / 7.0 / 10.0 ms |
| throughput | 5766 commands/s = events/s |
| SQL statements per command | 5.0 (including the two projections) |
| committed transactions per command | 3.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 1804 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 1219 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.3 / 2.8 / 5.8 / 15.9 ms |
| throughput | 9426 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 0.7 / 1.2 / 206.3 / 630.7 ms |
| throughput | 1205 commands/s |

## Projection rebuild (replay of all events)

The 0.12 replayer, into two new projections, over an in-memory bus state without a cursor; a PostgreSQL bus cursor that already moved past the events would stall it (F4).

| Measure | Value |
| --- | --- |
| events replayed | 107874 |
| events in the store | 107874 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| replay time | 34033 ms |
| until the events-based projection caught up | 34036 ms |
| rebuild throughput | 3169 events/s |
| heap before / peak during | 14 MB / 177 MB |
| writing the history directly through the event store | 21633 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new repository actor reads one aggregate's whole history (no snapshots before M3); events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result |
| --- | --- | --- | --- | --- | --- |
| 1000 | 13 ms | 75604 | 14 MB / 15 MB | 1 | ok |
| 10000 | 53 ms | 190401 | 14 MB / 31 MB | 1 | ok |
| 100000 | 361 ms | 276933 | 14 MB / 123 MB | 1 | ok |
| 500000 | 1991 ms | 251094 | 14 MB / 427 MB | 1 | ok |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C).

| Measure | Value |
| --- | --- |
| load | 917496 commands in 60 s from 16 callers, two projections; projections did not catch up within 120 s |
| heap after GC: before / after the load / after 20 s idle | 15 MB / 297 MB / 265 MB |

## Delivery soak (M2)

Subscriptions in the database (live mode, F29's bounded cache); the other scenarios keep the in-memory mode of the M1 baseline.

| Measure | Value |
| --- | --- |
| load | 100000 counters x 10 increments = 1000000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 12045 commands/s |
| every event projected | 572878 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 546704 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 248 MB / 44 MB / 45 MB |
| heap after GC after each of 5 restarts | 21 MB, 21 MB, 21 MB, 21 MB, 21 MB |
