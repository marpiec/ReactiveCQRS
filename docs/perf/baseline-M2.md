# Performance: baseline-M2

Measured on 2026-10-16 with `sbt "bench/run baseline-M2"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 8858190 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 100000 x 10 events, failing 30 s, 5 restarts |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 525 ms |
| first event seen by a projection | 560 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.2 / 4.5 / 6.2 / 10.1 ms |
| throughput | 6024 commands/s = events/s |
| SQL statements per command | 5.0 (including the two projections) |
| committed transactions per command | 3.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 1758 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 1289 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.2 / 2.7 / 7.7 / 16.7 ms |
| throughput | 9738 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 0.7 / 1.3 / 196.8 / 650.5 ms |
| throughput | 1166 commands/s |

## Projection rebuild (replay of all events)

The 0.12 replayer (replaced in M4), into two new projections that keep their subscriptions in memory until the replayer dumps them; since M2 the bus keeps no cursor that could stall it (F4).

| Measure | Value |
| --- | --- |
| events replayed | 107874 |
| events in the store | 107874 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| replay time | 33985 ms |
| until the events-based projection caught up | 33990 ms |
| rebuild throughput | 3174 events/s |
| heap before / peak during | 14 MB / 201 MB |
| writing the history directly through the event store | 21924 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new repository actor reads one aggregate's whole history (no snapshots before M3); events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result |
| --- | --- | --- | --- | --- | --- |
| 1000 | 12 ms | 83804 | 14 MB / 15 MB | 1 | ok |
| 10000 | 47 ms | 211899 | 14 MB / 31 MB | 1 | ok |
| 100000 | 355 ms | 281500 | 14 MB / 125 MB | 1 | ok |
| 500000 | 2158 ms | 231736 | 14 MB / 450 MB | 1 | ok |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right after the load holds the projections' backlog (F93).

| Measure | Value |
| --- | --- |
| load | 348474 commands in 60 s from 16 callers, two projections |
| projections caught up | 255184 ms after the load |
| heap after GC: before / right after the load / caught up and 20 s idle | 14 MB / 288 MB / 18 MB |

## Delivery soak (M2)

Subscriptions in the database (live mode, F29's bounded cache); the other scenarios keep the in-memory mode of the M1 baseline.

| Measure | Value |
| --- | --- |
| load | 100000 counters x 10 increments = 1000000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 10943 commands/s |
| every event projected | 602775 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 0 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 247 MB / 42 MB / 41 MB |
| heap after GC after each of 5 restarts | 21 MB, 21 MB, 21 MB, 21 MB, 21 MB |
