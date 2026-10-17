# Performance: baseline-M5

Measured on 2026-10-17 with `sbt "bench/run baseline-M5"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | c904613 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 100000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 668 ms |
| first event seen by a projection | 767 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.6 / 5.2 / 7.2 / 16.8 ms |
| throughput | 5250 commands/s = events/s |
| SQL statements per command | 3.3 (including the two projections) |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 2788 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 538 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.5 / 2.8 / 3.7 / 10.6 ms |
| throughput | 9123 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 8.4 / 13.1 / 13.6 / 15.6 ms |
| throughput | 1753 commands/s |

## Commands on many aggregates, no subscribers, serial mode (R8)

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.3 / 2.3 / 3.0 / 7.5 ms |
| throughput | 10698 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate, serial mode (R8)

Each command waits for the one before it on the aggregate; none conflicts, none is re-run.

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 7.4 / 10.5 / 13.3 / 14.5 ms |
| throughput | 2031 commands/s |

## Projection rebuild (the M4 builder)

Two new managed projections of the counters, built from the store when they start (D57–D62): the events-based one in commit order, in chunks of 1 000 events applied in one transaction each; the state-only one from each counter's final state (D62). Times count from the start of the system.

| Measure | Value |
| --- | --- |
| events in the store | 112211 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| until the events-based projection had every event | 17146 ms |
| until both projections were built and switched | 17176 ms |
| rebuild throughput (events-based projection) | 6544 events/s |
| heap before / peak during | 17 MB / 155 MB |
| writing the history directly through the event store | 29937 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new owner reads one aggregate's whole history; then it stops, writing a snapshot, and a new owner restores from that. Events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result | From a snapshot (D30) |
| --- | --- | --- | --- | --- | --- | --- |
| 1000 | 10 ms | 100272 | 17 MB / 19 MB | 3 | ok | 6 ms |
| 10000 | 55 ms | 182241 | 17 MB / 40 MB | 3 | ok | 3 ms |
| 100000 | 394 ms | 253612 | 17 MB / 92 MB | 3 | ok | 1 ms |
| 500000 | 3442 ms | 145265 | 17 MB / 150 MB | 3 | ok | 2 ms |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right after the load holds the projections' backlog (F93).

| Measure | Value |
| --- | --- |
| load | 695806 commands in 60 s from 16 callers, two projections |
| projections caught up | 96999 ms after the load |
| heap after GC: before / right after the load / caught up and 20 s idle | 17 MB / 136 MB / 23 MB |

## Delivery soak (M2)

Subscriptions in the database, as in every scenario since M4 (F3), with F29's bounded cache of 10 000 aggregates.

| Measure | Value |
| --- | --- |
| load | 100000 counters x 10 increments = 1000000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 10525 commands/s |
| every event projected | 256690 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 0 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 216 MB / 216 MB / 214 MB |
| heap after GC after each of 5 restarts | 26 MB, 26 MB, 26 MB, 26 MB, 26 MB |
