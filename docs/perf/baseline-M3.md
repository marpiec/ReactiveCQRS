# Performance: baseline-M3

Measured on 2026-10-16 with `sbt "bench/run baseline-M3"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 64235a7 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 100000 x 10 events, failing 30 s, 5 restarts |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 787 ms |
| first event seen by a projection | 822 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.8 / 5.3 / 7.8 / 15.3 ms |
| throughput | 6112 commands/s = events/s |
| SQL statements per command | 5.0 (including the two projections) |
| committed transactions per command | 3.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 1630 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 1440 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.1 / 2.5 / 3.9 / 7.1 ms |
| throughput | 11151 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 0.5 / 1.1 / 380.1 / 1046.0 ms |
| throughput | 738 commands/s |

## Commands on many aggregates, no subscribers, serial mode (R8)

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 0.9 / 2.1 / 3.4 / 5.2 ms |
| throughput | 12793 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate, serial mode (R8)

Each command waits for the one before it on the aggregate; none conflicts, none is re-run.

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 6.0 / 9.4 / 11.2 / 11.3 ms |
| throughput | 2501 commands/s |

## Projection rebuild (replay of all events)

The 0.12 replayer (replaced in M4), into two new projections that keep their subscriptions in memory until the replayer dumps them; since M2 the bus keeps no cursor that could stall it (F4).

| Measure | Value |
| --- | --- |
| events replayed | 112211 |
| events in the store | 112211 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| replay time | 36857 ms |
| until the events-based projection caught up | 36863 ms |
| rebuild throughput | 3044 events/s |
| heap before / peak during | 14 MB / 167 MB |
| writing the history directly through the event store | 33814 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new owner reads one aggregate's whole history; then it stops, writing a snapshot, and a new owner restores from that. Events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result | From a snapshot (D30) |
| --- | --- | --- | --- | --- | --- | --- |
| 1000 | 17 ms | 58025 | 15 MB / 17 MB | 2 | ok | 7 ms |
| 10000 | 53 ms | 188774 | 15 MB / 38 MB | 2 | ok | 2 ms |
| 100000 | 397 ms | 252121 | 15 MB / 133 MB | 2 | ok | 4 ms |
| 500000 | 3271 ms | 152848 | 15 MB / 145 MB | 2 | ok | 3 ms |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right after the load holds the projections' backlog (F93).

| Measure | Value |
| --- | --- |
| load | 802826 commands in 60 s from 16 callers, two projections |
| projections caught up | 269129 ms after the load |
| heap after GC: before / right after the load / caught up and 20 s idle | 15 MB / 425 MB / 20 MB |

## Delivery soak (M2)

Subscriptions in the database (live mode, F29's bounded cache); the other scenarios keep the in-memory mode of the M1 baseline.

| Measure | Value |
| --- | --- |
| load | 100000 counters x 10 increments = 1000000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 11189 commands/s |
| every event projected | 662685 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 0 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 176 MB / 48 MB / 47 MB |
| heap after GC after each of 5 restarts | 21 MB, 21 MB, 21 MB, 21 MB, 21 MB |
