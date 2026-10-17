# Performance: baseline-M6

Measured on 2026-10-17 with `sbt "bench/run baseline-M6 soakAggregates=100000 scenarios=commands,contended,serial,rebuild,restore,soak,deliverySoak"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, in one run of the bench on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 67ded0b |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 100000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Startup to ready

A new system with two projections; the event bus waits for its subscribers before it publishes.

| Measure | Value |
| --- | --- |
| first command answered | 719 ms |
| first event seen by a projection | 793 ms |

## Commands on many aggregates, two projections

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 2.8 / 6.3 / 8.8 / 20.8 ms |
| throughput | 4658 commands/s = events/s |
| SQL statements per command | 3.4 (including the two projections) |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |
| projection throughput | 2698 events/s (events-based projection, from load start until it caught up) |
| projection lag after the last command | 499 ms |

## Commands on many aggregates, no subscribers

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.5 / 2.4 / 3.3 / 10.8 ms |
| throughput | 9156 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate

Conflicting commands are re-run with jittered back-off, at most 10 times (D37).

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 9.3 / 11.3 / 15.5 / 18.8 ms |
| throughput | 1640 commands/s |

## Commands on many aggregates, no subscribers, serial mode (R8)

Each command adds one event with a 200-character note.

| Measure | Value |
| --- | --- |
| commands | 3200 (16 callers, each on its own aggregate, one command at a time) |
| outcomes | SuccessResponse 3200 |
| latency p50 / p95 / p99 / max | 1.4 / 2.6 / 3.2 / 4.9 ms |
| throughput | 9573 commands/s = events/s |
| SQL statements per command | 1.0 |
| committed transactions per command | 1.0 (pg_stat_database; autocommit reads count too) |

## Concurrent commands on one aggregate, serial mode (R8)

Each command waits for the one before it on the aggregate; none conflicts, none is re-run.

| Measure | Value |
| --- | --- |
| commands | 800 (16 callers at once, all on one aggregate, `ConcurrentCommand`) |
| outcomes | SuccessResponse 800 |
| events stored | 800 |
| latency p50 / p95 / p99 / max | 7.8 / 11.0 / 21.7 / 22.1 ms |
| throughput | 1922 commands/s |

## Projection rebuild (the M4 builder)

Two new managed projections of the counters, built from the store when they start (D57–D62): the events-based one in commit order, in chunks of 1 000 events applied in one transaction each; the state-only one from each counter's final state (D62). Times count from the start of the system.

| Measure | Value |
| --- | --- |
| events in the store | 112211 (1000 aggregates of 100 events written for this, plus the earlier scenarios' events) |
| until the events-based projection had every event | 16760 ms |
| until both projections were built and switched | 16791 ms |
| rebuild throughput (events-based projection) | 6695 events/s |
| heap before / peak during | 17 MB / 154 MB |
| writing the history directly through the event store | 18701 events/s (batches of up to 1000 events per transaction) |

## Aggregate restore

A new owner reads one aggregate's whole history; then it stops, writing a snapshot, and a new owner restores from that. Events carry a 200-character note.

| Events | Restore time | Events/s | Heap before / peak | SQL statements | Result | From a snapshot (D30) |
| --- | --- | --- | --- | --- | --- | --- |
| 1000 | 11 ms | 89983 | 17 MB / 19 MB | 3 | ok | 6 ms |
| 10000 | 50 ms | 198972 | 17 MB / 40 MB | 3 | ok | 3 ms |
| 100000 | 384 ms | 260139 | 17 MB / 95 MB | 3 | ok | 2 ms |
| 500000 | 2074 ms | 241124 | 17 MB / 151 MB | 3 | ok | 3 ms |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right after the load holds the projections' backlog (F93).

| Measure | Value |
| --- | --- |
| load | 651861 commands in 60 s from 16 callers, two projections |
| projections caught up | 107083 ms after the load |
| heap after GC: before / right after the load / caught up and 20 s idle | 18 MB / 129 MB / 23 MB |

## Delivery soak (M2)

Subscriptions in the database, as in every scenario since M4 (F3), with F29's bounded cache of 10 000 aggregates.

| Measure | Value |
| --- | --- |
| load | 100000 counters x 10 increments = 1000000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 9005 commands/s |
| every event projected | 249971 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 0 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 213 MB / 216 MB / 216 MB |
| heap after GC after each of 5 restarts | 26 MB, 26 MB, 26 MB, 26 MB, 26 MB |
