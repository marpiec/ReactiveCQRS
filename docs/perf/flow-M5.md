# Performance: flow-M5

Measured on 2026-10-17 with `sbt "bench/run flow-M5"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 5541e9a |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters |

## Memory over a soak run

Growth that stays after the idle period points at bookkeeping that is never released (KNOWN-ISSUES §C); the heap right after the load holds the projections' backlog (F93).

| Measure | Value |
| --- | --- |
| load | 671768 commands in 60 s from 16 callers, two projections |
| projections caught up | 88687 ms after the load |
| heap after GC: before / right after the load / caught up and 20 s idle | 16 MB / 128 MB / 22 MB |

## Delivery soak (M2)

Subscriptions in the database, as in every scenario since M4 (F3), with F29's bounded cache of 10 000 aggregates.

| Measure | Value |
| --- | --- |
| load | 20000 counters x 10 increments = 200000 events from 16 callers; the events-based listener fails on every tenth counter for the first 30 s |
| command throughput | 13139 commands/s |
| every event projected | 53530 ms after the load started |
| bus deliveries waiting / undelivered afterwards | 0 / 0 |
| outbox rows afterwards | 0 |
| recorded subscription failures | 0 (the failures lasted less than the retries) |
| heap after GC: before / after the load / after 20 s idle | 55 MB / 60 MB / 58 MB |
| heap after GC after each of 5 restarts | 23 MB, 23 MB, 23 MB, 23 MB, 23 MB |
