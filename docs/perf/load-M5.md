# Performance: load-M5

Measured on 2026-10-17 with `sbt "bench/run load-M5"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | cd23537 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts |

## 10000 commands at once

Every command is sent at once, with no client waiting for an answer before the next; no subscribers. The command timeout is 60 s; the blocking dispatcher has 32 threads and the JDBC pool 40 connections.

| Target | Outcomes | All answered after | Throughput | Latency p50 / p95 / p99 / max | Heap before / peak |
| --- | --- | --- | --- | --- | --- |
| 10000 new aggregates (FirstCommand) | SuccessResponse 10000 | 2342 ms | 4269 commands/s | 1797.3 / 2168.9 / 2203.8 / 2228.9 ms | 14 MB / 163 MB |
| 10000 existing aggregates, one command each | SuccessResponse 10000 | 891 ms | 11226 commands/s | 469.6 / 814.0 / 867.2 / 882.5 ms | 34 MB / 118 MB |
| one aggregate, concurrent mode (F99) | SuccessResponse 10000 | 3815 ms | 2621 commands/s | 2135.9 / 3628.5 / 3767.6 / 3803.8 ms | 35 MB / 106 MB |
| one aggregate, serial mode (R8, at most 1 000 waiting) | CommandRejected 8997, SuccessResponse 1003 | 594 ms | 16828 commands/s | 92.2 / 392.2 / 552.0 / 593.1 ms | 17 MB / 52 MB |

## An aggregate with 50000 events

Events carry a 200-character note. The event store of the bench has no limit of events per aggregate; the library's default is 10 000.

| Measure | Value |
| --- | --- |
| history written directly through the event store | 1293 ms (38675 events/s) |
| restore by a new owner, every event | 392 ms; version 50000; heap 17 MB / 52 MB |
| a command on it after the restore | 3 ms: SuccessResponse |
| every event in one answer (GetEventsForAggregate) | 264 ms; 50001 events; heap peak 168 MB |
| that answer, serialized for another node | 23573 KiB |
| a re-publication part of 1 000 events, serialized | 470 KiB |
| one event's delivery, serialized | 561 bytes |
| restore from its snapshot (D30) | 12 ms; version 50001 |
| events table with its indexes, per event | 382 bytes (81005 events, 29 MB) |
