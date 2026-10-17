# Performance: large-M5

Measured on 2026-10-17 with `sbt "bench/run large-M5"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | ee1b140 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters |

## Commands and events of 5000000 characters

Each command carries a document of 5000000 random letters and digits, stored in one event (the event store allows such events for this class; the library's default is 100 000 characters).

| Measure | Value |
| --- | --- |
| 20 commands one after another, on one aggregate | SuccessResponse 20; latency p50 / max 90 / 164 ms; 31.8 MB/s of documents; heap 15 MB / 105 MB |
| every delivery answered (two projections), after the last command | 2 ms |
| 20 commands at once, each on its own aggregate | SuccessResponse 20; last answer after 890 ms; latency p50 / p95 / p99 / max 519.3 / 833.5 / 886.6 / 886.6 ms; heap 117 MB / 358 MB (the documents were made before) |
| every delivery answered (two projections), after the last answer | 0 ms |
| events table with its indexes and TOAST, per event | 5.0 MB |
| restore by a new owner of the aggregate of 20 such events | 517 ms; version 21; heap 117 MB / 323 MB |
| every event in one answer (GetEventsForAggregate) | 377 ms; 21 events; heap peak 324 MB |
| one event's delivery, serialized for another node | 4.8 MB, against a limit of 248 KiB |
