# Performance: heavy-M5

Measured on 2026-10-17 with `sbt "bench/run heavy-M5"` (bench module, docs/IMPLEMENTATION-PLAN.md �7). All numbers are measured, from a single run on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 8414de5 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 200 commands each on many aggregates, 50 each on one; notes of 200 characters; rebuild data 1000 x 100 events; restore sizes 1000, 10000, 100000, 500000; soak 60 s; delivery soak 20000 x 10 events, failing 30 s, 5 restarts; large: 20 documents of 5000000 characters; heavy handlers of 5000 ms |

## Command handlers of 5000 ms

Each handler takes 5000 ms, standing in for one that takes minutes. Pekko's default dispatcher, on which the library runs command handlers and its buses, has 12 threads here; the command timeout is 60 s unless said otherwise.

| Case | Outcome |
| --- | --- |
| 16 at once, each holding its thread | SuccessResponse 16; the last answered after 10030 ms; a light command on another aggregate meanwhile: 4939 ms |
| 16 at once, each on a pool of the application's own (`AsyncCommandResult`) | SuccessResponse 16; the last answered after 5103 ms; a light command on another aggregate meanwhile: 8 ms |
| one on an aggregate that light commands change while it runs (concurrent mode) | SuccessResponse after 10248 ms; light commands meanwhile: SuccessResponse 25, latency p50 / max 6 / 4887 ms |
| one longer than its timeout (2500 ms) | CommandHandlingError after 2519 ms; the aggregate's version once the handler ended: 1 (1: nothing stored) |
