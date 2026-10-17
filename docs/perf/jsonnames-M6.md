# Performance: jsonnames-M6

Measured on 2026-10-17 with `sbt "bench/run jsonnames-M6 scenarios=jsonNames"` (bench module, docs/IMPLEMENTATION-PLAN.md §7). All numbers are measured, in one run of the bench on the machine below; they are a baseline for comparing later runs on the same machine, not absolute figures.

| Environment | Value |
| --- | --- |
| commit | 98cd448 |
| OS | Windows 10 10.0, amd64 |
| CPU threads | 12 |
| JVM | OpenJDK 64-Bit Server VM 17.0.8, max heap 2048 MB |
| PostgreSQL | 17.4, same machine (jdbc:postgresql://localhost:5432/reactivecqrs) |
| sizes | 16 callers; 3 runs of each form; 1000000 calls per CPU measure; 100000 commands; restore of 50000 events; rebuild over 1000000 events; 100000 documents |

## Short JSON keys: sizes

The same order line event (eight fields with descriptive names of 8 to 21 characters and short values) and the same projection document (six fields) with their Scala names as JSON keys and with one-character keys (mpjsons `@jsonName`); the control is the bench counter's event, whose note outweighs its one key. Sizes do not vary between runs except the command path's, whose WAL includes the full-page images written after each checkpoint; those give the median and the range of 3 runs.

| Measure | Long keys | Short keys | Change |
| --- | --- | --- | --- |
| JSON of an order line event, characters (mean of 1 000 lines) | 239 | 104 | -56 % |
| JSON of the control event, a 200-character note, characters | 211 | 208 | -1 % |
| stored event JSON (`events.event`), bytes per event | 239.2 | 104.3 | -56 % |
| the same column as PostgreSQL keeps it (`pg_column_size`), bytes | 243.2 | 105.3 | -57 % |
| `events` with its indexes, growth per event over 1000000 events written directly | 421.3 | 280.0 | -34 % |
| WAL per 100 000 events written directly (events, keys, outbox), MB | 123.6 | 116.6 | -6 % |
| writing them directly through the event store, events/s (one run, batches of 1 000) | 22296 | 24207 | +9 % |
| `events` with its indexes, growth per event on the command path | 449.7 (449.7–451.5) | 308.2 (308.0–310.1) | -31 % |
| WAL per 100 000 commands, each writing one event (with its key, the outbox row and its deletion), MB | 147.8 (147.0–156.0) | 132.1 (131.2–141.1) | -11 % |
| 1 000 order line events in one delivery, serialized for another node, KiB | 500.2 | 373.2 | -25 % |
| of which each event's envelope besides its JSON, bytes | 273 | 278 | +2 % |
| order line events that fit one delivery part (half the 248 KiB frame) | 248 | 332 | +34 % |
| 1 000 control events in one delivery, KiB | 469.9 | 474.8 | +1 % |
| projection document (JSONB, `pg_column_size`), bytes | 203.0 | 113.2 | -44 % |
| projection table with its two path indexes, bytes per document | 312.0 | 220.5 | -29 % |

## Short JSON keys: time

Median of 3 runs, with the range in brackets; the two forms ran alternately. Serialization: one thread, 1000000 calls after a warm-up of 200000, with the store's own `MPJsons`. Command path: `ConcurrentCommand`s, each caller on 100 orders of its own, no subscribers. Restore: no snapshots. Rebuild: a projection keeping each order's last line and line count, each event reading and writing its order's document, built from the store in commit order (M4), from its start until it had every event. Documents: 1 000 lookups and 200 lists per run, the same values for both forms.

| Measure | Long keys | Short keys | Change of the median |
| --- | --- | --- | --- |
| serialize one order line event, µs | 0.397 (0.396–0.402) | 0.373 (0.372–0.404) | -6 % |
| deserialize one order line event, µs | 0.969 (0.937–1.005) | 0.448 (0.443–0.584) | -54 % |
| serialize the control event, µs | 0.371 (0.352–0.371) | 0.355 (0.350–0.427) | -4 % |
| deserialize the control event, µs | 0.368 (0.365–0.408) | 0.359 (0.353–0.362) | -2 % |
| command path: 100000 commands from 16 callers, commands/s | 14260 (12647–15131) | 16296 (12486–16635) | +14 % |
| command path latency p50, ms | 1.01 (0.91–1.14) | 0.88 (0.86–1.16) | -13 % |
| command path latency p95, ms | 1.64 (1.51–1.73) | 1.30 (1.30–1.73) | -21 % |
| command path latency p99, ms | 2.24 (2.17–2.52) | 1.90 (1.86–2.41) | -15 % |
| command path outcomes | SuccessResponse 100000 | SuccessResponse 100000 |  |
| restore of an order of 50000 events by a new owner, ms | 338 (328–397) | 231 (215–291) | -32 % |
| restore: heap peak, MB | 50 (49–94) | 50 (49–51) | -1 % |
| rebuild of an events-based managed projection over 1000000 events, events/s | 5902 (5745–6081) | 6067 (6024–6271) | +3 % |
| document lookup by an indexed path, 5 of 100000 documents: p50, ms | 0.09 (0.09–0.11) | 0.08 (0.08–0.09) | -3 % |
| document lookup: p95, ms | 0.12 (0.10–0.20) | 0.12 (0.09–0.12) | -5 % |
| document list by an indexed path, about 277 documents: p50, ms | 0.87 (0.86–0.89) | 0.66 (0.66–0.70) | -24 % |
| document list: p95, ms | 1.06 (0.96–1.11) | 0.86 (0.77–0.94) | -19 % |

## What the numbers say

Written after the run, from its tables above: every number below is measured on that machine,
unless it says *estimated*.

- **The JSON more than halves.** An order line event with descriptive keys is 239 characters, with
  one-character keys 104 (−56 %): keys were most of it. The control event, whose 200-character note
  outweighs its one key, changes by 1 %.
- **The table shrinks by a third.** `events` grows 421 bytes per event with long keys and 280 with
  short ones (−34 %; −31 % on the command path). The rest of a row (its header, ids, version, time,
  type ids and two index entries) is the same for both: about 175 bytes per event (estimated from
  the two growths minus the stored JSON).
- **The WAL shrinks less**: −6 % for direct writes and −11 % on the command path. A command's WAL is
  mostly the outbox row and its deletion, the event's key, the aggregate's version update and the
  index entries, which keys do not touch.
- **Reading gets twice as fast; writing hardly changes.** Deserializing an order line takes 0.97 µs
  with long keys and 0.45 µs with short ones (−54 %): mpjsons matches each key to its field, and
  long keys cost most of the parse. Serializing changes by 6 %. So what reads many events gains: a
  restore of 50 000 events takes 338 ms with long keys and 231 ms with short ones (−32 %, ranges
  apart), and a list of 277 documents 0.87 ms against 0.66 ms (−24 %).
- **What the database dominates does not change.** The command path ran at 14 260 and 16 296
  commands/s (medians), but the three runs of each overlap (12 647–15 131 against 12 486–16 635),
  so no difference is shown at this sample size. The rebuild, where every event reads and writes a
  document, runs at 5 902 and 6 067 events/s (+3 %, within the noise). Lookups of 5 documents take
  the same time: the index decides.
- **Between nodes the keys are not the main cost.** 1 000 order lines in one delivery take 500 KiB
  with long keys and 373 KiB with short ones (−25 %), but each event there also carries about
  275 bytes of envelope (class names, field names, version, user, time; F170), more than a short
  event's whole JSON. A delivery part holds 248 events with long keys and 332 with short ones; a
  compact envelope of about 30 bytes would hold about 470 long or 900 short ones (estimated).
- **Documents.** A projection document shrinks from 203 to 113 bytes of JSONB (−44 %), its table
  with two path indexes from 312 to 221 bytes per document (−29 %).

## Recommendation for Neula

**Do not annotate every class. Annotate the few classes that make most of the volume, and do it in
one planned release.** The saving is real for storage (a third of the events table and of
projection tables, for small events with descriptive keys) and for reading (restores and document
lists by about a third), and nil for commands, rebuilds and events whose values outweigh their keys.
At Neula's scale the storage part is the one that pays (for 100 000 000 such events, 421 against
280 bytes per event is about 42 GB against 28 GB, estimated from this bench).

1. **First, measure where the volume is.** Count stored bytes per event class on a copy of
   production (`SELECT event_type_id, count(*), sum(octet_length(event)) FROM events GROUP BY 1`);
   a handful of classes usually hold most of it. Only those, and the documents of the largest
   projections, are worth short keys.
2. **Fix the delivery envelope first (F170).** It costs more per event between nodes than short
   keys save, it needs no change in the domain classes and no data migration, and it helps every
   class at once.
3. **Then annotate the chosen classes in one release**, with the steps below.

### What short keys cost, and how to roll them out

- **A full stop, not a rolling upgrade.** A node without the annotations cannot read short keys.
  With the default `MPJsons`, stored events are read strictly (an unknown key fails), so an old node fails loudly; but the
  cluster serializer reads the application's classes tolerantly (D68, F135), so an old node would
  read a short-key event or command between nodes as fields of zero, null or `None`, silently.
  Stop every node, start the release that adds the annotations. Rolling upgrades would need mpjsons
  to read a short name before writing it (an alias that reads only), which 0.7.0 does not offer.
- **No way back without the annotations.** Once short keys are written, a rollback to a release
  without them cannot read them: keep the annotations in every later release.
- **Old data stays readable.** mpjsons reads the Scala field name as a fallback, so events and
  documents written before keep reading; a squash, a rewrite or a rebuild writes them anew with
  short keys.
- **Projections change with their documents.** A document's JSON paths change (`Seq("name")`
  becomes `Seq("n")`), so every query and index on them changes, and documents written before keep
  the long paths, which the new queries do not find. Give such a projection a new version in the
  same release, so it is rebuilt into new tables (D57), and give each changed index a new
  `uniqueId` (F171: the store keeps an index whose path changed under the same id; fixed in M7,
  where the store builds such an index again at start).
- **Readability.** Support and operators read stored JSON less easily; the annotated classes are
  the dictionary. Keep keys unique within a class.

### Not measured here

- mpjsons 0.7 has format levers that shrink JSON without renaming (`omitDefaults`,
  `NullPolicy.Omit`, `compactOptions`, `escapeSlash = false`); readers of 0.7 accept both forms,
  so they may suit rolling upgrades better. Worth a run of this bench before choosing.
- PostgreSQL compresses only rows over about 2 kB (TOAST), so small events are stored as written;
  these numbers hold for them, not for large events.
