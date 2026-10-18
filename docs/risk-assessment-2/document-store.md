# Working notes: document store, configuration and shared utilities (second assessment)

- **Area:** R2-DOCS — the document store, configuration and shared utilities.
- **Scope:** `core/.../documentstore/*` (`PostgresDocumentStore` incl. the `space_id` index and
  `buildSpaceIndexesConcurrently`, `JsonPath`, `OutdatedTables`, `DocumentStoreCache`, `MemoryDocumentStore`),
  `core/.../util/*` (`SqlLogging`, `Errors`, `BlockingDispatcher`/`PoolCheck`, `ReadWatch`, `SchemaLock`, `Ddl`,
  `ActorLogging`, `AfterCommit`), `core/src/main/resources/reference.conf`, `core/.../types/TypesNamesState.scala`; calls
  followed into `Projections`, `ProjectionRegistry`, `ProjectionActor`, `AggregateCommandBusActor`.
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT) · read only, nothing run.
- **De-dup basis:** `docs/KNOWN-ISSUES.md`, `docs/KNOWN-ISSUES-ARCHIVE.md` (F16, F71, F171, F185, F186, F189–F191, F210,
  F211, F217, F219, F230, F247, F254, F266, F272, F273), `docs/RISK-ASSESSMENT.md` §3.1 and F189/F272,
  `docs/risk-assessment/document-store.md` (DOCS-1…6 and its lesser items are not repeated).

All numbers are estimates unless a source is named.

---

## Findings

### R2-DOCS-1 — Schema-lock waiters each hold a pooled connection, so the holder cannot get one: concurrent store set-ups at start fail in a loop
- **Severity** High · **Likelihood** Possible · **Confidence** Strongly suspected (the hold is in the code; the
  starvation follows from the pool's documented wait).
- **Where:**
  - `util/SchemaLock.scala:185-192`: `ConnectionPool.borrow()` *before* `acquire`; the connection is held while polling
    (`:200-206`, `pg_try_advisory_lock` every 50 ms, up to 5 min).
  - `documentstore/PostgresDocumentStore.scala:72-95`: `init()` runs in every store's constructor under `SchemaLock` and
    borrows further connections (`DB.autoCommit`, `Ddl.change`'s `DB.readOnly`/`DB.localTx`, `readExistingIndices`).
  - `rebuild/Projections.scala:353`: each build's instance is constructed (`Props(create(build))`, the store inside) on
    the blocking dispatcher, all supervisors starting together.
  - `util/BlockingDispatcher.scala:306`: `PoolCheck` accepts `connections >= threads`; `reference.conf:333` 32 threads;
    the book's chapter 2 (`02-setting-up.md:114`) and the test kit (`TestDatabase.scala:41,59`) use a pool of exactly 32.
- **Scenario:** a node starts with 32 or more managed projections (Neula has about 100 subscribers, H2) and a pool equal to
  the dispatcher (32), or with 40 (chapter 18) while 8 or more connections are used elsewhere (handlers, host threads,
  `OutboxDeletes`).
- **Mechanism:**
  1. 32 instance constructors run at once on the 32 threads; each borrows a connection and polls the advisory lock.
  2. The one that gets the lock calls `DB.autoCommit` for its catalog reads; the pool is empty, so it waits
     `connectionTimeoutMillis` (3–5 s) and its store's `init` throws; the instance fails to construct.
  3. Its thread is freed together with its connection and immediately runs the next queued constructor, which borrows that
     connection for `SchemaLock` before the new lock holder (polling at 50 ms) asks for it. The new holder fails the same way.
  4. A failed instance comes back after the supervisor's 10 s, so while 32 or more constructions are pending this repeats.
- **Impact:** projections fail to start, repeatedly; meanwhile all 32 blocking threads sleep in `SchemaLock`, so owners and
  every library JDBC call of the node wait (commands time out during the start). 31 waiters also poll the database about
  620 times a second (estimated). Even without starvation, about 100–300 store set-ups pass one lock with up to 50 ms of
  hand-over each: seconds of a fully occupied dispatcher at each start (estimated).
- **Validation:** pool `maxSize = 32`, dispatcher 32; start 40 managed projections in one `Projections` at once; watch for
  "Cannot get a connection, pool error Timeout waiting for idle object" from `init` and instances restarting every 10 s.
  Unit level: 33 threads run `SchemaLock { DB.autoCommit(_ => ()) }` concurrently with a pool of 32.
- **Suggested fix:** take a JVM-local lock (one `ReentrantLock` per process) before borrowing the advisory-lock connection,
  so at most one thread per process holds a connection while waiting; or run the work on the lock's own connection
  (`DB(connection)`) so the holder needs no second one. Raise `PoolCheck`'s threshold to threads + 1 and count the handler
  dispatcher's readers in the book. Longer term: no DDL in constructors (store set-up as an explicit step).
- **Regression test:** `SchemaLockSpec`: with a pool of N, N + 8 threads each run a store `init`; all succeed.
- **Continues:** F185 (waiters "each holding a thread and a connection") and F272 (the pool sizing rule).

### R2-DOCS-2 — Paging with an offset has no stable order on PostgreSQL; the memory store's tie-break hides it
- **Severity** Medium · **Likelihood** Likely · **Confidence** Strongly suspected (PostgreSQL documents that LIMIT/OFFSET
  without a total ORDER BY gives unpredictable subsets).
- **Where:** `PostgresDocumentStore.scala:271-277` (`orderAndPage`: `ORDER BY` only the declared sorts, no `id`; none when
  `sortBy` is empty); `DocumentStore.scala:111-112` (`TooManyDocumentsException` says "page with an offset");
  `docs/book/08-projections.md:421-422`; `MemoryDocumentStore.scala:51` (ties and unsorted queries ordered by key).
- **Scenario:** after F211 a query that found more than 100 000 documents fails; the host does what the message says and
  pages with `offset`/`limit`, unsorted or sorted on a non-unique field (a status, a date).
- **Mechanism:** without a total order each page is planned and run on its own. Unsorted, a sequential scan may start
  mid-table (`synchronize_seqscans`, on by default, for tables larger than a quarter of `shared_buffers`); sorted on ties,
  a top-N sort returns ties in any order. Pages overlap and leave gaps. Offset paging also reads `offset` rows per page,
  so walking N rows costs O(N²) (1 M rows in pages of 1 000: about 500 M rows read, estimated). The memory store sorts by
  key first, so a level-2 test of the same paging passes.
- **Impact:** silently missing or repeated documents in exports, bulk jobs and lists; quadratic cost on large models.
- **Validation:** 300 000 documents with 10 distinct `status` values; page by `SortAscText("status")`, limit 1 000, and
  count distinct ids: fewer than 300 000. Unsorted: run concurrent scans of the table while paging.
- **Suggested fix:** always append `id` as the last sort key (`ORDER BY …, id`) when there is a limit or an offset (or
  always), matching the memory store; document keyset paging (`id > lastId`) for large walks, or offer it
  (`ExpectedGreaterThanLongValue` on the key / a `findAfter(id, limit)`).
- **Regression test:** `PostgresDocumentStoreSpec`: pages over a sort with ties cover every id exactly once.
- **Continues:** F211 (its advice), F273 (the memory store's order is stricter than PostgreSQL's).

### R2-DOCS-3 — The F211 cap counts rows only after the driver has buffered and mpjsons has read all of them
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code); sizes estimated.
- **Where:** `PostgresDocumentStore.scala:280-283` (`run`: `.map(read).list()`, then `rows.size > maxRows`),
  `:291-298` (`findDocumentWithTransform` caches each row inside `read`, before the check).
- **Scenario:** a query without a limit on a large read model whose documents are tens of KB.
- **Mechanism:** the query reads up to `maxRows + 1` = 100 001 rows. pgjdbc buffers the whole result (no fetch size),
  every row is deserialized, and only then is the count compared. With 20 KB documents that is about 2 GB of byte arrays
  plus the objects (estimated) on the projection's single thread before the exception; the CPU of 100 000 deserializations
  is spent on a query that then fails. `findDocumentWithTransform` also puts up to 100 001 entries into the cache (now, or
  after commit) for a query that fails.
- **Impact:** a GC storm or `OutOfMemoryError` on the node from a query that was supposed to be refused; the projection
  stalls meanwhile. The cap bounds rows, not bytes.
- **Validation:** 150 000 documents of 20 KB; `findDocumentByPath` matching all with a 2 GB heap; watch the heap and time
  before `TooManyDocumentsException`.
- **Suggested fix:** check the count before reading documents: `SELECT count(*) FROM (SELECT 1 … WHERE … LIMIT maxRows+1)`
  (index-only where possible), or stream with a fetch size inside the session and throw at row `maxRows + 1` before
  deserializing more; cache rows only after the check. Consider a lower default or a byte budget.
- **Regression test:** a store with `maxRows = 10` and 11 matching rows: no row is deserialized and nothing is cached.
- **Continues:** F211.

### R2-DOCS-4 — A read's deferred cache write overwrites a newer absence or presence: deleted documents come back, new ones vanish and their inserts fail until a restart
- **Severity** Medium · **Likelihood** Unlikely (needs a cache, plus `parallelUpdateProcessing` or readers outside the
  projection's actor) · **Confidence** Confirmed (code) for the guard; the race needs timing.
- **Where:** `PostgresDocumentStore.scala:479-490` (`cacheAfterCommit`: the "newer" guard only compares `InCache` with
  `Some`), `:237-241` and `:449-451` (reads cache `None`), `:467-472` (`getFromCacheOrDB`: `InCacheEmpty` answers `None`
  without the database), `:548-549` and `:595-596` (F266's second eviction).
- **Scenario A (absent wins):** with `parallelUpdateProcessing`, listener A reads document K in its transaction
  (`getDocument`/`getDocuments`, not found); listener B inserts K and commits, its after-commit puts K v1; A commits later,
  and its after-commit puts `None` over v1 (`(InCache, None)` is not guarded).
- **Scenario B (deleted comes back):** A reads K v3 before B's `removeDocument`/`clearSpace`/`clearAllData` commits; B's
  after-commit stores `InCacheEmpty` or evicts; A's after-commit (or, for a read without a session, its immediate put
  after a slow `findDocument` of many rows) then stores v3.
- **Mechanism:** the guard protects version order only; a cached absence and an evicted key do not carry a version, so any
  older read wins. In A, every later `updateDocument(K)` takes `None` from the cache, inserts, and fails on the primary
  key; the rollback leaves the cache as it is, so the listener fails until it gives up (D20a) and stays failed until the
  node restarts. In B, a purged document is served from the cache.
- **Impact:** wrong reads; a stuck subscription that retry cannot clear; a purged organization's document still served
  (GDPR-adjacent).
- **Validation:** a `PostgresDocumentStore` with a map cache: in `AfterCommit.localTx` T1 call `getDocument(K)` (none),
  hold T1 open; insert K in T2 and commit; commit T1; `getDocument(K)` returns `None` and `updateDocument(K)` throws.
- **Suggested fix:** let reads only fill an empty slot, and only when no write touched the key since the read began (a
  per-key write stamp taken before the read, bumped by every write's eviction); writes' after-commit puts always win.
  On a unique violation in `updateDocument`'s insert path, evict and retry from the database.
- **Regression test:** both scenarios in `CachesAfterRollbackSpec`, plus the insert-after-stale-absence case.
- **Continues:** F16, F266 (its fix covers reads that fill the cache before the commit, not those that fill it after).

### R2-DOCS-5 — An empty value list makes invalid SQL on PostgreSQL and an empty result in memory
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code).
- **Where:** `PostgresDocumentStore.scala:330-332` (`in ()` for `ExpectedMultipleTextValues`/`IntValues`/`LongValues`),
  `:336` (`?| ARRAY[]` for `ExpectedMultipleTextValuesInArray`); `MemoryDocumentStore.scala:62-65` (`vals.exists` is
  false: no match); only `findDocumentsByPathWithOneOfTheValues` guards it (`DocumentStore.scala:236-242`).
- **Scenario:** a listener or query builds the values from data: `ExpectedMultipleLongValues(Seq("projectId"),
  event.projectIds)` with an empty list, or a UI filter with nothing selected.
- **Mechanism:** PostgreSQL rejects `x in ()` (42601) and `?| ARRAY[]` (cannot determine the type of an empty array). Not
  transient, so a listener is retried for about 2 minutes, then recorded as failed; the retry fails the same way, so the
  aggregate's later events stay held until `SkipFailedVersion`. Level-2 tests pass.
- **Impact:** a projection stuck on an aggregate in production for an input tests never fail on; 500s for empty filters.
- **Validation:** `findDocument(DocumentStoreQuery.basic(Seq(ExpectedMultipleTextValues(Seq("a"), Nil))))` on each store.
- **Suggested fix:** an empty list is `FALSE` in SQL (`1 = 0`), matching the memory store and set semantics.
- **Regression test:** the same query in `MemoryDocumentStoreSpec` and `PostgresDocumentStoreSpec`, against one expectation.

### R2-DOCS-6 — A statement timeout counts as "transient": a deterministically slow listener is retried every 30 s for ever and never recorded
- **Severity** Medium · **Likelihood** Possible (needs `statement_timeout` on the role or database, a common production
  setting) · **Confidence** Confirmed (code).
- **Where:** `util/Errors.scala:54-57` (classes 08, 40, 53, 57 are transient; 57014 `query_canceled` is what
  `statement_timeout` raises); `projection/ProjectionActor.scala:781-790` (no attempt counted, retried every
  `retries.max`, logged as "cannot reach its database"); `rebuild/ProjectionBuilder.scala:300,382` (the same rule).
- **Scenario:** a listener's query or a targeted reset (F217: one transaction) runs longer than the role's
  `statement_timeout` every time.
- **Mechanism:** each run is cancelled with 57014, classed as the database's failure, put back, and tried again in 30 s,
  without limit and without a `subscription_failures` row. The warning blames connectivity.
- **Impact:** the aggregate never advances in that projection, invisibly to `GetProjectionStatus`; flow control keeps its
  deliveries held; the database repeats a heavy query twice a minute.
- **Validation:** `ALTER ROLE … SET statement_timeout = '1s'`; a listener running `pg_sleep(2)`; it is never recorded.
- **Suggested fix:** treat 57014 as transient only for a bounded number of attempts (or when the message is the operator's
  cancel, not the timeout), then count it as the listener's failure; name the SQLState in the warning.
- **Regression test:** `ProjectionDeliverySpec`: a listener that always times out is recorded after the retries.
- **Continues:** F254.

### R2-DOCS-7 — Retiring a projection drops its tables under the schema lock with no lock timeout
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code); the lock queue is PostgreSQL's behaviour.
- **Where:** `rebuild/Projections.scala:70-76` (`retire` under `SchemaLock`, every node at start);
  `rebuild/ProjectionRegistry.scala:189-197` (`DROP TABLE IF EXISTS` in a plain `DB.localTx`, not through `Ddl`).
- **Scenario:** a release removes a projection, and the node starts while a long transaction reads its table (`pg_dump`,
  an organization export).
- **Mechanism:** the `DROP` waits for ACCESS EXCLUSIVE for the length of the dump while holding the advisory lock; every
  other initializer of every node waits up to 5 minutes and then fails (R2-DOCS-1 makes the wait pin threads and
  connections).
- **Impact:** F185's crash-loop of starting nodes, through the one start-up path the F185 fix did not cover.
- **Validation:** session A: `BEGIN; SELECT count(*) FROM projection_old;`; start a node without the projection; start a
  second node: it fails after 5 minutes.
- **Suggested fix:** run the drops through `Ddl`'s bounded lock wait (`SET LOCAL lock_timeout`, retried), and retry the
  retire later instead of holding the schema lock.
- **Regression test:** a `StartupLocksSpec` scenario for `retire`.
- **Continues:** F185.

### R2-DOCS-8 — `Ddl.has` finds an index of the same name in any schema on the search path; the other checks use the current schema
- **Severity** Low · **Likelihood** Unlikely · **Confidence** Confirmed (code); `to_regclass` resolving by `search_path` is
  PostgreSQL's documented behaviour.
- **Where:** `util/Ddl.scala:117` (`to_regclass(name)`), `:128` (`createIndex` skips when `has`), `:131` (`dropIndex`
  runs an unqualified `DROP INDEX IF EXISTS`); `PostgresDocumentStore.scala:74` (`existed`); unconditional legacy drops
  (`PostgresEventStoreSchemaInitializer.scala:195,202`, `CommandResponseState.scala:117`). `hasColumn`,
  `readExistingIndices` and `spaceIndexes` use `current_schema()`.
- **Scenario:** a search path of several schemas (PostgreSQL's default `"$user", public`, with both holding library
  objects: a second installation, a copy, a move from `public`).
- **Mechanism:** an index that exists only in the later schema makes `createIndex` skip it in the current one; before
  F185, `CREATE INDEX IF NOT EXISTS … ON table` looked in the table's schema. A legacy `dropIndex` absent here drops the
  other schema's index. A new document table whose name exists in the other schema gets no `space_id` index.
- **Impact:** missing indexes in the current installation, silently (slow), or loudly for unique ones used by
  `ON CONFLICT` (`commands_responses_unique_key_idx`, `types_names_names_idx`); another installation's index dropped.
- **Validation:** schemas `a` and `public` both initialized; drop `a.events_aggregates_idx`; start with search path
  `a, public`: it is not created again.
- **Suggested fix:** look indexes up in `pg_class` joined to `pg_namespace` on `current_schema()`; qualify `DROP INDEX`
  with `current_schema()`.
- **Regression test:** `SchemaAwareCatalogSpec` with a two-schema search path.
- **Continues:** F71 (a regression brought by the F185 fix).

### R2-DOCS-9 — `buildSpaceIndexesConcurrently` takes no lock: two runs drop each other's index in progress or build two
- **Severity** Low · **Likelihood** Possible (if the host calls it at every node's start, as `buildIndexesConcurrently`
  is described "in the background") · **Confidence** Strongly suspected (lock behaviour); the deadlock part is a Hypothesis.
- **Where:** `PostgresDocumentStore.scala:32-41`.
- **Mechanism:** run B sees run A's index under construction as invalid (`indisvalid = false`) and issues
  `DROP INDEX CONCURRENTLY` on it; that waits for A's `ShareUpdateExclusiveLock` until A finishes, then drops the now
  valid index and builds another (double work, a window without the index). Started together, both build one each: two
  indexes on `space_id` for good. A `CREATE INDEX CONCURRENTLY` waiting for older snapshots can deadlock with B's waiting
  `DROP`, failing one run (its remaining tables are skipped).
- **Suggested fix:** take the advisory lock (a separate key, so the long build does not block starts) or
  `pg_try_advisory_lock` per table and skip when taken; drop an invalid index only when no backend reports it in
  `pg_stat_progress_create_index`.
- **Regression test:** two concurrent calls on one large table end with exactly one valid index.

---

## Lesser items

- Mixed-case store names (allowed by `PostgresDocumentStore.scala:56`) are folded by PostgreSQL, but
  `readExistingIndices` (`:144`) and `spaceIndexes` (`:21`) compare the name as written: no index is ever seen, so F171's
  rebuild of a changed definition and the drop of undeclared indexes never happen for such a table. Confirmed (code).
- `findDocumentByObjectInArray` with `limit = -1` fails on PostgreSQL (`LIMIT` must not be negative,
  `PostgresDocumentStore.scala:382-383`) and returns nothing in memory (`MemoryDocumentStore.scala:127`); `-1` means "all"
  everywhere else.
- `logReceive` wraps a failure in `ActorException` with the raw cause (`util/ActorLogging.scala:154`): Pekko's supervisor
  logs the whole cause chain, unredacted, for an exception that escapes (latent: the command bus catches everything and
  `UidGeneratorActor` works in a future today). And `LoggingReceive` (`:148`) logs whole messages when `pekko.actor.debug.receive` is on,
  outside the `LogValues` switch.
- `Errors.redacted` returns the exception unchanged when nothing in its first five causes is withheld
  (`util/Errors.scala:24,73`), and keeps a wrapper's message (`:99`) even when it is a cause's `toString`
  (`new RuntimeException(cause)`): a `MatchError` value passes through a host's wrapper or a deep chain.
- The command bus answers an unknown message with the message's `toString` (`AggregateCommandBusActor.scala:251`), so its
  contents reach the caller's logs (continues F191).
- A read `ReadWatch` gives up keeps its thread and connection (`util/ReadWatch.scala:258-264`); without the book's
  `socketTimeout` one more is pinned per `readTimeout` (5 min) for as long as the connection hangs.
- `TypesNamesState.idFromDB` takes a second connection inside the caller's open transaction (`TypesNamesState.scala:89`):
  the first burst of a new event class across all blocking threads, with the pool equal to the threads, waits for the pool
  and fails.
- `buildSpaceIndexesConcurrently` keeps one connection for all tables; a table whose build exceeds the book's
  `socketTimeout=600` fails the run (the book already says to run long jobs without it, `18-production.md:43-44`).

## Fixes checked and found sound

- **F210:** `JsonPath.literal` quotes as PostgreSQL's `array_out` does and doubles quotes; no element ends the literal or
  splits, also with `standard_conforming_strings` off; index definitions read back equal (`sameDefinition`).
- **F211:** the limit (`maxRows + 1`) and the exception work in every finder that goes through `run` and in the memory store;
  `Unbounded` and `findAll` stay unbounded as documented (cost: R2-DOCS-3; paging advice: R2-DOCS-2).
- **F273:** the memory store's sort, offset, limit and `maxRows` follow PostgreSQL, the missing-value order included.
- **F266:** sound for reads that fill the cache before the commit (later fills: R2-DOCS-4).
- **F189:** `ProjectionBuild.documentStore` marks the table before the store's constructor runs; outdated tables keep their
  indexes.
- **F217 (index part):** a new table gets its `space_id` index in the same set-up; the index is not taken for one of the
  store's own (`_idx_N`) and so never dropped by the index sync.
- **F185:** `Ddl` reads the catalog first and bounds lock waits for the steps it covers (gaps: R2-DOCS-7, R2-DOCS-8).
- **F186:** `ReadWatch` numbers reads and ignores late results correctly.
- **F190:** `SqlLogging.withoutValues` turns off both of ScalikeJDBC's logs for the library's sessions.
- **F191:** `Errors.describe`/`redacted` withhold the listed classes' values (gaps in Lesser items).
- **F219:** `AfterCommit`'s per-transaction state in a concurrent map, actions run after the commit and dropped on rollback.
- **F230, F247:** `TypesNamesState` holds no lock while reading or storing a name, and looks a name up before it inserts.
- **F272:** `PoolCheck` warns once per system as described (its threshold: R2-DOCS-1).
