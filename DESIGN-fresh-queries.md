# Design: Always-Fresh Queries over a Block Buffer Pool

Status: proposed, August 2026. Companion to [ARCHITECTURE.md](ARCHITECTURE.md); supersedes the
prepared-statement materialization model described nowhere but implemented in
`packages/core/src/engine/database.ts`.

## Summary

Delete `prepareQuery()` from the public API and the prepare-time vector materialization behind
it. Replace both with the architecture every mature engine uses: IndexedDB is the single source
of truth, an immutable-block buffer pool is the only data cache, and a streaming vectorized
executor scans blocks in fixed-width batches. Every query starts with a ~0.1 ms manifest-version
probe, so a stale read becomes unrepresentable rather than a lifecycle bug the user must avoid.

This is a return to the stated architecture, not a departure: ARCHITECTURE.md already names
"Vectorized; streaming inputs" as a required exit gate. The fused-array prepare path was a
shortcut past the streaming executor, and its costs are now measured.

## The problem, measured

All numbers from this machine (Darwin, Chromium for browser numbers); harness scripts noted
inline. They establish relationships, not absolutes.

| Defect                                         | Measurement                                                                                                                                                                                                       |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stale reads by design                          | `prepared.execute()` returns the prepare-time snapshot forever; the docs teach this as a feature (`apps/site/src/lib/examples.ts`, "snapshot" example)                                                            |
| Re-prepare is O(all rows), not O(changed rows) | 200k-row table, memory store: re-prepare after a 1-row insert costs 3.5–6.7 ms (~17 ns/row); unchanged re-prepare costs 0.2 ms. At 10M rows every write costs ~175 ms on the next prepare                         |
| Dead cache growth                              | 1-row-write + re-prepare loop leaks ~0.4 MB of orphaned vectors per cycle until the 64 MB `prepareCacheBytes` cap; every orphaned entry is unhittable because its key (the table's segment list) no longer exists |
| Catalog read is O(segments) per prepare        | Real Chromium IndexedDB, 2,000 segment records: full catalog scan costs 8.6 ms per prepare                                                                                                                        |
| Freshness would have been nearly free          | Same environment: one readonly transaction reading the current-manifest key costs 0.10 ms median, 0.20 ms p95                                                                                                     |

Root cause, one line: the vector cache is keyed by the table's **visible segment list**
(`fingerprint = segments.map(s => s.id).join(",")`), a mutable identity. Every write destroys
every cached vector for the table, forces full re-materialization, and orphans the old entries.
Staleness, the re-prepare cliff, and the dead cache are all the same bug.

## Principles

1. **IndexedDB is the source of truth.** The manifest version is the database's change counter,
   exactly as the WAL-index header is for a SQLite reader. No in-memory structure is ever
   authoritative.
2. **Cache only immutable identities.** Blocks are immutable and content-keyed; a cache keyed by
   block id can never be wrong and never needs invalidation — superseded blocks simply stop
   being referenced and age out of the byte-budgeted LRU.
3. **Freshness is a probe, not a protocol.** One 0.1 ms version read at statement start anchors
   the query. Version unchanged ⇒ every cache is provably valid. Version moved ⇒ read the new
   catalog. There is no invalidation message to miss.
4. **Prepare compiles. Only compiles.** SQLite compiles to bytecode, Postgres and DuckDB to a
   plan; none of them pin data. Plan caching stays internal, keyed by SQL text plus schema
   version. No public handle holds rows, so no user can hold rows too long.
5. **The executor consumes the storage layout.** Fixed-width batches streamed from immutable
   blocks, DuckDB-style. The prepare-time fused-array materialization existed only to bridge the
   executor to a layout it did not understand; the bridge is the machinery being deleted.

## How the reference engines do it

| Mechanism       | SQLite                                     | DuckDB                                   | This design                                                                                    |
| --------------- | ------------------------------------------ | ---------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Source of truth | database file                              | database file                            | IndexedDB stores                                                                               |
| Data cache      | page cache, keyed by page number           | buffer manager, copy-on-write blocks     | decoded-block LRU, keyed by immutable block id (already exists and is already correctly keyed) |
| Freshness check | WAL-index header read per read-transaction | transaction sees a consistent block set  | manifest-version probe per statement (0.1 ms measured)                                         |
| Prepare         | compile to bytecode                        | compile to plan                          | compile to plan (internal cache)                                                               |
| Execution       | row VM over cached pages                   | 2,048-row vectors streamed per row group | 2,048-row batches streamed per block                                                           |
| Snapshot scope  | the transaction                            | the transaction                          | the statement; `snapshot()` scope for multi-statement                                          |

The multi-tab twist: SQLite's pager learns about writes synchronously because writer and cache
share a process. Minnow cannot assume that — another tab commits whenever it likes. Immutability
is what makes this safe anyway: a foreign commit creates new blocks under new ids, so the buffer
pool is never wrong about the blocks it holds; only "which blocks are visible" changes, and that
is exactly what the version probe re-establishes.

## Design

### 1. Freshness protocol

Every statement:

1. Read the current manifest version (one readonly IndexedDB get, 0.1 ms).
2. If it equals the cached catalog's version, reuse the cached table records, segment lists, and
   visibility catalog with no further storage reads.
3. If it moved, re-read the catalog for the referenced tables, cache it under the new version,
   and proceed. Gaps and unknown states never widen incorrectly because the catalog is re-read
   wholesale — there is no incremental replay to get wrong.
4. Execute against that version's visible segment set. The internal read lease spans the
   statement (or the `snapshot()` scope), never a user-held handle.

Time travel (`options.version`) is unchanged: an explicit version anchors to that manifest's
block set. Old blocks are immutable, so historical reads share the same buffer pool.

### 2. Catalog cache and indexed segment listing

- Cache the query-catalog state (tables, segments, transaction records) keyed by manifest
  version. Invalidation is the version probe; no other mechanism.
- Replace the full-store segment scan with an IndexedDB index range read on `tableId`, so a
  cold catalog read for one table is O(that table's segments), not O(all segments). The 8.6 ms
  scan at 2,000 segments becomes a handful of index gets.

### 3. Buffer pool

The existing decoded-block cache (`dpb\0${blockId}`) is promoted from an internal detail to
**the** data cache:

- Keyed by immutable block id. Never invalidated; evicted only by the byte budget.
- Blocks referenced by a running statement are pinned for the statement's duration so eviction
  cannot thrash mid-scan; unpinned at statement end.
- Compaction retires blocks by simply ceasing to reference them; they LRU out naturally.
- One byte budget replaces `prepareCacheBytes`. Long-running tabs hold at most the budget, and
  every resident byte is hittable by construction.

### 4. Streaming executor

The single load-bearing change. The executor today scans fused whole-table `ColumnVector`s; it
must scan **batches sliced from block-resident columns**.

- **Batch contract:** operators consume fixed-width vectors (2,048 rows) with a validity mask.
  A batch never spans a block, so per-block dictionaries and encodings never leak across a batch
  boundary. Scan sources slice batches out of the current block and advance block by block.
- **Zone maps move into the scan.** Per-block min/max statistics are checked as the scan
  advances; a rejected block skips its batches entirely. This deletes the prepare-time pruned
  materialization and its cache (`prn\0…` keys) and makes pruning free for every query rather
  than only repeated ones.
- **Strings stay as per-block dictionary codes** until result construction. The evidence this is
  tractable rather than a rewrite:
  - Group and join indexes hash encoded key bytes, not dictionary codes
    (`group-index.ts` scratch encoders; `ByteJoinIndex` imports the same), so they accept
    batches from different blocks unchanged.
  - The dictionary fast paths (`dictionaryEquality`, per-dictionary LIKE, `codeLookup`) already
    cache per dictionary object, so per-block dictionaries slot in.
  - `VectorWindow` shows the scan layer already tolerates partially resident columns.
- **Safety net:** the SQL conformance corpus (node:sqlite oracle diff) and the seeded
  differential fuzzer must pass unchanged. New conformance templates are not required — the SQL
  surface does not change — but the fuzzer's row counts should be raised to cross many block
  boundaries.

### 5. Compaction is the row-group builder

DuckDB scans fast because row groups are large (122,880 rows) and uniform. Minnow's analog:

- Small commit-sized segments are the scan's tail; compaction folds them into large base blocks,
  which are its body.
- The compaction target block row count becomes a measured performance knob (scan throughput vs
  block size), not only a space knob. The existing incremental, resumable compaction machinery
  is unchanged; only its sizing policy gains a benchmark.

### 6. API changes

Deleted (zero users; no compatibility work):

- `MinnowDatabase.prepareQuery()`, `PreparedQuery`, `ClientPreparedQuery`, the worker-host
  prepared-handle plumbing, and `prepareCacheBytes`.
- The prepare-time vector assembly, the segment-list fingerprint cache, and the pruned-table
  cache in `database.ts`.

Kept and clarified:

- `query(sql, options?)` — one verb, always fresh, no lifecycle. `execute`, the typed DSL, and
  live queries are unchanged in surface and become uniformly fresh in semantics.
- Live queries keep `changedTableIds` change sets for selectivity only; correctness never
  depends on them (unchanged from today's design).

Added:

```ts
// Multi-statement consistency: a scope you enter deliberately, released on exit.
await database.snapshot(async (snap) => {
  const total = await snap.query("SELECT COUNT(*) FROM people");
  const sum = await snap.query("SELECT SUM(score) FROM people");
  // total and sum observe the same manifest version
});
```

`snapshot()` pins one manifest version and one internal lease for the duration of the callback.
It is the only pinning primitive, it cannot leak (scope exit releases it), and it proxies to the
worker client with identical semantics.

Documentation updates that land with the implementation (docs site is the single source of
truth): the "prepared queries hold one immutable snapshot" example and its runnable snippet are
replaced by a `snapshot()` example; the best-practices "close what you open" entry drops
prepared queries; the workers page drops the prepared-proxy section; the API table drops
`prepareQuery`.

### 7. Result memoization (later, optional)

With the probe in place, a result cache keyed by `(sql, params, manifestVersion)` is a pure
cache: it can serve a repeat query in ~0.1 ms and can never be stale, because a version change
changes the key. This recovers today's "instant repeated query" benchmark behavior honestly. It
is explicitly out of scope for the first implementation — it is an optimization the design
permits, not one it depends on.

## Benchmark changes

### duckdb-wasm joins the engine comparison

DuckDB's executor is the performance model this design copies, so it must be a measured line,
not an aspiration. Add a `duckdb` engine to `apps/bench` behind the existing `EngineSession`
interface, running the official `@duckdb/duckdb-wasm` bundle in the same worker harness as
sqlite-wasm and PGlite.

**Disclosure requirement — in-memory storage.** duckdb-wasm holds its database in memory:
default storage is a transient in-memory catalog, and its OPFS persistence remains experimental
and is not exercised by this harness. The other engines in the comparison run genuinely
disk-backed in a persistent browser profile (IndexedDB or OPFS). duckdb-wasm therefore skips
storage I/O entirely and carries an unfair advantage on every number it posts.

This must be visible in the record and on the page, not a footnote in a commit message:

- The capture JSON records `storage: "memory"` in the duckdb engine metadata.
- The benchmarks page (`apps/site/src/pages/benchmarks.astro`) renders a disclosure wherever
  duckdb-wasm numbers appear, generated from that metadata so the page and the record cannot
  disagree — same rule the page already follows for its other caveats. Suggested copy: "DuckDB
  runs fully in memory in this harness (no disk-backed persistence); its numbers exclude all
  storage I/O and are not directly comparable to the disk-backed engines. It is included as an
  executor reference, not a storage peer."
- The dataset-ingest comparison either excludes duckdb-wasm or labels its ingest numbers with
  the same disclosure, since ingest is dominated by exactly the I/O it skips.

### Honest per-engine timing

- The prepare/execute split disappears for Minnow: with prepare-as-compile, the reported number
  per query is fresh statement execution, like every other engine. Prepare (compile) time can
  stay in tooltips for all engines uniformly.
- Fix the harness no-ops: the sqlite adapter's `prepare()` currently returns a closure around
  `selectObjects` (which re-prepares per call) and PGlite's is equivalent; both should use the
  engines' real prepared statements so any reported prepare time means the same thing
  everywhere.
- The benchmarks-page paragraph explaining the prepared-statement split is replaced by the
  duckdb disclosure and a one-line note that all engines report fresh statement execution.

### Perf gate

`npm run test:perf` ratios re-baseline after the executor lands. The gate's meaning sharpens:
minnow-vs-sqlite ratios compare fresh execution to fresh execution. Slower-than-sqlite remains a
failure. Expect the hot repeated-query numbers to regress from "sub-0.1 ms fused-array scan" to
"probe + streamed scan of buffer-pool-resident blocks"; expect write-then-query workloads to
improve sharply (no O(table) re-materialization cliff). The gate must be green before any stage
merges.

## Sequencing

Ordered so every stage is independently shippable and the gates beneath it stay green.

| Stage | Change                                                                           | Exit gate                                                                           |
| ----- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 1     | Version-gated catalog cache + `tableId` index-range segment listing              | Conformance + perf gate green; catalog read cost measured flat vs segment count     |
| 2     | Streaming executor: batch pipeline, scan-level zone maps, per-block dictionaries | Conformance corpus, differential fuzzer (raised row counts), perf gate green        |
| 3     | Delete prepare machinery + public API; add `snapshot()`; docs-site updates       | Whole-tree check green; no `prepareQuery` reference remains outside history         |
| 4     | Compaction block-size tuning                                                     | Scan-throughput-vs-block-size curve captured; chosen default recorded with the data |
| 5     | Bench: duckdb-wasm engine + disclosures; re-capture; perf-gate re-baseline       | New capture JSONs published; benchmarks page regenerated with disclosure            |

## Measured outcomes (updated as stages land)

- **Stage 1 (landed):** catalog read flat vs unrelated segment count (200x segments -> 1.8x
  read time, formerly linear); probe 0.06-0.24 ms in fake-indexeddb, 0.10 ms in Chromium.
- **Stage 2 (landed):** streamed execution beats sqlite on every perf-gate query; group,
  join, and top-n match the old fused-path numbers (5.6/5.9/2.2 ms at 200k rows) while every
  statement returns provably fresh data. Perf gate re-baselined to fresh-execution semantics.
- **Stage 3 (landed):** prepareQuery deleted; snapshot() scope in engine and worker client;
  bufferPoolBytes replaces prepareCacheBytes.
- **Stage 4 (landed):** scan throughput vs block size, 400k rows, streamed executor
  (median ms; filter / group / like / top-n):

  | rows per block | filter | group | like | top-n |
  | -------------- | ------ | ----- | ---- | ----- |
  | 2,048          | 11.36  | 8.35  | 8.61 | 5.18  |
  | 8,192          | 11.18  | 7.18  | 7.62 | 3.50  |
  | 16,384         | 9.99   | 6.71  | 7.83 | 3.17  |
  | 65,536         | 9.88   | 6.54  | 7.98 | 3.11  |
  | 262,144        | 9.83   | 6.37  | 7.25 | 3.07  |

  Throughput is flat from ~16k rows per block. The write default stays 65,536 rows per
  block and the compaction default stays 2 MiB target blocks — both on the plateau, with
  moderate buffer-pool eviction granularity. Recorded beside the default in database.ts.

- **Stage 5 (landed):** DuckDB (wasm) joined the browser engine comparison as an
  in-memory executor reference with the disclosure generated from the capture metadata;
  sqlite/pglite use genuine prepared statements. Scale-100 re-capture (9.56M rows,
  15 queries, all four engines checksum-verified, both browsers passed): suite totals
  Chromium — Minnow 434 ms, SQLite 2,118 ms, PGlite 1,894 ms, DuckDB (in-memory) 167 ms;
  Firefox — Minnow 606 ms, SQLite 12,712 ms, PGlite 7,169 ms, DuckDB (in-memory) 1,355 ms.
  Minnow's median prepare is now ~0.3 ms (compile only), versus 28–158 ms materializing
  prepares before this design.

## Risks and open questions

- **Executor regression risk is concentrated in stage 2.** Mitigation: the conformance oracle
  and fuzzer run against `src` (not `dist`) throughout, and the fused-array path is deleted only
  in stage 3, after the streaming path has carried the full suite.
- **Batch-boundary overhead.** Fixed 2,048-row batches add per-batch dispatch that fused arrays
  did not pay. The perf gate decides whether the constant is acceptable; the compaction knob
  (stage 4) is the lever if block-count overhead shows up at realistic scales.
- **String-heavy aggregation.** Byte-hashing group keys across per-block dictionaries is the
  designed path; if profiling shows dictionary-code direct addressing mattered on hot shapes, a
  per-scan unified dictionary for low-cardinality columns is a contained follow-up.
- **duckdb-wasm bundle weight and loading.** The wasm bundle is tens of megabytes; the bench
  must load it once per session and must not let bundle fetch time contaminate query timing.
- **`snapshot()` and garbage collection.** A long callback holds a lease and can delay block
  GC; the lease TTL and renewal already bound this, but the docs must say "keep snapshot scopes
  short" — the scope makes the cost visible, which is the point.
