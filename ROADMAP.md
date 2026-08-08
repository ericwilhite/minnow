# BrowserDatabase Roadmap

This roadmap is organized as vertical slices with measurable exit gates. Phase numbers preserve the implementation order developed during design; later phases may overlap experimentally but may not bypass the correctness and performance gates beneath them.

## Milestone overview

| Phase | Deliverable                  | Exit evidence                                                        |
| ----- | ---------------------------- | -------------------------------------------------------------------- |
| 0     | Architectural invariants     | Decisions recorded and enforced by package boundaries                |
| 1     | Benchmark laboratory         | Reproducible browser results for the compressed IndexedDB round trip |
| 2     | Binary storage format        | Versioned round trips, corruption rejection, codec conformance       |
| 3     | IndexedDB block store        | Immutable writes and atomic manifest compare-and-swap under faults   |
| 4     | MVCC core                    | Snapshot-consistent reads, conflict tests, recovery and leases       |
| 5     | Write path                   | Batch inserts, update deltas, deletes, bounded buffers               |
| 6     | Compaction                   | Incremental resumable L0 -> L1 -> L2 compaction                      |
| 7–9   | Vector scans and aggregation | Bounded batches, pruning curves, core aggregate coverage             |
| 10    | Spill framework              | Correct large sort/aggregate/join under small memory limits          |
| 11    | Worker parallelism           | Segment-level speedup without shared memory                          |
| 12–13 | SQL and optimizer            | Feature matrix plus plan/execution conformance                       |
| 14–16 | Schema, ORM, live queries    | Typed unified plans and correct dependency invalidation              |
| 17    | Backup/restore               | Snapshot-consistent streamed round trip                              |
| 18    | Buffered durability          | Explicit bounded pending-data semantics                              |
| 19    | Adaptive optimization        | Measured workload-driven physical improvements                       |

## Phase 0 — Repository and invariants

Deliver:

- architecture and roadmap documents;
- npm-workspace TypeScript monorepo;
- strict type checking, formatting, linting, unit test, and build commands;
- browser-only package contracts with no Node runtime dependencies in shipped code;
- automated checks that implementation packages do not import existing SQL engines.

Exit gate: clean install, typecheck, tests, and production build from a fresh checkout.

## Phase 1 — Benchmark laboratory

Deliver a browser page that runs all database work in a dedicated worker and measures:

- typed-array generation and encoding;
- raw, byte-RLE, and browser-native gzip compression;
- immutable IndexedDB writes using relaxed durability;
- committed bytes, elapsed time, and MiB/s;
- reads, checksum verification, decode, and a vector sum;
- logical bytes, stored bytes, compression ratio, block count, and failures;
- environment metadata sufficient to compare runs.

The dashboard's default correctness workload is a deterministic 27-table commerce graph. One scale
multiplier grows every dimension, bridge, transaction, and ledger table. It validates primary keys,
41 foreign-key paths, value domains, and transaction coverage before running 15 oracle-checked
reference queries. A measured, read-only ad-hoc reference SQL console remains explicitly separate
from library timings. The library now has a correctness-first native `query()`/`prepareQuery()` SQL
subset for projections, filters, equi-joins, grouping, core aggregates, ordering, and limits. The
four-engine comparison uses that public API and requires matching checksums. Vectorized execution,
statistics, broader SQL, and cost-based optimization remain planned in Phases 7–13.

Initial matrix:

- compressed block targets: 256 KiB, 512 KiB, 1 MiB, 2 MiB, 4 MiB;
- datasets: quick local smoke tests first, then 100 MiB, 1 GiB, 5 GiB, and 10+ GiB where quota allows;
- memory budgets to preserve in future query tests: 16, 32, 64, 128, and 256 MiB.

The dashboard automates the raw/RLE/gzip comparison across all five block sizes and can preserve a
raw result bundle from Playwright. The checked-in 2026-08-08 Chromium and Firefox runs cover
1,930,800 rows and about 90 MiB of encoded payload per cell. All 30 cells verified. The decision
record selects gzip with a provisional 2 MiB target for storage-oriented benchmark and physical
rewrite work. Phase 6B uses that as its default estimated uncompressed physical target and persists
the selected byte target with each rewrite job; ordinary row-partitioned writes remain separately
configurable. Larger quota-dependent tiers and repeated-sample distributions remain open.

Later comparison adapters may benchmark SQLite-Wasm and DuckDB-Wasm as opt-in development-only competitors. They must never be imported by engine packages or used for correctness.

Exit gate: checked-in measurements from at least two browser engines, with a documented block-size decision or a documented reason to keep it configurable.

## Phase 2 — Binary block format

Deliver:

- branded/versioned fixed header and extensible metadata;
- logical types, encodings, compression IDs, and segment manifest types;
- checksum validation and strict length/version checks;
- raw and deterministic RLE codecs plus browser-native compression adapter;
- automatic numeric zone-map calculation;
- golden byte fixtures once the experimental format is promoted from version zero.

Exit gate: round-trip and corruption tests for every supported typed vector and codec; decode never trusts unvalidated lengths.

## Phase 3 — IndexedDB block store

Deliver:

- stores for catalog, manifests, segments, blocks, transactions, leases, statistics, temp, and GC;
- immutable add/read/remove primitives;
- atomic expected-version manifest publication;
- default relaxed durability with explicit strict option;
- deterministic memory implementation for unit tests;
- named fault-injection wrapper and crash-window scenarios.

Exit gate: block immutability and manifest atomicity hold under failures before/after each persistence boundary. Browser tests exercise real IndexedDB, not only a mock.

## Phase 4 — MVCC and transaction core

Deliver:

- immutable manifest history and snapshot handles;
- transaction IDs, expected versions, pending-block journals, and change sets;
- hidden 64-bit row IDs with range reservation;
- conflict detection/rebase policy;
- reader and backup leases with expiry;
- reachability-based recovery and garbage-collection planning.

Exit gate: multiple tabs can read stable snapshots while writers prepare concurrently and publish through IndexedDB compare-and-swap. Web Locks and BroadcastChannel can be disabled without correctness failures.

## Phase 5 — High-throughput writes

Deliver:

- typed/columnar `insertBatch` path and convenience row inserts (complete);
- typed/columnar `upsertBatch` with a named unique key and persistent key lookup: new records are inserted and matching records are updated (complete);
- bounded age/row/byte write buffers (complete);
- append-only insert segments, upsert patches, narrow keyed update patches, and key deletion markers (complete);
- immediate flush request on hidden/pagehide as a best-effort optimization (complete);
- instrumentation for rows/s, encoding/staging/commit latency, retries, and write amplification (complete);
- atomic versioned unique-key chunks that replace per-key IndexedDB operations (complete);
- one bounded IndexedDB block-staging transaction per insert/upsert batch instead of one per column
  (complete; update batches currently stage one column at a time);

The current read path fetches projected blocks for each visible segment in bounded bulk windows of
up to 16 block IDs and correctly replays insert, upsert, update, and delete segments. The checked-in
`2026-08-07-mutation-durability-matrix.json` completes the planned four-way browser/durability
matrix: it records verified insert, upsert, update, projected-read, delete, snapshot/version, and
competing-writer outcomes in Chromium and Firefox with relaxed and strict IndexedDB durability.
These are single-scale observations; the broader Phase 5 exit gate still calls for performance
curves.

The 2026-08-08 1.93-million-row comparison records 303,723 BrowserDatabase rows/s after the key
chunk and batch-staging optimization, versus 213,557 for persistent SQLite, 87,938 for persistent
PGlite, and 1,589,803 for in-memory DuckDB in the same host Chromium run. BrowserDatabase's total
logical persisted payload is reported separately from implementation-specific physical page sizes.

Exit gate: published benchmark curves for insert, upsert, update, delete, and competing commits under relaxed and strict durability. Upsert tests cover new rows, matching rows, duplicate keys, null keys, and two writers updating the same key.

## Phase 6 — Compaction

Phase 6A and the first Phase 6B physical-rewrite slice are implemented:

- atomic manifest publication can supersede snapshot blocks without deleting historical data;
- revisioned compaction job records persist in the IndexedDB `gc` store with planned, running,
  ready, published, cancelled, and aborted states;
- `compactTableStep()` plans or advances a job by a bounded output-block count,
  `resumeCompactionJob()` continues it after a yield or restart, and `listCompactionJobs()` exposes
  durable progress; `compactTable()` remains the convenience wrapper that drives those steps to
  publication;
- `cancelCompactionJob(jobId)` atomically settles an unpublished job as `cancelled` and aborts its
  active transaction; repeated requests return the same terminal outcome, while a commit that won
  the race is reported as `published` with its manifest version;
- the immutable `rechunk-v1` plan fingerprints the ordered columns and source blocks with row
  ranges, stored/encoded lengths, and checksums, and persists output windows, row-ID bounds, logical
  order, target encoding, and execution settings;
- execution decompresses validated physical columns, slices and concatenates row ranges, and
  re-encodes them without materializing JavaScript row objects;
- the defaults are gzip, a 2 MiB estimated uncompressed physical target per output column block,
  and a 32 MiB budget for JavaScript-owned executor buffers;
- each output-block checkpoint records the next window/column cursor, output IDs and bytes, rows,
  and modeled working-memory high-water for completed outputs; a conservative preflight minimum
  rejects a budget that cannot execute its largest planned output;
- retries decompress and validate existing output before comparing its physical semantics; this
  avoids depending on byte-identical gzip streams and lets recovery reattach the block to a
  replacement transaction;
- a job resumes its persisted transaction, and a lost response after a committed transaction is
  reconciled by marking the job published;
- ordinary write segments are L0; the current whole-table append policy publishes one L1 segment
  whose stable `logicalOrder` is inherited from its earliest source;
- publication can rebase safely across a concurrent append when every planned source remains
  visible, so the newly appended L0 segment stays after the consolidated output;
- historical manifests and source blocks remain available to old snapshots;
- contiguous internal row-ID ranges are retained without allocating replacement identities;
- mutation-bearing and non-contiguous inputs return explicit skip reasons;
- metrics distinguish superseded blocks from physically reclaimed bytes, which remain zero.

The byte target is estimated from source-block encoded density and shared across column output
windows, then windows are measured and split for skew or a tighter execution budget before the plan
is persisted. A single row cannot be split, so it may exceed the target only when its physical and
codec bounds still fit the format cap. The conservative memory figures cover buffers owned by
rewrite execution, not the whole browser process: planning reads one stored block at a time outside
executor high-water accounting, and native codec, IndexedDB internal allocations, and persisted
job/transaction metadata are excluded.

Phase 6 remains open. The current policy rewrites all eligible contiguous append-only inputs into
one whole-table L1 segment. It does not compact upsert/update/delete deltas, choose subsets or level
policies beyond L0 -> L1, build L2 segments, or reclaim physical blocks. Cancellation is
cooperative: it cannot preempt physical transforms or native codec work already in progress, but an
in-flight step observes it at the next durable boundary and `resumeCompactionJob()` throws
`CompactionJobCancelledError`. Cancelled records retain their plan, cursor, progress, metrics, and
completed artifact IDs; physical cleanup remains deferred to lease-aware garbage collection.

Deliver:

- L0 write-oriented, L1 query-oriented, and optional clustered L2 segment policies;
- incremental, resumable, memory-budgeted jobs;
- publication through the same manifest protocol;
- safe cleanup of superseded blocks;
- write-amplification and reclaimed-byte metrics.

Exit gate: interrupted compactions recover safely and sustained ingestion stays within an explicit write-amplification budget.

## Phase 7 — Vector execution kernel

Deliver bounded `Batch`, typed `Vector`, validity bitmap, string-vector, selection-vector, and memory-context primitives. Initial operators: segment scan, filter, project, limit, aggregate, hash aggregate, sort, and hash join.

Exit gate: hot paths avoid row-object materialization and per-row callbacks; operator memory is accounted against a configured budget.

## Phase 8 — Filtering and data skipping

Deliver automatic min/max, null count, approximate distinct, dictionary membership, and optional Bloom metadata; segment/row-group pruning; predicate-column-first reads; late materialization.

Benchmark selectivity at 100%, 50%, 10%, 1%, 0.1%, and 0.01%.

Exit gate: selective queries read fewer blocks/bytes and get materially faster as more row groups can be ruled out.

## Phase 9 — Aggregation

Deliver COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT, GROUP BY, HAVING, partial aggregation, and dictionary-specialized kernels. Add GROUPING SETS, ROLLUP, CUBE, and aggregate FILTER after the fundamentals.

Exit gate: aggregate correctness suite plus single/multi-worker throughput and memory profiles.

## Phase 10 — Spill framework

Deliver external merge sort, partitioned hash aggregation, partitioned hash join, DISTINCT spilling, and compressed temporary blocks where beneficial.

Exit gate: representative queries over data much larger than memory complete correctly with a 32 MiB budget, without UI stalls.

## Phase 11 — Worker parallelism

Deliver adaptive coordinator/executor scheduling by immutable segment, transferable vector batches, partial-result merging, cancellation, and backpressure. Small queries remain single-worker when message cost dominates.

Exit gate: measured wall-time improvement on large scans/aggregates and no SharedArrayBuffer, Atomics, SharedWorker, or COOP/COEP requirement.

## Phase 12 — SQL frontend

Deliver lexer, parser, AST, binder/type checker, logical plan, physical plan, and a machine-readable SQL feature matrix. Implement relational fundamentals before breadth: SELECT/FROM/WHERE, joins, grouping, ordering, limits, distinct, subqueries, sets, CTEs, windows, and mutations.

Foundation implemented early: a dependency-free read-only lexer/parser and bound projected snapshot
executor now powers the public `query()` and reusable `prepareQuery()` APIs. It supports SELECT,
aliases, inner/left equi-joins, AND comparisons, arithmetic, core aggregates, ROUND, GROUP BY,
multi-column ORDER BY, and LIMIT. Unsupported syntax fails explicitly. This row-oriented foundation
does not complete Phase 12: typed logical/physical plans, DISTINCT, subqueries, sets, CTEs, windows,
mutations, and the vector kernel remain outstanding.

Exit gate: SQL compiles into the same logical representation used by all other APIs; unsupported syntax fails explicitly rather than silently changing semantics.

## Phase 13 — Optimizer

Deliver deterministic rewrites first: constant folding, projection pruning, predicate/limit/aggregate pushdown, segment pruning, late materialization, and dictionary rewriting. Then add collected statistics and cost-based join order, strategies, parallelism, and spill decisions.

Exit gate: plan snapshots and workload benchmarks demonstrate each rule's correctness and value.

## Phase 14 — Schema DSL and migrations

Deliver stable table/column IDs, relational type constructors, catalog compiler, Standard Schema-compatible runtime metadata, relations, and automatic migration planning. Prefer metadata-only compatible evolution and piggyback rewrites on compaction.

Exit gate: compile-time select/insert/update types and crash-safe migration tests across supported schema changes.

## Phase 15 — Type-safe ORM

Deliver CRUD, relational query builder, typed results/nullability, batch APIs, and raw SQL escape hatch. ORM expressions construct logical plans directly; they do not generate SQL strings.

Exit gate: ORM and equivalent SQL produce equivalent bound logical plans and results.

## Phase 16 — Live queries

Deliver persisted change sets, dependency extraction, cross-tab hints, version reconciliation, and correctness-first selective re-execution. Add incremental maintenance gradually for filters and core aggregates, always retaining re-execution fallback.

Exit gate: missed BroadcastChannel messages, suspended tabs, and unrelated commits cannot create stale results; latency and avoided reruns are measured.

## Phase 17 — Backup and restore

Deliver a versioned streaming format for catalog, pinned manifest, descriptors, blocks, and checksums. Restore verifies/write blocks before final manifest publication and supports future format migration hooks.

Exit gate: backup under concurrent writes restores the exact pinned snapshot and corruption is detected before publication.

## Phase 18 — Buffered durability

Deliver opt-in pre-persistence acknowledgment bounded by bytes, rows, and age; explicit `flush()`; pending-durability visibility; byte/row/timer/hidden/pagehide triggers.

Exit gate: API and tests clearly distinguish accepted, persisted, and visible states; abrupt close is documented as an unguaranteed flush opportunity.

## Phase 19 — Adaptive physical optimization

Deliver normalized workload fingerprints, decayed usage telemetry, automatic clustering/encoding/Bloom decisions, benefit/cost accounting, and rollback of ineffective structures.

Exit gate: representative changing workloads improve without manual indexes and without unbounded background rewrite cost.

## Immediate iteration checklist

The current repository slice has completed the Phase 5 storage/write foundation and its planned
four-way durability matrix. Phase 6A plus the first Phase 6B slice now provide durable,
restart-safe, output-block-stepped physical rechunking and re-encoding under a modeled
JavaScript-owned memory budget, including safe cooperative job cancellation. Phase 6 remains open
until compaction handles mutation deltas and level/subset selection and safely reclaims unreachable
data. The broader
Phase 5 performance curves and larger/repeated benchmark tiers remain outstanding.

- [x] Record architecture and roadmap.
- [x] Scaffold packages and quality gates.
- [x] Implement block-format types and codecs.
- [x] Implement IndexedDB and in-memory block stores.
- [x] Implement versioned worker protocol and coordinator skeleton.
- [x] Implement deterministic fault injection.
- [x] Add unit and real-browser tests.
- [x] Build the bounded multi-entity storage dashboard and block inventory.
- [x] Automate the 15-run compression and block-size comparison.
- [x] Check in the full raw/RLE/gzip comparison across all five block sizes and document the
      block-size decision.
- [x] Implement stable snapshots, transaction journals, atomic commit/rebase, and stale-write recovery.
- [x] Implement persistent tables, immutable insert/upsert/update/delete segments, and hidden row-ID allocation.
- [x] Implement persistent unique-key lookup with conflict-safe two-writer retries.
- [x] Implement bounded buffered writers and keyed partial updates.
- [x] Coalesce insert/upsert block staging and read projected segment blocks in bounded bulk windows.
- [x] Add projected table reads and per-write performance metrics.
- [x] Add insert/upsert/update/delete/projected-read dashboard probes.
- [x] Run the browser storage benchmark and record smoke results.
- [x] Record the relaxed/strict write benchmark matrix across Chromium and Firefox.
- [x] Add lifecycle-triggered best-effort buffered flush requests.
- [x] Add conservative append-only compaction with snapshot-preserving supersession.
- [x] Persist revisioned compaction jobs and block cursors in the GC store.
- [x] Resume compaction transactions and idempotently reconcile interrupted block, segment, and
      publication checkpoints.
- [x] Preserve L0/L1 logical order while rebasing publication across concurrent appends.
- [x] Rechunk and re-encode append-only inputs under an explicit JavaScript-owned byte memory budget.
- [ ] Add subset/level selection beyond whole-table L0 -> L1 and compact mutation deltas.
- [x] Add safe compaction-job cancellation.
- [ ] Add lease-aware garbage collection and physical reclamation for superseded blocks.
- [x] Provide `npm run check:release` for quality checks plus both real-browser suites.

## Result recording

Every checked-in benchmark result must include date/time, browser and version, operating system, logical/stored bytes, dataset shape, codec, block size/count, durability, write/read/decode/aggregate timings, verification result, and known environmental caveats. Results are observations, not promises, and should never be silently compared across materially different environments.
