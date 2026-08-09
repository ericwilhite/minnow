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
four-engine comparison uses that public API and requires matching checksums. The Phase 7A columnar
executor now backs the public subset; memory-accounted execution, statistics, broader SQL, and
cost-based optimization remain planned in Phases 7–13.

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
- age timers drain rows accepted behind an in-flight flush instead of losing their wake-up
  (complete);
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

Phase 6A, the Phase 6B physical-rewrite/cancellation slices, Phase 6C mutation merging, Phase 6D
bounded L0-prefix selection and lease-aware physical reclamation for known artifacts, and the Phase
6E-A append-row-range L2 slice are implemented:

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
- the immutable `merge-v1` plan fingerprints keyed source segments in logical/commit order, including
  normalized kind, key, level, hidden row-ID spans, and unique source block metadata; logical key
  replay is frozen as ordered per-column source ranges before output execution starts;
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
- ordinary write segments are L0; the selector folds an optional leading L1 anchor plus the oldest
  complete L0 groups into one L1 segment whose stable `logicalOrder` is inherited from its earliest
  source;
- the defaults require two L0 sources and target at most 16 L0 segments or 64 MiB of
  newly promoted stored bytes; the minimum and an equal-`logicalOrder` group may cross those targets
  to preserve progress and replay order, while an explicit minimum of one can drain `L1 + 1 L0`;
- explicit `targetLevel: 2` starts the append-row-range policy only for non-keyed ordinary inserts
  with contiguous source row IDs; after one valid L2 partition exists, an omitted target continues
  L2 automatically, and an explicit minimum of one permits direct one-segment L0-to-L2 promotion;
- the accepted L2 layout is an optional retained legacy L1 insert, immutable L2 inserts with exact
  consecutive `partitionOrdinal` values, and an L0 insert suffix; each job promotes only the oldest
  complete L0 prefix to the next ordinal and never rewrites an existing L2 source;
- L2 jobs default `maxWriteAmplification` to 16 and persist the ordinal, ratio,
  `maximumOutputStoredBytes = floor(level0SourceStoredBytes * maxWriteAmplification)`, and a
  conservative `plannedOutputStoredBytesUpperBound` covering complete serialized block envelopes;
- a plan whose upper bound exceeds the ceiling returns a `write-amplification-budget` skip before
  the job or output artifacts are persisted, and execution checks cumulative actual stored full-block
  bytes against both fields before staging each block;
- a non-empty mutation merge publishes an explicit full-row `base` segment with ordered
  `rowIdSpans`; updates and matching upserts preserve existing identities, deletes remove them, and
  new keys retain their reserved IDs even when reservation order differs from commit order;
- an all-delete merge publishes the manifest change with no output blocks or visible empty segment;
- publication can rebase safely across later append or mutation segments when every planned source
  remains visible; the later deltas stay after the consolidated output and are not absorbed;
- historical manifests and source blocks remain available while an active transaction or
  unexpired reader/backup lease roots the version;
- hidden row identities are retained as ordered, nonoverlapping spans of consecutive IDs without
  allocating replacements; the spans may be numerically out of order;
- compaction metrics distinguish logical supersession from physical reclamation; a compaction
  result remains zero for `physicallyReclaimedBytes` because publication never deletes blocks;
- jobs persist L0 and anchor input bytes separately, and completed results report incremental
  `compactionWriteAmplification` as output stored bytes divided by newly promoted L0 bytes;
- `collectGarbage()`, `collectGarbageStep()`, `resumeGarbageCollectionJob()`, and
  `listGarbageCollectionJobs()` expose completion, one-step execution, restart/resume, and durable
  job inspection;
- revisioned garbage-collection jobs persist immutable candidate lists, a fixed lease cutoff,
  manifest/segment/block cursors, cumulative outcome counters, and `planned`, `running`, or
  `completed` state in the `gc` store;
- each Memory or IndexedDB step atomically revalidates roots, applies tombstones/deletions, and
  checkpoints progress under compare-and-swap; roots cover the current manifest, reader/backup
  leases unexpired at the fixed pass cutoff, active transaction snapshots and pending artifacts,
  and active compaction source/output artifacts;
- cancelled and aborted compaction artifacts, superseded published inputs, and aborted-transaction
  artifacts can be collected once no live root reaches them;
- collected manifest descriptors retain `prunedAt` tombstones for lost-commit reconciliation, but
  cannot be newly opened or pinned and no longer root their blocks;
- transient internal reader leases protect `BrowserDatabase` table/query materialization, while
  begin/rebase/pin and lease-expiry races serialize with collection;
- stale-transaction recovery routes physical deletion through the same durable collector;
- completed collection results distinguish pruned/already-pruned/retained/missing manifests,
  reclaimed/retained/missing segments and blocks, and deleted immutable block byte lengths in
  `physicallyReclaimedBytes`—not browser quota recovery.
- candidate discovery uses stable 64-record manifest/transaction/compaction pages, 64-block
  existence windows, and a configurable/default-1,024 block/segment cap per durable job;

The byte target is estimated from source-block encoded density and shared across column output
windows, then windows are measured and split for skew or a tighter execution budget before the plan
is persisted. A single row cannot be split, so it may exceed the target only when its physical and
codec bounds still fit the format cap. `memoryBudgetBytes` gates two separate conservative models:
mutation replay first checks a planner safety estimate based on candidate rows, table width, and key
bytes, while physical output execution checks its own largest-buffer minimum and persists a
high-water mark. The merge planner does not spill or checkpoint replay, so only the frozen job and
its output cursor are restartable. Neither model covers native codec allocations, IndexedDB
internals, persisted metadata, or the whole browser process.

`maxItems` bounds candidates examined and possible candidate mutations within a durable collection
step. `maxPlanningItems` bounds block/segment IDs copied into one job while storage cursors page the
candidate sources. Provenance and root revalidation also stream storage cursors, keep only the
current candidate/dependency envelope, and conservatively retain on dependency overflow. Their scan
work still scales with all metadata, and one large source record remains unbounded. Candidate
admission currently requires persisted provenance from a historical
manifest, aborted transaction journal, or terminal compaction job. An unknown immutable block left
by a crash after `addBlock()` but before journal attachment is deliberately omitted until provenance
or age tracking can make it safe to collect.

Phase 6 remains open. The L1 policy still rewrites an oldest L0 prefix plus an optional whole L1
anchor into one L1 segment—a `base` for a keyed mutation history—or no segment when no selected row
survives, so its incremental ratio is not a hard lifetime bound. Phase 6E-A adds append-row-range L2,
not clustered/key-range L2: ordinary contiguous insert prefixes on non-keyed tables become immutable
ordinal partitions and prior L2 sources are retained.

At a common configured cap, disjoint-source accounting proves that successfully published L2 output
block bytes stay within the cap times their corresponding promoted L0 block bytes. It does not bound
bytes written by cancelled or aborted attempts, metadata or IndexedDB write overhead, total browser
disk traffic, garbage collection, or quota recovery. Merge planning still cannot spill or resume
before its immutable plan is created. Collection still needs a chunked planner and indexed/chunked
root discovery plus broader unknown-orphan, catalog, terminal-job, and metadata cleanup.
Cancellation remains cooperative: it cannot preempt physical transforms or native codec work already
in progress, but an in-flight step observes it at the next durable boundary and
`resumeCompactionJob()` throws `CompactionJobCancelledError`. Cancelled records retain their plan,
cursor, progress, metrics, and completed artifact IDs until a collection pass proves their artifacts
unreachable.

Remaining Phase 6 delivery:

- keyed/clustered multi-range L2 selection;
- lifetime write-amplification accounting that includes cancelled and aborted attempts;
- spillable or resumable merge planning before the immutable plan exists;
- bounded source-record envelopes and indexed root discovery that avoids full metadata scans; and
- broader unknown-orphan, catalog, terminal-job, and metadata cleanup.

Exit gate: interrupted compactions recover safely and sustained append and keyed/clustered ingestion
stay within their explicitly scoped write-amplification budgets.

## Phase 7 — Vector execution kernel

Deliver bounded `Batch`, typed `Vector`, validity bitmap, string-vector, selection-vector, and memory-context primitives. Initial operators: segment scan, filter, project, limit, aggregate, hash aggregate, sort, and hash join.

Phase 7A is implemented as a deliberately narrower first slice. The public `query()` and
`prepareQuery()` paths decode append/base physical blocks directly into preallocated vectors and
replay insert, upsert, update, and delete segments through typed mutation workspaces, then execute
against typed boolean, number, datetime, and dictionary-coded string vectors with packed validity
bitmaps. Neither path builds a full boxed-value copy. Scans advance in 2,048-row source batches, and duplicate-match
join fan-out is passed through the remaining operators in reserved typed chunks. Multi-table snapshot
preparation shares one transaction/segment visibility catalog, and metadata-only append/base scans do
not load an arbitrary data column. Filters, projection, limits, core
aggregates, grouped aggregation, ordering, and equi-joins use this executor; row objects are created
for the returned `QueryResult` only at the public API boundary.

Phase 7B-A adds a modeled query memory context without claiming the phase exit. Public query options
accept `executionMemoryBudgetBytes`, prepared queries report current and peak accounted bytes, and
typed-vector payloads plus scan, selection, join row-index, and chunked fan-out buffers reserve and
release against the shared budget. Exact-boundary tests cover success, pre-allocation rejection, and
cleanup after both success and failure.

Phase 7B-B extends the same context to growing group and output state. Counts and sums live in typed
numeric arrays rather than boxed accumulator objects. Group entries reserve logical key payload and
fixed aggregate slots, retained `MIN`/`MAX` values replace their reservations, and result construction
reserves row-reference plus tagged scalar payload. Ordering uses an explicit stable typed-index
merge/cycle sort with fully reserved scratch; LIMIT truncates in place. Exact-boundary tests cover grouped string state, accumulated results,
ordering, limiting, failure atomicity, and cleanup.

Phase 7C-A replaces the nested JavaScript grouping maps with an insertion-ordered byte key index.
Canonical typed/compound keys live in a growable arena addressed by collision-checked typed bucket,
chain, hash, offset, and length arrays. Growth reserves replacement capacity before allocation, keeps
old and new buffers in the high-water mark while copying, and releases old reservations afterward.
Direct and SQL-level tests cover type boundaries, compound-key framing, `-0`, growth, budget failure,
cleanup, insertion order, and randomized grouping parity. The group lookup-or-insert hot path encodes
and hashes a new key once and writes exact contiguous bytes without a boxed per-byte staging array.

Phase 7C-B replaces the general hash-join `Map` and boxed duplicate arrays with a scalar byte key
index and typed build-row chains. Exact encoded bytes resolve hash collisions, typed tags preserve SQL
key distinctions, duplicate traversal retains build order, and every retained arena/index growth is
reserved before allocation. Dense unique integer joins keep their direct typed lookup. Tests cover
hash collisions, typed keys, duplicate order, `-0`, SQL `NaN` behavior, exact budget failure, and
row/vector parity.

Phase 7 remains open. Preparation still materializes every projected typed input column in full before
the modeled reservations take effect, and mutation replay can temporarily retain a typed slot
workspace plus its compacted output. Returned result objects, group-key and retained aggregate
reference containers, property and JavaScript array-capacity overhead, encoding temporaries, caller-owned
result lifetime, and allocator overhead are not counted; there is no spill path. Configured exhaustion
fails instead of spilling, while the default remains effectively unbounded. Phase 7A/B/C-A also do not
perform segment or row-group data skipping. Later Phase 7 work must stream inputs, replace the
remaining boxed growing containers, and spill within a hard working-set budget; Phase 8 owns
statistics-driven pruning and late materialization.

The schema-less empty-table row-adapter compatibility path rejects configured budgets rather than
silently escaping the model; catalog-backed `BrowserDatabase` empty tables stay on the typed path.

Exit gate: hot paths avoid row-object materialization and per-row callbacks; operator memory is accounted against a configured budget.

## Phase 8 — Filtering and data skipping

Deliver automatic min/max, null count, approximate distinct, dictionary membership, and optional Bloom metadata; segment/row-group pruning; predicate-column-first reads; late materialization.

Phase 8A implements a conservative first slice for one append/base table. Simple `AND`-combined
number/datetime column-to-literal predicates checksum-validate and physically decode predicate blocks
before trusting persisted min/max and null counts, skip impossible groups before logical vector
materialization, evaluate candidate predicate vectors into a typed selection, and late-load projected blocks only for candidate groups before compacting exact
matches. Joins, mutation replay, strings, computed predicates, and layouts without aligned block
counts fall back to the full scan. Predicate values are still fetched as complete IndexedDB records;
header-only metadata access, segment summaries, richer statistics, and the selectivity benchmark
matrix remain open. The authenticated physical decode is deliberate because version-zero metadata
JSON does not carry an independent checksum.

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
multi-column ORDER BY, and LIMIT. Unsupported syntax fails explicitly. Phase 7A routes this subset
through its initial columnar executor, but does not complete Phase 12: typed logical/physical plans,
DISTINCT, subqueries, sets, CTEs, windows, and SQL mutations remain outstanding.

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
four-way durability matrix. Phase 6A through Phase 6E-A now provide durable, restart-safe,
output-block-stepped physical rechunking, bounded oldest-prefix L0-to-L1 selection, keyed mutation
merging with stable row IDs, immutable append-row-range L2 partitions with a hard published-output
byte budget, safe cooperative job cancellation, and lease-aware physical reclamation for artifacts
with persisted provenance. Phase 6 remains open for keyed/clustered multi-range L2, failed-attempt
lifetime amplification accounting, spillable/resumable merge planning, chunked planning/indexed
roots, and broader orphan/catalog/job cleanup. The broader Phase 5 performance curves and
larger/repeated benchmark tiers remain outstanding.

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
- [x] Merge keyed upsert/update/delete deltas into row-ID-preserving full-row base segments.
- [x] Add bounded oldest-prefix L0 -> L1 selection with persisted incremental amplification inputs.
- [x] Add immutable append-row-range L2 partitions with a hard published-output byte budget.
- [ ] Build keyed/clustered multi-range L2 selection and failed-attempt lifetime amplification
      accounting.
- [x] Add safe compaction-job cancellation.
- [x] Add lease-aware garbage collection and physical reclamation for superseded blocks.
- [x] Route the public SQL subset and mutation replay through the Phase 7A columnar executor.
- [x] Add the Phase 7B-A modeled query memory context for vector and row-index buffers.
- [x] Add Phase 7B-B logical group/result payload and ordering/limit workspace accounting.
- [x] Replace nested grouping maps with a reserved byte-key arena and typed hash index.
- [x] Replace hash-join maps and duplicate arrays with a reserved byte-key index and typed row chains.
- [x] Add numeric/datetime zone-map row-group pruning and predicate-first projected-block loading.
- [ ] Replace remaining boxed containers, stream projected inputs, and spill under the query budget.
- [x] Provide `npm run check:release` for quality checks plus both real-browser suites.

## Result recording

Every checked-in benchmark result must include date/time, browser and version, operating system, logical/stored bytes, dataset shape, codec, block size/count, durability, write/read/decode/aggregate timings, verification result, and known environmental caveats. Results are observations, not promises, and should never be silently compared across materially different environments.
