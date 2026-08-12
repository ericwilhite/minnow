# Minnow Roadmap

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

The dashboard's default correctness workload is a deterministic 50-table commerce graph. One scale
multiplier grows every dimension, bridge, transaction, and ledger table. It validates primary keys,
81 foreign-key paths, value domains, and transaction coverage before running 15 oracle-checked
reference queries. A measured, read-only ad-hoc reference SQL console remains explicitly separate
from library timings. The library now has a correctness-first native `query()`/`prepareQuery()` SQL
subset for projections, filters, equi-joins, grouping, core aggregates, ordering, and limits. The
engine comparison uses that public API and requires matching checksums. The checked-in 2026-08-11
three-engine record measures MinnowDatabase SQL for the first time: at scale 10 in Chromium and
Firefox, all 15 reference queries execute with oracle-verified results and summed repeated-execution
medians beat SQLite Wasm and PGlite in both browsers. The same-day prepare-cache follow-up record
halved the remaining per-statement prepare cost with one batched catalog read
(`BlockStore.getQueryCatalogState`), a shared internal reader lease per manifest version, and a
byte-bounded prepare cache (`prepareCacheBytes`, default 64 MiB) keyed by exact visible segment
ids over assembled column vectors, zone-pruned projections, and derived-block results; execution
medians were unchanged and repeated same-statement prepares fell to millisecond scale. The Phase 7A columnar
executor now backs the public subset; memory-accounted execution, statistics, broader SQL, and
cost-based optimization remain planned in Phases 7–13.

A 2026-08-11 optimization pass (measured with a Node/vite-node micro-benchmark over
`MemoryBlockStore` at 200k rows — not browser evidence; the reference suite should be re-captured
before publishing numbers) reworked the hot paths without changing the public API. Query memory
accounting drops its per-row reservation objects and per-string UTF-8 encodes in favor of an
aggregated tally at one byte per UTF-16 code unit; the same estimate now backs write metrics'
logical bytes. A 512-statement LRU plan cache removes the double SQL compile per `query()`. The
block format moved to version 1, whose envelope checksum authenticates the header and metadata
JSON, letting zone-map pruning trust statistics header-only instead of decompressing and
revalidating predicate blocks it is about to discard. Decoded physical blocks now cache by
immutable block id inside the `prepareCacheBytes` budget, so a commit that invalidates every
assembled-vector fingerprint re-pays vector assembly but not fetch/decompress/validate. Result
projection builds rows by direct property assignment, ORDER BY extracts sort keys once per row and
shares one `Intl.Collator`, the raw codec no longer copies in either direction, string encode
writes through `encodeInto` into a single content buffer, and the row-to-column pivot reads each
row object once. Micro-benchmark medians: ordered scans and wide string projections 3–6× faster,
repeated filtered queries up to 10× faster, single-batch insert throughput about 2× (322k to
~650k rows/s), and post-commit re-preparation ~25% faster with zero store reads.

A same-day second round targeted the profiled residuals. Grouped queries went 2.7× faster
(27.4 → 10.1 ms at 200k rows): compound GROUP BY over dictionary-coded string columns now
direct-addresses a combined-code slot array (no hashing, capped at 65,536 slots with a byte-index
fallback), group-state miss factories stopped allocating a closure per row, and aggregates over
bare number columns read their Float64Array slots without interpreter dispatch. On the write path,
staging became one atomic storage transaction (`stageTransactionArtifacts`: blocks + segment +
journal, with a sequential fallback that fault injection deliberately exercises), and the chunked
unique-key lookup became log-structured with a sixteen-chunk tail folded into per-key base records
at commit time. Sustained small-batch inserts over fake-indexeddb (100-row batches, quartile
ms/batch over 800 commits) went from 10.0 → 29.1 → 49.0 → 69.6 (linear per-commit growth) to
13.1 → 21.7 → 29.7 → 37.4; the crossover sits near 250 commits and the gap keeps widening. The
residual per-commit growth was the full-manifest rewrite: every commit re-sorted and rewrote the
complete live block-id list and re-read it at snapshot load, so commit cost still scaled with
total database size. A third same-day pass landed the delta-manifest format: manifests store as
checkpoint-every-32 plus per-commit added/removed ids, reads resolve through the chain (public
`Manifest` views are unchanged), `CommitTransactionInput` carries only the delta, commit returns
a `ManifestSummary`, snapshot/lease validation walks the record chain instead of probing every
live block, and GC reachability and pruning resolve with one ascending pass over the (tombstoned,
never-deleted) records. The same 800-commit fake-indexeddb curve flattened to
9.7 → 11.6 → 12.5 → 13.1 ms/batch — 5.3× faster at the tail than the original and no longer
growing linearly per commit. A 2026-08-12 real-browser re-capture
(`benchmarks/results/engine-comparison-2026-08-12.md`) confirms the passes end to end: scale-10
dataset inserts 1.85× faster in Chromium (1.24× Firefox), summed prepares a further 1.45×/1.21×
lower, summed execution medians 1.31×/1.27× lower, competitors unchanged within noise, and all 15
reference queries oracle-verified in both browsers. COALESCE and
DATE_TRUNC turned out to be fully implemented (parser through executor and the SQL feature
matrix), so their checklist item is now ticked; streaming keyed-mutation scan inputs and spilling
hash-join build sides remain the open executor checkbox.

Initial matrix:

- compressed block targets: 256 KiB, 512 KiB, 1 MiB, 2 MiB, 4 MiB;
- datasets: quick local smoke tests first, then 100 MiB, 1 GiB, 5 GiB, and 10+ GiB where quota allows;
- memory budgets to preserve in future query tests: 16, 32, 64, 128, and 256 MiB.

The dashboard automates the raw/RLE/gzip comparison across all five block sizes and can preserve a
raw result bundle from Playwright. The checked-in 2026-08-08 27-table scale-100 Chromium and
Firefox runs cover 1,930,800 rows and about 90 MiB of encoded payload per cell; the 2026-08-09
50-table scale-10 refresh covers 956,160 rows and about 46 MiB per cell. All cells in both records
verified, and the refreshed observations remain consistent with the decision
record, which selects gzip with a provisional 2 MiB target for storage-oriented benchmark and physical
rewrite work. Phase 6B uses that as its default estimated uncompressed physical target and persists
the selected byte target with each rewrite job; ordinary row-partitioned writes remain separately
configurable. A 50-table scale-100 capture is currently infeasible because each matrix cell reruns
the measured per-cell reference suite; bounding that work, larger quota-dependent tiers, and
repeated-sample distributions remain open.

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

The 2026-08-08 1.93-million-row comparison records 303,723 MinnowDatabase rows/s after the key
chunk and batch-staging optimization, versus 213,557 for persistent SQLite, 87,938 for persistent
PGlite, and 1,589,803 for in-memory DuckDB in the same host Chromium run. MinnowDatabase's total
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
- transient internal reader leases protect `MinnowDatabase` table/query materialization, while
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

Remaining Phase 6 delivery (keyed multi-range L2 selection and lifetime write-amplification
accounting for cancelled/aborted attempts landed 2026-08-12; see the checklist entry):

- key-range (clustered) L2 partition rewrite, so mutations referencing keys inside published
  partitions can fold instead of accumulating as replayable level-zero history;
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
preparation shares one segment visibility catalog and batch-fetches only referenced transaction
owners in 64-ID windows; it does not scan the complete transaction history. Metadata-only append/base
scans do not load an arbitrary data column. Filters, projection, limits, core
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

Phase 7D-A adds durable temp-run pages plus asynchronous external merge sort and partitioned hash
aggregation. Budgeted `MinnowDatabase.query()` spills ungrouped ordered output (including joins) or
single-table grouped ordered output, pairwise-merges fixed pages, applies LIMIT at the final read, and
removes every owner/run page after ordinary success or failure. Spill owners now register and renew
durable leases while executing, and `cleanupQuerySpill()` reclaims pages whose owner lease is expired
or missing at a fixed cutoff—covering abrupt tab/process loss—while atomically retaining owners that
renewed concurrently. Synchronous
prepared execution remains a no-I/O fast path; `executeAsync()` exposes the spill-capable path.

Phase 7E-A adds sliding-window scan streaming for budgeted single-table append/base
`MinnowDatabase.query()` plans without joins. The executor awaits a window load before every scan
batch in each asynchronous execution path, bound expressions read through per-vector resident
windows, and the loader decodes whole blocks forward-only, reserving fixed window bytes before
allocation and measured per-window dictionary bytes before installation. Under a budget, streaming
takes precedence over Phase 8A pruning. Tests cover a table too large to
materialize inside the budget, grouped and order-spilled parity against the materialized path, and
the keyed-mutation fallback.

Phase 7E-B makes partitioned hash-aggregate spill value-carrying: partition pages hold each
surviving row's evaluated group keys and aggregate arguments instead of scan-row indexes, and the
partition phase accumulates groups from decoded values without re-reading source vectors. This
removes the streaming exclusion for grouped-and-ordered single-table plans and extends the spill to
grouped ordered equi-joins with materialized build sides. Buffered values are reserved per row and
flushed in fixed 512-row scan chunks. Budgeted unordered grouped plans reuse the same partitioned
path—the empty ordering merges as a stable concatenation—bounding peak accounted group state to one
partition at the cost of partition-order rather than first-appearance group output, which SQL
leaves unspecified without `ORDER BY`. Hash-join build sides and DISTINCT remain unspillable.

Phase 7E-C streams the probe side of joined plans: the scanned base table uses the sliding window
while join build sides, including keyed mutation replay, are materialized at the same leased
snapshot. Self-joins keep the materialized path. Tests cover ordered, grouped-ordered, and
left-join parity against the materialized path with an upsert-replayed build table and a base too
large to materialize inside the budget.

Phase 7 remains open. Prepared queries and non-streamed shapes still materialize every projected
typed input column in full before
the modeled reservations take effect, and mutation replay can temporarily retain a typed slot
workspace plus its compacted output. Merge planning updates source slots in place and emits row-ID
spans and column ranges in one pass, but still retains a whole-plan key map and source slots. Returned
result objects, group-key and retained aggregate
reference containers, property and JavaScript array-capacity overhead, spill serialization/native
IndexedDB work, caller-owned result lifetime, allocator overhead, and the prepare cache's retained
bytes (byte-bounded separately by `prepareCacheBytes`) are not counted. Unordered
grouped state, hash-join build sides, and DISTINCT still have no spill path, single-table global
aggregates rely on streamed input plus bounded accumulator state rather than a spill, and the
default remains effectively unbounded. Later Phase 7 work must stream joined and mutation inputs,
replace the remaining boxed growing containers, and cover those operator shapes within a hard
working-set budget.

The schema-less empty-table row-adapter compatibility path rejects configured budgets rather than
silently escaping the model; catalog-backed `MinnowDatabase` empty tables stay on the typed path.

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

Phase 12 is delivered as a breadth-complete correctness-first surface. The dependency-free
lexer/parser compiles every statement into one shared compiled-plan representation
(`CompiledQuery`/`CompiledStatement`) consumed identically by the row-reference executor, the
columnar executor, and the public `query()`/`prepareQuery()`/`execute()` APIs. The surface covers
SELECT with DISTINCT, aliases, inner/left equi-joins, AND comparisons with IN/NOT IN and
uncorrelated scalar and membership subqueries, arithmetic, core aggregates, ROUND, GROUP BY,
HAVING, multi-column ORDER BY, LIMIT, non-recursive CTEs, derived tables, top-level
UNION/UNION ALL, ROW_NUMBER/RANK/DENSE_RANK windows, INSERT ... VALUES, and keyed UPDATE/DELETE.
DISTINCT desugars into grouping, windows desugar into windowed sources, unions fold positionally,
and subqueries resolve post-order at the same leased snapshot. The machine-readable feature matrix
(`packages/engine/sql-feature-matrix.json`) records each supported and rejected form with an
executable example verified by a conformance test. A separate optimizer-facing logical/physical
plan split with plan snapshots is deferred to Phase 13, where it becomes load-bearing. A later
completeness pass closed the remaining breadth: full boolean WHERE/HAVING trees (OR/NOT/parens
under three-valued logic, with top-level ANDs still split into the classic predicate list so
pushdown, zone maps, and the dictionary fast path are unchanged), LIKE, CASE, uncorrelated
EXISTS, NOT BETWEEN, LIMIT OFFSET, INTERSECT/EXCEPT with standard precedence, aggregate window
functions with the SQL default frame, RIGHT JOIN as the mirrored sole join, non-equi and
multi-key joins via a nested-loop fallback, and WITH RECURSIVE with linear delta recursion under
explicit iteration and row caps. Correlated subqueries, FULL OUTER joins, explicit window
frames, and DDL remain explicitly rejected.

Exit gate: SQL compiles into the same logical representation used by all other APIs; unsupported syntax fails explicitly rather than silently changing semantics.

## Phase 13 — Optimizer

Deliver deterministic rewrites first: constant folding, projection pruning, predicate/limit/aggregate pushdown, segment pruning, late materialization, and dictionary rewriting. Then add collected statistics and cost-based join order, strategies, parallelism, and spill decisions.

Phase 13A implements the deterministic core. Every compiled statement passes through one
plan-to-plan rewrite pass: constant arithmetic and ROUND fold when the result stays finite,
predicates push into derived and CTE sources on the base and inner-join sides (group-key outputs
only for grouped inners, never across a left join), unreferenced projections prune out of plain
derived blocks (grouped and DISTINCT blocks keep their semantics-bearing select lists), and outer
LIMIT combines into an unordered derived base. `renderPlan()` produces stable snapshots verified
per rule, `MinnowDatabase.explain()` reports the optimized plan plus physical strategy notes, and
the differential fuzzer compares optimized columnar execution against the raw-plan row reference,
so every rewrite sits inside the correctness net; an 80-seed sweep of 9,600 randomized queries
passed. Zone-map segment pruning from Phase 8A and streamed/late-loaded scans from Phase 7 remain
the physical layer these rewrites feed.

Phase 13B adds the cost-based slice this engine can decide today. Preparation collects exact
visible row counts for every input — including executed derived tables — and a single inner
equi-join swaps its scan and build sides when the joined table is more than twice the base, so
the built index always covers the smaller input; left joins and wildcard selects keep the written
order, and multi-join reordering waits for a cost model over richer statistics. String equality
predicates rewrite to per-row dictionary-code comparison with a per-window cached code, and the
existing optimistic execute-then-spill policy is the shipped spill decision: a budgeted query
first attempts bounded in-memory execution and takes the durable spill path only on budget
exhaustion. The checked-in `optimizer-rules-2026-08-10.md` record measures each rule on a
200,000-row workload: 2.42x for CTE predicate pushdown, 3.87x for derived projection pruning, a
16% lower accounted peak for build-side selection, and 1.12x for the dictionary rewrite including
preparation. Partial aggregation and aggregate pushdown belong to Phase 9, parallelism decisions
require Phase 11's workers, and workload-adaptive statistics beyond exact row counts belong to
Phase 19.

Exit gate: plan snapshots and workload benchmarks demonstrate each rule's correctness and value.
Met by the per-rule snapshot tests, the raw-versus-optimized differential net, and the checked-in
rule-value benchmark record; the explicitly re-scoped items above carry their phase homes.

## Phase 14 — Schema DSL and migrations

Deliver stable table/column IDs, relational type constructors, catalog compiler, Standard Schema-compatible runtime metadata, relations, and automatic migration planning. Prefer metadata-only compatible evolution and piggyback rewrites on compaction.

Phase 14 is delivered as metadata-only evolution over the existing stable column IDs. The typed
DSL builds tables from `column.boolean/number/string/datetime` constructors with `.nullable()`,
`.unique()`, `.renamedFrom()`, and `.references()`; `InferRow`, `InferInsertRow`, and
`InferUpdateChanges` provide the compile-time select/insert/update types, verified by type-level
tests, and `typedTable()` wraps the batch APIs in those types. Each table definition carries a
Standard Schema-compatible `~standard` validator, and declared relations validate against the
schema at definition time (they are catalog metadata, not write-time constraints).
`planMigration` diffs the live catalog into deterministic steps — create table, add nullable
column, rename column through its stable ID, widen nullability — and rejects everything else
explicitly: type changes, drops, unique-key changes, non-null tightening, and non-nullable
additions. `MinnowDatabase.migrate()` executes the plan idempotently with one atomic
compare-and-swap per catalog alteration on a new revisioned `updateTable`, so an interrupted
migration completes by re-running and a concurrent migrator fails with a typed conflict. Because
every supported step is catalog-only, no rewrite piggybacking is needed yet: columns added after a
segment was written read as NULL through every path (append scans, keyed replay, row reads), and
the streamed and pruned paths conservatively fall back for such segments. Physical rewrites, and
compaction-time normalization of evolved segments, arrive with future incompatible-change support.

Exit gate: compile-time select/insert/update types and crash-safe migration tests across supported schema changes. Met.

## Phase 15 — Type-safe ORM

Deliver CRUD, relational query builder, typed results/nullability, batch APIs, and raw SQL escape hatch. ORM expressions construct logical plans directly; they do not generate SQL strings.

Phase 15 is delivered on the shared compiled-plan representation. `refs()` produces typed column
references from schema tables, expression builders (`eq`/`gt`/`inList`/arithmetic/aggregates/
`round`) assemble the same expression nodes the SQL parser emits, and `from().join().where()
.groupBy().having().select().distinct().orderBy().limit().build()` constructs a `CompiledQuery`
directly — never SQL text — then runs the same deterministic optimizer, including the parser's
DISTINCT-to-grouping desugaring. `select()` shapes infer the result row type, `nullableRefs()`
carries explicit `| null` typing for left-joined columns, and `MinnowDatabase.run()` executes
built queries through the same preparation pipeline as SQL. CRUD and batch writes are the Phase 14
`typedTable` handles, and `query()`/`execute()` remain the raw SQL escape hatch. The exit gate is
met by structural plan-equality tests — ORM plans compare deeply equal to compiled SQL across
filtered projections, grouped joins with HAVING, DISTINCT, left joins, and expression shapes —
plus result-equality execution tests and compile-time row-type assertions. Correlated builder
subqueries, relation-aware join sugar from `.references()`, and set operations in the builder
remain open follow-ups.

Exit gate: ORM and equivalent SQL produce equivalent bound logical plans and results. Met.

## Phase 16 — Live queries

Deliver persisted change sets, dependency extraction, cross-tab hints, version reconciliation, and correctness-first selective re-execution. Add incremental maintenance gradually for filters and core aggregates, always retaining re-execution fallback.

Phase 16 delivers correctness-first selective re-execution. Every transaction commit persists its
changed table IDs in the manifest — derived automatically from staged segments, with compaction
marking itself logically unchanged so supersession never triggers reruns — and
`MinnowDatabase.liveQueries()` builds subscription sets whose every hint path (local commit,
injected cross-tab channel message, poll tick, or explicit `refresh()`) converges on one
authoritative check: read the durable manifest version through a version-only store read, and when
it moved, union the change sets of the intervening manifests in bounded 64-record pages. A missing
version or a manifest without a change set widens conservatively to every subscription, so missed
channel messages and suspended tabs delay a refresh but cannot produce stale results.
Subscriptions retain their SQL, dependency table IDs extracted from the full plan (including CTEs,
derived tables, and subqueries), and a numeric result digest — never rows — and a rerun whose
digest is unchanged suppresses its notification. Stats expose hints, version checks, sweeps,
reruns, avoided reruns, suppressed notifications, and sweep latency. Incremental maintenance for
filters and core aggregates remains the planned follow-on, always retaining this re-execution
fallback.

Exit gate: missed BroadcastChannel messages, suspended tabs, and unrelated commits cannot create stale results; latency and avoided reruns are measured. Met.

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
byte budget, safe cooperative job cancellation, lease-aware physical reclamation for artifacts
with persisted provenance, keyed multi-range L2 promotion through the merge path, and lifetime
write-amplification accounting that charges cancelled and aborted attempts against their retry's
ceiling. Phase 6 remains open for key-range (clustered) L2 partition rewrite,
spillable/resumable merge planning, chunked planning/indexed roots, and broader
orphan/catalog/job cleanup. The broader Phase 5 performance curves and larger/repeated benchmark
tiers remain outstanding.

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
- [x] Build keyed multi-range L2 selection and failed-attempt lifetime amplification
      accounting (2026-08-12). A keyed table's [partitions][optional anchor][L0 prefix] history
      now promotes (anchor + oldest prefix) through the merge path into a new level-2 partition:
      a full-row base segment carrying its merged row-ID spans and the next ordinal, with the
      same immutable amplification policy fields as append-row-range promotions (anchor bytes
      never count toward the L0 ceiling). A prefix whose mutations reference keys frozen into
      published partitions skips with `keys-outside-selected-sources` — those deltas stay as
      replayable level-zero history, because folding them requires key-range partition rewrite,
      which remains open. Lifetime accounting: attempts at promoting the same manifest version's
      sources share one ceiling — bytes written by cancelled or aborted attempts persist as
      `priorAttemptOutputStoredBytes` and reduce the retry's `maximumOutputStoredBytes`, results
      report `lifetimeOutputStoredBytes`, and a starved ceiling skips with
      `write-amplification-budget` before any write.
- [ ] Key-range (clustered) L2 partition rewrite: fold mutations that reference keys inside
      published partitions by selecting and rewriting the intersecting partitions as merge
      sources, with rewritten-partition bytes entering the lifetime amplification accounting.
- [x] Add safe compaction-job cancellation.
- [x] Add lease-aware garbage collection and physical reclamation for superseded blocks.
- [x] Route the public SQL subset and mutation replay through the Phase 7A columnar executor.
- [x] Add the Phase 7B-A modeled query memory context for vector and row-index buffers.
- [x] Add Phase 7B-B logical group/result payload and ordering/limit workspace accounting.
- [x] Replace nested grouping maps with a reserved byte-key arena and typed hash index.
- [x] Replace hash-join maps and duplicate arrays with a reserved byte-key index and typed row chains.
- [x] Add durable external sort and single-table partitioned hash-aggregate spill paths.
- [x] Add numeric/datetime zone-map row-group pruning and predicate-first projected-block loading.
- [x] Reclaim spill pages abandoned by abrupt tab/process loss through durable spill-owner leases.
- [x] Stream budgeted single-table append/base scan inputs through block-aligned resident windows.
- [x] Carry evaluated values in hash-aggregate spill partitions, covering grouped-and-ordered
      streams and grouped ordered joins.
- [x] Stream the joined probe side with build sides materialized at the same leased snapshot.
- [x] Spill unordered grouped state through the value-carrying partitions.
- [x] Add SELECT DISTINCT as compiled grouping and HAVING as shared group-finishing filters.
- [x] Stream keyed-mutation scan inputs (2026-08-12): a budgeted scan over a keyed history of
      update and delete segments now replays key visibility into resident state bounded by the
      mutation size — mutation key/changed-column vectors, a dead-row bitmap, and per-slot patch
      references, built from one block-at-a-time pass over the scan segments' key columns
      tracking only mutation-touched tokens — and streams the base rows through the existing
      block-aligned resident window, compacting dead rows and overlaying patches per window. Row
      order and values are exactly the materialized replay's; histories containing upsert
      segments keep the materialized path because upsert-new rows interleave into slot order at
      their segment position (compaction folds upserts into full-row base segments, so the
      restriction shrinks over a table's lifecycle).
- [x] Partition oversized hash-join build sides (2026-08-12). Implemented as
      partition-by-rescan rather than the sketched spill pages, with the same bounded-memory
      guarantee and no new persistent format: for a single unordered/ungrouped inner equi-join
      whose build side's size estimate exceeds a quarter of the budget, each of P passes
      (power of two sized so a partition fits in an eighth of the budget, capped at 64)
      re-streams the build table keeping resident only rows whose avalanche-mixed join-key hash
      falls in the pass's partition, then runs the unchanged plan against a fresh streamed base
      scan. Equal keys share a partition, so each inner match occurs in exactly one pass;
      NULL/NaN build keys drop at partition time; datetimes hash as their epoch milliseconds so
      coerced equality keeps partitions aligned; LIMIT/OFFSET strip per pass and re-apply after
      early-stopping collection. Both inputs stream through the existing block-aligned windows
      (mutation histories included), the decoded-block cache keeps the P rescans cheap, and row
      order across passes is implementation-defined like any unordered query. Grouped joins
      await partial aggregation (Phase 9), ordered joins a final re-sort pass, and left joins a
      per-pass probe-ownership filter.
- [x] Add SELECT DISTINCT, HAVING, IN, uncorrelated subqueries, CTEs, derived tables,
      UNION/UNION ALL, and ROW_NUMBER/RANK/DENSE_RANK windows to the SQL surface.
- [x] Add SQL mutations through execute() with keyed read-then-mutate semantics.
- [x] Check in the machine-readable SQL feature matrix with a conformance test.
- [x] Add the deterministic optimizer core: folding, derived/CTE predicate pushdown, projection
      pruning, LIMIT combining, plan snapshots, and explain().
- [x] Add exact-count join build-side selection, the dictionary-equality rewrite, and the
      checked-in optimizer rule-value benchmark record.
- [x] Add the typed schema DSL, Standard Schema validators, and crash-safe metadata-only
      migrations with catalog compare-and-swap.
- [x] Add the type-safe ORM builder with plan equivalence to compiled SQL.
- [x] Add live queries with persisted change sets, digest notifications, and selective
      re-execution.
- [x] Close SQL gaps for IS [NOT] NULL, inclusive BETWEEN, COUNT(DISTINCT), and constants
      alongside aggregates; add the aligned bulk decode fast path and version-only manifest
      reads.
- [x] Execute the 15 reference queries as SQL through `prepareQuery()`, decide engine support by
      compiling during the run, and verify results against independent JavaScript oracles.
- [x] Show optimized `explain()` plans and the checked-in SQL feature matrix in the dashboard.
- [x] Record whole-agent memory and JavaScript heap for the storage run and for all four compared
      engines, sampled on the main thread.
- [x] Add date truncation and `COALESCE` so the monthly cohort and adjustment-burden reference
      queries compile; they are the only two the current surface cannot express.
- [x] Provide `npm run check:release` for quality checks plus both real-browser suites.
- [x] Add auto-increment keys and declarative column defaults (`uuid`/`nanoid`/`now`/literals):
      counters reserved atomically with transaction begin (cross-tab safe, explicit values bump
      past their maximum), generated values echoed through `returning`/RETURNING, and
      metadata-only `alter-default` migrations.
- [x] Add zero-ceremony full-text search: document-level `MATCH(cols | *) AGAINST` and
      `BM25(...) AGAINST` relevance over string/number/datetime columns with a deterministic
      versioned tokenizer (NFKC, CJK bigrams, `*` prefixes), dictionary-code scan matching under
      the query memory budget, and `.search()` / `db.search()` sugar with builder/SQL plan
      parity.
- [x] Add the persisted per-column full-text postings index (`fts-chunks-v1`) as a pruning
      accelerator for append-only histories: per-commit delta chunks written atomically with the
      manifest publish, a lazily scheduled background base build advertised through
      `TableRecord` state (`building`/`ready`/`invalid` with the stale-writer/keyed-mutation
      self-heal), segment-level MATCH pruning with scan re-verification, and fold-by-rebuild
      once the delta tail grows.
- [x] Serve exact BM25 statistics from full-text postings (per-term document frequency from
      posting unions, token totals from base+delta merges, every row a document), keyed to the
      scoring node's compiled signature and injected at bind — restoring index pruning for
      scoring plans and lifting their streaming restriction; a limited ORDER BY retains only
      the top rows, so ranked search is bounded by candidate segments plus k.
- [ ] Extend full-text indexing to keyed-mutation histories via key-range partition rewrites,
      and serve per-document scores directly from postings (row fetch-by-id) to skip candidate
      segments entirely.

## Result recording

Every checked-in benchmark result must include date/time, browser and version, operating system, logical/stored bytes, dataset shape, codec, block size/count, durability, write/read/decode/aggregate timings, verification result, and known environmental caveats. Results are observations, not promises, and should never be silently compared across materially different environments.
