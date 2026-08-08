# BrowserDatabase Architecture

Status: foundational design, August 2026

BrowserDatabase is a browser-only relational database engine for large local datasets. It is designed around immutable compressed columnar storage, asynchronous worker execution, and IndexedDB transactions. The current implementation proves the storage and MVCC path, exposes a bounded correctness-first SQL subset, and leaves the unified vectorized planner and ORM surface for later phases.

## Architectural invariants

These are constraints, not preferences:

| Area                   | Decision                                                                                                     |
| ---------------------- | ------------------------------------------------------------------------------------------------------------ |
| Runtime                | Modern browsers only                                                                                         |
| UI thread              | Database work never executes on the main thread                                                              |
| Cross-origin isolation | No COOP/COEP requirement                                                                                     |
| Shared memory          | No `SharedArrayBuffer` or `Atomics` dependency                                                               |
| Baseline persistence   | IndexedDB                                                                                                    |
| Optional persistence   | OPFS may be added later behind the storage interface                                                         |
| Published data         | Immutable compressed columnar blocks                                                                         |
| Writes                 | Append-only delta segments, compacted asynchronously                                                         |
| Transactions           | Snapshot isolation through versioned MVCC manifests                                                          |
| Default durability     | IndexedDB `relaxed` durability                                                                               |
| Query execution        | Vectorized, streaming, and explicitly memory-bounded                                                         |
| Physical optimization  | Automatic statistics and data skipping; no user-managed indexes                                              |
| Concurrency            | Concurrent readers and write preparation; serialized metadata commits                                        |
| Coordination           | Correctness never depends on BroadcastChannel, Web Locks, page-close handlers, or successful background work |
| API direction          | Type-safe schema DSL and ORM, raw SQL, live queries, migrations, backup and restore                          |
| SQL direction          | SQL-standard-oriented parser, binder, planner, optimizer, and executor owned by this project                 |
| External engines       | SQLite, DuckDB, and other SQL engines are forbidden in the implementation                                    |

The durability contract ends at a successfully committed IndexedDB transaction. Visibility and page lifecycle signals may request earlier flushing, but cannot make browser shutdown reliable.

## System shape

```text
Application / UI thread
  - async API proxy only
  - request IDs, cancellation, streaming results
  - no encoding, storage, planning, or execution
                 |
                 | postMessage + transferable ArrayBuffers
                 v
Coordinator dedicated worker
  - catalog and snapshot ownership
  - transaction coordination
  - query planning and scheduling
  - live-query dependency tracking
                 |
        +--------+--------+
        |                 |
        v                 v
Storage modules      Executor workers (later)
  - IndexedDB          - segment-grained tasks
  - block codecs       - vector kernels
  - manifests          - bounded local caches
  - leases / GC        - partial results
```

Every tab owns a dedicated coordinator worker. Workers communicate by transferring ordinary `ArrayBuffer` instances. Parallelism is coarse-grained by immutable segment, so the system does not require a shared queue or shared buffer cache.

## Repository boundaries

The initial monorepo uses these boundaries:

```text
packages/
  block-format/       versioned binary containers, statistics, codecs
  storage-idb/        IndexedDB block and metadata persistence
  transactions/       snapshots, transaction journals, atomic commits, recovery
  worker-protocol/    stable UI-to-worker message protocol
  testing/            deterministic fault injection and test helpers
  engine/             coordinator worker and initial storage benchmark
apps/
  bench/              browser benchmark laboratory
benchmarks/
  results/            checked-in, environment-stamped measurements
```

Future packages may split vector execution, transactions, SQL, schema, ORM, live queries, migrations, and backup/restore. They should share logical plans and storage primitives rather than create parallel implementations.

## Logical and physical storage

The logical hierarchy is:

```text
Database -> Table -> Segment -> Row group -> Column block
```

- A segment is the immutable visibility and compaction unit.
- A row group is the primary pruning and vector-scan unit.
- A column block contains one encoded logical column for one row group.
- Inserts, updates, and deletes first land in append-only L0 deltas.
- Compaction rewrites deltas into larger query-oriented columnar segments.

No row is stored as an individual IndexedDB value, and no entire table is stored as one giant value. Initial block-size experiments cover 256 KiB, 512 KiB, 1 MiB, 2 MiB, and 4 MiB compressed blocks.

### Binary block container

Every block starts with a fixed-width, little-endian header containing:

- magic bytes and format version;
- logical type and encoding ID;
- compression codec ID;
- row count and null count;
- uncompressed and stored payload lengths;
- checksum of the uncompressed encoded payload;
- optional zone-map statistics in extensible metadata.

Readers reject unknown mandatory versions, invalid lengths, unsupported codecs, and checksum mismatches. Published formats are append-only: new codecs or metadata use new IDs/versioned extensions rather than silently changing old bytes.

Initial codecs are intentionally small:

| Logical data       | Initial representation                                                           |
| ------------------ | -------------------------------------------------------------------------------- |
| Boolean / validity | Bitmaps                                                                          |
| Signed integer     | Plain typed arrays first; frame-of-reference and bit packing next                |
| Timestamp          | Plain 64-bit first; delta and bit packing next                                   |
| Float              | Plain typed arrays                                                               |
| Text               | UTF-8 with offsets; dictionary encoding when beneficial                          |
| Compression        | Raw, byte RLE for deterministic tests, and browser-native gzip for the benchmark |

The codec interface operates on bytes and reports identity/version metadata. It is independent from IndexedDB and query execution.

### IndexedDB layout

The baseline database creates separate stores for:

```text
catalog, manifests, segments, blocks, transactions,
leases, statistics, temp, gc
```

Large immutable values live in `blocks`; small transactional control data lives in the remaining stores. A published block is written with add-if-absent semantics so accidental mutation is rejected. Revisioned compaction-job records share the `gc` store and survive coordinator or database restarts.

## MVCC and atomic publication

Each transaction records a transaction ID, snapshot manifest version, pending blocks, and a compact change set. Readers resolve all data through one immutable manifest.

Commit follows this order:

```text
encode/compress new blocks
        -> persist immutable blocks
        -> open a short IndexedDB metadata transaction
        -> compare expected manifest version
        -> publish manifest N + 1
        -> commit metadata transaction
```

The manifest never references a partial block. A crash before publication can leave unreachable blocks, which later garbage collection may safely reclaim. A compare-and-swap failure is a normal write conflict; the caller rebases or retries according to transaction semantics. Compaction uses the same atomic publication path: a new manifest replaces its planned source blocks with newly staged blocks. If a concurrent commit only appends data and all planned sources remain visible, compaction rebases and publishes without dropping or reordering that append. If a source changed, the job aborts rather than publishing a stale rewrite. Historical manifests and their source blocks remain unchanged, so replacement is not physical deletion.

The library now implements block writes, saved manifest history, stable snapshots, transaction records, competing-writer checks, and reader/backup leases. Garbage collection remains part of the MVCC milestone.

### Row identity and mutations

Tables receive a hidden immutable row ID. Its storage width is not part of the public type system. Writers atomically reserve ranges so they do not coordinate once per row. Inserts create immutable column segments. Upserts create newer keyed segments; reads use the newest row for each key while old snapshots keep the older value. Partial updates create narrow segments containing the key and changed columns only. Deletes create small key-only markers. Published blocks are never updated in place.

Tables with a unique key keep a small persistent key lookup in IndexedDB. The key changes and new manifest version commit together, so another tab cannot observe one without the other. Older tables that do not have this lookup remain correct by using a table scan until they are rebuilt.

Block storage supports bulk reads and writes. Insert/upsert batches encode one column at a time and
coalesce their produced blocks into one IndexedDB block transaction and one journal update; update
batches currently stage a column at a time. A materialized read fetches projected blocks for each
visible segment in windows of up to 16 block IDs and retains a unique-key column when delta replay
needs it.

### Resumable compaction foundation

Ordinary write segments are recorded at L0 with a `logicalOrder` derived from their commit version.
The first policy selects every eligible append-only segment visible in one manifest, requires
contiguous row-ID ranges, and publishes one L1 segment. Its `logicalOrder` is inherited from the
earliest source, so the consolidated segment remains in the same logical position even if a new L0
append commits while the job is running.

A compaction job persisted in the IndexedDB `gc` store records its source manifest, segment and block
IDs, deterministic output IDs, target level, transaction ID, revisioned state, processed rows, and a
source-segment/block cursor. `compactTableStep()` copies at most the requested number of blocks and
checkpoints the cursor; `resumeCompactionJob()` reopens the job's active transaction; and
`listCompactionJobs()` exposes the durable records. `compactTable()` repeatedly uses this machinery
as a run-to-publication convenience API.

Output blocks are deterministic byte-for-byte copies in this phase. If execution stops after the
immutable block or output segment is stored but before its transaction journal or job cursor is
updated, resume verifies that object and idempotently attaches it to the transaction. If the
transaction committed before the job reached `published`, resume reconciles the committed version
instead of publishing twice. Manifest publication supersedes the source blocks only in the new
version; prior manifests and their blocks remain readable by historical snapshots.

This is durable segment-consolidation scaffolding. It does not rechunk or re-encode blocks, enforce
a byte memory budget, compact upsert/update/delete deltas, select subsets or levels beyond the
whole-table L0 -> L1 policy, cancel a job, or physically reclaim source/output garbage. Lease-aware
reachability and reclamation remain separate future steps.

## Multi-tab correctness

IndexedDB is authoritative. The final metadata transaction serializes manifest publication across tabs and workers. Web Locks may later reduce wasted work around the short commit section, but the compare-and-swap in IndexedDB remains the correctness mechanism.

BroadcastChannel may announce that a new version exists. Receivers always reconcile against IndexedDB, so missed or duplicated announcements are harmless. Reader/backup leases are persistent records with expiry; they are not inferred from open channels.

## Worker and ownership model

The public API is asynchronous from its first release. Main-thread modules only validate protocol envelopes, post requests, and expose results. The coordinator owns database operations.

Large buffers cross worker boundaries as transferables. Transfer of ownership is explicit, and senders must treat transferred buffers as detached. Later executor workers use independent bounded caches and operate on assigned immutable segments. Duplicate cached pages are acceptable in exchange for eliminating shared-memory and cross-origin-isolation requirements.

The wire protocol is independently versioned. Requests carry protocol version, request ID, operation, and payload. Responses are discriminated success, failure, progress, or result messages. Unknown versions and operations fail explicitly.

## Vectorized bounded-memory execution

The future vectorized hot path will avoid arrays of row objects and per-row callbacks. Operators
consume bounded batches:

```ts
interface Vector<T extends ArrayBufferView> {
  values: T;
  validity?: Uint8Array;
}

interface Batch {
  length: number;
  columns: Vector<ArrayBufferView>[];
  selection?: Uint32Array;
}
```

The target executor gives every query and physical rewrite job a memory context. Operators reserve
and release bytes. Sort, hash aggregate, hash join, and distinct operations must spill to temporary
storage when reservations fail. Memory use is a function of the configured working set, not total
database size. The current block-count-bounded compaction scaffolding does not yet implement this
byte-budget contract.

## Automatic data skipping

There are no user-managed indexes. The engine records row count, null count, min/max zone maps, approximate distinct counts, dictionary membership, and optional Bloom filters per row group. Scans prune segments and row groups before loading predicate columns, evaluate a selection vector, then late-load projected columns.

Workload telemetry may eventually drive compaction order, clustering, and auxiliary per-segment structures. These structures are derived and disposable; logical correctness does not depend on them.

## SQL, schema, ORM, and live queries

SQL and the ORM converge on one typed logical plan:

```text
SQL -> lexer -> parser -> binder ------+
                                      +-> logical plan -> optimizer -> physical plan
Schema-aware ORM builder --------------+
```

The schema DSL owns stable table and column IDs and emits catalog definitions, select/insert/update types, runtime validation metadata compatible with Standard Schema, relation metadata, and migration intents. Automatic migrations compare persisted and application schemas; compatible changes remain metadata-only and physical rewrites are deferred to compaction where possible.

Live queries record table, column, and predicate dependencies. Commits persist a change set before any notification. The correctness baseline reruns a query when a relevant dependency changes; incremental maintenance is introduced only for proven-safe operator shapes.

A bounded row-oriented read-only SQL subset is implemented ahead of this architecture. Typed shared
logical/physical plans, the schema DSL, ORM, and live queries remain postponed until their lower
gates are satisfied.

## Backup, restore, and garbage collection

A backup pins immutable manifest N and streams its catalog, manifest, segment descriptors, referenced blocks, and checksums. Writers may continue publishing newer manifests. Restore writes and verifies blocks first and publishes the restored manifest last.

Garbage collection computes reachability across current manifests, active snapshots, backups, transactions, and unexpired leases. It removes only immutable blocks that are unreachable from every root. Recovery treats temporary and unpublished objects as reclaimable garbage.

## Durability and lifecycle

`relaxed` IndexedDB durability is the default. A later opt-in buffered mode may acknowledge before persistence and bound pending work by bytes, rows, and age. `visibilitychange` and `pagehide` can request immediate flushes, but documentation and APIs must distinguish “accepted by the worker” from “committed to IndexedDB.” Correctness never assumes a close-time promise completes.

## Fault model

Fault injection is required from the first storage implementation. Named points include before/after block write and before/after manifest commit. The suite will grow to cover abandoned writers, stale tabs, detached buffers, corrupted bytes, quota exhaustion, transaction aborts, missed notifications, and crashes between block persistence and manifest publication.

Core invariants under faults:

1. A visible manifest references only complete, checksum-valid blocks.
2. A failed or stale manifest comparison cannot publish changes.
3. Retrying an immutable block write cannot mutate already-published bytes.
4. Unpublished objects never affect reads and are safe to collect later.
5. UI responsiveness does not depend on operation size.

## Explicit non-goals for the foundation

- No full SQL surface, ORM, schema DSL, migrations, or live-query API yet; the current SQL subset is
  deliberately bounded and row-oriented.
- No claim of multi-gigabyte performance until browser measurements support it.
- No required OPFS, Web Locks, BroadcastChannel, SharedWorker, WASM, or SharedArrayBuffer.
- No user-facing index DDL.
- No compatibility promise for the experimental version-zero block format.

## Decision gates

The project advances only when evidence supports the current layer:

1. Typed vectors -> encode -> compress -> IndexedDB write/commit -> read -> decompress -> aggregate achieves acceptable throughput across browsers and block sizes.
2. Manifest publication remains atomic under injected faults and multi-context contention.
3. Write/delta and compaction amplification stay within explicit budgets.
4. Selective scans become materially cheaper as pruning selectivity increases.
5. Large operations respect configured memory limits and complete through spilling.

Measured failures should change the architecture before higher-level APIs make the decision expensive.

`npm run check:release` is the release-oriented verification gate: it runs formatting, lint,
typechecking, the build and unit tests, then both the real IndexedDB library suite and browser
dashboard suite.
