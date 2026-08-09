# BrowserDatabase Architecture

Status: foundational design, August 2026

BrowserDatabase is a browser-only relational database engine for large local datasets. It is designed around immutable compressed columnar storage, asynchronous worker execution, and IndexedDB transactions. The current implementation proves the storage and MVCC path, exposes an intentionally limited correctness-first SQL subset backed by an initial columnar executor, and leaves a fully memory-accounted vectorized planner and the ORM surface for later phases.

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
| Query execution        | Vectorized; streaming inputs, explicit memory budgets, and spill remain required exit gates                  |
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

No row is stored as an individual IndexedDB value, and no entire table is stored as one giant value. Initial block-size experiments cover 256 KiB, 512 KiB, 1 MiB, 2 MiB, and 4 MiB estimated pre-compression column-block targets.

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

Large immutable values live in `blocks`; small transactional control data lives in the remaining stores. A published block is written with add-if-absent semantics so accidental mutation is rejected. Revisioned compaction and garbage-collection job records share the `gc` store and survive coordinator or database restarts.

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

The manifest never references a partial block. A crash before publication can leave unreachable blocks, which later garbage collection may safely reclaim. A compare-and-swap failure is a normal write conflict; the caller rebases or retries according to transaction semantics. Compaction uses the same atomic publication path: a new manifest replaces its planned source blocks with newly staged blocks. If a concurrent commit adds later append or mutation segments and all planned sources remain visible, compaction rebases and publishes without absorbing, dropping, or reordering those later deltas. If a source changed, the job aborts rather than publishing a stale rewrite. Cancellation and publication serialize through storage transactions: cancellation wins by atomically marking the job `cancelled` and aborting its active transaction, or commit wins and cancellation observes `published` with the committed manifest version. Historical manifests and their source blocks remain unchanged, so replacement or cancellation is not physical deletion.

The library now implements block writes, saved manifest history, stable leased snapshots,
transaction records, competing-writer checks, reader/backup leases, and reachability-based physical
collection for artifacts with persisted provenance. Broader orphan and metadata cleanup remains part
of the MVCC/compaction work.

### Row identity and mutations

Tables receive a hidden immutable row ID. Its storage width is not part of the public type system. Writers atomically reserve ranges so they do not coordinate once per row. Inserts create immutable column segments. Upserts create newer keyed segments; reads use the newest row for each key while old snapshots keep the older value. Partial updates create narrow segments containing the key and changed columns only. Deletes create small key-only markers. Published blocks are never updated in place.

Mutation compaction preserves those identities with ordered `rowIdSpans`. Each span maps a positive
run of logical output rows to consecutive hidden IDs. Together the spans cover the output row count
exactly and do not overlap, but their numeric ID ranges may be out of order because reservation order
can differ from commit order. Matching upserts and updates retain the existing ID, deletes remove it,
and a newly inserted key uses its reserved ID. A non-empty merged output is an explicit full-row
`base` segment rather than an ordinary insert. This discriminator is also a rolling-version safety
property: readers treat it as full-row data, while an older append-only compactor rejects it instead
of silently discarding the span metadata.

Tables with a unique key keep a small persistent key lookup in IndexedDB. The key changes and new manifest version commit together, so another tab cannot observe one without the other. Older tables that do not have this lookup remain correct by using a table scan until they are rebuilt.

Block storage supports bulk reads and writes. Insert/upsert batches encode one column at a time and
coalesce their produced blocks into one IndexedDB block transaction and one journal update; update
batches currently stage a column at a time. A materialized read fetches projected blocks for each
visible segment in windows of up to 16 block IDs and retains a unique-key column when delta replay
needs it.

### Resumable physical compaction

Ordinary write segments are recorded at L0 with a `logicalOrder` derived from their commit version.
The L1 policy selects a canonical oldest prefix from one leased source snapshot: an optional single
L1 anchor followed by complete groups of L0 segments. It requires at least two L0 segments by default
and targets at most 16 L0 segments or 64 MiB of newly promoted L0 blocks. The minimum and an
indivisible equal-`logicalOrder` group can exceed those targets to guarantee progress without
changing replay order. Contiguous append-only inputs use `rechunk-v1`; keyed histories containing
upsert, update, delete, a prior base, or noncontiguous row IDs use `merge-v1`. Both publish at L1 and
inherit the earliest source `logicalOrder`, so an unselected or concurrently committed L0 suffix
remains after the consolidated output. A caller can explicitly lower the minimum to one to drain
`L1 + 1 L0`; a table without an anchor still requires at least two sources.

The Phase 6E-A policy is append-row-range L2, not clustered/key-range L2. A caller explicitly starts
it with `targetLevel: 2` on a non-keyed table. The visible layout must be one optional retained legacy
L1 insert, then L2 insert partitions with exact consecutive `partitionOrdinal` values `0..N-1`, then
ordinary L0 inserts. The planner takes only an oldest complete L0 prefix and emits one new L2
partition at ordinal `N`; neither the L1 nor any earlier L2 partition is a rewrite source. Once `N`
is positive, an omitted target automatically continues L2. This policy permits a minimum of one,
including direct one-segment L0-to-L2 promotion. A unique key, mutation/base segment, row-ID gap in
the selected sources, or malformed level/ordinal layout makes the policy skip without weakening its
append-only assumptions.

Ordinal-less level-two jobs persisted by the earlier generic target-level implementation remain
resumable for durable upgrade compatibility. They lack the four Phase 6E-A policy fields and are
therefore outside its amplification proof; their output is treated as a legacy unsupported layout,
not silently adopted as an ordinal partition.

A `rechunk-v1` plan fingerprints each ordered column block's ID, row range, stored/encoded length,
and checksum. It fixes output windows, schema IDs/types, contiguous row-ID bounds, compression, and
logical order. A `merge-v1` plan additionally fingerprints source segments in exact
logical/commit/ID order with normalized kind, key, level, row-ID spans, and unique source blocks.
Planning replays keys once and freezes the surviving value of every output cell as an ordered
source-block range. Each output column's ranges cover the logical output without gaps. That
immutable per-column map is the durable logical replay result; a cursor over an otherwise in-memory
key map would not be restart-safe.

Mutable job progress holds deterministic output IDs, the next output-window/column cursor,
transaction ID, revisioned state, processed rows and bytes, and the modeled executor-memory
high-water. New jobs also persist stored bytes attributed to newly promoted L0 inputs separately
from the rewritten L1 anchor. Completed results divide output stored bytes by promoted L0 bytes as
`compactionWriteAmplification`; summing those numerators and denominators across jobs produces the
cumulative incremental ratio. `compactTableStep()` processes at most the requested number of
output blocks;
`resumeCompactionJob()` continues the persisted plan after a yield or reopen;
`listCompactionJobs()` exposes it; and `compactTable()` repeatedly drives the same workflow to
publication. `cancelCompactionJob(jobId)` settles an unpublished job in a distinct terminal
`cancelled` state and returns the terminal state plus any published version. Repeated calls are
idempotent; terminal `published` and `aborted` jobs keep their existing outcome.

An L2 job adds four immutable policy fields: `outputPartitionOrdinal`,
`maxWriteAmplification`, `maximumOutputStoredBytes`, and
`plannedOutputStoredBytesUpperBound`. The ratio defaults to 16, and the exact job ceiling is
`floor(level0SourceStoredBytes * maxWriteAmplification)`. Planning measures each output payload and
its canonical metadata, then sums the complete block envelope plus the codec's maximum output to
obtain a conservative full-plan bound. If that upper bound exceeds the ceiling, planning returns a
`write-amplification-budget` skip before it persists a job, transaction, block, or segment. Otherwise
the fields survive restart unchanged, and execution checks cumulative actual stored full-block
bytes against both bounds before staging or reattaching each output block.

Each output is built directly from validated physical column ranges. The implementation decompresses
source blocks, slices and concatenates their validity/value buffers, recalculates canonical metadata,
and compresses the result without materializing column values as JavaScript row objects. Output
windows are shared across columns and derived from observed encoded density, then measured exactly
and split further when skew would exceed the target or the chosen executor budget. The defaults are
gzip, a 2 MiB estimated uncompressed physical target per output column block, and a 32 MiB
`memoryBudgetBytes` setting. A single row cannot be split and may exceed the target only when its
physical and codec bounds remain within the 64 MiB format cap.

The one setting gates two different conservative models. Before keyed replay, `memoryBudgetBytes`
acts as a planner safety cap on an estimate based on candidate rows, table width, and encoded key
bytes. Physical execution separately treats it as an executor-buffer budget for the largest
source/decompression, constructed-output, compression, envelope, and reconciliation buffer bound,
and checkpoints the high-water for completed outputs. The planner estimate is not executor
high-water: merge planning currently neither spills its key map/source references nor checkpoints a
replay cursor, so an interruption before the immutable plan is written restarts planning. Native
codec and IndexedDB allocations, persisted metadata, and other browser heap remain outside both
figures.

If execution stops after an immutable block or output segment is stored but before its journal or
cursor update, resume validates and reattaches the existing object. Existing output is decompressed
and compared by validated physical payload, type, compression, and row count. This semantic check
does not require equivalent gzip streams to be byte-identical. A replacement transaction preserves
the completed-output cursor, and a transaction committed before the job reached `published` is
reconciled instead of published twice. Publication can rebase across later append or mutation
segments while every planned source remains visible and unchanged; the frozen plan does not absorb
them. Otherwise the job aborts. Manifest replacement affects only the new version, so historical
snapshots continue reading their original segments and blocks.

L2 rebase adds an exact order proof. The active job pins its `sourceManifestVersion`; publication
reconstructs that manifest's optional L1 plus ordinal L2 retained prefix, verifies the selected IDs
are exactly its oldest L0 prefix, and compares every retained and selected segment descriptor with
the current visible prefix. Only ordinary-insert L0 segments ordered strictly after the selected
sources may appear as a concurrent tail. Publication supersedes exactly the selected L0 block IDs,
places the new partition after the retained prefix with the earliest selected `logicalOrder`, and
leaves the tail after it. Any changed prefix, ordinal gap, interleaving, or unsupported concurrent
segment aborts instead of risking row reordering. The pinned source manifest and active job artifacts
remain garbage-collection roots until the job settles.

A successful non-empty merge publishes one full-row `base` segment and copies the plan's canonical
`rowIdSpans` into its descriptor. When replay deletes every row, the job still commits a new manifest
that supersedes all source blocks, but its output segment ID is null and it publishes no output
blocks or globally visible empty segment. Both forms remain restart-safe after the immutable plan
exists.

A successful L2 job instead publishes one non-empty ordinary insert segment with a contiguous hidden
row-ID envelope and its planned partition ordinal. The ordinal records append partition order; it
does not claim that separately reserved row-ID ranges are globally gap-free or that numeric row-ID
order replaces logical/commit order.

Cancellation is cooperative rather than preemptive. Physical transforms and an active native codec
operation run to their next durable boundary; an in-flight step that then observes cancellation, or
any attempt to resume the terminal job, throws `CompactionJobCancelledError`. The atomic
cancellation operation also aborts the linked transaction, so a cancelled job cannot publish. If
the commit transaction wins the race first, the job is reconciled as `published` instead. The
cancelled record preserves its immutable plan, cursor, completed-output IDs, byte counts, and
high-water metrics for diagnosis and recovery accounting. Already-written immutable blocks and
segment artifacts are left unreachable by cancellation itself. A later garbage-collection pass may
delete them only after atomic reachability revalidation.

### Lease-aware physical garbage collection

The public collection surface has four layers. `collectGarbage()` drives a pass to completion;
`collectGarbageStep()` plans or advances one bounded durable step;
`resumeGarbageCollectionJob(jobId)` continues a known pass after a yield or reopen; and
`listGarbageCollectionJobs()` exposes persisted records. A revisioned job stores immutable manifest,
segment, and block candidate lists, one fixed lease cutoff, manifest/segment/block cursors,
cumulative outcome counters, and `planned`, `running`, or `completed` state. Step progress exposes
cumulative examined counts. A completed result reports pruned, already-pruned, retained, and missing
manifest counts; reclaimed, retained, and missing segment/block counts; and
`physicallyReclaimedBytes`.

Planning admits historical manifests, pending artifacts from aborted transaction journals, and
source/output artifacts recorded by terminal published, cancelled, or aborted compaction jobs.
Storage rejects candidate block or segment IDs without one of those persisted provenance paths.
Each step then recomputes live roots rather than trusting the plan:

- the current manifest;
- reader and backup leases whose expiry is after the job's fixed cutoff;
- active transaction snapshot manifests plus pending blocks and segments;
- active (`planned`, `running`, or `ready`) compaction source manifests, source blocks/segments, and
  output blocks/segments; and
- every block referenced by a remaining unpruned manifest, with reachable segment descriptors
  closed over those block and transaction roots.

Terminal compaction records are candidate provenance rather than roots, so cancelled and aborted
outputs and superseded published inputs become collectible when nothing live reaches them. In the
Memory store, revision comparison, root revalidation, deletion, and cursor/counter checkpoint share
the same serialized atomic operation as other metadata changes. IndexedDB performs the equivalent
work in one read-write transaction spanning the GC, block, segment, catalog, manifest, transaction,
and lease stores. A racing operation therefore either installs a root first and collection retains
the artifact, or collection wins and the stale operation fails instead of reviving deleted data.

Collection marks a historical manifest descriptor with `prunedAt` instead of deleting it. The
descriptor remains available to reconcile a commit whose response was lost, but the version cannot
be opened as a new snapshot, used to begin/rebase a transaction, or created/renewed as a lease; a
tombstone no longer roots its former blocks. Transaction begin and rebase retry when the latest
manifest loses this race. Lease creation and renewal validate availability atomically, and expired
lease removal uses its own revision comparison so it cannot erase a concurrent renewal.
`BrowserDatabase` holds transient internal reader leases across table and query materialization,
renewing long operations and releasing after the required data is materialized. Recovery marks stale
transactions aborted, then routes any requested physical deletion through a durable collection job
instead of directly removing their pending objects.

`maxItems`/`maxItemsPerStep` bound how many candidate manifests, segments, and blocks one durable
step examines and thus how many candidate mutations it can apply. They do not bound the initial
candidate plan or the complete metadata/root scans currently needed for atomic revalidation. The
reported `physicallyReclaimedBytes` is the sum of byte lengths for immutable block values actually
deleted; it excludes descriptor metadata and is not a measurement of browser quota recovery. A
block written by `addBlock()` before a crash but never attached to a journal or another provenance
record is not yet enumerated or admitted. Collecting those unknown orphans requires durable
provenance or a conservative age policy.

An unleased `TransactionManager.openSnapshot()` is only an in-process view of one descriptor; it is
not a persistent GC root. Long-lived callers must use `openLeasedSnapshot()`, while
`BrowserDatabase` creates and renews its own short-lived leases around materialization.

Phase 6 remains deliberately incomplete. The L1 policy folds an oldest L0 prefix and optional whole
L1 anchor into one L1 segment—a `base` for a keyed mutation history—or no segment when no selected
row survives. Its 16-segment/64-MiB L0 targets and incremental metric do not hard-bound repeated
anchor rewrites. The append-row-range L2 slice avoids that rewrite cycle for ordinary contiguous
inserts on non-keyed tables: each successfully published L2 partition consumes a disjoint L0 source
prefix and no existing L2 partition is rewritten.

Under a common configured cap, the sum of those successfully published L2 full-block bytes is at
most the cap times the sum of their promoted L0 block bytes. This is a hard publication invariant,
not a claim about all physical writes: cancelled or aborted attempts may already have written
unpublished output, and metadata, IndexedDB internals, garbage collection, browser disk traffic, and
quota recovery are outside the accounting boundary. Keyed/clustered multi-range L2, lifetime
accounting for failed attempts, spillable or resumable merge planning, chunked collection
planning/indexed root discovery, and broader orphan, catalog, terminal-job, and metadata cleanup
remain future work. Known unreachable source/output artifacts are physically reclaimable by the
separate collector.

## Multi-tab correctness

IndexedDB is authoritative. The final metadata transaction serializes manifest publication across tabs and workers. Web Locks may later reduce wasted work around the short commit section, but the compare-and-swap in IndexedDB remains the correctness mechanism.

BroadcastChannel may announce that a new version exists. Receivers always reconcile against IndexedDB, so missed or duplicated announcements are harmless. Reader/backup leases are persistent records with expiry; they are not inferred from open channels.

## Worker and ownership model

The public API is asynchronous from its first release. Main-thread modules only validate protocol envelopes, post requests, and expose results. The coordinator owns database operations.

Large buffers cross worker boundaries as transferables. Transfer of ownership is explicit, and senders must treat transferred buffers as detached. Later executor workers use independent bounded caches and operate on assigned immutable segments. Duplicate cached pages are acceptable in exchange for eliminating shared-memory and cross-origin-isolation requirements.

The wire protocol is independently versioned. Requests carry protocol version, request ID, operation, and payload. Responses are discriminated success, failure, progress, or result messages. Unknown versions and operations fail explicitly.

## Vectorized execution and the bounded-memory target

Phase 7A introduces typed boolean, number, datetime, and string vectors. Every vector carries a packed
validity bitmap; strings are dictionary-coded, numbers and datetimes use `Float64Array`, and booleans
use byte values. `query()` and `prepareQuery()` materialize the referenced columns at one leased
snapshot, replay visible insert, upsert, update, and delete segments into column values, and bind the
public SQL plan to those vectors. The executor scans 2,048 source rows per batch and passes
duplicate-match join fan-out through downstream operators in chunks. Returned `QueryResult` values
still materialize row objects at the public API boundary.

The target operator shape remains:

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

Phase 7B-A adds one shared modeled memory context to each prepared vector query. Retained vector
payloads reserve their validity/value/code arrays and UTF-8 dictionary bytes. Join lookup row indexes,
2,048-row scan indexes, unique-join selection/build arrays, duplicate fan-out workspaces, and joined
row-index batches reserve before their modeled executor buffers are allocated. Child contexts release
temporary reservations after every execution path; the prepared query retains its vectors and lookup
indexes until `close()`. Public options set `executionMemoryBudgetBytes`, `memoryUsage` exposes the
current/peak model, and failed reservations throw `QueryMemoryBudgetError` without changing usage.

Phase 7B-B uses the same execution child for cardinality-growing state. A group entry models one
row-reference slot, tagged logical key payload, and three fixed eight-byte slots per accumulator;
retained `MIN`/`MAX` payloads are separately replaceable. Accumulated output models one row-reference
slot plus tagged logical scalar payload. In-place ordering reserves a modeled row-reference scratch
set, and `LIMIT` reserves the returned reference slice. Those reservations participate in the peak
while execution owns them and are released when the result transfers to the caller.

Phase 7C-A makes grouping-key lookup physically accountable. A canonical encoder writes null,
boolean, finite-number, and length-delimited UTF-8 string keys into a byte arena; compound keys are
unambiguous and `-0` follows JavaScript/SQL equality with `0`. FNV hashes choose typed buckets, while
stored hashes and exact encoded-byte comparisons resolve collisions. Insertion-ordered group values
are addressed by typed hash, offset, length, and chain arrays. Arena, entry, and bucket growth reserves
the full replacement buffer before allocation, copies under the combined peak, then releases the old
reservation. This removes the unmodeled nested `Map` tree without changing grouped result order.

Phase 7C-B makes the general hash-join lookup physically accountable. Canonical scalar key bytes are
stored in a collision-checked arena addressed by typed bucket, hash, offset, length, and chain arrays.
Each build row owns one typed next-row slot, so duplicate keys form insertion-ordered `Int32Array`
chains without boxed arrays. Null and `NaN` keys are not indexed, `-0` shares the zero key, and scalar
type tags prevent numeric/string aliasing. Dense declared-unique integer keys continue to use a direct
typed lookup. Growth follows the same reserve-new, copy, release-old high-water contract as grouping.

This slice still does not satisfy the bounded-memory target. Whole projected columns and their boxed
preparation values are materialized before vector accounting begins. The model excludes boxed
aggregate/result objects, property and JavaScript array-capacity overhead,
sort-implementation scratch, encoding/accounting temporaries, caller-owned result lifetime, and
browser allocator overhead. Exhaustion throws instead of spilling, and the public result remains
materialized under the current API contract.

The completed design gives every query and physical rewrite job a memory context. Operators reserve
and release bytes, while sort, hash aggregate, hash join, and distinct spill to temporary storage when
reservations fail. Physical compaction already advances by output block under a specialized
conservative executor-buffer model. Mutation merge planning applies a separate preflight safety
estimate to its in-memory key and source-reference state, but does not spill or resume that state. The
general query context now provides the narrower Phase 7B/C-A/C-B model above; byte-addressable result
containers, complete physical accounting, and spilling remain future work. As described above,
query and compaction figures are modeled workflow bounds rather than measurements or hard limits on
all browser heap.

## Automatic data skipping

Phase 7A performs no data skipping: it reads every block needed for each referenced projected column.
There are no user-managed indexes. A later phase will record row count, null count, min/max zone maps,
approximate distinct counts, dictionary membership, and optional Bloom filters per row group. Those
statistics will let scans prune segments and row groups before loading predicate columns, evaluate a
selection vector, and then late-load projected columns.

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

An intentionally limited read-only SQL subset and the Phase 7A columnar executor are implemented ahead
of this architecture. Typed shared logical/physical plans, the schema DSL, ORM, and live queries
remain postponed until their lower gates are satisfied.

## Backup, restore, and garbage collection

A future backup workflow pins immutable manifest N with a `backup` lease and streams its catalog,
manifest, segment descriptors, referenced blocks, and checksums while writers publish newer
versions. Restore will write and verify blocks first and publish the restored manifest last.

The implemented garbage collector treats unexpired backup leases exactly like reader leases and
removes only known-provenance artifacts that remain unreachable after the atomic root check described
above. Recovery uses that collector for stale transaction artifacts. Unknown pre-journal blocks,
temporary-store cleanup, and broader catalog/job lifecycle policies remain future work.

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
  deliberately limited, and its Phase 7A executor does not claim the bounded-memory exit gate.
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
