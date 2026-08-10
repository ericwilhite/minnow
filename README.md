# BrowserDatabase

BrowserDatabase is an experimental browser-only relational database built around immutable compressed
columnar blocks and IndexedDB. The current slice includes the block format, atomic storage
publication, persistent writes and snapshots, an intentionally limited read-only SQL API with an
initial columnar executor, resumable and cancellable physical compaction of append and keyed mutation
segments, lease-aware physical garbage collection, deterministic fault injection, and a browser
benchmark laboratory.

The public logical types are intentionally small:

- `boolean`
- `number`
- `string`
- `datetime`

Numeric widths and physical encodings are storage details, not schema choices exposed to callers.

## Library write API

The library can now save real tables and column-based batches. A table uses only the four data types
above. Inserts validate column names, row counts, null values, and value types before writing.

```ts
const database = new BrowserDatabase(store);

await database.createTable({
  name: "people",
  uniqueKey: "name",
  columns: [
    { name: "name", type: "string" },
    { name: "score", type: "number" },
    { name: "joined", type: "datetime", nullable: true },
  ],
});

const result = await database.insertBatch("people", {
  columns: {
    name: ["Ada", "Grace"],
    score: [10, 20],
    joined: [new Date(), null],
  },
});

await database.upsertBatch("people", {
  columns: {
    name: ["Grace", "Katherine"],
    score: [25, 30],
    joined: [new Date(), new Date()],
  },
});

await database.updateBatch("people", {
  keys: ["Grace", "Katherine"],
  changes: { score: [26, 31] },
});

const scores = await database.readTable("people", {
  columns: ["name", "score"],
});

await database.deleteBatch("people", { keys: ["Ada"] });

const resultSet = await database.query(`
  SELECT score, COUNT(*) AS people
  FROM people
  WHERE score >= 20
  GROUP BY score
  ORDER BY score DESC
`);

// Preparation fixes one immutable version and decodes only referenced columns.
const prepared = await database.prepareQuery("SELECT COUNT(*) AS people FROM people");
const first = prepared.execute();
const sameSnapshot = prepared.execute();
prepared.close();

const writer = database.bufferedWriter("people", {
  mode: "upsert",
  maxRows: 1_000,
  maxBytes: 1024 * 1024,
  maxAgeMs: 1_000,
});
await writer.add({ name: "Linus", score: 30, joined: new Date() });

// Called by a worker-side handler when a main-thread lifecycle proxy requests a flush.
writer.requestFlush();
await writer.close();

await database.createTable({
  name: "events",
  columns: [{ name: "value", type: "number" }],
});
await database.insert("events", { value: 1 });
await database.insert("events", { value: 2 });

// Advance a durable physical rewrite by one output block.
let progress = await database.compactTableStep("events", { maxBlocks: 1 });
while (progress.result === null) {
  if (progress.jobId === null) throw new Error("Expected a compaction job");
  // To stop instead, cancellation is durable and idempotent:
  // await database.cancelCompactionJob(progress.jobId);
  // break;
  progress = await database.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });
}
const jobs = await database.listCompactionJobs("events");

// Reclaim unreachable history and terminal-job artifacts to completion.
const collection = await database.collectGarbage({
  maxItemsPerStep: 64,
  maxPlanningItems: 1024,
});

// Or drive the same durable pass explicitly across yields or database reopens.
let gcProgress = await database.collectGarbageStep({ maxItems: 8 });
while (gcProgress.result === null) {
  gcProgress = await database.resumeGarbageCollectionJob(gcProgress.jobId, { maxItems: 8 });
}
const collectionJobs = await database.listGarbageCollectionJobs();

// Reclaim query spill pages abandoned by an abrupt tab loss once their owner leases expire.
const spillCleanup = await database.cleanupQuerySpill({ maxOwners: 64 });
```

`result` reports the row count, block count, saved bytes, database version, encoding/staging/commit
time, retries, rows/s, and write amplification. The library assigns row IDs internally; callers do
not choose an integer size. Tables may name one non-null column as a unique key. `upsertBatch`
updates rows matching that key and inserts new keys. `updateBatch` writes immutable patches that
contain only the key and changed columns; it rejects missing keys and rechecks them after conflicts.
Unique-key checks use atomic, versioned persistent key chunks, so they avoid both table-block scans
and one-IndexedDB-operation-per-key overhead.
Rows can be deleted by unique key. Buffered writers combine individual rows into column batches and
flush at a row, byte, or age limit. An age-triggered flush waits behind an already-running batch and
then drains rows accepted during that batch, so the timer cannot be consumed while data remains
pending. `readTable` accepts a column projection and fetches the required
blocks for each visible segment in bulk windows of up to 16 block IDs; replay may also load a unique
key column needed to apply mutations. `requestFlush()` is non-blocking and reports failures through
the writer's `onError`; a main-thread worker proxy can use `attachLifecycleFlush()` to send that
request when the document becomes hidden or the page fires `pagehide`. This is an optimization, not
an unload-time durability guarantee.

`query()` and `prepareQuery()` are the public read-only SQL API, and `execute()` additionally
routes SQL mutations. The SQL surface supports `SELECT` with `DISTINCT`, aliases, inner and left
equi-joins, `WHERE` comparisons joined by `AND` with `IN`/`NOT IN` lists and uncorrelated scalar
and membership subqueries, arithmetic, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `ROUND`, `GROUP BY`,
`HAVING` conditions over aggregates, literals, and group keys, multi-column `ORDER BY`, `LIMIT`,
non-recursive `WITH` CTEs, derived tables, top-level `UNION`/`UNION ALL`, and
`ROW_NUMBER`/`RANK`/`DENSE_RANK` window functions over `PARTITION BY`/`ORDER BY`.
`SELECT DISTINCT` compiles into grouping by every selected expression, so it reuses
the grouped executor, its partitioned spill, and streamed scan inputs; CTEs, derived tables, set
operations, and window inputs execute at the same leased snapshot and materialize as typed
in-memory tables. `INSERT ... VALUES` maps onto one column batch insert, and `UPDATE`/`DELETE` on
unique-key tables read matching keys at one snapshot and apply the keyed batch mutation — two
steps, not one serializable transaction, so a competing key change fails the statement explicitly.
Every compiled statement passes through a deterministic optimizer:
constant folding, predicate pushdown into derived and CTE sources (base and inner-join sides
only; group keys only for grouped inners), projection pruning of plain derived blocks, and LIMIT
combining. Preparation then applies exact-row-count decisions: a single inner equi-join builds
its index over the smaller input, and string equality predicates compare dictionary codes per
row. `explain()` renders the optimized plan with physical strategy notes, and the
differential fuzzer executes optimized plans against the raw-plan row reference. The checked-in
machine-readable feature matrix
(`packages/engine/sql-feature-matrix.json`) records every supported and rejected form with an
executable example, and a conformance test holds it honest. Unsupported syntax — `OR`, `LIKE`,
`BETWEEN`, `IS NULL`, `CASE`, `EXISTS`, correlated subqueries, recursive CTEs,
`INTERSECT`/`EXCEPT`, aggregate windows, non-equi joins, DDL, and more — fails explicitly instead
of being silently interpreted. A prepared query materializes only
its referenced columns at one manifest version, remains stable across later commits, and must be
closed when no longer needed. This is a correctness-first SQL subset, not a claim of full SQL-92
coverage.

Phase 7A backs that public SQL surface with typed column vectors. Booleans use byte values, numbers
and datetimes use `Float64Array`, strings use dictionary-coded `Uint32Array` values, and every vector
has a packed validity bitmap. Append/base snapshots decode validated physical blocks directly into
preallocated vectors without a full boxed-value copy. Keyed mutation snapshots replay inserts,
upserts, updates, and deletes through typed mutable vectors and compact live slots into the retained
vectors, likewise avoiding full-table boxed column arrays. Execution scans 2,048
source rows at a time and feeds duplicate-match join fan-out onward in reserved `Int32Array` chunks.
Multi-table preparation loads one shared segment visibility catalog instead of rescanning it per
table, then batch-fetches only the transaction owners referenced by those segments in 64-ID windows.
It does not materialize the database-wide transaction history. Column-free append/base queries such
as `COUNT(*)` derive their scan cardinality from
visible segment metadata without loading a data block; keyed mutation replay loads the key when it is
needed to recover the live-row count. The executor still materializes result row objects at the
`QueryResult` API boundary.

Phase 7B-A adds a deliberately scoped query-memory model. `query()` and `prepareQuery()` accept
`executionMemoryBudgetBytes`; prepared queries expose `{ budgetBytes, usedBytes, peakBytes }` through
`memoryUsage`. The model reserves retained typed-vector payloads (including validity, codes, and UTF-8
dictionary bytes), join row indexes, scan row-index batches, selection/build arrays, and chunked join
fan-out buffers. A reservation that would exceed the configured budget throws
`QueryMemoryBudgetError`; typed executor buffers reserve before allocation, and logical group/result
state reserves before it is retained by the operator. Temporary reservations are released after each
execution, including failures, and `close()` releases retained reservations.

Phase 7B-B extends that model to cardinality-growing operators. Aggregate counts and sums use typed
numeric arrays instead of per-aggregate objects; each group reserves its logical key payload and
fixed aggregate slots, while retained `MIN`/`MAX` values replace their own reservations atomically.
Accumulated output reserves a row-reference slot plus tagged logical scalar payload. `ORDER BY` uses
an explicit stable merge/cycle sort whose two `Uint32Array` indexes and visited bitmap are reserved
before allocation, and `LIMIT` truncates the owned result array in place. These bytes
contribute to `peakBytes` and are released when execution returns ownership of the `QueryResult`.

Phase 7C-A replaces the grouped executor's nested JavaScript `Map` tree with a byte-addressable key
index. Typed scalar and compound keys are encoded into a retained byte arena; canonical numeric keys,
type tags, and length-delimited strings preserve SQL grouping distinctions. Collision-checked hashes
address typed bucket, chain, offset, length, and hash arrays. Every arena or index growth reserves the
new typed capacity before allocation and releases the superseded reservation only after copying.
The lookup-or-insert path encodes and hashes each new group key once, and the encoder allocates the
exact contiguous key bytes instead of an intermediate boxed byte array. Insertion order remains
stable for deterministic grouped results.

Phase 7C-B applies the same physical-accounting boundary to hash joins. Non-direct join keys use a
collision-checked scalar byte arena with typed bucket and entry arrays; duplicate build rows are
linked through one reserved `Int32Array` rather than boxed `Map` entries and growable row arrays.
Probe traversal preserves build-row order, canonicalizes `-0`, keeps numeric and string keys
distinct, and follows SQL null and `NaN` equality semantics. Dense unique integer keys retain their
direct typed lookup fast path.

Phase 7D-A adds durable query spill pages in the existing `temp` store. With an explicit execution
budget, `BrowserDatabase.query()` automatically uses asynchronous external merge sort for ungrouped
`ORDER BY` plans, including joined output, and 64-way partitioned hash aggregation for single-table
`GROUP BY ... ORDER BY` plans. Sorted pages merge pairwise with left-run tie stability; LIMIT applies
only while reading the final run. Temp pages are removed after ordinary success or failure. Each
spill owner also registers a durable lease before its first page write and renews it while pages are
read or written; the default lifetime is one minute and `spillOwnerLeaseMs` tunes it.
`cleanupQuerySpill()` reclaims owner pages abandoned by an abrupt process/tab loss once their lease
is expired or missing at a cutoff fixed when the pass starts, retains every unexpired owner, pages
through owner IDs in bounded windows, accepts `maxOwners`, and reports examined, reclaimed, and
retained owner counts. Reclamation races a live renewal atomically, so a concurrently renewed owner
is retained rather than torn down mid-query.
`PreparedQuery.execute()` remains the synchronous no-I/O path; `executeAsync()` accepts a spill store
for callers that want the operator path directly; leases protect only spill stores created by
`BrowserDatabase.query()`, and a caller-supplied spill store manages its own cleanup.

Phase 7E-A streams the scan input for one deliberately narrow plan family. A budgeted
`BrowserDatabase.query()` over a single append/base table with no join executes against a sliding
block-aligned window instead of fully materialized projected vectors: each scan batch first makes
its rows resident, forward-only, decoding whole physical blocks, reserving the replacement window's
typed bytes before allocation and its measured per-window string dictionary bytes before
installation, and releasing the superseded window afterward. Global aggregates, unordered and
order-spilled scans, and grouped-unordered plans over tables far larger than the configured budget
now complete instead of failing at preparation. Keyed mutation snapshots of the scanned base table,
prepared queries, and unbudgeted or spill-disabled queries
keep the materialized path. Under an explicit budget the streaming path takes precedence over Phase
8A zone-map pruning, so a pruned-eligible predicate reads more blocks than the pruned path would in
exchange for the bounded working set.

Phase 7E-C extends the same scan window to the probe side of joined plans. The base table streams
while every join build side is materialized in full at the same leased snapshot, including keyed
mutation replay on a build table. A self-join keeps the materialized path because the base table
would also be probed as a build side at arbitrary row positions. Build-side bytes are still
reserved only when the bound plan installs its table payloads, so a large build side fails the
budget exactly as the materialized path does.

Phase 7E-B rewrites the partitioned hash-aggregate spill to carry values instead of scan-row
indexes. Each partition page stores the evaluated group keys and aggregate arguments for its
surviving rows, so the partition phase accumulates groups without re-reading source vectors. That
makes grouped-and-ordered single-table plans streamable and extends the same spill to grouped
ordered equi-joins, whose build sides remain fully materialized. Buffered evaluated values are
reserved per row and flushed in fixed 512-row scan chunks. Budgeted grouped plans without
`ORDER BY` take the same partitioned path—the empty ordering reduces the run merge to a stable
concatenation—so peak accounted group state is bounded by one partition rather than the whole
key space; their group order follows partition processing rather than first-appearance order,
which SQL leaves unspecified without `ORDER BY`. Global aggregates keep bounded in-memory
accumulators, `DISTINCT` and `HAVING` ride the same grouped machinery, and hash-join build sides
remain the one operator input with no spill path.

This is not yet the Phase 7 bounded-memory exit or a hard browser-heap limit. Prepared queries and
every non-streamed shape still materialize each projected typed input in full before the vector
reservation is installed;
mutation replay can temporarily retain both its typed slot workspace and compacted output. Mutation
merge planning now updates retained source slots in place and emits row-ID spans plus all output
ranges in one pass, avoiding a second live-row array and per-column source-array copies, but its key
map and source slots remain whole-plan state. The model
does not include returned result objects, properties, group-key and retained `MIN`/`MAX` reference
containers, JavaScript array-capacity overhead, spill serialization/native IndexedDB work, returned
row lifetime, or engine/browser allocator overhead. Hash-join build sides are the remaining
unspillable operator input and still fail on budget exhaustion. Phase 7 remains open
for streamed mutation scan inputs, byte-addressable aggregate/result containers, complete
physical accounting, and that remaining spill path.

Phase 8A adds conservative row-group skipping for a single append/base table with `AND`-combined
number or datetime column-to-literal comparisons. Preparation checksum-validates and physically
decodes predicate blocks before trusting their derived min/max and null-count metadata, rejects
impossible groups before logical vector materialization, evaluates surviving predicate vectors into
a typed selection, and only then loads projected
blocks for candidate groups and compacts exact matches. Joins, keyed mutation snapshots, strings,
computed predicates, and unsupported layouts retain the full-scan path. IndexedDB currently returns
the complete predicate block value rather than trusted header/index metadata, so this slice primarily
saves non-predicate block I/O and vector work; richer statistics, authenticated metadata access, and
selective benchmarks remain.

The exported row-array helper retains a compatibility oracle for schema-less empty inputs. Because it
cannot construct typed empty vectors without column types, that fallback rejects an explicit memory
budget instead of silently bypassing it. `BrowserDatabase` knows catalog types and supports budgeted
queries over empty tables through the columnar path.

Compaction is restart-safe and cooperative. A revisioned job in the IndexedDB `gc` store records an
immutable physical rewrite plan. Append-only inputs use `rechunk-v1`, which fixes the ordered
column/block layout, source row ranges, byte lengths and checksums, output windows, row-ID bounds,
compression, and logical order. Keyed inputs containing upserts, partial updates, deletes, or an
earlier merged base use `merge-v1`. That plan fingerprints the source segments in logical/commit
order—including their kind, key, level, hidden row-ID metadata, and unique block fingerprints—and
freezes logical key replay as ordered per-column source ranges. Execution therefore never has to
repeat replay or trust mutable segment metadata after planning. The job checkpoints its output IDs,
active transaction, state, and next output-window/column cursor.
`compactTableStep()` creates or advances the active table job by at most `maxBlocks` output blocks;
`resumeCompactionJob()` continues a known job after a yield or database reopen; and
`listCompactionJobs()` exposes persisted progress. `compactTable()` is a convenience wrapper that
drives the same checkpointed workflow to completion in steps controlled by `maxBlocksPerStep`.
`cancelCompactionJob(jobId)` durably settles an unpublished job as `cancelled` and atomically aborts
its linked transaction. It is idempotent and returns the job's terminal state plus any published
version, so a caller also sees when publication or an earlier abort had already won.

Planning supports two deliberately different targets. The L1 policy selects one order-safe prefix:
an existing L1 anchor, when present, plus the oldest L0 segments. It requires two L0 segments by
default and targets at most 16 L0 segments or 64 MiB of newly promoted stored data.
`minimumLevel0Segments`, `maxLevel0Segments`, and `maxLevel0StoredBytes` tune those targets; the old
`minimumSegments` name remains a deprecated alias for the L0 minimum. For L1, setting the minimum to
one drains a one-segment tail only when an anchor already exists. The minimum and a complete
equal-`logicalOrder` group take precedence over either maximum so the planner cannot split an
ordering unit or starve an oversized oldest segment.

`targetLevel: 2` opts a non-keyed table into the append-row-range L2 policy. Its only accepted visible
layout is an optional retained L1 insert, an immutable L2 insert prefix whose `partitionOrdinal`
values are exactly `0..N-1`, and an L0 insert suffix. A job promotes only the oldest complete L0
prefix into one new L2 partition at ordinal `N`; it never rewrites the retained L1 or an existing L2
partition. The first L2 job must be requested explicitly. Once a valid L2 prefix exists, omitting
`targetLevel` continues that policy automatically. L2 permits `minimumLevel0Segments: 1`, including
a direct one-segment L0-to-L2 promotion. Tables with a unique key, mutation/base segments,
noncontiguous source row IDs, or any other level layout are skipped rather than treated as this
append-only policy.

For durable upgrade compatibility, an ordinal-less `targetLevel: 2` job created by the older generic
compactor can still resume. It is not a Phase 6E-A policy-tagged job, carries no hard amplification
contract, and its legacy output is not inferred as an append-row-range partition by later planning.

Execution decodes verified physical column payloads, slices and concatenates the frozen source
ranges, and re-encodes output windows without materializing column values as JavaScript row objects.
The defaults are gzip, a 2 MiB estimated uncompressed physical target per output column block, and a
32 MiB `memoryBudgetBytes` setting. Append planning reads one stored block at a time. Merge planning
must also hold keyed replay metadata, so it first checks a separate conservative safety estimate
derived from candidate rows, table width, and encoded key bytes. The same configured budget gates
that planner estimate and the independently modeled executor-buffer minimum; only executor output
work records the persisted high-water mark. The merge planner currently has neither spill nor a
durable replay cursor, so planning itself is not restartable even though the resulting job is.

The planner derives shared output windows from observed source-block density, then measures and
splits them when skew or a tighter executor budget requires it. A single row cannot be split, so 2
MiB remains a sizing target rather than a hard output limit; one large value may exceed it when its
physical and codec bounds still fit the block format. Browser-native codec allocations, IndexedDB
internals, persisted job metadata, and other browser heap are excluded from both modeled figures, so
`memoryBudgetBytes` is a conservative workflow guard rather than a strict total-process heap limit.

An L2 job also fixes `maximumOutputStoredBytes` to
`floor(level0SourceStoredBytes * maxWriteAmplification)`, using a default
`maxWriteAmplification` of 16. Before a job or output artifact is persisted, planning measures the
encoded payload and metadata for every planned block and sums a conservative complete-block bound,
including the envelope and the selected codec's maximum output. If this
`plannedOutputStoredBytesUpperBound` exceeds the exact ceiling, compaction returns
`skipReason: "write-amplification-budget"`. The ordinal, ratio, ceiling, and planned bound are
immutable job fields. During execution, cumulative actual stored full-block bytes are checked against
both persisted bounds before each new or resumed block is staged.

Every completed output block is checkpointed under a deterministic ID. Resume decompresses and
validates an existing output, then compares its physical payload, type, compression, and row count.
This semantic reconciliation matters for gzip because equivalent streams need not be
byte-identical. The block is reattached to a replacement transaction when needed. A prepared output
segment and a commit whose job-state update was lost are reconciled similarly. Normal writes remain
L0 segments. A non-empty L1 prefix rewrite publishes one L1 segment with the earliest source
`logicalOrder`; a later L0 suffix remains visible and replays afterward. An L2 rewrite publishes one
ordinary insert segment at its planned ordinal, between the retained L1/L2 prefix and the unselected
L0 suffix. Mutation merges use the explicit full-row `base` kind and ordered `rowIdSpans` so
updates and matching upserts preserve identity, deleted identities disappear, new keys retain their
reserved IDs, and numerically out-of-order reservations do not change logical row order. If every row
was deleted, publication supersedes the sources with no output block or globally visible empty
segment.

The frozen plan represents one leased source snapshot. Publication may rebase across later appends
or mutation deltas while every planned source remains visible and unchanged; those later segments
stay after the consolidated output and are not absorbed into it. For L2, publication reconstructs
the exact retained prefix from the pinned source manifest and fingerprints it against the current
snapshot, verifies that the selected sources are still the oldest L0 prefix, and admits only a later
ordinary-insert L0 tail. A changed, missing, or reordered source or retained partition aborts the
job. Older manifests still reference their original source blocks, so reads at historical versions
remain valid before, during, and after publication.

Cancellation is cooperative: it does not preempt synchronous physical transforms or browser-native
codec work already in progress. An in-flight step observes cancellation at its next durable boundary
and throws `CompactionJobCancelledError`; resuming a cancelled job throws the same typed error. The
cancellation record update and transaction abort are one atomic storage operation. If cancellation
wins, publication cannot commit; if commit wins, cancellation reports `published` and its manifest
version. The terminal job retains its plan, cursor, metrics, and output IDs for inspection. Any
immutable output blocks or segment artifacts already written remain unreachable rather than being
deleted by cancellation itself; a later garbage-collection pass can reclaim them after revalidating
that they are still unreachable.

Garbage collection is restart-safe and lease-aware. `collectGarbage()` drives one bounded persisted
candidate pass to completion, using `maxItemsPerStep` to control each checkpoint and
`maxPlanningItems` to cap block/segment IDs copied into a newly persisted job. Repeated calls discover
later candidate chunks. `collectGarbageStep()` plans or
advances one pass, `resumeGarbageCollectionJob()` continues its durable cursor by ID, and
`listGarbageCollectionJobs()` exposes the revisioned `planned`, `running`, and `completed` records
stored in `gc`. Progress reports cumulative examined manifest, segment, and block counts. The
completed result reports the job ID; pruned, already-pruned, retained, and missing manifest counts;
reclaimed, retained, and missing segment counts; reclaimed, retained, and missing block counts; and
`physicallyReclaimedBytes`. That byte value is the sum of the deleted immutable block byte lengths.
It does not include deleted metadata and does not claim that a browser has returned the same number
of bytes to its storage quota.

Each Memory or IndexedDB step atomically checks the expected job revision, re-reads reachability,
applies any manifest tombstones or physical deletions, and checkpoints the new cursor and cumulative
counters. Roots include the current manifest; reader and backup leases unexpired at the pass's fixed
cutoff; active transaction snapshots and pending blocks/segments; and the source manifest,
segments, and source/output blocks of active compactions. Every remaining unpruned manifest roots
its blocks. Terminal cancelled and aborted compaction artifacts, superseded inputs from published
compactions, and aborted-transaction artifacts are candidates rather than permanent roots; the
atomic recheck still retains anything another live root reaches.

Historical manifest descriptors are tombstoned with `prunedAt`, not deleted, so a transaction can
reconcile a commit whose response was lost. A tombstoned version can no longer be opened for a data
read, newly pinned, or used to root its former blocks. `BrowserDatabase` creates transient internal
reader leases while materializing table reads and queries, then releases them after materialization.
Transaction begin/rebase, lease creation/renewal, and compare-and-swap lease expiry are serialized
with collection, so a race either establishes a valid root or receives
`SnapshotManifestMissingError`; it cannot silently pin already-pruned data. Stale-transaction
recovery also routes deletion through the durable collector instead of deleting pending artifacts
directly.

Candidate discovery walks manifests, transactions, and compaction jobs through stable storage cursor
pages of at most 64 records. It checks block existence in 64-ID windows and defaults each new durable
job to at most 1,024 block/segment candidates; at most 64 manifest provenance records accompany a
job. `maxItems` separately bounds the candidates examined and possible mutations in one durable
step. Provenance and atomic root revalidation also stream storage cursors instead of materializing
database-wide metadata arrays. They retain only the current candidate slice and at most 4,096
related segment/block dependencies; dependency overflow safely retains that slice. Scan work still
scales with database history, and a single large source record is still cloned by the underlying
store. Collection intentionally
accepts only candidates with persisted provenance in historical manifests, aborted transaction
journals, or terminal compaction jobs. An otherwise unknown immutable block left by a crash after
`addBlock()` but before journal attachment is omitted until provenance or conservative age tracking
exists.

Phase 6 remains open. The L1 policy folds an order-safe oldest L0 prefix and optional leading L1
anchor into one L1 segment. Contiguous append-only inputs use `rechunk-v1`, while keyed mutation
histories use `merge-v1` and publish a row-ID-preserving `base`. Its reported incremental rewrite
ratio is not a hard total bound because the mandatory anchor can grow. The append-row-range L2 slice
is implemented for ordinary contiguous inserts on non-keyed tables: disjoint oldest L0 prefixes
become immutable ordinal L2 partitions, so prior L2 bytes are never rewritten.

For a common configured cap, summing successfully published L2 output block bytes across those
disjoint sources stays at or below that cap times the corresponding promoted L0 block bytes. This
guarantee is intentionally narrower than total browser write amplification. It excludes output
written by cancelled or aborted attempts, IndexedDB and manifest metadata, browser storage-engine
overhead, garbage collection, and any claim about bytes ultimately returned to quota. Phase 6 still
needs keyed/clustered multi-range L2, lifetime accounting for failed attempts, and spillable or
resumable merge planning. Garbage collection now reclaims known unreachable physical artifacts with
paged/capped candidate and root envelopes, but scan work still scales with history and a single large
metadata record remains unbounded. Unknown pre-journal orphans plus broader catalog, terminal-job,
and metadata cleanup remain future work. A compaction result's
`physicallyReclaimedBytes` remains zero because compaction never deletes in the publication path;
the separate garbage-collection result reports bytes deleted later.

An unleased `TransactionManager.openSnapshot()` handle is not a durable collection root. Code that
keeps a snapshot across asynchronous work or an explicit collection pass must use
`openLeasedSnapshot()` and renew or release that lease explicitly.

## Typed schema and migrations

The schema DSL defines tables with compile-time row types and migrates the catalog through
metadata-only steps:

```ts
import { BrowserDatabase, column, schema, table, typedTable } from "@browserdatabase/engine";

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});

await database.migrate(schema([people]));
const handle = typedTable(database, people);
await handle.insert([{ name: "Ada", score: 10 }]); // joined may be omitted; it reads as null
const rows = await handle.rows(); // Array<{ name: string; score: number; joined: Date | null }>
```

`InferRow`, `InferInsertRow`, and `InferUpdateChanges` expose the select/insert/update types, and
each table definition carries a Standard Schema-compatible `~standard` validator. `migrate()`
diffs the live catalog into deterministic metadata-only steps — create table, add nullable column,
rename a column through its stable ID with `.renamedFrom()`, widen nullability — executed
idempotently with one atomic compare-and-swap per catalog change, so an interrupted migration
completes by re-running and a concurrent migrator fails with a typed conflict. Type changes,
drops, unique-key changes, non-null tightening, and non-nullable additions are rejected
explicitly. Columns added after a segment was written read as NULL everywhere without rewriting
stored data; declared `.references()` relations validate at definition time and live in catalog
metadata rather than being enforced per write.

The ORM query builder constructs the same compiled plans as SQL, with typed results:

```ts
import { count, eq, from, refs, sum } from "@browserdatabase/engine";

const p = refs(people, "p");
const o = refs(orders, "o");
const rows = await database.run(
  from(people, "p")
    .innerJoin(orders, "o", eq(o.person, p.name))
    .groupBy(p.name)
    .select({ name: p.name, orders: count(), revenue: sum(o.total) })
    .orderBy("revenue", "desc")
    .build(),
); // Array<{ name: string; orders: number; revenue: number | null }>
```

Builder expressions assemble plan nodes directly — never SQL strings — and `build()` runs the
same deterministic optimizer, so an ORM query and its equivalent SQL bind to identical plans;
structural plan-equality tests enforce that. `nullableRefs()` types left-joined columns with
`| null`, `typedTable` covers CRUD and batch writes, and `query()`/`execute()` remain the raw SQL
escape hatch.

## Storage laboratory

The browser dashboard currently supports:

- one deterministic 50-table commerce graph spanning geography, currencies, channels, stores,
  staff, tax, catalog, procurement, customers, loyalty, carts, orders, payments, fulfillment,
  returns, support, audit, and inventory ledgers;
- a single scale multiplier that grows every dimension, bridge, transaction, and supporting table;
- data groups with numbers, text, dates, and yes/no values;
- block sizes from 256 KB to 4 MB;
- no compression, simple repeat compression, and gzip;
- faster or safer browser saving;
- repeated tests that highlight the whole run, saving, or loading;
- progress, cancellation, timing charts, storage estimates, past results, and downloads;
- one-click comparison of all three compression choices and all five block sizes;
- optional Playwright evidence capture with repeated cells, explicit aggregation timing, and
  verified raw JSON downloads; the current cross-browser result provisionally selects gzip with a
  2 MiB target for storage-oriented physical rewrites;
- a lazy-loaded public SQL comparison against SQLite Wasm, DuckDB-Wasm, and PGlite using the same
  50-table batches, explicit persistence/configuration disclosures, portable checksum-verified SQL,
  per-query median/p95 timing, engine-reported database size, loaded asset bytes, and available heap
  telemetry; all four engines must return matching checksums, and BrowserDatabase reports snapshot
  preparation separately from repeated `PreparedQuery.execute()` timing;
- an exact list of every saved block, with every loaded value checked against the original;
- measured journaled commits through the transaction API instead of direct manifest publication;
- fixed correctness probes for stable snapshots, persistent transaction states, competing writers,
  rebase, renewable reader/backup leases, stale segment/block cleanup, and lost commit responses;
- public-API write measurements that create every scenario table and insert bounded column batches;
- concurrent `insertBatch` checks across two IndexedDB connections, including non-overlapping
  internal row-ID ranges;
- a bounded unique-key upsert probe covering mixed inserts/updates, rejected keys, historical
  `readTable` values, immutable upsert segments, and two-tab conflict retries;
- real partial-update, projected-read, and key-delete measurements with immutable-segment checks;
- per-write encoding, staging, commit, retry, throughput, and write-amplification metrics;
- an append-compaction probe that verifies current rows, historical snapshots, manifest visibility,
  and retained source blocks;
- 81 checked foreign-key paths plus primary-key, value-domain, and transaction-ledger integrity
  checks;
- 15 relational reference queries ranging from a point lookup and date filter to joins,
  aggregates, anti-joins, cohort analysis, revenue matrices, tax, fulfillment, payment-funnel,
  supplier-ledger, and adjustment analysis;
- a read-only ad-hoc SQL console over the latest generated snapshot, with hash joins, filters,
  grouping, aggregates, ordering, a bounded preview, and parse/median/p95/row-flow metrics.

Transaction probe time is reported separately and excluded from storage throughput and comparison
rankings. Each probe uses a throwaway IndexedDB database and does not send data over the network.

Each data group is also created as a persistent table for the public-API write workload. The
lower-level storage baseline still decodes blocks directly to verify every value. The 15 catalog
relational cards use a separate reference-workload implementation, so their timings are not public
SQL API timings. `readTable` loads one shared snapshot, reusable JavaScript hash indexes are built
once, and each optimized query reports a seven-sample total plus median and p95 per execution. Fast
queries are batched within each sample to exceed coarse browser timer resolution, and the actual
execution count plus measurement wall time are disclosed. A separate implementation acts as a
result oracle; row counts and normalized checksums must agree. The suite also validates all 50 table
counts, primary-key uniqueness, 81 foreign-key paths, dates, statuses, quantities, discounts,
monetary values, and transaction/ledger coverage. The dashboard reports snapshot loading and index
construction separately and shows the reference SQL for every catalog query.

The ad-hoc console reopens the latest persisted IndexedDB dataset and prepares only columns referenced
by the query. It supports one read-only `SELECT` with equi-joins, `WHERE` predicates joined by `AND`, grouping,
`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, ordering, and a limit. It rejects mutations and unsupported SQL.
Catalog-query and ad-hoc timings are correctness and workload-shape measurements, not native query
engine performance claims. They remain separate from the public vector-backed SQL subset; the full
bounded-memory vector contract, data skipping, and broader SQL coverage remain roadmap work. The
upsert probe is capped at 1,000 saved keys and is not presented as a large-scale throughput benchmark.

The cross-engine comparison keeps those reference timings separate. BrowserDatabase is measured
through `createTable()`, `insertBatch()`, `prepareQuery()`, `query()`, and `listTables()`; each SQL
query is prepared against an immutable snapshot, and repeated execution is timed separately from
snapshot loading and decoding. SQLite uses the OO1 API in the existing benchmark worker with the
primary OPFS VFS or persistent OPFS SAH-pool fallback; DuckDB uses its async worker and Arrow
ingestion; PGlite uses its IndexedDB filesystem with default durable syncing. BrowserDatabase,
SQLite, and PGlite must close, reopen, and verify their data before passing. DuckDB is the only
memory-only adapter: its available OPFS database path was tested, but the installed Wasm build
reopened an empty catalog, so the dashboard refuses to call it persistent. Every report puts all four
engines in one comparison matrix and reports database size in MB. BrowserDatabase includes blocks,
key chunks, manifests, segments, transactions, and catalog records in its complete logical IndexedDB
payload; SQLite and PGlite report full database sizes; DuckDB reports database memory.

## Development

```sh
npm install
npm run check
npx playwright install chromium firefox webkit
npm run check:release
npm run dev --workspace @browserdatabase/bench
```

`npm run check` is the local formatting, lint, type, build, and unit-test gate.
`npm run check:release` adds the real IndexedDB library suite and full browser dashboard suite.

See `ARCHITECTURE.md` and `ROADMAP.md` for the design and milestone gates.
