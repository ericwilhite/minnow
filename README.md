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
const collection = await database.collectGarbage({ maxItemsPerStep: 64 });

// Or drive the same durable pass explicitly across yields or database reopens.
let gcProgress = await database.collectGarbageStep({ maxItems: 8 });
while (gcProgress.result === null) {
  gcProgress = await database.resumeGarbageCollectionJob(gcProgress.jobId, { maxItems: 8 });
}
const collectionJobs = await database.listGarbageCollectionJobs();
```

`result` reports the row count, block count, saved bytes, database version, encoding/staging/commit
time, retries, rows/s, and write amplification. The library assigns row IDs internally; callers do
not choose an integer size. Tables may name one non-null column as a unique key. `upsertBatch`
updates rows matching that key and inserts new keys. `updateBatch` writes immutable patches that
contain only the key and changed columns; it rejects missing keys and rechecks them after conflicts.
Unique-key checks use atomic, versioned persistent key chunks, so they avoid both table-block scans
and one-IndexedDB-operation-per-key overhead.
Rows can be deleted by unique key. Buffered writers combine individual rows into column batches and
flush at a row, byte, or age limit. `readTable` accepts a column projection and fetches the required
blocks for each visible segment in bulk windows of up to 16 block IDs; replay may also load a unique
key column needed to apply mutations. `requestFlush()` is non-blocking and reports failures through
the writer's `onError`; a main-thread worker proxy can use `attachLifecycleFlush()` to send that
request when the document becomes hidden or the page fires `pagehide`. This is an optimization, not
an unload-time durability guarantee.

`query()` and `prepareQuery()` are the public read-only SQL API. The current intentionally limited SQL
surface supports `SELECT`, aliases, inner and left equi-joins, `WHERE` comparisons joined by
`AND`, arithmetic, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `ROUND`, `GROUP BY`, multi-column `ORDER BY`,
and `LIMIT`. It rejects multiple statements, comments, writes, unknown or ambiguous columns, and
unsupported functions instead of silently interpreting them. A prepared query materializes only
its referenced columns at one manifest version, remains stable across later commits, and must be
closed when no longer needed. This is a correctness-first SQL subset, not a claim of full SQL-92
coverage.

Phase 7A backs that public SQL surface with typed column vectors. Booleans use byte values, numbers
and datetimes use `Float64Array`, strings use dictionary-coded `Uint32Array` values, and every vector
has a packed validity bitmap. Snapshot preparation replays visible inserts, upserts, updates, and
deletes directly into projected column values before creating the vectors. Execution scans 2,048
source rows at a time and feeds duplicate-match join fan-out onward in chunks. It still materializes
result row objects at the `QueryResult` API boundary.

Phase 7B-A adds a deliberately scoped query-memory model. `query()` and `prepareQuery()` accept
`executionMemoryBudgetBytes`; prepared queries expose `{ budgetBytes, usedBytes, peakBytes }` through
`memoryUsage`. The model reserves retained typed-vector payloads (including validity, codes, and UTF-8
dictionary bytes), join row indexes, scan row-index batches, selection/build arrays, and chunked join
fan-out buffers. A reservation that would exceed the configured budget throws
`QueryMemoryBudgetError`; typed executor buffers reserve before allocation, and logical group/result
state reserves before it is retained by the operator. Temporary reservations are released after each
execution, including failures, and `close()` releases retained reservations.

Phase 7B-B extends that model to cardinality-growing operators. Each group reserves a modeled entry
containing its logical key payload and fixed aggregate slots; retained `MIN`/`MAX` values replace their
own reservations atomically. Accumulated output reserves a row-reference slot plus tagged logical
scalar payload, and `ORDER BY`/`LIMIT` reserve their modeled row-reference workspaces. These bytes
contribute to `peakBytes` and are released when execution returns ownership of the `QueryResult`.

Phase 7C-A replaces the grouped executor's nested JavaScript `Map` tree with a byte-addressable key
index. Typed scalar and compound keys are encoded into a retained byte arena; canonical numeric keys,
type tags, and length-delimited strings preserve SQL grouping distinctions. Collision-checked hashes
address typed bucket, chain, offset, length, and hash arrays. Every arena or index growth reserves the
new typed capacity before allocation and releases the superseded reservation only after copying.
Insertion order remains stable for deterministic grouped results.

This is not yet the Phase 7 bounded-memory exit or a hard browser-heap limit. Snapshot preparation
still materializes each projected input in full before the vector reservation is installed. The model
does not include boxed preparation values; the remaining join `Map`, aggregate/result objects,
properties, or JavaScript array-capacity overhead; the sort implementation's internal scratch;
encoding/accounting temporaries; the lifetime of returned rows after ownership transfers to the
caller; or engine/browser allocator overhead. Configured exhaustion fails the query instead of
spilling. It also performs no statistics-driven segment or row-group data skipping. Phase 7 remains
open for streaming inputs, byte-addressable join/aggregate/result containers, complete physical
accounting, and spill; Phase 8 remains open for pruning and late materialization.

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

Garbage collection is restart-safe and lease-aware. `collectGarbage()` drives one persisted pass to
completion, using `maxItemsPerStep` to control each checkpoint. `collectGarbageStep()` plans or
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

`maxItems` bounds the number of manifest, segment, and block candidates examined—and therefore the
maximum candidate mutations—within one durable step. It does not bound initial candidate planning
or the full metadata/root scans currently repeated for atomic revalidation. Collection intentionally
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
resumable merge planning. Garbage collection now reclaims known unreachable physical artifacts, but
its planner and root discovery are not yet chunked/indexed, and unknown pre-journal orphans plus
broader catalog, terminal-job, and metadata cleanup remain future work. A compaction result's
`physicallyReclaimedBytes` remains zero because compaction never deletes in the publication path;
the separate garbage-collection result reports bytes deleted later.

An unleased `TransactionManager.openSnapshot()` handle is not a durable collection root. Code that
keeps a snapshot across asynchronous work or an explicit collection pass must use
`openLeasedSnapshot()` and renew or release that lease explicitly.

## Storage laboratory

The browser dashboard currently supports:

- one deterministic 27-table commerce graph spanning geography, currencies, tax, catalog,
  suppliers, customers, addresses, payment methods, promotions, orders, line items, discounts,
  payments, payment transactions, fulfillment, returns, refunds, and inventory ledgers;
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
  27-table batches, explicit persistence/configuration disclosures, portable checksum-verified SQL,
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
- 41 checked foreign-key paths plus primary-key, value-domain, and transaction-ledger integrity
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
result oracle; row counts and normalized checksums must agree. The suite also validates all 27 table
counts, primary-key uniqueness, 41 foreign-key paths, dates, statuses, quantities, discounts,
monetary values, and transaction/ledger coverage. The dashboard reports snapshot loading and index
construction separately and shows the reference SQL for every catalog query.

The ad-hoc console uses a deliberately bounded reference runner over the materialized snapshot. It
supports one read-only `SELECT` with equi-joins, `WHERE` predicates joined by `AND`, grouping,
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
