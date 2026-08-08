# BrowserDatabase

BrowserDatabase is an experimental browser-only relational database built around immutable compressed
columnar blocks and IndexedDB. The current slice includes the block format, atomic storage
publication, persistent writes and snapshots, a bounded read-only SQL API, resumable physical
compaction of append segments, deterministic fault injection, and a browser benchmark laboratory.

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

// Advance a durable append-only physical rewrite by one output block.
let progress = await database.compactTableStep("events", { maxBlocks: 1 });
while (progress.result === null) {
  if (progress.jobId === null) throw new Error("Expected a compaction job");
  progress = await database.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });
}
const jobs = await database.listCompactionJobs("events");
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

`query()` and `prepareQuery()` are the public read-only SQL API. The current intentionally bounded
SQL surface supports `SELECT`, aliases, inner and left equi-joins, `WHERE` comparisons joined by
`AND`, arithmetic, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `ROUND`, `GROUP BY`, multi-column `ORDER BY`,
and `LIMIT`. It rejects multiple statements, comments, writes, unknown or ambiguous columns, and
unsupported functions instead of silently interpreting them. A prepared query materializes only
its referenced columns at one manifest version, remains stable across later commits, and must be
closed when no longer needed. This is a correctness-first SQL subset, not a claim of full SQL-92
coverage.

Compaction is restart-safe and cooperative. A revisioned job in the IndexedDB `gc` store records an
immutable rewrite plan: the source manifest and segments; ordered column/block layout; source row
ranges, byte lengths, and checksums; output row windows; output compression; row-ID bounds; and logical
order. It also checkpoints its output IDs, active transaction, state, and next output cursor.
`compactTableStep()` creates or advances the active table job by at most `maxBlocks` output blocks;
`resumeCompactionJob()` continues a known job after a yield or database reopen; and
`listCompactionJobs()` exposes persisted progress. `compactTable()` is a convenience wrapper that
drives the same checkpointed workflow to completion in steps controlled by `maxBlocksPerStep`.

Execution decodes verified physical column payloads, slices and concatenates row ranges, and
re-encodes output windows without materializing JavaScript row objects. The defaults are gzip, a
2 MiB estimated uncompressed physical target per output column block, and a 32 MiB budget for
JavaScript-owned rewrite buffers. The planner derives a shared row-window size from observed source
block density, then measures and splits windows when skew or a tighter execution budget requires it.
A single row cannot be split, so 2 MiB remains a sizing target rather than a hard output limit; one
large value may exceed it when its physical and codec bounds still fit the block format. The job
records a conservative minimum and a modeled high-water mark. Planner inspection reads one stored block at a
time outside that executor high-water accounting; browser-native codec allocations, IndexedDB
internals, persisted job/transaction metadata, and other browser heap are also excluded, so the
option is not a strict total-process heap limit.

Every completed output block is checkpointed under a deterministic ID. Resume decompresses and
validates an existing output, then compares its physical payload, type, compression, and row count.
This semantic reconciliation matters for gzip because equivalent streams need not be
byte-identical. The block is reattached to a replacement transaction when needed. A prepared output
segment and a commit whose job-state update was lost are reconciled similarly. Normal writes remain
L0 segments. The completed whole-table append rewrite publishes one L1 segment with the earliest source
`logicalOrder`, which keeps a concurrent append after the consolidated rows when publication safely
rebases. A changed or missing planned source aborts the job. Older manifests still reference the
source blocks, so historical snapshots remain valid.

Phase 6 remains open. The implemented policy only rewrites every eligible, contiguous append-only
segment in a table from L0 to one L1 segment. It does not compact upsert/update/delete deltas, choose
source subsets, implement L2 policy, cancel jobs, or reclaim physical blocks. Mutation-bearing and
non-contiguous inputs are skipped explicitly; superseded blocks remain stored and
`physicallyReclaimedBytes` is zero until lease-aware garbage collection exists.

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
engine performance claims. They remain separate from the public row-oriented SQL subset; vector
execution and broader SQL coverage are later roadmap work. The upsert probe is capped at 1,000 saved
keys and is not presented as a large-scale throughput benchmark.

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
