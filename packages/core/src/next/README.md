# `next/`: the rebuild core (unreleased)

This is the storage core and executor that replace the current engine. The plan is in `PROPOSAL.md`,
sections "Architecture" and "Wide and large analytics".

Nothing here ships yet:

- No package entry point imports it.
- `package.json` `files` excludes `dist/next/**`.
- `tree-shaking.test.ts` and `scripts/package-shape.test.ts` prove both.

Public-contract decisions are in [`DECISIONS.md`](./DECISIONS.md).

## Module map

| Module             | Owner track                      | What it is                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------ | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `types.ts`         | contracts                        | Physical types; the one SQL → physical mapping; declared widths are mandatory; column statistics types shared by format and exec                                                                                                                                                                                                                                                                                       |
| `errors.ts`        | contracts                        | Errors shared across tracks: `OwnerUnresponsiveError`, `RequestExpiredError`, `SessionLostError`                                                                                                                                                                                                                                                                                                                       |
| `temporal.ts`      | contracts                        | DATE/TIMESTAMP on PostgreSQL's 2000 epoch; infinities; ISO text                                                                                                                                                                                                                                                                                                                                                        |
| `hash/`            | contracts                        | xxHash32, the only checksum                                                                                                                                                                                                                                                                                                                                                                                            |
| `vector/`          | contracts → executor             | Batches (byte-bounded), vectors, validity, exact int64, int128 decimals, UTF-8 builder under a grant, locators                                                                                                                                                                                                                                                                                                         |
| `storage/`         | storage                          | Backend contract (lock + async fence open, async mutations), file store, simulated disk with crash model, the storage kit (conformance + durability sweep); the extent space (`extents.ts`: allocation, writer lifecycle, publish, pins and reclaim, the verified page cache) and its implementation (`space.ts`, `page-cache.ts`), the in-memory backend (`memory-store.ts`), free map, read coalescing, OPFS backend |
| `keys/`            | contracts → executor, key index  | Memcomparable key encoding: reusable writer, batched encoder over vectors, indexable-type rules                                                                                                                                                                                                                                                                                                                        |
| `format/`          | format, write path               | Log frames and multipart transactions, header slots, in-memory and incremental recovery scans, replay rules, request ledger, manifest types, the reachability walk (`reachable.ts`)                                                                                                                                                                                                                                    |
| `page/`            | format                           | Page codecs, page directory and statistics, streaming group writer, chunk reader (ChunkPages), inflated-page cache                                                                                                                                                                                                                                                                                                     |
| `spill/`           | executor (OPFS service: storage) | Spill contract (one long-lived scratch file, batching charged to the operator's budget); in-memory double                                                                                                                                                                                                                                                                                                              |
| `memory/`          | executor                         | Memory governor: admission, progress loans, held vs allocated bytes (reference implementation)                                                                                                                                                                                                                                                                                                                         |
| `exec/`            | executor                         | Entry point (`executor.ts`), result frames (`result.ts`), scans (`scan.ts`, `memory-scan.ts`), table rules (`rules.ts`), push driver (`pipeline.ts`), fetcher (`fetch.ts`), comparators, decimal SUM                                                                                                                                                                                                                   |
| `ir/`              | lowering, executor               | Operator IR (`ir.ts`); the front-end boundary (`lowering.ts`)                                                                                                                                                                                                                                                                                                                                                          |
| `access/`          | write path + key index           | Storage access contracts the write path implements: snapshots and segments, catalog, key-index probes and ranges, `modify` (rows, conflicts, RETURNING), transactions, group commit, sequences, delivery                                                                                                                                                                                                               |
| `testing/`         | all                              | Differential-harness adapter for a partial new core                                                                                                                                                                                                                                                                                                                                                                    |
| `write/` (to come) | write path + key index           | Group commit, memtables, key index runs, maintenance (implements `access/`)                                                                                                                                                                                                                                                                                                                                            |
| `lower/` (to come) | planner lowering                 | `Lowerer`, cost model, statistics                                                                                                                                                                                                                                                                                                                                                                                      |
| `owner/`           | ownership                        | Owner/client protocol (`protocol.ts`), wire payloads (re-exported from exec) and stream dictionaries (`frame.ts`), ledger outcomes per request (`outcomes.ts`), wire errors (`errors.ts`), client sequencing (`sequence.ts`), election, liveness and step-down (`election.ts`), public client API (`api.ts`), engine boundary and idle-transaction timer (`engine.ts`); composition root to come                       |

## Cross-track interfaces (owned by the implementing track)

These integration interfaces are deliberately not frozen here. Each is the first deliverable of
the track that implements it, reviewed adversarially before the consuming tracks build on it
(lead ruling on review A1-3 finding 2):

| Interface                                                                                                     | Owner                     |
| ------------------------------------------------------------------------------------------------------------- | ------------------------- |
| Owner/client protocol: messages, request identity, step-down, liveness                                        | B5 ownership              |
| Executor entry: `PhysicalPlan` + parameters + snapshot + grants → streaming columnar results                  | B4 executor               |
| Extent allocation (free space from the root) and the data writer's lifecycle                                  | B1 storage                |
| Key-index lookup, range and batch APIs; `modify` (row ids, conflicts, RETURNING); snapshot and catalog access | B3 write path + key index |

`SnapshotReleasedError` and the snapshot type live in `access/snapshot.ts`; `FetchRequest.snapshot`
is access's `Snapshot`.

## Dependency rules

`dependency-rules.test.ts` enforces these.

- Nothing outside `next/` imports `next/` until cut-over.
- Inside `next/`, each module imports only the layers below it:

  ```text
  types/temporal ← vector ← memory ← exec
  types, hash, vector ← keys ← exec, write
  hash ← storage ← format ← write
  vector, keys, storage, format, memory ← access ← exec, write, lower
  hash, storage ← spill ← exec;  vector ← ir ← lower
  types, hash, vector, keys, storage, format, memory ← page ← exec, write
  owner ← everything;  testing ← everything
  ```

- Non-test sources use old code only through `import type`, except in the bridge modules:
  - `lower/` may call the front end (parser, catalog rewrites, optimizer);
  - `owner/` is the composition root;
  - `testing/` holds the differential adapter.
- The executor never calls the front end.

## Contracts and their invariants

### Types (`types.ts`, `temporal.ts`)

- Every value has an exact physical type:
  - int32 (SMALLINT, INTEGER), int64;
  - float64, float32 (REAL);
  - decimal;
  - date as int32 days, time and timestamp as int64 µs, both on the 2000-01-01 epoch;
  - interval;
  - text, json, enum, array (bytes); uuid.
- No tagged strings.
- Date and timestamp ±infinity are PostgreSQL's int min/max sentinels and never take the f64 fast
  path.
- Collation belongs to expressions, never to stored types.

### Vectors (`vector/`)

- A batch holds at most 2,048 rows and about 1 MiB in total, so wide rows get fewer rows. One huge
  value is a batch of one.
- Arrays are stored as reversible values (`array-value.ts`) and decoded before any operation.
  Values are validated on write and on read: int32 bounds, element ranges, NUMERIC limits, and
  an empty array only as zero dimensions.
- int64 is exact. It uses `Float64Array` only when every slot is proven to lie within ±(2^53 − 1),
  and `BigInt64Array` otherwise.
- Decimals have three tiers: int64, then 128-bit limbs, then `bigint[]`. Kernels widen before
  overflow, and spill keeps the tier.
- A decimal vector has one uniform scale, or a scale per slot when values differ. Coefficients are
  never rescaled to a shared maximum; kernels partition by scale.
- `VarlenBuilder` allocates from its grant's held bytes, stops at the batch byte target, admits one
  oversized value alone, and can truncate a partly filled row.
- Garbage under a null slot never raises an error.
- Text stays UTF-8 until output.
- A consumer borrows a batch until it signals readiness, or owns it under an explicit transfer.

### Storage (`storage/`)

- Ownership comes first. Acquisition takes the Web Lock, then opens the backend's fence, which may
  be asynchronous (`createSyncAccessHandle` is), with bounded retry and the deadline enforced
  during the open. If the fence stays busy, it fails with `OwnerUnresponsiveError`. A handle that
  arrives late, or after the lock was stolen, is closed, and every failure releases the lock.
  There are two fences:
  - OPFS: the sync handles themselves, held by one context at a time.
  - IndexedDB: a durable epoch record that every transaction checks.
- Every mutation is async and checks ownership in the same step that applies its effect. A fenced
  owner changes nothing.
- The store is four long-lived files: log, data space, and two root slots. OPFS adds the spill
  scratch file and the epoch counter (two checksummed slots, flushed before a lease exists), all six
  opened under the same fence. Extents are reused in place inside the data space.
- `publish` is atomic and durable: it flushes data, then writes the inactive root slot.
- The log has a flush barrier and two header slots.
- **The extent space** (`extents.ts`) is the only caller of the data-space calls and of `publish`.
  - A writer reserves extents, appends checksummed blobs, seals, and either publishes once or
    aborts. Abort frees at once; a crash before publish leaves orphans that the walk on open frees.
  - `publish(root, { writers, retired })` is serialized and atomic. Two-generation reclaim: retired
    extents are reused only after the _next_ publish is durable and no pin of an older generation
    remains, so the current and previous roots always read intact.
  - A rejected publish is indeterminate: the space keeps everything allocated and refuses writes
    until the owner reopens.
  - Free space is rebuilt on open from the extents the current and previous roots reach (the format
    track's `reachable`), first fit, and released to the platform only as a free tail.
  - Blobs are at most 1 MiB (larger structures span several), and one read asks for at most 4 MiB.
  - It is the write path's only way to the data space: reads, `peek`, writers, `publish`, pins.
  - A builder can `extend` its extent: in place when the space after it is free, else relocated
    once at seal. Every `BlobRef` carries its extent and its offset within it.
  - Startup: fence → store session → extent space (walks both roots) → write path (the single log
    scan and replay) → spill sweep → ready.
  - One cache of encoded, verified blobs, charged to the governor and evicted before anything
    spills. Misses are read in coalesced slabs (gaps ≤ 64 KiB, slabs ≤ 1 MiB) and copied out, so
    eviction frees exactly what it uncharges. Every leased byte is charged to the reader's budget
    until release, or the read is refused with `ReadDeniedError` (borrow, then retry); `peek` is the
    synchronous, uncharged hit path.
  - Implemented in `space.ts` (writers, publication, reclaim, open walk, `audit`) and
    `page-cache.ts` (segmented LRU: a one-pass scan recycles probation and leaves hot blobs).
    `space-crash.test.ts` crashes it at every physical step of a workload and reopens.
- **The in-memory backend** (`memory-store.ts`) is the file store over growable in-memory files:
  the store of an in-memory database and of the end-to-end suites. It has no scratch space.
- **The OPFS backend** (`opfs.ts`: `createOpfsBackend`, `webLocks`) is the file store over real
  sync access handles: the Web Lock, then every handle with bounded retry; a lost lock closes them
  at once; quota signals become `StoreQuotaError` by name. It passes the storage kit over
  simulated handles (`opfs.test.ts`), and `browser-tests/next-storage.spec.ts` runs conformance,
  a worker killed during publish, a closed owner tab, the cross-worker fence and spill in real
  Chromium, Firefox and WebKit.
- **The IndexedDB backend** (`indexeddb.ts` over `idb-like.ts`) is the same contract where OPFS is
  absent: the Web Lock plus a durable epoch read inside every read-write transaction (a stale
  owner that never heard of its lock loss, as after Safari's bfcache, changes nothing); byte
  records keyed by offset, kept disjoint, ArrayBuffers only; relaxed writes, strict publish and
  log flush; the spill store. `sim-idb.ts` simulates IndexedDB as a checksum-chained write-ahead
  log on the simulated disk, so the IndexedDB backend passes the same storage kit and extent-space
  crash sweep as the files. The browser suite runs it on the persistent profile and in Firefox
  private browsing and a WebKit ephemeral session.
- `select.ts` (`chooseBackend`) picks OPFS where a dedicated worker can open sync handles, else
  IndexedDB (reporting OPFS's refusal), else memory (Safari Lockdown Mode). Layout `minnow-core/`
  and `minnow-core:<name>`, apart from the pre-reset engine's names.
- A writer that interleaves many builders (the page format's group writer) passes a `budget` to
  `beginWrite`: each builder stages up to 256 KiB of its own. Without it, alternating page-by-page
  appends cost one platform write per page (measured 2–9× a flush on Chromium and Firefox).
- The simulated disk crashes at any physical step. Every write after the last flush may land
  whole, torn or not at all, and may land out of order. It models honest and lying flush.
- A backend is accepted by the **storage kit** (`kit.ts`): the conformance cases together with a
  physical durability sweep. A store that skips its data flush or its log flush fails the kit.

### Log, replay and ledger (`format/`)

- Frames are checksum-chained. A transaction is one or more consecutive frames, numbered by part
  index.
- The acknowledged boundary must match a transaction end exactly: offset, chain and LSN.
- Incomplete transactions are dropped, and recovery truncates the log before appending again.
- Frames are at most 1 MiB; larger commits are multipart.
- Recovery scans incrementally: one frame in memory at a time (`RECOVERY_SCAN_BYTES`, admitted and
  charged before the scan), transactions handed to a sink that stages their parts. If anything
  fails, the sink's staged parts are abandoned and the memory is freed.
- A transaction is an ordered list of effects: DDL, sequence reservations, row changes and a
  request outcome, replayed in order. A table created by a transaction replays that
  transaction's rows.
- The root checkpoints global state at `globalLsn` and each table's rows at its own
  `flushedThroughLsn`.
- A checkpoint that covers a schema change of a table also covers that table's rows through it,
  and replay checks schema versions.
- A transaction too large to log row by row stages its rows and their index runs in a published
  root, then commits with a `publishedRows` frame naming that root and those indexes. Replay
  applies rows and runs together exactly once (skipped once folded or after a checkpointed DROP),
  refuses a mismatch between the frame's indexes and the staged runs or a rolled-back root, and
  drops staging no commit claimed.
- The log must reach every checkpoint the root claims.
- A rolled-back root is detected through the log header's `rootSequence`.
- Commits are exactly-once through a bounded per-client ledger of outcomes, with expiry enforced
  by the receiver (`RequestExpiredError`). A statement inside an open transaction records a
  non-durable outcome keyed by its client, engine session and session generation, which answers
  its duplicates. Ending a generation (`endSession`) drops that session's statement outcomes and
  pending statements only: a late completion is ignored, a retry is refused with
  `SessionLostError`, a pending commit still records, and other sessions keep their protection.
  Standalone queries (reads, `SELECT nextval(...)`) are at-least-once: settled without an outcome,
  so a retry runs again.
- Errors shared across tracks (`OwnerUnresponsiveError`, `RequestExpiredError`,
  `SessionLostError`) are defined once, in `errors.ts`.

### Key encoding (`keys/key-encoding.ts`)

- Memcomparable: byte order gives SQL order, and byte equality gives SQL equality.
- Equality keys (persisted) and sort keys (query time) are separate.
- Persisted index keys are scalar types only. Indexes on arrays and JSON are refused at DDL; those
  types sort and group through the semantic comparators in `exec/semantic.ts`.
- An enum index is equality-only (its key is the label). ORDER BY enum uses the declaration rank.
- Operators encode a whole batch straight from vectors into a reused buffer (`encodeKeyBatch`). Its
  buffers are allocated from a grant, stop at the batch byte target (one oversized key is held
  alone at its exact size) and shrink on reset; a key that does not fit is reported with its
  required size, and encoding resumes from where it stopped.
- Frozen by golden vectors and generated tests over every encodable type.

### Pages (`page/`)

- A column chunk is `[pages][dictionary page][directory]` in one extent; each is one blob of at
  most 1 MiB, and its `BlobRef` carries the extent (R15.1). `ColumnChunk.data` is the chunk's
  range in that extent (offset 0 for its own extent, its place in a pack otherwise); page and
  dictionary offsets are relative to it. The directory lists every page's
  rows, encoding, null count, min/max and checksum, plus the chunk's distinct sketch (a range of
  the directory blob), so pruning and planning never read pages.
- Encodings (`encode.ts` has the selection rules): constant (and all-null), plain, bool bitmap,
  frame of reference at u8/u16/u32 over an int64 base, delta (FOR over successive differences,
  a checkpoint every 128 rows), and dictionary codes (u8/u16/u32, or one code) into a
  group-scoped dictionary.
- NUMERIC: a declared scale makes a fixed-scale chunk; an unconstrained NUMERIC keeps each
  value's scale (a per-row section, or one scale per page), and its min/max are normalized sort
  keys (value order, scale ignored, R15.10), chosen by comparing the exact key bytes. Coefficients are stored by the int64 rules when they
  fit, else as 128-bit limbs. Past 128 bits, and a single value too large for one page, is
  `UnsupportedPageLayoutError` (large values: Phase 2, R15.4).
- Floats: min/max follow PostgreSQL's order (NaN above +Infinity): max is NaN when any NaN is
  present, both are NaN only when no other value is.
- gzip is a second stage only maintenance compaction applies (writer option `gzip`; never a
  memtable flush), to varlen and uuid pages ≥ 16 KiB that halve. Readers keep gzip pages inflated
  in the `InflatedPageCache` (charged at inflated size, evictable when unpinned), so a page
  inflates once per cache residency and a cached one loads synchronously (R15.5).
- `GroupWriter` writes through the caller's `ExtentWriter` (R15.2): small chunks are packed into
  one shared extent at `finish`; a larger chunk opens an `ExtentBuilder` sized from its bytes per
  row, appends each page as it is encoded and calls `extend` when it runs short. Memory per
  column is about one raw page (32 KiB) plus its sketch and small-chunk pages; dictionaries are
  bounded and charged apart. Everything is charged before it is allocated (a 200-column text
  group peaks at about 10.6 MB without dictionaries).
- `ChunkReader` is the executor's `ChunkPages` over the space's `ExtentReader`. The space
  verifies each blob's checksum once; the reader parses (structural checks) when it loads a page.
  `open`, `load` and `loadDictionary` are synchronous when everything is cached. Concurrent loads
  of a page share one read; `close` is final, and loads still in flight reject with
  `ReaderClosedError` after giving back what they read. Every byte held (pages, inflated pages,
  the parsed directory) is charged to the reader's budget until `release` or `close`.
- Decoders write into caller-owned vector buffers, with no JS strings and no per-value
  allocation: by range, by selection (late materialization), or as dictionary codes. A page
  flagged `safe` is rechecked when decoded into f64 unless its int64 or decimal header bounds every
  value (interval pages are always rechecked).
- `memory-extents.ts` is an in-memory extent space (writer, builder with `extend`, verified and
  cached reads) for tests and benchmarks; `scripts/page-format-bench.mts` measures the codecs
  against today's block format.

### Manifest (`format/manifest.ts`)

- The root is O(tables + ledger). Nothing is O(rows).
- Every reference names the extent it lives in (`BlobRef = {extent, offset, length, checksum}`,
  the storage track's shape), so reachability walks list extents without inferring them. Small
  blobs share 4 KiB-aligned extents; an extent is reclaimed as a unit. Page offsets are relative
  to their chunk's data blob.
- Every reference carries the checksum of what it names (a chunk's pages carry theirs).
- Space is reusable only when neither the current durable root, nor the previous one (the other
  root slot), nor any pinned root reaches it (two generations), and startup walks both roots.
- Startup order: acquire the fence → open the store session → open the extent space (walk the
  current and previous roots) → open the write path (a single log scan and replay, `publishedRows`
  staging included) → spill sweep → ready.
- Snapshot pins live in memory and are dropped when their client's lock is released.
- `walkReachable(root, reader)` streams every reference a root reaches (catalog, table
  statistics, group directories, staged transactions' group directories and index runs, group
  roots, deletion bitmaps, row-id columns, chunk extents, page directories, sketches, index
  entries, fences and Bloom filters); `reachableExtents(codecs)` is the extent space's
  `ReachableExtents` over it. A failed metadata read rejects the walk.

### Storage access (`access/`)

The single read/write contract (ruling R1): the write path implements it; the executor, lowering
and the owner import it directly.

- **One writer at a time.** `begin()` hands out the writer slot in FIFO order, so transactions never
  conflict. Readers never take it.
- **`commit()` is the irreversible boundary.** Before it the owner may cancel; once called, the
  transaction commits unless the write path fails. The write path records committed outcomes in the
  ledger (when durable under strict, when applied under relaxed); the owner records failures and
  settles no-effect requests.
- **Visibility is one comparison.** Every in-memory change has a `seq`: its commit LSN, or
  `PENDING_BASE + statement` while its transaction is open. A view sees `seq ≤ bound`. A read
  snapshot's bound is an LSN; a statement view sees earlier statements of its transaction, never
  its own.
- **Strict reads see only durable commits.** Nothing reaches a client before every commit its view
  observed, and every sequence block it drew from, is durable (`durable(lsn)`).
- **Sequences are not transactional.** Blocks (1 → 1,024 values, adaptive) are reserved by
  standalone sequence frames, so a value waits for one group flush, never for COMMIT. A crash skips
  the rest of a block. A standalone `SELECT nextval(…)` is at-least-once: a retry draws new values.
- **Scans are built on segments.** A snapshot lists each table's segments in locator order (groups,
  then memory), with sound page bounds (exact or null), deletion vectors for live rows, and
  dictionary codes kept. Groups are read through B1's verified cache with the format chunk readers. A
  memory chunk read that stops early (varlen output full or denied) resumes at `resumeRow(result)`.
- **Retention is bounded.** A reader borrows at most one chunk per memory segment. A frozen memtable
  stays resident (capped, writers wait past the cap) only until its flush publishes; then its
  readers are re-pointed to the published group under a pin taken during that publication, and
  the memtable is freed. Writers never wait on readers.
- **Large transactions commit through a staged root.** Rows past the memtable bounds are written as
  staged groups with staged index runs, published under the transaction's id, and committed by a
  `publishedRows` frame.
- **Indexes resolve by tombstones.** Entries are `(key, rowId, put | tombstone)` in layers, newest
  first. A delete tombstones every index, so a lookup never reads a deletion bitmap
  (`resolveVersions`).
- **The executor evaluates, the write path enforces.** Expressions, casts, defaults, generated
  columns, CHECK (through `TableRules`, which the executor's `compileTableRules` provides) and
  RETURNING (spooled until the owner acknowledges the commit) belong to the executor. NOT NULL,
  unique, foreign keys and their actions, and "row affected twice" belong to the write path. Unique
  and foreign keys are checked at `Statement.end()`; ON CONFLICT probes immediately.
- **Statements are atomic.** After any error the only call is `rollback()`, which undoes the
  statement.
- **`openWritePath` does the single log scan.** It scans, truncates and replays the log itself,
  after the extent space opens and before the spill sweep.
- **Storage goes through the extent space only.** No free map of its own; the log is reset only
  after the publish that covers it resolves; on an idle database with
  `waitingBytes > max(16 MiB, 5% of the data space)` it publishes one no-change root (ruling R10).
- **A failed flush or publish fails the write path.** Close and reopen; the ledger answers retries.

### Spill (`spill/`)

- The backend's scratch space (`StorageBackend.scratch`: the OPFS `spill` file, or IndexedDB's
  `spill` object store), fenced like the store, with space reused across queries through the
  storage free map. Only the in-memory backend uses the in-memory service.
- Logically emptied on open, never named by the root, catalog or log.
- No per-query caps.
- Write batching is charged to the operator's budget, and is skipped when the budget refuses, so
  spilling never needs memory beyond the page in hand.
- `scratch-spill.ts` is the service over a backend's `ScratchSpace` (the OPFS `spill` file or
  IndexedDB's `spill` store): one free map, 256 KiB batches written as extents that extend
  the file's last one, checksummed reads, `sweep` at startup.

### Memory (`memory/governor.ts`)

- One cap, which `usedBytes` never exceeds.
- Pipelines are admitted with plan-bounded minimums. Admission waits; it is never refused.
- A grant _holds_ bytes, and its operator _allocates_ buffers out of them, so a non-spillable
  operator works inside its admitted minimum.
- Growth is charged before it happens, including the overlap while a buffer is copied.
- The page cache is evicted before anything spills.
- Progress loans are scheduled before admissions and may use the reserve: a queued admission,
  growth or the cache never delays a loan, and a loan waits only for earlier loans to be repaid.
  Progress follows if each loan covers one bounded step. A loan belongs to its grant from the
  moment it is served, so a close at any point returns it; `repay` validates before it changes
  anything.
- The drain is not reentrant, a closed grant is inert, and abort listeners are removed.
- Loan-backed bytes leave a grant only through `repay`; `release` returns only holdings beyond the
  outstanding loan (`LoanBackedReleaseError` otherwise).
- `tryAdmit` admits synchronously when the minimum fits and nothing is queued (the point-read fast
  path); it never jumps a queue and otherwise returns null.
- The one size error is a single value larger than the reserve.

### Pipeline (`exec/pipeline.ts`)

- Kernels are synchronous.
- `push` returns `ready` or `blocked`, and a blocked batch stays borrowed until `ready()`.
  Operators resume partly consumed batches themselves.
- `next()` and `finish()` return a value when nothing waits and a Promise only when they must
  wait; the driver awaits only real Promises, and `drive` returns no Promise when the whole
  pipeline finished synchronously.
- After `YIELD_INTERVAL_MS` (4 ms) of synchronous work the driver yields a task (`setImmediate`
  or a `MessageChannel` message, never a resolved Promise) at the next batch boundary.
- `abort` releases grants, spill files and transferred batches.
- The driver adds no cancellation machinery without a signal, and with one each wait removes its
  listener, so a long stream retains nothing per batch.
- Each edge holds at most one in-flight batch, plus whatever credits the producer has handed out
  as transfers.

### Executor entry point (`exec/executor.ts`, `result.ts`, `scan.ts`, `memory-scan.ts`, `rules.ts`)

- `executor.prepare(plan)` compiles a plan once; `bind(values)` fills its placeholders;
  `execute(request)` runs it over the access contracts: a read snapshot for a query, or an open
  `Statement` for DML, which the executor ends or rolls back. It never commits.
- The executor admits its own pipelines with plan-bounded minimums. State that outlives a
  pipeline is in spillable grants. Nothing is refused for size.
- A plan from another catalog version fails with `PlanStaleError` before anything runs.
- Three completions: `executed` (the plan ran; for DML the statement ended), committed
  (`acknowledgeCommit()`: autocommit RETURNING waits in a charged, spillable spool until then),
  and `delivered` (every frame released; the result grant closes).
- Results are wire-ready frames: a plain-data payload plus a transfer list, about 1 MiB each,
  with per-stream dictionary deltas up to 1 MiB per stream. A frame stays charged until the
  client's credit returns, so a slow client pauses the pipeline. There is no row-count cap.
- The pipeline driver progresses synchronously whenever sources and sinks are ready and awaits
  only real Promises, so a point read whose index blocks and pages are cached runs inside
  `execute` and its frame is taken with `results.poll()`. Long work yields a task (not a
  microtask) every 4 ms, so owner messages are serviced even during a cached scan.
- The frame payload in `result.ts` is the one wire definition (the owner re-exports it); its
  dictionary deltas are per frame, over per-stream dictionaries.
- Scans read access/ segments and the page track's chunk readers (`scan.ts`); a memory-segment
  scan (`memory-scan.ts`) borrows one chunk at a time, passes on async re-pointed chunks, and
  resumes a partial one (a full or denied varlen builder) at `resumeRow`. CHECK and
  generated columns are compiled by `compileTableRules` into the write path's `TableRules`.

### Owner and client protocol (`owner/`)

- One owner per database holds the store's fence: a dedicated owner worker started by a page, or
  a dedicated worker hosting in place. Every other context is a thin client over a MessagePort
  brokered by a SharedWorker; the owner tab's own client uses a direct channel.
- Startup: fence → store session → extent space → write path (one log scan and replay) → spill
  sweep → ready.
- Election is the host lock `minnow:<db>:host`, requested only by an eligible context that
  currently needs an owner, so idle pages keep bfcache and a dead host is replaced by the first
  waiter.
- A client holds its liveness lock only while it has pins or `live` requests (plus 1 s of idle
  hysteresis). The owner watches it only then; a grant ends the client's connection.
- On `hidden` the owner flushes pending relaxed commits; it terminates on `freeze` and `pagehide`,
  and steps down gracefully on `hidden` only on mobile.
- Every request has a per-client `seq` (the ledger sequence) and exactly one terminal reply.
  Cancellation is acknowledged by that reply; the write path's `commit()` call is the point of no
  return.
- Only commits are exactly-once. Statements that change an open transaction keep a
  session-scoped outcome, so a duplicate never runs twice; everything else, standalone
  `nextval` included, is at-least-once.
- A transaction idle for `transactionIdleTimeoutMs` (30 s) is rolled back; the session then fails
  with `TransactionIdleTimeoutError` until the client ends it.
- Result frames are the executor's payloads, pulled and transferred without a copy, and charged
  until the client releases them. Dictionaries are per stream, sent as frame-level deltas.
- A warm cross-tab key read has a synchronous owner path and one message each way: targets
  ≤ 0.25 ms Chromium, ≤ 0.4 ms Firefox, ≤ 0.6 ms WebKit; p95 ≤ 5 ms beside analytics.
- Another protocol version is refused; a newer build supersedes older tabs, which reload.

### IR (`ir/`)

- Fully typed, positional and plain data. It is bound once, with parameter slots.
- Late materialization uses `scan.emitLocator` and `fetch`, executed through the `RowFetcher`
  contract (`exec/fetch.ts`): an explicit, enforced output capacity, charged and bounded staging,
  and partial completion that may reread pages.
- `apply` memoizes only when evaluation multiplicity is unobservable.
- `lowering.ts` lists the front-end passes that are kept and the code that lowering replaces.

### Differential adapter (`testing/differential-adapter.ts`)

- A track implements `NextCore` (any subset of it) and runs the frozen harness against the current
  engine.
- Values are converted to the oracle's representation; known divergences fail loudly.

## Platform facts the design relies on

From the browser storage probe of 2026-11-05 (`scripts/storage-probe/REPORT.md` on the probe
branch).

- **The OPFS sync handle fences on all three engines.** No other context can open, write, remove
  or move a file while its handle is held, even after stealing the lock.
- **The lock frees before the handle.** The lock frees 2 ms–2 s before the handle. Busy shows up
  as `NoModificationAllowedError` on Chromium and Firefox, and as `InvalidStateError` on WebKit.
- **A stolen owner must close its handles.** An owner whose lock is stolen gets `AbortError` and
  must close its handles at once: the steal alone does not stop a hung owner.
- **Keep the hot path on open handles.** On WebKit, a busy tab delays its owner worker's async
  OPFS calls and messages, but not reads or writes on an already-open sync handle.
- **Group commit is essential.** `flush()` is one real disk flush on every engine: 3.4–4.7 ms.
- **Cross-tab round trips are slow on WebKit.** Measured at about 55–65 µs on Chromium, 140 µs on
  Firefox and 280 µs on WebKit (p95 520 µs).
