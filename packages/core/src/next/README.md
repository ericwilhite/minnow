# `next/`: the rebuild core (unreleased)

This is the storage core and executor that replace the current engine. The plan is in `PROPOSAL.md`,
sections "Architecture" and "Wide and large analytics".

Nothing here ships yet:

- No package entry point imports it.
- `package.json` `files` excludes `dist/next/**`.
- `tree-shaking.test.ts` and `scripts/package-shape.test.ts` prove both.

Public-contract decisions are in [`DECISIONS.md`](./DECISIONS.md).

## Module map

| Module        | Owner track                      | What it is                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `types.ts`    | contracts                        | Physical types; the one SQL → physical mapping; declared widths are mandatory; column statistics types shared by format and exec                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `errors.ts`   | contracts                        | Errors shared across tracks: `OwnerUnresponsiveError`, `RequestExpiredError`, `SessionLostError`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `temporal.ts` | contracts                        | DATE/TIMESTAMP on PostgreSQL's 2000 epoch; infinities; ISO text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `hash/`       | contracts                        | xxHash32, the only checksum                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `vector/`     | contracts → executor             | Batches (byte-bounded), vectors, validity, exact int64, int128 decimals, UTF-8 builder under a grant, locators                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `storage/`    | storage                          | Backend contract (lock + async fence open, async mutations), file store, simulated disk with crash model, the storage kit (conformance + durability sweep); the extent space (`extents.ts`: allocation, writer lifecycle, publish, pins and reclaim, the verified page cache) and its implementation (`space.ts`, `page-cache.ts`), the in-memory backend (`memory-store.ts`), free map, read coalescing, OPFS backend                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `keys/`       | contracts → executor, key index  | Memcomparable key encoding: reusable writer, batched encoder over vectors, indexable-type rules                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `format/`     | format, write path               | Log frames and multipart transactions, header slots, recovery scans, replay rules, request ledger, manifest types and codecs (root, paged group directory, group root), parsed-blob cache, metadata reader, reachability walk                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `page/`       | format                           | Page codecs, page directory and statistics, streaming group writer, chunk reader (ChunkPages), inflated-page cache                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `spill/`      | executor (OPFS service: storage) | Spill contract (one long-lived scratch file, batching charged to the operator's budget); in-memory double                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `memory/`     | executor                         | Memory governor: admission, progress loans, held vs allocated bytes (reference implementation)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `exec/`       | executor                         | Query factory (`create.ts`, contract `executor.ts`), result frames (`result.ts`), scans (`scan.ts` contract, `table-scan.ts`, `memory-scan.ts`, `column-target.ts`, the pushdown seam `pushdown.ts`), table rules (`rules.ts`), push driver (`pipeline.ts`), fetcher (`fetch.ts`), expression kernels (`kernels/`), filter and project (`filter.ts`), aggregation (`agg/`), comparators, decimal SUM; heavy operators (track B7): hash join (`join*.ts`), sort / top-N / per-group top-k (`sort*.ts`), windows (`window*.ts`), column store (`column-store.ts`), spill pages (`spill-pages.ts`) and the spill pool (`spill-pool.ts`) shared by join, sort and aggregation spill                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `ir/`         | lowering, executor               | Operator IR (`ir.ts`); scalar function and operator table (`functions.ts`); table rules in IR form (`rules.ts`); executor costs the planner reads (`costs.ts`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `access/`     | write path + key index           | Storage access contracts the write path implements: snapshots and segments, catalog, key-index probes and ranges, `modify` (rows, conflicts, RETURNING), transactions, group commit, sequences, delivery                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `testing/`    | all                              | Real-storage SQL differential adapter (`storage-core.ts`) over the query factory and write path; explicit protocol stand-ins (`owner-standins.ts`) for tests that need synthetic scheduling or persistence, an in-memory Web Locks for election tests (`fake-locks.ts`); the registry of intended divergences from the current engine (R21, `divergences.ts`); the write path's generated differential against PostgreSQL and the current engine                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `write/`      | write path + key index           | Implements `access/`: catalog and DDL, memtables (chunked columns, key deltas, deletions; memory charged before allocation, backpressure across tables), transactions with an undo log, modify and constraint checks, log appender and group commit, commit-body codec, flush to column groups and index runs, background index run merges (`merge.ts`: levels of fan-in 8, streamed, task-yield slices, published under the lock flushes share; writers wait only past 24 level-0 runs), log recycling past 8 MiB (the writer slot, every table flushed through the cut, a root naming a new incarnation, then the reset; frames appended meanwhile go to the new log), re-point, recovery (`openWritePath`), sequences; manifests through `format/`'s codecs (paged group directory) and metadata reader, per-table planning statistics stored with the root at every publish (an open reads one blob per table, never a group root), the resident group directory held as shared immutable typed-array chunks (charged exactly, refcounted by the generations that hold them; an append copies one chunk), each root charged until its last reader closes, on a grant of their own. Tested by a crash sweep over every simulated-disk step (and by the generated differential in `testing/`) |
| `lower/`      | planner lowering                 | `Lowerer` (`lowerer.ts` contract, `create.ts`): front-end passes over the catalog, typing (`coerce.ts`, `expressions.ts`), queries (`query.ts`), FROM clauses, join order and subqueries (`from.ts`), access paths (`access-path.ts`), windows (`windows.ts`), plan building (`plan.ts`), DML and MERGE (`modify.ts`), DDL, table rules, result descriptors; an in-memory catalog; front-end inventory (`INVENTORY.md`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `owner/`      | ownership                        | Owner/client protocol (`protocol.ts`), wire payloads (re-exported from exec) and stream dictionaries (`frame.ts`), ledger routing (`outcomes.ts`), wire errors (`errors.ts`), client sequencing (`sequence.ts`), election, liveness and step-down (`election.ts`), public client API (`api.ts`), engine boundary and idle-transaction timer (`engine.ts`), planner port (`planner.ts`), the composed engine (`compose.ts`), the protocol server (`server.ts`) and client (`client.ts`), the owner worker runtime (`host.ts`), the port broker (`broker.ts`), the client-side election and lifecycle (`connector.ts`), Web Locks (`web-locks.ts`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

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

## Rollback and in-flight publication

Rollback can revive a partially full frozen memtable only when no flush of its table is in
flight. A flush keeps its inputs immutable through publication; later inserts use a new
memtable. `write/freeze-rollback-race.test.ts` holds publication across rollback and subsequent
inserts over memory and simulated IndexedDB, then checks their rows through durable reopen.
It also checks non-unique index results. The PostgreSQL write differential retains every seed
and step under the real task clock and forced third/seventh-check yields, so publication races
are exercised without requiring host contention.

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
- `write/` also imports `exec/pipeline.ts` for the shared worker task clock and task yields
  (R23.5). This is the only write-to-exec dependency: retirement and merge slice checks use that
  clock, and every write task yield stamps it through the shared scheduler. Publication, table
  and merge entry never reset it. Cached metadata promises do not begin a task.
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
  an empty array only as zero dimensions. Far oversized NUMERIC coefficients are refused by a
  bounded bit-width check before text conversion; values near the digit limit still receive an
  exact decimal-digit check. The stored bytes and accepted value range are unchanged.
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

### Checksum (`hash/xxhash32.ts`)

- xxHash32 is the one checksum of the new core (log frames, header and root slots, blobs and
  pages, spill pages). It equals the reference XXH32 for any range, alignment and byte order,
  frozen by the published `xxhsum` sanity and string vectors plus vectors derived with the
  reference CLI on both sides of the read threshold.
- The seed is taken as `seed >>> 0` (ToUint32) before any arithmetic, so a fractional,
  non-finite or large number means exactly what `>>> 0` makes of it.
- Inputs of at least `WORD_READ_BYTES` (256) read 32-bit little-endian words through one
  `DataView` per call (R15.8); shorter ones read bytes, which is faster there. Words are about
  2.5–3.5× faster from 4 KiB (about 2 GB/s against 0.6 GB/s); cold page loads, which verify
  every page, rose about 1.4× on text columns. Nothing is cached between calls, so no buffer
  outlives its holder. `scripts/xxhash-bench.mts` measures it by length and alignment.

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
    aborts. Abort frees at once; a crash before publish leaves orphans that open frees.
  - `publish(root, { writers, retired })` is serialized and atomic. Two-generation reclaim: retired
    extents are reused only after the _next_ publish is durable and no pin of an older generation
    remains, so the current and previous roots always read intact.
  - A rejected publish is indeterminate: the space keeps everything allocated and refuses writes
    until the owner reopens.
  - Free space is rebuilt on open from the extents the current and previous roots reach, first
    fit, and released to the platform only as a free tail. Ruling R25: each root references a
    paged, offset-sorted list of its extents (`extent-list.ts`), so open reads the two roots'
    lists (`listedExtents`) and no group root; the audit cross-checks them against the format
    track's full walk (`walk`), as the open does in test builds (`verifyAtOpen`). The write path's
    extent-delta sink, `ExtentListKeeper`, keeps each list incrementally: only the pages a
    publication touches are rewritten, the rest shared (DECISIONS.md "R25").
  - Blobs are at most 1 MiB (larger structures span several), and one read asks for at most 4 MiB.
  - It is the write path's only way to the data space: reads, `peek`, writers, `publish`, pins.
  - A builder can `extend` its extent: in place when the space after it is free, else relocated
    once at seal. Every `BlobRef` carries its extent and its offset within it.
  - Startup: fence → store session → extent space (reads both roots' extent lists) → write path
    (the single log scan and replay) → spill sweep → ready.
  - One cache of encoded, verified blobs, charged to the governor and evicted before anything
    spills. Misses are read in coalesced slabs (gaps ≤ 64 KiB, slabs ≤ 1 MiB) and copied out, so
    eviction frees exactly what it uncharges. Every leased byte is charged to the reader's budget
    until release, or the read is refused with `ReadDeniedError` (borrow, then retry); `peek` is the
    synchronous, uncharged hit path.
  - Implemented in `space.ts` (writers, publication, reclaim, open walk, `audit`) and
    `page-cache.ts` (segmented LRU: a one-pass scan recycles probation and leaves hot blobs).
    `space-crash.test.ts` crashes it at every physical step of a workload and reopens.
  - A reader's parsed descriptor rides on its entry (`attachParsed`) and is charged to the cache
    from attachment until the entry is gone, pinned or not; pinning moves only the blob's bytes
    and overhead to the leases. `page-cache-model.test.ts` checks every charge after each step of
    random operation sequences, concurrent cold reads included.
  - Cached bytes are immutable by contract. Test builds enforce it (`verify-cached.ts`): with
    `MINNOW_VERIFY_CACHED_BYTES=1` (every Vitest suite) or `globalThis.__MINNOW_VERIFY_CACHED_BYTES__`
    (a browser harness, or the define in `vite.library.config.ts` for the browser lanes), a lease
    release and an eviction hash the blob again and throw `CachedBytesWrittenError` if a consumer
    wrote into it. Off, it is one branch on a module constant.
- **The in-memory backend** (`memory-store.ts`) is the file store over growable in-memory files:
  the store of an in-memory database and of the end-to-end suites. It has no scratch space.
- **The OPFS backend** (`opfs.ts`: `createOpfsBackend`, `webLocks`) is the file store over real
  sync access handles: the Web Lock, then every handle with bounded retry; a lost lock closes them
  at once; quota signals become `StoreQuotaError` by name, as does a write the platform reports
  short (Chromium returns a bogus count for a write the quota cuts short instead of throwing). It passes the storage kit over
  simulated handles (`opfs.test.ts`), and `browser-tests/next-storage.spec.ts` runs conformance,
  a worker killed during publish, a closed owner tab, the cross-worker fence and spill in real
  Chromium, Firefox and WebKit.
- **The IndexedDB backend** (`indexeddb.ts` over `idb-like.ts`) is the same contract where OPFS is
  absent: the Web Lock plus a durable epoch read inside every transaction, reads included (a stale
  owner that never heard of its lock loss, as after Safari's bfcache, changes and reads nothing);
  byte records of ≤ 256 KiB keyed by offset, kept disjoint, ArrayBuffers only, each with an entry
  in a companion index store, so no read or write needs a cursor over record values
  (`idb-bytes.ts`: Firefox's cursor read the neighbouring record's value and held bulk writes to
  ~6.7 MB/s; now 5× faster end to end). Data/log open reads only the last-record boundaries;
  reads and mutations select needed records from the persisted index inside their transaction.
  No per-record data/log index remains resident. Audit and test-build open validate the full
  physical index (an orphan record, an entry without its record, overlap or a malformed entry is
  `StoreCorruptionError`), in bounded pages without building a resident index. Reads continue to
  refuse a missing or mismatched selected record. Scratch uses the eager block index charged to
  its memory registration. Relaxed writes are
  batched in call order and committed with each barrier (publish, log flush, release, truncate,
  reset) on one commit chain (this assumes each engine's one write-ahead log per database commits
  in order, so a strict commit makes earlier relaxed ones durable; checksums turn a broken
  assumption into a detected corruption, never a silent read: storage/contract.ts); range lists
  of any length are driven iteratively, and a release of more than 1,024 ranges is split into
  transactions of at most that many with a task between them (released space is free, so a crash
  between them only leaves free space to be reused). The log's last
  record is kept in memory while under 64 KiB and rewritten whole by the next commit, so log
  records scale with log bytes / 64 KiB rather than with commits (the tail's buffer is charged to
  the memory registration); truncate and reset move the
  logical end when called, in order with appends. A malformed or exhausted epoch refuses
  ownership; the spill store's record index is charged to the spill service's registration.
  Every acquisition opens its own connection, and every transaction of it runs through one
  serialized chain. Releasing the lease lets in-flight transactions finish (up to 500 ms), aborts
  any left and waits for their abort events (up to 1 s more: past it, the release still closes and
  unlocks but rejects with `ReleaseUnsettledError`, the outcome unknown), closes the connection,
  then releases the lock, and reports how many drained and how many it aborted: WebKit can block IndexedDB for the browser
  session when a worker dies with two chains of transactions in flight on its connection
  (DECISIONS.md "Step-down").
  `sim-idb.ts` simulates IndexedDB as a
  checksum-chained write-ahead log on the simulated disk with IndexedDB's request order, activity
  and clone rules, so the IndexedDB backend passes the same storage kit and extent-space crash
  sweep as the files; the browser suite checks the simulator against real IndexedDB on pinned
  scenarios (`idb-scenarios.ts`) and runs the backend on the persistent profile and in Firefox
  private browsing and a WebKit ephemeral session.
- At scale: an opt-in Chromium run (`MINNOW_NEXT_STORAGE_HEAVY=1`, `MINNOW_NEXT_STORAGE_GIB`,
  default 4) fills OPFS with gigabytes of extents, reopens (the walk, samples, an exact audit),
  reclaims half and refills it in place, then lowers the origin's quota over CDP: a writer fails
  with `StoreQuotaError` while the space stays up (the same space then reads retained blobs and
  audits exactly under the lowered quota, and writes and publishes again once there is room), an
  oversized publish fails and fails the space, and a new owner recovers exactly the last
  successful root (its round and every blob's coordinates and checksum; finding no root is
  refused) and keeps writing. Chromium refuses even the first megabyte of growth under 32 MiB of
  headroom by `estimate()`, so the run records where the refusal came rather than assuming it.
- Kill campaign (Gate A, `browser/next-storage-chaos.ts`): a worker runs a seeded random workload
  (builders, publish, retire and reclaim, log append, flush, truncate and reset, spill) and is
  killed at seeded request checkpoints (`worker.terminate()`, sometimes a page close); the next worker reopens,
  checks the acknowledged root and every acknowledged log byte, audits exactly, and carries on. The
  worker announces each step and waits for the page's go, and posts when the step's storage work
  begins and returns, on its own channel; the backends post every platform request (an IndexedDB
  transaction once created and when it ends, an OPFS sync call) through a test-only observer. The
  worker numbers every message and atomically claims its number in shared memory before posting; the
  page reads the counter just before the kill and receives every message up to it before it
  judges anything (a message that never arrives fails the campaign), so it knows every
  acknowledgement and failure posted before the counter read. A claim interrupted before posting
  fails the barrier deadline, rather than passing with a missing message; its operation has an
  unknown outcome. What the worker claims after the counter is read is an unobserved tail, its operation left in flight for recovery to find done or not. The
  page is cross-origin isolated for this (`?isolated=1`). Serial terminate kills now use a
  shared-memory park at a real request in the selected phase: the request marker is fully posted
  before the parked flag. The page drains that stable prefix before termination, avoiding both
  the counter/post gap and loss of undelivered port traffic during teardown. Buffered IDB
  writes in the selected phase wait for their real scheduled native batch; batching stays enabled.
  No-op occurrences advance under the original deadline and earn no reach. IDB parks in a
  microtask after the transaction body issues its initial requests, before its end callback;
  OPFS parks before its synchronous call. A kill lands before, inside (a platform
  request open), in the window (begun, no request open) or after a step's storage work. A page
  close takes its expectation at a boundary just before the close. Any failure the workload
  reports, at any time, fails the campaign. On IndexedDB kills land inside each phase's storage
  work. OPFS's storage calls are synchronous: termination stops the worker between JavaScript
  steps, so a kill can take effect after a sync call began and before its end was posted (the
  call itself has returned), never halfway through one; kills are required at each step's edges,
  and the simulated
  physical crash sweeps cover the inside. 50 kills per store and browser in the browser lane,
  1,000 per store in Chromium nightly (`npm run test:storage-kills`), with declared reach
  (`STORAGE_KILL_REACH`, `STORAGE_KILL_EDGE_REACH`).
- Concurrent kill campaign: the same workload as lanes that run at once (two writers with their
  builders, a pinned reader, the log, spill, settling reclaims), each operation still announced and
  given a go at once. A seeded delay arms the next request gate, cycling publish/log/spill/data
  with at least two operation windows open on IndexedDB; at the kill the page stops giving gos,
  so everything the worker does before the kill takes effect was announced, and recovery accepts
  the acknowledged root or any announced one. On IndexedDB kills land with a transaction open
  (one at a time: the backend serializes them per connection) of each kind, beside two to five
  operations in their windows; the reach counts open requests, windows are only reported
  (`STORAGE_CONCURRENT_KILL_REACH`). OPFS's synchronous calls never overlap at a kill (at most one
  is open, and never halfway through), so
  `spill/chaos-crash.test.ts` runs the same workload in Node over the simulated OPFS and IndexedDB
  stores, with every session and spill call's result handed back one to three tasks after the call
  ran, and loses power at a seeded physical step; its reach counts the calls that had run and not
  returned at that step (`STORAGE_CONCURRENT_CRASH_REACH`). The
  workload and recovery live in `spill/chaos-workload.ts`. `npm run benchmark:storage` records the
  storage microbench nightly (docs: reference/testing).
  The worker gate uses 100 ms futex sleeps so the VM can process worker termination between wakes.
  Every wake keeps the worker parked; it does not advance the storage call or change the kill point.
  This avoids an indefinite wait retaining WebKit's owner during teardown.
  Storage kill diagnostics accept `MINNOW_STORAGE_CPU_THROTTLE=4` for Chromium CDP
  `Emulation.setCPUThrottlingRate` on the controller page (reapplied after page-close kills).
  Reports record the rate; this is delayed controller scheduling, not a calibrated disk/worker
  slowdown. The 50-kill default, 1,000-kill nightly counts, reach states and minima are unchanged.
- `select.ts` (`chooseBackend`) picks OPFS where a dedicated worker can open sync handles, else
  IndexedDB only on the private modes' refusals (`SecurityError`, `UnknownError`,
  `NotSupportedError`; other errors propagate), else memory (Safari Lockdown Mode). Before
  creating anything it refuses the pre-reset engine's data (`minnowdb/<name>` in OPFS, or an
  IndexedDB database of the old engine's stores under the bare name) with `LegacyDataError`, and
  refuses to open OPFS over this name's IndexedDB store (`StoreConflictError`). Layout
  `minnow-core/` and `minnow-core:<name>`.
- A writer that interleaves many builders (the page format's group writer) passes a `budget` to
  `beginWrite`: each builder stages up to 256 KiB of its own. Without it, alternating page-by-page
  appends cost one platform write per page (measured 2–9× a flush on Chromium and Firefox). To a
  session that keeps its writes until a commit (`keepsWrites`: IndexedDB), a full buffer is handed
  over (`transfer`) instead of copied, and the builder stages on in a fresh one; a session that
  writes at once (OPFS, memory) copies into its file, so the buffer is reused.
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

**Historical experimental writer data loss (B2-p2m).** Rebuild writers before the capacity
clamp could persist wrong values after a page flush when the raw buffer held fewer than 64
rows. The soft 64-row preference exceeded the buffer: scalar copies silently discarded stores,
and an undersized validity bitmap could turn non-NULL input into NULL. Safe intervals and
decimals (safe, int64, or int128 coefficient input) can complete with wrong zero/NULL values;
wide int64 and other flat typed-array copies can instead throw. Dictionary/constant source
forms using scalar copies can also be affected. This concerns the unreleased, unexported
rebuild format, not shipped MinnowDB formats.

For a fixed-width row of `w` bytes, the raw-buffer capacity is
`min(maxRows, floor(rawPageBytes / w))`; the raw-size danger range is `rawPageBytes < 64*w`.
Supported fixed widths are 1, 4, 8 and 16 bytes, so the largest affected raw budgets are
63, 255, 511 and 1023 bytes respectively (for positive capacities). At the reproduced
512-byte budget, the smallest affected supported width is 16 bytes: capacity 32.
A 128-byte encoded-page target initially admits 8 such rows, then the old preference admits
64 into the next 32-row buffer. The encoded-page target alone does not shrink the buffer.
Defaults use 32 KiB raw pages and at most 16-byte rows, so capacity is at least 2048;
default settings cannot reach this defect. A `maxRows < 64` group limit alone also cannot
overrun its buffer, because the entire group stops at that limit.

The clamp prevents new damage. Pages successfully written by those older experimental builds
can contain checksummed, structurally valid wrong values; the current reader cannot detect
or reconstruct the original lost values. Treat affected experimental stores as untrusted
and reconstruct them from an independent source. No automatic upgrade can recover overwritten
values. This format has never been released, so no supported-version migration is owed or
introduced; there is no encoding/version bump. Independent generated type/representation,
flat/dictionary source and all-valid/sparse-null campaigns live in
`page/small-page-representations.test.ts`; the original throwing regression remains intact.

- A column chunk is `[pages][dictionary page][directory]` in one extent; each is one blob of at
  most 1 MiB, and its `BlobRef` carries the extent (R15.1). `ColumnChunk.data` is the chunk's
  range in that extent (offset 0 for its own extent, its place in a pack otherwise); page and
  dictionary offsets are relative to it. The directory lists every page's
  rows, encoding, null count, min/max and checksum, plus the chunk's distinct sketch (a range of
  the directory blob), so pruning and planning never read pages.
- Encodings (`encode.ts` has the selection rules): constant (and all-null), plain, bool bitmap,
  frame of reference at u8/u16/u32 over an int64 base, delta (FOR over successive differences,
  a checkpoint every 128 rows), dictionary codes (u8/u16/u32, or one code) into a
  group-scoped dictionary, and scaled float (float64 exact decimals as FOR integers over 10^e,
  every other value patched with its own 8 bytes, bit-identical; decoded and gathered straight into
  the target, while predicates read an f64 lane the reader materializes on first use and is
  charged for). Every page starts at an 8-byte offset of its extent. Bit-packed FOR
  widths were measured and not adopted: unpacking cost +38% to +659% in the decode kernel and
  +46% to +194% in the pushdown compare (`scripts/bitpack-bench.mts`, 7–20 bits, two runs), far
  past the ~5% a 6% size saving was allowed to cost.
- NUMERIC: a declared scale makes a fixed-scale chunk; an unconstrained NUMERIC keeps each
  value's scale (a per-row section, or one scale per page), and its min/max are normalized sort
  keys (value order, scale ignored, R15.10), chosen by comparing the exact key bytes. Coefficients are stored by the int64 rules when they
  fit, else as 128-bit limbs. Past 128 bits is `UnsupportedPageLayoutError`.
- Large values (`large-value.ts`, R15.4): a text, bytes, json or array value over 64 KiB is stored
  out of line, as consecutive parts of at most 1 MiB (each with its own xxHash32) in the chunk's
  own extent, written straight from the caller's vector through the builder (the writer holds no
  copy: about 0.1 MiB of working memory however large the values). Its page slot holds a pointer
  (offset, length, part checksums) and the page marks the row in a `large` bitmap; such a page
  is plain, and the chunk's dictionary carries on for the other pages. Bounds use the value's
  first 33 bytes and the sketch hashes the whole value. The parts are inside the chunk's data
  range, so the reachability walk reaches them through the chunk and they are reclaimed with it.
  Each page's directory entry records its large-value region (`PageEntry.largeRegion`): it lies
  after the previous page and before this one (pages are in order and never overlap), and the
  page's pointers must tile it exactly, in row order (checked when the page is parsed, one
  comparison per value), so a value can alias neither another value nor a page. A value over
  64 MiB (`MAX_VALUE_BYTES`) is `UnsupportedPageLayoutError`. A group ends before a varlen chunk
  passes `maxChunkBytes` (1 GiB, far below what the directory's u32 offsets address); it always
  takes at least one row.
- Reading large values: `load` reads pages only. `loadValues(from, to)` / `loadValuesOf(rows)`
  load the out-of-line values a decode will reach, charged to the budget before they are read
  (the space's lease), in reads of at most 4 MiB, and kept leased (a decode copies the parts
  straight into the target; no reassembly copy) until `releaseValues`, `release` of their page
  or `close`. Synchronous when every part is resident. A decode that reaches an unloaded one
  throws `ValueNotLoadedError`; `varlenBytes` and `gatherBytes` count full value lengths, so a
  caller can size batches first. Values read together share one lease, charged until the last
  of them is released; a release while a load is in flight wins (the load gives its holds back,
  and a later load reads afresh). A consumer that can stream uses `valueParts(row)`: one
  verified part (≤ 1 MiB) held at a time, whatever the value's size. `close` ends every stream
  and releases what it holds. A stream may yield verified parts before a later part fails: a
  failed stream is wholly invalid, and its consumer must discard what it got; `loadValues` (and
  so every decode) verifies every part first. `scripts/large-value-bench.mts` measures it.
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

### Predicates on encoded pages (`page/predicate.ts`, `page/select.ts`)

The read-side pushdown the executor's scan uses; four calls and one helper:

- `reader.compilePredicate(predicate)`: one column compared with constants (`=`, `<>`, `<`, `<=`,
  `>`, `>=`, `in`, `between`, `isNull`, `isNotNull`), compiled once per chunk into intervals in
  the column's order. Null when the type or operator is not pushed down (interval, json, arrays,
  mixed-scale decimals, enum ordering): the caller evaluates it on decoded values. Constants are
  in stored form (integers as bigint or number, decimals as `{coefficient, scale}` or a special,
  text/bytes/uuid as bytes).
- `chunkMayMatch(compiled, rowCount, chunk.statistics)` prunes a group; `reader.pagesMatching`
  lists the pages a range needs (load only those). Statistics prune only when they prove no row
  qualifies; absent or differently shaped statistics never prune.
- `reader.select(compiled, from, to, out)` writes the qualifying group positions (`Uint16Array`,
  ascending) of loaded pages without materializing values: dictionary pages test codes against a
  bitmap of matching dictionary entries built once per chunk; frame-of-reference pages compare
  stored deltas with bounds moved into delta space once per page; plain int32, float, bool and
  varlen pages compare in place. Delta, plain int64 and decimal pages decode into a reused
  scratch and compare word pairs or 128-bit limbs. Row loops do no BigInt work: bounds become
  numbers once per page, and the dictionary-code bitmap once per chunk. `reader.refine(compiled, rows, …)`
  narrows a selection (in place allowed), for conjunctions across columns.
- Semantics are PostgreSQL's: NULLs never satisfy a comparison; float NaN equals NaN and sorts
  above +Infinity, −0 equals +0; decimals compare exactly (specials −Infinity < finite <
  +Infinity < NaN); text, bytes and uuid in byte order (C collation).
- Selection-aware decode: `gather` decodes only the selected rows of the other columns; at or
  above `DENSE_SELECTION` (40 %, measured) of a span, `prefersDense` says decode the span with
  `decodeRange` and keep the selection instead.
- Every dictionary page's codes are checked against its chunk's dictionary once, when the page
  is validated (`checkDictionaryCodes` in `parsePage`), so select, refine, gather and decode all
  rely on the same check.
- The dense path: `reader.selectMask(compiled, from, to, mask)` writes a 0/1 `Uint8Array` (null
  rows 0) without building positions, for a fused, branch-free filter and aggregate over dense
  decodes (`decodeRange`) of the aggregated columns (`sum += value & -mask[row]`). Bytes beat
  Uint32 bit words for such a consumer (measured: both ≈ 3 ns/row to write; int sums 2.8 against
  5.4 ns/row, f64 sums 3.2 against 4.2).
- Memory: no per-row allocation. The code bitmaps and the decode scratch are charged to the
  reader's budget and returned at `close`.
- `scripts/pushdown-bench.mts` measures it on the W1M table's columns against decode-then-filter.
- Dictionary translation: `reader.translationMap(intern)` maps the chunk's dictionary codes into
  the query's id space (`map[code] = intern(entry bytes)`), so text GROUP BY and join keys agree
  across groups without strings. Built once per reader and interner at O(dictionary), charged
  (4 bytes per entry) until `close`; null for a chunk with no dictionary. A dictionary never has
  more entries than the rows it codes: the writer keeps a page dictionary-encoded only while its
  codes and new entries cost no more than the plain page, so a group of mostly distinct values is
  stored plain, and a map never costs more than the rows it replaces.
- The page-stream half of a full export (the page benchmark, not the real table scan) reads with
  `ChunkReadOptions.once` (storage `ReadOptions.once`, `PeekOptions.once`): nothing it misses is
  admitted to the page cache, nothing it hits is promoted or reordered (synchronous peeks and
  pins included: an entry only one-pass leases pin keeps its place and last use), a
  directory it parses or a gzip page it inflates is not admitted to its cache (reusing a cached
  one leaves its eviction order alone: `BlobCache` one-pass pins), and it attaches no
  descriptor (the space also refuses one whose bytes are not the resident entry's own). So the
  hot working set stays and memory stays flat: the bench's hot set, protected and outside the
  export, survives whole when everything but its size is evicted afterwards (a promoting scan
  loses all of it). `reader.pagesAhead(from, to, bytes)` names a batch's pages plus the next ones
  within a byte budget, so one load call reads ahead and the space coalesces neighbours (6× fewer
  platform reads at 256 KiB per column). Pages behind the stream are released as it moves on.
  `scripts/export-bench.mts` over files on disk (`export-bench-build.mts`, the real file store,
  page cache and governor), 200 columns: decoding from the page cache runs at about 1M rows/s at
  W1M; through the file reads 94–131k rows/s on table copies not read since they were written
  (not verified cold in the OS cache) and 230–420k on a copy just read, on this host (load
  12–26), readahead giving 1.4–1.9×; W5M (5,242,880 × 200, five byte copies of W1M) completes at
  168k rows/s with peak RSS 126 MiB and heap 29 MiB above the baseline. Without `once` the same
  W1M export keeps 404 MiB in the page cache and peaks at +503 MiB RSS.
- The same export on OPFS in Chromium (`browser-tests/next-export-bench.spec.ts`, opt-in
  `MINNOW_NEXT_EXPORT_BENCH=1`, which also turns the cached-bytes guard off for its run): the
  table is written through the OPFS backend in a worker (W1M × 200 plus one group the export
  does not read, 1,138 MiB; 4 groups generated, the rest byte copies), and every pass streams
  1,048,576 rows in a fresh worker. Cold means the profile's OPFS files were synced and dropped
  from the OS page cache (`dd iflag=nocache`, checked with `fincore`). Two runs on this host
  (load 13–18): cold 103–127k rows/s without readahead, 116–207k at 256 KiB per column,
  129–144k at 1 MiB; on later passes whose OS-cache residency was not controlled (76–95% of
  the files resident; the bench now primes and verifies a warm pass, review B2-p2h #3)
  123–141k, 162–225k, 121–224k; from the page cache (decode only) 0.6–1.2M. Readahead cuts platform reads from 49,013 to 8,094 (256 KiB) and 6,819
  (1 MiB); 256 KiB is the default. It is not disk-bound: dd reads the same file at 1,119 MB/s
  cold, the export 110–241 MB/s, and loads take 68–79% of a pass warm or cold. A warm pass's
  CPU profile: `PageCache` miss handling 29% self time, the sync access handle's `read` 9%,
  xxHash32 11%, decode about 20%. Memory with `once`: renderer RSS +96 to +157 MiB over the
  value before the pass, the worker's JS heap at most 28 MiB, governor peak 53–78 MiB; without
  `once`, +621 to +656 MiB and the hot set is lost.
- The export's read path (increment 9). Measured first: a warm W1M pass spent 0.39 s in the
  platform reads, 0.64 s copying 57,452 blobs (1.1 GB) out of the shared slab with `slice`,
  0.65 s in xxHash32 and 0.12 s in entries and planning; a cold one 6.25 s in the reads alone
  (176 MB/s from a device dd reads at 1.1 GB/s), because each column read its own 256 KiB
  window, so a batch made 200 strided synchronous reads. Now:
  - the writer starts every page, dictionary page and directory at an 8-byte offset of its
    extent (up to 7 zero bytes before it; directories allow the gaps), so a page read together
    with its neighbours is viewed in place;
  - a one-pass read's misses (storage `#readPrivate`) are read into one buffer per slab that its
    lease alone holds (slabs up to `MAX_READ_BYTES`), every blob a verified, aligned view of it:
    no copy, no `Entry`, no shared slab; the lease is charged for the blobs and the gaps between
    them and gives both back on release;
  - `ChunkReader.loadTogether` loads several readers' pages through shared reads in address
    order, and the export stream's row window (`windowBytes`) loads every column's next rows at
    once, so a group is read as a few large reads per column instead of one per batch.
    In Chromium on OPFS (1M × 200, interleaved runs on a host giving the worker about half a
    core): the miss path fell from 29% to 5% of a warm pass, the sync access handle's `read` is
    now the largest item (28%); per CPU-second of the export worker, warm 330–376k rows/s with a
    64 MiB window (per-column 328k), cold 200–320k; wall rows/s on this host 105–297k cold and
    119–344k warm, renderer RSS +125 to +172 MiB at 64 MiB (one earlier run reached +268 MiB,
    not profiled; released one-pass buffers waiting for the collector are the likely cause).
  - One-pass page buffers are pooled (`ReadOptions.reuseBuffers`, increment 10): a one-pass read
    of pages packs its slabs into one pooled buffer (power-of-two sizes from 64 KiB to 16 MiB),
    charged whole to its lease (overlapping references refund their excess initial charge); on
    release the buffer returns to the pool, charged to the cache
    while idle (at most 64 MiB, evicted first, dropped when the governor refuses) and poisoned in
    test builds. Readers ask for it for page loads only, never a dictionary or a value they hand
    out past its lease: an inline `valueParts` stream retains the page's shared read lease until
    its next step, return or reader close (the whole buffer stays charged). Peeked/inflated pages
    without a lease use a charged value-sized copy. At 1M × 200 on OPFS, renderer RSS +104 to +172 MiB over 22 passes in two runs with the
    pool, +75 to +251 MiB over 11 passes without it (on a 262,144-row table, where the collector
    keeps up, the pool's idle 64 MiB makes it +76 to +124 against +45 to +98).
  - The nightly browser-performance workflow runs the export bench (`npm run benchmark:export`,
    1M × 200, five cold and five warm rounds per configuration) and records rows/s, the worker's CPU seconds and rows per CPU-second, RSS, worker
    heap and pool residency per pass in `export-bench.json`; nothing judges it yet. Locally, the
    export is compared per CPU-second too; host scheduling and memory reclaim can dominate wall time.
- Reproduce increment 10 comparisons with `scripts/gather-bench.mts` (finite float64 gathers,
  all-word / Number-only / production NaN-only copies) and `scripts/export-compare.mts --dir
~/.cache/minnow-export` (the whole file-backed page export against p2i on identical stored data).
  Both use at least five interleaved rounds in fresh processes and print CPU and wall medians,
  min/max spread and every sample; the export also records sampled RSS. The file comparison uses
  one untimed priming pass per variant, keeps the OS cache, and does not establish the cold OPFS Gate A target. Number-only is a finite-input
  comparator: NaNs still require raw copies to preserve their bits.
- Export phase profiling (`MINNOW_EXPORT_PROFILE_ONLY=1` with `MINNOW_NEXT_EXPORT_BENCH=1`
  on `next-export-bench.spec.ts`) runs a cold and a warm Chromium/OPFS profile, then unprofiled
  references. `MINNOW_EXPORT_ROWS` selects the rows (default 1,048,576), with 200 columns.
  Root open, worker imports, dataset construction and the hot-set audit are outside the timed
  export; this mode skips hot-set prefill entirely. Cold file residency is recorded; warm still
  requires at least 98% residency at its start, not a promise it stays resident under pressure.
  - The wall phases are synchronous `readDataInto` (native read/wait and validation), target
    decode, window/buffer planning and release, and residual directory/page load work (checksum,
    parsing, allocation, GC and coordination). Raw CDP CPU profiles are attached to the HTML
    report; thread CPU at CDP profile boundaries excludes setup, with clock-tick/notification
    latency. The wall counters are cross-checked against platform reads and batch decode calls.
  - Decompression is zero for these uncompressed flushed pages (the profiler refuses gzip
    pages rather than silently misattribute them). Gather and row transfer are not exercised:
    this bench range-decodes reusable column buffers and sends only its summary back. It is the
    page-streaming half, not a SQL/result-transport Gate A certification.
  - `MINNOW_EXPORT_COMPARE_STREAM=1` runs five interleaved cold/warm attempts per loop version,
    without CPU profiling. `MINNOW_EXPORT_BASELINE_COMMIT` (default `18548e34`) freezes only the
    old export loop; both versions use the current engine and both loop modules are imported
    before timing. This checks disabled-probe overhead, not an arbitrary engine change.
  - `MINNOW_EXPORT_VARIANCE=1` alternates nine cold/warm pairs (reversing order each round).
    Each pass records read sizes, backing-buffer sizes and timings, pending promises versus
    synchronous read concurrency, cache/pool/governor samples, file residency, host load and
    Linux CPU/IO/memory pressure and reclaim counters. Chromium timeline traces attach the
    exporting worker's GC pauses between user-timing marks; GC overlaps the wall phases and
    must not be added to them. A benchmark-only compiler wrapper times xxHash separately (it
    lives in the residual load phase). No checksum timer is shipped in the engine.
  - The native read timer excludes xxHash. One-pass page loads already read into leased pooled
    buffers and verify their views in place; OS cache residency does not switch on a blob copy.
    OPFS sync reads block this worker, so outstanding promises do not mean concurrent IO or
    decode/read overlap. The row window is also capped at one group, even when configured larger.
  - `MINNOW_EXPORT_READ_COMPARE=1` tests a benchmark-only 8 MiB read-call candidate against
    the current 4 MiB bound, five interleaved cold/warm rounds. Both use the existing 16 MiB
    maximum pool class and 64 MiB idle cap, with normal lease charging. The isolated
    `scripts/read-batch-bench.mts` runs full read/verify/release over contiguous and gapped
    ranges in memory, with CPU/wall samples and charge checks. The candidate does not change
    the engine's bound: fewer calls alone are not evidence of a faster export.
  - `MINNOW_EXPORT_DEPTH_COMPARE=1` compares retaining one versus two whole groups, five
    interleaved cold/warm attempts each. Two groups are opened and prefetched before decoding
    the first; their existing shared leases pay for both groups until release. It does not
    create IO overlap on a sync handle. The default stays one group. The isolated
    `scripts/readahead-bench.mts` runs the actual page-export loop and PageCache over a wide
    fixture outside IO, checks row/charge totals, and reports five CPU/wall rounds. Its memory
    peak is only the cache handle; the OPFS comparison reports the full governor and RSS.
- Scan/transport mode: `MINNOW_EXPORT_SCAN_COMPARE=1 npm run benchmark:export` runs five
  interleaved cold/warm attempts of the page stream and B4's production Executor/result path.
  `MINNOW_EXPORT_SCAN_COLD_ONLY=1` keeps five cold attempts of each mode; the nightly workflow
  records these separately as `export-scan-bench.json`, without a threshold. The governor cap
  is 512 MiB for both modes and is reported; its counter is not an RSS bound.
  - `scripts/browser-perf/page/executor-export.ts` prepares a real scan plan with `createExecutor`
    before timing. `execute` owns admission, TableScan, ResultSink and ResultChannel; frames use
    the unchanged owner transfer policy, real MessagePort delivery and cumulative client credits.
    Ordinary `mode:"scan"` is now Gate A's export number. Execution defaults are used, including
    the compiler's terminal full-scan window policy. The bench does not override scan windows,
    output ownership, batch targets or result framing. Fixture preparation/SQL/catalog/root reads
    and client row-object construction remain excluded. Generated `executor-export.test.ts`
    histories check actual factory preparation, all transferred values, multiple groups, sparse
    NULLs, genuine >64 KiB values, delayed credits, storage survival and returned execution charges.
    The production sink keeps eligible UTF-8 dictionaries as copied codes and per-stream owned
    deltas, applied by `StreamDictionaries` before client access. Dictionary bytes and registry
    entries share the existing 1 MiB stream cap; subsequent dictionaries fall back to flat varlen.
    Registry entries use ordinary result-grant capacity, so a delta frame can return its progress
    loan before the stream ends. Dictionary pages and borrowed codes never enter a transfer list.
    A direct terminal full scan decodes tight dedicated output into the result grant.
    Only explicit recycling exports double its owned batch target, bounded by
    the existing result credit window (2 MiB at the default cap/window). This amortizes wide-column
    allocation without changing the 2,048-row limit. Ordinary results, other producers and
    consumer pipelines keep their existing target;
    copying fallbacks retain conservative frame row capacities. The sink
    adopts only buffers explicitly named by that source's detach permission and charged to its
    own grant. Before returning the source token (and its query dictionary references), it
    reclassifies the dedicated buffers into a frame charge in the same result grant. That charge
    survives until frame credit; a refused optional reclassification keeps the copying path. Capped dictionary columns and other
    ineligible columns are copied into the same frame. The owning sink admits dictionaries
    before source sizing: accepted columns decode to dedicated codes, refused ones directly to
    dedicated UTF-8. Each group's decision stays fixed across sizing, reservation and decode,
    so the batch target reflects its wire form. A reserved dictionary id is published only with
    its first owned delta; cancel/end returns unused registry charges.
    If the complete batch exceeds the frame
    byte or row limit, the ordinary copying splitter remains in use. Identity/computed projects
    retain that safe copying path. A terminal scan may fall back to ordinary borrowed decode
    when the result budget refuses even one dedicated row, preserving progress loans.
    `testing/result-owned.test.ts` checks source/client equality, mixed ownership, foreign grants,
    selections, row limits and cancellation; `testing/result-owned-bytes.test.ts` checks exact
    transferred buffer identity and bytes across all native layouts, including NaN payload words; the real executor bridge observes cached and pooled
    PageCache buffers through actual transfers, with poison verification off.
    Generated `testing/result-dictionaries.test.ts` checks exact client values, NULLs, selections,
    identity reuse/change, large deltas, cumulative cap fallback, pressure and cancellation.
  - The receiver opens `browser/next-export.html`, which loads no unrelated conformance
    runner or released legacy engine. `MINNOW_EXPORT_GENERIC_PAGE=1` reproduces the historical
    page bootstrap; reports identify the bootstrap. This changes harness residency, not the
    production executor, receiver, transfers or credit policy. Compare absolute RSS across
    those scopes only with the bootstrap stated.
  - `MINNOW_EXPORT_PAGE_HEAP=/home/eric/.cache/export.heapsnapshot` adds a page-side CDP
    heap snapshot after all frame delivery. It records pre/post snapshot heap, backing storage
    and renderer RSS. Snapshotting performs GC: use a separate diagnostic run, not its timings
    or post-GC snapshot as an export peak measurement. CDP `backingStorageSize` includes
    external strings as well as ArrayBuffers. Captures label it `backingStoragePeakMiB`; the
    historical `arrayBufferPeakMiB` key remains an alias, not a pure ArrayBuffer measurement.
  - `MINNOW_EXPORT_RSS_SERIES=1 npm run benchmark:export` keeps one worker, OPFS space,
    governor and page pool across five cold exports. RSS growth uses one fixed campaign baseline;
    each pass records page/worker heap and backing stores plus end-of-pass pool/cache charges.
    Without the diagnostic GC flag, collection runs only after the campaign, followed by space close and zero-charge checks. With
    `MINNOW_EXPORT_EXECUTOR_REFERENCE=REV`, five interleaved reference/current pairs share that
    actor; both source families load before the baseline. This paired scope includes both families.
    `MINNOW_EXPORT_DISCARD=1` is an explicit discard-consumer diagnostic: detach consumed column
    buffers, retaining dictionary deltas until stream close. It changes no public Result lifetime
    and does not guarantee lower RSS: backing-store disposal can still wait for GC/allocator work.
  - `MINNOW_EXPORT_RECYCLE=1 MINNOW_EXPORT_RSS_SERIES=1` is the dedicated executor/transport component bench for the adopted explicit return mode.
    The production owner client exposes it through `exportStream()`, separately from `query()`. It keeps the existing per-buffer frame contract. Its export
    receiver exposes `recyclingFrame(...).release()`: only explicit relinquishment transfers
    the dedicated frame buffers back. Receipt and cursor credits keep their existing timing and
    do not end ordinary result/view lifetimes. After release, typed views are detached (length
    zero, or `TypeError` on operations that require a live buffer); they must never be used again.
    JavaScript value arrays contain copied values and are not reusable native storage.
    The receiver copies dictionary deltas into compact stream storage before releasing a frame,
    so later dictionary codes never refer to detached buffers. Callers retaining views after credit
    hold client memory outside the worker result grant, as before; the pool does not bound arbitrary
    consumer retention. The benchmark discards each frame
    after counting it and explicitly releases it; normal buffered results and cursors do not.
    Worker returns use one-use slot capabilities tied to the originally sent dedicated buffer,
    its size and the detached sender wrapper. Unknown, duplicate, resized, detached and expired
    returns are refused. A cooperative receiver can only return the buffers captured by its
    handle; this is an internal protocol, not a physical-identity proof against a compromised
    receiver sending a same-sized replacement with a stolen capability. Pages, borrowed dictionaries,
    slabs and cached storage never receive capabilities. Only exact-size buffers are reused,
    cleared to fresh-allocation semantics, after the output grant reserves their charge.
    The return cache is capped at twice the 2 MiB credit window, including conservative entry
    metadata charges; capabilities are also limited to 4,096 slots. It evicts stale size classes
    and deletes empty ones. Refused/lost/late returns never delay execution or cancellation.
    Closing the stream's pool returns its charges; frames still held by a caller stay valid until
    that caller explicitly releases them. `scripts/output-return-bench.mts` records five
    interleaved CPU/wall component rounds. Five persistent-process Chromium A/B pairs are still
    required for throughput and fixed-baseline RSS. Five standalone recycling passes met the RSS
    bound; paired passes can inherit ordinary-result garbage from the previous variant. The return
    component increases CPU with disjoint ranges, and whole-export throughput does not improve
    beyond the spread. The lead accepted its memory/CPU tradeoff for explicit export; ordinary
    queries keep their batch target. Absolute Gate A throughput will be measured on a quiet host.
    `MINNOW_EXPORT_GC_EACH=1` adds post-pass forced-GC diagnostics, outside timed exports and
    outside the sampled peak. It is diagnostic evidence, not an RSS acceptance run.
  - `scripts/vector-materialize-bench.mts --compare=REV` isolates loaded-page vector building,
    including output allocation, sizing and decode, in five interleaved CPU/wall rounds.
    `--batch` compares 512/1024-row batches over equal rows; `--offsets --reused` isolates offset
    decoding without output allocation. Writing/loading, frame assembly and transport are excluded.
  - `scripts/result-sink-bench.mts --compare=REV --owned` isolates framing with fresh
    authorized source buffers and mixed numeric/dictionary columns. It charges source buffers
    before pushing and returns their charges on release. Input construction is excluded; decode
    and OPFS are measured by the whole export instead.
  - `scripts/executor-export-bench.mts` compares the real executor/result path with the historical
    adapter over the same memory-backed PageCache. `--compare=REV` instead freezes the whole
    executor dependency family at a Git revision and compares it with the current compiler.
    Physical groups have distinct addresses in both variants. Five interleaved fresh
    processes per variant exclude preparation, setup, warmup and GC and record process/thread
    CPU and wall time. This isolates worker export cost, not OPFS/renderer memory. Both use real
    in-process transfers; the adapter is not an alternative Gate A implementation.
  - `MINNOW_EXPORT_EXECUTOR_REFERENCE=REV npm run benchmark:export` runs five interleaved
    cold baseline/current production-executor pairs. The revision must exist locally; every core
    dependency is frozen, with no fallback to current sources. Both code families load before
    timing. Use standalone current runs for absolute RSS acceptance: the paired comparison
    also retains the reference code family.
  - The packed-frame copy-compaction experiment is **rejected**, with the production frame
    contract unchanged. `scripts/browser-perf/experiments/packed-frame-copy.patch` and its
    hashed base archive preserve the candidate without production imports. `MINNOW_EXPORT_PACKED_EXPERIMENT=1`
    requires the explicit export-bench opt-in; combine it with `MINNOW_EXPORT_RSS_SERIES=1`
    and `MINNOW_EXPORT_EXECUTOR_REFERENCE=095f5f1c` for five cold A/B rounds in one worker.
    `scripts/executor-export-bench.mts --compare=095f5f1c --packed` replays its CPU component.
    Every packed frame has one fresh buffer and aligned, bounded, disjoint views; both endpoints
    validate them, and retained UTF-8 dictionary deltas are copied into compact client storage.
    Generated diagnostics compare native bytes across layouts (including wide decimals, NaN words,
    NULLs, constants, dictionaries and >64 KiB values), reject malformed subviews, and check
    completion/abort/cancel charges. This is not an accepted frame format or a supported stored format.
    It adds a compaction copy after per-column decode: measured throughput regresses and RSS
    ranges overlap beyond the budget. Its doubled temporary-buffer reservation also refuses a
    previously supported 550 KiB row under a 1 MiB progress reserve. That failure is preserved
    as rejection evidence; it does not weaken the ordinary large-row properties. These results
    concern this copy-compaction implementation, not a proof that direct decode into packed output
    cannot work. The benchmark excludes SQL/catalog/root loading and client row-object assembly.
  - `owner-frames.ts` and `scan-export.ts` remain **historical scan-adapter diagnostics**, excluded
    from Gate A. Ownership/batch/IO/range/shape-transport/frame-copy experiment flags explicitly
    use `mode:"scan-adapter"`. No production module imports the old assembler.
  - The real `ResultChannel`, `frameByteLength` and owner transfer policy are reused. Frames
    go over a real `MessagePort`; the client checks sequence/dictionary/row/byte totals and
    returns cumulative credits at half the window or the owner's release delay. The timed
    export ends after all credits return. The client consumes typed frames, not JS row objects.
  - Table/group descriptors come from the fixture, replacing SQL lowering and B3 catalog/root
    loading. The historical scan-adapter/scan-core diagnostics request a 64 MiB row window
    through `TableScan.readWindowBytes` and
    the `chunkReads` factory: B2's shared `loadTogether` and one-pass pooled leases. This
    window is also the production default for a terminal, unfiltered full-table scan (including
    identity projections), with no runtime filter or locator consumer. Other plans retain cached
    reads; direct TableScan callers still opt in with `readWindowBytes`. Filtered/deleted groups
    retain selective reads. Resident shared loads preserve synchronous `next()` steps until a task yield is due. A refused
    optional window drops partial loads and shrinks before
    falling back to batch reads. Window planning, cached installation, sorting and read/page
    continuations use the same R23.5 task clock as the scan/driver through B1's `task.ts`
    extraction. Already-resolved storage promises do not restart it. Missing pages are claimed
    before a yield so concurrent loads share their completion; started siblings still drain on
    errors. Each scan owns a cancel signal, passed to its readers and storage. Abort skips
    closed readers' unstarted page jobs; already-started reads drain, and other live readers
    keep their jobs. Different cancel signals are never grouped into one shared read. Started
    rejections are observed immediately while preparation yields, then propagated after drain.
    `testing/scan-windows.test.ts` generates timers/cancel messages in multi-read
    windows, carried task budget and concurrent followers. `MINNOW_EXPORT_SCAN_IO_COMPARE=1` compares both policies over
    five interleaved cold rounds; `scripts/scan-io-bench.mts` isolates the real scan/cache over
    memory, with fresh caches, untimed warmup GC, and CPU/wall/read/charge measurements. Reported
    scan assembly/post/credit and inclusive `next()` timers can overlap; they are not exclusive
    phase shares. Raw scan statistics reveal dense/gather batches, splits and storage calls.
  - `MINNOW_EXPORT_SCAN_BATCH_COMPARE=1` measures 1/2/3 MiB decoded targets over five cold
    interleaved rounds, holding the row limit (2,048), borrowed outputs, read window and owner
    credits (4 MiB) fixed. This experiment also changes frame size because the labelled adapter
    owns one compact batch per frame. Actual frame/transfer counts, maximum frame bytes and
    outstanding credits are recorded. `scripts/scan-batch-bench.mts` isolates the same targets
    over memory with real query/result grants and in-process transfers (no MessagePort IPC).
    The core target stays 1 MiB: neither the isolated nor whole CPU medians beat their full
    spreads in the recorded investigation. A timed-out foreground campaign can resume an
    exact completed-case prefix; combine both logs and retain every completed pass.
  - Sparse CDP profiles (one-second sample interval) bracket setup-excluded worker CPU, not
    hotspots. Profile-count checks refuse missing brackets. CPU tick/notification latency
    remains. Both modes verify zero governor/cache/pool charge after worker close. A final
    client GC/RSS probe is after the campaign and never changes a recorded peak.
  - `MINNOW_EXPORT_SCAN_PROFILE=1` profiles only the scan mode, cold and warm, requesting
    500 µs samples. `scripts/scan-profile.mts` summarizes its `export-cpu-N` attachments:
    exclusive buckets and top self-time functions. CDP durations include descheduling and
    native read waits; active sample-count shares are hotspot estimates, not exact CPU seconds.
    Native reads, GC and idle/program samples remain separate. The worker's `/proc` CPU bracket
    still reports the actual thread CPU, excluding setup.
  - Varlen frame offsets use a typed ownership copy, with a subtraction pass only for biased
    offsets. `scripts/frame-copy-bench.mts` compares the full assembler against the parent generic
    mapping formula, five interleaved CPU/wall rounds over real decoded wide-column layouts.
    It compares complete frame digests and charges, without requiring a historical Git object.
    `MINNOW_EXPORT_FRAME_COPY_COMPARE=1` compares both formulas over real cold scan/transport,
    five interleaved rounds. Both own all borrowed buffers and use the same transfer policy.
  - Compact scan batches whose selected positions are contiguous use range APIs for sizing,
    value loading and decoding. Every position is checked; selections with gaps keep the gather
    path. Batch sizes, byte limits, borrowing, pushdown and output values are unchanged.
    `gatherBatches` still counts the compact-selection branch, including these contiguous spans.
    `scripts/scan-range-bench.mts` isolates this dispatch in five interleaved CPU/wall rounds over
    the real scan/cache/targets in memory. `MINNOW_EXPORT_SCAN_RANGE_COMPARE=1` compares the
    parent gather policy and range dispatch through whole cold scan/transport, keeping the
    adapter and owner transfer policy fixed. Neither comparison needs a historical Git object.
- The checksum-load CPU hypothesis from the export profile is reproducible with
  `npx vite-node scripts/hash-load-bench.mts`: five rotating rounds in fresh processes, CPU and
  wall medians/min/max, aligned and unaligned inputs, byte offsets, stripe tails, and consumed
  hash results checked across variants. The aligned unsigned/signed native-word candidates stay
  in this benchmark only: neither cleared the median-versus-spread rule. The engine keeps its
  explicit little-endian `DataView` reads. Cold IO/directory coordination and fixed-width decode
  remain the next profile-led candidates; window assembly is a smaller share of this workload.
- `scripts/scaled-width-bench.mts` screens separate dense scaled-float loops for u8/u16/u32
  deltas against the existing shared loop. It calls the full decode function, interleaves five
  fresh-process rounds, reports CPU and wall spreads, and compares every output byte (including
  raw NaN patches and signed zero). The prototype keeps division, never reciprocal multiplication.
  Its gain did not clear the spread consistently on patched pages, so it remains benchmark-only.
- What an export reads (`scripts/export-size.mts`, 262,144 rows of W1M): 1,055 B/row as flushed.
  High-cardinality text is 34% (`h02`'s free text alone 113 B/row), float64 31%, nullable
  floats 9%, int32 9%, decimals 8%; dictionary text 3%. Compaction gzip stores 869 B/row (−18%)
  but slows the decode stream from 224k to 65k rows/s from memory (inflate), so it does not pay
  for an export. Candidates, measured on the same values and page boundaries: float64 pages
  whose values are exact decimals stored as frame-of-reference integers, estimated −103 B/row
  (−10%), now the `scaledFloat` encoding: W1M stores 926.5 B/row (−12%), the OPFS table 998 MiB
  instead of 1,137; frame of reference bit-packed instead of u8/u16/u32 widths, −63 B/row
  (−6%), not adopted (its decode cost, under Pages above).
- First batch (`SELECT *` first page, and `ORDER BY` the primary key, the same rows): a scan opens
  each projected column's chunk (its directory only), loads just the pages the batch covers (with
  the dictionary for dictionary pages) and decodes those rows; nothing is done for the rest of the
  group. `scripts/first-batch-bench.mts`, 2,048 rows, warm with descriptors (load 15–27): W1M
  (200 columns) 4.2 ms, W500 (500 columns) 11.9–13.9 ms; cold (no byte or directory cache) 21–24
  ms and 62–79 ms, about half opening (directory read, checksum and parse), most of the rest page
  reads and their checksums. `varlenBytes` counts nothing for non-varlen pages and sums
  dictionary entry lengths in one loop per code width.
- Parsed pages ride on their space-cache entries (`ExtentReader.parsed` / `attach`): a reader
  attaches the page's descriptor (its `PageView`: views into the entry's own bytes, the validated
  structure) to the entry, charged in that entry at `PAGE_DESCRIPTOR_BYTES` (768; measured
  337–548 bytes, a fixed set of fields and at most eight views whatever the page's size), and
  dropped with it. Later readers reuse it when it was validated against the same directory fields
  (encoding, rows, nulls, large-value region, tag, scale, dictionary size), so a page is validated
  once per cache residency, not once per load. Views share the cached bytes, so nothing is
  counted twice. A chunk's dictionary rides on its dictionary page's entry the same way (the
  parsed dictionary, 306–413 bytes measured, charged at the same bound; each reader builds its own
  vector over it): 83 dictionaries of the W500 table load in 1.1–1.3 ms instead of 1.4–1.7. gzip
  pages are parsed per load: parsing inflated pages measured 1–11 % of a warm scan of a compacted
  text column (inflating, the larger cost, already happens once per residency, R15.5), so they
  get no descriptor. W1M, 0.1 % filter returning 30 columns, warm: 82.6 ms without descriptors, 52.9 ms with
  (load ≈ 16); W5M: 235 ms warm.

### Manifest (`format/manifest.ts`)

- The root is O(tables + ledger). Nothing is O(rows).
- Every reference names the extent it lives in (`BlobRef = {extent, offset, length, checksum}`,
  the storage track's shape), so reachability walks list extents without inferring them. Small
  blobs share 4 KiB-aligned extents; an extent is reclaimed as a unit. Page offsets are relative
  to their chunk's data blob.
- Every reference carries the checksum of what it names (a chunk's pages carry theirs).
- Space is reusable only when neither the current durable root, nor the previous one (the other
  root slot), nor any pinned root reaches it (two generations), and startup reads both roots'
  extent lists (ruling R25: `ManifestRoot.extentList`, format 2; format 1 roots, written only by
  development builds, are refused).
- Startup order: acquire the fence → open the store session → open the extent space (read the
  current and previous roots' extent lists) → open the write path (a single log scan and replay, `publishedRows`
  staging included) → spill sweep → ready.
- Snapshot pins live in memory and are dropped when their client's lock is released.
- `walkReachable(root, reader)` streams every reference a root reaches (catalog, table
  statistics, group directory indexes and pages, staged transactions' directories and index runs,
  group roots, deletion bitmaps, row-id columns, chunk data, page directories, sketches, index
  entries, fences and Bloom filters) and the root's extent list pages; `reachableExtents()` is
  that walk as the extent space's `ReachableExtents` over the real codecs, the audit's
  cross-check. `listedRootExtents()` is what the space opens with: the root's extent list only,
  no group root. A failed metadata read rejects either.
- The write path keeps each root's extent list (`write/engine.ts`, an `ExtentListKeeper` built
  from the durable root at open): it prepares the list once a root's contents are fixed, encodes
  the root with it, and commits or aborts it with the publication. An injected `extentDeltas`
  sink is told the whole delta, list pages included. `ExtentDeltaResult.list` carries the
  prepared references; the engine owns the keeper, so observer sinks must return empty `list`
  and `pages` arrays.

### Manifest codecs (`format/manifest-codec.ts`, `blob-cache.ts`, `metadata-reader.ts`)

- Binary codecs (manifest version 2; envelope version 1) for the root (≤ 1 MiB), group roots, the paged group directory and each table's statistics blob (`TableStatistics`: per-column null counts, values, bytes and bounds over every group)
  (each blob ≤ 1 MiB). Each blob lists its extents once and gives every reference into one extent
  the same `ExtentRef`. Encoding is canonical (re-encoding a decoded value gives the same bytes).
- `TableRoot.groups` names a directory **index**; each index entry names a **page** of up to
  `GROUPS_PER_PAGE` (4,096) `GroupRef`s with its row-id range and counts. `writeGroupDirectory`
  keeps unchanged pages, so a flush rewrites only the last page and the index. A table with no
  groups has a zero-length `groups` reference. The index has one 30-byte entry per page, so the
  per-flush rewrite grows with the page count: up to about 113 KiB for the last page plus ~30 bytes per
  4,096 groups (a two-level index can replace it if that ever matters).
- Decoding is canonical: an extent table or type table with a duplicate, unused or out-of-order
  entry, a non-minimal integer, or ledger counts out of key order is refused, and a blob over its
  cap is refused before anything is charged. Strings keep a leading byte-order mark, and ledger
  counts decode into a null-prototype object (a `__proto__` key is an ordinary key).
- The walk checks each directory against what references it: index entries against their pages,
  pages against the table's `groupCount` and `liveRowCount` (staged directories: their
  `rowCount` and `firstRowId`), every row id against `nextRowId`, and each group against its group
  root's row count. A contradiction rejects the walk with `ManifestCorruptionError`.
- A cached chunk directory is checked against each chunk that opens it (its pages and dictionary
  must lie inside that chunk's data).
- Decoders never hash (the space or the root slot verified the bytes); they check structure and
  throw `ManifestCorruptionError`. Each charges `parseCharge(bytes)` to the caller's budget
  before it builds anything (`ParseBudgetError` on refusal) and returns the charge with the value.
- `BlobCache<T>` keeps decoded values across readers: pinned entries are charged to their holder,
  unpinned ones to the governor, which evicts them first. It backs the chunk-directory cache
  (`ChunkReadOptions.directories`, R15.7), the inflated-page cache (R15.5) and the
  `MetadataReader`'s group roots, pages and indexes, which are synchronous on a cache hit.

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
- **Index runs are tiered.** A level of 8 sorted runs merges into one at the next level, so a
  point read probes at most 7 runs per level once merges catch up (7·L for L = 1 + ⌈log₈(N / M)⌉
  levels over N entries and M per flush), and at most 23 + 7·(L − 1) while they lag (writers wait
  at 24 level-0 runs). Each entry is written L times.
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
  the file's last one, checksummed reads, `sweep` at startup. It owns an optional memory
  registration, charged over its lifetime with what the scratch space keeps per extent
  (IndexedDB's record index; `chargeTo`), returned at `sweep` and `close` (`dispose`). Its `close`
  is terminal: it refuses new files, lets every file finish what already started and release its
  space, and returns the charge last.

### Memory (`memory/governor.ts`)

- One cap, which `usedBytes` never exceeds.
- Pipelines are admitted with plan-bounded minimums. Admission waits; it is never refused.
- A grant _holds_ bytes, and its operator _allocates_ buffers out of them, so a non-spillable
  operator works inside its admitted minimum.
- Growth is charged before it happens, including the overlap while a buffer is copied.
- Caches are evicted before anything spills. Several may register (the space's page cache, the
  decoded directory and metadata caches, the inflated-page cache); eviction takes the entry unused
  longest across all of them (`oldestUnusedAt`).
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
- A progress loan larger than the reserve raises `ValueTooLargeError`. Non-spillable parameter inputs must fit normal capacity; an over-cap fixed/input admission raises `MemoryAdmissionError`.

### Pipeline (`exec/pipeline.ts`)

- Kernels are synchronous.
- `push` returns `ready` or `blocked`, and a blocked batch stays borrowed until `ready()`.
  Operators resume partly consumed batches themselves.
- `next()` and `finish()` return a value when nothing waits and a Promise only when they must
  wait; the driver awaits only real Promises, and `drive` returns no Promise when the whole
  pipeline finished synchronously.
- After `YIELD_INTERVAL_MS` (4 ms) of synchronous work the driver yields a task (`setImmediate`
  or a `MessageChannel` message, never a resolved Promise) at the next batch boundary. In a
  browser or worker, a yield is a `setTimeout(…, 0)` timer turn once `TIMER_TURN_MS` (50 ms) has
  passed without one, because a user agent may run message tasks ahead of timers indefinitely.
  So a timer due during a long query (owner liveness, idle flush, transaction timeouts) runs
  within about 50 ms plus a slice of becoming due: an extra scheduling-latency budget, not a
  deadline, so a timer that must finish its work by a deadline has to be armed that much
  earlier. Operators pace their own output the same way: a slice ends only at a task yield, not
  when downstream backpressure resolves at once. `pipeline-yield.test.ts` checks the yield on an
  event loop that always runs messages first, and `browser-tests/next-yield.spec.ts` in real
  Chromium, Firefox and WebKit (where message-only yields serviced no timer at all). Node's
  `setImmediate` already follows the timers phase.
- `abort` releases grants, spill files and transferred batches.
- The driver adds no cancellation machinery without a signal, and with one each wait removes its
  listener, so a long stream retains nothing per batch.
- Each edge holds at most one in-flight batch, plus whatever credits the producer has handed out
  as transfers.

### Executor entry point (`exec/executor.ts`, `result.ts`, `scan.ts`, `memory-scan.ts`, `rules.ts`)

`createExecutor()` (`exec/create.ts`) now implements the query entry point. It prepares immutable
recipes for VALUES, table scans with pushdown, filter/project kernels, hash aggregation and
DISTINCT (including empty tuples: zero rows over empty input, one otherwise), hash joins (including keyless joins), sort/top-N, windows, and LIMIT/OFFSET. Each run
owns its kernels, grants and source borrows. Preparation checks immutable operator metadata,
without admitting execution grants or building breaker pools. A bound zero LIMIT skips its
input in both reservation and execution, including a zero stored on an ORDER BY sort.
The active node first binds both bounds and validates OFFSET before LIMIT, once per execution;
zero does not suppress its own negative/overflow error. Bounds in an unopened child stay
unreached, and prepare-time capability checks still cover that input. Small compiler probes determine
kernel buffer minimums; the generated allocation matrix checks them against real batch-sized
buffers. Execute accepts caller-owned parameter values (keep them unchanged until execution ends).
It admits the encoded inputs and active operator minimums atomically, then binds using that
execution grant. UTF-8 buffers, mutable byte copies and numeric conversion scratch are charged
before allocation and released on execution completion, cancellation or error. Immutable BigInt
coefficients can be adopted. An input that cannot fit normal capacity raises the governor's
`MemoryAdmissionError`; parameter inputs do not spill. Explicit `bind(values, grant)` callers
must keep that grant alive for the bound vectors' lifetime. Computed join/sort/window arguments are projected
into hidden columns and removed from visible results. No runtime code generation is used.

The owner composition uses it by default; protocol tests over `StandinWritePath` explicitly keep
their synthetic executor. DML, keyed nested-loop joins, index lookup/range, fetch, init plans, apply, set operations and
recursion are still typed prepare-time refusals, as are unsupported existing kernel overloads.
This factory does not turn those missing capabilities into current-engine fallbacks. Main's M2
lowerer also refuses joins/windows before they reach the factory; direct IR tests exercise the
operator wiring until M3 lowering lands.

Fixed operator buffers are reserved atomically before data-dependent state grows, so a join build
cannot consume the later probe's admission minimum. Individual operators retain separate grants
for growth and progress loans. Existing join/sort/window spilling remains active. Hash aggregation
still has its documented refused-growth limitation until partitioned aggregation is implemented.
Hash-join spill staging also has a known large-row defect: 600 kB build payloads at an 8 MiB
budget can request a second progress loan. This is a bug, not an intended size limit.
The result sink copies selected/dictionary/constant input into owned flat frames, with credit and
charges retained until release; it currently emits no dictionary deltas. It bounds total frame
capacity across columns, honors requested row caps, and permits one oversized row per frame.
Query completion waits
for pipeline/spill cleanup; cancellation also discards handed-out frames.

`testing/storage-core.ts` implements the existing differential adapter over strict committed
`WritePathImpl` writes, the real extent space and page readers. Generated SQL compares it with
PGlite and the current engine across flush/reopen. The frozen Gate B/SELECT-guide catalogue
executes the currently supported statements and separately reports typed refusals. R21's query
rule adapts only direct integer AVG outputs according to C11; it grants no floating tolerance.
`scripts/next-executor-bench.mts` measures Node CPU from SQL/lowering through materialized rows
over the real write path and flushed MemoryBackend storage. Ingestion is outside query timing;
this is not the browser Gate B lane.

The full interface contract includes the following (DML remains unimplemented by the factory):

- `executor.prepare(plan)` compiles a plan once; execute accepts raw values and binds after
  admission. Explicit `bind(values, grant)` also requires an admitted execution budget;
  `execute(request)` runs it over the access contracts: a read snapshot for a query, or an open
  `Statement` for DML, which the executor ends or rolls back. It never commits.
- The executor admits its own pipelines with plan-bounded minimums. State that outlives a
  pipeline is in spillable grants. A bounded step whose required loan exceeds the reserve
  fails with `ValueTooLargeError`. Non-spillable parameter inputs must fit normal capacity;
  an over-cap fixed/input admission raises `MemoryAdmissionError`.
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

### Expression kernels (`exec/kernels/`, `exec/filter.ts`)

- Prebuilt kernels, no code generation. An IR expression compiles once per pipeline instance;
  its output buffers come from the grant at compile time and are reused for every batch.
- Exact types: int32 with PostgreSQL's range errors; int64 as `safe` f64 while every value stays
  within ±(2^53 − 1), recomputed exactly in BigInt64Array otherwise; floats with PostgreSQL's
  overflow, underflow and division-by-zero errors, REAL rounded with `Math.fround`; decimals with
  per-slot scales, never rescaled to a shared maximum: f64 while exact, then the int64 and int128
  tiers in 32-bit limbs (no BigInt per value; rows grouped by scale pair, any number of pairs).
  A batch is recomputed with one BigInt per value only when a value of it passes 128 bits, an
  input is already wide, or a product passes the display-scale cap.
- Casts carry their target type (a widened SMALLINT is an INTEGER to the call above it).
  DATE → TIMESTAMP(TZ) is 22008 past 294276-12-31, as PostgreSQL.
- Comparisons follow PostgreSQL: NaN equals NaN and sorts last, decimals compare by value across
  scales, text compares bytes. A dictionary column against a constant is compared once per
  entry.
- AND, OR, CASE and COALESCE evaluate a later argument only on the rows still undecided, so a
  guarded division never divides by zero. Filters narrow a selection vector conjunct by conjunct.
- Hot loops are branch-free (a data-dependent branch mispredicts on half of random rows): range
  checks look at the batch's largest result after the loop, and a suspicious batch is rechecked
  row by row.
- Memory: output buffers are allocated once per compiled expression; number paths allocate
  nothing per row, and a batch costs tens to a few hundred bytes of small objects (measured by
  `scripts/next-kernels-bench.mts`). Only BigInt paths (values past 128 bits, int64 past 2^53 in
  casts and comparisons) allocate per value.
- `pglite.test.ts` judges generated cases by PostgreSQL itself: every numeric type, casts, NULLs
  with garbage under them, flat/dictionary/constant operands, wide values.
- `ir/functions.ts` (lowering's) is the one table of functions, overloads and casts (R19).
  `exec/kernels/bindings.ts` binds each entry to a kernel or refuses it with a construct key
  (`UnsupportedPlanError`, stage "execute"); `bindings.test.ts` fails on an unbound entry, a
  stale binding, or a kernel binding that does not evaluate. Not implemented yet: date, time and
  interval arithmetic, text and math functions, casts to and from text, bounded text casts.
- NUMERIC division and modulo (`decimal-div.ts`, `DecimalDivNode`): PostgreSQL's result scale
  (`select_div_scale`: at least 16 significant digits, capped at 1000), rounded half away from
  zero; 128-bit limbs while the scaled operands fit, BigInt past that.

### Table scan (`exec/table-scan.ts`, `exec/pushdown.ts`, `exec/column-target.ts`)

- One table of a snapshot, every segment in row-id order, batches of at most 2,048 rows and about
  1 MiB, the scan's columns in output order (plus a locator column for late materialization).
- Group segments: prune → pushed predicates → decode the selected positions. The filter's
  conjuncts that compare one column with constants (comparisons, `IN`, `IS [NOT] NULL`) go to the
  page format: groups whose statistics rule one out are skipped unread, then per range the live
  rows are narrowed by `select` / `refine` on encoded pages. `select` over a whole range loads
  only the pages the predicate cannot rule out; `refine` (after deletions or an earlier conjunct)
  loads every page its positions touch. The rest of the filter (the residual) runs on decoded
  values.
- At or above 40 % of a range selected, every column decodes densely (residual columns too) and
  keeps the selection, the residual then narrowing it. Below it only the selected positions are
  gathered; with a residual, its columns are gathered first and the others only for the rows it
  keeps.
- Shape setup reuses only verified scalar header facts while endpoint pages remain resident;
  install/release/materialization invalidates them. Exact-range byte counts have a separate
  memo. `exec/column-shape.test.ts` compares against header queries with memoization disabled
  and byte counts summed directly from immutable source rows, including gathers. It never
  uses the candidate reader's `varlenBytes` or `gatherBytes` for reference sizing.
  `testing/column-shape-differential.test.ts` extends that comparison over actual cache
  eviction/backend reload, three same-layout groups through one target, all-valid/sparse/all-null
  masks, empty ranges/gathers, genuine >64 KiB out-of-line values, stored gzip, and stored
  dictionary code widths 0/1/2. Reach assertions require each named state and encoding. The
  current writer's 65,536-row group limit cannot emit a 4-byte dictionary code; that reader
  capability is outside this writer-generated comparison. A stale exact-range-size mutant
  must fail byte accounting even when an oversized output could still decode correct values.
- Batches are bounded by their decoded bytes: validity, values or codes, scales, limbs,
  specials, offsets, varlen bytes (full value lengths, out-of-line values included) and
  locators, all known from page headers before any value is read. One row larger than the target
  is a batch of its own. Decode buffers grow with the batches (at least 64 rows, doubling, at
  most 2,048), so a small target takes small buffers.
- Before a batch decodes, the scan reserves its whole working set from the grant: the
  out-of-line values it leases and every buffer it grows. A refusal halves the batch; for one
  row the scan borrows a progress loan (R18.3) for that working set and repays it once the row's
  buffers and values are given back: after the batch's borrow ends, or at once when the
  residual rejected the row. Out-of-line values are
  released when the batch's borrow ends; the scan's own scratch (position lists, selections,
  locators, memory-segment staging) is charged before it is built.
- Pushed predicates on the decoded fallback (below) load the values they read when they
  evaluate, before the batch is sized.
- Long runs that emit nothing (ranges, chunks and groups filtered or pruned away) yield to the
  event loop with a task once 4 ms have passed since the worker's task began (R23, R23.5: the
  one task clock, so `next()` calls and resolved continuations never start a slice).
- Ending: at the end of the table the scan gives back everything it holds by itself; `abort` (a
  cancellation, an error, a LIMIT) does the same at once and repays a loan, never throwing. A
  `next()` still waiting on a read then resolves null: whatever settles after the abort (a reader
  opened, a loan served) is closed or repaid, and nothing more is read, decoded or emitted. A
  loan the abort could not repay because the scan's own read still held loan-backed bytes is
  repaid when that read settles; `close` reports one that another operator's bytes still hold.
- Pushdown (`pushdown.ts`): pushed conjuncts run on encoded pages through the page format's
  read-side pushdown (`compilePredicate`, `pagesMatching`, `select`, `refine`, `chunkMayMatch`;
  "Predicates on encoded pages" above). Where it does not push a predicate down for a chunk
  (interval, json, arrays, mixed-scale decimals, enum order: `compilePredicate` returns null),
  the same comparison runs on decoded values with the executor's kernels, without statistics
  pruning. Semantics are PostgreSQL's either way, so where a conjunct runs never changes the
  answer. `selectMask` (the fused dense filter and aggregate) is not called by any operator yet.
- Memory segments are read through `memory-scan.ts` (one lent chunk at a time, zero copy), the
  whole filter as the residual.
- `table-scan.test.ts` checks generated tables (groups through the page writer, memory segments,
  deletions, columns some groups predate, values stored out of line) and filters, with pushed
  conjuncts on encoded pages or forced onto decoded values, against the same scan with pushdown
  off (decode-then-filter), and that against the source rows; every batch it decodes stays
  within the byte target, and the scan holds nothing once it ends. Pinned tests cover loans
  (repaid on close, on abort, after a failure; sized for a row's value lease and buffers), abort
  at every suspension of a group scan and during a memory chunk's read, pacing over rejected
  rows, and the charged scratch.

### Aggregation (`exec/agg/`)

- `aggregate.ts`: the `aggregate` operator. Each batch maps its rows to dense group ids, then
  every aggregate (after its FILTER and DISTINCT) updates its state. `finish` emits groups in
  first-seen order in batches of at most 2,048 groups and about 1 MiB, each charged to the grant
  before it is built and released when downstream is done with it; it yields to the event loop
  with a task every 4 ms, checking the deadline per group inside AVG(NUMERIC)'s divisions and
  resuming mid-batch (it prepares at most what the next batch holds, charged by `bigintBytes`,
  and released as a batch takes it; with several AVG columns one may keep up to a batch's worth
  across a yield or backpressure, still charged, until the next batch or close/abort); after an abort, or once its abort signal fires (checked at every slice and
  after backpressure, R23), it rejects with the reason. Scratch that outlives a slice belongs to
  the operator; module-level scratch is used only within one synchronous call (R23). Without
  GROUP BY there is one row even over no input.
- `groups.ts`: group ids from open-addressing tables chosen by key type — interleaved
  (key, id) tables for 32-bit and 64-bit integers (int8 by its two words, whatever its
  representation: no BigInt, no Map), an f64 table for floats, xxHash32 over bytes for text (a
  dictionary column translated once per entry), key encoding for NUMERIC and the rest, and an id
  tuple for several keys: direct-mapped (the value ids bit-packed into a slot of a table of at
  most 2^16) while the columns' value counts fit, hashed column by column past that. SQL GROUP BY equality: NULLs together, −0 = 0, NaN = NaN, NUMERIC by
  value.
- One int2/int4/date key whose range spans no more slots than the hash table would hold (twice
  its slots, at most 2^22 values) is direct-mapped instead: slot `key − base`, no hashing or
  probing, half the bytes. A key past the span widens it; a range too wide goes back to hashing;
  modes change only between batches, and ids stay first-seen either way. Per-group arrays grow
  2× while small and 4× from 65,536 elements (less fresh memory on the way to many groups).
- With more than 4,096 groups and at least four aggregates over the batch's own rows, the
  operator hands its accumulators the rows stably sorted by group id (LSD radix, 11 bits a
  pass), so each one's state arrays are read nearly sequentially; a group's rows keep their input
  order, which float sums and MIN/MAX ties depend on. Measured faster than a group-row state
  layout for many aggregates (which gained 10–25 %), and not used for few aggregates, where it
  costs more than it saves.
- `states.ts`: struct-of-arrays state per aggregate, no object per group. Integer and NUMERIC
  sums are exact at any size: an f64 part within ±2^52, a 128-bit carry, and past that an exact
  BigInt offset per group that changes only when the carry overflows (or for inputs already
  wider than 128 bits), never per row. A scalar NUMERIC SUM over a declared precision ≤ 12 is
  fused: four accumulators, no per-row check. Without GROUP BY, the batches of several such
  columns that qualify (no NULL, every row) are summed in one loop over up to four columns
  (`fusedSums`), and a SUM and an AVG of one exact column share one state. Accumulator loops
  start from −0 (a double), so V8 keeps them unboxed. Float SUM and AVG follow PostgreSQL's float8pl and
  float8_accum, including their 22003 rules. MIN/MAX ties take the newer value, as PostgreSQL
  does (MIN(1.5, 1.50) is 1.50); int8 and NUMERIC comparisons use words and limbs.
- MIN/MAX over dictionary-encoded text, bytea and uuid compare ranks, not codes (page
  dictionaries are first-seen order): a dictionary small for its batch is ranked by value once,
  and a group's best gets its place among those ranks by a binary search over values when the
  dictionary first meets it. Groups and dictionaries are only ever compared by value.
- SUM over int2/int4 reads the decoded Int32Array directly and, while no batch can push a part
  past ±2^52 (a bound on every part, rescanned about once per 2^21 rows), skips the per-row
  check; near the limit it takes the checked loop and the carry.
- `values.ts`: per-group values for MIN/MAX and NUMERIC keys: int8 and interval microseconds as
  two words, NUMERIC as f64 or 128-bit limbs; only coefficients past 128 bits are BigInts, each
  charged at the governor's `bigintBytes` (its size change charged before a replacement). Exact
  sums' offsets past 128 bits are charged the same way, and output estimates use those sizes.
  Int64-family MIN/MAX also charges an 8-byte numeric winner cache per allocated group slot;
  comparisons use it only while both values are safe integers, otherwise comparing exact words.
- NUMERIC results past PostgreSQL's limits (131,072 digits before the decimal point) are 22003 at
  emission, SUM's and AVG's alike; exact intermediates may pass them and come back.
- `distinct.ts`: exact DISTINCT through value ids and a (group, value id) set.
- Scan statistics hints (future; pre-sizing and AVG Sxx skipping are deferred).
  High-cardinality pre-sizing is deferred. The scan would need to supply a conservative NDV
  estimate for the grouping expressions (not merely a row count), its scope across pages and
  dictionaries, and whether a selection/filter invalidates it. A shared dictionary's entry count
  can bound one dictionary key; per-page counts must not be added as if dictionaries were disjoint.
  Composite keys need a joint estimate. The aggregate must charge any reservation before growth,
  cap it to the grant, and retain ordinary incremental growth when the estimate is absent or low.
  AVG also needs a query-wide finite max|x| and upper row count N, excluding NaN and infinities.
  A written bound must prove PostgreSQL's rounded Youngs-Cramer tmp, tmp²/(n(n−1)), and
  accumulated Sxx cannot overflow for any input order. Per-batch bounds are insufficient:
  once skipped, the omitted Sxx cannot be reconstructed in PostgreSQL's operation order
  when a later batch approaches overflow. Keep the exact path until that proof exists.

- Memory: every typed array is charged before it is constructed, and a constructor that fails
  releases what it took. A refused growth fails the query until partitioned spill (milestone 3).
  Not yet: GROUPING SETS, ordered-set and string/array/JSON aggregates (`UnsupportedPlanError`,
  construct `aggregate`, refused before anything is allocated).
- `aggregate-pglite.test.ts` judges generated rows and batchings by PostgreSQL, including sums
  across ±2^127, with declared reach; `groups.test.ts` checks the group tables against a model
  past several growths; `review-1.test.ts` holds the review's probes.
  `scripts/next-agg-bench.mts` measures the shapes of measurements.md.

### Hash join (`exec/join.ts`, `join-keys.ts`, `join-table.ts`, `join-filter.ts`) and shared heavy-operator files (`column-store.ts`, `spill-pages.ts`)

- Every kind: inner, left, right, full, semi, anti and mark. A join may also have no keys,
  only a residual (ruling R24.2: PostgreSQL refuses such a FULL JOIN, 0A000, for an
  implementation reason; the result is the LEFT JOIN plus the unmatched build rows): it runs
  over one constant key, so every pair is a candidate and memory stays bounded by chunk mode.
  Equality is SQL's: `=` never
  matches NULL, `nullSafe` keys use IS NOT DISTINCT FROM, floats treat −0 = 0 and NaN = NaN,
  NUMERIC and INTERVAL compare normalized. A residual ON condition is a vector predicate over
  the pairs; FALSE and NULL both reject.
- A mark join's mark is `T IN (build T where build C = probe C)` with SQL's three-valued
  result, exact per column for composite keys. Its output is the probe columns plus one bool
  (the shape lowering proposes for the IR, ruling R17.5).
- Keys are input column indexes; the executor projects key expressions first.
- Key bytes are work in steps (R23.2): every byte hashed, copied into an encoded key or
  compared counts toward a 256 KiB step budget, and a spent step yields when its slice is due —
  inside one key, a collision chain, a mark join's candidates and its compatibility tests. A
  composite key with text or bytes length-prefixes them instead of escaping them (equality
  needs no order). A batch's encoded keys get one buffer of their exact size, charged before it
  is allocated, and each value is copied into it once, in budgeted pieces (no regrowth copies).
- The build store and table are allocated from the join's spillable grant. A dense integer key
  is direct-addressed; others use open addressing with the key's words inline. Matched outputs
  reuse probe key vectors for build key columns that are identical in representation.
- When refused memory the join partitions 16 ways (grace/hybrid), reads back the partitions
  that fit, spills the rest of the probe, and joins each spilled pair one level down with
  independent hash bits. A partition that does not split, or an oversized mark join, runs in
  chunks with per-probe-row state. A bounded step that exceeds the governor reserve is
  refused with `ValueTooLargeError`; a value fitting the reserve is not a general entitlement
  to every multi-column working set.
- Spill pages (`spill-pages.ts`) are the spill contract's typed layout; every `VectorData`
  variant round-trips. `column-store.ts` is the chunked, charged row store operators keep rows
  in. Zero-column stores still count rows and obey single-chunk limits, so key-only
  joins can reuse their keys without a payload gather. Wide decimal copy accounting inspects
  only decimal columns, preserving mixed numeric, boolean and text payloads. Both are shared exec/ files (ruling R20.2) for aggregation spill and sort runs as well.
  Join abort waits for spill reads, appends and file closes before releasing borrowed pages, stores,
  the spill pool or the grant; `closed` covers that drain, including close failures.
  A read registers its ownership before entering the backend. Cancelled read results are
  discarded while charged, before decoding or resuming load/replay; read leases also fence
  per-run closes. `join-read-fence.test.ts` gates load and replay across read/close failures.
  Oversized staging rows flush immediately, including when their first allocation fits: their
  arenas and page buffers cannot accumulate across partitions and pin a progress loan.
  Join staging checks the shared task deadline between oversized rows, even when every spill
  append resolves immediately; resumption checks abort before copying another row.
  `join-large-row.test.ts` checks generated large TEXT rows against PGlite at 8 MiB and the
  admitted minimum, with a probe connected during build. Two-sided oversized-row chunks
  reserve loading and replay by their peak lifetimes, borrow a single probe row during output,
  and retire loans as soon as live usage fits ordinary pool capacity.
  `join-oversized-pair.test.ts` checks generated neighboring widths and residuals;
  `join-reserve-boundary.test.ts` checks the two-row 850 KiB reproduction and the complete
  six-kind, three-sided width matrix from 65,535 through 1,048,575 bytes against PostgreSQL,
  comparing complete payloads under immediate downstream backpressure.
  Store-to-store gathers bulk-copy fully checked contiguous spans of non-null int32/f64
  storage (including safe int64), across chunk boundaries; other permutations and negative
  row ids keep the ordinary gather. `exec/column-gather.bench.ts` isolates contiguous and
  arbitrary and partition-strided gathers with prebuilt input, untimed warmup and result
  checks. Compare revisions in at least seven interleaved rounds, recording process CPU
  medians, max-minus-min spreads and host load for each run.
- Inner, semi and right joins publish runtime filters (exact bitmap, split-block Bloom, bounds)
  when the build stays resident; a filter never drops a row that can match, and stops testing
  when it keeps over 90 % of probes.
- Probe continuations check cancellation and the remaining worker-task deadline immediately
  after downstream readiness. An already-resolved readiness promise never restarts the clock.

- Push returns `blocked` when the downstream blocks or a spill write waits; with no spill the
  whole join runs without an await.

### Sort (`exec/sort.ts`, `sort-keys.ts`, `sort-lsd.ts`)

- ORDER BY follows PostgreSQL: NaN above every number, −0 = 0, NUMERIC by value, NULLS FIRST/
  LAST, DESC, text in C (byte) order. Keys are input column indexes.
- Key mode: rows carry the frozen sort-key bytes and five prefix words; byte-key sorting uses an
  MSD radix sort over (prefix words, row id) tuples, refining by bytes only when a key is longer
  than the prefix. Fixed-width keys (booleans, integers, dates, times, timestamps, floats) that
  fit the prefix skip the key bytes: they are encoded straight into the words (direct mode).
  For at least 65,536 direct-key rows, stable 16-bit LSD passes scatter row ids against
  contiguous normalized word columns, skipping digits proven constant. Smaller direct sorts
  and byte keys retain MSD. Both paths use the same normalized order, including NULLs, DESC,
  NaN and exact int64 words; the LSD arrays fit inside the existing sorting reservation.
- An ICU `COLLATE` sorts by query-wide ranks from `Intl.Collator` (ties by bytes) when the
  distinct values fit; otherwise, and for JSON and array keys, a value comparator (compare
  mode), in memory and across runs (DECISIONS.md C8). Compare mode decodes each row's JSONB,
  array and ICU text keys once, as the row arrives (for a merge, once per page), and charges the
  decoded values to the grant, so a run's comparisons never decode.
- Top-N periodically compacts buffered candidates to the best k rows (WITH TIES kept).
  Once a threshold is available, rows whose first fixed-width numeric key is strictly worse
  are rejected before key encoding. First-key ties and unsupported first-key types retain the
  full comparison. The fixed scalar snapshot is charged and retains no input buffers.
  Up to 65,536 kept rows, it holds its next compaction's store in reserve, so a full
  grant compacts instead of spilling whenever it can hold the kept rows twice. `GroupTopKOperator` is ROW_NUMBER() ≤ k per partition (the row number appended);
  after a compaction each partition keeps its k-th key, and rows that cannot beat it are dropped.
- Grant-or-spill: sorted runs of typed spill pages, merged with a fan-in the spill pool's room
  allows, in passes when needed; OFFSET rows are counted, not materialized.
- Sorter scratch buffers, including the radix histograms, are constructed only after their
  owning charge succeeds. Refusing scratch admission constructs no radix arrays and returns
  the pool allocation made earlier in the constructor.
- Finish work (compaction, the sort, the merge, emission) runs in slices of `YIELD_INTERVAL_MS`
  with a real task yield between them; each slice and each resumption after backpressure checks
  the abort signal (ruling R23). Scratch that lives across a suspension belongs to the operator.

The isolated CPU benchmarks `exec/sort-radix.bench.ts` and `exec/window-sort.bench.ts`
measure prefix sorting and grouped window sorting separately from input construction and
window evaluation. Bundle each with esbuild for Node and run with `node --expose-gc`; `ROWS`
and `RUNS` set the input size and measured repetitions. The grouped benchmark also accepts
`GROUPS`. Compare revision bundles in at least five interleaved rounds. These component
measurements do not establish the whole-query browser performance gates.
`exec/sort-direct.bench.ts` isolates fixed-width key normalization on prebuilt batches,
checking every word against frozen sort-key bytes outside CPU timing.
`exec/topn-filter.bench.ts` compares encode-first and raw-filter-first on prebuilt batches
with a fixed threshold; use `MODE=encode` and `MODE=filter` in interleaved rounds.
The `exec/sort-lsd.bench.ts` comparison accepts `MODE=msd`, `lsd` (contiguous columns), or
`lsd-tuples` (the rejected strided prototype), and `SHAPE=u32`, `u64`, or `gate` (the Gate B
float64/int64 keys). Compare `ROWS=100000` and `1000000` in at least seven interleaved rounds,
recording load per run. The component excludes key preparation; whole-operator comparisons
include it. Validation checks all normalized words, row multiplicity and stable ties untimed.

### Windows (`exec/window.ts`, `window-sort.ts`, `window-frames.ts`, `window-stream.ts`)

- Prefix counts, ordinary and bigint sums, promotion, and long moving-frame aggregate scans
  check the shared task clock inside bounded chunks. Resumptions after a task yield check
  cancellation before continuing the kernel. Short streamed frames keep a synchronous path.

- Float RANGE offsets follow PostgreSQL's `in_range_float8_float8`: NaN peers compare equal,
  NaN sorts above infinity, and NULL validity stays separate from numeric values. Infinite
  offsets retain PostgreSQL's infinity rules on both resident and streamed paths. NaN and
  negative offsets are rejected with SQLSTATE 22013; positive infinity offsets remain valid.

- One task clock per worker (`exec/pipeline.ts` `TaskClock`, ruling R23.5): the driver, scans,
  aggregation, join, sort, top-k and window all yield once `YIELD_INTERVAL_MS` has passed since
  the worker's task began, and keep no slice start of their own, so several operators in one
  task share the one bound. The clock is stamped only when a `yieldTask` resolves and at the
  owner's task entry points (the server's message dispatch): never in `drive()`, an operator's
  entry or a resolved promise. A missing stamp yields early, never late.
- `WindowOperator` evaluates a `window` rel. Its rows are ordered by (PARTITION BY, ORDER BY),
  then evaluated one partition at a time. Output is the input columns, then one column per
  function.
- The grouped sort (`window-sort.ts`) orders them when it applies: at most one PARTITION BY
  column of text, bytes, an integer, date, time or timestamp type, and ORDER BY keys that are
  all fixed-width numbers, dates or times. Rows are held with a partition id (a dictionary
  column translated once per dictionary) and their keys normalized to two 32-bit words each;
  partitions are ranked by value and rows placed by an LSD radix on the rank (no comparisons),
  then each partition by an MSD radix on the first key (skipped when the rows arrive in order),
  ties ordered by the other keys. Each row is gathered once, in order. A partition column
  nothing else reads is neither stored nor gathered: it goes out as a constant vector of the
  partition's value.
- Otherwise the sort operator orders the rows, and the grouped sort moves its rows there when
  the grant refuses its memory.
- Functions:
  - ranking: row_number, rank, dense_rank, percent_rank, cume_dist, ntile;
  - values: lag, lead, first_value, last_value, nth_value, over any type;
  - aggregates: count, and sum, avg, min, max of integers and floats.
- Frames follow PostgreSQL:
  - ROWS, RANGE (one numeric ORDER BY key) and GROUPS, with offsets;
  - EXCLUDE CURRENT ROW, GROUP and TIES;
  - for a non-NULL row, RANGE offsets treat NULL keys as below every value (NULLS FIRST) or
    above every value (NULLS LAST).
- Float sums add a frame's members in order, as PostgreSQL does; exact sums use prefix sums.
- int8 values stay exact past 2^53 on both paths, as bigints where a double would round:
  sums, MIN/MAX, and RANGE keys and their offsets (RANGE over int8 compares exactly). COUNT
  counts non-NULL rows only. Bigint state derived from int64 values is reserved at 48 bytes per fixed-capacity
  slot (up to 128-bit sums), including promotions caused by sums of safe inputs. The resident
  path reserves a conservative bound before evaluating and streams on refusal. Streaming bigint
  arrays grow at explicit capacities, with replacements charged while old arrays are live
  (governor rule 4). Result references are dropped before their charge is released.
- lag and lead accept negative offsets (lead(x, −k) looks back), resident and streamed.
- A partition is held in memory and evaluated over typed arrays while the grant holds it, up
  to `streamRows` (4,194,304) rows.
- A larger partition, or one the grant cannot hold, streams from its spilled pages. Memory is
  then bounded by the frame, not by the partition:
  - ROWS frames and running aggregates keep the frame's rows;
  - RANGE and GROUPS frames keep the peers plus the frame's extent;
  - frames that need the whole partition (UNBOUNDED FOLLOWING, ntile, percent_rank,
    cume_dist) read it twice, counting first.
- A few combinations cannot stream: a column-valued lag, lead, ntile or nth_value amount;
  nth_value past 4,096; and float sums or MIN/MAX over a moving frame that ends at UNBOUNDED
  FOLLOWING. Such a partition must fit the grant.
- Partition and peer boundaries are found a batch at a time, column by column; plain frames
  (no EXCLUDE) sum and count from prefix arrays reused across partitions.
- Arrival, the per-partition kernels and emission run in slices, checking the abort signal
  (ruling R23).

### Owner and client protocol (`owner/`)

The internal owner client is `ProtocolClient` (`owner/client.ts`). Its explicit columnar export
API is `exportStream(sql, params?, { session?, windowBytes? })`. It is not a published package API.
It accepts queries, including queries in a session, and rejects modifying/control statements.
`query()` and the existing cursor retain their current lifetimes and 1 MiB source target.

```ts
const stream = client.exportStream("SELECT * FROM t");
const columns = await stream.columns; // resolves with the first reply, before iteration completes
try {
  for await (const frame of stream.frames) {
    try {
      // Consume frame.payload's physical vectors. A dictionary column's codes use
      // frame.dictionaries.locate(id, code); entries remain bytes, not decoded strings.
      consume(columns, frame.payload, frame.dictionaries);
    } finally {
      frame.release();
    }
  }
  await stream.done; // the owner's StatementSummary; it never waits for buffer returns
} finally {
  stream.close();
}
```

Credits return when the iterator hands out a frame, at the existing piggyback/half-window/timer
pacing. Only `frame.release()` relinquishes its buffers. It is idempotent and transfers all
frame-owned buffers, even for a tiny frame that ordinary results would clone. After release,
its typed views have length zero or throw `TypeError`; callers must not use them. Plain JS value
arrays are copied values, not recycled storage. Per-stream dictionary deltas are copied into
compact client storage before frame handout, so later codes remain readable after earlier frames
are released. These dictionary chunks are shared stream storage, not frame-owned views.

`close()` (also iterator `return()` on a break) ends normally and relinquishes queued frames;
`cancel()` aborts iteration. Closing/cancelling before a request is sent rejects it locally with
`CancelledError`, without replaying it after connect. Only one iterator `next()` may be pending.
Both preserve frames already handed to the consumer, which remain
valid until that consumer calls their release. A reconnect/owner change fails an already-started
stream with `SessionLostError`; it never replays frames already received. Old frame handles
capture their original connection. Releasing one after reconnect, client close or owner change
still detaches locally, and never sends its capabilities to the replacement connection.

Each connection creates its return pool lazily on its first export. It is charged to that owner's
governor, capped at 4 MiB (twice the default credit window, regardless of larger user prefetch),
and closed on connection teardown. Capabilities are one-use and bounded; expired, duplicate,
foreign, detached or wrong-sized returns are dropped. No lost return is awaited. Client-held
frames after credit are client memory: the cap bounds returned idle buffers, not arbitrary caller
retention. A terminal full-table export can use the larger 2 MiB target and decode directly into
exclusive returned output. SQL's names-only identity projection forwards that ownership and keeps
its profile counters; computed/reordered projections retain the copying path. No page-cache or
slab buffer is eligible. Protocol version 6 adds the explicit mode, capability lists and independent
`returnBuffers` signal; old versions are refused during handshake. This is a wire-only change,
with no stored-format change.

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
- On `hidden` the owner flushes pending relaxed commits (mobile steps down instead). Deliberate
  shutdown on `freeze`, `pagehide`, replacement or close stops new work, waits up to 500 ms for
  active work (including disconnected clients), engine close and the factory's `closeSession`
  adapter to `StoreSession.close()`, then awaits its `closeFence` adapter to
  `lease.release()` before terminating the worker. Lease release drains storage for up to 500 ms,
  then aborts remaining IndexedDB transactions and waits up to 1 s for their end. The page's
  fallback termination bound is 3 s, including owner drain and scheduling margin. A release
  failure (including `ReleaseUnsettledError`) or an owner drain timeout is logged and reported
  to pending calls as `SessionLostError` with effect `unknown`, with a reload suggestion.
  A stolen fence still forces immediate shutdown. Abrupt page destruction may prevent graceful
  shutdown; the storage backend's serialized chain remains the defense against that kill.
- Every request has a per-client `seq` (the ledger sequence) and exactly one terminal reply.
  Cancellation is acknowledged by that reply; the write path's `commit()` call is the point of no
  return.
- Only commits are exactly-once. Statements that change an open transaction keep a
  session-scoped outcome, so a duplicate never runs twice; everything else, standalone
  `nextval` included, is at-least-once.
- A transaction idle for `transactionIdleTimeoutMs` (30 s) is rolled back; the session then fails
  with `TransactionIdleTimeoutError` until the client ends it.
- Sessions follow PostgreSQL: any error inside a transaction aborts it (25P02 until it ends);
  savepoints are an ordered stack (a newer one shadows an older one of the same name; read-only
  transactions may use them; ROLLBACK TO restores the transaction's counts). Statements plan
  against the session's catalog, a transaction's own DDL included; plans of uncommitted schema
  stay private to the session.
- A hello whose generation is not newer than one already seen from the client is refused
  (`staleGeneration`). A retried write whose RETURNING rows are lost fails with
  `CommittedWithoutResultError`, never an empty result.
- Result frames are the executor's payloads, pulled and transferred without a copy, and charged
  until the client releases them. Dictionaries are per stream, sent as frame-level deltas.
- An ordinary message whose buffers total less than `TRANSFER_MIN_BYTES` (1 KiB) is cloned, not
  transferred: below it a copy is cheaper than detaching the buffers (R22.1; the latency lane
  measures the crossover per engine). Releases ride on the client's next request, or go alone
  after `RELEASE_DELAY_MS` (10 ms) or once half the result window is unreleased (R22.2).
- A session with an open transaction belongs to its connection generation. After a reconnect
  every request on it fails with `SessionLostError` in the client, unsent, until the client ends
  the session (ROLLBACK); nothing meant for the lost transaction runs as autocommit. A COMMIT in
  flight at the loss is resent and answered from the ledger, prepared or not.
- What a request does to its transaction comes from authority: the owner's front end for a
  prepared statement (`Prepared.statementKind`), and for SQL text a classifier that follows the
  front end's lexer (`classifyStatementText`, tested against `compileStatement`); multi-statement
  text is `unknown`. A request that may open, end or change a transaction is never resendable.
- After a reconnect every sent, unanswered request goes back unchanged, in send order, and the
  new owner's ledger resolves it: a recorded outcome is delivered; a retry it has no outcome for
  is refused, never run, with `SessionLostError.effect` `none` for a BEGIN, COMMIT or ROLLBACK
  (a commit records its outcome atomically with its effect) and `unknown` otherwise. The client
  itself decides only "never sent" (`none`). A session open at the loss, or with a BEGIN or
  unknown request unanswered, refuses new requests until the owner confirms a ROLLBACK.
- `close()` settles everything it interrupts: requests and cursors fail with
  `ClientClosedError`, and so does a `start()` still electing, discovering or waiting for its
  owner to be ready.
- A warm cross-tab key read has a synchronous owner path and one message each way: targets
  ≤ 0.25 ms Chromium, ≤ 0.4 ms Firefox, ≤ 0.6 ms WebKit; p95 ≤ 5 ms beside analytics.
- Another protocol version is refused; a newer build supersedes older tabs, which reload.
- The owner worker (`host.ts`) takes the fence, opens the engine, then steals the host lock
  from the page that won the election, so any death of the owner frees it. After every startup
  await it checks the fence; a stolen fence tears everything down before anything is served. The broker hands out
  a registration only while its recorded lock holder still holds the host lock. Clients watch the
  host lock while requests are in flight, and the owner watches a client's liveness lock while it
  holds that client's pins or live requests; both watches stop only after 1 s idle, so a request
  loop pays no lock call per request. The client re-announces its liveness lock on every new
  connection. The handshake is watched (an owner dying mid-handshake frees the host lock) and
  bounded (`HANDSHAKE_TIMEOUT_MS`, 5 s); either way the connector starts over. The broker holds a
  client's port only until the next registration of a database it asked about.
- The broker can be replaced under its ports: WebKit 26.6 restarts a SharedWorker on a
  cross-origin-isolated page when the page that created it closes, with empty state, the other
  pages' ports reattached, and messages sent during the switch dropped
  (`owner/webkit-sharedworker.md`). So every broker request is bounded: it waits
  `BROKER_REQUEST_TIMEOUT_MS` (1 s), doubling, first on the same port and then on fresh ones, and
  past `BROKER_REQUEST_BOUND_MS` (15 s) it fails with `OwnerUnreachableError` and the connector
  becomes `unreachable` instead of staying `connecting`. The same 15 s budget bounds the whole
  discovery/startup attempt, including repeated `noOwner` or `unreachable` answers, refused or
  unanswered handshakes, queued election and a worker that never reports ready. Broker or worker
  construction failures also arm the SQL grace and release the pending election. Replies never
  restart this budget. The next request, a broker that announces
  itself, or an owner announcement starts it again. Each broker instance has a random id it
  announces on every port: the client sends its unanswered requests again to it, and an owner
  registers again on a new id, but only while it still holds both its locks. Registration is
  confirmed by an acknowledgement naming that instance and owner. Unacknowledged registrations
  retry at 1, 2, 4 and 8 s, checking both locks and the stopped state each time, and fail at 15 s.
  A terminal failure is exposed in `OwnerHost.registration.error` and `minnow:failed`; the owner
  steps down instead of holding the database while undiscoverable. Repeated announcements of the
  same instance never restart the budget. An older broker without acknowledgements also reaches
  this explicit failure; reload its contexts to replace it. A connect request id is deduplicated
  within one instance's 1,024-entry cache; restart or eviction can create another connection,
  while SQL commit identity remains protected by the ledger.
- Requests issued before the first connection are sent on that connection, once and in issue
  order. After the first `unreachable` status, `unresponsiveAfterMs` (10 s by default) bounds
  waiting requests: `OwnerUnreachableError` reports effect `none` for a never-sent request and
  `unknown` for an unanswered sent request. A sent BEGIN or unknown statement keeps its session
  lost until an explicit end. After the deadline, new requests fail immediately with `none` and
  trigger another connection attempt. A successful connection clears the deadline; closing cancels it.
  A continuously disconnected request therefore waits at most 15 s plus 10 s at defaults,
  conditional on event-loop execution. Retries do not renew the latter deadline. This is a
  connection-availability bound, not a timeout on connected SQL execution.
  Supersession by a newer protocol fails pending SQL immediately with the same effect rule;
  a reload prompt never stands in for completing those calls.
  Closing cancels the underlying discovery work, including skew discovery's timer and
  BroadcastChannel, synchronously; it does not merely reject the promise waiting on it.
- A client learns that its owner was replaced even while idle: a new owner announces itself on
  the database's BroadcastChannel (`{t: "owner", ownerId, epoch}`, the fence epoch, strictly
  increasing). An announcement is a hint: a client acts only on an epoch above its owner's, and
  only after the host lock shows its owner no longer holds it; then it reconnects and resends.
  A stale, repeated or forged one changes nothing. Rediscovery that finds the same live owner
  resumes the connection instead of starting a generation (which would roll its transactions
  back). Without a usable BroadcastChannel (absent, or a constructor that throws) there are no
  announcements, only the backstop. Without it, an idle client kept its dead owner's port
  (a port does not report its other end's death) and its next request waited forever.
- The backstop is a silence deadline, `SILENCE_DEADLINE_MS` = 2.5 s (2.5 × `ALIVE_INTERVAL_MS`):
  a client with requests in flight that hears nothing for that long probes its connection with
  `ping`, which the owner answers with `alive` at once from its message handler (it runs between
  a statement's yields). Answered: the owner is alive and only slow, and the count restarts, so a
  false alarm costs one message, not a reconnect (which would end the client's open
  transactions). Unanswered: the client compares the host lock's holder with its owner's
  (`Welcome.lockHolder`). Another or none: it reconnects and resends; reads stay at-least-once
  and writes go through the ledger, as on any reconnect. Hidden holders: it rediscovers, and the
  same owner resumes. The same holder: it waits. Each probe waits up to 1 s for its answer, and
  each unanswered one doubles the next deadline (2.5 s, 5 s, then 10 s, counted from when the
  last probe went unanswered). After `SILENT_PROBES_LIMIT` (3) unanswered probes the
  connection is dead. At the production deadline: a ping at 2.5 s, unanswered at 3.5 s; a ping
  at 8.5 s, unanswered at 9.5 s; a ping at 19.5 s, and the connection is replaced at 20.5 s.
  That is about 20.5 s plus scheduling, lock-query and broker delays, while the client gets
  execution time. The connection is dead whatever the lock or the broker says: a live owner is
  not a live connection, so the client opens a new generation and resolves through the ledger.
  Before this (B5-p2d review 2, D2), a resume left the deadline expired and rediscovery ran with
  zero delay: 68 broker connections in 2.8 s, the request never answered. Measured `alive` gaps
  (headless Playwright, visible tabs; no engine here can hide a tab): Chromium and Firefox about
  1.0 s, at most 1.04 s; WebKit sends none while the owner runs a long statement, which the probe
  covers. The owner also sends an overdue `alive` whenever it handles a message or posts a
  frame, not only from its timer: in WebKit a worker whose work yields through message tasks runs
  no timers at all until it stops (a strict commit beside a long statement waited 1.8 s for its
  flush timer), which the executor's yield (exec/pipeline.ts `yieldTask`) must fix.
- `freeze` (on the document) and `pagehide` start the bounded step-down without asking the page
  for its context: Firefox can abandon a closing page's handler midway, and the `hidden` that
  follows must then find no live owner to flush.
- Statements go through real lowering (`lower/`, B6). The owner's LRU cache is bounded by
  both bytes (4 MiB by default, `planCacheBytes`) and entries (512, `planCacheSize`); zero in
  either limit disables retention. Keys, templates, raw plans and markers are charged to the
  governor using object/string/bigint/backing-buffer sizes, and registered for cache-first
  reclaim. Every hit refreshes recency, including marker and raw fallback hits. Eviction and
  engine close release charges; close also unregisters, even if the write path fails to close.
  Oversized or refused entries still execute, without retention. The cache stores `lowerParameterized` templates or `NotParameterizable`
  markers under normalized keys, including catalog version and client parameter types when
  supplied. A marker falls back to `lower(text)`, cached under that exact text only: constants
  never leak between family members. Each execution binds its own lifted literals after client
  parameters with `bindParameterized`. Warm prepared, parameterized and eligible literal reads
  neither lower nor prepare again, even when distinct literal texts exceed the cache capacity.
  Plans with no statistics basis skip drift checks. Uncommitted schemas retain session-private,
  full-text plans with the same byte/entry bounds and governor accounting, cleared at transaction
  end or rollback-to and unregistered on session/engine close; they never populate the shared cache.
- Table rules (CHECK, generated columns, defaults) are compiled per catalog identity, never per
  (table id, version) (`owner/rule-cache.ts`): the committed catalog's in one bounded (512 entries)
  cache that drops older committed catalogs' entries and closes with the engine; a transaction's
  own schema in a session cache dropped on ROLLBACK and ROLLBACK TO. Both are the governor's
  evictable caches (`registerCache`): charged at a size walked from the expressions, and evicted,
  oldest unused across caches, before growth is refused or an operator spills.
- SQL `BEGIN` / `START TRANSACTION` and `SET TRANSACTION` apply lowering's typed modes.
  READ ONLY starts with a pinned snapshot and no writer slot. Switching to it after writes
  keeps the transaction and commits those earlier writes; subsequent writes fail with 25006.
  Sequence mutation is checked at `SequenceAccess.next` / `set`, when execution reaches it:
  currval and unexecuted conditional calls remain allowed. SQL lastval() is still refused
  by the pinned lowerer (42883); it is outside this effect campaign.
- SET follows PostgreSQL's per-attribute restrictions (verified against PGlite). READ ONLY
  remains allowed after data work; READ WRITE is a no-op when already read-write, but switching
  from read-only is forbidden after a snapshot or within a savepoint. An unchanged isolation
  level remains allowed; changing it, or setting either deferrability value, is forbidden after
  a snapshot or within a savepoint (25001). Query/data attempts, including refused writes and
  synchronous point reads, take that boundary; SHOW does not. ROLLBACK TO restores modes and
  prior writes without resetting the snapshot boundary; RELEASE keeps subsequent modes.
  Modes reset at transaction end. SHOW transaction_isolation reports the selected level per
  session; the default is repeatable read. Every accepted isolation level uses the engine's
  snapshot level (lowering refuses SERIALIZABLE); DEFERRABLE has no execution effect.
  SET outside a transaction changes nothing; the wire has no PostgreSQL warning notices.
- Effective row locks and ordered duplicate SET mode instructions await reviewed B6 public
  `rowLocking` and `modeItems` metadata. Owner checks consume those interfaces without parsing
  SQL; interface tests run now. Real-SQL generators remain explicitly pending in the R21
  registry (`testing/divergences.ts`, `testing/owner-lowering-pending.test.ts`), because the
  pinned lowerer discards locking clauses and exposes only the final modes. Until that
  dependency lands, READ ONLY locking reads and order-sensitive SET are known gaps.
- The hidden-flush browser control terminates the worker before closing its tab. An ordinary
  page close can flush through visibility or graceful step-down, so it cannot require data loss.
- Tests: in process (`testing/owner-*.test.ts`, the election ones on in-memory Web Locks) and
  across real tabs in Chromium, Firefox and WebKit (`npm run test:browser:next-owner`), over real
  lowering and the stand-in executor and write path persisted in OPFS (IndexedDB where workers
  have no OPFS). The browser run includes the cross-tab point-read latency lane: it asserts at most
  4× a bare round trip to the broker in the same run, and R6's absolute targets with
  `MINNOW_PERF_LANE=1`, judged on the nightly browser-performance workflow (job `owner`) and on a
  quiet local run (R22.3).
- Owner-side cost of a warm prepared point read (p50, Node, `testing/owner-point-read.bench.test.ts`):
  the server alone ~3 µs; the owner's share in the full composition (server, plan-cache hit,
  admission, frames) ~15 µs, gated at 50 µs. The stand-in executor's key lookup scans the table
  (~100 µs at 1,000 rows); this protocol benchmark still explicitly injects stand-ins. It does
  not measure the real executor, whose index lookup plans remain unsupported, so real warm-PK
  latency is not established by those numbers.

### IR (`ir/`)

- Fully typed, positional and plain data. It is bound once, with parameter slots.
- Late materialization uses `scan.emitLocator` and `fetch`, executed through the `RowFetcher`
  contract (`exec/fetch.ts`): an explicit, enforced output capacity, charged and bounded staging,
  and partial completion that may reread pages.
- `apply` memoizes only when evaluation multiplicity is unobservable.
- `rules.ts` is a table's CHECK, generated and default expressions in IR form: lowering builds
  them, the executor compiles them (`compileTableRules`).

### Lowering (`lower/`)

- `Lowerer.lower(text, context)` runs the whole front end (parse, catalog passes over access/'s
  `CatalogView`, shape passes), types every expression, and returns a `LoweredStatement`: a plan
  with result descriptors, read and write sets and its statistics basis; a catalog change; or a
  transaction or utility command. `lowerer.ts` lists the front-end passes that are kept and the
  code that lowering replaces.
- Pure and synchronous; the owner caches by (text, catalog version, `parameterTypes` when sent)
  and re-lowers when `statisticsDrifted` says the data moved. A plan whose shape no statistics
  decided (a unique-key lookup, a lone scan) has an empty `basis`, so its hit skips the check.
- Literal auto-parameterization (`parameterize.ts`): `parameterize(text)` lifts each integer or
  plain string literal that is the whole operand of `=` into a trailing placeholder and returns
  the cache key texts differing only in those values share (a text scan, ~4 µs).
  `lowerParameterized` gives a lifted slot exactly the type lowering gives its literal alone and
  accepts the rewrite only where each slot is an `=` operand of the same type or an index key.
  It returns a `ParameterizedPlan` (run with `bindParameterized`, which refuses another key's
  text) or a `NotParameterizable` marker. Cache rule: cache the result under the key; on a marker
  lower each text itself and cache that plan under the text, never under the key. A literal
  point-read loop over 10,000 keys lowers once.
- Every failure is a `PlanningError` with PostgreSQL's SQLSTATE (42601 syntax, 42P01 relation,
  42703 column, 42883 function, 42704 type, …) or an `UnsupportedPlanError` (0A000). Front-end
  errors are translated by message (`frontEndError`), keeping the parse position and the
  original as `cause`. The messages it depends on are pinned (`testing/front-end-states.test.ts`:
  a snapshot of every mapped front-end message, and per pattern a PGlite-checked statement).
  HAVING names resolve before the grouping check, as in PostgreSQL: an unknown qualifier is
  42P01, an unknown column 42703, an ambiguous one 42702 (`lower/frontend.ts`). A negative frame
  offset is 22013 and a NULL one 22004.
- Transaction modes come from the parser's AST in written order; `modes` holds what applies (the
  last of each kind). `BEGIN READ ONLY` lowers with `readOnly: true` (R11.2); `SET TRANSACTION …`
  lowers to `transactionControl` action `setTransaction` with its `modes`, for the owner to apply.
  SERIALIZABLE and SET SESSION CHARACTERISTICS are refused (0A000).
- Declared numeric widths and VARCHAR(n) / CHAR(n) widths come from the parser only under
  `compileWithDeclaredTypes` (engine/query.ts), so the current engine compiles exactly what it
  did (C5, R17.4).
- Types follow PostgreSQL: integer constants are int4 → int8 → numeric by magnitude, string
  constants and placeholders take the type of their context, overloads resolve against
  `ir/functions.ts` with implicit casts, and values written to a column get assignment casts.
- A unique key fixed by `=` constants (or one `IN` list) becomes `keyLookup`: a point read. A
  constant that cannot be a key of the column's type keeps the scan.
- int8 renders `bigint` only when an operand does (R17.1); SUM(int8) is `decimal(null, 0)`
  rendered `bigint` (R17.2).
- Aggregates are typed by `ir/functions.ts` `AGGREGATES` (R19), the table the plan checker
  reads too: PostgreSQL's overloads, with an argument cast implicitly to the one overload it
  reaches (`sum(time)` is `sum(interval)`), and no `min` / `max` over bool, json, jsonb or uuid
  (42883). AVG of integers and floats is double precision (C11).
- Joins are ordered by cost (`from.ts`): every left-deep connected order up to 8 relations,
  greedy above, with the smaller estimate as the hash build side. Outer, semi and anti joins keep
  their place; a WHERE conjunct over one source is pushed into it unless an outer join
  null-supplies it.
- A range on an ordered index under byte-order collation becomes `indexRange` when it reads at
  most 5% of the table by the estimate (`access-path.ts`), else a scan with a filter.
- Subqueries: the decorrelator rewrites correlated IN / EXISTS / scalar / quantified subqueries
  into joins first. What reaches lowering as a subquery: `x [NOT] IN (simple subquery)` is a mark
  join (the subquery's WHERE in `filter`, R20.5); anything else correlated is an `apply` with
  correlated values in parameter slots (rare from SQL); uncorrelated scalar and EXISTS subqueries
  where no join can attach are init plans, run lazily on the first read of their slot (R24.1).
  The front end refuses correlated subqueries in HAVING, in grouped or nested select lists, and
  with GROUP BY / HAVING inside them, and a FULL JOIN after another join or under a window
  (INVENTORY.md "Still needed" 4).
- Set operations resolve each pair's column types as PostgreSQL does: an unknown-typed constant
  (NULL, a string literal) takes the other arm's type, and two unknowns are text.
- A FULL JOIN with no hash-joinable condition lowers, though PostgreSQL refuses it (0A000): an
  intended extension (R24.2), checked against PostgreSQL's LEFT JOIN UNION ALL anti-join rewrite.
- Windows, set operations, recursive CTEs and GENERATE_SERIES, UPDATE … FROM, DELETE … USING and
  MERGE lower too. Still refused (`UnsupportedPlanError`, stage "lower"): ordered-set and JSON
  aggregates, JSON / array / regex functions, batch UPDATE / DELETE (`batchWrite`), LATERAL the
  decorrelator cannot remove, full-text search. `INVENTORY.md` has every construct's status.
- `testing/lowering-differential.test.ts` runs DDL, seed rows, queries (joins, subqueries,
  windows, set operations, recursion included), writes, RETURNING and failing writes through the
  current engine and through lowering + the IR reference interpreter (`testing/ir-reference.ts`),
  and requires identical results; `testing/lowering-pglite.test.ts` and
  `testing/lowering-pglite-m3.test.ts` do the same against PostgreSQL (PGlite), the authority,
  through `testing/pglite-oracle.ts`, and `testing/lowering-pglite-review.test.ts` holds review
  B6-p2b's cases (lazy init plans, set-operation types, FULL JOIN extension, window frames,
  decimal casts, RANGE offsets at NULL boundaries and past 2^53, over exact NUMERIC, float
  infinities and NaN, aggregate signatures over every column type). The reference interpreter
  keeps NUMERIC exact (`testing/reference-decimal.ts`) and RANGE frames follow PostgreSQL's
  in_range functions. `testing/ir-check.ts` checks every
  plan they run: every declared slot filled by exactly one producer that dominates its readers
  (placeholders, the clock, init plans without cycles, apply bindings within their apply), LIMIT /
  OFFSET bounds, positions on both sides, aggregate FILTER / ORDER BY and signatures (every
  aggregate an overload of `AGGREGATES`, no fallback), window
  signatures and frames, recursion widths and work tables (`ir-check.test.ts`).
- Lowering cost (`scripts/lowering-bench.mts`, warm, median on a loaded host): a point read
  ~160 µs, a 3-way join ~350 µs, a 6- to 9-way chain 530–840 µs.
- Under `compileWithDeclaredTypes` the front end keeps constants typed (no folding with the
  current engine's numbers), keeps INSERT values as expressions, marks TRUNCATE and leaves bare
  ON CONFLICT names unqualified; lowering folds integer constants after overload resolution.

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

The terminal export experiment can also request `TableScan.transferOutputs`. Unfiltered group
output without locators, deletions or missing columns then decodes into fresh, tight, dedicated
buffers. `"dictionary"` retains dictionary codes and keeps the dictionary charge through output
release; `true` flattens text to UTF-8 bytes as an alternative diagnostic. Only fresh output buffers are marked in
`Transfer.detachableBuffers`. Cached pages, dictionaries, read slabs and reusable output pools
are never detachable. Four batch credits and a four-MiB threshold (plus one batch) bound retained
output, and loan-backed output must be released before the next step. Charges last through
client release, including after detachment or source abort. Terminal scans can provide
`outputBudget`, so dedicated buffers are charged to the result grant from creation. The bench
adopts only buffers marked with that exact result-budget identity; other handoffs copy. Output
reservation waits for retained credits, then shrinks on refusal. A separate output budget that
refuses even one row reports an error; the experiment does not borrow a second grant's loan. Other scan paths keep borrowing.

`MINNOW_EXPORT_SCAN_OWNED_COMPARE=1 npm run benchmark:export` compares borrowed dictionary/copy
output with dictionary-preserving owned decode/adopt output in five interleaved cold attempts. Both use real
TableScan, ResultChannel, owner transfer policy and MessagePort credits through the labelled
bench-only frame adapter. `npx vite-node scripts/scan-owned-bench.mts` isolates scan decode and
frame assembly over memory-backed input, using the real governor and separate query/result grants (setup and warmup cleanup excluded; process/thread CPU
and wall time). It includes output allocations, frame accounting and real in-process transfers,
with four frames held; it excludes MessagePort IPC. `--profile` records one diagnostic pass per
variant, never a performance comparison.
The component benchmark includes borrowed, dictionary-preserving owned, and all-flat owned
variants in five interleaved rounds. The all-flat diagnostic changes text policy and can change
frame counts and wire bytes. Dictionary-preserving ownership still copies dictionary deltas and
capped text fallback in the bench adapter; fixed-width values and emitted codes can transfer.
Bench frame-boundary memory snapshots report charges, including idle pool, active read/descriptor
remainder, decoded output and live frames. Frames overlap the source/result allocation counters;
independent maxima must never be added, and these counters do not partition physical RSS.

### IndexedDB log queue ownership

Log append snapshots and coalescing tails are charged to the backend's governor registration
before allocation. Their charges survive queued and in-flight transactions, even after append
resolves or a tail is replaced, and are returned only after the final reference settles.
Backend-owned log buffers total at most 4 MiB across all batches. At that bound or when a copy
charge cannot fit, append borrows the caller's input and waits for its transaction to settle.
The caller must keep that input immutable and charged until resolution. No size refusal or
uncharged fallback copy is introduced. OPFS writes synchronously and retains no append copy.

The keeper reserves its own input-copy/sort workspace, decoded page objects, bounded output
buffer, encoded page and result metadata through `ByteBudget` before allocating them. It reads
one touched source page and writes one output page at a time. Prepared metadata stays charged
across publication; abort/failure returns it, commit replaces the keeper's retained reservation,
and close releases that reservation. List readers keep decoded objects charged across generator
suspension and release them when the consumer finishes or stops. Startup carries a single page
cursor across read batches, so traversing the root’s page references is linear in page count.

### R25 startup measurement

`MINNOW_NEXT_STORAGE_REOPEN=1` enables the Chromium storage reopen lane in
`browser-tests/next-storage.spec.ts`: 100k and 1M real, empty-column group roots, one extent per
group, published in batches of 16,384 through `ExtentListKeeper`. Each of three reopens uses a
fresh worker over the disk-backed profile. It reports backend setup, fence, session/index loading, extent-space
startup and list-cache bytes read (including coalescing gaps). Root byte counts are separate;
IndexedDB opens data/log byte spaces with last-record boundary lookups and selects persisted
record entries lazily for the ranges the lists, log replay and later operations need. Whole-store
orphan/missing/key/order checks run in audit and test-build open, in bounded 4,096-entry pages
with three bulk requests issued together. Their O(records) cost is outside ordinary open. OS caches
remain warm. This synthetic metadata stress lane reports storage startup. Gate A acceptance uses an actual
S5/W5M load and owner-ready reopening, including write-path recovery and composition.

The Linux Chromium lane also samples `/proc` CPU ticks for this profile's DedicatedWorker
threads and complete browser process tree. Samples bracket reopen after worker-ready and before
cleanup; they include RPC handling, with the host clock's tick resolution (normally 10 ms).
Build, module boot and cleanup are excluded. Data-read bytes count coalesced list reads; root
payload bytes are separate. IndexedDB's native metadata/range lookup time is included, but its
opaque on-disk byte count is unavailable. The fixture uses a real zero-column catalog,
group roots, directory pages and table statistics, not a full SQL workload.

R25 preparation and startup use the worker-wide R23.5 task clock reexported by
`exec/pipeline.ts` (implemented in `task.ts` so storage does not depend on the executor).
No operation entry or resolved read restarts it. Bounded loops check the deadline, yield a real
task when due, and check cancellation on resume. Delta sorting uses an in-place sliced heapsort. Normal listed-root startup stream-merges both
ordered roots directly into the free map, decoding one page per root at a time. Scattered list-page
allocations use seven bounded radix passes over page references, so reconstruction is O(extents +
list pages), with no extent Maps or comparison sort. Retained free-run arrays and retirement
state reserve conservative capacity (including growth overlap) before allocating. Startup
transfers that charge to a non-evictable lifetime handle, released at space close. Capacity
charges remain at their high-water mark after entries are deleted. Audits serialize with
publication so their roots and allocator belong to one generation. Unordered reachability callbacks and full
audit/test-open validation retain their separate collection/sort path. All paths cooperate with
the shared clock and cancellation.
Timer/cancellation regressions include cached reads, immediate writes and two preparations
sharing one task's elapsed time.

`MINNOW_OWNER_REOPEN=1` enables `browser-tests/next-storage-owner-reopen.spec.ts`, the
actual S5/W5M owner-ready lane in Chromium over OPFS and IndexedDB. It loads the canonical
commerce scale 1,667 / wide 5M × 200-column generators (three wide dimensions included), in
50,000-row transactions through the real write path, using its default maintenance tuning and
a 256 MiB governor. Each reopen starts a fresh worker, opens the real backend and extent space,
recovers `WritePathImpl`, sweeps scratch, and starts the real owner host with no broker.
The executor is a stand-in and is unused before `minnow:ready`; the harness asserts that.
The owner factory acquires handles in `openFence` and runs session/space/write-path recovery
in `open`, as the host contract requires. The default fence deadline is unchanged; the ready
timer includes both acquisition and recovery.

Three wall-only samples report backend/ByteStore loading, extent space, write-path recovery,
and host overhead separately, plus page-observed host ready. Three separate CPU-bracketed
samples report worker and browser-tree CPU ticks for those phases without charging the sampler's
barrier wait to phase wall time. Returned OPFS bytes and IndexedDB cloned byte payloads are
counted per phase, with numeric index/meta reads separate; native IndexedDB disk bytes are opaque.
Build, module boot, metadata collection and cleanup are outside owner-ready. Disk caches are not dropped; residency is not verified.
The reported first `SELECT count(*)` uses real lowering, owner/client protocol and frame delivery,
but a statistics-only executor stand-in: it reads the real write path's row count, not table pages.
Client connection/metadata overhead is reported separately. This is not integrated query execution.

`MINNOW_OWNER_REOPEN_CACHE` can retain profiles outside Playwright's output directory;
`MINNOW_OWNER_REOPEN_REUSE=1` explicitly reuses completed loads marked by the harness, so a
later clock rebase can measure the same stored roots. The report includes actual group, extent,
list-page and native IndexedDB record counts rather than deriving them from row counts.
`MINNOW_OWNER_REOPEN_SMOKE=1` caps each table at 1,000 rows to verify the harness only; those
samples are labelled and never count as Gate A measurements.

The host-ready endpoint is captured in the host’s actual ready callback. Phase counters capture
before awaiting a CPU checkpoint, so reads arriving during that wait are kept in the next
interval. Wall-only checkpoints are synchronous. The owner composition opens with initial maintenance
held, posts ready, and resumes initial flush/merge/recycle work only after the page acknowledges
receiving that message. The work is deferred, not removed: count setup and the first count may
overlap it. The report includes completed counters and in-flight state at ready and after count.
`MINNOW_OWNER_TRACE=1` records native file ranges and logical read call stacks with maintenance
state for diagnosis only; turn it off for timing. `MINNOW_OWNER_DEFER_MAINTENANCE=0` enables the
pre-gate diagnostic comparison without changing the standalone write path's default policy.

The harness returns its unused extent-space startup reservation before opening the write path.
`MINNOW_OWNER_REOPEN_LOAD_MIB` selects the load governor only; reopens remain at 256 MiB.
`MINNOW_OWNER_REOPEN_RESUME=1` explicitly continues an interrupted W5M load after recovery,
quiescence, physical/list audit, schema and complete-batch-prefix checks, and first/last key
probes. It preserves the canonical 50,000-row transactions and generator values. The report
records the load budget and resumed row count. A failed 256 MiB load is a separate result and
is not treated as successful completion within that cap when loading continues at 512 MiB.

R25 delta preparation reserves its copy workspace before reading inputs and checks the shared
clock and cancellation every 128 entries while copying. The write-path adapter passes its
owned vectors directly; it does not make unsliced integration copies.

A flush revalidates its captured schema and index identifiers under the publication lock.
If concurrent DDL changed them while it encoded groups, it aborts the provisional writer
and retries against the current catalog; the published root must carry its catalog’s indexes.

Quiet-table checkpoints and first table roots stay provisional while a publication is prepared.
Only adoption of the durable root installs them, so another flush cannot settle an index build
from a checkpoint the recycler has not published.

Resuming deferred initial maintenance joins the existing flush chain. Close and step-down still
wait for every accepted flush and merge, including work started before the ready acknowledgement.

### Row-lock admission (B6/B5)

Every lowered statement carries `rowLocking`: `none`, `keyShare`, `share`, `noKeyUpdate`, or
`update`, strongest last. The owner must refuse a query whose intent is not `none` in READ ONLY
with 25006, before executing it, including a cached or parameterized statement. Lowering does
not acquire locks or enforce transaction access mode. DML/DDL write admission stays separate.
The shared front end resolves targets through FROM subqueries but never pushes an outer clause
into a CTE. A referenced CTE's own clause and a view's own clause count, including references from
CTEs not consumed by the final SELECT. Only zero-reference CTEs in that query scope are
omitted; declarations inside a discarded outer CTE do not leak into the caller.
View bodies are included after catalog expansion. The shipped engine uses the same target and
strength helpers. Metadata is retained even for a point read; the owner must check it before
its synchronous fast path.

### Transaction mode items (B6/B5)

`LoweredTransactionControl.modeItems` keeps the written order and duplicates for BEGIN,
START TRANSACTION and SET TRANSACTION. Access items have `value: readOnly | readWrite`,
isolation items have the named level, and deferrable items have a boolean. Other transaction
controls carry an empty list. `modes` remains the final value of each kind; the owner must
validate every item in order before using it. In a savepoint that starts READ WRITE,
`SET TRANSACTION READ ONLY, READ WRITE` fails with 25001, while `READ WRITE, READ ONLY`
succeeds. If it already starts READ ONLY, either list fails. This is metadata for B5;
it does not wire owner enforcement.

The parser records CTE reference counts before execution pruning. Referenced bodies remain
as `CompiledQuery.rowLockingCtes`, so catalog expansion includes locking views in a CTE
referenced only by an unused referrer. This admission-only data survives ordinary clones and
worker transport; it is not lowered as an execution input. The strongest intent still wins
when the final SELECT has a weaker clause. Nested query scopes retain their own references.

The retained admission declaration includes both members of a recursive CTE. A step's
locking subquery or view contributes even when the only external reference is in an
unused CTE. Self-reference alone does not count. This is lowering metadata; the owner
must apply it before execution. The shipped SQL enforcement is inherited from Stream A.

### Real-executor M3 coverage

The B7 generated campaigns run joins and windows through real writes, storage and the executor
against PostgreSQL and the shipped engine. These complement B6's broader lowering/reference
campaigns; lowering support does not imply every emitted operation has an executor binding.
The frozen catalogue records typed executor refusals separately from matching executions.
