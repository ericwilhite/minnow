# `next/`: the rebuild core (unreleased)

This is the storage core and executor that replace the current engine. The plan is in `PROPOSAL.md`,
sections "Architecture" and "Wide and large analytics".

Nothing here ships yet:

- No package entry point imports it.
- `package.json` `files` excludes `dist/next/**`.
- `tree-shaking.test.ts` and `scripts/package-shape.test.ts` prove both.

Public-contract decisions are in [`DECISIONS.md`](./DECISIONS.md).

## Module map

| Module             | Owner track                      | What it is                                                                                                                                                                                   |
| ------------------ | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `types.ts`         | contracts                        | Physical types; the one SQL → physical mapping; declared widths are mandatory                                                                                                                |
| `temporal.ts`      | contracts                        | DATE/TIMESTAMP on PostgreSQL's 2000 epoch; infinities; ISO text                                                                                                                              |
| `hash/`            | contracts                        | xxHash32, the only checksum                                                                                                                                                                  |
| `vector/`          | contracts → executor             | Batches (byte-bounded), vectors, validity, exact int64, int128 decimals, UTF-8 builder under a grant, locators                                                                               |
| `storage/`         | storage                          | Backend contract (lock + async fence open, async mutations), file store, simulated disk with crash model, the storage kit (conformance + durability sweep)                                   |
| `keys/`            | contracts → executor, key index  | Memcomparable key encoding: reusable writer, batched encoder over vectors, indexable-type rules                                                                                              |
| `format/`          | format, write path               | Log frames and multipart transactions, header slots, in-memory and incremental recovery scans, replay rules, request ledger, manifest types                                                  |
| `spill/`           | executor (OPFS service: storage) | Spill contract (one long-lived file); in-memory double                                                                                                                                       |
| `memory/`          | executor                         | Memory governor: admission, progress loans, held vs allocated bytes (reference implementation)                                                                                               |
| `exec/`            | executor                         | Push protocol and driver (`pipeline.ts`), row fetcher (`fetch.ts`), array/JSONB comparators (`semantic.ts`), scale-partitioned decimal SUM (`decimal-sum.ts`); kernels and operators to come |
| `ir/`              | lowering, executor               | Operator IR (`ir.ts`); the front-end boundary (`lowering.ts`)                                                                                                                                |
| `testing/`         | all                              | Differential-harness adapter for a partial new core                                                                                                                                          |
| `write/` (to come) | write path + key index           | Group commit, memtables, key index runs, maintenance                                                                                                                                         |
| `lower/` (to come) | planner lowering                 | `Lowerer`, cost model, statistics                                                                                                                                                            |
| `owner/` (to come) | ownership                        | Web Lock election, client protocol, step-down, composition root                                                                                                                              |

## Dependency rules

`dependency-rules.test.ts` enforces these.

- Nothing outside `next/` imports `next/` until cut-over.
- Inside `next/`, each module imports only the layers below it:

  ```text
  types/temporal ← vector ← memory ← exec
  types, hash, vector ← keys ← exec, write
  hash ← storage ← format ← write
  hash ← spill ← exec;  vector ← ir ← lower
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
- The store is four long-lived files: log, data space, and two root slots. Extents are reused in
  place inside the data space.
- `publish` is atomic and durable: it flushes data, then writes the inactive root slot.
- The log has a flush barrier and two header slots.
- The simulated disk crashes at any physical step. Every write after the last flush may land
  whole, torn or not at all, and may land out of order. It models honest and lying flush.
- A backend is accepted by the **storage kit** (`kit.ts`): the conformance cases together with a
  physical durability sweep. A store that skips its data flush or its log flush fails the kit.

### Log, replay and ledger (`format/`)

- Frames are checksum-chained. A transaction is one or more consecutive frames, numbered by part
  index.
- The acknowledged boundary must match a transaction end exactly: offset, chain and LSN.
- Incomplete transactions are dropped, and recovery truncates the log before appending again.
- Recovery scans incrementally: one frame in memory at a time, transactions handed to a sink that
  stages their parts.
- A transaction is an ordered list of effects: DDL, sequence reservations, row changes and a
  request outcome, replayed in order. A table created by a transaction replays that
  transaction's rows.
- The root checkpoints global state at `globalLsn` and each table's rows at its own
  `flushedThroughLsn`.
- A checkpoint that covers a schema change of a table also covers that table's rows through it,
  and replay checks schema versions.
- The log must reach every checkpoint the root claims.
- A rolled-back root is detected through the log header's `rootSequence`.
- Requests are exactly-once through a bounded per-client ledger of outcomes, with expiry enforced
  by the receiver.

### Key encoding (`keys/key-encoding.ts`)

- Memcomparable: byte order gives SQL order, and byte equality gives SQL equality.
- Equality keys (persisted) and sort keys (query time) are separate.
- Persisted index keys are scalar types only. Indexes on arrays and JSON are refused at DDL; those
  types sort and group through the semantic comparators in `exec/semantic.ts`.
- An enum index is equality-only (its key is the label). ORDER BY enum uses the declaration rank.
- Operators encode a whole batch straight from vectors into a reused buffer (`encodeKeyBatch`).
- Frozen by golden vectors and generated tests over every encodable type.

### Manifest (`format/manifest.ts`)

- The root is O(tables + ledger). Nothing is O(rows).
- Each column chunk names its data extent.
- Every reference carries the checksum of what it names.
- Space is free when neither the durable root nor any pinned root reaches it.
- Snapshot pins live in memory and are dropped when their client's lock is released.

### Spill (`spill/`)

- One long-lived file, with space reused across queries.
- Logically emptied on open, never named by the root, catalog or log.
- No per-query caps.

### Memory (`memory/governor.ts`)

- One cap, which `usedBytes` never exceeds.
- Pipelines are admitted with plan-bounded minimums. Admission waits; it is never refused.
- A grant _holds_ bytes, and its operator _allocates_ buffers out of them, so a non-spillable
  operator works inside its admitted minimum.
- Growth is charged before it happens, including the overlap while a buffer is copied.
- The page cache is evicted before anything spills.
- Progress loans are scheduled before admissions and may use the reserve, so a queued admission
  never blocks an admitted pipeline. A grant repays its loan before borrowing again.
- The drain is not reentrant, a closed grant is inert, and abort listeners are removed.
- The one size error is a single value larger than the reserve.

### Pipeline (`exec/pipeline.ts`)

- Kernels are synchronous.
- `push` returns `ready` or `blocked`, and a blocked batch stays borrowed until `ready()`.
  Operators resume partly consumed batches themselves.
- `finish` is async.
- `abort` releases grants, spill files and transferred batches.
- The driver adds no cancellation machinery without a signal, and with one each wait removes its
  listener, so a long stream retains nothing per batch.
- Each edge holds at most one in-flight batch, plus whatever credits the producer has handed out
  as transfers.

### IR (`ir/`)

- Fully typed, positional and plain data. It is bound once, with parameter slots.
- Late materialization uses `scan.emitLocator` and `fetch`, executed through the `RowFetcher`
  contract (`exec/fetch.ts`).
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
