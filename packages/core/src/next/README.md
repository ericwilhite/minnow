# `next/` — the rebuild core (unreleased)

The storage core and executor that replace the current engine (plan: `PROPOSAL.md`, "Architecture"
and "Wide and large analytics"). Nothing here ships: no package entry point imports it and
`package.json` `files` excludes `dist/next/**`; `tree-shaking.test.ts` and
`scripts/package-shape.test.ts` prove both. Open questions and public-contract decisions are in
[`DECISIONS.md`](./DECISIONS.md).

## Module map

| Module             | Owner track                      | What it is                                                                        |
| ------------------ | -------------------------------- | --------------------------------------------------------------------------------- |
| `types.ts`         | contracts                        | Physical types; the one SQL → physical mapping (`physicalTypeOf`)                 |
| `hash/`            | contracts                        | xxHash32, the only checksum                                                       |
| `vector/`          | contracts → executor             | Batches, vectors, validity, selections, exact int64, UTF-8 data                   |
| `storage/`         | storage                          | `DurableStore` contract; memory store; fault store; root slots; conformance cases |
| `format/`          | format, write path               | Log frames, header slots, recovery scan; manifest root and page metadata types    |
| `spill/`           | executor (OPFS service: storage) | Spill service contract; memory double                                             |
| `memory/`          | executor                         | Memory governor contract                                                          |
| `ir/`              | lowering, executor               | Operator IR (`ir.ts`); the front-end boundary (`lowering.ts`)                     |
| `testing/`         | all                              | Differential-harness adapter for a partial new core                               |
| `exec/` (to come)  | executor                         | Kernels and operators over `ir/`                                                  |
| `write/` (to come) | write path + key index           | Group commit, memtables, key index runs, maintenance                              |
| `lower/` (to come) | planner lowering                 | `Lowerer`, cost model, statistics                                                 |
| `owner/` (to come) | ownership                        | Web Lock election, client protocol, composition root                              |

## Dependency rules

Enforced by `dependency-rules.test.ts`.

- Nothing outside `next/` imports `next/` until cut-over.
- Inside `next/`, each module imports only the layers below it:

  ```text
  types ← vector ← ir ← lower
  hash ← storage ← format ← write
  hash ← spill;  memory
  exec ← types, hash, vector, ir, memory, spill
  write ← types, hash, vector, storage, format, memory
  owner ← everything;  testing ← everything
  ```

- Non-test sources outside the bridge modules (`lower/`, `owner/`, `testing/`) use old code only
  through `import type`. `lower/` may call the front end (parser, catalog rewrites, optimizer);
  the executor never does.

## Contracts and their invariants

**Physical types** (`types.ts`). Every value has an exact physical type: int32 (SMALLINT,
INTEGER), int64, float64, decimal (scaled coefficient + display scale), date (int32 days), time
and timestamp (int64 µs), interval (months, days, µs), text/json/enum/array (bytes), uuid (16
bytes). No tagged strings. Collation belongs to expressions, never to stored types.

**Vectors** (`vector/vector.ts`). Batches of ≤2,048 rows; flat, constant and dictionary vectors;
validity bitmaps (bit set = valid; garbage under invalid slots never raises); Uint16 selections.
int64 is exact: Float64Array only when every slot is proven within ±(2^53 − 1), else
BigInt64Array. Text stays UTF-8 until output. A consumer borrows a batch for one `push`; buffers
are allocated once per operator and recycled; dictionaries are immutable for the query.

**Storage** (`storage/contract.ts`). Async, so OPFS and IndexedDB both implement it. Atomic,
durable `publish(root, packs)`; immutable packs read by byte range (batched, coalescible);
an append-only log with a flush barrier and two header slots; unflushed bytes are undetermined
after a crash; reclaim may be undone by a crash; a newer session fences every older one by
epoch. `conformance.ts` is the shared test; `fault-store.ts` produces every post-crash state
the contract allows.

**Log and manifest** (`format/`). Frames are checksum-chained (a stale frame behind a
rewritten one never validates), LSNs are contiguous, and the newest header acknowledges every
frame before `ackEnd`: losing one is corruption, never a silently shorter log. The root is
O(tables); everything below it is O(groups × columns), never O(rows); every reference carries
the checksum of what it names (verify once, at read). Recovery replays a frame iff its salt is the
root's and its LSN is past the table's `flushedThroughLsn`.

**Spill** (`spill/contract.ts`). Typed, checksummed pages in engine temp files. Never named by
the root, catalog or log; the temp directory is deleted on open; no per-query caps.

**Memory governor** (`memory/governor.ts`). One cap. Grants grow or are refused; refused
spillable operators spill, never fail for size. Minimum reservations always succeed. The page
cache is evicted before any operator spills. Accounted in ArrayBuffer capacity bytes.

**IR** (`ir/`). Fully typed, positional, plain data, bound once with parameter slots. Lowering
starts from the front end's optimized `CompiledQuery`; `lowering.ts` lists the passes kept and
the code it replaces.

**Differential adapter** (`testing/differential-adapter.ts`). A track implements `NextCore`
(any subset) and runs the frozen harness against the current engine. Values convert to the
oracle's representation; known divergences fail loudly.
