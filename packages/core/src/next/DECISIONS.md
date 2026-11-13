# Public-contract and format decisions

Status: **decided** on 2026-11-05, by the lead's rulings on review A1-1 and on the browser storage
probe. Each item names the code that freezes it.

The public API changes at the reset. Stored formats may not change after the freeze without
upgrade coverage (AGENTS.md).

## D1. TIMESTAMP and TIMESTAMPTZ values: ISO 8601 text

**Storage** (`temporal.ts`):

- int64 µs since 2000-01-01T00:00:00Z, PostgreSQL's epoch, so PostgreSQL's whole range is exact:
  4714-11-24 BC to 294276-12-31.
- `-infinity` and `infinity` are INT64 min and max.
- Values with either sentinel never use the f64 fast path. That path covers about 1715–2285.

**Row value:**

| Type        | Example                       |
| ----------- | ----------------------------- |
| TIMESTAMPTZ | `2024-01-02T03:04:05.123456Z` |
| TIMESTAMP   | `2024-01-02T03:04:05.123456`  |

- Always exactly six fraction digits.
- Years are astronomical (`0000` is 1 BC). Years outside 0000–9999 use ISO's expanded form:
  `+012345-…`, `-004713-…`.
- Infinities are the text `infinity` and `-infinity`.
- TIMESTAMPTZ is an instant shown in UTC; the session time zone is always UTC. TIMESTAMP is a
  wall-clock time with no zone. A cast between them keeps the stored µs.
- Text in years 0000–9999 sorts as the values do.

**Inputs:**

- ISO text or PostgreSQL text (`2024-01-02 03:04:05.5+00`).
- JS `Date`, exact to the millisecond.

**Helpers:** `parseTimestamp`, `formatTimestamp`, `timestampFromDate` and
`unixMillisFromTimestamp`. The docs point to `Temporal.Instant.from` and
`Temporal.PlainDateTime.from` for the text.

**Columnar accessor:** a discriminated descriptor, never a bare typed-array union:

```ts
type ColumnView =
  | { kind: "timestamp"; zoned: boolean; unit: "microseconds"; epoch: "2000-01-01T00:00:00Z";
      repr: "safe"; values: Float64Array; validity: Uint8Array | null }
  | { kind: "timestamp"; zoned: boolean; unit: "microseconds"; epoch: "2000-01-01T00:00:00Z";
      repr: "int64"; values: BigInt64Array; validity: Uint8Array | null }
  | { kind: "date"; unit: "days"; epoch: "2000-01-01"; values: Int32Array; validity: Uint8Array | null }
  | { kind: "int64"; repr: "safe" | "int64"; values: Float64Array | BigInt64Array; … }
  | …;
```

When a timestamp column holds an infinity, its view is `repr: "int64"`.

**Changes from today:** TIMESTAMP was a `Date`, and `.123456` was silently cut to `.123`. That loss
is gone. It touches the changelog, `InferRow`, the Kysely and React typings, the docs, the agent
rules, and the harness `["datetime"]` encoding at cut-over.

## D2. BIGINT values: always `bigint`

- BIGINT/INT8 columns and int64 results are always `bigint`, with no `number | bigint` union.
  - `column.bigint()` infers `bigint`.
  - Parameters for int8 positions accept `bigint`, or a safe-integer `number`. An unsafe `number`
    is refused: it was already rounded.
- **SUM(bigint)** returns `bigint`, exactly (JS bigint has arbitrary precision; PostgreSQL returns
  NUMERIC).
- **COUNT** returns `number`. This is documented: counts are bounded by the rows the engine holds,
  far below 2^53.
- **SUM(int2/int4)** returns `number`, exact or `RangeError`.
  - PostgreSQL returns int8 here.
  - This can actually happen: about 4.2M INT4 values near the maximum pass 2^53. The error message
    suggests `SUM(x::bigint)`.
- **The columnar accessor** is the `int64` descriptor above: `repr: "safe"` gives a
  `Float64Array`, proven exact; otherwise a `BigInt64Array`.
- **The differential adapter** keeps the oracle's `number` until cut-over.

## Other decisions

| #   | Decision                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Frozen by                                                      |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| C1  | DATE is PostgreSQL text: `2024-02-29`, `0044-03-15 BC`, `10000-01-01` (5+ digit years as PostgreSQL prints them), `infinity` / `-infinity`. Stored as int32 days since 2000-01-01, finite range 4714-11-24 BC to 5874897-12-31; the columnar view is `Int32Array` days.                                                                                                                                                                                                                                                                                                                                                                                               | `temporal.ts`                                                  |
| C2  | NUMERIC is exact text, including `NaN`, `Infinity` and `-Infinity` (PostgreSQL 14+), which are stored in the `specials` side array. Coefficients are int64-family, then int128 limbs, then `bigint[]`. A vector has one uniform scale ≥ 0, or a scale per slot when values differ; that scale is PostgreSQL's display scale. Coefficients are never rescaled to a shared maximum, and kernels partition by scale (`exec/decimal-sum.ts`). A declared negative scale rounds on write.                                                                                                                                                                                  | `vector/vector.ts`, `vector/int128.ts`, `keys/key-encoding.ts` |
| C3  | TIME and INTERVAL are PostgreSQL text. INTERVAL equality and order use the normalized span (`1 mon` = `30 days`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `keys/key-encoding.ts`                                         |
| C4  | DATE and TIMESTAMP infinities are the int min/max sentinels, reserved now and excluded from the f64 proof.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `temporal.ts`                                                  |
| C5  | SMALLINT and INTEGER have PostgreSQL ranges. Declared widths are mandatory before lowering (`requireDeclaredWidths`); the front end must keep the declared type name, which it drops today.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `types.ts`                                                     |
| C6  | REAL is float32: `Float32Array` storage, computed in f64, with `Math.fround` at REAL type boundaries.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `types.ts`, `vector/vector.ts`                                 |
| C7  | The key encoding is byte-level and frozen, with golden vectors and generated order/equality/hash tests over every encodable type. Equality keys (persisted) and sort keys (query time) are separate. Persisted index keys are scalar only: indexes on arrays and JSON/JSONB are refused at DDL (`UnindexableTypeError`), and those types sort and group through semantic comparators (`exec/semantic.ts`: JSONB numbers compare numerically, `1` = `1.0`; arrays compare elements first). An enum index is equality-only (keyed by label, since inserting a label changes ranks), so range predicates on enums never use it; ORDER BY enum uses the declaration rank. | `keys/key-encoding.ts`, `exec/semantic.ts`                     |
| C8  | The default text order is byte order (code point order, PostgreSQL's C collation). Today it is JS UTF-16 order: `'￿' < '😀'` is false today and true from now on. An explicit ICU `COLLATE` sorts by query-wide ranks from `Intl.Collator` when the distinct values fit the grant, and otherwise by an external comparison sort. That rare path may decode to JS strings (the one documented exception, charged via `stringBytes`). Ties break by bytes, so peers and equality are byte equality (PostgreSQL's deterministic collations).                                                                                                                             | `vector/varlen.ts`, `ir/ir.ts`                                 |
| C9  | An array is stored as one reversible binary value per row (`vector/array-value.ts`): element tag, dimensions with lower bounds, a null bitmap, then each element in a lossless form (interval months, days and µs separately; NUMERIC with its display scale; text, bytes and enum as bytes). It is decoded before any operation and is never compared as bytes: ordering and equality are `compareArrays` / `arrayEqualityKey` on the decoded value (PostgreSQL's array_cmp: elements first, then element count, dimensions, lower bounds). Arrays cannot be indexed. Elements: every scalar type except JSON.                                                       | `vector/array-value.ts`, `exec/semantic.ts`                    |
| C10 | **Correction:** today's JSONB already canonicalizes. `encodeBoundedJson` with `canonical` sorts object keys with JS `sort()` (UTF-16 order) and re-serializes without whitespace; the parser keeps the last duplicate (`engine/sql-domains.ts` around lines 497 and 957). Today `'{"b":1,"aa":2,"a":3,"b":4}'::jsonb` gives `{"a":3,"aa":2,"b":4}`; PostgreSQL gives `{"a": 3, "b": 4, "aa": 2}` (keys by length, then bytes; `": "` and `", "` separators). The new core stores JSONB in PostgreSQL's canonical key order and renders PostgreSQL's text, so JSONB output changes in key order and spacing. JSON keeps its text verbatim.                             | format track                                                   |
| C11 | AVG of integer and float inputs returns float64, a documented deviation from PostgreSQL's NUMERIC; `AVG(x::numeric)` is exact. AVG of NUMERIC stays NUMERIC.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | lowering                                                       |
| C12 | Arbitrary-version reads (`options.version`) are dropped. A read pins one root plus that moment's memtable views for one query or read transaction. Pins live in memory, and a client's pins drop when its Web Lock is released. Space is reusable only when neither the current nor the previous durable root reaches it and no pin does (two generations).                                                                                                                                                                                                                                                                                                           | `format/manifest.ts`                                           |
| C13 | Replaced by the request ledger (review finding 5).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `format/ledger.ts`                                             |

## R25. Startup reads the roots' extent lists, not the group roots

- **What.** Each published root references a paged, offset-sorted list of the exact extents it
  reaches (`storage/extent-list.ts`): pages of up to 4,096 extents, each a blob, named in the
  root with its first offset and count. The list names every extent the root reaches except the
  ones its own pages live in, which the root names directly. Startup reads the current and
  previous roots' lists (`listedExtents` as `OpenSpaceOptions.reachable`) and never a group root;
  the audit, and the open in test builds (`verifyAtOpen`), compare them with the full walk
  (`OpenSpaceOptions.walk`), and a difference is `StoreCorruptionError`.
- **Kept incrementally.** The write path's extent-delta sink (`ExtentListKeeper`) rewrites only
  the pages a publication's delta touches, through the publication's own writer, shares the rest
  by reference, and retires replaced page extents in the same publication (the R8 hold applies,
  as to `released`). Prepared lists are provisional until `commit`, dropped on `abort`, and the
  keeper is rebuilt from the durable root's list at every open.
- **Stored format.** The root gains `extentList` (`format/manifest.ts`, manifest format 2; the
  codec checks the pages' order and counts). No supported writer of rebuild-core roots exists: `next/` is unexported, and the package
  excludes `dist/next/**` (`files` in `packages/core/package.json`, checked by
  `scripts/package-shape.test.ts`), so no released build wrote one. Roots written by development
  builds and tests before R25 are refused at open rather than upgraded; pre-reset data is refused
  by the approved policy. The first release that ships the rebuild core makes this format a
  supported one, and from then on AGENTS.md's upgrade rules apply to every change.

## Ownership protocol facts (for the ownership track)

These rulings follow the storage probe of 2026-11-05.

- **Acquisition.** The owner takes the Web Lock `minnow:<db>`, then opens its sync handles with
  bounded backoff, treating `NoModificationAllowedError` and `InvalidStateError` as busy
  (`storage/fence.ts` `acquireFenced`).
  - If the lock is stolen (`AbortError`), the owner closes its handles at once.
  - A frozen owner blocks takeover until it thaws or is discarded. A successor reports
    `OwnerUnresponsiveError`.
- **IndexedDB.** It is a second implementation of the same contract, built after OPFS. It is
  needed for Safari and Firefox private browsing, which have no OPFS.
  - Its fence is the Web Lock plus an epoch record checked inside every transaction.
  - It stores ArrayBuffers, never Blobs.
- **Client liveness.** A client holds its Web Lock only while it has snapshot pins or requests in
  flight. A permanent lock costs bfcache on Firefox, and Safari drops locks on bfcache.
  - Releasing the lock drops the client's pins.
  - A client returning from bfcache expects "snapshot gone" and registers again.
- **Step-down.** The owner steps down on `freeze` and `pagehide`, and on `hidden` only on mobile.
  - In `pagehide`, the owner's page flushes and terminates its owner worker, which matters for
    Safari bfcache.
  - The owner does not step down on every visibility change.
  - **Before terminating its worker, the owner stops starting storage work, lets what is in
    flight finish, closes its session and awaits `lease.release()`** (2026-11-06). WebKit
    (Playwright's 26.5) leaves IndexedDB blocked for the rest of the browser session when a worker
    is terminated with two independent chains of IndexedDB transactions in flight on its
    connection: 5 of 40 such kills in a plain-IndexedDB repro (no Minnow code), 0 of 25 in
    Chromium and in Firefox. A probe still blocked 120 s later. Closing the connection just
    before the kill does not prevent it (2 of 40); letting the in-flight transactions finish first
    does (0 of 40). The IndexedDB backend's `release()` does that part: it refuses new
    transactions, waits up to 500 ms for those in flight, aborts any still running and waits for
    their abort events (an aborted strict commit fails: not durable), closes the connection, and
    only then releases the lock, so the next owner never starts beside a live transaction; it
    reports how many transactions drained and how many it aborted. Every acquisition opens its own
    connection, so releasing a stolen lease never closes its successor's (review B1-p2h-1); a
    connection object handed to several acquisitions still has one chain (review B1-p2h-2). If an
    aborted transaction never reports its end within 1 s more, the release closes, releases the
    lock anyway and rejects with `ReleaseUnsettledError` (counts and the transactions' scopes); those
    transactions' operations reject with it too, so a strict commit among them is unknown, never
    acknowledged. Holding the lock forever would give no successor a chance; releasing it cannot
    promise the database progresses. The owner host decides what follows (B5: log, terminate,
    suggest a reload). Measured graceful step-down under the concurrent kill workload, 40 cycles each: waiting
    for in-flight operations median 88–153 ms, p95 200–481 ms; session close ≤ 33 ms; release
    ≤ 7 ms; the next owner recovered every time in WebKit, Chromium and Firefox. OPFS is the
    default wherever a worker has sync access handles; this matters on IndexedDB, WebKit's
    private-browsing fallback.
  - **A worker can die without a step-down** (the browser kills it at unload, a crash, OOM), so
    the IndexedDB backend also runs every transaction of its connection through one serialized
    chain (`serializeTransactions`, the default everywhere): in the plain repro one chain never
    wedged (0 of 45 kills, against 3 of 45 with two chains; spill on its own connection or in its
    own database also 0 of 45), and the abrupt-kill concurrent campaign in WebKit, 2–5 workload
    operations in their windows at every kill (windows, not platform requests: with one chain at
    most one transaction is open), wedged 0 times in 100 kills (before: within 5–17). Cost
    under the concurrent workload, medians of 8 rounds: Chromium 81 vs. 92 operations/s, Firefox
    71 vs. 80 (about 11%, within a noisy spread); single-chain bulk writes and commits show no
    consistent difference.
- **Files.**
  - A few long-lived files (log, data, two root slots, spill, epoch), opened at acquisition and kept
    open.
  - Space is reused inside them; only a free tail is truncated.
  - Never create or delete files per object: quota is not returned on truncate (Chromium, WebKit),
    and WebKit's IndexedDB cannot reuse space freed by deleting OPFS files.
- **WebKit.** A busy tab delays its worker's async OPFS calls and incoming messages; reads and
  writes on an open sync handle are not delayed. The owner keeps its hot path on open handles.
- **Targets.**
  - Cross-tab round trip: ~55–65 µs Chromium, ~140 µs Firefox, ~280 µs WebKit (p95 520 µs).
  - `flush()`: one real disk flush, 3.4–4.7 ms on every engine. Group commit stays essential.

## Where these contracts differ from PROPOSAL.md

1. **IndexedDB is required.** The gate fails because Safari and Firefox private browsing have no
   OPFS. So the store contract is async and its fence is pluggable.
2. **Extents in one data space, not per-object pages or files.** Space is reused in place
   (probe). Reads are batched ranges that the store coalesces.
3. **Commit frames carry delete keys for every index.** Insert keys are derived from the inserted
   columns.
4. **The log is recycled only when it is covered.** The root that switches salt must checkpoint
   global state and every table through the old log's end. Recycling on every memtable flush is
   unsafe with several tables sharing one log.
5. **DDL and sequences are logged.** A transaction is an ordered list of effects, replayed in
   order against a global checkpoint. A checkpoint that covers a schema change of a table also
   covers that table's rows through it (`format/replay.ts`). The proposal publishes metadata only
   through roots.
6. **Exactly-once uses a bounded ledger of outcomes.** A last-sequence watermark is not enough
   (`format/ledger.ts`).
7. **Lying flush.** The proposal says every acknowledged group but the newest is protected. That
   holds only for a cleanly truncated log body; such loss is detected through the acknowledged
   boundary and the root's coverage.
   - A write-back cache lost in order can silently drop a suffix of groups. No format can detect
     that without a durable witness outside the device.
   - The log never reorders or invents a group, and a rolled-back root is detected.
   - `format/recovery-sweep.test.ts` ("lying flush") demonstrates this.
8. **ICU collations use ranks, not sort-key bytes.** JavaScript has no sort-key API (C8).

## ROUND overload at cutover

R19 follows PostgreSQL's overload table: `round(float8)`, `round(numeric)`, or `round(numeric, int4)`.
The shipped engine additionally accepts `round(float8, int4)`; this is a legacy extension to
remove when the rebuilt core becomes the default, rather than a new intentional extension.
Public retail examples and the analytics S_q7/guide-4/guide-5 cells cast the input to NUMERIC.
The catalogue checks original SQL gives PG 42883, records legacy acceptance, then compares the
corrected SQL through PostgreSQL, the shipped engine and the real rebuild executor. Its original
frozen fingerprint is still checked after reversing only those three SQL corrections.
Authority: https://www.postgresql.org/docs/18/functions-math.html.

## Durable format freeze and mixed-version fencing

The first released1.0 writer starts the supported stored-format chain. Pre-release next/ roots
now interpret the11 reserved high sequence bits in the16-byte outer header as biased
minimum-reader/manifest selectors. Zero selects reader1/manifest2, preserving current bytes. Both root slots must pass their checksummed
version barrier before native file creation or any epoch update. Semantic manifest replay still
follows ownership, but a future nested format must raise the outer requirement when publishing.
Unknown intact formats refuse, never recover as empty. `durable-format.ts` is the canonical
registry; raw inner codecs inherit explicit versioned parents. Snapshot constants are reserved
for the design only. The generated decoder sweep and zero-write native histories enforce this.

## Long index keys: unpublished format 3 (B5, 2026-11-12)

Manifest version 3 explicitly discriminates inline and overflow-capable runs. Unknown/newer
encodings are typed refusals. The rebuild has never shipped: there are no supported older
next/ writers to upgrade, and no next/-to-next/ migration or new frozen predecessor fixtures
are required. Existing development fixtures test refusal, not compatibility. Short-key data
blocks retain their exact bytes. The 1.0 rebuild starts a new chain and refuses 0.x data without mutation, rather than
converting it. Subsequent supported 1.0 formats require ordinary-open upgrade evidence.
