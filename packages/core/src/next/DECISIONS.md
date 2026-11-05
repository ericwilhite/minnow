# Decisions to make before the format freezes

Status: **proposed** (2026-11-05). Each item needs an owner's decision. The public API may change
in the reset; stored formats may not change after the freeze without upgrade coverage.

Evidence probes below ran against the current engine (0.19.3, `MinnowDatabase` on the memory
store).

## D1. The JS value of TIMESTAMP / TIMESTAMPTZ

Facts:

- The new format stores int64 microseconds since 1970-01-01 UTC (decided 2026-11-04).
- Today both types return `Date`. A value written as `'2024-01-02 03:04:05.123456'` reads back
  as `2024-01-02T03:04:05.123Z`: the microseconds are already dropped silently.
- DATE returns `'YYYY-MM-DD'` and NUMERIC returns exact decimal text. TIME and INTERVAL return
  PostgreSQL text.
- The f64-safe range of epoch microseconds is about 1684–2255. Inside it, a column of
  timestamps is an exact `Float64Array`.

| Option                                                                                                                              | Exact                                                                     | Cost per value | JSON / `postMessage`                                     | Ergonomics                                               | Consistent with DATE/NUMERIC |
| ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- | -------------- | -------------------------------------------------------- | -------------------------------------------------------- | ---------------------------- |
| A. `Date` (today)                                                                                                                   | No: ms only; a returned value no longer equals its row in `WHERE ts = $1` | 1 object       | Yes / yes                                                | Familiar                                                 | No                           |
| B. ISO 8601 text, 6 fixed fraction digits: `2024-01-02T03:04:05.123456Z` (TIMESTAMPTZ) and `2024-01-02T03:04:05.123456` (TIMESTAMP) | Yes, full PostgreSQL range (expanded years outside 0000–9999)             | 1 string       | Yes / yes                                                | Must parse to compute; sorts as text for years 0000–9999 | Yes                          |
| C. `bigint` µs                                                                                                                      | Yes                                                                       | 1 bigint       | `JSON.stringify` throws                                  | Mixing with numbers throws; no zone                      | No                           |
| D. `number` µs                                                                                                                      | Only 1684–2255                                                            | none           | Yes                                                      | Looks like any number; needs the domain to read          | No                           |
| E. `Temporal.Instant` / `PlainDateTime`                                                                                             | Yes (ns)                                                                  | 1 object       | Not structured-cloneable: rebuilt at the worker boundary | The right types                                          | No                           |
| F. Custom `Timestamp` class                                                                                                         | Yes                                                                       | 1 object       | Prototype lost across `postMessage`                      | New API to learn                                         | No                           |

Notes:

- B's zone suffix carries the type: no `Z` means wall-clock time without a zone. `new Date(text)`
  reads text without `Z` as local time, which is correct for TIMESTAMP but differs from today,
  where TIMESTAMP comes back as a UTC instant. More than three fraction digits is outside
  ECMAScript's strict date-time format. Engines accept it today, but docs should point to
  `Temporal.*.from(text)` or a small exported `timestampToDate(text)` helper.
- E is the right model but cannot be required yet: it is not in every target browser, a
  polyfill would cost much of the 300 KiB gzip budget, and it would need rehydration at the
  worker boundary.
- Performance is the same for A, B, E and F (one allocation per value, only at row output). The
  columnar accessor makes this matter less: it exposes the µs column directly.

**Recommendation: B for row objects. The columnar accessor returns epoch µs as a
`Float64Array` (BigInt64Array outside 1684–2255).** Parameters accept text (ISO or PostgreSQL) and
`Date` (exact to the millisecond). B is exact, JSON- and clone-safe, matches DATE and NUMERIC,
maps one-to-one to Temporal later, and adds no dial.

Cost: a breaking change. It touches the changelog, `InferRow` (timestamp → `string`), the
Kysely/React typings, the docs and the agent rules. The differential harness's `["datetime", …]`
encoding must follow at cut-over; the adapter maps to the oracle's `Date` until then.

## D2. The JS value of BIGINT values beyond 2^53

Facts:

- Since 0.19.3, integers are exact or an error. BIGINT holds only safe integers, and
  `SUM(bigint)` past 2^53 throws `RangeError: Integer out of range`. PostgreSQL returns NUMERIC
  `9007199254740993`.
- The new format stores the full int64 range. A stored value the API cannot return is not an
  option.

| Option                                               | Exact | Common case                                                                   | Failure mode                                                  | PostgreSQL fidelity |
| ---------------------------------------------------- | ----- | ----------------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------- |
| A. Always `bigint` for int64                         | Yes   | Every count and id becomes bigint: `n + 1`, `JSON.stringify` and charts break | Loud, everywhere                                              | Type-faithful       |
| B. `number` within ±(2^53−1), `bigint` beyond        | Yes   | Unchanged                                                                     | Loud, only at large values (TypeError on mixing; JSON throws) | Value-faithful      |
| C. `number`, error beyond (today)                    | Yes   | Unchanged                                                                     | A stored value cannot be read; sums overflow into errors      | No                  |
| D. Text beyond 2^53, or always (node-postgres style) | Yes   | A: breaks / B: unchanged                                                      | **Silent**: `"9007199254740993" + 1` concatenates             | Value-faithful      |
| E. Per-connection option                             | —     | —                                                                             | —                                                             | Adds a dial         |

**Recommendation: B.**

- Only BIGINT columns and int64 results are typed `number | bigint`. SMALLINT and INTEGER stay
  `number`, so `InferRow` changes only for BIGINT. The schema DSL gains `column.bigint()`;
  `column.integer()` stays int32.
- The columnar accessor returns `Float64Array` when the batch is proven safe, otherwise
  `BigInt64Array`.
- Parameters accept a safe-integer `number` or a `bigint`. An unsafe `number` is refused,
  because it was already rounded before it reached the engine.
- Failures are loud, and only at the values that need them. Text (D) is exact but fails silently.

## Other decisions needed before the freeze

- **C1. DATE value.** Keep `'YYYY-MM-DD'` (with `' BC'` for year ≤ 0, as PostgreSQL writes it).
  The accessor returns `Int32Array` days. _Recommend: keep._
- **C2. NUMERIC value and specials.** Keep exact text. PostgreSQL 14+ also has NaN and ±Infinity.
  The vector layout has a `specials` side array, and the page format should reserve it now even
  if SQL support comes later. _Recommend: reserve in the format; enable later._
- **C3. TIME and INTERVAL values.** Keep PostgreSQL text. _Recommend: keep._
- **C4. Infinite DATE and TIMESTAMP.** PostgreSQL stores `'infinity'` as int min/max. Those
  sentinels must be reserved in the format. In the safe f64 layout they become ±Infinity.
  _Recommend: reserve._
- **C5. Integer widths.** SMALLINT and INTEGER get PostgreSQL ranges (int16/int32). Today INTEGER
  accepts any safe integer, so this changes behaviour. It needs the front end to keep the
  declared type name, which it drops today (`createTableTypeNames` maps all of them to
  `"number"`). _Recommend: PostgreSQL ranges._
- **C6. REAL.** Today it is float64. PostgreSQL's float4 would need `Math.fround` on every write
  and operation. _Recommend: keep float64 and document the deviation._
- **C7. Key encoding.** Key-index runs, fences, Bloom filters and normalized sort keys share one
  memcomparable byte encoding, and runs persist it, so it freezes with the format. Its rules:
  - ints: sign-flipped big-endian;
  - float64: order-preserving bits, with −0 → 0, one canonical NaN, NaN above +Infinity
    (PostgreSQL);
  - NUMERIC: normalized, so `1.0` and `1.00` encode equal (PostgreSQL equality ignores scale);
  - text: bytes with 0x00 escaped and a terminator, so composite keys are prefix-free;
  - a null-order byte per component.
    _Recommend: freeze these rules with the page format._
- **C8. Text order.**
  - **Default order.** It is JS `<` on UTF-16 code units today (`compareSqlStrings`). PostgreSQL's
    C collation orders by bytes, which is code-point order. The two differ only between
    supplementary characters and U+E000–U+FFFF: `'￿' < '😀'` is false today and true in
    PostgreSQL. _Recommend: byte order._ It is PostgreSQL's order, it matches the stored UTF-8
    and the page statistics, and it needs no decoding. Record it as a known divergence from the
    oracle.
  - **Explicit `COLLATE`.** This uses `Intl.Collator`, which has only `compare` and no sort-key
    API. The proposal's "normalized sort-key bytes per collation" cannot be built without
    shipping collation tables. _Recommend:_ rank the distinct values once with the collator (the
    size of a dictionary), sort by integer rank, and keep equality on bytes. This is an executor
    contract change, not a format one.
- **C9. Arrays.** The choice is one canonical binary value per row (element tag, dimensions, null
  bitmap, elements) or list vectors (offsets plus a child vector). Arrays are rare in analytics.
  _Recommend: the binary value at 1.0._
- **C10. JSON vs JSONB storage.** Today both keep the same document text. PostgreSQL normalizes
  JSONB: keys sorted, duplicate keys last-wins, whitespace dropped. Which one we pick changes the
  stored bytes and the results. _Recommend: normalize JSONB (PostgreSQL fidelity)._
- **C11. Aggregate result types.**
  - PostgreSQL types `SUM(int4)` as int8, `SUM(int8)` and `AVG(int)` as NUMERIC, and `COUNT` as
    int8.
  - Today `AVG(integer)` returns the number `1.5`, where PostgreSQL returns NUMERIC
    `1.5000000000000000`, and `SUM(bigint)` overflows into an error.
  - Following PostgreSQL turns `AVG(int)` into decimal text. _Recommend:_ follow PostgreSQL for
    SUM (exact) but keep `AVG` of int and float inputs as float64. Document the AVG deviation.
    This needs an owner's decision: it is the most visible result-type change.
- **C12. Time travel.** Today `options.version` reads an older version. The reset does not
  mention it. Keeping it means reclaim must respect pinned roots, at the cost of disk space.
  _Recommend:_ drop arbitrary-version reads. Keep consistent snapshot reads while a read
  transaction is open, and keep snapshot copies.
- **C13. Exactly-once retry window.** `CLIENT_RETRY_WINDOW_MS` is 10 minutes. The client protocol
  must stop retrying before it ends (format/manifest.ts). _Recommend: 10 minutes._

## Where these contracts deviate from or refine PROPOSAL.md

1. **IndexedDB is needed, so the store contract is async.** Safari private browsing has no OPFS,
   so the IndexedDB gate fails. IndexedDB implements the same `DurableStore`, and its fence is
   the epoch check inside each transaction plus the Web Lock: there is no exclusive handle.
   OPFS pays one resolved promise per batched call.
2. **Batched `read(ranges)` instead of `readPage`.** The store knows the physical layout, so
   coalescing into 256 KiB–1 MiB slabs belongs there. One call serves a whole scan step.
3. **Packs instead of loose pages.** `publish(root, packs)` publishes immutable packs written by a
   streaming `PackWriter`. A 64k-row × 200-column group is ~100 MB and is never held in memory
   to publish. Reclaim works per pack: the maintenance track rewrites packs that are mostly
   dead.
4. **Frames carry delete keys, not a full key-index delta.** Insert keys are derived from the
   inserted key columns. Repeating them would add 8–16 B per inserted row to the log for nothing.
5. **The log is recycled only when every table's memtable is flushed past its end.** The proposal
   recycles the log on each flush. With several tables sharing one log, that is unsafe unless
   all of them flush, or the remaining rows are copied forward into the new log. The rule is
   stated in `format/manifest.ts`; the write path may add copy-forward.
6. **Exactly-once needs a ledger in the root.** Frames are the record only until the log is
   recycled. After that, `clients` in the root keeps the last sequence per client for the
   retry window.
7. **Checksums are chained across frames** (frame n's xxHash32 is seeded with frame n−1's), and
   recovery truncates to the valid end before appending. Without chaining, a stale but intact
   frame behind a rewritten one validates. `log-frame.test.ts` shows the case.
8. **The front end must carry declared types** (C5), and it executes nested blocks eagerly today
   (`subqueryResolutionSteps`). Lowering turns them into subtrees, `initPlans` and `apply`
   (`ir/lowering.ts`).
9. **Sort keys for ICU collations are ranks, not bytes** (C8).
