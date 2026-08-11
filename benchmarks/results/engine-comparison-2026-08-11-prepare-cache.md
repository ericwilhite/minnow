# Prepare-cost reduction — 2026-08-11 follow-up

## Scope

Same-day follow-up to `engine-comparison-2026-08-11.md`, which identified per-statement
`prepareQuery()` cost — snapshot leasing, catalog round trips, input materialization, and
CTE execution — as MinnowDatabase's dominant one-shot cost. This run measures the same
15-query, three-engine, scale-10 workload on the same host after three engine changes:

1. **One coherent catalog read.** A new optional `BlockStore.getQueryCatalogState()`
   reads the current manifest version, the referenced table records, their segments, and
   those segments' transaction records in one atomic store read (one IndexedDB readonly
   transaction) instead of sequential per-record round trips. Engines without the method
   fall back to the sequential shape.
2. **Shared internal reader lease.** Prepares at the same manifest version share one
   renewed lease record instead of paying lease create/release write transactions per
   statement. The lease is anchored to the catalog state's exact version; a pruned
   manifest re-reads the state, and explicit time-travel versions keep per-call leases.
3. **Byte-bounded prepare cache** (`prepareCacheBytes`, default 64 MiB, 0 disables).
   Assembled append-table column vectors, zone-pruned projections, and derived / union /
   windowed / recursive-base / scalar-subquery block results are keyed by the exact
   ordered visible segment ids (plus predicate values and the compiled block where
   relevant). Segment records are immutable, so a cached entry can never serve stale
   data: any write changes the visible segment set, the key stops matching, and old
   entries age out of the LRU. Derived-block entries carry their inferred output schema,
   captured at miss time because a cache hit skips nested schema registration.

Profiling drove the design: at scale 10 only ~17 ms of the original 472 ms summed prepare
was IndexedDB block fetch and ~20 ms was decode — the rest was sequential catalog/lease
round-trip latency (the page sat ~48% idle) and repeated vector assembly and CTE
execution, so caching assembled results mattered more than faster I/O.

## Results (Playwright capture, Chromium 151.0.7922.34 / Firefox 153.0)

| Aggregate (15 queries)     |   Before |    After | SQLite Wasm |   PGlite |
| -------------------------- | -------: | -------: | ----------: | -------: |
| Chromium sum of prepare    | 478.5 ms | 222.7 ms |       ~0 ms |    ~0 ms |
| Chromium sum of medians    |  37.9 ms |  38.1 ms |    174.1 ms | 511.8 ms |
| Chromium prepare + medians | 516.4 ms | 260.8 ms |    174.2 ms | 511.9 ms |
| Firefox sum of prepare     | 797.8 ms | 283.6 ms |       ~0 ms |    ~0 ms |
| Firefox sum of medians     |  46.3 ms |  45.8 ms |  1,114.0 ms | 796.2 ms |
| Firefox prepare + medians  | 844.1 ms | 329.4 ms |  1,114.2 ms | 796.4 ms |

SQLite and PGlite columns are this run's fresh measurements; their adapters defer all
work to execution, so their per-statement cost sits inside every sample (see the base
record's measurement-semantics section). All 90 engine-query cells across both browsers
returned oracle-verified results, and the write path is unchanged (Chromium insert
3.43 s, 18.99 MB stored).

- Repeated-execution medians are untouched: MinnowDatabase still wins every one of the
  15 queries in both browsers.
- First-touch prepare halved in Chromium (2.1×) and dropped 2.8× in Firefox. In Firefox
  the prepare-inclusive one-shot total now beats both engines outright; in Chromium it
  remains ~1.5× SQLite's warm sum of medians, which is expected — this figure still pays
  every cold block read once, which SQLite's warm medians do not.
- A separate main-thread harness (same dataset, prepare called twice per statement)
  shows the steady-state effect the suite's one-prepare shape cannot: a repeated prepare
  of the same statement fell from 426 ms summed to 44 ms (1.5–9 ms per statement), so a
  re-preparing caller — live queries, dashboards — now pays about a millisecond-scale
  cost instead of re-materializing inputs.

## Caveats

- The prepare cache's retained bytes are bounded by `prepareCacheBytes` but are not part
  of the per-query accounted-memory model; the roadmap's Phase 7 uncounted list records
  this.
- Cached vectors and block results are shared read-only across prepared queries; the
  randomized parity, differential fuzz, and cross-engine oracle suites all pass, but the
  sharing invariant is enforced by convention, not the type system.
- Mutation-replay table materialization is not vector-cached (its block results still
  are); replay-heavy workloads keep their previous prepare cost.
- Single host, single run per browser, as with the base record.
