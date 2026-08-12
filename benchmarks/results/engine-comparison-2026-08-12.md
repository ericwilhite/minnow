# Engine comparison after the optimization passes — 2026-08-12

## Scope

Re-capture of the 15-query, three-engine, scale-10 reference workload after the
2026-08-11/12 optimization passes, on the same host and configuration as
`engine-comparison-2026-08-11.md` and its prepare-cache follow-up. The engine changes
between the captures, in rough order of impact on this workload:

1. **Query memory accounting** dropped its per-row reservation objects and per-string
   UTF-8 measurement encodes for an aggregated tally (one byte per UTF-16 code unit).
2. **Plan cache** (512-statement LRU over immutable compiled plans) and single-compile
   `query()`.
3. **Block format v1**: an envelope checksum authenticates the header and metadata, so
   zone-map pruning reads statistics header-only and never decodes a pruned block; a
   decoded-block cache keyed by immutable block id (inside `prepareCacheBytes`) survives
   commits that invalidate assembled-vector fingerprints.
4. **Executor kernels**: direct-address compound dictionary-code grouping, unboxed bare
   number-column aggregates, direct-assignment row projection, once-per-row sort-key
   extraction with a shared collator.
5. **Write path**: raw-codec zero-copy, `encodeInto` string encoding, single-pass
   row-to-column pivot, atomic block+segment+journal staging, one-round-trip begin with
   row-id reservation, log-structured unique keys (sixteen-chunk tail folded into
   per-key base records), and **delta manifests** (checkpoint every 32 versions), which
   made commit cost independent of total database size.

## Results (Playwright capture, Chromium 151.0.7922.34 / Firefox 153.0)

Dataset: 956,160 rows across 50 tables, gzip, 1 MiB target blocks, relaxed durability.
All 15 queries supported and oracle-verified on all three engines in both browsers.

| Minnow, scale 10         | 2026-08-11 |    Today | Change |
| ------------------------ | ---------: | -------: | -----: |
| Chromium dataset build   |    3946 ms |  2341 ms |  1.69× |
| Chromium insert portion  |    3431 ms |  1852 ms |  1.85× |
| Chromium sum of prepares |   222.7 ms | 154.0 ms |  1.45× |
| Chromium sum of medians  |    38.1 ms |  29.2 ms |  1.31× |
| Firefox dataset build    |    2607 ms |  2151 ms |  1.21× |
| Firefox insert portion   |    2195 ms |  1777 ms |  1.24× |
| Firefox sum of prepares  |   283.6 ms | 235.1 ms |  1.21× |
| Firefox sum of medians   |    45.8 ms |  36.0 ms |  1.27× |
| Stored bytes (both)      |   ~19.9 MB | ~18.9 MB |        |

Competitor aggregates were unchanged within noise (Chromium sums of medians: SQLite Wasm
174.5 ms, PGlite 522.0 ms; Firefox: 1115.1 ms and 820.2 ms), which doubles as an
environment sanity check. Minnow's Chromium insert rate at this scale is now ~516k rows/s
through the full public batch API against IndexedDB, and repeated-execution medians lead
SQLite Wasm by ~6× in Chromium and ~31× in Firefox on this suite.

## Method

Standalone Playwright spec driving the bench worker (`BenchWorker` `datasetCreate` then
`suiteReference`) against the Vite dev server, 1 warm-up plus 7 measured samples per
query, medians reported. Raw JSON: `2026-08-12-engine-comparison-{chromium,firefox}.json`
(these become the site's published numbers automatically).

## Caveats

- Same single-host, single-run methodology as the prior records; treat as observations.
- Minnow `prepareQuery()` includes catalog reads, lease anchoring, and input
  materialization; `execute()` medians exclude that. The sqlite/pglite adapters re-run
  the full statement per sample. Sum-of-medians and prepare totals are reported
  separately for that reason.
- The block-format v1 change means these databases are not readable by pre-2026-08-11
  builds; the comparison numbers are unaffected.
