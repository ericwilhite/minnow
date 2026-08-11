# Persistent browser engine comparison — 2026-08-11

## Scope

- First checked-in run in which BrowserDatabase's own SQL is measured: all 15 reference queries
  execute through the public `prepareQuery()`/`execute()` API and every result is verified
  against the suite's independent JavaScript oracle.
- Dataset: the deterministic 50-table commerce graph at scale 10 — 956,160 rows, generated
  identically for every engine (the same dataset shape as the 2026-08-09 codec matrix refresh).
- BrowserDatabase: gzip, 1 MiB target blocks, relaxed IndexedDB durability (dashboard defaults).
- SQLite Wasm 3.53.0: persistent OPFS SAH-pool VFS, prepared inserts in immediate transactions.
- PGlite 0.5.x: persistent IndexedDB filesystem, default durable syncing.
- DuckDB-Wasm is no longer part of the dashboard; the current comparison is three engines.
- Method: one Playwright run per browser on ephemeral profiles with COOP/COEP cross-origin
  isolation active. Each engine is materialized once, then every query records prepare time plus
  one warm-up and seven measured executions (median and p95). Oracle verification and
  materialization happen in the same page session; reads run through sessions reopened from the
  materialized copies.
- Host: macOS arm64 (Darwin 25.5.0), Apple silicon, 12 logical cores. Chromium 151.0.7922.34 and
  Firefox 153.0 via Playwright.
- Raw bundles: `2026-08-11-engine-comparison-chromium.json` and
  `2026-08-11-engine-comparison-firefox.json`.

Results are observations on this host, not performance promises. This run is not directly
comparable to the 2026-08-08 four-engine report: that run used the earlier 27-table graph at
scale 100 (1,930,800 rows), a six-query workload, and 512 KiB blocks.

## Measurement semantics — read this before the tables

The three adapters expose different public fast paths, and the suite measures each engine's
intended repeated-execution API rather than forcing a common shape:

- BrowserDatabase `prepareQuery()` compiles the statement, leases one snapshot, materializes the
  referenced typed columns, and executes CTE/derived-table blocks at that snapshot. Repeated
  `execute()` calls therefore measure query execution over resident columnar inputs without
  storage I/O. Prepare cost is reported separately and is the dominant one-shot cost.
- The SQLite adapter re-runs the complete statement per sample (`selectObjects`), so its samples
  include parsing and reads through SQLite's warm page cache; its prepare column is ~0.
- The PGlite adapter likewise re-runs the complete statement per sample; prepare is ~0.

Both aggregates below are therefore meaningful and different: the sum of medians compares
repeated-execution latency (the live-query and dashboard-refresh case), while prepare plus
medians approximates one-shot cost including BrowserDatabase's snapshot materialization.

## Materialization and storage

| Metric               |            BrowserDatabase |        SQLite Wasm 3.53.0 |             PGlite 0.5.x |
| -------------------- | -------------------------: | ------------------------: | -----------------------: |
| Chromium insert      |    3.39 s · 281,804 rows/s |   6.00 s · 159,493 rows/s |  10.76 s · 88,871 rows/s |
| Chromium total build |                     3.93 s |                    8.82 s |                  20.44 s |
| Firefox insert       |    2.33 s · 409,841 rows/s |   8.15 s · 117,277 rows/s |  39.50 s · 24,206 rows/s |
| Firefox total build  |                     2.78 s |                   11.26 s |                  67.05 s |
| Stored size          |                   18.99 MB |                  76.95 MB |                143.04 MB |
| Persistence          | IndexedDB · gzip’d columns | OPFS SAH-pool SQLite file | IndexedDB PostgreSQL dir |

Insert time is the summed public insert calls; total build adds store/schema creation, a row-count
verification query, and size sampling. Stored size definitions follow each engine's own accounting
(BrowserDatabase logical IndexedDB payload, SQLite allocated pages, PGlite database directory), as
in the 2026-08-08 report. BrowserDatabase's copy is 4.1× smaller than SQLite's and 7.5× smaller
than PGlite's.

## Reference suite totals (15 queries, 7 samples each)

| Aggregate                  | BrowserDatabase | SQLite Wasm |   PGlite |
| -------------------------- | --------------: | ----------: | -------: |
| Chromium sum of prepare    |        478.5 ms |      ~0 ms¹ |   ~0 ms¹ |
| Chromium sum of medians    |         37.9 ms |    177.0 ms | 539.1 ms |
| Chromium prepare + medians |        516.4 ms |    177.1 ms | 539.2 ms |
| Firefox sum of prepare     |        797.8 ms |      ~0 ms¹ |   ~0 ms¹ |
| Firefox sum of medians     |         46.3 ms |   1139.1 ms | 839.5 ms |
| Firefox prepare + medians  |        844.1 ms |   1139.3 ms | 839.7 ms |
| Queries supported          |         15 / 15 |     15 / 15 |  15 / 15 |
| Oracle-verified results    |         15 / 15 |     15 / 15 |  15 / 15 |

¹ These adapters defer all work to execution, so their per-statement cost sits inside every
sample rather than in a prepare step.

On repeated execution BrowserDatabase's summed medians are 4.7× faster than SQLite and 14.2×
faster than PGlite in Chromium, and 24.6× and 18.1× faster in Firefox. Including prepare,
one-shot cost is 2.9× slower than SQLite in Chromium and roughly tied with PGlite; in Firefox the
prepare-inclusive total still beats SQLite and ties PGlite. All 90 engine-query cells across both
browsers returned oracle-verified results, including the DATE_TRUNC monthly-cohort query the SQL
surface previously could not express.

## Per-query medians / p95 — Chromium (ms)

| Query                                  | BrowserDatabase | SQLite Wasm |      PGlite |
| -------------------------------------- | --------------: | ----------: | ----------: |
| q1 Order point lookup                  |       0.0 / 0.1 |   0.2 / 0.3 | 27.5 / 28.3 |
| q2 Paid orders in a date range         |       0.4 / 0.7 |   1.0 / 1.1 | 27.8 / 28.4 |
| q3 Revenue by order status             |       0.9 / 1.2 |   3.0 / 3.1 | 29.6 / 30.3 |
| q4 Top customers by captured revenue   |      7.2 / 12.7 |   8.3 / 8.6 | 33.8 / 40.7 |
| q5 Category revenue after discounts    |       5.3 / 6.1 | 16.9 / 17.1 | 37.6 / 38.6 |
| q6 Repeat customers without returns    |       3.4 / 3.9 |   7.2 / 7.7 | 36.8 / 37.8 |
| q7 Top products within each category²  |       0.0 / 0.1 | 21.3 / 21.6 | 40.4 / 42.5 |
| q8 Monthly cohort revenue and returns² |       0.4 / 0.4 | 11.1 / 11.6 | 34.0 / 34.7 |
| q9 Region and segment revenue matrix   |       2.1 / 2.3 | 12.8 / 15.1 | 37.5 / 45.0 |
| q10 Return rate by product category    |       3.9 / 4.1 | 16.4 / 16.7 | 38.1 / 39.0 |
| q11 Tax collected by jurisdiction      |       3.7 / 4.4 | 15.3 / 15.5 | 39.5 / 40.8 |
| q12 Fulfillment volume by warehouse    |       0.5 / 0.7 | 17.8 / 17.9 | 41.4 / 46.9 |
| q13 Supplier inventory ledger          |       4.6 / 5.5 | 21.4 / 21.5 | 41.8 / 57.3 |
| q14 Payment transaction funnel         |       2.9 / 3.2 |   8.3 / 8.4 | 32.5 / 32.9 |
| q15 Discount and tax burden by status  |       2.4 / 2.6 | 16.0 / 16.1 | 40.7 / 41.2 |

² q7 and q8 do nearly all their work in CTEs, which BrowserDatabase executes during prepare
(43.7 ms and 33.7 ms here); their `execute()` medians measure only the small outer block. The
other engines re-run the complete statement per sample.

## Per-query medians / p95 — Firefox (ms)

| Query                                  | BrowserDatabase |   SQLite Wasm |      PGlite |
| -------------------------------------- | --------------: | ------------: | ----------: |
| q1 Order point lookup                  |       0.1 / 0.1 |     0.2 / 0.5 | 10.5 / 12.3 |
| q2 Paid orders in a date range         |       0.3 / 0.7 |     6.3 / 6.4 | 15.5 / 18.3 |
| q3 Revenue by order status             |       1.2 / 1.3 |   18.8 / 19.2 | 24.1 / 25.3 |
| q4 Top customers by captured revenue   |      7.4 / 10.5 |   49.9 / 50.2 | 44.1 / 49.4 |
| q5 Category revenue after discounts    |       6.0 / 6.1 | 108.9 / 110.2 | 71.7 / 72.8 |
| q6 Repeat customers without returns    |       4.0 / 6.3 |   30.1 / 30.5 | 41.6 / 45.0 |
| q7 Top products within each category²  |       0.0 / 0.1 | 142.4 / 143.6 | 81.8 / 85.7 |
| q8 Monthly cohort revenue and returns² |       0.4 / 0.5 |   78.7 / 79.4 | 51.7 / 99.1 |
| q9 Region and segment revenue matrix   |       1.6 / 2.3 |   76.2 / 76.8 | 58.3 / 60.9 |
| q10 Return rate by product category    |       6.3 / 7.8 | 100.6 / 102.6 | 66.2 / 67.8 |
| q11 Tax collected by jurisdiction      |       4.0 / 5.8 | 100.6 / 136.5 | 78.7 / 81.1 |
| q12 Fulfillment volume by warehouse    |       0.7 / 2.2 | 116.1 / 155.9 | 88.9 / 90.0 |
| q13 Supplier inventory ledger          |       4.6 / 6.0 | 137.7 / 138.2 | 84.3 / 86.0 |
| q14 Payment transaction funnel         |       5.5 / 6.9 |   65.9 / 77.3 | 43.1 / 45.3 |
| q15 Discount and tax burden by status  |       4.2 / 5.1 | 106.8 / 140.6 | 79.0 / 91.1 |

## Observations

Follow-up: the prepare cost identified below was reduced the same day; see
`engine-comparison-2026-08-11-prepare-cache.md` for the changes and re-measured run.

- Repeated execution over prepared plans is BrowserDatabase's strongest surface: every one of the
  15 medians beats both engines in both browsers.
- Prepare — snapshot leasing, full input-column materialization, and CTE execution — is now the
  dominant BrowserDatabase cost at 20–46 ms per statement (478.5 ms summed in Chromium, 797.8 ms
  in Firefox). Reducing it is the clearest next optimization target, and it is exactly the open
  Phase 7 item: prepared and non-streamed shapes still materialize every projected input column
  in full, and Phase 8 predicate pruning still fetches complete IndexedDB records.
- Firefox magnifies the gap: SQLite's OPFS SAH-pool reads are 6.4× slower there than in Chromium
  on summed medians, while BrowserDatabase's totals stay within 1.3× across browsers.
- The write path remains ahead at this scale: fastest insert, fastest total build, and the
  smallest stored copy of the three engines in both browsers.

## Caveats

- Single host, single run per browser; no repeated-sample distribution across runs.
- Playwright ephemeral profiles; quota-constrained or long-lived-profile behavior is not covered.
- The BrowserDatabase memory model's unbounded default was in effect; no execution memory budget
  was configured for any engine.
- Adapter choices matter: SQLite could amortize parsing with persistent prepared statements, and
  PGlite pays IndexedDB-VFS syncing costs its OPFS backends might not. Each adapter uses the
  engine's documented persistent-browser configuration as shipped in the dashboard.
