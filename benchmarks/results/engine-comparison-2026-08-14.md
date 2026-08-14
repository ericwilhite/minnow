# Engine comparison at scale 100, disk-backed — 2026-08-14

## Scope

First capture of the 15-query, three-engine reference workload at **scale 100 — 9,561,600
rows across 50 tables per engine** (the previous captures were scale 10), and the first with
**disk-backed browser storage**. Two things changed materially versus the 2026-08-12 capture:

1. **Persistent browser profiles.** Every earlier capture ran in Playwright's default
   ephemeral context, which backs IndexedDB with memory — no bytes ever reached disk, in any
   engine. These runs use `launchPersistentContext` with a real on-disk profile; Minnow's
   IndexedDB store, SQLite's OPFS SAH-pool file, and PGlite's IndexedDB-VFS data directory
   were all verified growing on disk during the load (456 MB / 830 MB / 1,390 MB
   respectively). Earlier captures compared the engines fairly against each other, but their
   absolute persistence numbers were optimistic; these are honest.
2. **The unique-key base cache** (`IndexedDbBlockStore#uniqueKeyCache`, landed today). At
   this scale the old code probed the folded unique-key base with one IndexedDB `getKey`
   request per row — roughly eight million sequential requests across the load — which
   stalled dataset creation indefinitely. Scale 10 never surfaced it. The commit that fixed
   it also covers the fold/cache/scan/probe paths with unit tests.

Configuration otherwise matches earlier captures: gzip, 1 MiB target blocks, relaxed
durability, engine SQL identical per query (dialect overrides where recorded), every result
checksum-verified identical across engines and against two independent JavaScript oracles
before any timing counts. All 15 queries supported and verified on all three engines in both
browsers.

## Headlines (median per query, 7 samples, host Apple M2 Max, 12 cores)

|                                    |           Chromium 151 |              Firefox 153 |
| ---------------------------------- | ---------------------: | -----------------------: |
| Minnow dataset build (9.56 M rows) |                  258 s |                    426 s |
| SQLite Wasm dataset build          |                   61 s |                    118 s |
| PGlite dataset build               |                  163 s |                    633 s |
| Minnow stored bytes                |                 456 MB |                   457 MB |
| SQLite stored bytes                |                 830 MB |                   830 MB |
| PGlite stored bytes                |               1,390 MB |                 1,390 MB |
| Minnow sum of query medians        |             **255 ms** |               **341 ms** |
| SQLite sum of query medians        | 1,950 ms (7.7× slower) | 13,082 ms (38.4× slower) |
| PGlite sum of query medians        | 1,775 ms (7.0× slower) |  7,363 ms (21.6× slower) |
| Minnow prepare-inclusive total     |           **1,149 ms** |             **2,040 ms** |
| SQLite prepare-inclusive total     |               1,950 ms |                13,083 ms |
| PGlite prepare-inclusive total     |               1,775 ms |                 7,363 ms |

Minnow's `prepareQuery()` anchors the snapshot, materializes input columns, and executes
CTE/derived blocks, so its execute medians exclude that work while the SQLite and PGlite
adapters re-run the full statement per sample. Both rows are shown for that reason; Minnow
leads on either accounting.

Bulk load is the one axis where Minnow pays more than SQLite: the write path validates,
compresses, and maintains unique-key and columnar structures per commit. 22–37 k rows/s
sustained to disk is the current cost of that (SQLite Wasm: ~81–157 k rows/s into a plain
B-tree file).

## Chromium 151 per query

| Query                                       |       Minnow |                 SQLite Wasm |                      PGlite |
| ------------------------------------------- | -----------: | --------------------------: | --------------------------: |
| q1 Order point lookup                       |  **0.02 ms** |       0.04 ms (1.6× slower) |   25.06 ms (1002.6× slower) |
| q2 Paid orders in a date range              |  **0.85 ms** |       6.53 ms (7.7× slower) |     31.58 ms (37.4× slower) |
| q3 Revenue by order status                  |  **3.20 ms** |      29.52 ms (9.2× slower) |     48.32 ms (15.1× slower) |
| q4 Top customers by captured revenue        | **41.02 ms** |      85.44 ms (2.1× slower) |      85.24 ms (2.1× slower) |
| q5 Category revenue after discounts         | **51.59 ms** |     189.37 ms (3.7× slower) |     128.51 ms (2.5× slower) |
| q6 Repeat customers without returns         | **27.35 ms** |      83.96 ms (3.1× slower) |     113.10 ms (4.1× slower) |
| q7 Top products within each category        |  **0.02 ms** | 237.81 ms (11890.4× slower) | 217.03 ms (10851.2× slower) |
| q8 Monthly cohort revenue and return rate   |  **2.90 ms** |    117.46 ms (40.5× slower) |     91.38 ms (31.5× slower) |
| q9 Region and segment revenue matrix        |  **7.88 ms** |    134.90 ms (17.1× slower) |    120.70 ms (15.3× slower) |
| q10 Return rate by product category         | **30.52 ms** |     188.88 ms (6.2× slower) |     129.92 ms (4.3× slower) |
| q11 Tax collected by jurisdiction           | **25.27 ms** |     175.12 ms (6.9× slower) |     152.60 ms (6.0× slower) |
| q12 Fulfillment volume by warehouse         |  **2.30 ms** |    190.73 ms (82.7× slower) |    178.60 ms (77.5× slower) |
| q13 Supplier inventory ledger               | **25.28 ms** |     248.94 ms (9.8× slower) |     206.81 ms (8.2× slower) |
| q14 Payment transaction funnel              | **11.88 ms** |      86.80 ms (7.3× slower) |      86.11 ms (7.2× slower) |
| q15 Discount and tax burden by order status | **24.65 ms** |     174.01 ms (7.1× slower) |     160.38 ms (6.5× slower) |

## Firefox 153 per query

| Query                                       |       Minnow |                  SQLite Wasm |                       PGlite |
| ------------------------------------------- | -----------: | ---------------------------: | ---------------------------: |
| q1 Order point lookup                       |  **0.06 ms** |        0.18 ms (3.0× slower) |     12.36 ms (206.0× slower) |
| q2 Paid orders in a date range              |  **1.24 ms** |      61.76 ms (49.8× slower) |      59.12 ms (47.7× slower) |
| q3 Revenue by order status                  |  **3.48 ms** |     210.00 ms (60.3× slower) |     144.18 ms (41.4× slower) |
| q4 Top customers by captured revenue        | **66.38 ms** |      562.80 ms (8.5× slower) |      327.34 ms (4.9× slower) |
| q5 Category revenue after discounts         | **61.32 ms** |    1271.98 ms (20.7× slower) |      590.04 ms (9.6× slower) |
| q6 Repeat customers without returns         | **41.92 ms** |     451.04 ms (10.8× slower) |      304.82 ms (7.3× slower) |
| q7 Top products within each category        |  **0.04 ms** | 1605.00 ms (40125.0× slower) | 1152.22 ms (28805.5× slower) |
| q8 Monthly cohort revenue and return rate   |  **3.54 ms** |    867.72 ms (245.1× slower) |    371.16 ms (104.8× slower) |
| q9 Region and segment revenue matrix        |  **7.54 ms** |    823.96 ms (109.3× slower) |     460.54 ms (61.1× slower) |
| q10 Return rate by product category         | **49.18 ms** |    1241.62 ms (25.2× slower) |     539.56 ms (11.0× slower) |
| q11 Tax collected by jurisdiction           | **26.76 ms** |    1188.38 ms (44.4× slower) |     670.62 ms (25.1× slower) |
| q12 Fulfillment volume by warehouse         |  **2.68 ms** |   1286.88 ms (480.2× slower) |    887.16 ms (331.0× slower) |
| q13 Supplier inventory ledger               | **24.92 ms** |    1620.60 ms (65.0× slower) |     867.96 ms (34.8× slower) |
| q14 Payment transaction funnel              | **11.46 ms** |     711.54 ms (62.1× slower) |     313.96 ms (27.4× slower) |
| q15 Discount and tax burden by order status | **40.54 ms** |    1178.92 ms (29.1× slower) |     661.48 ms (16.3× slower) |

## Reading notes

- q7 (window function over category partitions) is where columnar execution and the plan
  shape diverge most: Minnow answers from the prepared scan in microseconds while both
  row engines re-aggregate ~218 ms per sample.
- PGlite beats SQLite Wasm on several heavy aggregations in Chromium but both trail by an
  order of magnitude in Firefox; Firefox's Wasm and IndexedDB paths are simply slower.
- The 2026-08-12 scale-10 captures remain in this directory as history; they are
  memory-backed and should not be compared against these numbers.
