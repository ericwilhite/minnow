# Persistent browser database comparison — 2026-08-08

## Scope

- Dataset: 1,930,800 deterministic commerce rows across 27 related tables (100×).
- Generated batches: at most 50,000 rows and 2.61 MB of logical values.
- BrowserDatabase: gzip, 512 KiB target blocks, relaxed IndexedDB durability.
- SQLite Wasm: persistent OPFS SAH-pool VFS, WAL, `synchronous=NORMAL`, foreign keys, prepared
  inserts in immediate transactions.
- DuckDB-Wasm: in-memory async worker with Arrow ingestion. Its experimental OPFS database path
  reopened as an empty catalog in two real tests, so it is not mislabeled as persistent.
- PGlite: persistent IndexedDB filesystem, default durable syncing, 10,000-bind statement cap, and
  `ANALYZE` after index creation.
- SQL timing: one warm-up plus three measured executions for SQLite, DuckDB, and PGlite through
  each public API. This run predates BrowserDatabase's current `query()`/`prepareQuery()` subset.

BrowserDatabase, SQLite, and PGlite were closed and reopened before their result could pass. All six
normalized query checksums matched across the three SQL engines. This run used the host Chromium
browser; the dashboard layout is separately tested at a 390 × 844 mobile viewport.

## Four-engine comparison

| Metric                |          BrowserDatabase |  SQLite Wasm 3.53.0 | DuckDB-Wasm 1.5.4 |             PGlite 0.5.4 |
| --------------------- | -----------------------: | ------------------: | ----------------: | -----------------------: |
| Persistence           | IndexedDB; reopen passed | OPFS; reopen passed |       Memory only | IndexedDB; reopen passed |
| Public insert         |                   6.36 s |              9.04 s |            1.21 s |                  21.96 s |
| Throughput            |           303,723 rows/s |      213,557 rows/s |  1,589,803 rows/s |            87,938 rows/s |
| Schema + indexes      |                   7.6 ms |              1.04 s |          978.1 ms |                   5.17 s |
| Total adapter time    |                   8.05 s |             14.61 s |            5.19 s |                  35.66 s |
| Reopen                |                  0.19 ms |             0.48 ms |               n/a |                   3.15 s |
| Total database size   |                 33.48 MB |           149.25 MB |         377.70 MB |                288.09 MB |
| Public projected read |                 111.9 ms |           SQL below |         SQL below |                SQL below |

Size definitions are explicit because browser engines do not expose one universal physical-size
API:

- BrowserDatabase: complete logical IndexedDB payload, including block bytes, key chunks,
  manifests, segments, transactions, and catalog records. Browser-specific IndexedDB page overhead
  is not exposed.
- SQLite: complete allocated SQLite pages (`page_count × page_size`) after indexes.
- DuckDB: database memory reported by `PRAGMA database_size`; this is not durable storage.
- PGlite: complete PostgreSQL database size from `pg_database_size(current_database())`.

This historical run did not measure BrowserDatabase SQL. BrowserDatabase now exposes a bounded,
read-only `query()`/`prepareQuery()` subset, and the current dashboard adapter runs the same six
checksum-verified queries through it. The values below are intentionally not backfilled without a
new benchmark run.

## SQL latency recorded in this run

| Query median / p95            | BrowserDatabase |      SQLite Wasm |    DuckDB-Wasm |           PGlite |
| ----------------------------- | --------------: | ---------------: | -------------: | ---------------: |
| Q1 order count                |    Not measured |   0.03 / 0.03 ms | 0.24 / 0.34 ms |   27.7 / 29.0 ms |
| Q2 revenue by status          |    Not measured |   29.4 / 31.0 ms |   3.1 / 3.3 ms |   41.8 / 42.9 ms |
| Q3 customer-segment revenue   |    Not measured |   50.2 / 50.7 ms |   4.8 / 4.9 ms |   55.2 / 55.4 ms |
| Q4 category line-item revenue |    Not measured | 189.6 / 189.6 ms | 10.7 / 10.9 ms | 122.0 / 122.1 ms |
| Q5 tax by jurisdiction        |    Not measured | 170.3 / 171.7 ms | 10.9 / 11.2 ms | 152.2 / 152.5 ms |
| Q6 payment funnel             |    Not measured | 130.5 / 132.1 ms | 14.3 / 14.9 ms | 109.8 / 109.8 ms |
| Sum of query medians          |             n/a |         570.0 ms |        44.0 ms |         508.7 ms |

All result row counts and normalized checksums matched between SQLite, DuckDB, and PGlite.

## Optimization result

BrowserDatabase's previous 100× insert time was 126.55 s at 15,257 rows/s. The optimized run is
6.36 s at 303,723 rows/s: 19.9× faster. The internal unique-key representation now stores atomic,
versioned key chunks instead of issuing one IndexedDB operation per key, and each bounded
insert/upsert batch is staged in one IndexedDB write instead of one write transaction per column.

At this scale BrowserDatabase insert throughput is 1.42× SQLite's and 3.45× PGlite's. In-memory
DuckDB remains 5.23× faster. BrowserDatabase's total adapter time is 1.82× SQLite's speed and is now
within 1.55× of DuckDB's total time, despite persisting, compressing, and close/reopen verification.

## Frontend artifact cost

Adapters are lazy-loaded and are not part of the initial dashboard path.

| Path                                       | Raw production assets | Gzip estimate |
| ------------------------------------------ | --------------------: | ------------: |
| Initial HTML + CSS + JS + benchmark worker |        about 200.6 KB |   about 55 KB |
| SQLite selected path                       |         about 1.12 MB | about 0.48 MB |
| DuckDB EH selected path                    |              35.19 MB |       7.97 MB |
| PGlite path                                |              16.52 MB |       5.27 MB |

The complete production artifact directory contains both DuckDB alternatives and is about 98 MB
raw. Runtime bundle selection loads only the supported DuckDB path. Resource Timing can undercount
cached and sub-worker resources, so production artifact size remains authoritative.

## Memory disclosure

The test browser did not expose worker heap usage, so memory telemetry is reported as unavailable
instead of estimated. BrowserDatabase holds at most one 2.61 MB logical generated batch, encodes one
column at a time, and fetches/decodes each visible segment in windows of up to 16 blocks. The
generated insert batch's compressed blocks share one bounded IndexedDB transaction. `readTable()`
necessarily allocates the rows returned to its caller.
