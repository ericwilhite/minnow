# Benchmark results

Checked-in result files must record the fields listed in `ROADMAP.md`. Results are observations for
one environment, not performance promises. Do not compare runs when browser, operating system,
dataset shape, codec, block size, or durability differ materially.

The two small results dated 2026-08-07 are quick one-block smoke runs. They verify the complete
browser round trip in Chromium and Firefox but are too small to support a throughput or block-size
decision.

`2026-08-07-mutation-durability-matrix.json` records a 50,000-row commerce workload in Chromium and
Firefox with relaxed and strict IndexedDB durability. All four runs passed the same insert, upsert,
partial-update, projected-read, delete, snapshot/version, and competing-writer checks. On this one
machine, strict durability changed total time from 255.6 ms to 256.5 ms in Chromium and from 262 ms
to 269 ms in Firefox. Those single runs are directional observations, not durable browser rankings
or performance promises. They complete the planned browser/durability matrix, but they are not the
multi-scale performance curves required by the broader write-path exit gate.

The dashboard now writes a deterministic 27-table commerce graph and lists every physical block.
One multiplier grows all dimension, bridge, transaction, and ledger tables. Its public-library probe
measures insert, persistent-key upsert, narrow partial update, projected read, key delete, and
competing commits. Mutation results include encoding, staging, commit, retry, rows/s, and
write-amplification metrics. It also verifies conservative append-only compaction without presenting
retained historical blocks as reclaimed space.

The generated graph is checked across 81 foreign-key paths and exercised by 15 oracle-checked
reference queries. Those queries are now submitted to `prepareQuery()` and executed by the engine,
so their headline timings are public query API measurements; support is decided by compiling each
statement during the run, and the two statements the current surface cannot express — a monthly
cohort needing date truncation and an adjustment rollup needing `COALESCE` — record the engine's
own error rather than being quietly reworded. A hand-written JavaScript implementation runs beside
each query as a labeled baseline. The dashboard also exposes a bounded read-only ad-hoc SQL console
that reports the optimized plan from `explain()` with prepare, median, p95, and row-flow metrics.

Memory is recorded two ways. `performance.measureUserAgentSpecificMemory()` gives whole-agent bytes
including WebAssembly memories, which is the only figure that sees where SQLite, DuckDB, and PGlite
keep their data; `performance.memory.usedJSHeapSize` gives the JavaScript heap alone. Both are
sampled on the main thread, because a worker exposes neither API, and both are taken outside every
timed region because the whole-agent measurement waits for a garbage collection. Whole-agent bytes
require a Chromium browser on a cross-origin-isolated page with site isolation active; where that is
not met the dashboard records the reason instead of a zero. Engines run one after another in the
same agent and WebAssembly memories are never returned to the operating system, so per-engine growth
is the attributable number and the running total is not.

The dated 2026-08-08 codec/block-size bundle records the complete raw/RLE/gzip × five-target matrix
in Chromium and Firefox over 1,930,800 rows and about 90 MiB of encoded column payload. All 30 cells
verified. The accompanying decision record selects gzip with a provisional 2 MiB target for
storage-oriented benchmark and physical-rewrite work while leaving the row-partitioned public API
configurable. Larger quota-dependent datasets and repeated-sample distributions remain future
evidence work.

The dated four-engine report predates BrowserDatabase's public read-only SQL API, so its
BrowserDatabase SQL cells remain explicitly unmeasured. The current dashboard comparison now runs
the shared six-query workload through `prepareQuery()`/`query()` and requires matching checksums
from all four engines; a new checked-in run is still needed to publish those timings.
