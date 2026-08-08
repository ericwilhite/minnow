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

The generated graph is checked across 41 foreign-key paths and exercised by 15 oracle-checked
reference queries. The dashboard also exposes a bounded read-only ad-hoc reference SQL runner with
parse, median, p95, and row-flow metrics. These timings describe JavaScript execution over a loaded
snapshot; they are not public `query()`/`prepareQuery()` measurements.

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
