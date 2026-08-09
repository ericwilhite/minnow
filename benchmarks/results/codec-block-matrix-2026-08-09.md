# Codec and block-size matrix — 2026-08-09

## Scope

- Source revision: `7128ed0` (the first 50-table dashboard revision; the dev server served this
  working tree for both browser runs).
- Host: macOS on Apple silicon (`arm64`, Darwin 25.5.0), 12 logical cores.
- Runner: Playwright 1.54.2, headless, one browser and one matrix cell at a time.
- Browsers: Headless Chromium 151.0.7922.34 and Firefox 153.0.
- Dataset: deterministic commerce scale 10, or 956,160 rows across 50 tables and 242 columns.
- Encoded logical column payload: approximately 48.30 million bytes (46.06 MiB) per cell.
- Matrix: raw, byte-RLE, and gzip at 256 KiB, 512 KiB, 1 MiB, 2 MiB, and 4 MiB targets.
- IndexedDB durability: relaxed.
- Samples: one observation per browser/configuration cell. All 30 observations passed block,
  transaction, public-library, mutation, compaction, and reference-query verification.
- Wall time: about 1.4 h for the Chromium run and 1.0 h for the Firefox run; each cell also runs
  the public-library write probes and the measured 15-query reference suite, which dominate cell
  time at this scale.

The raw exports retain the environment, every timing, all higher-level checks, and every physical
block measurement:

- [`2026-08-09-codec-block-matrix-chromium.json`](./2026-08-09-codec-block-matrix-chromium.json)
- [`2026-08-09-codec-block-matrix-firefox.json`](./2026-08-09-codec-block-matrix-firefox.json)

`Round trip` is the lower-level `timingsMs.total`: generate, encode/compress, journaled IndexedDB
write and manifest commit, read, checksum/decode, value verification, and numeric aggregation. The
public-library and reference-query probes run afterward and are preserved in the raw files, but are
not included in this particular total.

This record replaces neither the 2026-08-08 27-table scale-100 record nor its decision; it refreshes
the observation set for the expanded 50-table graph. A scale-100 attempt on the 50-table graph
(9,561,600 rows) was abandoned: a single cell took roughly 40 minutes, dominated by the measured
per-cell reference suite rather than storage work, so the 15-cell matrix would have run for many
hours per browser. Bounding the per-cell reference work for matrix runs is tracked as follow-up
work; larger-scale captures remain open until then.

## Raw

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    361 |       46.09 MiB |          3,143.2 ms |      46.09 MiB |         5,258.2 ms |    5,258.2 ms |
| 512 KiB |    290 |       46.08 MiB |          2,884.8 ms |      46.08 MiB |         4,031.8 ms |    4,031.8 ms |
|   1 MiB |    258 |       46.08 MiB |          3,068.7 ms |      46.08 MiB |         3,558.0 ms |    3,558.0 ms |
|   2 MiB |    245 |       46.08 MiB |          3,168.0 ms |      46.08 MiB |         3,090.5 ms |    3,168.0 ms |
|   4 MiB |    242 |       46.08 MiB |          3,002.5 ms |      46.08 MiB |         3,064.3 ms |    3,064.3 ms |

## Byte-RLE

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    361 |       61.70 MiB |          5,462.1 ms |      61.70 MiB |         5,707.7 ms |    5,707.7 ms |
| 512 KiB |    290 |       62.15 MiB |          4,553.4 ms |      62.15 MiB |         4,352.9 ms |    4,553.4 ms |
|   1 MiB |    258 |       62.34 MiB |          4,368.0 ms |      62.34 MiB |         3,753.6 ms |    4,368.0 ms |
|   2 MiB |    245 |       62.44 MiB |          4,636.0 ms |      62.44 MiB |         3,879.6 ms |    4,636.0 ms |
|   4 MiB |    242 |       62.50 MiB |          4,508.5 ms |      62.50 MiB |         3,965.0 ms |    4,508.5 ms |

## Gzip

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    361 |        6.71 MiB |          5,254.8 ms |       6.78 MiB |         4,586.9 ms |    5,254.8 ms |
| 512 KiB |    290 |        6.73 MiB |          4,517.0 ms |       6.81 MiB |         3,854.2 ms |    4,517.0 ms |
|   1 MiB |    258 |        6.74 MiB |          4,391.3 ms |       6.82 MiB |         3,490.3 ms |    4,391.3 ms |
|   2 MiB |    245 |        6.74 MiB |          4,441.5 ms |       6.82 MiB |         3,387.2 ms |    4,441.5 ms |
|   4 MiB |    242 |        6.75 MiB |          4,457.5 ms |       6.83 MiB |         3,409.8 ms |    4,457.5 ms |

Numeric aggregation took 13.3–14.7 ms per cell in Chromium and 13.6–19.6 ms in Firefox. The full
per-phase observations are in the raw exports.

## Reading

The 2026-08-08 decision — gzip with a provisional 2 MiB target for storage-oriented physical
rewrites — remains consistent with these observations. Gzip stores 6.7–6.8 MiB against 46.1 MiB raw
(a 6.8× reduction) at every block size; its worst-browser round trip at 2 MiB (4,441.5 ms) sits
within about 10% of its best size, and raw's speed advantage in Chromium (roughly 29% at 2 MiB)
shrinks to near parity in Firefox. Byte-RLE again stores more than raw on this workload and is not
competitive. Block-size sensitivity is small across all three codecs at this scale; 256 KiB is
consistently the slowest target in both browsers.
