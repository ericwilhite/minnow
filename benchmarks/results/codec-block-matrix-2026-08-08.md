# Codec and block-size matrix — 2026-08-08

## Scope

- Source revision: `5a8a660` (`cc5d20d` introduced the capture workflow; `5a8a660` added the
  explicit aggregate timer used by these final runs).
- Host: macOS 26.5.2 build 25F84, Apple silicon (`arm64`).
- Runner: Playwright 1.62.1, headless, one browser and one matrix cell at a time.
- Browsers: Headless Chromium 151.0.7922.34 and Firefox 153.0.
- Dataset: deterministic commerce scale 100, or 1,930,800 rows across 27 tables and 122 columns.
- Encoded logical column payload: approximately 94.50 million bytes (90.12 MiB) per cell.
- Matrix: raw, byte-RLE, and gzip at 256 KiB, 512 KiB, 1 MiB, 2 MiB, and 4 MiB targets.
- IndexedDB durability: relaxed.
- Samples: one observation per browser/configuration cell. All 30 observations passed block,
  transaction, public-library, mutation, compaction, and reference-query verification.

The raw exports retain the environment, every timing, all higher-level checks, and every physical
block measurement:

- [`2026-08-08-codec-block-matrix-chromium.json`](./2026-08-08-codec-block-matrix-chromium.json)
- [`2026-08-08-codec-block-matrix-firefox.json`](./2026-08-08-codec-block-matrix-firefox.json)

`Round trip` is the lower-level `timingsMs.total`: generate, encode/compress, journaled IndexedDB
write and manifest commit, read, checksum/decode, value verification, and numeric aggregation. The
public-library and reference-query probes run afterward and are preserved in the raw files, but are
not included in this particular total.

## Raw

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    445 |       90.15 MiB |          3,455.7 ms |      90.15 MiB |         3,728.1 ms |    3,728.1 ms |
| 512 KiB |    264 |       90.14 MiB |          3,555.3 ms |      90.14 MiB |         3,664.7 ms |    3,664.7 ms |
|   1 MiB |    179 |       90.13 MiB |          3,604.5 ms |      90.13 MiB |         3,928.4 ms |    3,928.4 ms |
|   2 MiB |    143 |       90.13 MiB |          3,612.5 ms |      90.13 MiB |         3,987.4 ms |    3,987.4 ms |
|   4 MiB |    123 |       90.13 MiB |          3,986.3 ms |      90.13 MiB |         3,868.9 ms |    3,986.3 ms |

## Byte-RLE

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    445 |      111.13 MiB |          7,432.7 ms |     111.13 MiB |         5,813.0 ms |    7,432.7 ms |
| 512 KiB |    264 |      111.61 MiB |          6,764.6 ms |     111.61 MiB |         5,864.2 ms |    6,764.6 ms |
|   1 MiB |    179 |      111.85 MiB |          6,369.9 ms |     111.85 MiB |         6,102.7 ms |    6,369.9 ms |
|   2 MiB |    143 |      111.94 MiB |          6,203.9 ms |     111.94 MiB |         6,119.0 ms |    6,203.9 ms |
|   4 MiB |    123 |      111.98 MiB |          6,324.7 ms |     111.98 MiB |         6,235.4 ms |    6,324.7 ms |

## Gzip

|  Target | Blocks | Chromium stored | Chromium round trip | Firefox stored | Firefox round trip | Worst browser |
| ------: | -----: | --------------: | ------------------: | -------------: | -----------------: | ------------: |
| 256 KiB |    445 |        9.59 MiB |          9,661.3 ms |       9.79 MiB |         4,268.4 ms |    9,661.3 ms |
| 512 KiB |    264 |        9.54 MiB |          7,433.6 ms |       9.77 MiB |         4,178.6 ms |    7,433.6 ms |
|   1 MiB |    179 |        9.51 MiB |          7,006.3 ms |       9.77 MiB |         4,075.8 ms |    7,006.3 ms |
|   2 MiB |    143 |        9.49 MiB |          6,321.0 ms |       9.77 MiB |         4,138.5 ms |    6,321.0 ms |
|   4 MiB |    123 |        9.48 MiB |          6,271.3 ms |       9.76 MiB |         4,086.7 ms |    6,271.3 ms |

Numeric aggregation took 33.2–40.8 ms per cell in Chromium and 35.7–40.1 ms in Firefox. The full
per-phase observations are in the raw exports.

## Decision

Use **gzip with a provisional 2 MiB target** for storage-oriented benchmark and physical-rewrite
workloads:

- 2 MiB is the smallest target within 10% of the best gzip worst-browser round trip; it is only
  0.8% behind 4 MiB while using smaller individual units.
- Gzip at 2 MiB saved 89.5% versus raw in Chromium and 89.2% in Firefox on this dataset.
- The accepted tradeoff is a 1.75× Chromium lower-level round trip versus raw at 2 MiB; Firefox's
  measured penalty was 1.04×.
- Byte-RLE is dominated here: it is slower than raw and increases stored bytes. It remains a
  correctness codec because other value distributions may behave differently.

This does not silently change the current public `MinnowDatabase` default. That API partitions by
rows rather than target bytes and remains configurable. Phase 6B must persist an explicit byte
budget, target layout, and output codec before adopting this policy for background rewrites.
Selective-scan and memory-pressure evidence from Phase 7 may revise the provisional target.

## Caveats

- These are single observations from one otherwise-idle machine, not browser rankings or
  performance promises; no median or p95 can be inferred.
- Headless browser behavior and browser-native gzip implementations differ.
- Target bytes are estimated before encoding. At the selected 2 MiB target, actual encoded blocks
  ranged up to 2,492,251 bytes; p95 was 2,097,152 bytes. Compressed blocks were at most 539,001
  bytes in these runs.
- The lower-level matrix now honors all five target sizes. The public-library probe still caps
  generated batches at 100,000 rows, and the reference-query probe uses 50,000-row blocks; their
  upper target settings therefore do not represent distinct layouts.
- Browser heap telemetry was unavailable. The result records exact block bytes and browser storage
  estimates, not total transient browser memory.
