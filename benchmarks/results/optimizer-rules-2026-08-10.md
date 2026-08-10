# Optimizer rule value — 2026-08-10

## Scope

- Source revision: the Phase 13B working tree (deterministic rewrites plus exact-count join
  ordering and the dictionary-equality rewrite).
- Host: macOS on Apple silicon (`arm64`, Darwin 25.5.0), 12 logical cores, Node under vitest.
- Store: `MemoryBlockStore` row-input preparation through `createPreparedQuery`, isolating rule
  cost from IndexedDB I/O. Timings are the median of five runs including preparation; peaks are
  the modeled accounted bytes reported by `memoryUsage.peakBytes`.
- Dataset: a 200,000-row fact table (`v`, `tag` with 40 distinct strings, and four extra numeric
  columns) and a 20-row dimension table.
- Method: each rewrite compares the same SQL compiled with and without the rule (`optimize:
false`, `chooseJoinOrder`, or the detection temporarily disabled for the dictionary rewrite).

Results are observations on this host, not promises.

## Observations

| Rule                       | Without rule | With rule | Value                           |
| -------------------------- | -----------: | --------: | ------------------------------- |
| CTE predicate pushdown     |     208.7 ms |   86.4 ms | 2.42x faster                    |
| Derived projection pruning |     308.0 ms |   79.5 ms | 3.87x faster                    |
| Join build-side selection  |     155.0 ms |  147.8 ms | 4,921,214 B -> 4,144,148 B peak |
| Dictionary-code equality   |      37.2 ms |   33.1 ms | 1.12x faster                    |

- Predicate pushdown shrinks the materialized derived input before the outer scan; the win grows
  with the selectivity of the pushed predicate.
- Projection pruning avoids materializing four unreferenced columns of the derived block.
- Join build-side selection is roughly wall-time neutral at this shape; its value is the smaller
  built index (about 780 KB less accounted peak here), which grows with the mis-written build
  side and matters most under explicit budgets.
- The dictionary rewrite removes per-row string materialization and comparison from equality
  filters; the measured 11% includes full preparation, so the filter-only improvement is larger.
