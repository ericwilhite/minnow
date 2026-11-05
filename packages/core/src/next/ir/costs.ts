/**
 * The executor's per-row CPU costs, as the planner's cost model reads them. The executor owns the
 * values (exec/executor.ts `EXECUTOR_COSTS`, measured by its benches and versioned with its
 * kernels); the type lives in ir/ because lower/ may not import exec/. The owner hands the values
 * to `Lowerer.lower` (lower/lowerer.ts), which multiplies them with estimated cardinalities
 * (design-joins-agg-spill.md "Statistics and ordering"):
 *
 * ```text
 * cost = Σ decodedValues × decodePerValue + filteredRows × filterPerRow
 *      + buildRows × hashBuildPerRow + probeRows × hashProbePerRow
 *      + aggregatedRows × aggregatePerRow + sortedRows × log2(sortedRows) × sortPerRowLog
 *      + emittedValues × emitPerValue + spillBytes / spillBytesPerSecond × 1e9
 * ```
 *
 * Costs are nanoseconds, single thread, warm cache. Only their ratios matter to plan choice.
 */
export interface ExecutorCosts {
  readonly decodePerValue: number;
  readonly filterPerRow: number;
  readonly hashBuildPerRow: number;
  readonly hashProbePerRow: number;
  readonly aggregatePerRow: number;
  readonly sortPerRowLog: number;
  readonly emitPerValue: number;
  /** Bytes per second of the spill path, write plus read back. */
  readonly spillBytesPerSecond: number;
}
