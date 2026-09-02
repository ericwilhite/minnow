import type { PerformanceEngine } from "./performance-baseline.mts";

export interface PerformanceFailure {
  readonly workload: string;
  readonly engine: PerformanceEngine;
}

const FAILURE_LINE = /^ {2}(.+?) vs (sqlite|pglite): ratio /gm;

export function performanceFailureKey({ workload, engine }: PerformanceFailure): string {
  return `${workload} vs ${engine}`;
}

/** Extracts only the structured regression lines emitted after a completed benchmark run. */
export function parsePerformanceFailures(output: string): readonly PerformanceFailure[] {
  return [...output.matchAll(FAILURE_LINE)].map((match) => ({
    workload: match[1] ?? "",
    engine: match[2] as PerformanceEngine,
  }));
}

/**
 * A noisy flag is actionable only when the same workload fails in two fresh processes. Which
 * engine flags it may differ between the runs: each comparison has its own threshold, so a real
 * slowdown can clear the SQLite line in one run and the PGlite line in the other, and demanding
 * the identical pair would wave it through. Two runs that flag disjoint workloads are still read
 * as one-off noisy samples. Returns every flag on a repeated workload, first run then second,
 * so the report names both comparisons.
 */
export function repeatedPerformanceFailures(
  first: readonly PerformanceFailure[],
  second: readonly PerformanceFailure[],
): readonly PerformanceFailure[] {
  const firstWorkloads = new Set(first.map((failure) => failure.workload));
  const repeatedWorkloads = new Set(
    second.map((failure) => failure.workload).filter((workload) => firstWorkloads.has(workload)),
  );
  const repeated = new Map<string, PerformanceFailure>();
  for (const failure of [...first, ...second]) {
    if (!repeatedWorkloads.has(failure.workload)) continue;
    repeated.set(performanceFailureKey(failure), failure);
  }
  return [...repeated.values()];
}
