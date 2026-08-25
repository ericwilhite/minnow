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

/** A noisy flag is actionable only when the same comparison fails in two fresh processes. */
export function repeatedPerformanceFailures(
  first: readonly PerformanceFailure[],
  second: readonly PerformanceFailure[],
): readonly PerformanceFailure[] {
  const secondKeys = new Set(second.map(performanceFailureKey));
  return first.filter((failure) => secondKeys.has(performanceFailureKey(failure)));
}
